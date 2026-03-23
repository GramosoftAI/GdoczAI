#!/usr/bin/env python3

"""
Authentication Processing and Management.

Provides core authentication operations:
- User sign-up and sign-in
- Password reset with encrypted tokens
- OTP-based email verification
- Token management
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import secrets

from src.api.models.auth_models import (
    encrypt_email,
    decrypt_email,
    PasswordHasher,
    JWTManager,
    UserSignUpRequest,
    UserSignInRequest,
    UserResponse,
    AuthResponse
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuthManager:
    """Manages user authentication, registration, and password reset operations"""
    
    def __init__(self, db_config: Dict[str, Any], jwt_secret_key: str,
                 encryption_key: str, access_token_expire_minutes: int = 1440):

        self.db_config = db_config
        self.jwt_secret_key = jwt_secret_key
        self.encryption_key = encryption_key
        self.access_token_expire_minutes = access_token_expire_minutes
        
        # Initialize JWT manager
        self.jwt_manager = JWTManager(
            secret_key=jwt_secret_key,
            access_token_expire_minutes=access_token_expire_minutes
        )
        
        # Initialize password hasher
        self.password_hasher = PasswordHasher()
    
    def _get_db_connection(self):

        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}", exc_info=True)
            raise ValueError(f"Database connection failed: {str(e)}")
    
    def sign_up(self, name: str, email: str, password: str) -> AuthResponse:

        try:
            # Validate input using Pydantic
            signup_request = UserSignUpRequest(name=name, email=email, password=password)
            
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if user already exists
            cursor.execute("SELECT user_id FROM users WHERE email = %s", (email.lower(),))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return AuthResponse(
                    success=False,
                    message="User already exists with this email"
                )
            
            # Hash password
            hashed_password, salt = self.password_hasher.hash_password(password)
            
            # Insert new user
            cursor.execute("""
                INSERT INTO users (name, email, password_hash, password_salt, is_active, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING user_id, name, email, created_at, is_active
            """, (name, email.lower(), hashed_password, salt, True, datetime.utcnow()))
            
            user_data = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            # Create access token
            token_data = {
                "user_id": user_data['user_id'],
                "email": user_data['email']
            }
            access_token = self.jwt_manager.create_access_token(data=token_data)
            
            user_response = UserResponse(
                user_id=user_data['user_id'],
                name=user_data['name'],
                email=user_data['email'],
                created_at=str(user_data['created_at']),
                is_active=user_data['is_active']
            )
            
            logger.info(f"[SUCCESS] User registered successfully: {email}")
            
            return AuthResponse(
                success=True,
                message="User registered successfully",
                access_token=access_token,
                user=user_response,
                expires_in=self.access_token_expire_minutes * 60
            )
        
        except psycopg2.Error as e:
            logger.error(f"Database error during sign-up: {e}", exc_info=True)
            return AuthResponse(
                success=False,
                message=f"Registration failed: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Error during sign-up: {e}", exc_info=True)
            return AuthResponse(
                success=False,
                message=f"Registration failed: {str(e)}"
            )
    
    def sign_in(self, email: str, password: str) -> AuthResponse:

        try:
            # Validate input
            signin_request = UserSignInRequest(email=email, password=password)
            
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get user from database
            cursor.execute("""
                SELECT user_id, name, email, password_hash, password_salt, is_active, created_at
                FROM users WHERE email = %s
            """, (email.lower(),))
            
            user = cursor.fetchone()
            
            if not user:
                cursor.close()
                conn.close()
                logger.warning(f"[FAILED] Sign-in failed: User not found for {email}")
                return AuthResponse(
                    success=False,
                    message="Invalid email or password"
                )
            
            # Verify password
            if not self.password_hasher.verify_password(
                password, user['password_hash'], user['password_salt']
            ):
                cursor.close()
                conn.close()
                logger.warning(f"[FAILED] Sign-in failed: Invalid password for {email}")
                return AuthResponse(
                    success=False,
                    message="Invalid email or password"
                )
            
            if not user['is_active']:
                cursor.close()
                conn.close()
                logger.warning(f"[FAILED] Sign-in failed: Account is not active for {email}")
                return AuthResponse(
                    success=False,
                    message="Account is not active"
                )
            
            # Update last login
            cursor.execute("""
                UPDATE users SET last_login = %s WHERE user_id = %s
            """, (datetime.utcnow(), user['user_id']))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Create access token
            token_data = {
                "user_id": user['user_id'],
                "email": user['email']
            }
            access_token = self.jwt_manager.create_access_token(data=token_data)
            
            user_response = UserResponse(
                user_id=user['user_id'],
                name=user['name'],
                email=user['email'],
                created_at=str(user['created_at']),
                is_active=user['is_active']
            )
            
            logger.info(f"[SUCCESS] User signed in successfully: {email}")
            
            return AuthResponse(
                success=True,
                message="Sign-in successful",
                access_token=access_token,
                user=user_response,
                expires_in=self.access_token_expire_minutes * 60
            )
        
        except psycopg2.Error as e:
            logger.error(f"Database error during sign-in: {e}", exc_info=True)
            return AuthResponse(
                success=False,
                message=f"Sign-in failed: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Error during sign-in: {e}", exc_info=True)
            return AuthResponse(
                success=False,
                message=f"Sign-in failed: {str(e)}"
            )
    
    def request_password_reset(self, email: str) -> Tuple[bool, str, Optional[str]]:

        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if user exists
            cursor.execute("SELECT user_id, email FROM users WHERE email = %s", (email.lower(),))
            user = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if not user:
                logger.warning(f"[FAILED] Password reset requested for non-existent user: {email}")
                return False, "If an account exists with this email, a reset link will be sent", None
            
            # Encrypt email in token
            reset_token = encrypt_email(user['email'], self.encryption_key)
            
            logger.info(f"[SUCCESS] Password reset token generated for: {email}")
            
            return True, "Password reset token generated successfully", reset_token
        
        except Exception as e:
            logger.error(f"Error generating reset token: {e}", exc_info=True)
            return False, f"Error generating reset token: {str(e)}", None
    
    def reset_password(self, email: str, new_password: str) -> Tuple[bool, str]:

        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Hash new password
            hashed_password, salt = self.password_hasher.hash_password(new_password)
            
            # Update password
            cursor.execute("""
                UPDATE users
                SET password_hash = %s, password_salt = %s, updated_at = %s
                WHERE email = %s
            """, (hashed_password, salt, datetime.utcnow(), email.lower()))
            
            if cursor.rowcount == 0:
                conn.close()
                logger.warning(f"[FAILED] User not found for password reset: {email}")
                return False, "User not found"
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"[SUCCESS] Password reset successfully for: {email}")
            return True, "Password reset successfully"
        
        except psycopg2.Error as e:
            logger.error(f"Database error during password reset: {e}", exc_info=True)
            return False, f"Password reset failed: {str(e)}"
        except Exception as e:
            logger.error(f"Error during password reset: {e}", exc_info=True)
            return False, f"Password reset failed: {str(e)}"
    
    def reset_password_with_token(self, token: str, new_password: str) -> Tuple[bool, str]:

        try:
            # Decrypt email from token
            email = decrypt_email(token, self.encryption_key)
            
            if not email:
                logger.warning("[FAILED] Invalid or expired password reset token")
                return False, "Invalid or expired password reset token"
            
            # Reset password using the decrypted email
            return self.reset_password(email, new_password)
        
        except Exception as e:
            logger.error(f"Error during token-based password reset: {e}", exc_info=True)
            return False, f"Password reset failed: {str(e)}"
    
    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        return self.jwt_manager.verify_token(token)
    
    def get_user_by_id(self, user_id: int) -> Optional[UserResponse]:

        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT user_id, name, email, created_at, is_active
                FROM users WHERE user_id = %s
            """, (user_id,))
            
            user = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not user:
                return None
            
            return UserResponse(
                user_id=user['user_id'],
                name=user['name'],
                email=user['email'],
                created_at=str(user['created_at']),
                is_active=user['is_active']
            )
        
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving user: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error retrieving user: {e}", exc_info=True)
            return None
    
    def generate_signup_otp(self, name: str, email: str, password: str) -> Tuple[bool, str, Optional[str]]:

        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if user already exists
            cursor.execute("SELECT user_id FROM users WHERE email = %s", (email.lower(),))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                logger.warning(f"[FAILED] OTP generation failed: User already exists for {email}")
                return False, "User already exists with this email", None
            
            # Hash password
            hashed_password, salt = self.password_hasher.hash_password(password)
            
            # Generate 5-digit OTP
            otp = ''.join([str(secrets.randbelow(10)) for _ in range(5)])
            
            # Calculate expiry (5 minutes from now)
            expires_at = datetime.utcnow() + timedelta(minutes=5)
            
            # Store OTP in signup_otps table
            cursor.execute("""
                INSERT INTO signup_otps (email, otp, name, password_hash, password_salt, expires_at, attempts, is_verified)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (email) DO UPDATE SET
                    otp = EXCLUDED.otp,
                    name = EXCLUDED.name,
                    password_hash = EXCLUDED.password_hash,
                    password_salt = EXCLUDED.password_salt,
                    expires_at = EXCLUDED.expires_at,
                    attempts = 0,
                    is_verified = false
            """, (email.lower(), otp, name, hashed_password, salt, expires_at, 0, False))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"[SUCCESS] OTP generated successfully for: {email}")
            
            return True, f"OTP sent to {email}. Valid for 5 minutes.", otp
        
        except psycopg2.Error as e:
            logger.error(f"Database error generating OTP: {e}", exc_info=True)
            return False, f"OTP generation failed: {str(e)}", None
        except Exception as e:
            logger.error(f"Error generating OTP: {e}", exc_info=True)
            return False, f"OTP generation failed: {str(e)}", None
    
    def verify_signup_otp(self, email: str, otp: str) -> AuthResponse:

        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get OTP record
            cursor.execute("""
                SELECT email, otp, name, password_hash, password_salt, expires_at, attempts, is_verified
                FROM signup_otps WHERE email = %s
            """, (email.lower(),))
            
            otp_record = cursor.fetchone()
            
            if not otp_record:
                cursor.close()
                conn.close()
                logger.warning(f"[FAILED] OTP verification failed: No OTP found for {email}")
                return AuthResponse(
                    success=False,
                    message="No OTP found for this email"
                )
            
            # Check if OTP has expired
            if datetime.utcnow() > otp_record['expires_at']:
                cursor.close()
                conn.close()
                logger.warning(f"[FAILED] OTP expired for {email}")
                return AuthResponse(
                    success=False,
                    message="OTP has expired"
                )
            
            # Check if max attempts exceeded
            if otp_record['attempts'] >= 3:
                cursor.close()
                conn.close()
                logger.warning(f"[FAILED] OTP verification failed: Max attempts exceeded for {email}")
                return AuthResponse(
                    success=False,
                    message="Maximum OTP verification attempts exceeded"
                )
            
            # Verify OTP
            if otp_record['otp'] != otp:
                # Increment attempts
                cursor.execute("""
                    UPDATE signup_otps SET attempts = attempts + 1 WHERE email = %s
                """, (email.lower(),))
                conn.commit()
                cursor.close()
                conn.close()
                logger.warning(f"[FAILED] OTP verification failed: Invalid OTP for {email}")
                return AuthResponse(
                    success=False,
                    message="Invalid OTP"
                )
            
            # Mark as verified
            cursor.execute("""
                UPDATE signup_otps SET is_verified = true WHERE email = %s
            """, (email.lower(),))
            
            # Create user account
            cursor.execute("""
                INSERT INTO users (name, email, password_hash, password_salt, is_active, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING user_id, name, email, created_at, is_active
            """, (
                otp_record['name'],
                email.lower(),
                otp_record['password_hash'],
                otp_record['password_salt'],
                True,
                datetime.utcnow()
            ))
            
            user_data = cursor.fetchone()
            
            # Delete OTP record
            cursor.execute("DELETE FROM signup_otps WHERE email = %s", (email.lower(),))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Create access token
            token_data = {
                "user_id": user_data['user_id'],
                "email": user_data['email']
            }
            access_token = self.jwt_manager.create_access_token(data=token_data)
            
            user_response = UserResponse(
                user_id=user_data['user_id'],
                name=user_data['name'],
                email=user_data['email'],
                created_at=str(user_data['created_at']),
                is_active=user_data['is_active']
            )
            
            logger.info(f"[SUCCESS] User verified and registered successfully: {email}")
            
            return AuthResponse(
                success=True,
                message="User registered and verified successfully",
                access_token=access_token,
                user=user_response,
                expires_in=self.access_token_expire_minutes * 60
            )
        
        except psycopg2.Error as e:
            logger.error(f"Database error during OTP verification: {e}", exc_info=True)
            return AuthResponse(
                success=False,
                message=f"OTP verification failed: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Error during OTP verification: {e}", exc_info=True)
            return AuthResponse(
                success=False,
                message=f"OTP verification failed: {str(e)}"
            )