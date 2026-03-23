#!/usr/bin/env python3

"""
Authentication Models and Utilities

Provides Pydantic models, password hashing, and JWT token management for authentication.
"""

import logging
import hashlib
import secrets
import jwt
import base64
import json
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
from pydantic import BaseModel, EmailStr, Field, validator
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# ENCRYPTION/DECRYPTION FUNCTIONS FOR PASSWORD RESET TOKENS
# ============================================================================

def encrypt_email(email: str, secret_key: str) -> str:
    """
    Encrypt email address with timestamp using AES-256 encryption (Fernet)
    Returns a URL-safe token without needing URL encoding
    """
    try:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'password_reset_salt',
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(secret_key.encode()))
        cipher = Fernet(key)
        
        expiry = datetime.utcnow() + timedelta(hours=24)
        payload = {
            "email": email.lower(),
            "exp": expiry.isoformat()
        }
        
        encrypted = cipher.encrypt(json.dumps(payload).encode())
        
        # Convert to URL-safe base64 (no padding)
        token = base64.urlsafe_b64encode(encrypted).decode('utf-8').rstrip('=')
        logger.info(f"?? Email encrypted successfully for: {email}")
        return token
        
    except Exception as e:
        logger.error(f"? Encryption error: {e}", exc_info=True)
        raise ValueError(f"Failed to encrypt email: {str(e)}")


def decrypt_email(token: str, secret_key: str) -> Optional[str]:
    """
    Decrypt token to extract email address and validate expiration
    """
    try:
        logger.info(f"?? Attempting to decrypt token (length: {len(token)})")
        
        # Add back padding if needed
        padding = 4 - (len(token) % 4)
        if padding != 4:
            token += '=' * padding
        
        # Decode from URL-safe base64
        encrypted = base64.urlsafe_b64decode(token.encode('utf-8'))
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'password_reset_salt',
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(secret_key.encode()))
        cipher = Fernet(key)
        
        decrypted = cipher.decrypt(encrypted)
        payload = json.loads(decrypted.decode())
        
        expiry = datetime.fromisoformat(payload['exp'])
        now = datetime.utcnow()
        
        if now > expiry:
            logger.warning(f"?? Token has expired")
            return None
        
        email = payload['email']
        logger.info(f"? Token decrypted successfully for: {email}")
        return email
        
    except Exception as e:
        logger.error(f"? Decryption error: {type(e).__name__}: {e}", exc_info=True)
        return None

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class UserSignUpRequest(BaseModel):
    """User sign-up request model"""
    name: str = Field(..., min_length=2, max_length=100, description="User's full name")
    email: EmailStr = Field(..., description="User's email address")
    password: str = Field(..., min_length=8, max_length=128, description="User's password")
    
    @validator('name')
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip()
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'[0-9]', v):
            raise ValueError('Password must contain at least one digit')
        return v

class UserSignInRequest(BaseModel):
    """User sign-in request model"""
    email: EmailStr = Field(..., description="User's email address")
    password: str = Field(..., description="User's password")

class ForgotPasswordRequest(BaseModel):
    """Forgot password request model"""
    email: EmailStr = Field(..., description="User's email address")

class ResetPasswordRequest(BaseModel):
    """Reset password request model using encrypted token."""
    token: str = Field(..., description="Encrypted password reset token")
    new_password: str = Field(..., min_length=8, max_length=128, description="New password")
    confirm_password: str = Field(..., min_length=8, max_length=128, description="Confirm new password")
    
    @validator('new_password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'[0-9]', v):
            raise ValueError('Password must contain at least one digit')
        return v
    
    @validator('confirm_password')
    def passwords_match(cls, v, values):
        if 'new_password' in values and v != values['new_password']:
            raise ValueError('Passwords do not match')
        return v

class UserResponse(BaseModel):
    """User response model"""
    user_id: int
    name: str
    email: str
    created_at: str
    is_active: bool

class AuthResponse(BaseModel):
    """Authentication response model"""
    success: bool
    message: str
    access_token: Optional[str] = None
    token_type: str = "bearer"
    user: Optional[UserResponse] = None
    expires_in: Optional[int] = None

class PasswordHasher:
    """Secure password hashing utility using SHA-256 with salt"""
    
    @staticmethod
    def hash_password(password: str, salt: Optional[str] = None) -> Tuple[str, str]:

        if salt is None:
            salt = secrets.token_hex(32)  # 64 character hex string
        
        # Combine password and salt, then hash
        password_salt = f"{password}{salt}".encode('utf-8')
        hashed = hashlib.sha256(password_salt).hexdigest()
        
        return hashed, salt
    
    @staticmethod
    def verify_password(password: str, hashed_password: str, salt: str) -> bool:

        computed_hash, _ = PasswordHasher.hash_password(password, salt)
        return computed_hash == hashed_password

class JWTManager:
    """JWT token management for authentication"""
    
    def __init__(self, secret_key: str, algorithm: str = "HS256",
                 access_token_expire_minutes: int = 1440):

        self.secret_key = secret_key
        self.algorithm = algorithm
        self.access_token_expire_minutes = access_token_expire_minutes
    
    def create_access_token(self, data: Dict[str, Any],
                           expires_delta: Optional[timedelta] = None) -> str:

        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        })
        
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
 
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            return None
    
    def create_reset_token(self, user_id: int, email: str) -> str:

        data = {
            "user_id": user_id,
            "email": email,
            "type": "password_reset"
        }
        
        expire = datetime.utcnow() + timedelta(hours=24)  # 24 hours
        data["exp"] = expire
        
        return jwt.encode(data, self.secret_key, algorithm=self.algorithm)
    
    def verify_reset_token(self, token: str) -> Optional[Dict[str, Any]]:

        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            if payload.get("type") != "password_reset":
                logger.warning("Invalid token type")
                return None
            
            return payload
        
        except jwt.ExpiredSignatureError:
            logger.warning("Reset token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid reset token: {e}")
            return None