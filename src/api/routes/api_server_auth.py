#!/usr/bin/env python3

"""
Authentication endpoints for Document Processing Pipeline API.

Provides:
- User signup and signin
- Password reset with encrypted tokens
- OTP-based email verification
- Current user information
"""

import logging
from fastapi import HTTPException, Depends, Form
from pydantic import EmailStr
from src.api.models.auth_models import (
    UserSignUpRequest, UserSignInRequest,
    ForgotPasswordRequest, ResetPasswordRequest, AuthResponse
)
from src.api.processing.auth_processing import AuthManager
from src.services.email.email_service import EmailService
from src.api.models.api_server_models import ConfigManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global references (set from main app)
auth_manager: AuthManager = None
email_service: EmailService = None

def set_global_services(auth_mgr: AuthManager, email_svc: EmailService):
    """Set global service instances"""
    global auth_manager, email_service
    auth_manager = auth_mgr
    email_service = email_svc

# ==================== AUTHENTICATION ENDPOINTS ====================

def create_auth_routes(app, get_current_user):
    """Create authentication routes"""
    
    @app.post("/auth/signup", tags=["Authentication"], response_model=AuthResponse)
    async def sign_up(request: UserSignUpRequest):
        """Register a new user"""
        try:
            if not auth_manager:
                raise HTTPException(status_code=503, detail="Authentication service not initialized")
            
            result = auth_manager.sign_up(request.name, request.email, request.password)
            
            if result.success and email_service and result.user:
                try:
                    email_service.send_welcome_email(result.user.email, result.user.name)
                except Exception as e:
                    logger.warning(f"Failed to send welcome email: {e}")
            
            if not result.success:
                raise HTTPException(status_code=400, detail=result.message)
            
            return result
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Sign up error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

    @app.post("/auth/signin", tags=["Authentication"], response_model=AuthResponse)
    async def sign_in(request: UserSignInRequest):
        """Authenticate user and get access token"""
        try:
            if not auth_manager:
                raise HTTPException(status_code=503, detail="Authentication service not initialized")
            
            result = auth_manager.sign_in(request.email, request.password)
            
            if not result.success:
                raise HTTPException(status_code=401, detail=result.message)
            
            return result
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Sign in error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Authentication failed: {str(e)}")

    @app.post("/auth/forgot-password", tags=["Authentication"])
    async def forgot_password(request: ForgotPasswordRequest):
        """Request password reset with encrypted token."""
        try:
            if not auth_manager:
                raise HTTPException(status_code=503, detail="Authentication service not initialized")
            
            # ? UPDATED: Now returns (success, message, encrypted_token)
            success, message, encrypted_token = auth_manager.request_password_reset(request.email)
            
            if success and email_service and encrypted_token:
                try:
                    conn = auth_manager._get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT name FROM users WHERE email = %s",
                        (request.email.lower(),)
                    )
                    user = cursor.fetchone()
                    cursor.close()
                    conn.close()
                    
                    if user:
                        user_name = user[0]
                        config_manager = ConfigManager("config/config.yaml")
                        config = config_manager.config
                        reset_url = config.get('email', {}).get('reset_password_url', 'http://localhost:3000/reset-password')
                        
                        # ? UPDATED: Pass encrypted_token to email service
                        email_sent = email_service.send_password_reset_email(
                            request.email,
                            user_name,
                            reset_url_base=reset_url,
                            encrypted_token=encrypted_token
                        )
                        
                        if email_sent:
                            message = "Password reset email sent successfully"
                            logger.info(f"? Password reset email sent with encrypted token to {request.email}")
                        else:
                            logger.warning("Failed to send reset email")
                
                except Exception as e:
                    logger.error(f"Failed to send reset email: {e}")
            
            return {
                "success": success,
                "message": message
            }
        
        except Exception as e:
            logger.error(f"Forgot password error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Password reset request failed: {str(e)}")

    @app.post("/auth/reset-password", tags=["Authentication"])
    async def reset_password(request: ResetPasswordRequest):
        """Reset password using encrypted token."""
        try:
            if not auth_manager:
                raise HTTPException(status_code=503, detail="Authentication service not initialized")
            
            # ? UPDATED: Call new reset_password_with_token method
            success, message = auth_manager.reset_password_with_token(request.token, request.new_password)
            
            if not success:
                raise HTTPException(status_code=400, detail=message)
            
            logger.info(f"? Password reset successful via encrypted token")
            
            return {
                "success": True,
                "message": message
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Reset password error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Password reset failed: {str(e)}")

    @app.post("/auth/signup/send-otp", tags=["Authentication"])
    async def send_signup_otp(request: UserSignUpRequest):
        """
        Send OTP for email verification (Step 1 of signup)
        
        Args:
            request: User signup details (name, email, password)
        
        Returns:
            Success message with OTP (for testing if email not configured)
        """
        try:
            if not auth_manager:
                raise HTTPException(status_code=503, detail="Authentication service not initialized")
            
            success, message, otp = auth_manager.generate_signup_otp(
                request.name,
                request.email,
                request.password
            )
            
            if not success:
                raise HTTPException(status_code=400, detail=message)
            
            # ? ADDED: Send OTP email if email service is configured
            if success and email_service:
                try:
                    email_sent = email_service.send_signup_otp_email(
                        request.email,
                        request.name,
                        otp
                    )
                    if email_sent:
                        logger.info(f"? OTP email sent successfully to {request.email}")
                    else:
                        logger.warning(f"?? Failed to send OTP email to {request.email}")
                except Exception as e:
                    logger.error(f"? Email sending error: {e}", exc_info=True)
            
            return {
                "success": success,
                "message": message,
                "otp": otp
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Send OTP error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to send OTP: {str(e)}")

    @app.post("/auth/signup/verify-otp", tags=["Authentication"])
    async def verify_signup_otp(email: EmailStr = Form(...), otp: str = Form(...)):
        """
        Verify OTP and complete signup (Step 2 of signup)
        
        Args:
            email: User's email
            otp: 5-digit OTP received in email
        
        Returns:
            Access token and user details
        """
        try:
            if not auth_manager:
                raise HTTPException(status_code=503, detail="Authentication service not initialized")
            
            response = auth_manager.verify_signup_otp(email, otp)
            
            if not response.success:
                raise HTTPException(status_code=400, detail=response.message)
            
            return response
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Verify OTP error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

    @app.get("/auth/me", tags=["Authentication"])
    async def get_current_user_info(current_user = Depends(get_current_user)):
        """Get current authenticated user information"""
        try:
            if not auth_manager:
                raise HTTPException(status_code=503, detail="Authentication service not initialized")
            
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid token data")
            
            user = auth_manager.get_user_by_id(user_id)
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            
            return {
                "success": True,
                "user": user
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Get user info error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to get user info: {str(e)}")
