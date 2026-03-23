#!/usr/bin/env python3

"""
Alert Mail CRUD endpoints for Document Processing Pipeline API.

Provides:
- Create, read, update, delete alert mail configurations
- Email validation for CC addresses
- Comma-separated multiple email addresses support
- One configuration per user
"""

import logging
import re
from fastapi import HTTPException, Depends
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field, validator
from typing import Optional
from src.api.models.api_server_models import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== EMAIL VALIDATION ====================

def validate_email(email: str) -> bool:
    """Validate email format using regex"""
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_pattern, email.strip()) is not None

def validate_email_list(email_string: str) -> tuple[bool, list[str], list[str]]:

    if not email_string or not email_string.strip():
        return True, [], []
    
    emails = [email.strip() for email in email_string.split(',')]
    valid_emails = []
    invalid_emails = []
    
    for email in emails:
        if email:  # Skip empty strings
            if validate_email(email):
                valid_emails.append(email)
            else:
                invalid_emails.append(email)
    
    return len(invalid_emails) == 0, valid_emails, invalid_emails

# ==================== PYDANTIC MODELS ====================

class AlertMailCreate(BaseModel):
    cc_mail: Optional[str] = Field(None, description="Comma-separated CC email addresses")
    
    @validator('cc_mail')
    def validate_cc_mail(cls, v):
        if v is None or v.strip() == '':
            return None
        
        all_valid, valid_emails, invalid_emails = validate_email_list(v)
        
        if not all_valid:
            raise ValueError(
                f"Invalid email format(s) detected: {', '.join(invalid_emails)}. "
                f"Please provide valid email addresses in format: user@domain.com"
            )
        
        # Return cleaned up email list
        return ', '.join(valid_emails)
    
    class Config:
        schema_extra = {
            "example": {
                "cc_mail": "admin@example.com, manager@example.com"
            }
        }

class AlertMailUpdate(BaseModel):
    cc_mail: Optional[str] = Field(None, description="Comma-separated CC email addresses (null to clear)")
    
    @validator('cc_mail')
    def validate_cc_mail(cls, v):
        # Allow explicit None to clear emails
        if v is None:
            return None
        
        # Allow empty string to clear emails
        if v.strip() == '':
            return None
        
        all_valid, valid_emails, invalid_emails = validate_email_list(v)
        
        if not all_valid:
            raise ValueError(
                f"Invalid email format(s) detected: {', '.join(invalid_emails)}. "
                f"Please provide valid email addresses in format: user@domain.com"
            )
        
        # Return cleaned up email list
        return ', '.join(valid_emails)
    
    class Config:
        schema_extra = {
            "example": {
                "cc_mail": "admin@example.com, manager@example.com, support@example.com"
            }
        }

class AlertMailResponse(BaseModel):
    id: int
    user_id: int
    cc_mail: Optional[str]
    created_at: str
    updated_at: str

# ==================== ALERT MAIL ENDPOINTS ====================

def create_alert_mail_routes(app, get_current_user):
    """Create alert mail configuration routes (one configuration per user)"""

    @app.post("/alert-mail", tags=["Alert Mail"], response_model=AlertMailResponse)
    async def create_alert_mail(
        request: AlertMailCreate,
        current_user = Depends(get_current_user)
    ):

        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if alert mail already exists for this user
            cursor.execute("""
                SELECT id FROM alert_mail
                WHERE user_id = %s
            """, (user_id,))
            
            existing = cursor.fetchone()
            if existing:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"Alert mail configuration already exists for user {user_id}. Use PUT to update or DELETE to remove it first."
                )
            
            # Insert new alert mail configuration
            cursor.execute("""
                INSERT INTO alert_mail 
                (user_id, cc_mail)
                VALUES (%s, %s)
                RETURNING id, user_id, cc_mail, created_at, updated_at
            """, (
                user_id,
                request.cc_mail
            ))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"?? Created alert mail configuration for user_id: {user_id}")
            if request.cc_mail:
                email_count = len(request.cc_mail.split(','))
                logger.info(f"   ? CC emails: {email_count} address(es)")
                logger.info(f"   ?? Addresses: {request.cc_mail}")
            else:
                logger.info(f"   ?? No CC emails configured")
            
            return AlertMailResponse(
                id=result['id'],
                user_id=result['user_id'],
                cc_mail=result['cc_mail'],
                created_at=result['created_at'].isoformat(),
                updated_at=result['updated_at'].isoformat()
            )
        
        except HTTPException:
            raise
        except ValueError as e:
            # Email validation error
            logger.error(f"Email validation error: {e}")
            if conn:
                conn.close()
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Error creating alert mail: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to create alert mail: {str(e)}")

    @app.get("/alert-mail", tags=["Alert Mail"])
    async def get_alert_mail(current_user = Depends(get_current_user)):

        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT id, user_id, cc_mail, created_at, updated_at
                FROM alert_mail
                WHERE user_id = %s
            """, (user_id,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result:
                return {
                    "success": True,
                    "alert_mail": None,
                    "message": "No alert mail configuration found for this user"
                }
            
            # Parse email list
            email_list = []
            if result['cc_mail']:
                email_list = [email.strip() for email in result['cc_mail'].split(',')]
            
            return {
                "success": True,
                "alert_mail": {
                    "id": result['id'],
                    "user_id": result['user_id'],
                    "cc_mail": result['cc_mail'],
                    "cc_mail_list": email_list,
                    "email_count": len(email_list),
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting alert mail: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to get alert mail: {str(e)}")

    @app.put("/alert-mail", tags=["Alert Mail"])
    async def update_alert_mail(
        request: AlertMailUpdate,
        current_user = Depends(get_current_user)
    ):

        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if alert mail exists for user
            cursor.execute("""
                SELECT id FROM alert_mail
                WHERE user_id = %s
            """, (user_id,))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404, 
                    detail="Alert mail configuration not found. Please create one first using POST /alert-mail"
                )
            
            # Update alert mail
            cursor.execute("""
                UPDATE alert_mail
                SET cc_mail = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING id, user_id, cc_mail, created_at, updated_at
            """, (request.cc_mail, user_id))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"?? Updated alert mail for user_id: {user_id}")
            if request.cc_mail:
                email_count = len(request.cc_mail.split(','))
                logger.info(f"   ? New CC emails: {email_count} address(es)")
                logger.info(f"   ?? Addresses: {request.cc_mail}")
            else:
                logger.info(f"   ??? CC emails cleared")
            
            # Parse email list
            email_list = []
            if result['cc_mail']:
                email_list = [email.strip() for email in result['cc_mail'].split(',')]
            
            return {
                "success": True,
                "message": "Alert mail configuration updated successfully",
                "alert_mail": {
                    "id": result['id'],
                    "user_id": result['user_id'],
                    "cc_mail": result['cc_mail'],
                    "cc_mail_list": email_list,
                    "email_count": len(email_list),
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except ValueError as e:
            # Email validation error
            logger.error(f"Email validation error: {e}")
            if conn:
                conn.close()
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Error updating alert mail: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to update alert mail: {str(e)}")

    @app.delete("/alert-mail", tags=["Alert Mail"])
    async def delete_alert_mail(current_user = Depends(get_current_user)):

        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Check if alert mail exists for user
            cursor.execute("""
                SELECT id FROM alert_mail
                WHERE user_id = %s
            """, (user_id,))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404, 
                    detail="Alert mail configuration not found for this user"
                )
            
            # Delete alert mail
            cursor.execute("""
                DELETE FROM alert_mail
                WHERE user_id = %s
            """, (user_id,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"??? Deleted alert mail configuration for user_id: {user_id}")
            
            return {
                "success": True,
                "message": "Alert mail configuration deleted successfully"
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting alert mail: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to delete alert mail: {str(e)}")

    @app.get("/alert-mail/validate-email", tags=["Alert Mail"])
    async def validate_email_endpoint(
        email: str,
        current_user = Depends(get_current_user)
    ):

        try:
            all_valid, valid_emails, invalid_emails = validate_email_list(email)
            
            return {
                "success": True,
                "all_valid": all_valid,
                "total_emails": len(valid_emails) + len(invalid_emails),
                "valid_count": len(valid_emails),
                "invalid_count": len(invalid_emails),
                "valid_emails": valid_emails,
                "invalid_emails": invalid_emails,
                "message": "All emails are valid" if all_valid else f"Found {len(invalid_emails)} invalid email(s)"
            }
        
        except Exception as e:
            logger.error(f"Error validating email: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to validate email: {str(e)}")

    logger.info("Alert mail routes registered successfully")
    logger.info("   POST /alert-mail - Create alert mail configuration")
    logger.info("   GET /alert-mail - Get alert mail configuration")
    logger.info("   PUT /alert-mail - Update alert mail configuration")
    logger.info("   DELETE /alert-mail - Delete alert mail configuration")
    logger.info("   GET /alert-mail/validate-email - Validate email format")