#!/usr/bin/env python3

"""
User Webhooks CRUD endpoints for Document Processing Pipeline API.

Provides:
- Create, read, update, delete user webhooks
- One webhook per user applies to all document types
- Webhook URL, token, and agent name configuration
"""

import logging
from fastapi import HTTPException, Depends
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field
from typing import Optional
from src.api.models.api_server_models import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== PYDANTIC MODELS ====================

class UserWebhookCreate(BaseModel):
    webhook_url: str = Field(..., description="Webhook URL endpoint")
    webhook_token: Optional[str] = Field(None, description="Authentication token for webhook")
    webhook_agent_name: Optional[str] = Field(None, description="Agent/service name")
    is_active: bool = Field(True, description="Enable/disable webhook")

class UserWebhookUpdate(BaseModel):
    webhook_url: Optional[str] = Field(None, description="Webhook URL endpoint")
    webhook_token: Optional[str] = Field(None, description="Authentication token for webhook")
    webhook_agent_name: Optional[str] = Field(None, description="Agent/service name")
    is_active: Optional[bool] = Field(None, description="Enable/disable webhook")

class UserWebhookResponse(BaseModel):
    id: int
    webhook_url: str
    webhook_token: Optional[str]
    webhook_agent_name: Optional[str]
    user_id: int
    is_active: bool
    created_at: str
    updated_at: str

# ==================== USER WEBHOOKS ENDPOINTS ====================

def create_user_webhook_routes(app, get_current_user):
    """Create user-based webhooks routes (one webhook per user)"""

    @app.post("/user-webhooks", tags=["User Webhooks"], response_model=UserWebhookResponse)
    async def create_user_webhook(
        request: UserWebhookCreate,
        current_user = Depends(get_current_user)
    ):
        """
        Create new webhook configuration for the user.
        
        Each user can only have ONE webhook that applies to all document types.
        
        Parameters:
        - webhook_url: The endpoint URL to send processed data
        - webhook_token: Optional authentication token
        - webhook_agent_name: Optional agent/service name
        - is_active: Enable/disable webhook (default: True)
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if webhook already exists for this user
            cursor.execute("""
                SELECT id FROM user_webhooks
                WHERE user_id = %s
            """, (user_id,))
            
            existing = cursor.fetchone()
            if existing:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"Webhook already exists for user {user_id}. Use PUT to update or DELETE to remove it first."
                )
            
            # Insert new webhook
            cursor.execute("""
                INSERT INTO user_webhooks 
                (user_id, webhook_url, webhook_token, webhook_agent_name, is_active)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, user_id, webhook_url, webhook_token, webhook_agent_name, 
                          is_active, created_at, updated_at
            """, (
                user_id,
                request.webhook_url, 
                request.webhook_token,
                request.webhook_agent_name,
                request.is_active
            ))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"?? Created webhook for user_id: {user_id}")
            logger.info(f"   ?? URL: {request.webhook_url}")
            logger.info(f"   ?? Agent: {request.webhook_agent_name or 'Not specified'}")
            logger.info(f"   ? Active: {request.is_active}")
            
            return UserWebhookResponse(
                id=result['id'],
                webhook_url=result['webhook_url'],
                webhook_token=result['webhook_token'],
                webhook_agent_name=result['webhook_agent_name'],
                user_id=result['user_id'],
                is_active=result['is_active'],
                created_at=result['created_at'].isoformat(),
                updated_at=result['updated_at'].isoformat()
            )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating user webhook: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to create webhook: {str(e)}")

    @app.get("/user-webhooks", tags=["User Webhooks"])
    async def get_user_webhook(current_user = Depends(get_current_user)):
        """
        Get webhook configuration for the authenticated user.
        
        Returns user's webhook configuration if exists.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT id, user_id, webhook_url, webhook_token, webhook_agent_name,
                       is_active, created_at, updated_at
                FROM user_webhooks
                WHERE user_id = %s
            """, (user_id,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result:
                return {
                    "success": True,
                    "webhook": None,
                    "message": "No webhook configured for this user"
                }
            
            return {
                "success": True,
                "webhook": {
                    "id": result['id'],
                    "user_id": result['user_id'],
                    "webhook_url": result['webhook_url'],
                    "webhook_token": result['webhook_token'],
                    "webhook_agent_name": result['webhook_agent_name'],
                    "is_active": result['is_active'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting user webhook: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to get webhook: {str(e)}")

    @app.put("/user-webhooks", tags=["User Webhooks"])
    async def update_user_webhook(
        request: UserWebhookUpdate,
        current_user = Depends(get_current_user)
    ):
        """
        Update webhook configuration for the user.
        
        Parameters:
        - webhook_url: Optional new webhook URL
        - webhook_token: Optional new authentication token
        - webhook_agent_name: Optional new agent name
        - is_active: Optional enable/disable webhook
        
        Only provided fields will be updated.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if webhook exists for user
            cursor.execute("""
                SELECT id FROM user_webhooks
                WHERE user_id = %s
            """, (user_id,))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404, 
                    detail="Webhook not found. Please create one first using POST /user-webhooks"
                )
            
            # Build dynamic update query
            update_fields = []
            update_values = []
            
            if request.webhook_url is not None:
                update_fields.append("webhook_url = %s")
                update_values.append(request.webhook_url)
            
            if request.webhook_token is not None:
                update_fields.append("webhook_token = %s")
                update_values.append(request.webhook_token)
            
            if request.webhook_agent_name is not None:
                update_fields.append("webhook_agent_name = %s")
                update_values.append(request.webhook_agent_name)
            
            if request.is_active is not None:
                update_fields.append("is_active = %s")
                update_values.append(request.is_active)
            
            if not update_fields:
                cursor.close()
                conn.close()
                raise HTTPException(status_code=400, detail="No fields to update")
            
            # Add user_id to values
            update_values.append(user_id)
            
            # Execute update
            update_query = f"""
                UPDATE user_webhooks
                SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING id, user_id, webhook_url, webhook_token, webhook_agent_name,
                          is_active, created_at, updated_at
            """
            
            cursor.execute(update_query, update_values)
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"?? Updated webhook for user_id: {user_id}")
            if request.webhook_url:
                logger.info(f"   ?? New URL: {request.webhook_url}")
            if request.is_active is not None:
                logger.info(f"   ? Active: {request.is_active}")
            
            return {
                "success": True,
                "message": "Webhook updated successfully",
                "webhook": {
                    "id": result['id'],
                    "user_id": result['user_id'],
                    "webhook_url": result['webhook_url'],
                    "webhook_token": result['webhook_token'],
                    "webhook_agent_name": result['webhook_agent_name'],
                    "is_active": result['is_active'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating user webhook: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to update webhook: {str(e)}")

    @app.patch("/user-webhooks/toggle", tags=["User Webhooks"])
    async def toggle_user_webhook(current_user = Depends(get_current_user)):
        """
        Toggle webhook active status (enable/disable).
        
        Returns updated webhook with toggled active status.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if webhook exists for user
            cursor.execute("""
                SELECT id, is_active FROM user_webhooks
                WHERE user_id = %s
            """, (user_id,))
            
            result = cursor.fetchone()
            if not result:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404, 
                    detail="Webhook not found. Please create one first using POST /user-webhooks"
                )
            
            # Toggle active status
            new_status = not result['is_active']
            
            cursor.execute("""
                UPDATE user_webhooks
                SET is_active = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING id, user_id, webhook_url, webhook_token, webhook_agent_name,
                          is_active, created_at, updated_at
            """, (new_status, user_id))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"?? Toggled webhook for user_id {user_id} to {'ACTIVE' if new_status else 'INACTIVE'}")
            
            return {
                "success": True,
                "message": f"Webhook {'activated' if new_status else 'deactivated'} successfully",
                "webhook": {
                    "id": result['id'],
                    "user_id": result['user_id'],
                    "webhook_url": result['webhook_url'],
                    "webhook_token": result['webhook_token'],
                    "webhook_agent_name": result['webhook_agent_name'],
                    "is_active": result['is_active'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error toggling user webhook: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to toggle webhook: {str(e)}")

    @app.delete("/user-webhooks", tags=["User Webhooks"])
    async def delete_user_webhook(current_user = Depends(get_current_user)):
        """
        ??? Delete webhook configuration for the user
        
        **Returns:**
        - Success message
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Check if webhook exists for user
            cursor.execute("""
                SELECT id FROM user_webhooks
                WHERE user_id = %s
            """, (user_id,))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404, 
                    detail="Webhook not found for this user"
                )
            
            # Delete webhook
            cursor.execute("""
                DELETE FROM user_webhooks
                WHERE user_id = %s
            """, (user_id,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"??? Deleted webhook for user_id: {user_id}")
            
            return {
                "success": True,
                "message": "Webhook deleted successfully"
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting user webhook: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to delete webhook: {str(e)}")