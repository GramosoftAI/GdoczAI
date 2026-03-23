# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
User SMTP Connector CRUD endpoints for Document Processing Pipeline API.
Provides:
- Create, read, update, delete user SMTP configurations
- One SMTP configuration per user
- SMTP email credentials, approved senders, email method, and interval configuration
"""

import logging
from fastapi import HTTPException, Depends, Form
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from typing import Optional
from src.api.models.api_server_models import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== PYDANTIC MODELS ====================

class UserSMTPResponse(BaseModel):
    id: int
    user_id: int
    email_id: str
    app_password: str
    approved_senders: Optional[str]
    email_method: Optional[str]
    interval_minute: int
    is_active: bool
    created_at: str
    updated_at: str

# ==================== USER SMTP ENDPOINTS ====================

def create_user_smtp_routes(app, get_current_user):
    """Create user-based SMTP routes (one SMTP configuration per user)"""

    @app.post(
        "/user-smtp", tags=["User SMTP Connector"], response_model=UserSMTPResponse
    )
    async def create_user_smtp(
        email_id: str = Form(..., description="SMTP email address"),
        app_password: str = Form(..., description="SMTP app password or credentials"),
        approved_senders: Optional[str] = Form(
            None, description="Comma-separated list of approved sender email addresses"
        ),
        email_method: Optional[str] = Form(
            None, description="Email sending method (e.g. SMTP, STARTTLS, SSL)"
        ),
        interval_minute: Optional[int] = Form(
            5, description="Interval in minutes for polling (default: 5)"
        ),
        is_active: bool = Form(True, description="Enable/disable SMTP connector"),
        current_user=Depends(get_current_user),
    ):

        conn = None
        try:
            user_id = current_user.get("user_id")
            if not user_id:
                raise HTTPException(
                    status_code=401, detail="User ID not found in token"
                )

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Check if SMTP configuration already exists for this user
            cursor.execute(
                """
                SELECT id FROM smtp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            existing = cursor.fetchone()
            if existing:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"SMTP configuration already exists for user {user_id}. Use PUT to update or DELETE to remove it first.",
                )

            # Insert new SMTP configuration
            cursor.execute(
                """
                INSERT INTO smtp_connector
                (user_id, email_id, app_password, approved_senders, email_method, interval_minute, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, user_id, email_id, app_password, approved_senders,
                          email_method, interval_minute, is_active, created_at, updated_at
            """,
                (
                    user_id,
                    email_id,
                    app_password,
                    approved_senders,
                    email_method,
                    interval_minute,
                    is_active,
                ),
            )

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"[OK] Created SMTP configuration for user_id: {user_id}")
            logger.info(f"   [EMAIL] Email ID: {email_id}")
            logger.info(f"   [METHOD] Email Method: {email_method or 'Not specified'}")
            logger.info(f"   [SENDERS] Approved Senders: {approved_senders or 'Not specified'}")
            logger.info(f"   [TIMER] Interval: {interval_minute} minutes")
            logger.info(f"   [CHECK] Active: {is_active}")

            return UserSMTPResponse(
                id=result["id"],
                user_id=result["user_id"],
                email_id=result["email_id"],
                app_password=result["app_password"],
                approved_senders=result["approved_senders"],
                email_method=result["email_method"],
                interval_minute=result["interval_minute"],
                is_active=result["is_active"],
                created_at=result["created_at"].isoformat(),
                updated_at=result["updated_at"].isoformat(),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating SMTP configuration: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to create SMTP configuration: {str(e)}"
            )

    @app.get("/user-smtp", tags=["User SMTP Connector"])
    async def get_user_smtp(current_user=Depends(get_current_user)):

        conn = None
        try:
            user_id = current_user.get("user_id")
            if not user_id:
                raise HTTPException(
                    status_code=401, detail="User ID not found in token"
                )

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute(
                """
                SELECT id, user_id, email_id, app_password, approved_senders,
                       email_method, interval_minute, is_active, created_at, updated_at
                FROM smtp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if not result:
                return {
                    "success": True,
                    "smtp": None,
                    "message": "No SMTP configuration found for this user",
                }

            return {
                "success": True,
                "smtp": {
                    "id": result["id"],
                    "user_id": result["user_id"],
                    "email_id": result["email_id"],
                    "app_password": result["app_password"],
                    "approved_senders": result["approved_senders"],
                    "email_method": result["email_method"],
                    "interval_minute": result["interval_minute"],
                    "is_active": result["is_active"],
                    "created_at": result["created_at"].isoformat(),
                    "updated_at": result["updated_at"].isoformat(),
                },
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting SMTP configuration: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to get SMTP configuration: {str(e)}"
            )

    @app.put("/user-smtp", tags=["User SMTP Connector"])
    async def update_user_smtp(
        email_id: Optional[str] = Form(None, description="SMTP email address"),
        app_password: Optional[str] = Form(None, description="SMTP app password or credentials"),
        approved_senders: Optional[str] = Form(
            None, description="Comma-separated list of approved sender email addresses"
        ),
        email_method: Optional[str] = Form(
            None, description="Email sending method (e.g. SMTP, STARTTLS, SSL)"
        ),
        interval_minute: Optional[int] = Form(
            None, description="Interval in minutes for polling"
        ),
        is_active: Optional[bool] = Form(None, description="Enable/disable SMTP connector"),
        current_user=Depends(get_current_user),
    ):

        conn = None
        try:
            user_id = current_user.get("user_id")
            if not user_id:
                raise HTTPException(
                    status_code=401, detail="User ID not found in token"
                )

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Check if SMTP configuration exists for user
            cursor.execute(
                """
                SELECT id FROM smtp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404,
                    detail="SMTP configuration not found. Please create one first using POST /user-smtp",
                )

            # Build dynamic update query
            update_fields = []
            update_values = []

            if email_id is not None:
                update_fields.append("email_id = %s")
                update_values.append(email_id)

            if app_password is not None:
                update_fields.append("app_password = %s")
                update_values.append(app_password)

            if approved_senders is not None:
                update_fields.append("approved_senders = %s")
                update_values.append(approved_senders)

            if email_method is not None:
                update_fields.append("email_method = %s")
                update_values.append(email_method)

            if interval_minute is not None:
                update_fields.append("interval_minute = %s")
                update_values.append(interval_minute)

            if is_active is not None:
                update_fields.append("is_active = %s")
                update_values.append(is_active)

            if not update_fields:
                cursor.close()
                conn.close()
                raise HTTPException(status_code=400, detail="No fields to update")

            # Add user_id to values
            update_values.append(user_id)

            # Execute update
            update_query = f"""
                UPDATE smtp_connector
                SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING id, user_id, email_id, app_password, approved_senders,
                          email_method, interval_minute, is_active, created_at, updated_at
            """

            cursor.execute(update_query, update_values)

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"[UPDATE] Updated SMTP configuration for user_id: {user_id}")
            if email_id:
                logger.info(f"   [EMAIL] New Email ID: {email_id}")
            if email_method:
                logger.info(f"   [METHOD] New Email Method: {email_method}")
            if interval_minute:
                logger.info(f"   [TIMER] New Interval: {interval_minute} minutes")
            if is_active is not None:
                logger.info(f"   [CHECK] Updated Active Status: {is_active}")

            return {
                "success": True,
                "message": "SMTP configuration updated successfully",
                "smtp": {
                    "id": result["id"],
                    "user_id": result["user_id"],
                    "email_id": result["email_id"],
                    "app_password": result["app_password"],
                    "approved_senders": result["approved_senders"],
                    "email_method": result["email_method"],
                    "interval_minute": result["interval_minute"],
                    "is_active": result["is_active"],
                    "created_at": result["created_at"].isoformat(),
                    "updated_at": result["updated_at"].isoformat(),
                },
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating SMTP configuration: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to update SMTP configuration: {str(e)}"
            )

    @app.patch("/user-smtp/toggle", tags=["User SMTP Connector"])
    async def toggle_user_smtp(current_user=Depends(get_current_user)):

        conn = None
        try:
            user_id = current_user.get("user_id")
            if not user_id:
                raise HTTPException(
                    status_code=401, detail="User ID not found in token"
                )

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Check if SMTP configuration exists for user
            cursor.execute(
                """
                SELECT id, is_active FROM smtp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            result = cursor.fetchone()
            if not result:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404,
                    detail="SMTP configuration not found. Please create one first using POST /user-smtp",
                )

            # Toggle active status
            new_status = not result["is_active"]

            cursor.execute(
                """
                UPDATE smtp_connector
                SET is_active = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING id, user_id, email_id, app_password, approved_senders,
                          email_method, interval_minute, is_active, created_at, updated_at
            """,
                (new_status, user_id),
            )

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(
                f"[TOGGLE] Toggled SMTP connector for user_id {user_id} to {'ACTIVE' if new_status else 'INACTIVE'}"
            )

            return {
                "success": True,
                "message": f"SMTP connector {'activated' if new_status else 'deactivated'} successfully",
                "smtp": {
                    "id": result["id"],
                    "user_id": result["user_id"],
                    "email_id": result["email_id"],
                    "app_password": result["app_password"],
                    "approved_senders": result["approved_senders"],
                    "email_method": result["email_method"],
                    "interval_minute": result["interval_minute"],
                    "is_active": result["is_active"],
                    "created_at": result["created_at"].isoformat(),
                    "updated_at": result["updated_at"].isoformat(),
                },
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error toggling SMTP connector: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to toggle SMTP connector: {str(e)}"
            )

    @app.delete("/user-smtp", tags=["User SMTP Connector"])
    async def delete_user_smtp(current_user=Depends(get_current_user)):

        conn = None
        try:
            user_id = current_user.get("user_id")
            if not user_id:
                raise HTTPException(
                    status_code=401, detail="User ID not found in token"
                )

            conn = get_db_connection()
            cursor = conn.cursor()

            # Check if SMTP configuration exists for user
            cursor.execute(
                """
                SELECT id FROM smtp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404, detail="SMTP configuration not found for this user"
                )

            # Delete SMTP configuration
            cursor.execute(
                """
                DELETE FROM smtp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"[DELETE] Deleted SMTP configuration for user_id: {user_id}")

            return {
                "success": True,
                "message": "SMTP configuration deleted successfully",
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting SMTP configuration: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to delete SMTP configuration: {str(e)}"
            )