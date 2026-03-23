# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
User SFTP Connector CRUD endpoints for Document Processing Pipeline API.

Provides:
- Create, read, update, delete user SFTP configurations
- One SFTP configuration per user
- SFTP host, port, credentials, and folder monitoring configuration
"""

import os
import logging
from datetime import datetime
from fastapi import HTTPException, Depends, UploadFile, File, Form, Request
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from typing import Optional, Annotated
from src.api.models.api_server_models import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Directory where PEM files are stored
PEM_STORAGE_DIR = "/home/Mineru_project/data/pem_files"


async def save_pem_file(pem_file: UploadFile) -> str:

    os.makedirs(PEM_STORAGE_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"ram_{timestamp}.pem"
    file_path = os.path.join(PEM_STORAGE_DIR, filename)

    contents = await pem_file.read()
    with open(file_path, "wb") as f:
        f.write(contents)

    # Secure permissions: owner read-only
    os.chmod(file_path, 0o600)

    logger.info(f"[KEY] PEM file saved: {file_path}")
    return file_path

# ==================== PYDANTIC MODELS ====================

class UserSFTPResponse(BaseModel):
    id: int
    user_id: int
    host_name: str
    port: int
    username: str
    password: Optional[str]
    private_key_path: Optional[str]
    monitor_folders: Optional[str]
    moved_folder: Optional[str]
    failed_folder: Optional[str]
    interval_minute: int
    is_active: bool
    created_at: str
    updated_at: str

# ==================== USER SFTP ENDPOINTS ====================

def create_user_sftp_routes(app, get_current_user):
    """Create user-based SFTP routes (one SFTP configuration per user)"""

    @app.post(
        "/user-sftp", tags=["User SFTP Connector"], response_model=UserSFTPResponse
    )
    async def create_user_sftp(
        host_name: str = Form(..., description="SFTP server hostname or IP address"),
        port: int = Form(22, description="SFTP server port (default: 22)"),
        username: str = Form(..., description="SFTP username"),
        password: Optional[str] = Form(None, description="SFTP password"),
        monitor_folders: Optional[str] = Form(
            None, description="Comma-separated list of folders to monitor"
        ),
        moved_folder: Optional[str] = Form(
            None, description="Folder path for successfully processed files"
        ),
        failed_folder: Optional[str] = Form(
            None, description="Folder path for failed files"
        ),
        interval_minute: Optional[int] = Form(
            5, description="Interval in minutes for folder monitoring"
        ),
        is_active: bool = Form(True, description="Enable/disable SFTP connector"),
        pem_file: Optional[UploadFile] = File(
            None, description="PEM private key file (.pem). Stored on server automatically."
        ),
        current_user=Depends(get_current_user),
    ):

        conn = None
        try:
            user_id = current_user.get("user_id")
            if not user_id:
                raise HTTPException(
                    status_code=401, detail="User ID not found in token"
                )

            # Validate that either password or pem_file is provided
            if not password and not pem_file:
                raise HTTPException(
                    status_code=400,
                    detail="Either password or pem_file must be provided for authentication",
                )

            # Save PEM file and get its path
            private_key_path = None
            if pem_file:
                private_key_path = await save_pem_file(pem_file)

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Check if SFTP configuration already exists for this user
            cursor.execute(
                """
                SELECT id FROM sftp_connector
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
                    detail=f"SFTP configuration already exists for user {user_id}. Use PUT to update or DELETE to remove it first.",
                )

            # Insert new SFTP configuration
            cursor.execute(
                """
                INSERT INTO sftp_connector 
                (user_id, host_name, port, username, password, private_key_path, 
                 monitor_folders, moved_folder, failed_folder, interval_minute, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, user_id, host_name, port, username, password, private_key_path,
                          monitor_folders, moved_folder, failed_folder, interval_minute,
                          is_active, created_at, updated_at
            """,
                (
                    user_id,
                    host_name,
                    port,
                    username,
                    password,
                    private_key_path,
                    monitor_folders,
                    moved_folder,
                    failed_folder,
                    interval_minute,
                    is_active,
                ),
            )

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"[OK] Created SFTP configuration for user_id: {user_id}")
            logger.info(f"   [HOST] Host: {host_name}:{port}")
            logger.info(f"   [USER] Username: {username}")
            logger.info(f"   [FOLDER] Monitor Folders: {monitor_folders or 'Not specified'}")
            logger.info(f"   [TIMER] Interval: {interval_minute} minutes")
            logger.info(f"   [CHECK] Active: {is_active}")
            if private_key_path:
                logger.info(f"   [KEY] PEM Key Path: {private_key_path}")

            return UserSFTPResponse(
                id=result["id"],
                user_id=result["user_id"],
                host_name=result["host_name"],
                port=result["port"],
                username=result["username"],
                password=result["password"],
                private_key_path=result["private_key_path"],
                monitor_folders=result["monitor_folders"],
                moved_folder=result["moved_folder"],
                failed_folder=result["failed_folder"],
                interval_minute=result["interval_minute"],
                is_active=result["is_active"],
                created_at=result["created_at"].isoformat(),
                updated_at=result["updated_at"].isoformat(),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating SFTP configuration: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to create SFTP configuration: {str(e)}"
            )

    @app.get("/user-sftp", tags=["User SFTP Connector"])
    async def get_user_sftp(current_user=Depends(get_current_user)):

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
                SELECT id, user_id, host_name, port, username, password, private_key_path,
                       monitor_folders, moved_folder, failed_folder, interval_minute,
                       is_active, created_at, updated_at
                FROM sftp_connector
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
                    "sftp": None,
                    "message": "No SFTP configuration found for this user",
                }

            return {
                "success": True,
                "sftp": {
                    "id": result["id"],
                    "user_id": result["user_id"],
                    "host_name": result["host_name"],
                    "port": result["port"],
                    "username": result["username"],
                    "password": result["password"],
                    "private_key_path": result["private_key_path"],
                    "monitor_folders": result["monitor_folders"],
                    "moved_folder": result["moved_folder"],
                    "failed_folder": result["failed_folder"],
                    "interval_minute": result["interval_minute"],
                    "is_active": result["is_active"],
                    "created_at": result["created_at"].isoformat(),
                    "updated_at": result["updated_at"].isoformat(),
                },
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting SFTP configuration: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to get SFTP configuration: {str(e)}"
            )

    @app.put("/user-sftp", tags=["User SFTP Connector"])
    async def update_user_sftp(
        host_name: Optional[str] = Form(None, description="SFTP server hostname or IP address"),
        port: Optional[int] = Form(None, description="SFTP server port"),
        username: Optional[str] = Form(None, description="SFTP username"),
        password: Optional[str] = Form(None, description="SFTP password"),
        monitor_folders: Optional[str] = Form(
            None, description="Comma-separated list of folders to monitor"
        ),
        moved_folder: Optional[str] = Form(
            None, description="Folder path for successfully processed files"
        ),
        failed_folder: Optional[str] = Form(
            None, description="Folder path for failed files"
        ),
        interval_minute: Optional[int] = Form(
            None, description="Interval in minutes for folder monitoring"
        ),
        is_active: Optional[bool] = Form(None, description="Enable/disable SFTP connector"),
        pem_file: Optional[UploadFile] = File(
            None, description="New PEM private key file (.pem). Replaces existing key path."
        ),
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

            # Check if SFTP configuration exists for user
            cursor.execute(
                """
                SELECT id FROM sftp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404,
                    detail="SFTP configuration not found. Please create one first using POST /user-sftp",
                )

            # Save new PEM file if provided
            private_key_path = None
            if pem_file:
                private_key_path = await save_pem_file(pem_file)

            # Build dynamic update query
            update_fields = []
            update_values = []

            if host_name is not None:
                update_fields.append("host_name = %s")
                update_values.append(host_name)

            if port is not None:
                update_fields.append("port = %s")
                update_values.append(port)

            if username is not None:
                update_fields.append("username = %s")
                update_values.append(username)

            if password is not None:
                update_fields.append("password = %s")
                update_values.append(password)

            if private_key_path is not None:
                update_fields.append("private_key_path = %s")
                update_values.append(private_key_path)

            if monitor_folders is not None:
                update_fields.append("monitor_folders = %s")
                update_values.append(monitor_folders)

            if moved_folder is not None:
                update_fields.append("moved_folder = %s")
                update_values.append(moved_folder)

            if failed_folder is not None:
                update_fields.append("failed_folder = %s")
                update_values.append(failed_folder)

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
                UPDATE sftp_connector
                SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING id, user_id, host_name, port, username, password, private_key_path,
                          monitor_folders, moved_folder, failed_folder, interval_minute,
                          is_active, created_at, updated_at
            """

            cursor.execute(update_query, update_values)

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"[UPDATE] Updated SFTP configuration for user_id: {user_id}")
            if host_name:
                logger.info(f"   [HOST] New Host: {host_name}:{port or 'default'}")
            if interval_minute:
                logger.info(f"   [TIMER] New Interval: {interval_minute} minutes")
            if is_active is not None:
                logger.info(f"   [CHECK] Updated Active Status: {is_active}")
            if private_key_path:
                logger.info(f"   [KEY] New PEM Key Path: {private_key_path}")

            return {
                "success": True,
                "message": "SFTP configuration updated successfully",
                "sftp": {
                    "id": result["id"],
                    "user_id": result["user_id"],
                    "host_name": result["host_name"],
                    "port": result["port"],
                    "username": result["username"],
                    "password": result["password"],
                    "private_key_path": result["private_key_path"],
                    "monitor_folders": result["monitor_folders"],
                    "moved_folder": result["moved_folder"],
                    "failed_folder": result["failed_folder"],
                    "interval_minute": result["interval_minute"],
                    "is_active": result["is_active"],
                    "created_at": result["created_at"].isoformat(),
                    "updated_at": result["updated_at"].isoformat(),
                },
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating SFTP configuration: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to update SFTP configuration: {str(e)}"
            )

    @app.patch("/user-sftp/toggle", tags=["User SFTP Connector"])
    async def toggle_user_sftp(current_user=Depends(get_current_user)):
        """
        Toggle SFTP connector active status (enable/disable).

        Returns updated configuration with toggled active status.
        """
        conn = None
        try:
            user_id = current_user.get("user_id")
            if not user_id:
                raise HTTPException(
                    status_code=401, detail="User ID not found in token"
                )

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Check if SFTP configuration exists for user
            cursor.execute(
                """
                SELECT id, is_active FROM sftp_connector
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
                    detail="SFTP configuration not found. Please create one first using POST /user-sftp",
                )

            # Toggle active status
            new_status = not result["is_active"]

            cursor.execute(
                """
                UPDATE sftp_connector
                SET is_active = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING id, user_id, host_name, port, username, password, private_key_path,
                          monitor_folders, moved_folder, failed_folder, interval_minute,
                          is_active, created_at, updated_at
            """,
                (new_status, user_id),
            )

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(
                f"[TOGGLE] Toggled SFTP connector for user_id {user_id} to {'ACTIVE' if new_status else 'INACTIVE'}"
            )

            return {
                "success": True,
                "message": f"SFTP connector {'activated' if new_status else 'deactivated'} successfully",
                "sftp": {
                    "id": result["id"],
                    "user_id": result["user_id"],
                    "host_name": result["host_name"],
                    "port": result["port"],
                    "username": result["username"],
                    "password": result["password"],
                    "private_key_path": result["private_key_path"],
                    "monitor_folders": result["monitor_folders"],
                    "moved_folder": result["moved_folder"],
                    "failed_folder": result["failed_folder"],
                    "interval_minute": result["interval_minute"],
                    "is_active": result["is_active"],
                    "created_at": result["created_at"].isoformat(),
                    "updated_at": result["updated_at"].isoformat(),
                },
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error toggling SFTP connector: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to toggle SFTP connector: {str(e)}"
            )

    @app.delete("/user-sftp", tags=["User SFTP Connector"])
    async def delete_user_sftp(current_user=Depends(get_current_user)):

        conn = None
        try:
            user_id = current_user.get("user_id")
            if not user_id:
                raise HTTPException(
                    status_code=401, detail="User ID not found in token"
                )

            conn = get_db_connection()
            cursor = conn.cursor()

            # Check if SFTP configuration exists for user
            cursor.execute(
                """
                SELECT id FROM sftp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404, detail="SFTP configuration not found for this user"
                )

            # Delete SFTP configuration
            cursor.execute(
                """
                DELETE FROM sftp_connector
                WHERE user_id = %s
            """,
                (user_id,),
            )

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"[DELETE] Deleted SFTP configuration for user_id: {user_id}")

            return {
                "success": True,
                "message": "SFTP configuration deleted successfully",
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting SFTP configuration: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(
                status_code=500, detail=f"Failed to delete SFTP configuration: {str(e)}"
            )