#!/usr/bin/env python3

"""
Document Logics CRUD endpoints for Document Processing Pipeline API.

Provides:
- Create, read, update, delete document logics
- User-based filtering
- Business logic JSON storage and management
"""

import json
import logging
from fastapi import HTTPException, Depends
from pydantic import BaseModel
from typing import Any
from psycopg2.extras import RealDictCursor
from src.api.models.api_server_models import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==================== PYDANTIC MODELS ====================

class DocumentLogicCreate(BaseModel):
    business_logic_name: str
    business_logic_json: Any  # Accepts dict, list, or JSON string


class DocumentLogicResponse(BaseModel):
    logic_type_id: int
    logic_name: str
    logic_json: Any
    user_id: int
    created_at: str
    updated_at: str


# ==================== DOCUMENT LOGICS ENDPOINTS ====================

def create_document_logic_routes(app, get_current_user):
    """Create document logics routes"""

    @app.post("/document-logics", tags=["Document Logics"], response_model=DocumentLogicResponse)
    async def create_document_logic(
        request: DocumentLogicCreate,
        current_user = Depends(get_current_user)
    ):
        """
        Create a new document logic.
        
        Accepts business_logic_name and business_logic_json.
        Stored as logic_name and logic_json in database.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            # Validate and normalize logic_json
            logic_json_value = request.business_logic_json
            if isinstance(logic_json_value, str):
                try:
                    logic_json_value = json.loads(logic_json_value)
                except json.JSONDecodeError:
                    raise HTTPException(
                        status_code=400,
                        detail="business_logic_json must be valid JSON"
                    )
            elif not isinstance(logic_json_value, (dict, list)):
                raise HTTPException(
                    status_code=400,
                    detail="business_logic_json must be a JSON object, array, or valid JSON string"
                )

            logic_json_str = json.dumps(logic_json_value)

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Check if logic_name already exists for this user
            cursor.execute("""
                SELECT logic_type_id FROM document_logics
                WHERE logic_name = %s AND user_id = %s
            """, (request.business_logic_name, user_id))

            existing = cursor.fetchone()
            if existing:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"Document logic '{request.business_logic_name}' already exists. Use PUT to update."
                )

            # Insert new document logic
            cursor.execute("""
                INSERT INTO document_logics (logic_name, logic_json, user_id)
                VALUES (%s, %s::jsonb, %s)
                RETURNING logic_type_id, logic_name, logic_json, user_id, created_at, updated_at
            """, (request.business_logic_name, logic_json_str, user_id))

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Created document logic '{request.business_logic_name}' for user_id: {user_id}")

            return DocumentLogicResponse(
                logic_type_id=result['logic_type_id'],
                logic_name=result['logic_name'],
                logic_json=result['logic_json'],
                user_id=result['user_id'],
                created_at=result['created_at'].isoformat(),
                updated_at=result['updated_at'].isoformat()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating document logic: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to create document logic: {str(e)}")


    @app.get("/document-logics", tags=["Document Logics"])
    async def list_document_logics(current_user = Depends(get_current_user)):
        """
        List all document logics for the authenticated user.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
                SELECT logic_type_id, logic_name, logic_json, user_id, created_at, updated_at
                FROM document_logics
                WHERE user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            logics = [
                {
                    "logic_type_id": row['logic_type_id'],
                    "logic_name": row['logic_name'],
                    "logic_json": row['logic_json'],
                    "user_id": row['user_id'],
                    "created_at": row['created_at'].isoformat(),
                    "updated_at": row['updated_at'].isoformat()
                }
                for row in results
            ]

            return {
                "success": True,
                "logics": logics,
                "total": len(logics)
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error listing document logics: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to list document logics: {str(e)}")


    @app.get("/document-logics/{logic_type_id}", tags=["Document Logics"])
    async def get_document_logic(
        logic_type_id: int,
        current_user = Depends(get_current_user)
    ):
        """
        Get a specific document logic by ID.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
                SELECT logic_type_id, logic_name, logic_json, user_id, created_at, updated_at
                FROM document_logics
                WHERE logic_type_id = %s AND user_id = %s
            """, (logic_type_id, user_id))

            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if not result:
                raise HTTPException(status_code=404, detail="Document logic not found")

            return {
                "success": True,
                "logic": {
                    "logic_type_id": result['logic_type_id'],
                    "logic_name": result['logic_name'],
                    "logic_json": result['logic_json'],
                    "user_id": result['user_id'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting document logic: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to get document logic: {str(e)}")


    @app.put("/document-logics/{logic_type_id}", tags=["Document Logics"])
    async def update_document_logic(
        logic_type_id: int,
        request: DocumentLogicCreate,
        current_user = Depends(get_current_user)
    ):
        """
        Update an existing document logic.
        
        Accepts business_logic_name and business_logic_json.
        Updates logic_name and logic_json in the database.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            # Validate and normalize logic_json
            logic_json_value = request.business_logic_json
            if isinstance(logic_json_value, str):
                try:
                    logic_json_value = json.loads(logic_json_value)
                except json.JSONDecodeError:
                    raise HTTPException(
                        status_code=400,
                        detail="business_logic_json must be valid JSON"
                    )
            elif not isinstance(logic_json_value, (dict, list)):
                raise HTTPException(
                    status_code=400,
                    detail="business_logic_json must be a JSON object, array, or valid JSON string"
                )

            logic_json_str = json.dumps(logic_json_value)

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Check if logic exists and belongs to user
            cursor.execute("""
                SELECT logic_type_id FROM document_logics
                WHERE logic_type_id = %s AND user_id = %s
            """, (logic_type_id, user_id))

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(status_code=404, detail="Document logic not found")

            # Check if new logic_name conflicts with another record for same user
            cursor.execute("""
                SELECT logic_type_id FROM document_logics
                WHERE logic_name = %s AND user_id = %s AND logic_type_id != %s
            """, (request.business_logic_name, user_id, logic_type_id))

            if cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"Another document logic with name '{request.business_logic_name}' already exists."
                )

            # Update the document logic
            cursor.execute("""
                UPDATE document_logics
                SET logic_name = %s, logic_json = %s::jsonb, updated_at = CURRENT_TIMESTAMP
                WHERE logic_type_id = %s AND user_id = %s
                RETURNING logic_type_id, logic_name, logic_json, user_id, created_at, updated_at
            """, (request.business_logic_name, logic_json_str, logic_type_id, user_id))

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Updated document logic ID {logic_type_id} for user_id: {user_id}")

            return {
                "success": True,
                "logic": {
                    "logic_type_id": result['logic_type_id'],
                    "logic_name": result['logic_name'],
                    "logic_json": result['logic_json'],
                    "user_id": result['user_id'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating document logic: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to update document logic: {str(e)}")


    @app.delete("/document-logics/{logic_type_id}", tags=["Document Logics"])
    async def delete_document_logic(
        logic_type_id: int,
        current_user = Depends(get_current_user)
    ):
        """
        Delete a document logic.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            conn = get_db_connection()
            cursor = conn.cursor()

            # Check if logic exists and belongs to user
            cursor.execute("""
                SELECT logic_type_id FROM document_logics
                WHERE logic_type_id = %s AND user_id = %s
            """, (logic_type_id, user_id))

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(status_code=404, detail="Document logic not found")

            # Delete the document logic
            cursor.execute("""
                DELETE FROM document_logics
                WHERE logic_type_id = %s AND user_id = %s
            """, (logic_type_id, user_id))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Deleted document logic ID {logic_type_id} for user_id: {user_id}")

            return {
                "success": True,
                "message": f"Document logic {logic_type_id} deleted successfully"
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting document logic: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to delete document logic: {str(e)}")