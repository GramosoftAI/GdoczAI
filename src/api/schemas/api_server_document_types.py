#!/usr/bin/env python3

"""
Document Types CRUD endpoints for Document Processing Pipeline API.

Provides:
- Create, read, update, delete document types
- User-based filtering
- Consolidated conditional_keys and langchain_keys stored in database
"""

import logging
from fastapi import HTTPException, Depends, Query
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field
from typing import Optional
from src.api.models.api_server_models import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== PYDANTIC MODELS ====================

class DocumentTypeCreate(BaseModel):
    document_type: str = Field(..., description="Document type name")
    conditional_keys: Optional[str] = Field(None, description="Conditional keys in plain text format")
    langchain_keys: Optional[str] = Field(None, description="LangChain keys in plain text format")

class DocumentTypeResponse(BaseModel):
    doc_type_id: int
    document_type: str
    conditional_keys: Optional[str]
    langchain_keys: Optional[str]
    user_id: int
    created_at: str
    updated_at: str

# ==================== DOCUMENT TYPES ENDPOINTS ====================

def create_document_type_routes(app, get_current_user):
    """Create document types routes with consolidated keys"""

    @app.post("/document-types", tags=["Document Types"], response_model=DocumentTypeResponse)
    async def create_document_type(
        request: DocumentTypeCreate,
        current_user = Depends(get_current_user)
    ):
        """
        Create a new document type with optional conditional and langchain keys.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if document type already exists for this user
            cursor.execute("""
                SELECT doc_type_id FROM document_types
                WHERE document_type = %s AND user_id = %s
            """, (request.document_type, user_id))
            
            existing = cursor.fetchone()
            if existing:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"Document type '{request.document_type}' already exists for this user"
                )
            
            # Insert new document type with conditional and langchain keys
            cursor.execute("""
                INSERT INTO document_types (document_type, conditional_keys, langchain_keys, user_id)
                VALUES (%s, %s, %s, %s)
                RETURNING doc_type_id, document_type, conditional_keys, langchain_keys, user_id, created_at, updated_at
            """, (request.document_type, request.conditional_keys, request.langchain_keys, user_id))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Created document type: {request.document_type} for user_id: {user_id}")
            if request.conditional_keys:
                logger.info(f"Added conditional keys")
            if request.langchain_keys:
                logger.info(f"Added langchain keys")
            
            return DocumentTypeResponse(
                doc_type_id=result['doc_type_id'],
                document_type=result['document_type'],
                conditional_keys=result['conditional_keys'],
                langchain_keys=result['langchain_keys'],
                user_id=result['user_id'],
                created_at=result['created_at'].isoformat(),
                updated_at=result['updated_at'].isoformat()
            )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating document type: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to create document type: {str(e)}")

    @app.get("/document-types", tags=["Document Types"])
    async def list_document_types(current_user = Depends(get_current_user)):
        """
        List all document types with their keys for the authenticated user.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT doc_type_id, document_type, conditional_keys, langchain_keys, 
                       user_id, created_at, updated_at
                FROM document_types
                WHERE user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            document_types = [
                {
                    "doc_type_id": row['doc_type_id'],
                    "document_type": row['document_type'],
                    "conditional_keys": row['conditional_keys'],
                    "langchain_keys": row['langchain_keys'],
                    "user_id": row['user_id'],
                    "created_at": row['created_at'].isoformat(),
                    "updated_at": row['updated_at'].isoformat()
                }
                for row in results
            ]
            
            return {
                "success": True,
                "document_types": document_types,
                "total": len(document_types)
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error listing document types: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to list document types: {str(e)}")

    @app.get("/document-types/{doc_type_id}", tags=["Document Types"])
    async def get_document_type(
        doc_type_id: int,
        current_user = Depends(get_current_user)
    ):
        """
        Get a specific document type with its keys by ID.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT doc_type_id, document_type, conditional_keys, langchain_keys,
                       user_id, created_at, updated_at
                FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (doc_type_id, user_id))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result:
                raise HTTPException(status_code=404, detail="Document type not found")
            
            return {
                "success": True,
                "document_type": {
                    "doc_type_id": result['doc_type_id'],
                    "document_type": result['document_type'],
                    "conditional_keys": result['conditional_keys'],
                    "langchain_keys": result['langchain_keys'],
                    "user_id": result['user_id'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting document type: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to get document type: {str(e)}")

    @app.put("/document-types/{doc_type_id}", tags=["Document Types"])
    async def update_document_type(
        doc_type_id: int,
        request: DocumentTypeCreate,
        current_user = Depends(get_current_user)
    ):
        """
        Update a document type including its conditional and langchain keys.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if document type exists and belongs to user
            cursor.execute("""
                SELECT doc_type_id FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (doc_type_id, user_id))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(status_code=404, detail="Document type not found")
            
            # Update document type with all fields
            cursor.execute("""
                UPDATE document_types
                SET document_type = %s, 
                    conditional_keys = %s, 
                    langchain_keys = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE doc_type_id = %s AND user_id = %s
                RETURNING doc_type_id, document_type, conditional_keys, langchain_keys, 
                          user_id, created_at, updated_at
            """, (request.document_type, request.conditional_keys, 
                  request.langchain_keys, doc_type_id, user_id))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Updated document type ID {doc_type_id} for user_id: {user_id}")
            
            return {
                "success": True,
                "document_type": {
                    "doc_type_id": result['doc_type_id'],
                    "document_type": result['document_type'],
                    "conditional_keys": result['conditional_keys'],
                    "langchain_keys": result['langchain_keys'],
                    "user_id": result['user_id'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating document type: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to update document type: {str(e)}")

    @app.delete("/document-types/{doc_type_id}", tags=["Document Types"])
    async def delete_document_type(
        doc_type_id: int,
        current_user = Depends(get_current_user)
    ):
        """
        Delete a document type. This will cascade delete associated schemas.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Check if document type exists and belongs to user
            cursor.execute("""
                SELECT doc_type_id FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (doc_type_id, user_id))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(status_code=404, detail="Document type not found")
            
            # Delete document type (will cascade delete schemas)
            cursor.execute("""
                DELETE FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (doc_type_id, user_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Deleted document type ID {doc_type_id} for user_id: {user_id}")
            
            return {
                "success": True,
                "message": f"Document type {doc_type_id} deleted successfully"
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting document type: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to delete document type: {str(e)}")

    # ==================== DEDICATED KEY MANAGEMENT ENDPOINTS ====================
    
    @app.put("/document-types/{doc_type_id}/conditional-keys", tags=["Document Types"])
    async def update_conditional_keys(
        doc_type_id: int,
        conditional_keys: str = Query(..., description="Conditional keys in plain text format"),
        current_user = Depends(get_current_user)
    ):
        """
        Update only conditional keys for a document type.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if document type exists and belongs to user
            cursor.execute("""
                SELECT doc_type_id FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (doc_type_id, user_id))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(status_code=404, detail="Document type not found")
            
            # Update only conditional keys
            cursor.execute("""
                UPDATE document_types
                SET conditional_keys = %s, updated_at = CURRENT_TIMESTAMP
                WHERE doc_type_id = %s AND user_id = %s
                RETURNING doc_type_id, document_type, conditional_keys, langchain_keys,
                          user_id, created_at, updated_at
            """, (conditional_keys, doc_type_id, user_id))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Updated conditional keys for doc_type_id: {doc_type_id}, user_id: {user_id}")
            
            return {
                "success": True,
                "message": "Conditional keys updated successfully",
                "document_type": {
                    "doc_type_id": result['doc_type_id'],
                    "document_type": result['document_type'],
                    "conditional_keys": result['conditional_keys'],
                    "langchain_keys": result['langchain_keys'],
                    "user_id": result['user_id'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating conditional keys: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to update conditional keys: {str(e)}")

    @app.put("/document-types/{doc_type_id}/langchain-keys", tags=["Document Types"])
    async def update_langchain_keys(
        doc_type_id: int,
        langchain_keys: str = Query(..., description="LangChain keys in plain text format"),
        current_user = Depends(get_current_user)
    ):
        """
        Update only langchain keys for a document type.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if document type exists and belongs to user
            cursor.execute("""
                SELECT doc_type_id FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (doc_type_id, user_id))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(status_code=404, detail="Document type not found")
            
            # Update only langchain keys
            cursor.execute("""
                UPDATE document_types
                SET langchain_keys = %s, updated_at = CURRENT_TIMESTAMP
                WHERE doc_type_id = %s AND user_id = %s
                RETURNING doc_type_id, document_type, conditional_keys, langchain_keys,
                          user_id, created_at, updated_at
            """, (langchain_keys, doc_type_id, user_id))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Updated langchain keys for doc_type_id: {doc_type_id}, user_id: {user_id}")
            
            return {
                "success": True,
                "message": "LangChain keys updated successfully",
                "document_type": {
                    "doc_type_id": result['doc_type_id'],
                    "document_type": result['document_type'],
                    "conditional_keys": result['conditional_keys'],
                    "langchain_keys": result['langchain_keys'],
                    "user_id": result['user_id'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating langchain keys: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to update langchain keys: {str(e)}")

    @app.get("/document-types/{doc_type_id}/conditional-keys", tags=["Document Types"])
    async def get_conditional_keys(
        doc_type_id: int,
        current_user = Depends(get_current_user)
    ):
        """
        Get only conditional keys for a document type.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT doc_type_id, document_type, conditional_keys
                FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (doc_type_id, user_id))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result:
                raise HTTPException(status_code=404, detail="Document type not found")
            
            return {
                "success": True,
                "doc_type_id": result['doc_type_id'],
                "document_type": result['document_type'],
                "conditional_keys": result['conditional_keys']
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting conditional keys: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to get conditional keys: {str(e)}")

    @app.get("/document-types/{doc_type_id}/langchain-keys", tags=["Document Types"])
    async def get_langchain_keys(
        doc_type_id: int,
        current_user = Depends(get_current_user)
    ):
        """
        Get only langchain keys for a document type.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT doc_type_id, document_type, langchain_keys
                FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (doc_type_id, user_id))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result:
                raise HTTPException(status_code=404, detail="Document type not found")
            
            return {
                "success": True,
                "doc_type_id": result['doc_type_id'],
                "document_type": result['document_type'],
                "langchain_keys": result['langchain_keys']
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting langchain keys: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to get langchain keys: {str(e)}")