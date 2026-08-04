#!/usr/bin/env python3

"""
Document Schemas CRUD endpoints for Document Processing Pipeline API.

Provides:
- Create, read, update, delete document schemas
- User-based filtering
- Support for prompt_field (text/json) validation
- Support for logic_type_id referencing document_logics table
- Returns logic_name from document_logics based on logic_type_id
"""

import json
import logging
from typing import Optional
from fastapi import HTTPException, Depends
from psycopg2.extras import RealDictCursor
from src.api.models.api_server_models import DocumentSchemaCreate, DocumentSchemaResponse, get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== DOCUMENT SCHEMAS ENDPOINTS ====================

def create_document_schema_routes(app, get_current_user):
    """Create document schemas routes"""

    @app.post("/document-schemas", tags=["Document Schemas"], response_model=DocumentSchemaResponse)
    async def create_document_schema(
        request: DocumentSchemaCreate,
        current_user = Depends(get_current_user)
    ):
        """
        Create a new document schema for a document type.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Verify doc_type_id belongs to user
            cursor.execute("""
                SELECT doc_type_id FROM document_types
                WHERE doc_type_id = %s AND user_id = %s
            """, (request.doc_type_id, user_id))

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404,
                    detail=f"Document type {request.doc_type_id} not found or doesn't belong to you"
                )

            # Check if schema already exists for this doc_type
            cursor.execute("""
                SELECT id FROM document_schemas
                WHERE doc_type_id = %s
            """, (request.doc_type_id,))

            existing = cursor.fetchone()
            if existing:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"Schema already exists for document type {request.doc_type_id}. Use PUT to update."
                )

            # Validate logic_type_id if provided and fetch logic_name
            logic_type_id = getattr(request, 'logic_type_id', None)
            logic_name = None
            if logic_type_id is not None:
                cursor.execute("""
                    SELECT logic_type_id, logic_name FROM document_logics
                    WHERE logic_type_id = %s AND user_id = %s
                """, (logic_type_id, user_id))
                logic_row = cursor.fetchone()
                if not logic_row:
                    cursor.close()
                    conn.close()
                    raise HTTPException(
                        status_code=404,
                        detail=f"Document logic {logic_type_id} not found or doesn't belong to you"
                    )
                logic_name = logic_row['logic_name']

            # Process extraction_schema based on prompt_field
            schema_value = request.extraction_schema

            if request.prompt_field == 'json':
                # For JSON prompt_field, store as JSON string
                if isinstance(schema_value, (dict, list)):
                    schema_value = json.dumps(schema_value)
                elif isinstance(schema_value, str):
                    # Validate it's valid JSON
                    try:
                        json.loads(schema_value)
                    except json.JSONDecodeError:
                        cursor.close()
                        conn.close()
                        raise HTTPException(
                            status_code=400,
                            detail="Invalid JSON in extraction_schema when prompt_field='json'"
                        )
            else:  # prompt_field == 'text'
                # For text prompt_field, store as plain string
                if isinstance(schema_value, (dict, list)):
                    cursor.close()
                    conn.close()
                    raise HTTPException(
                        status_code=400,
                        detail="extraction_schema must be text string when prompt_field='text'"
                    )
                schema_value = str(schema_value)

            # Insert new schema with prompt_field and logic_type_id
            cursor.execute("""
                INSERT INTO document_schemas (doc_type_id, schema_json, prompt_field, logic_type_id, user_id)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, doc_type_id, schema_json, prompt_field, logic_type_id, user_id, created_at, updated_at
            """, (request.doc_type_id, schema_value, request.prompt_field, logic_type_id, user_id))

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Created schema for doc_type_id: {request.doc_type_id}, prompt_field: {request.prompt_field}, logic_type_id: {logic_type_id}, user_id: {user_id}")

            return DocumentSchemaResponse(
                id=result['id'],
                doc_type_id=result['doc_type_id'],
                extraction_schema=result['schema_json'],
                prompt_field=result['prompt_field'],
                logic_type_id=result['logic_type_id'],
                logic_name=logic_name,
                user_id=result['user_id'],
                created_at=result['created_at'].isoformat(),
                updated_at=result['updated_at'].isoformat()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating document schema: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to create document schema: {str(e)}")


    @app.get("/document-schemas", tags=["Document Schemas"])
    async def list_document_schemas(current_user = Depends(get_current_user)):
        """
        List all document schemas for the authenticated user.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
                SELECT ds.id, ds.doc_type_id, ds.schema_json, ds.prompt_field,
                       ds.logic_type_id, ds.user_id, ds.created_at, ds.updated_at,
                       dt.document_type,
                       dl.logic_name
                FROM document_schemas ds
                JOIN document_types dt ON ds.doc_type_id = dt.doc_type_id
                LEFT JOIN document_logics dl ON ds.logic_type_id = dl.logic_type_id
                WHERE ds.user_id = %s
                ORDER BY ds.created_at DESC
            """, (user_id,))

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            schemas = [
                {
                    "id": row['id'],
                    "doc_type_id": row['doc_type_id'],
                    "document_type": row['document_type'],
                    "extraction_schema": row['schema_json'],
                    "prompt_field": row.get('prompt_field', 'text'),
                    "logic_type_id": row['logic_type_id'],
                    "logic_name": row['logic_name'],
                    "user_id": row['user_id'],
                    "created_at": row['created_at'].isoformat(),
                    "updated_at": row['updated_at'].isoformat()
                }
                for row in results
            ]

            return {
                "success": True,
                "schemas": schemas,
                "total": len(schemas)
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error listing document schemas: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to list document schemas: {str(e)}")


    @app.get("/document-schemas/{doc_type_id}", tags=["Document Schemas"])
    async def get_document_schema_by_type(
        doc_type_id: int,
        current_user = Depends(get_current_user)
    ):
        """
        Get document schema for a specific document type.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
                SELECT ds.id, ds.doc_type_id, ds.schema_json, ds.prompt_field,
                       ds.logic_type_id, ds.user_id, ds.created_at, ds.updated_at,
                       dt.document_type,
                       dl.logic_name
                FROM document_schemas ds
                JOIN document_types dt ON ds.doc_type_id = dt.doc_type_id
                LEFT JOIN document_logics dl ON ds.logic_type_id = dl.logic_type_id
                WHERE ds.doc_type_id = %s AND ds.user_id = %s
            """, (doc_type_id, user_id))

            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if not result:
                raise HTTPException(status_code=404, detail="Schema not found for this document type")

            return {
                "success": True,
                "schema": {
                    "id": result['id'],
                    "doc_type_id": result['doc_type_id'],
                    "document_type": result['document_type'],
                    "extraction_schema": result['schema_json'],
                    "prompt_field": result.get('prompt_field', 'text'),
                    "logic_type_id": result['logic_type_id'],
                    "logic_name": result['logic_name'],
                    "user_id": result['user_id'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting document schema: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to get document schema: {str(e)}")


    @app.put("/document-schemas/{schema_id}", tags=["Document Schemas"])
    async def update_document_schema(
        schema_id: int,
        request: DocumentSchemaCreate,
        current_user = Depends(get_current_user)
    ):
        """
        Update a document schema.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Check if schema exists and belongs to user
            cursor.execute("""
                SELECT id FROM document_schemas
                WHERE id = %s AND user_id = %s
            """, (schema_id, user_id))

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(status_code=404, detail="Schema not found")

            # Validate logic_type_id if provided and fetch logic_name
            logic_type_id = getattr(request, 'logic_type_id', None)
            logic_name = None
            if logic_type_id is not None:
                cursor.execute("""
                    SELECT logic_type_id, logic_name FROM document_logics
                    WHERE logic_type_id = %s AND user_id = %s
                """, (logic_type_id, user_id))
                logic_row = cursor.fetchone()
                if not logic_row:
                    cursor.close()
                    conn.close()
                    raise HTTPException(
                        status_code=404,
                        detail=f"Document logic {logic_type_id} not found or doesn't belong to you"
                    )
                logic_name = logic_row['logic_name']

            # Process extraction_schema based on prompt_field
            schema_value = request.extraction_schema

            if request.prompt_field == 'json':
                # For JSON prompt_field, store as JSON string
                if isinstance(schema_value, (dict, list)):
                    schema_value = json.dumps(schema_value)
                elif isinstance(schema_value, str):
                    # Validate it's valid JSON
                    try:
                        json.loads(schema_value)
                    except json.JSONDecodeError:
                        cursor.close()
                        conn.close()
                        raise HTTPException(
                            status_code=400,
                            detail="Invalid JSON in extraction_schema when prompt_field='json'"
                        )
            else:  # prompt_field == 'text'
                # For text prompt_field, store as plain string
                if isinstance(schema_value, (dict, list)):
                    cursor.close()
                    conn.close()
                    raise HTTPException(
                        status_code=400,
                        detail="extraction_schema must be text string when prompt_field='text'"
                    )
                schema_value = str(schema_value)

            # Update schema with prompt_field and logic_type_id
            cursor.execute("""
                UPDATE document_schemas
                SET schema_json = %s, prompt_field = %s, logic_type_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND user_id = %s
                RETURNING id, doc_type_id, schema_json, prompt_field, logic_type_id, user_id, created_at, updated_at
            """, (schema_value, request.prompt_field, logic_type_id, schema_id, user_id))

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Updated schema ID {schema_id} with prompt_field: {request.prompt_field}, logic_type_id: {logic_type_id} for user_id: {user_id}")

            return {
                "success": True,
                "schema": {
                    "id": result['id'],
                    "doc_type_id": result['doc_type_id'],
                    "extraction_schema": result['schema_json'],
                    "prompt_field": result['prompt_field'],
                    "logic_type_id": result['logic_type_id'],
                    "logic_name": logic_name,
                    "user_id": result['user_id'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat()
                }
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating document schema: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to update document schema: {str(e)}")


    @app.delete("/document-schemas/{schema_id}", tags=["Document Schemas"])
    async def delete_document_schema(
        schema_id: int,
        current_user = Depends(get_current_user)
    ):
        """
        Delete a document schema.
        """
        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")

            conn = get_db_connection()
            cursor = conn.cursor()

            # Check if schema exists and belongs to user
            cursor.execute("""
                SELECT id FROM document_schemas
                WHERE id = %s AND user_id = %s
            """, (schema_id, user_id))

            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(status_code=404, detail="Schema not found")

            # Delete schema
            cursor.execute("""
                DELETE FROM document_schemas
                WHERE id = %s AND user_id = %s
            """, (schema_id, user_id))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Deleted schema ID {schema_id} for user_id: {user_id}")

            return {
                "success": True,
                "message": f"Schema {schema_id} deleted successfully"
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting document schema: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to delete document schema: {str(e)}")