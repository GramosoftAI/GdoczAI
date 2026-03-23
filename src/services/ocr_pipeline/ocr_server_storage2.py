# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import jwt
import psycopg2
import logging
from typing import Dict, Optional, List, Tuple
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE CONNECTION HELPER
# ============================================================================
def get_db_connection(pg_config: Dict):
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=pg_config.get('database', 'document_pipeline'),
            user=pg_config.get('user'),
            password=pg_config.get('password')
        )
        return conn
    except Exception as e:
        logger.error(f"? Database connection error: {e}")
        return None

# ============================================================================
# ?? DOCUMENT TYPE CONFIGURATION RETRIEVAL FUNCTIONS (UPDATED)
# ============================================================================

def get_document_type_id(document_type: str, user_id: int, pg_config: Dict) -> Optional[int]:

    conn = get_db_connection(pg_config)
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT doc_type_id
            FROM document_types
            WHERE document_type = %s AND user_id = %s
        """, (document_type, user_id))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            logger.info(f"? Found doc_type_id: {result['doc_type_id']} for document_type: {document_type}")
            return result['doc_type_id']
        else:
            logger.info(f"?? Document type '{document_type}' not found in database")
            return None
            
    except Exception as e:
        logger.error(f"? Error fetching document type: {e}")
        if conn:
            conn.close()
        return None

def get_schema_for_document_type(doc_type_id: int, pg_config: Dict) -> Optional[Dict]:

    conn = get_db_connection(pg_config)
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT schema_json
            FROM document_schemas
            WHERE doc_type_id = %s
        """, (doc_type_id,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            schema_json = result['schema_json']
            logger.info(f"? Found schema for doc_type_id: {doc_type_id}")
            logger.info(f"?? Schema fields: {list(schema_json.keys()) if isinstance(schema_json, dict) else 'N/A'}")
            return schema_json
        else:
            logger.info(f"?? No schema found for doc_type_id: {doc_type_id}")
            return None
            
    except Exception as e:
        logger.error(f"? Error fetching schema: {e}")
        if conn:
            conn.close()
        return None

def get_conditional_keys(doc_type_id: int, pg_config: Dict) -> List[str]:

    conn = get_db_connection(pg_config)
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # ?? UPDATED QUERY: Fetch from document_types table
        cursor.execute("""
            SELECT conditional_keys
            FROM document_types
            WHERE doc_type_id = %s
        """, (doc_type_id,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result['conditional_keys']:
            keys_text = result['conditional_keys']
            
            # Parse keys (handle both comma-separated and newline-separated)
            if '\n' in keys_text:
                keys_list = [key.strip() for key in keys_text.split('\n') if key.strip()]
            else:
                keys_list = [key.strip() for key in keys_text.split(',') if key.strip()]
            
            logger.info(f"? Found {len(keys_list)} conditional keys for doc_type_id: {doc_type_id}")
            logger.info(f"?? Keys: {', '.join(keys_list[:5])}{'...' if len(keys_list) > 5 else ''}")
            logger.info(f"   ?? Source: document_types.conditional_keys column")
            return keys_list
        else:
            logger.info(f"?? No conditional keys found for doc_type_id: {doc_type_id}")
            return []
            
    except Exception as e:
        logger.error(f"? Error fetching conditional keys: {e}")
        if conn:
            conn.close()
        return []

def get_langchain_keys(doc_type_id: int, pg_config: Dict) -> List[str]:

    conn = get_db_connection(pg_config)
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # ?? UPDATED QUERY: Fetch from document_types table
        cursor.execute("""
            SELECT langchain_keys
            FROM document_types
            WHERE doc_type_id = %s
        """, (doc_type_id,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result['langchain_keys']:
            keys_text = result['langchain_keys']
            
            # Parse keys (handle both comma-separated and newline-separated)
            if '\n' in keys_text:
                keys_list = [key.strip() for key in keys_text.split('\n') if key.strip()]
            else:
                keys_list = [key.strip() for key in keys_text.split(',') if key.strip()]
            
            logger.info(f"? Found {len(keys_list)} LangChain keys for doc_type_id: {doc_type_id}")
            logger.info(f"?? Keys: {', '.join(keys_list)}")
            logger.info(f"   ?? Source: document_types.langchain_keys column")
            return keys_list
        else:
            logger.info(f"?? No LangChain keys found for doc_type_id: {doc_type_id}")
            return []
            
    except Exception as e:
        logger.error(f"? Error fetching LangChain keys: {e}")
        if conn:
            conn.close()
        return []

def get_document_config(document_type: str, user_id: int, pg_config: Dict) -> Dict:

    logger.info("=" * 80)
    logger.info(f"?? Fetching complete config for document_type: '{document_type}', user_id: {user_id}")
    logger.info("=" * 80)
    
    conn = get_db_connection(pg_config)
    if not conn:
        logger.error("? Database connection failed")
        return {
            "status": "connection_error",
            "message": "Could not connect to database",
            "doc_type_id": None,
            "document_type": document_type,
            "conditional_keys": [],
            "langchain_keys": [],
            "schema_json": None,
            "has_conditional_keys": False,
            "has_langchain_keys": False,
            "has_schema": False
        }
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # ?? OPTIMIZED QUERY: Fetch document_type info with keys in single query
        cursor.execute("""
            SELECT 
                dt.doc_type_id,
                dt.document_type,
                dt.conditional_keys,
                dt.langchain_keys
            FROM document_types dt
            WHERE dt.document_type = %s AND dt.user_id = %s
        """, (document_type, user_id))
        
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            logger.warning(f"?? Document type '{document_type}' not found for user {user_id}")
            return {
                "status": "not_found",
                "message": f"Document type '{document_type}' not configured for this user",
                "doc_type_id": None,
                "document_type": document_type,
                "conditional_keys": [],
                "langchain_keys": [],
                "schema_json": None,
                "has_conditional_keys": False,
                "has_langchain_keys": False,
                "has_schema": False
            }
        
        doc_type_id = result['doc_type_id']
        
        # Parse conditional_keys
        conditional_keys = []
        if result['conditional_keys']:
            keys_text = result['conditional_keys']
            if '\n' in keys_text:
                conditional_keys = [key.strip() for key in keys_text.split('\n') if key.strip()]
            else:
                conditional_keys = [key.strip() for key in keys_text.split(',') if key.strip()]
        
        # Parse langchain_keys
        langchain_keys = []
        if result['langchain_keys']:
            keys_text = result['langchain_keys']
            if '\n' in keys_text:
                langchain_keys = [key.strip() for key in keys_text.split('\n') if key.strip()]
            else:
                langchain_keys = [key.strip() for key in keys_text.split(',') if key.strip()]
        
        # Fetch schema (separate query - unchanged)
        cursor.execute("""
            SELECT schema_json
            FROM document_schemas
            WHERE doc_type_id = %s
        """, (doc_type_id,))
        
        schema_result = cursor.fetchone()
        schema_json = schema_result['schema_json'] if schema_result else None
        
        cursor.close()
        conn.close()
        
        # Build complete config
        config = {
            "status": "success",
            "doc_type_id": doc_type_id,
            "document_type": document_type,
            "conditional_keys": conditional_keys,
            "langchain_keys": langchain_keys,
            "schema_json": schema_json,
            "has_conditional_keys": len(conditional_keys) > 0,
            "has_langchain_keys": len(langchain_keys) > 0,
            "has_schema": schema_json is not None
        }
        
        # Log summary
        logger.info("=" * 80)
        logger.info("?? DOCUMENT CONFIG SUMMARY:")
        logger.info("=" * 80)
        logger.info(f"? Document Type ID: {doc_type_id}")
        logger.info(f"?? Document Type: {document_type}")
        logger.info(f"?? Conditional Keys: {len(conditional_keys)} keys")
        if conditional_keys:
            logger.info(f"   Keys: {', '.join(conditional_keys[:5])}{'...' if len(conditional_keys) > 5 else ''}")
            logger.info(f"   ?? Source: document_types.conditional_keys")
        logger.info(f"?? LangChain Keys: {len(langchain_keys)} keys")
        if langchain_keys:
            logger.info(f"   Keys: {', '.join(langchain_keys)}")
            logger.info(f"   ?? Source: document_types.langchain_keys")
        logger.info(f"?? Schema: {'? Available' if schema_json else '?? Not configured'}")
        if schema_json:
            logger.info(f"   Schema fields: {len(schema_json)} fields")
            logger.info(f"   ?? Source: document_schemas table")
        logger.info("=" * 80)
        
        return config
        
    except Exception as e:
        logger.error(f"? Error fetching document config: {e}")
        if conn:
            conn.close()
        return {
            "status": "error",
            "message": str(e),
            "doc_type_id": None,
            "document_type": document_type,
            "conditional_keys": [],
            "langchain_keys": [],
            "schema_json": None,
            "has_conditional_keys": False,
            "has_langchain_keys": False,
            "has_schema": False
        }

def get_document_config_or_fallback(
    document_type: str, 
    user_id: Optional[int], 
    pg_config: Dict,
    fallback_schema: Optional[Dict] = None
) -> Dict:
    
    if user_id:
        config = get_document_config(document_type, user_id, pg_config)
        
        if config.get('status') == 'success':
            return config
        
        logger.info(f"?? No config found in database for user {user_id}")
    else:
        logger.info("?? No user authentication - skipping database lookup")
    
    # Fallback: use provided schema or return empty config
    if fallback_schema:
        logger.info(f"? Using fallback schema with {len(fallback_schema)} fields")
        return {
            "status": "fallback",
            "message": "Using dynamic schema (not from database)",
            "doc_type_id": None,
            "document_type": document_type,
            "conditional_keys": [],  # No validation when using fallback
            "langchain_keys": [],     # No LangChain splitting when using fallback
            "schema_json": fallback_schema,
            "has_conditional_keys": False,
            "has_langchain_keys": False,
            "has_schema": True
        }
    else:
        logger.warning("?? No database config and no fallback schema provided")
        return {
            "status": "no_config",
            "message": "No configuration available",
            "doc_type_id": None,
            "document_type": document_type,
            "conditional_keys": [],
            "langchain_keys": [],
            "schema_json": None,
            "has_conditional_keys": False,
            "has_langchain_keys": False,
            "has_schema": False
        }

# ============================================================================
# JWT TOKEN VERIFICATION
# ============================================================================
def verify_jwt_token(authorization: Optional[str], jwt_secret: str) -> Optional[Dict]:

    if not authorization:
        return None
    
    try:
        # Extract token from "Bearer <token>"
        if not authorization.startswith("Bearer "):
            return None
        
        token = authorization.split(" ")[1]
        
        # Decode JWT token
        payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
        
        user_id = payload.get('user_id')
        if not user_id:
            return None
        
        logger.info(f"?? JWT verified - user_id: {user_id}")
        return {"user_id": user_id}
        
    except jwt.ExpiredSignatureError:
        logger.warning("?? JWT token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"?? Invalid JWT token: {e}")
        return None
    except Exception as e:
        logger.error(f"? JWT verification error: {e}")
        return None

# ============================================================================
# ?? VALIDATION HELPER FUNCTIONS
# ============================================================================

def validate_document_config(config: Dict) -> Tuple[bool, str]:

    if not config:
        return False, "Configuration is empty"
    
    status = config.get('status')
    
    if status == 'not_found':
        return False, f"Document type '{config.get('document_type')}' not configured"
    
    if status == 'no_config':
        return False, "No configuration available for this document type"
    
    # For fallback schemas, validation is still valid
    if status == 'fallback':
        if not config.get('schema_json'):
            return False, "Fallback schema is empty"
        return True, ""
    
    # For database configs, check if at least schema is present
    if status == 'success':
        has_schema = config.get('has_schema', False)
        if not has_schema:
            logger.warning("?? Document config has no schema - dynamic extraction will be used")
        return True, ""
    
    return False, "Unknown configuration status"

def should_use_langchain_chunking(config: Dict) -> bool:
    return config.get('has_langchain_keys', False) and len(config.get('langchain_keys', [])) > 0

def should_validate_markdown(config: Dict) -> bool:
    return config.get('has_conditional_keys', False) and len(config.get('conditional_keys', [])) > 0
