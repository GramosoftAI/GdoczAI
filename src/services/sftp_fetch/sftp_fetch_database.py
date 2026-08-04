"""
Database connection module for sftp_connector queries
"""

import psycopg2
import psycopg2.extras
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Global database storage instance
_db_storage = None

def init_db_connection(pg_config: Dict[str, Any]) -> None:
    """Initialize database connection pool"""
    global _db_storage
    try:
        from src.core.database.db_storage_util import DatabaseStorage
        _db_storage = DatabaseStorage(pg_config)
        logger.info("? Database connection pool initialized")
    except Exception as e:
        logger.error(f"? Failed to initialize database connection: {e}")
        raise

def get_db_connection():
    """Get a database connection from the pool"""
    global _db_storage
    if _db_storage is None:
        raise Exception("Database connection not initialized. Call init_db_connection() first.")
    return _db_storage._get_connection()

def query_active_sftp_connectors() -> List[Dict[str, Any]]:

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT id, user_id, host_name, port, username, password, 
                   private_key_path, monitor_folders, moved_folder, 
                   failed_folder, interval_minute, is_active, created_at, updated_at
            FROM sftp_connector 
            WHERE is_active = true
            ORDER BY user_id, id
        """
        
        cursor.execute(query)
        connectors = cursor.fetchall()
        cursor.close()
        
        return [dict(connector) for connector in connectors]
    
    except psycopg2.Error as e:
        logger.error(f"? Database query failed: {e}")
        raise
    except Exception as e:
        logger.error(f"? Unexpected error querying sftp_connector: {e}")
        raise
    finally:
        if conn:
            _db_storage._put_connection(conn)

def get_sftp_connector_by_id(connector_id: int) -> Dict[str, Any]:

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT id, user_id, host_name, port, username, password, 
                   private_key_path, monitor_folders, moved_folder, 
                   failed_folder, interval_minute, is_active, created_at, updated_at
            FROM sftp_connector 
            WHERE id = %s
        """
        
        cursor.execute(query, (connector_id,))
        connector = cursor.fetchone()
        cursor.close()
        
        return dict(connector) if connector else None
    
    except psycopg2.Error as e:
        logger.error(f"? Database query failed: {e}")
        raise
    except Exception as e:
        logger.error(f"? Unexpected error querying sftp_connector: {e}")
        raise
    finally:
        if conn:
            _db_storage._put_connection(conn)

def get_connectors_by_user_id(user_id: int) -> List[Dict[str, Any]]:

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT id, user_id, host_name, port, username, password, 
                   private_key_path, monitor_folders, moved_folder, 
                   failed_folder, interval_minute, is_active, created_at, updated_at
            FROM sftp_connector 
            WHERE user_id = %s AND is_active = true
            ORDER BY id
        """
        
        cursor.execute(query, (user_id,))
        connectors = cursor.fetchall()
        cursor.close()
        
        return [dict(connector) for connector in connectors]
    
    except psycopg2.Error as e:
        logger.error(f"? Database query failed: {e}")
        raise
    except Exception as e:
        logger.error(f"? Unexpected error querying sftp_connector: {e}")
        raise
    finally:
        if conn:
            _db_storage._put_connection(conn)
