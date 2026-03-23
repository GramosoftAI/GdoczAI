#!/usr/bin/env python3

"""
Shared Database Storage Utility.

Provides database storage functionality for OCR processing results.
Manages multi-user support, request tracking, and OCR fallback statistics.
"""

import json
import logging
import threading
from datetime import datetime
from typing import Dict, Optional, Any, List
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

class ProcessingStatus:
    """Processing status constants"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class DatabaseStorage:
    """Shared database storage for OCR results"""
    
    def __init__(self, pg_config: Dict[str, Any]):
        """
        Initialize database storage with PostgreSQL configuration
        
        Args:
            pg_config: Dictionary with PostgreSQL connection details
                      (host, port, database, user, password, connection_pool_size)
        """
        self.pg_config = pg_config
        self.lock = threading.Lock()
        self.connection_pool = None
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """Initialize PostgreSQL connection pool"""
        try:
            pool_size = self.pg_config.get('connection_pool_size', 10)
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=pool_size,
                host=self.pg_config.get('host', 'localhost'),
                port=self.pg_config.get('port', 5432),
                database=self.pg_config.get('database', 'document_pipeline'),
                user=self.pg_config.get('user'),
                password=self.pg_config.get('password')
            )
            logger.info("PostgreSQL connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL connection pool: {e}")
            raise
    
    def _get_connection(self):
        """Get a connection from the pool"""
        return self.connection_pool.getconn()
    
    def _put_connection(self, conn):
        """Return a connection to the pool"""
        self.connection_pool.putconn(conn)
    
    
    def store_ocr_result(
        self,
        file_name: str,
        markdown_output: str,
        json_output: Dict,
        page_count: int,
        processing_duration: float,
        token_usage: int,
        unique_id: Optional[str] = None,
        error_details: Optional[str] = None,
        request_id: Optional[str] = None,
        user_id: Optional[int] = None,
        file_path: Optional[str] = None,
        mineru_markdown: Optional[str] = None,
        olmocr_markdown: Optional[str] = None,
        olmocr_used: str = "No",
        missed_keys: Optional[List[str]] = None
    ) -> bool:
        """
        Store OCR processing result in the database.
        
        Supports multi-user storage with request tracking and OCR fallback statistics.
        """
        # ...existing code...
        
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                now = datetime.now()
                status = ProcessingStatus.COMPLETED if not error_details else ProcessingStatus.FAILED
                
                # Convert missed_keys list to comma-separated string
                missed_keys_str = None
                if missed_keys and len(missed_keys) > 0:
                    missed_keys_str = ", ".join(missed_keys)
                
                # Log what we're about to store
                logger.info(f"Storing OCR result for: {file_name}")
                
                if user_id is not None:
                    # Check for authenticated user's file
                    cursor.execute(
                        "SELECT file_id FROM processed_files WHERE file_name = %s AND user_id = %s",
                        (file_name, user_id)
                    )
                else:
                    # Check for SFTP file (user_id IS NULL)
                    cursor.execute(
                        "SELECT file_id FROM processed_files WHERE file_name = %s AND user_id IS NULL",
                        (file_name,)
                    )
                
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Update existing record
                    logger.info(f"Updating existing record for {file_name} (user_id: {user_id})")
                    if user_id is not None:
                        cursor.execute(
                            """UPDATE processed_files 
                            SET page_count = %s,
                                processed_on = %s,
                                processing_duration = %s,
                                json_output = %s,
                                markdown_output = %s,
                                mineru_markdown_content = %s,
                                olmocr_markdown_content = %s,
                                olmocr_used = %s,
                                missed_keys = %s,
                                token_usage = %s,
                                error_details = %s,
                                unique_id = %s,
                                processing_status = %s,
                                updated_on = %s,
                                request_id = %s,
                                file_path = %s
                            WHERE file_name = %s AND user_id = %s""",
                            (
                                page_count, now, processing_duration,
                                json.dumps(json_output) if json_output else None,
                                markdown_output,
                                mineru_markdown,
                                olmocr_markdown,
                                olmocr_used,
                                missed_keys_str,
                                token_usage, error_details,
                                unique_id, status, now, request_id, file_path,
                                file_name, user_id
                            )
                        )
                    else:
                        cursor.execute(
                            """UPDATE processed_files 
                            SET page_count = %s,
                                processed_on = %s,
                                processing_duration = %s,
                                json_output = %s,
                                markdown_output = %s,
                                mineru_markdown_content = %s,
                                olmocr_markdown_content = %s,
                                olmocr_used = %s,
                                missed_keys = %s,
                                token_usage = %s,
                                error_details = %s,
                                unique_id = %s,
                                processing_status = %s,
                                updated_on = %s,
                                request_id = %s,
                                file_path = %s
                            WHERE file_name = %s AND user_id IS NULL""",
                            (
                                page_count, now, processing_duration,
                                json.dumps(json_output) if json_output else None,
                                markdown_output,
                                mineru_markdown,
                                olmocr_markdown,
                                olmocr_used,
                                missed_keys_str,
                                token_usage, error_details,
                                unique_id, status, now, request_id, file_path,
                                file_name
                            )
                        )
                    
                    logger.info(f"Updated database record for {file_name}")
                    logger.info(f"request_id: {request_id}")
                    logger.info(f"user_id: {user_id}")
                    logger.info(f"file_path: {file_path}")
                    logger.info(f"olmocr_used: {olmocr_used}")
                    if missed_keys_str:
                        logger.info(f"missed_keys: {missed_keys_str}")
                
                else:
                    # Insert new record - each user gets their own row!
                    logger.info(f"Inserting new record for {file_name} (user_id: {user_id})")
                    cursor.execute(
                        """INSERT INTO processed_files 
                        (file_name, page_count, processed_on, processing_duration,
                         json_output, markdown_output, mineru_markdown_content,
                         olmocr_markdown_content, olmocr_used, missed_keys,
                         token_usage, error_details,
                         unique_id, processing_status, created_on, updated_on,
                         request_id, user_id, file_path)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            file_name, page_count, now, processing_duration,
                            json.dumps(json_output) if json_output else None,
                            markdown_output,
                            mineru_markdown,
                            olmocr_markdown,
                            olmocr_used,
                            missed_keys_str,
                            token_usage, error_details,
                            unique_id, status, now, now, request_id, user_id, file_path
                        )
                    )
                    
                    logger.info(f"Inserted new database record for {file_name}")
                    logger.info(f"request_id: {request_id}")
                    logger.info(f"user_id: {user_id}")
                    logger.info(f"file_path: {file_path}")
                    logger.info(f"olmocr_used: {olmocr_used}")
                    if missed_keys_str:
                        logger.info(f"missed_keys: {missed_keys_str}")
                
                conn.commit()
                return True
            
            except psycopg2.Error as e:
                logger.error(f"Database error storing OCR result: {e}")
                if conn:
                    conn.rollback()
                return False
            
            except Exception as e:
                logger.error(f"Unexpected error storing OCR result: {e}")
                if conn:
                    conn.rollback()
                return False
            
            finally:
                if conn:
                    self._put_connection(conn)
    
    def update_processing_status(
        self,
        file_name: str,
        status: str,
        error_details: Optional[str] = None,
        request_id: Optional[str] = None,
        user_id: Optional[int] = None,
        file_path: Optional[str] = None,
        olmocr_used: Optional[str] = None,
        missed_keys: Optional[List[str]] = None
    ) -> bool:
        """
        Update only the processing status of a file.
        """
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Build dynamic SQL based on provided parameters
                update_fields = [
                    "processing_status=%s",
                    "updated_on=CURRENT_TIMESTAMP"
                ]
                params = [status]
                
                if error_details is not None:
                    update_fields.append("error_details=%s")
                    params.append(error_details)
                if request_id is not None:
                    update_fields.append("request_id=%s")
                    params.append(request_id)
                if file_path is not None:
                    update_fields.append("file_path=%s")
                    params.append(file_path)
                if olmocr_used is not None:
                    update_fields.append("olmocr_used=%s")
                    params.append(olmocr_used)
                if missed_keys is not None and len(missed_keys) > 0:
                    update_fields.append("missed_keys=%s")
                    params.append(", ".join(missed_keys))
                # ? REMOVED: document_type update logic
                
                # Add file_name for WHERE clause
                params.append(file_name)
                
                if user_id is not None:
                    # Match specific user's file
                    sql = f"""UPDATE processed_files 
                              SET {', '.join(update_fields)} 
                              WHERE file_name=%s AND user_id=%s"""
                    params.append(user_id)
                else:
                    # Match SFTP file (user_id IS NULL)
                    sql = f"""UPDATE processed_files 
                              SET {', '.join(update_fields)} 
                              WHERE file_name=%s AND user_id IS NULL"""
                
                cursor.execute(sql, params)
                conn.commit()
                
                if request_id or user_id or file_path or olmocr_used or missed_keys:
                    logger.info(f"? Updated file status:")
                    logger.info(f"   File: {file_name}")
                    logger.info(f"   Status: {status}")
                    if request_id:
                        logger.info(f"   ?? request_id: {request_id}")
                    if user_id:
                        logger.info(f"   ?? user_id: {user_id}")
                    if file_path:
                        logger.info(f"   ?? file_path: {file_path}")
                    if olmocr_used:
                        logger.info(f"   ?? olmocr_used: {olmocr_used}")
                    if missed_keys:
                        logger.info(f"   ?? missed_keys: {', '.join(missed_keys)}")
                    # ? REMOVED: document_type logging
                
                return cursor.rowcount > 0
            
            except psycopg2.Error as e:
                logger.error(f"Database error updating status: {e}")
                if conn:
                    conn.rollback()
                return False
            
            finally:
                if conn:
                    self._put_connection(conn)
    
    def get_file_record(self, file_name: str = None, request_id: str = None) -> Optional[Dict]:
        """
        Retrieve a file record from the database by file_name or request_id
        
        Args:
            file_name: Name of the file (optional)
            request_id: Request ID (optional)
        
        Returns:
            Dictionary with file record data or None if not found
        """
        if not file_name and not request_id:
            logger.error("Either file_name or request_id must be provided")
            return None
        
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                if request_id:
                    cursor.execute(
                        "SELECT * FROM processed_files WHERE request_id = %s",
                        (request_id,)
                    )
                else:
                    cursor.execute(
                        "SELECT * FROM processed_files WHERE file_name = %s",
                        (file_name,)
                    )
                
                row = cursor.fetchone()
                if row:
                    # Convert to regular dict and parse JSON fields
                    record = dict(row)
                    if record.get('json_output'):
                        try:
                            record['json_output'] = json.loads(record['json_output'])
                        except json.JSONDecodeError:
                            pass
                    
                    # ?? Parse missed_keys from comma-separated string to list
                    if record.get('missed_keys'):
                        record['missed_keys_list'] = [k.strip() for k in record['missed_keys'].split(',')]
                    else:
                        record['missed_keys_list'] = []
                    
                    return record
                
                return None
            
            except psycopg2.Error as e:
                logger.error(f"Database error retrieving file record: {e}")
                return None
            
            finally:
                if conn:
                    self._put_connection(conn)
    
    def get_files_by_user(self, user_id: int, limit: int = 50, offset: int = 0) -> list:
        """
        Retrieve all files processed by a specific user
        
        Args:
            user_id: ID of the user
            limit: Maximum number of records to return
            offset: Offset for pagination
        
        Returns:
            List of file records
        """
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                cursor.execute(
                    """SELECT * FROM processed_files 
                    WHERE user_id = %s 
                    ORDER BY created_on DESC 
                    LIMIT %s OFFSET %s""",
                    (user_id, limit, offset)
                )
                
                rows = cursor.fetchall()
                records = []
                for row in rows:
                    record = dict(row)
                    if record.get('json_output'):
                        try:
                            record['json_output'] = json.loads(record['json_output'])
                        except json.JSONDecodeError:
                            pass
                    
                    # ?? Parse missed_keys
                    if record.get('missed_keys'):
                        record['missed_keys_list'] = [k.strip() for k in record['missed_keys'].split(',')]
                    else:
                        record['missed_keys_list'] = []
                    
                    records.append(record)
                
                return records
            
            except psycopg2.Error as e:
                logger.error(f"Database error retrieving user files: {e}")
                return []
            
            finally:
                if conn:
                    self._put_connection(conn)
    
    def get_files_by_document_type(self, document_type: str, limit: int = 50, offset: int = 0) -> list:
        """
        This method is deprecated. Use document_types table for document type filtering.
        
        Returns empty list for backward compatibility.
        """
        logger.warning("get_files_by_document_type is deprecated - document_type no longer stored in processed_files")
        logger.warning("Use document_types table for document type management")
        return []
    
    def get_olmocr_usage_stats(self, user_id: Optional[int] = None, days: int = 30) -> Dict:
        """
        Get OLM OCR usage statistics for the specified time period.
        """
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                base_query = """
                    SELECT 
                        COUNT(*) as total_files,
                        SUM(CASE WHEN olmocr_used = 'Yes' THEN 1 ELSE 0 END) as olmocr_count,
                        SUM(CASE WHEN olmocr_used = 'No' THEN 1 ELSE 0 END) as mineru_count,
                        SUM(CASE WHEN missed_keys IS NOT NULL AND missed_keys != '' THEN 1 ELSE 0 END) as files_with_missed_keys
                    FROM processed_files
                    WHERE processed_on >= NOW() - INTERVAL '%s days'
                """
                
                if user_id:
                    query = base_query + " AND user_id = %s"
                    cursor.execute(query, (days, user_id))
                else:
                    cursor.execute(base_query, (days,))
                
                result = cursor.fetchone()
                
                if result:
                    total = result['total_files'] or 0
                    olmocr = result['olmocr_count'] or 0
                    mineru = result['mineru_count'] or 0
                    missed = result['files_with_missed_keys'] or 0
                    
                    return {
                        'total_files': total,
                        'olmocr_used_count': olmocr,
                        'mineru_only_count': mineru,
                        'files_with_missed_keys': missed,
                        'olmocr_usage_percentage': round((olmocr / total * 100), 2) if total > 0 else 0,
                        'missed_keys_percentage': round((missed / total * 100), 2) if total > 0 else 0,
                        'period_days': days
                    }
                
                return {
                    'total_files': 0,
                    'olmocr_used_count': 0,
                    'mineru_only_count': 0,
                    'files_with_missed_keys': 0,
                    'olmocr_usage_percentage': 0,
                    'missed_keys_percentage': 0,
                    'period_days': days
                }
            
            except psycopg2.Error as e:
                logger.error(f"Database error retrieving OCR usage stats: {e}")
                return {
                    'error': str(e),
                    'total_files': 0,
                    'olmocr_used_count': 0,
                    'mineru_only_count': 0
                }
            
            finally:
                if conn:
                    self._put_connection(conn)
    
    def get_most_common_missed_keys(self, user_id: Optional[int] = None, limit: int = 10) -> List[Dict]:
        """
        Get most commonly missed conditional keys.
        """
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Get all files with missed keys
                base_query = """
                    SELECT missed_keys
                    FROM processed_files
                    WHERE missed_keys IS NOT NULL AND missed_keys != ''
                """
                
                if user_id:
                    query = base_query + " AND user_id = %s"
                    cursor.execute(query, (user_id,))
                else:
                    cursor.execute(base_query)
                
                rows = cursor.fetchall()
                
                # Count occurrences of each key
                key_counts = {}
                for row in rows:
                    if row['missed_keys']:
                        keys = [k.strip() for k in row['missed_keys'].split(',')]
                        for key in keys:
                            if key:
                                key_counts[key] = key_counts.get(key, 0) + 1
                
                # Sort by count and return top N
                sorted_keys = sorted(key_counts.items(), key=lambda x: x[1], reverse=True)[:limit]
                
                return [
                    {'key': key, 'count': count}
                    for key, count in sorted_keys
                ]
            
            except psycopg2.Error as e:
                logger.error(f"Database error retrieving common missed keys: {e}")
                return []
            
            finally:
                if conn:
                    self._put_connection(conn)
    
    def get_files_with_missed_keys(
        self, 
        user_id: Optional[int] = None, 
        limit: int = 50, 
        offset: int = 0
    ) -> List[Dict]:
        """
        Get all files that had missing conditional keys.
        """
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                base_query = """
                    SELECT file_id, file_name, user_id, request_id, 
                           missed_keys, olmocr_used, processed_on, processing_status
                    FROM processed_files
                    WHERE missed_keys IS NOT NULL AND missed_keys != ''
                    ORDER BY processed_on DESC
                    LIMIT %s OFFSET %s
                """
                
                if user_id:
                    query = base_query.replace(
                        "WHERE missed_keys",
                        "WHERE user_id = %s AND missed_keys"
                    )
                    cursor.execute(query, (user_id, limit, offset))
                else:
                    cursor.execute(base_query, (limit, offset))
                
                rows = cursor.fetchall()
                records = []
                
                for row in rows:
                    record = dict(row)
                    # Parse missed_keys to list
                    if record.get('missed_keys'):
                        record['missed_keys_list'] = [k.strip() for k in record['missed_keys'].split(',')]
                    else:
                        record['missed_keys_list'] = []
                    records.append(record)
                
                return records
            
            except psycopg2.Error as e:
                logger.error(f"Database error retrieving files with missed keys: {e}")
                return []
            
            finally:
                if conn:
                    self._put_connection(conn)
    
    def get_ocr_comparison_data(
        self, 
        user_id: Optional[int] = None,
        days: int = 30
    ) -> Dict:
        """
        Get comparison data between OCR engines.
        """
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                base_query = """
                    SELECT 
                        olmocr_used,
                        AVG(processing_duration) as avg_duration,
                        AVG(page_count) as avg_pages,
                        COUNT(*) as file_count,
                        SUM(CASE WHEN processing_status = 'completed' THEN 1 ELSE 0 END) as success_count,
                        SUM(CASE WHEN processing_status = 'failed' THEN 1 ELSE 0 END) as failure_count
                    FROM processed_files
                    WHERE processed_on >= NOW() - INTERVAL '%s days'
                """
                
                if user_id:
                    query = base_query + " AND user_id = %s GROUP BY olmocr_used"
                    cursor.execute(query, (days, user_id))
                else:
                    query = base_query + " GROUP BY olmocr_used"
                    cursor.execute(query, (days,))
                
                rows = cursor.fetchall()
                
                result = {
                    'mineru': {
                        'avg_duration': 0,
                        'avg_pages': 0,
                        'file_count': 0,
                        'success_count': 0,
                        'failure_count': 0,
                        'success_rate': 0
                    },
                    'olmocr': {
                        'avg_duration': 0,
                        'avg_pages': 0,
                        'file_count': 0,
                        'success_count': 0,
                        'failure_count': 0,
                        'success_rate': 0
                    },
                    'period_days': days
                }
                
                for row in rows:
                    ocr_type = 'olmocr' if row['olmocr_used'] == 'Yes' else 'mineru'
                    result[ocr_type] = {
                        'avg_duration': round(float(row['avg_duration'] or 0), 2),
                        'avg_pages': round(float(row['avg_pages'] or 0), 2),
                        'file_count': int(row['file_count'] or 0),
                        'success_count': int(row['success_count'] or 0),
                        'failure_count': int(row['failure_count'] or 0),
                        'success_rate': round(
                            (int(row['success_count'] or 0) / int(row['file_count'] or 1) * 100), 
                            2
                        ) if row['file_count'] else 0
                    }
                
                return result
            
            except psycopg2.Error as e:
                logger.error(f"Database error retrieving OCR comparison data: {e}")
                return {
                    'error': str(e),
                    'mineru': {},
                    'olmocr': {}
                }
            
            finally:
                if conn:
                    self._put_connection(conn)
    
    def close(self):
        """Close the connection pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("PostgreSQL connection pool closed")


# ============================================================================
# UTILITY FUNCTIONS FOR OCR TRACKING
# ============================================================================

def format_missed_keys_for_storage(missed_keys: List[str]) -> str:
    """
    Convert list of missed keys to comma-separated string for storage
    
    Args:
        missed_keys: List of missing key strings
    
    Returns:
        Comma-separated string
    """
    if not missed_keys:
        return ""
    return ", ".join(missed_keys)


def parse_missed_keys_from_storage(missed_keys_str: Optional[str]) -> List[str]:
    """
    Parse comma-separated missed keys string to list
    
    Args:
        missed_keys_str: Comma-separated string from database
    
    Returns:
        List of key strings
    """
    if not missed_keys_str:
        return []
    return [k.strip() for k in missed_keys_str.split(',') if k.strip()]


def get_ocr_usage_summary(db_storage: DatabaseStorage, user_id: Optional[int] = None) -> str:
    """
    Get a human-readable summary of OCR usage
    
    Args:
        db_storage: DatabaseStorage instance
        user_id: Optional user ID to filter by
    
    Returns:
        Formatted summary string
    """
    stats = db_storage.get_olmocr_usage_stats(user_id=user_id, days=30)
    
    summary = f"""
OCR Usage Summary (Last 30 Days):
{'=' * 50}
Total Files Processed: {stats['total_files']}
MinerU Only: {stats['mineru_only_count']} ({100 - stats['olmocr_usage_percentage']:.1f}%)
OLM OCR Fallback: {stats['olmocr_used_count']} ({stats['olmocr_usage_percentage']:.1f}%)
Files with Missed Keys: {stats['files_with_missed_keys']} ({stats['missed_keys_percentage']:.1f}%)
{'=' * 50}
"""
    return summary


def log_ocr_decision(
    file_name: str,
    use_olmocr: bool,
    missed_keys: Optional[List[str]] = None,
    reason: str = ""
):
    """
    Log OCR engine decision for debugging.
    """
    if use_olmocr:
        logger.info("=" * 80)
        logger.info(f"OCR ENGINE DECISION: OLM OCR FALLBACK")
        logger.info("=" * 80)
        logger.info(f"File: {file_name}")
        logger.info(f"Reason: {reason}")
        if missed_keys:
            logger.info(f"Missing Keys: {', '.join(missed_keys)}")
        logger.info("=" * 80)
    else:
        logger.info(f"OCR ENGINE DECISION: MinerU (No fallback needed)")
        logger.info(f"File: {file_name}")


def create_ocr_tracking_report(
    db_storage: DatabaseStorage,
    user_id: Optional[int] = None,
    output_file: Optional[str] = None
) -> str:
    """
    Create a comprehensive OCR tracking report with statistics and performance data.
    """
    # Get statistics
    stats = db_storage.get_olmocr_usage_stats(user_id=user_id, days=30)
    comparison = db_storage.get_ocr_comparison_data(user_id=user_id, days=30)
    common_missed = db_storage.get_most_common_missed_keys(user_id=user_id, limit=10)
    
    # Build report
    report_lines = [
        "=" * 80,
        "OCR TRACKING REPORT",
        "=" * 80,
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Period: Last 30 days",
        f"User Filter: {'User ID ' + str(user_id) if user_id else 'All Users'}",
        "",
        "OVERALL STATISTICS:",
        "-" * 80,
        f"Total Files Processed: {stats['total_files']}",
        f"MinerU Only: {stats['mineru_only_count']} ({100 - stats['olmocr_usage_percentage']:.1f}%)",
        f"OLM OCR Fallback: {stats['olmocr_used_count']} ({stats['olmocr_usage_percentage']:.1f}%)",
        f"Files with Missed Keys: {stats['files_with_missed_keys']} ({stats['missed_keys_percentage']:.1f}%)",
        "",
        "PERFORMANCE COMPARISON:",
        "-" * 80,
        "MinerU:",
        f"  - Files: {comparison['mineru']['file_count']}",
        f"  - Avg Duration: {comparison['mineru']['avg_duration']}s",
        f"  - Avg Pages: {comparison['mineru']['avg_pages']}",
        f"  - Success Rate: {comparison['mineru']['success_rate']}%",
        "",
        "OLM OCR:",
        f"  - Files: {comparison['olmocr']['file_count']}",
        f"  - Avg Duration: {comparison['olmocr']['avg_duration']}s",
        f"  - Avg Pages: {comparison['olmocr']['avg_pages']}",
        f"  - Success Rate: {comparison['olmocr']['success_rate']}%",
        "",
        "MOST COMMONLY MISSED KEYS:",
        "-" * 80,
    ]
    
    if common_missed:
        for i, item in enumerate(common_missed, 1):
            report_lines.append(f"{i}. {item['key']}: {item['count']} occurrences")
    else:
        report_lines.append("No missed keys recorded")
    
    report_lines.extend([
        "",
        "=" * 80,
        "END OF REPORT",
        "=" * 80
    ])
    
    report = "\n".join(report_lines)
    
    # Save to file if requested
    if output_file:
        try:
            with open(output_file, 'w') as f:
                f.write(report)
            logger.info(f"? Report saved to: {output_file}")
        except Exception as e:
            logger.error(f"Failed to save report: {e}")
    
    return report