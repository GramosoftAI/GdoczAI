#!/usr/bin/env python3

"""
File-related endpoints for Document Processing API.

Provides:
- Get user processed files from database
- Get file details (markdown + json)
- Download stored files
- Cleanup old files
- File statistics
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from fastapi import HTTPException, Query, Depends, BackgroundTasks
from fastapi.responses import FileResponse, Response
from psycopg2.extras import RealDictCursor

# API SERVER IMPORTS ONLY - NO PIPELINE IMPORTS
from src.api.models.api_server_models import FileStatistics, get_db_connection, ConfigManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== HELPER FUNCTIONS ====================

def get_relative_time(timestamp):
    """Convert timestamp to relative time string"""
    if not timestamp:
        return "Unknown"
    
    now = datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    
    diff = now - timestamp
    seconds = diff.total_seconds()
    minutes = seconds / 60
    hours = minutes / 60
    days = hours / 24
    weeks = days / 7
    months = days / 30
    years = days / 365
    
    if seconds < 60:
        return "Just now"
    elif minutes < 60:
        m = int(minutes)
        return f"{m} minute{'s' if m != 1 else ''} ago"
    elif hours < 24:
        h = int(hours)
        return f"{h} hour{'s' if h != 1 else ''} ago"
    elif days < 7:
        d = int(days)
        return f"{d} day{'s' if d != 1 else ''} ago"
    elif weeks < 4:
        w = int(weeks)
        return f"{w} week{'s' if w != 1 else ''} ago"
    elif months < 12:
        m = int(months)
        return f"{m} month{'s' if m != 1 else ''} ago"
    else:
        y = int(years)
        return f"{y} year{'s' if y != 1 else ''} ago"

# ==================== FILE ENDPOINTS ====================

def create_file_routes(app, auth_manager, get_current_user):
    """Create file routes"""

    @app.get("/files/file-details/{file_id}", tags=["Files"])
    async def get_file_details_by_id(
        file_id: int,
        credentials = Depends(get_current_user)
    ):
        """
        Get markdown plus JSON content by file_id for current user.
        """
        conn = None
        cursor = None
        try:
            # Extract user_id from token
            user_id = credentials.get("user_id")

            if not user_id:
                raise HTTPException(
                    status_code=401, 
                    detail="Invalid or missing user authentication"
                )

            logger.info(f"?? Fetching file content: file_id={file_id}, user_id={user_id}")

            conn = get_db_connection()
            if not conn:
                raise HTTPException(
                    status_code=503, 
                    detail="Database connection failed"
                )

            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Query file from database
            cursor.execute(
                """
                SELECT 
                    file_id,
                    file_name,
                    markdown_output,
                    json_output,
                    file_path,
                    processing_status,
                    request_id,
                    created_on,
                    processed_on,
                    processing_duration,
                    page_count,
                    token_usage,
                    error_details
                FROM processed_files
                WHERE file_id = %s AND user_id = %s
                """,
                (file_id, user_id)
            )

            result = cursor.fetchone()

            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"File ID {file_id} not found OR access denied"
                )

            logger.info(f"? File found: {result['file_name']}")

            return {
                "success": True,
                "file_id": result['file_id'],
                "file_name": result['file_name'],
                "markdown": result['markdown_output'],
                "json": result['json_output'],
                "file_path": result.get('file_path'),
                "status": result['processing_status'],
                "request_id": result.get('request_id'),
                "created_on": result['created_on'].isoformat() if result['created_on'] else None,
                "processed_on": result['processed_on'].isoformat() if result['processed_on'] else None,
                "processing_duration": result['processing_duration'],
                "page_count": result['page_count'],
                "token_usage": result['token_usage'],
                "error_details": result['error_details']
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"? Error fetching file: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @app.get("/files/user-files", tags=["Files"])
    async def get_user_processed_files(
        user_id: Optional[int] = Query(None, description="User ID to fetch files for"),
        email: Optional[str] = Query(None, description="User email to fetch files for"),
        status: Optional[str] = Query(None, description="Filter by processing status"),
        limit: int = Query(50, ge=1, le=100, description="Maximum number of files to return"),
        offset: int = Query(0, ge=0, description="Offset for pagination")
    ):
        """
        Get processed files for a specific user.
        """
        conn = None
        cursor = None
        try:
            # Validate that at least one identifier is provided
            if not user_id and not email:
                raise HTTPException(
                    status_code=400,
                    detail="Either user_id or email must be provided"
                )
            
            # If email provided, get user_id from users table
            if email:
                if not auth_manager:
                    raise HTTPException(
                        status_code=503, 
                        detail="Authentication service not initialized"
                    )
                
                auth_conn = auth_manager._get_db_connection()
                auth_cursor = auth_conn.cursor()
                auth_cursor.execute(
                    "SELECT user_id FROM users WHERE email = %s",
                    (email.lower(),)
                )
                user_result = auth_cursor.fetchone()
                auth_cursor.close()
                auth_conn.close()
                
                if not user_result:
                    raise HTTPException(status_code=404, detail="User not found")
                
                user_id = user_result[0]
            
            elif user_id:
                # Verify user_id exists
                if not auth_manager:
                    raise HTTPException(
                        status_code=503, 
                        detail="Authentication service not initialized"
                    )
                
                user = auth_manager.get_user_by_id(user_id)
                if not user:
                    raise HTTPException(status_code=404, detail="User not found")
            
            logger.info(f"?? Fetching files for user_id={user_id}, status={status}")
            
            # Query files from database
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query with user_id filter
            query = """
                SELECT 
                    file_id,
                    file_name,
                    page_count,
                    processing_status,
                    created_on,
                    processed_on,
                    processing_duration,
                    token_usage,
                    unique_id,
                    error_details,
                    request_id,
                    user_id,
                    file_path
                FROM processed_files 
                WHERE user_id = %s
            """
            params = [user_id]
            
            if status:
                query += " AND processing_status = %s"
                params.append(status)
            
            query += " ORDER BY created_on DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            
            files = []
            for row in cursor.fetchall():
                files.append({
                    "id": row['file_id'],
                    "file_name": row['file_name'],
                    "pages": row['page_count'] if row['page_count'] else 0,
                    "status": row['processing_status'],
                    "created_at": get_relative_time(row['created_on']),
                    "created_on_absolute": row['created_on'].isoformat() if row['created_on'] else None,
                    "processed_on": row['processed_on'].isoformat() if row['processed_on'] else None,
                    "processing_duration": row['processing_duration'],
                    "token_usage": row['token_usage'],
                    "unique_id": row['unique_id'],
                    "error_details": row['error_details'],
                    "request_id": row.get('request_id'),
                    "user_id": row.get('user_id'),
                    "file_path": row.get('file_path')
                })
            
            logger.info(f"? Found {len(files)} files for user {user_id}")
            
            return {
                "success": True,
                "user_id": user_id,
                "files": files,
                "total": len(files),
                "limit": limit,
                "offset": offset,
                "filtered_by_status": status,
                "filtering": "user_based"
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"? Failed to fetch user files: {e}", exc_info=True)
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to fetch user files: {str(e)}"
            )
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @app.get("/files/processed", tags=["Files"])
    async def list_processed_files(
        status: Optional[str] = Query(None, description="Filter by processing status"),
        limit: int = Query(50, ge=1, le=100),
        offset: int = Query(0, ge=0),
        user = Depends(get_current_user)
    ):
        """
        ?? List all processed files (admin view)
        """
        conn = None
        cursor = None
        try:
            logger.info(f"?? Listing processed files: status={status}, limit={limit}, offset={offset}")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            query = "SELECT * FROM processed_files WHERE 1=1"
            params = []
            
            if status:
                query += " AND processing_status = %s"
                params.append(status)
            
            query += " ORDER BY updated_on DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            
            files = []
            for row in cursor.fetchall():
                # Truncate markdown for preview
                markdown_preview = None
                if row['markdown_output']:
                    markdown_preview = (
                        row['markdown_output'][:500] + "..." 
                        if len(row['markdown_output']) > 500 
                        else row['markdown_output']
                    )
                
                files.append({
                    "id": row['file_id'],
                    "file_name": row['file_name'],
                    "page_count": row['page_count'],
                    "processed_on": row['processed_on'].isoformat() if row['processed_on'] else None,
                    "processing_duration": row['processing_duration'],
                    "token_usage": row['token_usage'],
                    "unique_id": row['unique_id'],
                    "processing_status": row['processing_status'],
                    "created_on": row['created_on'].isoformat() if row['created_on'] else None,
                    "updated_on": row['updated_on'].isoformat() if row['updated_on'] else None,
                    "error_details": row['error_details'],
                    "json_output": row['json_output'],
                    "markdown_output_preview": markdown_preview,
                    "user_id": row.get('user_id'),
                    "request_id": row.get('request_id'),
                    "file_path": row.get('file_path')
                })
            
            logger.info(f"? Found {len(files)} files")
            
            return {
                "files": files,
                "total": len(files),
                "limit": limit,
                "offset": offset,
                "filtered_by_status": status
            }
        
        except Exception as e:
            logger.error(f"? Failed to list processed files: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @app.get("/files/download/{file_id}", tags=["Files"])
    async def download_stored_file(
        file_id: int,
        format: str = Query("markdown", description="Download format: markdown, json, or pdf"),
        user = Depends(get_current_user)
    ):
        """
        ?? Download file content by file_id
        """
        conn = None
        cursor = None
        try:
            user_id = user.get("user_id")
            
            logger.info(f"?? Download request: file_id={file_id}, format={format}, user_id={user_id}")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Query file from database
            cursor.execute(
                """
                SELECT 
                    file_name,
                    markdown_output,
                    json_output,
                    file_path,
                    user_id
                FROM processed_files
                WHERE file_id = %s
                """,
                (file_id,)
            )
            
            result = cursor.fetchone()
            
            if not result:
                raise HTTPException(status_code=404, detail="File not found")
            
            # Verify user access
            if result['user_id'] != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            
            file_name = result['file_name']
            base_name = Path(file_name).stem
            
            # Handle different download formats
            if format == "markdown":
                if not result['markdown_output']:
                    raise HTTPException(
                        status_code=404, 
                        detail="Markdown output not available"
                    )
                
                content = result['markdown_output']
                filename = f"{base_name}.md"
                media_type = "text/markdown"
                
                return Response(
                    content=content,
                    media_type=media_type,
                    headers={
                        "Content-Disposition": f"attachment; filename={filename}"
                    }
                )
            
            elif format == "json":
                if not result['json_output']:
                    raise HTTPException(
                        status_code=404, 
                        detail="JSON output not available"
                    )
                
                content = json.dumps(result['json_output'], indent=2)
                filename = f"{base_name}.json"
                media_type = "application/json"
                
                return Response(
                    content=content,
                    media_type=media_type,
                    headers={
                        "Content-Disposition": f"attachment; filename={filename}"
                    }
                )
            
            elif format == "pdf":
                file_path = result.get('file_path')
                
                if not file_path:
                    raise HTTPException(
                        status_code=404, 
                        detail="Original PDF not stored"
                    )
                
                # Construct full path from storage config
                config_manager = ConfigManager("config/config.yaml")
                base_path = config_manager.get('storage.local_storage.base_path', './stored_pdfs/')
                full_path = Path(base_path) / file_path.lstrip('/')
                
                if not full_path.exists():
                    raise HTTPException(
                        status_code=404, 
                        detail="PDF file not found in storage"
                    )
                
                return FileResponse(
                    path=str(full_path),
                    filename=file_name,
                    media_type='application/pdf'
                )
            
            else:
                raise HTTPException(
                    status_code=400, 
                    detail="Invalid format. Use: markdown, json, or pdf"
                )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"? Failed to download file: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @app.get("/files/statistics", response_model=FileStatistics, tags=["Files"])
    async def get_file_statistics(user = Depends(get_current_user)):
        """
        ?? Get file processing statistics from database
        """
        try:
            logger.info("?? Fetching file statistics...")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get statistics from database
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_files,
                    SUM(CASE WHEN processing_status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN processing_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN processing_status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN processing_status = 'processing' THEN 1 ELSE 0 END) as processing,
                    AVG(processing_duration) as avg_duration
                FROM processed_files
            """)
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            total = result['total_files'] or 0
            completed = result['completed'] or 0
            failed = result['failed'] or 0
            pending = result['pending'] or 0
            processing = result['processing'] or 0
            avg_duration = result['avg_duration'] or 0.0
            
            # Calculate success rate
            success_rate = (completed / total * 100) if total > 0 else 0
            
            logger.info(f"? Statistics retrieved: total={total}, completed={completed}, failed={failed}")
            
            return FileStatistics(
                total_files=total,
                completed=completed,
                failed=failed,
                pending=pending,
                processing=processing,
                average_processing_time_minutes=avg_duration,
                success_rate_percent=round(success_rate, 2)
            )
        
        except Exception as e:
            logger.error(f"? Failed to get file statistics: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @app.delete("/files/cleanup", tags=["Maintenance"])
    async def cleanup_old_files(
        days_old: int = Query(7, ge=1, description="Delete files older than this many days"),
        user = Depends(get_current_user)
    ):
        """
        ??? Clean up old temporary files
        """
        try:
            logger.info(f"??? Cleaning up files older than {days_old} days...")
            
            cutoff_date = datetime.now() - timedelta(days=days_old)
            deleted_count = 0
            
            # Clean temp_uploads
            temp_dir = Path("temp_uploads")
            if temp_dir.exists():
                for file_path in temp_dir.iterdir():
                    if file_path.is_file():
                        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_time < cutoff_date:
                            file_path.unlink()
                            deleted_count += 1
                            logger.debug(f"   Deleted: {file_path.name}")
            
            # Clean temp_jobs
            jobs_dir = Path("temp_jobs")
            if jobs_dir.exists():
                for file_path in jobs_dir.iterdir():
                    if file_path.is_file():
                        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_time < cutoff_date:
                            file_path.unlink()
                            deleted_count += 1
                            logger.debug(f"   Deleted: {file_path.name}")
            
            logger.info(f"? Cleaned up {deleted_count} old files")
            
            return {
                "message": f"Cleaned up {deleted_count} old temporary files",
                "cutoff_date": cutoff_date.isoformat(),
                "deleted_count": deleted_count,
                "note": "Only temporary files deleted. Stored files and database records preserved."
            }
        
        except Exception as e:
            logger.error(f"? Failed to cleanup files: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))