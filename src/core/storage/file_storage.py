#!/usr/bin/env python3
"""
Filesystem-based Storage Utility (No Database)

Replaces database storage with JSON file-based storage.
All processed file metadata stored in JSON files.
"""

import json
import logging
import threading
from datetime import datetime
from typing import Dict, Optional, Any, List
from pathlib import Path
import hashlib

logger = logging.getLogger(__name__)


class ProcessingStatus:
    """Processing status constants"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class FileStorage:
    """File-based storage for OCR results (replaces database)"""
    
    def __init__(self, storage_dir: str = "./file_storage"):
        """
        Initialize file-based storage
        
        Args:
            storage_dir: Directory to store metadata JSON files
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        
        # Create subdirectories
        (self.storage_dir / "metadata").mkdir(exist_ok=True)
        (self.storage_dir / "users").mkdir(exist_ok=True)
        
        logger.info(f"File storage initialized at: {self.storage_dir}")
    
    def _generate_file_id(self, filename: str) -> str:
        """Generate unique file ID from filename"""
        return hashlib.md5(filename.encode()).hexdigest()[:16]
    
    def _get_metadata_path(self, file_id: str) -> Path:
        """Get path to metadata file"""
        return self.storage_dir / "metadata" / f"{file_id}.json"
    
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
        user_id: Optional[int] = None
    ) -> bool:
        """
        Store OCR processing result in filesystem
        
        Args:
            file_name: Name of the processed file
            markdown_output: Markdown content from OCR
            json_output: JSON extracted data
            page_count: Number of pages processed
            processing_duration: Time taken to process (in seconds)
            token_usage: Number of tokens used
            unique_id: Optional unique identifier for the file
            error_details: Optional error details if processing failed
            request_id: Unique identifier for this processing request
            user_id: User ID (stored but not used for filtering)
        
        Returns:
            True if storage was successful, False otherwise
        """
        with self.lock:
            try:
                file_id = self._generate_file_id(file_name)
                metadata_path = self._get_metadata_path(file_id)
                
                now = datetime.now()
                status = ProcessingStatus.COMPLETED if not error_details else ProcessingStatus.FAILED
                
                metadata = {
                    "file_id": file_id,
                    "file_name": file_name,
                    "page_count": page_count,
                    "processed_on": now.isoformat(),
                    "processing_duration": processing_duration,
                    "json_output": json_output,
                    "markdown_output": markdown_output,
                    "token_usage": token_usage,
                    "error_details": error_details,
                    "unique_id": unique_id,
                    "processing_status": status,
                    "created_on": now.isoformat(),
                    "updated_on": now.isoformat(),
                    "request_id": request_id,
                    "user_id": user_id
                }
                
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                logger.info(f"âœ… Stored metadata for {file_name} (request_id: {request_id})")
                return True
                
            except Exception as e:
                logger.error(f"âŒ Error storing metadata: {e}")
                return False
    
    def update_processing_status(
        self,
        file_name: str,
        status: str,
        error_details: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> bool:
        """
        Update only the processing status of a file
        
        Args:
            file_name: Name of the file
            status: New processing status
            error_details: Optional error details
            request_id: Optional request ID for tracking
        
        Returns:
            True if update was successful, False otherwise
        """
        with self.lock:
            try:
                file_id = self._generate_file_id(file_name)
                metadata_path = self._get_metadata_path(file_id)
                
                if not metadata_path.exists():
                    # Create new metadata file
                    metadata = {
                        "file_id": file_id,
                        "file_name": file_name,
                        "processing_status": status,
                        "error_details": error_details,
                        "request_id": request_id,
                        "created_on": datetime.now().isoformat(),
                        "updated_on": datetime.now().isoformat()
                    }
                else:
                    # Update existing metadata
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    metadata["processing_status"] = status
                    metadata["updated_on"] = datetime.now().isoformat()
                    
                    if error_details:
                        metadata["error_details"] = error_details
                    if request_id:
                        metadata["request_id"] = request_id
                
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                return True
                
            except Exception as e:
                logger.error(f"âŒ Error updating status: {e}")
                return False
    
    def get_file_record(self, file_name: str = None, request_id: str = None) -> Optional[Dict]:
        """
        Retrieve a file record from storage by file_name or request_id
        
        Args:
            file_name: Name of the file (optional)
            request_id: Request ID (optional)
        
        Returns:
            Dictionary with file record data or None if not found
        """
        with self.lock:
            try:
                if file_name:
                    file_id = self._generate_file_id(file_name)
                    metadata_path = self._get_metadata_path(file_id)
                    
                    if metadata_path.exists():
                        with open(metadata_path, 'r', encoding='utf-8') as f:
                            return json.load(f)
                
                elif request_id:
                    # Search all metadata files for matching request_id
                    metadata_dir = self.storage_dir / "metadata"
                    for metadata_file in metadata_dir.glob("*.json"):
                        try:
                            with open(metadata_file, 'r', encoding='utf-8') as f:
                                metadata = json.load(f)
                                if metadata.get("request_id") == request_id:
                                    return metadata
                        except:
                            continue
                
                return None
                
            except Exception as e:
                logger.error(f"âŒ Error retrieving file record: {e}")
                return None
    
    def list_all_files(self, status: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """
        List all processed files
        
        Args:
            status: Filter by status (optional)
            limit: Maximum number of results
        
        Returns:
            List of file metadata dictionaries
        """
        with self.lock:
            try:
                metadata_dir = self.storage_dir / "metadata"
                files = []
                
                for metadata_file in sorted(
                    metadata_dir.glob("*.json"),
                    key=lambda x: x.stat().st_mtime,
                    reverse=True
                ):
                    try:
                        with open(metadata_file, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                        
                        if status and metadata.get("processing_status") != status:
                            continue
                        
                        files.append(metadata)
                        
                        if len(files) >= limit:
                            break
                    except:
                        continue
                
                return files
                
            except Exception as e:
                logger.error(f"âŒ Error listing files: {e}")
                return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        with self.lock:
            try:
                metadata_dir = self.storage_dir / "metadata"
                
                stats = {
                    "total_files": 0,
                    "completed": 0,
                    "failed": 0,
                    "processing": 0,
                    "pending": 0,
                    "total_tokens": 0,
                    "total_pages": 0
                }
                
                for metadata_file in metadata_dir.glob("*.json"):
                    try:
                        with open(metadata_file, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                        
                        stats["total_files"] += 1
                        
                        status = metadata.get("processing_status", "")
                        if status == ProcessingStatus.COMPLETED:
                            stats["completed"] += 1
                        elif status == ProcessingStatus.FAILED:
                            stats["failed"] += 1
                        elif status == ProcessingStatus.PROCESSING:
                            stats["processing"] += 1
                        elif status == ProcessingStatus.PENDING:
                            stats["pending"] += 1
                        
                        stats["total_tokens"] += metadata.get("token_usage", 0)
                        stats["total_pages"] += metadata.get("page_count", 0)
                    except:
                        continue
                
                return stats
                
            except Exception as e:
                logger.error(f"âŒ Error getting statistics: {e}")
                return {}
    
    def close(self):
        """Cleanup (no-op for file storage)"""
        logger.info("File storage closed")
