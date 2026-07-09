# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class TaskStatus(Enum):
    """Status of a PDF processing task"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    MOVED = "moved"
    MOVED_TO_FAILED = "moved_to_failed"  #  File moved to Failed_folder


class OCRResult(Enum):
    """Result of OCR processing"""
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    INVALID_RESPONSE = "invalid_response"


@dataclass
class TokenState:

    access_token: str
    expires_at: datetime
    refresh_count: int = 0
    last_refreshed: Optional[datetime] = None
    
    def is_valid(self) -> bool:
        return datetime.now() < self.expires_at
    
    def is_expired(self) -> bool:
        return not self.is_valid()
    
    def time_until_expiry(self) -> float:
        delta = self.expires_at - datetime.now()
        return max(0, delta.total_seconds())
    
    def should_refresh(self, buffer_hours: int = 4) -> bool:
        buffer_seconds = buffer_hours * 3600
        return self.time_until_expiry() < buffer_seconds


@dataclass
class PDFTask:

    file_path: str  # Full SFTP path to the PDF
    filename: str  # Original filename (e.g., "toyota_8.pdf")
    folder_name: str  # Source folder (e.g., "folder_1") - for reference only, NOT for document_type
    document_type: Optional[str] = None  #  CHANGED: Now Optional, detected from PDF text content
    file_size_bytes: int = 0
    detected_at: datetime = field(default_factory=datetime.now)
    status: TaskStatus = TaskStatus.PENDING
    
    # Processing tracking
    processing_started_at: Optional[datetime] = None
    processing_completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    
    # OCR result tracking
    request_id: Optional[str] = None
    ocr_result: Optional[OCRResult] = None
    
    #  NEW: PDF text extraction tracking
    extracted_text: Optional[str] = None  # First N pages of extracted text
    text_extraction_method: Optional[str] = None  # "pymupdf" or "pdfplumber"
    detection_keywords_found: Optional[List[str]] = None  # Keywords that matched for document_type
    
    #  Track final filename after move (may differ if UUID was added)
    moved_filename: Optional[str] = None  # e.g., "toyota_8_a1b2c3d4.pdf" if renamed
    
    #  Track if file was moved to failed folder
    moved_to_failed: bool = False
    
    def mark_processing(self):
        self.status = TaskStatus.PROCESSING
        self.processing_started_at = datetime.now()
    
    def mark_completed(self, request_id: str):
        self.status = TaskStatus.COMPLETED
        self.processing_completed_at = datetime.now()
        self.request_id = request_id
        self.ocr_result = OCRResult.SUCCESS
    
    def mark_failed(self, error: str):
        self.status = TaskStatus.FAILED
        self.processing_completed_at = datetime.now()
        self.error_message = error
        self.ocr_result = OCRResult.FAILED
    
    def mark_moved(self):
        self.status = TaskStatus.MOVED
    
    def mark_moved_to_failed_folder(self):
        self.status = TaskStatus.MOVED_TO_FAILED
        self.moved_to_failed = True
    
    def increment_retry(self):
        self.retry_count += 1
    
    def set_document_type(self, document_type: str, keywords_found: Optional[List[str]] = None):

        self.document_type = document_type
        if keywords_found:
            self.detection_keywords_found = keywords_found
    
    def set_extracted_text(self, text: str, method: str):
        self.extracted_text = text
        self.text_extraction_method = method
    
    def has_document_type(self) -> bool:
        return self.document_type is not None
    
    def was_renamed(self) -> bool:
        return (
            self.moved_filename is not None and 
            self.moved_filename != self.filename
        )
    
    def get_final_filename(self) -> str:

        if self.moved_filename:
            return self.moved_filename
        return self.filename
    
    def get_processing_duration(self) -> Optional[float]:
        if self.processing_started_at and self.processing_completed_at:
            delta = self.processing_completed_at - self.processing_started_at
            return delta.total_seconds()
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        task_dict = {
            "filename": self.filename,
            "folder_name": self.folder_name,  # For reference only
            "document_type": self.document_type,  #  Now detected from PDF text
            "status": self.status.value,
            "file_size_bytes": self.file_size_bytes,
            "retry_count": self.retry_count,
            "request_id": self.request_id,
            "error_message": self.error_message,
            "moved_to_failed": self.moved_to_failed
        }
        
        #  Include text extraction details
        if self.text_extraction_method:
            task_dict["text_extraction_method"] = self.text_extraction_method
        if self.detection_keywords_found:
            task_dict["detection_keywords_found"] = self.detection_keywords_found
        
        #  Include moved filename if file was renamed
        if self.was_renamed():
            task_dict["moved_filename"] = self.moved_filename
            task_dict["was_renamed"] = True
        
        return task_dict

@dataclass
class OCRRequest:

    file_bytes: bytes
    filename: str
    document_type: str  #  Now comes from keyword detection, not folder name
    authorization_token: str
    schema_json: Optional[str] = None
    
    def get_file_size_mb(self) -> float:
        """Get file size in MB"""
        return len(self.file_bytes) / (1024 * 1024)


@dataclass
class OCRResponse:

    success: bool
    request_id: Optional[str] = None
    markdown: Optional[str] = None
    json_output: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    
    def is_successful(self) -> bool:
        """Check if OCR was successful"""
        return self.success and self.request_id is not None
    
    def has_error(self) -> bool:
        """Check if response contains an error"""
        return not self.success or self.error is not None
    
    def get_error_message(self) -> str:
        """Get error message or default"""
        return self.error or "Unknown error occurred"


@dataclass
class SFTPFile:

    file_path: str  # Full path on SFTP
    filename: str  # Just filename
    folder_path: str  # Parent folder path
    folder_name: str  # Folder name (e.g., "folder_1") - for reference only
    size_bytes: int
    modified_time: Optional[datetime] = None
    
    def is_pdf(self) -> bool:
        """Check if file is a PDF"""
        return self.filename.lower().endswith('.pdf')
    
    def to_pdf_task(self) -> PDFTask:

        return PDFTask(
            file_path=self.file_path,
            filename=self.filename,
            folder_name=self.folder_name,  # For reference only
            document_type=None,  #  CHANGED: No longer using folder_name for document_type
            file_size_bytes=self.size_bytes
        )


@dataclass
class PipelineStats:

    total_scans: int = 0
    total_pdfs_detected: int = 0
    total_pdfs_processed: int = 0
    total_pdfs_failed: int = 0
    total_pdfs_moved: int = 0
    
    total_token_refreshes: int = 0
    total_api_errors: int = 0
    total_sftp_errors: int = 0
    
    #  Track files that were renamed with UUID
    total_files_renamed_with_uuid: int = 0
    
    #  Track failed file movements
    total_pdfs_moved_to_failed: int = 0
    
    #  NEW: Track text extraction and detection statistics
    total_text_extractions_success: int = 0
    total_text_extractions_failed: int = 0
    total_document_type_detections_success: int = 0
    total_document_type_detections_failed: int = 0
    
    #  NEW: Track which extraction methods were used
    total_pymupdf_extractions: int = 0
    total_pdfplumber_extractions: int = 0
    
    last_scan_time: Optional[datetime] = None
    last_successful_process: Optional[datetime] = None
    last_token_refresh: Optional[datetime] = None
    
    pipeline_started_at: datetime = field(default_factory=datetime.now)
    
    def record_scan(self, pdf_count: int):
        self.total_scans += 1
        self.total_pdfs_detected += pdf_count
        self.last_scan_time = datetime.now()
    
    def record_success(self):
        self.total_pdfs_processed += 1
        self.last_successful_process = datetime.now()
    
    def record_failure(self):
        self.total_pdfs_failed += 1
    
    def record_moved(self):
        self.total_pdfs_moved += 1
    
    def record_moved_to_failed(self):
        self.total_pdfs_moved_to_failed += 1
    
    def record_uuid_rename(self):
        self.total_files_renamed_with_uuid += 1
    
    def record_text_extraction_success(self, method: str):

        self.total_text_extractions_success += 1
        if method.lower() == "pymupdf":
            self.total_pymupdf_extractions += 1
        elif method.lower() == "pdfplumber":
            self.total_pdfplumber_extractions += 1
    
    def record_text_extraction_failure(self):
        self.total_text_extractions_failed += 1
    
    def record_document_type_detection_success(self):
        self.total_document_type_detections_success += 1
    
    def record_document_type_detection_failure(self):
        self.total_document_type_detections_failed += 1
    
    def record_token_refresh(self):
        self.total_token_refreshes += 1
        self.last_token_refresh = datetime.now()
    
    def record_api_error(self):
        self.total_api_errors += 1
    
    def record_sftp_error(self):
        self.total_sftp_errors += 1
    
    def get_success_rate(self) -> float:
        total = self.total_pdfs_processed + self.total_pdfs_failed
        if total == 0:
            return 0.0
        return (self.total_pdfs_processed / total) * 100
    
    def get_failure_rate(self) -> float:
        total = self.total_pdfs_processed + self.total_pdfs_failed
        if total == 0:
            return 0.0
        return (self.total_pdfs_failed / total) * 100
    
    def get_text_extraction_success_rate(self) -> float:

        total = self.total_text_extractions_success + self.total_text_extractions_failed
        if total == 0:
            return 0.0
        return (self.total_text_extractions_success / total) * 100
    
    def get_document_type_detection_success_rate(self) -> float:

        total = self.total_document_type_detections_success + self.total_document_type_detections_failed
        if total == 0:
            return 0.0
        return (self.total_document_type_detections_success / total) * 100
    
    def get_uuid_rename_rate(self) -> float:

        total_moved = self.total_pdfs_moved + self.total_pdfs_moved_to_failed
        if total_moved == 0:
            return 0.0
        return (self.total_files_renamed_with_uuid / total_moved) * 100
    
    def get_uptime_hours(self) -> float:
        delta = datetime.now() - self.pipeline_started_at
        return delta.total_seconds() / 3600
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary"""
        stats_dict = {
            "total_scans": self.total_scans,
            "total_pdfs_detected": self.total_pdfs_detected,
            "total_pdfs_processed": self.total_pdfs_processed,
            "total_pdfs_failed": self.total_pdfs_failed,
            "total_pdfs_moved": self.total_pdfs_moved,
            "total_pdfs_moved_to_failed": self.total_pdfs_moved_to_failed,
            "success_rate_percent": round(self.get_success_rate(), 2),
            "failure_rate_percent": round(self.get_failure_rate(), 2),
            "total_token_refreshes": self.total_token_refreshes,
            "total_api_errors": self.total_api_errors,
            "total_sftp_errors": self.total_sftp_errors,
            "uptime_hours": round(self.get_uptime_hours(), 2),
            "last_scan": self.last_scan_time.isoformat() if self.last_scan_time else None,
            "last_success": self.last_successful_process.isoformat() if self.last_successful_process else None
        }
        
        #  Add UUID rename statistics
        stats_dict["total_files_renamed_with_uuid"] = self.total_files_renamed_with_uuid
        stats_dict["uuid_rename_rate_percent"] = round(self.get_uuid_rename_rate(), 2)
        
        #  NEW: Add text extraction and detection statistics
        stats_dict["total_text_extractions_success"] = self.total_text_extractions_success
        stats_dict["total_text_extractions_failed"] = self.total_text_extractions_failed
        stats_dict["text_extraction_success_rate_percent"] = round(self.get_text_extraction_success_rate(), 2)
        
        stats_dict["total_document_type_detections_success"] = self.total_document_type_detections_success
        stats_dict["total_document_type_detections_failed"] = self.total_document_type_detections_failed
        stats_dict["document_type_detection_success_rate_percent"] = round(self.get_document_type_detection_success_rate(), 2)
        
        stats_dict["total_pymupdf_extractions"] = self.total_pymupdf_extractions
        stats_dict["total_pdfplumber_extractions"] = self.total_pdfplumber_extractions
        
        return stats_dict


@dataclass
class AuthCredentials:

    username: str
    password: str
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for API request"""
        return {
            "username": self.username,
            "password": self.password
        }


@dataclass
class PipelineState:

    token_state: Optional[TokenState] = None
    current_tasks: List[PDFTask] = field(default_factory=list)
    stats: PipelineStats = field(default_factory=PipelineStats)
    is_running: bool = False
    
    def add_task(self, task: PDFTask):
        """Add a new task to current tasks"""
        self.current_tasks.append(task)
    
    def remove_task(self, task: PDFTask):
        """Remove a task from current tasks"""
        if task in self.current_tasks:
            self.current_tasks.remove(task)
    
    def get_pending_tasks(self) -> List[PDFTask]:
        """Get all pending tasks"""
        return [t for t in self.current_tasks if t.status == TaskStatus.PENDING]
    
    def get_processing_tasks(self) -> List[PDFTask]:
        """Get all processing tasks"""
        return [t for t in self.current_tasks if t.status == TaskStatus.PROCESSING]
    
    def get_renamed_tasks(self) -> List[PDFTask]:
        """ Get all tasks that were renamed with UUID"""
        return [t for t in self.current_tasks if t.was_renamed()]
    
    def get_failed_tasks(self) -> List[PDFTask]:
        """ Get all tasks that failed"""
        return [t for t in self.current_tasks if t.status == TaskStatus.FAILED]
    
    def get_moved_to_failed_tasks(self) -> List[PDFTask]:
        """ Get all tasks moved to Failed_folder"""
        return [t for t in self.current_tasks if t.status == TaskStatus.MOVED_TO_FAILED]
    
    def get_tasks_without_document_type(self) -> List[PDFTask]:
        """
         NEW: Get all tasks where document type hasn't been detected yet
        
        Returns:
            List[PDFTask]: Tasks with document_type = None
        """
        return [t for t in self.current_tasks if not t.has_document_type()]
    
    def clear_completed_tasks(self):
        """Remove completed and moved tasks"""
        self.current_tasks = [
            t for t in self.current_tasks 
            if t.status not in [TaskStatus.COMPLETED, TaskStatus.MOVED, TaskStatus.MOVED_TO_FAILED]
        ]
    
    def has_valid_token(self) -> bool:
        """Check if pipeline has a valid token"""
        return self.token_state is not None and self.token_state.is_valid()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary"""
        state_dict = {
            "is_running": self.is_running,
            "has_valid_token": self.has_valid_token(),
            "token_expires_in_hours": round(self.token_state.time_until_expiry() / 3600, 2) if self.token_state else None,
            "pending_tasks": len(self.get_pending_tasks()),
            "processing_tasks": len(self.get_processing_tasks()),
            "stats": self.stats.to_dict()
        }
        
        #  Add renamed tasks count
        state_dict["renamed_tasks"] = len(self.get_renamed_tasks())
        
        #  Add failed tasks counts
        state_dict["failed_tasks"] = len(self.get_failed_tasks())
        state_dict["moved_to_failed_tasks"] = len(self.get_moved_to_failed_tasks())
        
        #  NEW: Add tasks without document type
        state_dict["tasks_without_document_type"] = len(self.get_tasks_without_document_type())
        
        return state_dict