# -*- coding: utf-8 -*-


from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

class TaskStatus(Enum):
    PENDING          = "pending"
    DOWNLOADING      = "downloading"     # Being fetched from IMAP (replaces SFTP download)
    PROCESSING       = "processing"      # Sent to OCR endpoint
    COMPLETED        = "completed"       # OCR returned success
    FAILED           = "failed"          # OCR or processing error
    MOVED            = "moved"           # Moved to processed_dir after success
    MOVED_TO_FAILED  = "moved_to_failed" # Moved to failed_dir after failure
    DUPLICATE        = "duplicate"       # Skipped  SHA-256 matched an existing file
    SENDER_REJECTED  = "sender_rejected" # Skipped  sender not in approved list

class OCRResult(Enum):
    SUCCESS          = "success"
    FAILED           = "failed"
    TIMEOUT          = "timeout"
    INVALID_RESPONSE = "invalid_response"

class IMAPError(Enum):
    LOGIN_FAILED      = "login_failed"
    CONNECTION_FAILED = "connection_failed"
    SEARCH_FAILED     = "search_failed"
    FETCH_FAILED      = "fetch_failed"
    MARK_SEEN_FAILED  = "mark_seen_failed"

@dataclass
class TokenState:
    access_token: str
    expires_at: datetime
    refresh_count: int = 0
    last_refreshed: Optional[datetime] = None

    def is_valid(self) -> bool:
        """Return True if the token has not yet expired"""
        return datetime.now() < self.expires_at

    def is_expired(self) -> bool:
        """Return True if the token has expired"""
        return not self.is_valid()

    def time_until_expiry(self) -> float:
        """Return remaining seconds until token expires (minimum 0)"""
        delta = self.expires_at - datetime.now()
        return max(0.0, delta.total_seconds())

    def should_refresh(self, buffer_hours: int = 4) -> bool:

        return self.time_until_expiry() < (buffer_hours * 3600)

@dataclass
class InboxEmail:
    imap_uid: str               # IMAP UID of the message (used to fetch + mark seen)
    subject: str                # Email subject line
    sender: str                 # Raw From header value (e.g. "Name <addr@example.com>")
    sender_address: str         # Extracted email address in lowercase
    received_at: Optional[datetime] = None
    message_id: Optional[str] = None   # RFC 2822 Message-ID header
    attachment_count: int = 0          # Total PDF attachments found in this email

    def is_approved_sender(self, approved_senders_lower: List[str]) -> bool:

        return self.sender_address.lower() in approved_senders_lower

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            "imap_uid": self.imap_uid,
            "subject": self.subject,
            "sender": self.sender,
            "sender_address": self.sender_address,
            "received_at": self.received_at.isoformat() if self.received_at else None,
            "message_id": self.message_id,
            "attachment_count": self.attachment_count,
        }

@dataclass
class EmailAttachmentTask:
    # --- Source identification (replaces file_path / folder_name) ---
    imap_uid: str               # IMAP UID of the parent email
    attachment_filename: str    # Original filename from the email attachment
    sender_address: str         # Sender email address (lowercase)
    email_subject: str          # Subject of the parent email

    # --- File content tracking ---
    sha256_hash: Optional[str] = None       # SHA-256 of file bytes (for dedup)
    file_size_bytes: int = 0
    local_path: Optional[str] = None        # Full local path after download
    moved_path: Optional[str] = None        # Full path after move to processed/failed dir

    # --- Timestamps ---
    detected_at: datetime = field(default_factory=datetime.now)
    download_started_at: Optional[datetime] = None
    download_completed_at: Optional[datetime] = None
    processing_started_at: Optional[datetime] = None
    processing_completed_at: Optional[datetime] = None

    # --- Status ---
    status: TaskStatus = TaskStatus.PENDING
    error_message: Optional[str] = None
    retry_count: int = 0

    # --- OCR result tracking ---
    request_id: Optional[str] = None
    ocr_result: Optional[OCRResult] = None

    # --- Document type detection (same pattern as PDFTask) ---
    document_type: Optional[str] = None
    extracted_text: Optional[str] = None           # First N pages of extracted text
    text_extraction_method: Optional[str] = None   # "pymupdf" or "pdfplumber"
    detection_keywords_found: Optional[List[str]] = None
    moved_to_failed: bool = False

    def mark_downloading(self):
        """Mark task as currently downloading from IMAP"""
        self.status = TaskStatus.DOWNLOADING
        self.download_started_at = datetime.now()

    def mark_download_complete(self, local_path: str, sha256_hash: str, file_size_bytes: int):

        self.local_path = local_path
        self.sha256_hash = sha256_hash
        self.file_size_bytes = file_size_bytes
        self.download_completed_at = datetime.now()

    def mark_processing(self):
        """Mark task as currently being sent to OCR"""
        self.status = TaskStatus.PROCESSING
        self.processing_started_at = datetime.now()

    def mark_completed(self, request_id: str):
        """Mark task as successfully processed by OCR"""
        self.status = TaskStatus.COMPLETED
        self.processing_completed_at = datetime.now()
        self.request_id = request_id
        self.ocr_result = OCRResult.SUCCESS

    def mark_failed(self, error: str):
        """Mark task as failed during OCR or download"""
        self.status = TaskStatus.FAILED
        self.processing_completed_at = datetime.now()
        self.error_message = error
        self.ocr_result = OCRResult.FAILED

    def mark_moved(self, moved_path: str):

        self.status = TaskStatus.MOVED
        self.moved_path = moved_path

    def mark_moved_to_failed(self, moved_path: str):

        self.status = TaskStatus.MOVED_TO_FAILED
        self.moved_to_failed = True
        self.moved_path = moved_path

    def mark_duplicate(self):
        """Mark task as duplicate  SHA-256 matched an existing local file"""
        self.status = TaskStatus.DUPLICATE

    def mark_sender_rejected(self):
        """Mark task as rejected  sender not in approved list"""
        self.status = TaskStatus.SENDER_REJECTED

    def increment_retry(self):
        """Increment retry counter"""
        self.retry_count += 1
    # ----------------------------------------------------------------
    # Document type detection helpers  (identical to PDFTask)
    # ----------------------------------------------------------------
    def set_document_type(self, document_type: str, keywords_found: Optional[List[str]] = None):

        self.document_type = document_type
        if keywords_found:
            self.detection_keywords_found = keywords_found

    def set_extracted_text(self, text: str, method: str):
        self.extracted_text = text
        self.text_extraction_method = method

    def has_document_type(self) -> bool:
        """Return True if document_type has been detected"""
        return self.document_type is not None

    def get_download_duration(self) -> Optional[float]:
        """Return download duration in seconds, or None if not complete"""
        if self.download_started_at and self.download_completed_at:
            return (self.download_completed_at - self.download_started_at).total_seconds()
        return None

    def get_processing_duration(self) -> Optional[float]:
        """Return OCR processing duration in seconds, or None if not complete"""
        if self.processing_started_at and self.processing_completed_at:
            return (self.processing_completed_at - self.processing_started_at).total_seconds()
        return None

    def get_total_duration(self) -> Optional[float]:
        """Return total wall-clock duration from detection to completion"""
        if self.processing_completed_at:
            return (self.processing_completed_at - self.detected_at).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary for logging and status reporting"""
        task_dict = {
            "imap_uid": self.imap_uid,
            "attachment_filename": self.attachment_filename,
            "sender_address": self.sender_address,
            "email_subject": self.email_subject,
            "sha256_hash": self.sha256_hash,
            "file_size_bytes": self.file_size_bytes,
            "local_path": self.local_path,
            "moved_path": self.moved_path,
            "status": self.status.value,
            "document_type": self.document_type,
            "retry_count": self.retry_count,
            "request_id": self.request_id,
            "ocr_result": self.ocr_result.value if self.ocr_result else None,
            "error_message": self.error_message,
            "moved_to_failed": self.moved_to_failed,
            "download_duration_seconds": self.get_download_duration(),
            "processing_duration_seconds": self.get_processing_duration(),
            "total_duration_seconds": self.get_total_duration(),
        }

        if self.text_extraction_method:
            task_dict["text_extraction_method"] = self.text_extraction_method
        if self.detection_keywords_found:
            task_dict["detection_keywords_found"] = self.detection_keywords_found

        return task_dict

@dataclass
class OCRRequest:

    file_bytes: bytes
    filename: str
    document_type: str          # Detected from PDF text content
    authorization_token: str
    schema_json: Optional[str] = None

    def get_file_size_mb(self) -> float:
        """Return file size in megabytes"""
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
        """Return True if OCR succeeded and a request_id was returned"""
        return self.success and self.request_id is not None

    def has_error(self) -> bool:
        """Return True if the response indicates an error"""
        return not self.success or self.error is not None

    def get_error_message(self) -> str:
        """Return the error string or a generic fallback"""
        return self.error or "Unknown error occurred"

@dataclass
class InboxCheckResult:

    checked_at: datetime = field(default_factory=datetime.now)
    emails_found: int = 0               # Total unseen emails from approved senders
    attachments_found: int = 0          # Total PDF attachments across all emails
    attachments_downloaded: int = 0     # Successfully saved to download_dir
    attachments_duplicate: int = 0      # Skipped  SHA-256 already seen
    attachments_sender_rejected: int = 0 # Skipped  sender not approved
    attachments_processed: int = 0      # Successfully sent to OCR
    attachments_failed: int = 0         # OCR or processing failures
    attachments_moved: int = 0          # Moved to processed_dir
    attachments_moved_to_failed: int = 0 # Moved to failed_dir
    imap_error: Optional[IMAPError] = None
    imap_error_message: Optional[str] = None
    success: bool = True                # False if IMAP login / search failed entirely

    def record_imap_error(self, error_type: IMAPError, message: str):
        """Record an IMAP-level error that aborted the entire check"""
        self.imap_error = error_type
        self.imap_error_message = message
        self.success = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            "checked_at": self.checked_at.isoformat(),
            "success": self.success,
            "emails_found": self.emails_found,
            "attachments_found": self.attachments_found,
            "attachments_downloaded": self.attachments_downloaded,
            "attachments_duplicate": self.attachments_duplicate,
            "attachments_sender_rejected": self.attachments_sender_rejected,
            "attachments_processed": self.attachments_processed,
            "attachments_failed": self.attachments_failed,
            "attachments_moved": self.attachments_moved,
            "attachments_moved_to_failed": self.attachments_moved_to_failed,
            "imap_error": self.imap_error.value if self.imap_error else None,
            "imap_error_message": self.imap_error_message,
        }

@dataclass
class EmailFetchStats:
    # --- Inbox check counters ---
    total_inbox_checks: int = 0
    total_emails_checked: int = 0           # Distinct email messages opened
    total_attachments_found: int = 0        # Total PDF attachments across all emails
    total_attachments_downloaded: int = 0   # Written to download_dir

    # --- Dedup / rejection counters ---
    total_attachments_duplicate: int = 0    # Skipped  SHA-256 already seen
    total_sender_rejections: int = 0        # Skipped  sender not approved

    # --- OCR outcome counters ---
    total_attachments_processed: int = 0    # Sent to OCR successfully
    total_attachments_failed: int = 0       # OCR or processing errors
    total_attachments_moved: int = 0        # Moved to processed_dir
    total_attachments_moved_to_failed: int = 0  # Moved to failed_dir

    # --- Auth / API error counters ---
    total_token_refreshes: int = 0
    total_api_errors: int = 0
    total_imap_errors: int = 0              # Replaces total_sftp_errors

    # --- Text extraction counters (same as PipelineStats) ---
    total_text_extractions_success: int = 0
    total_text_extractions_failed: int = 0
    total_document_type_detections_success: int = 0
    total_document_type_detections_failed: int = 0
    total_pymupdf_extractions: int = 0
    total_pdfplumber_extractions: int = 0

    # --- Timing ---
    last_inbox_check_time: Optional[datetime] = None
    last_successful_process: Optional[datetime] = None
    last_token_refresh: Optional[datetime] = None
    fetcher_started_at: datetime = field(default_factory=datetime.now)

    def record_inbox_check(self, emails_found: int, attachments_found: int):

        self.total_inbox_checks += 1
        self.total_emails_checked += emails_found
        self.total_attachments_found += attachments_found
        self.last_inbox_check_time = datetime.now()

    def record_download(self):
        """Record one successfully downloaded attachment"""
        self.total_attachments_downloaded += 1

    def record_duplicate(self):
        """Record one attachment skipped due to SHA-256 hash deduplication"""
        self.total_attachments_duplicate += 1

    def record_sender_rejection(self):
        """Record one attachment/email skipped due to unapproved sender"""
        self.total_sender_rejections += 1

    def record_success(self):
        """Record one successfully OCR-processed attachment"""
        self.total_attachments_processed += 1
        self.last_successful_process = datetime.now()

    def record_failure(self):
        """Record one failed attachment"""
        self.total_attachments_failed += 1

    def record_moved(self):
        """Record one attachment moved to processed_dir"""
        self.total_attachments_moved += 1

    def record_moved_to_failed(self):
        """Record one attachment moved to failed_dir"""
        self.total_attachments_moved_to_failed += 1

    def record_token_refresh(self):
        """Record one JWT token refresh"""
        self.total_token_refreshes += 1
        self.last_token_refresh = datetime.now()

    def record_api_error(self):
        """Record one OCR API error"""
        self.total_api_errors += 1

    def record_imap_error(self):
        """Record one IMAP connectivity error (replaces record_sftp_error)"""
        self.total_imap_errors += 1

    def record_text_extraction_success(self, method: str):

        self.total_text_extractions_success += 1
        if method.lower() == "pymupdf":
            self.total_pymupdf_extractions += 1
        elif method.lower() == "pdfplumber":
            self.total_pdfplumber_extractions += 1

    def record_text_extraction_failure(self):
        """Record failed PDF text extraction"""
        self.total_text_extractions_failed += 1

    def record_document_type_detection_success(self):
        """Record successful document type detection"""
        self.total_document_type_detections_success += 1

    def record_document_type_detection_failure(self):
        """Record failed document type detection"""
        self.total_document_type_detections_failed += 1
    # ----------------------------------------------------------------
    # Rate / duration helpers
    # ----------------------------------------------------------------
    def get_success_rate(self) -> float:
        """Return OCR success rate as a percentage"""
        total = self.total_attachments_processed + self.total_attachments_failed
        return (self.total_attachments_processed / total * 100) if total else 0.0

    def get_failure_rate(self) -> float:
        """Return OCR failure rate as a percentage"""
        total = self.total_attachments_processed + self.total_attachments_failed
        return (self.total_attachments_failed / total * 100) if total else 0.0

    def get_duplicate_rate(self) -> float:
        """
        Return deduplication rate as a percentage of all attachments found.

        Replaces get_uuid_rename_rate() from PipelineStats (hash-based here).
        """
        total = self.total_attachments_found
        return (self.total_attachments_duplicate / total * 100) if total else 0.0

    def get_text_extraction_success_rate(self) -> float:
        """Return text extraction success rate as a percentage"""
        total = self.total_text_extractions_success + self.total_text_extractions_failed
        return (self.total_text_extractions_success / total * 100) if total else 0.0

    def get_document_type_detection_success_rate(self) -> float:
        """Return document type detection success rate as a percentage"""
        total = (
            self.total_document_type_detections_success
            + self.total_document_type_detections_failed
        )
        return (self.total_document_type_detections_success / total * 100) if total else 0.0

    def get_uptime_hours(self) -> float:
        """Return fetcher uptime in hours since startup"""
        delta = datetime.now() - self.fetcher_started_at
        return delta.total_seconds() / 3600

    def to_dict(self) -> Dict[str, Any]:
        """Convert statistics to dictionary for logging and status reporting"""
        return {
            # Inbox check counters
            "total_inbox_checks": self.total_inbox_checks,
            "total_emails_checked": self.total_emails_checked,
            "total_attachments_found": self.total_attachments_found,
            "total_attachments_downloaded": self.total_attachments_downloaded,
            # Dedup / rejection
            "total_attachments_duplicate": self.total_attachments_duplicate,
            "total_sender_rejections": self.total_sender_rejections,
            "duplicate_rate_percent": round(self.get_duplicate_rate(), 2),
            # OCR outcomes
            "total_attachments_processed": self.total_attachments_processed,
            "total_attachments_failed": self.total_attachments_failed,
            "total_attachments_moved": self.total_attachments_moved,
            "total_attachments_moved_to_failed": self.total_attachments_moved_to_failed,
            "success_rate_percent": round(self.get_success_rate(), 2),
            "failure_rate_percent": round(self.get_failure_rate(), 2),
            # Auth / connectivity
            "total_token_refreshes": self.total_token_refreshes,
            "total_api_errors": self.total_api_errors,
            "total_imap_errors": self.total_imap_errors,
            # Text extraction
            "total_text_extractions_success": self.total_text_extractions_success,
            "total_text_extractions_failed": self.total_text_extractions_failed,
            "text_extraction_success_rate_percent": round(
                self.get_text_extraction_success_rate(), 2
            ),
            # Document type detection
            "total_document_type_detections_success": self.total_document_type_detections_success,
            "total_document_type_detections_failed": self.total_document_type_detections_failed,
            "document_type_detection_success_rate_percent": round(
                self.get_document_type_detection_success_rate(), 2
            ),
            # Extraction methods breakdown
            "total_pymupdf_extractions": self.total_pymupdf_extractions,
            "total_pdfplumber_extractions": self.total_pdfplumber_extractions,
            # Timing
            "uptime_hours": round(self.get_uptime_hours(), 2),
            "last_inbox_check": (
                self.last_inbox_check_time.isoformat() if self.last_inbox_check_time else None
            ),
            "last_success": (
                self.last_successful_process.isoformat()
                if self.last_successful_process
                else None
            ),
            "last_token_refresh": (
                self.last_token_refresh.isoformat() if self.last_token_refresh else None
            ),
        }

@dataclass
class AuthCredentials:

    username: str
    password: str

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for the API request body"""
        return {
            "username": self.username,
            "password": self.password,
        }

@dataclass
class EmailFetcherState:

    token_state: Optional[TokenState] = None
    current_tasks: List[EmailAttachmentTask] = field(default_factory=list)
    stats: EmailFetchStats = field(default_factory=EmailFetchStats)
    last_check_result: Optional[InboxCheckResult] = None
    is_running: bool = False
    # ----------------------------------------------------------------
    # Task queue management  (mirrors PipelineState methods)
    # ----------------------------------------------------------------
    def add_task(self, task: EmailAttachmentTask):
        """Add a new attachment task to the active queue"""
        self.current_tasks.append(task)

    def remove_task(self, task: EmailAttachmentTask):
        """Remove a task from the active queue"""
        if task in self.current_tasks:
            self.current_tasks.remove(task)

    def get_pending_tasks(self) -> List[EmailAttachmentTask]:
        """Return all tasks with PENDING status"""
        return [t for t in self.current_tasks if t.status == TaskStatus.PENDING]

    def get_downloading_tasks(self) -> List[EmailAttachmentTask]:
        """Return all tasks currently being downloaded from IMAP"""
        return [t for t in self.current_tasks if t.status == TaskStatus.DOWNLOADING]

    def get_processing_tasks(self) -> List[EmailAttachmentTask]:
        """Return all tasks currently being sent to OCR"""
        return [t for t in self.current_tasks if t.status == TaskStatus.PROCESSING]

    def get_failed_tasks(self) -> List[EmailAttachmentTask]:
        """Return all tasks that have failed"""
        return [t for t in self.current_tasks if t.status == TaskStatus.FAILED]

    def get_duplicate_tasks(self) -> List[EmailAttachmentTask]:
        """Return all tasks skipped due to hash deduplication"""
        return [t for t in self.current_tasks if t.status == TaskStatus.DUPLICATE]

    def get_moved_to_failed_tasks(self) -> List[EmailAttachmentTask]:
        """Return all tasks moved to failed_dir"""
        return [t for t in self.current_tasks if t.status == TaskStatus.MOVED_TO_FAILED]

    def get_tasks_without_document_type(self) -> List[EmailAttachmentTask]:
        """Return all tasks where document_type has not yet been detected"""
        return [t for t in self.current_tasks if not t.has_document_type()]

    def clear_completed_tasks(self):
        """Remove all tasks in terminal states from the active queue"""
        terminal_statuses = {
            TaskStatus.COMPLETED,
            TaskStatus.MOVED,
            TaskStatus.MOVED_TO_FAILED,
            TaskStatus.DUPLICATE,
            TaskStatus.SENDER_REJECTED,
        }
        self.current_tasks = [
            t for t in self.current_tasks if t.status not in terminal_statuses
        ]

    def has_valid_token(self) -> bool:
        return self.token_state is not None and self.token_state.is_valid()

    def to_dict(self) -> Dict[str, Any]:
        state_dict = {
            "is_running": self.is_running,
            "has_valid_token": self.has_valid_token(),
            "token_expires_in_hours": (
                round(self.token_state.time_until_expiry() / 3600, 2)
                if self.token_state
                else None
            ),
            "pending_tasks": len(self.get_pending_tasks()),
            "downloading_tasks": len(self.get_downloading_tasks()),
            "processing_tasks": len(self.get_processing_tasks()),
            "failed_tasks": len(self.get_failed_tasks()),
            "duplicate_tasks": len(self.get_duplicate_tasks()),
            "moved_to_failed_tasks": len(self.get_moved_to_failed_tasks()),
            "tasks_without_document_type": len(self.get_tasks_without_document_type()),
            "last_check_result": (
                self.last_check_result.to_dict() if self.last_check_result else None
            ),
            "stats": self.stats.to_dict(),
        }
        return state_dict