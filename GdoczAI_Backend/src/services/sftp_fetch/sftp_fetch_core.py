# -*- coding: utf-8 -*-

import logging
import time
import requests
from typing import Optional, List, Tuple, Set
from pathlib import Path
from threading import Lock
from datetime import datetime, timedelta

from src.services.sftp_fetch.sftp_fetch_config import PipelineConfig, SFTPConfig, SlimPipelineConfig, EmailConfig
from src.services.sftp_fetch.sftp_fetch_models import (
    PDFTask, OCRRequest, OCRResponse, 
    PipelineStats, TaskStatus, OCRResult
)
from src.services.sftp_fetch.sftp_fetch_auth import AuthenticationManager
from src.services.sftp_fetch.sftp_fetch_sftp import SFTPManager, SFTPOperationError, SFTPConnectionError
from src.services.sftp_fetch.sftp_fetch_utils import (
    retry_on_failure, log_execution_time,
    extract_pdf_text, detect_document_type_with_logging  # ?? NEW: Import PDF text utilities
)
from src.services.sftp_fetch.sftp_fetch_notifier import (
    EmailNotifier, SFTPFailureContext, SFTPRecoveryContext, 
    FileFailureContext, EmailSendError
)

logger = logging.getLogger(__name__)

class PipelineError(Exception):
    pass

class OCRAPIError(Exception):
    pass

class ProcessingTracker:

    def __init__(self):
        """Initialize processing tracker"""
        self._processing_files: Set[str] = set()
        self._completed_files: Set[str] = set()
        self._lock = Lock()
        
        logger.info("?? ProcessingTracker initialized")
    
    def is_processing(self, file_path: str) -> bool:

        with self._lock:
            return file_path in self._processing_files
    
    def is_completed(self, file_path: str) -> bool:

        with self._lock:
            return file_path in self._completed_files
    
    def mark_processing(self, file_path: str) -> bool:

        with self._lock:
            if file_path in self._processing_files:
                logger.warning(f"?? File already being processed: {Path(file_path).name}")
                return False
            
            self._processing_files.add(file_path)
            logger.debug(f"?? Marked as processing: {Path(file_path).name}")
            return True
    
    def mark_completed(self, file_path: str):

        with self._lock:
            if file_path in self._processing_files:
                self._processing_files.remove(file_path)
            
            self._completed_files.add(file_path)
            logger.debug(f"? Marked as completed: {Path(file_path).name}")
    
    def mark_failed(self, file_path: str):

        with self._lock:
            if file_path in self._processing_files:
                self._processing_files.remove(file_path)
            
            logger.debug(f"? Marked as failed: {Path(file_path).name}")
    
    def clear_completed(self):

        with self._lock:
            count = len(self._completed_files)
            self._completed_files.clear()
            if count > 0:
                logger.debug(f"??? Cleared {count} completed file entries")
    
    def get_status(self) -> dict:

        with self._lock:
            return {
                'processing_count': len(self._processing_files),
                'completed_count': len(self._completed_files),
                'processing_files': list(self._processing_files),
                'completed_files': list(self._completed_files)
            }


class SFTPAlertManager:

    def __init__(self, cooldown_minutes: int = 30):

        self.cooldown_minutes = cooldown_minutes
        self._is_failing = False
        self._failure_start_time: Optional[datetime] = None
        self._last_alert_time: Optional[datetime] = None
        self._failure_notified = False
        self._lock = Lock()
        
        logger.info(f"?? SFTPAlertManager initialized (cooldown: {cooldown_minutes} minutes)")
    
    def should_send_failure_alert(self) -> bool:

        with self._lock:
            now = datetime.now()
            
            # First failure - always alert
            if not self._failure_notified:
                return True
            
            # Already notified - check cooldown
            if self._last_alert_time:
                minutes_since_last = (now - self._last_alert_time).total_seconds() / 60
                if minutes_since_last < self.cooldown_minutes:
                    logger.debug(f"?? Alert cooldown active ({minutes_since_last:.1f}/{self.cooldown_minutes} min)")
                    return False
            
            return True
    
    def mark_failure(self, send_alert: bool = True):

        with self._lock:
            now = datetime.now()
            
            if not self._is_failing:
                # Transition from working to failing
                self._is_failing = True
                self._failure_start_time = now
                logger.warning(f"?? SFTP marked as FAILING (started at {now.isoformat()})")
            
            if send_alert:
                self._failure_notified = True
                self._last_alert_time = now
                logger.info(f"?? Failure alert sent at {now.isoformat()}")
    
    def mark_recovery(self) -> bool:

        with self._lock:
            if self._is_failing:
                # Calculate downtime
                if self._failure_start_time:
                    downtime_minutes = (datetime.now() - self._failure_start_time).total_seconds() / 60
                    logger.info(f"? SFTP RECOVERED after {downtime_minutes:.1f} minutes downtime")
                
                self._is_failing = False
                self._failure_start_time = None
                self._failure_notified = False
                
                return True  # Send recovery alert
            
            return False  # Was not failing, no recovery alert needed
    
    def is_failing(self) -> bool:
        with self._lock:
            return self._is_failing
    
    def get_status(self) -> dict:

        with self._lock:
            status = {
                'is_failing': self._is_failing,
                'failure_notified': self._failure_notified,
                'cooldown_minutes': self.cooldown_minutes
            }
            
            if self._failure_start_time:
                status['failure_start_time'] = self._failure_start_time.isoformat()
                downtime_minutes = (datetime.now() - self._failure_start_time).total_seconds() / 60
                status['downtime_minutes'] = round(downtime_minutes, 1)
            
            if self._last_alert_time:
                status['last_alert_time'] = self._last_alert_time.isoformat()
                minutes_since = (datetime.now() - self._last_alert_time).total_seconds() / 60
                status['minutes_since_last_alert'] = round(minutes_since, 1)
            
            return status


class FileAlertTracker:

    def __init__(self):
        self._alerted_files: Set[str] = set()
        self._lock = Lock()
        
        logger.info("?? FileAlertTracker initialized")
    
    def should_send_alert(self, file_path: str) -> bool:

        with self._lock:
            return file_path not in self._alerted_files
    
    def mark_alerted(self, file_path: str):

        with self._lock:
            self._alerted_files.add(file_path)
            logger.debug(f"?? File alert sent: {Path(file_path).name}")
    
    def clear_alerted(self):
        with self._lock:
            count = len(self._alerted_files)
            self._alerted_files.clear()
            if count > 0:
                logger.debug(f"??? Cleared {count} file alert entries")
    
    def get_status(self) -> dict:
        with self._lock:
            return {
                'alerted_count': len(self._alerted_files),
                'alerted_files': list(self._alerted_files)
            }


class PipelineCore:

    _global_processing_lock = Lock()
    _is_processing = False
    
    def __init__(self, slim_config: SlimPipelineConfig, sftp_config: SFTPConfig, 
                 email_config: EmailConfig = None, connector_id: int = None, user_id: int = None):

        self.slim_config = slim_config
        self.config_auth = slim_config.auth
        self.config_ocr = slim_config.ocr
        self.sftp_config = sftp_config
        
        # Store identifiers for logging
        self.connector_id = connector_id
        self.user_id = user_id
        
        # ?? Initialize email config (default to disabled if not provided)
        if email_config is None:
            email_config = EmailConfig(
                enabled=False,
                smtp_host="",
                smtp_port=587,
                smtp_username="",
                smtp_password="",
                from_email="",
                from_name="OCR Pipeline",
                use_tls=True,
                developer_recipients=[],
                client_recipients=[],
                alert_cooldown_minutes=30
            )
        self.email_config = email_config
        
        self.stats = PipelineStats()
        self.tracker = ProcessingTracker()
        
        # ?? Initialize alert managers
        self.alert_manager = SFTPAlertManager(
            cooldown_minutes=email_config.alert_cooldown_minutes
        )
        self.file_alert_tracker = FileAlertTracker()
        
        # ?? Initialize email notifier (with email config)
        self.email_notifier = EmailNotifier(email_config)
        
        # Initialize managers
        self.auth_manager = AuthenticationManager(self.config_auth)
        self.sftp_manager = SFTPManager(sftp_config)
        
        # Log connector information
        connector_str = f"[Connector {self.connector_id} / User {self.user_id}] " if self.connector_id else ""
        logger.info(f"?? {connector_str}PipelineCore initialized")
        logger.info(f"?? SFTP: {sftp_config.host}:{sftp_config.port}")
        logger.info(f"?? Folders: {sftp_config.monitored_folders}")
        logger.info(f"?? Max retry attempts: {slim_config.max_retry_attempts}")
        logger.info(f"?? Retry delay: {slim_config.retry_delay_seconds}s")
        
        # ?? Log email notification status
        if email_config.enabled:
            logger.info(f"?? Email alerts: ENABLED")
            logger.info(f"   Recipients: {len(email_config.get_all_recipients())}")
        else:
            logger.info(f"?? Email alerts: DISABLED (no email config)")
    
    def initialize(self) -> bool:

        logger.info("=" * 80)
        logger.info("?? INITIALIZING PIPELINE")
        logger.info("=" * 80)
        
        try:
            # Obtain initial JWT token
            logger.info("?? Obtaining initial JWT token...")
            try:
                auth_token = self.auth_manager.get_auth_header()
                logger.info(f"? Initial token obtained successfully")
            except Exception as token_error:
                logger.error(f"? Failed to obtain initial token: {token_error}")
                return False
            
            token_status = self.auth_manager.get_status()
            if token_status.get('token_valid'):
                logger.info(f"?? Token expires in {token_status.get('time_until_expiry_hours', 0):.2f} hours")
            else:
                logger.warning("? Token status check returned invalid state")
            
            logger.info("=" * 80)
            logger.info("? PIPELINE INITIALIZED SUCCESSFULLY")
            logger.info("=" * 80)
            
            return True
        
        except Exception as e:
            logger.error(f"? Failed to initialize pipeline: {e}", exc_info=True)
            return False
    
    def _send_sftp_failure_alert(self, error_message: str, exception: Exception):
 
        if not self.alert_manager.should_send_failure_alert():
            logger.debug("?? SFTP failure alert skipped (cooldown active)")
            return
        
        try:
            logger.info("?? Sending SFTP failure alert email...")
            
            context = SFTPFailureContext(
                sftp_host=self.sftp_config.host,
                sftp_port=self.sftp_config.port,
                sftp_username=self.sftp_config.username,
                error_message=error_message,
                exception_type=type(exception).__name__,
                exception_details=str(exception),
                timestamp=datetime.now()
            )
            
            success = self.email_notifier.send_sftp_failure_alert(context)
            
            if success:
                logger.info("? SFTP failure alert sent successfully")
                self.alert_manager.mark_failure(send_alert=True)
            else:
                logger.warning("?? SFTP failure alert send failed")
                self.alert_manager.mark_failure(send_alert=False)
        
        except EmailSendError as e:
            logger.error(f"? Email send error: {e}")
            self.alert_manager.mark_failure(send_alert=False)
        except Exception as e:
            logger.error(f"? Unexpected error sending SFTP failure alert: {e}")
            self.alert_manager.mark_failure(send_alert=False)
    
    def _send_sftp_recovery_alert(self, downtime_minutes: float):

        try:
            logger.info("?? Sending SFTP recovery alert email...")
            
            context = SFTPRecoveryContext(
                sftp_host=self.sftp_config.host,
                sftp_port=self.sftp_config.port,
                downtime_minutes=downtime_minutes,
                timestamp=datetime.now()
            )
            
            success = self.email_notifier.send_sftp_recovery_alert(context)
            
            if success:
                logger.info("? SFTP recovery alert sent successfully")
            else:
                logger.warning("?? SFTP recovery alert send failed")
        
        except EmailSendError as e:
            logger.error(f"? Email send error: {e}")
        except Exception as e:
            logger.error(f"? Unexpected error sending SFTP recovery alert: {e}")
    
    def _send_file_failure_alert(self, task: PDFTask, failure_stage: str):

        if not self.file_alert_tracker.should_send_alert(task.file_path):
            logger.debug(f"?? File failure alert skipped (already sent): {task.filename}")
            return
        
        try:
            logger.info(f"?? Sending file failure alert for: {task.filename}")
            
            context = FileFailureContext(
                filename=task.filename,
                folder_name=task.folder_name,
                document_type=task.document_type or "Unknown",
                failure_stage=failure_stage,
                error_message=task.error_message or "Unknown error",
                file_size_mb=task.file_size_bytes / (1024 * 1024) if task.file_size_bytes else 0,
                failed_folder_path=self.sftp_config.failed_folder,
                final_filename=task.get_final_filename(),
                timestamp=datetime.now()
            )
            
            success = self.email_notifier.send_file_failure_alert(context)
            
            if success:
                logger.info(f"? File failure alert sent successfully for: {task.filename}")
                self.file_alert_tracker.mark_alerted(task.file_path)
            else:
                logger.warning(f"?? File failure alert send failed for: {task.filename}")
        
        except EmailSendError as e:
            logger.error(f"? Email send error: {e}")
        except Exception as e:
            logger.error(f"? Unexpected error sending file failure alert: {e}")
    
    def discover_pdfs(self) -> List[PDFTask]:

        try:
            logger.info("=" * 80)
            logger.info("?? DISCOVERING PDFs IN ALL MONITORED FOLDERS")
            logger.info("=" * 80)
            
            # ========================================================================
            # STEP 1: SCAN ALL FOLDERS AND GET ALL PDF FILES
            # ========================================================================
            with self.sftp_manager as sftp_client:
                # Scan all monitored folders
                all_files = sftp_client.scan_all_monitored_folders()
                
                # Filter only PDF files
                pdf_files = sftp_client.filter_pdf_files(all_files)
            
            # ?? SFTP connection successful - check for recovery
            if self.alert_manager.is_failing():
                status = self.alert_manager.get_status()
                downtime_minutes = status.get('downtime_minutes', 0)
                
                logger.info("=" * 80)
                logger.info("? SFTP CONNECTION RECOVERED")
                logger.info("=" * 80)
                logger.info(f"?? Downtime: {downtime_minutes:.1f} minutes")
                logger.info("=" * 80)
                
                # Mark recovery and send alert
                should_send_recovery = self.alert_manager.mark_recovery()
                if should_send_recovery:
                    self._send_sftp_recovery_alert(downtime_minutes)
            
            # Show file counts by folder
            if pdf_files:
                logger.info("")
                logger.info(f"?? Total PDFs discovered: {len(pdf_files)}")
                
                # Count files per folder
                folder_file_counts = {}
                for pdf_file in pdf_files:
                    folder_file_counts[pdf_file.folder_name] = folder_file_counts.get(pdf_file.folder_name, 0) + 1
                
                logger.info("?? Files by folder:")
                for folder, count in sorted(folder_file_counts.items()):
                    logger.info(f"    {folder}: {count} PDFs")
            
            # ========================================================================
            # STEP 2: FILTER OUT ALREADY PROCESSING/COMPLETED FILES
            # ========================================================================
            logger.info("")
            logger.info("?? Filtering duplicates and in-progress files...")
            
            filtered_files = []
            skipped_processing = 0
            skipped_completed = 0
            
            for pdf_file in pdf_files:
                # Skip if currently being processed
                if self.tracker.is_processing(pdf_file.file_path):
                    logger.debug(f"?? Skipping (already processing): {pdf_file.filename}")
                    skipped_processing += 1
                    continue
                
                # Skip if recently completed (waiting to be moved)
                if self.tracker.is_completed(pdf_file.file_path):
                    logger.debug(f"? Skipping (already completed): {pdf_file.filename}")
                    skipped_completed += 1
                    continue
                
                filtered_files.append(pdf_file)
            
            # ========================================================================
            # STEP 3: SHOW FILTERING RESULTS
            # ========================================================================
            logger.info("")
            logger.info("?? Filtering results:")
            logger.info(f"    Total discovered: {len(pdf_files)}")
            
            if skipped_processing > 0:
                logger.info(f"    Skipped (processing): {skipped_processing}")
            
            if skipped_completed > 0:
                logger.info(f"    Skipped (completed): {skipped_completed}")
            
            logger.info(f"    Ready to process: {len(filtered_files)}")
            
            # ========================================================================
            # STEP 4: CONVERT TO TASKS (WITHOUT SETTING document_type YET)
            # ========================================================================
            # ?? NEW: Document type will be set during processing based on PDF text
            tasks = []
            for pdf_file in filtered_files:
                task = pdf_file.to_pdf_task()
                # ?? Leave document_type as None - will be detected from PDF content
                task.document_type = None
                tasks.append(task)
            
            # Update statistics
            self.stats.record_scan(len(tasks))
            
            # ========================================================================
            # STEP 5: SHOW FINAL SUMMARY
            # ========================================================================
            logger.info("")
            logger.info("=" * 80)
            
            if not tasks:
                if self.alert_manager.is_failing():
                    logger.warning("?? No PDFs discovered - SFTP connection is down")
                else:
                    logger.info("? No new PDFs to process")
            else:
                logger.info(f"?? {len(tasks)} NEW PDFs READY FOR PROCESSING")
                logger.info(f"?? Document types will be detected from PDF text content")
                
                # Show breakdown by folder for new files
                new_folder_counts = {}
                for task in tasks:
                    new_folder_counts[task.folder_name] = new_folder_counts.get(task.folder_name, 0) + 1
                
                logger.info("")
                logger.info("?? New files by folder:")
                for folder, count in sorted(new_folder_counts.items()):
                    logger.info(f"    {folder}: {count} PDFs")
            
            logger.info("=" * 80)
            
            return tasks
        
        except SFTPConnectionError as e:
            # ?? CRITICAL: SFTP connection failed - send alert
            logger.error("=" * 80)
            logger.error("?? SFTP CONNECTION FAILED")
            logger.error("=" * 80)
            logger.error(f"??? Host: {self.sftp_config.host}:{self.sftp_config.port}")
            logger.error(f"?? Username: {self.sftp_config.username}")
            logger.error(f"? Error: {str(e)}")
            logger.error("=" * 80)
            
            # Send failure alert
            self._send_sftp_failure_alert(
                error_message="SFTP connection failed during PDF discovery",
                exception=e
            )
            
            # Record statistics
            self.stats.record_sftp_error()
            
            # Return empty list - processing cannot continue without SFTP
            logger.warning("?? Document processing paused until SFTP connection is restored")
            return []
        
        except SFTPOperationError as e:
            # Non-connection SFTP error (e.g., folder listing failed)
            logger.error(f"? SFTP operation failed: {e}")
            self.stats.record_sftp_error()
            return []
        
        except Exception as e:
            logger.error(f"? Failed to discover PDFs: {e}", exc_info=True)
            self.stats.record_sftp_error()
            return []
    
    def send_to_ocr(self, ocr_request: OCRRequest) -> OCRResponse:

        try:
            logger.debug(f"?? Sending to OCR API: {ocr_request.filename}")
            logger.debug(f"?? Document type: {ocr_request.document_type}")
            logger.debug(f"?? File size: {ocr_request.get_file_size_mb():.2f} MB")
            
            # Prepare multipart form data
            files = {
                'file': (ocr_request.filename, ocr_request.file_bytes, 'application/pdf')
            }
            
            data = {
                'model': 'olmocr',  # Default OCR model
                'document_type': ocr_request.document_type
            }
            
            # Add schema_json if provided
            if ocr_request.schema_json:
                data['schema_json'] = ocr_request.schema_json
            
            headers = {
                'Authorization': ocr_request.authorization_token
            }
            
            # Make API request
            response = requests.post(
                self.config_ocr.endpoint_url,
                files=files,
                data=data,
                headers=headers,
                timeout=self.config_ocr.timeout_seconds
            )
            
            logger.debug(f"?? OCR API Response Status: {response.status_code}")
            
            # Parse response
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    
                    return OCRResponse(
                        success=response_data.get('success', False),
                        request_id=response_data.get('request_id'),
                        markdown=response_data.get('markdown'),
                        json_output=response_data.get('json_output'),
                        metadata=response_data.get('metadata'),
                        status_code=response.status_code
                    )
                
                except Exception as e:
                    logger.error(f"? Failed to parse OCR response: {e}")
                    return OCRResponse(
                        success=False,
                        error=f"Failed to parse response: {str(e)}",
                        status_code=response.status_code
                    )
            else:
                # Handle error responses
                error_msg = f"OCR API returned status {response.status_code}"
                try:
                    error_data = response.json()
                    error_detail = error_data.get('error') or error_data.get('message')
                    if error_detail:
                        error_msg = f"{error_msg}: {error_detail}"
                except:
                    error_msg = f"{error_msg}: {response.text[:200]}"
                
                return OCRResponse(
                    success=False,
                    error=error_msg,
                    status_code=response.status_code
                )
        
        except requests.exceptions.Timeout:
            error_msg = f"OCR request timed out after {self.config_ocr.timeout_seconds} seconds"
            logger.error(f"?? {error_msg}")
            return OCRResponse(success=False, error=error_msg)
        
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Failed to connect to OCR API: {str(e)}"
            logger.error(f"? {error_msg}")
            return OCRResponse(success=False, error=error_msg)
        
        except requests.exceptions.RequestException as e:
            error_msg = f"OCR API request failed: {str(e)}"
            logger.error(f"? {error_msg}")
            return OCRResponse(success=False, error=error_msg)
        
        except Exception as e:
            error_msg = f"Unexpected error during OCR: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            return OCRResponse(success=False, error=error_msg)
    
    def _move_failed_file_to_failed_folder(self, task: PDFTask, failure_stage: str):

        try:
            logger.info("=" * 80)
            logger.info(f"? MOVING FAILED FILE TO Failed_folder")
            logger.info("=" * 80)
            logger.info(f"?? File: {task.filename}")
            logger.info(f"?? Source: {task.folder_name}")
            logger.info(f"? Reason: {task.error_message or 'Unknown error'}")
            logger.info(f"?? Stage: {failure_stage}")
            
            with self.sftp_manager as sftp_client:
                # Check if file still exists in source location
                if not sftp_client.file_exists(task.file_path):
                    logger.warning(f"?? File no longer exists at source: {task.file_path}")
                    logger.warning("?? File may have already been moved or deleted")
                    return
                
                # Move to Failed_folder (handles UUID renaming automatically)
                move_success, final_filename = sftp_client.move_to_failed(task.file_path)
                
                if move_success:
                    # ?? Check if filename was changed due to UUID
                    if final_filename != task.filename:
                        logger.info(f"?? File renamed to avoid conflict:")
                        logger.info(f"   Original: {task.filename}")
                        logger.info(f"   Final: {final_filename}")
                        task.moved_filename = final_filename
                        
                        # ?? RECORD UUID RENAME IN STATISTICS
                        self.stats.record_uuid_rename()
                        logger.debug("?? UUID rename recorded in statistics")
                    
                    logger.info(f"? Moved to: {self.sftp_config.failed_folder}")
                    
                    # ? Mark task as moved to failed folder
                    task.mark_moved_to_failed_folder()
                    
                    # ? Record statistics
                    self.stats.record_moved_to_failed()
                    
                    logger.info("=" * 80)
                    logger.info(f"? FAILED FILE MOVED SUCCESSFULLY")
                    if final_filename != task.filename:
                        logger.info(f"?? Final filename in Failed_folder: {final_filename}")
                    logger.info("=" * 80)
                    
                    # ?? NEW: Send email alert to developers
                    logger.info("")
                    self._send_file_failure_alert(task, failure_stage)
                    
                else:
                    logger.error(f"? Failed to move file to Failed_folder")
        
        except SFTPOperationError as e:
            logger.error(f"? SFTP error moving failed file: {e}")
            logger.error(f"?? File remains in source folder: {task.file_path}")
        
        except Exception as e:
            logger.error(f"? Unexpected error moving failed file: {e}", exc_info=True)
            logger.error(f"?? File remains in source folder: {task.file_path}")
    
    @log_execution_time
    def process_pdf(self, task: PDFTask) -> bool:

        logger.info("=" * 80)
        logger.info(f"?? PROCESSING PDF: {task.filename}")
        logger.info("=" * 80)
        logger.info(f"?? Source folder: {task.folder_name}")
        logger.info(f"?? File size: {task.file_size_bytes / (1024 * 1024):.2f} MB")
        
        # ?? STEP 0: Mark file as processing to prevent duplicates
        if not self.tracker.mark_processing(task.file_path):
            logger.warning(f"?? File already being processed, skipping: {task.filename}")
            return False
        
        task.mark_processing()
        
        try:
            # Step 1: Get valid JWT token
            logger.info("?? Step 1: Obtaining valid JWT token...")
            try:
                auth_token = self.auth_manager.get_auth_header()
                logger.info("? Token obtained successfully")
            except Exception as e:
                logger.error(f"? Failed to get auth token: {e}")
                task.mark_failed(f"Authentication failed: {str(e)}")
                self.tracker.mark_failed(task.file_path)
                self.stats.record_failure()
                
                # ? NEW: Move failed file to Failed_folder + send alert
                self._move_failed_file_to_failed_folder(task, "Authentication")
                
                return False
            
            # Step 2: Download PDF from SFTP
            logger.info("?? Step 2: Downloading PDF from SFTP...")
            try:
                with self.sftp_manager as sftp_client:
                    file_bytes = sftp_client.download_file(task.file_path)
                logger.info(f"? Downloaded {len(file_bytes) / (1024 * 1024):.2f} MB")
            except SFTPOperationError as e:
                logger.error(f"? Failed to download PDF: {e}")
                task.mark_failed(f"SFTP download failed: {str(e)}")
                self.tracker.mark_failed(task.file_path)
                self.stats.record_failure()
                self.stats.record_sftp_error()
                
                # ? NEW: Move failed file to Failed_folder + send alert
                self._move_failed_file_to_failed_folder(task, "Download")
                
                return False
            
            # ?? NEW STEP 3: Extract text from PDF and detect document_type
            logger.info("?? Step 3: Extracting text and detecting document type...")
            try:
                # Extract text from PDF (first 5 pages)
                success, result = extract_pdf_text(file_bytes, max_pages=5)
                
                if not success:
                    logger.error(f"? Failed to extract text from PDF: {result}")
                    task.mark_failed(f"PDF text extraction failed: {result}")
                    self.tracker.mark_failed(task.file_path)
                    self.stats.record_failure()
                    
                    # Move failed file to Failed_folder + send alert
                    self._move_failed_file_to_failed_folder(task, "Text Extraction")
                    
                    return False
                
                pdf_text = result
                logger.info(f"? Extracted {len(pdf_text)} characters from PDF")
                
                # Detect document type from text
                document_type = detect_document_type_with_logging(pdf_text, task.filename)
                
                if not document_type:
                    logger.error(f"? Failed to detect document type from PDF text")
                    logger.warning(f"?? No matching keywords found in PDF content")
                    task.mark_failed("Document type detection failed: No matching keywords found in PDF")
                    self.tracker.mark_failed(task.file_path)
                    self.stats.record_failure()
                    
                    # Move failed file to Failed_folder + send alert
                    self._move_failed_file_to_failed_folder(task, "Document Type Detection")
                    
                    return False
                
                # Set detected document type
                task.document_type = document_type
                logger.info(f"? Document type detected: '{document_type}'")
                
            except Exception as e:
                logger.error(f"? Error during text extraction/detection: {e}", exc_info=True)
                task.mark_failed(f"Document type detection error: {str(e)}")
                self.tracker.mark_failed(task.file_path)
                self.stats.record_failure()
                
                # Move failed file to Failed_folder + send alert
                self._move_failed_file_to_failed_folder(task, "Document Type Detection")
                
                return False
            
            # Step 4: Send to OCR API (BLOCKS UNTIL COMPLETE)
            logger.info("?? Step 4: Sending to OCR API... ??")
            logger.info("? WAITING FOR OCR PROCESSING TO COMPLETE...")
            
            ocr_request = OCRRequest(
                file_bytes=file_bytes,
                filename=task.filename,
                document_type=task.document_type,
                authorization_token=auth_token
            )
            
            # ?? THIS BLOCKS UNTIL OCR COMPLETES - NO OTHER FILE PROCESSES DURING THIS TIME
            ocr_start_time = time.time()
            ocr_response = self.send_to_ocr(ocr_request)
            ocr_duration = time.time() - ocr_start_time
            
            if not ocr_response.is_successful():
                logger.error(f"? OCR processing failed: {ocr_response.get_error_message()}")
                task.mark_failed(f"OCR failed: {ocr_response.get_error_message()}")
                self.tracker.mark_failed(task.file_path)
                self.stats.record_failure()
                self.stats.record_api_error()
                
                # ? NEW: Move failed file to Failed_folder + send alert
                self._move_failed_file_to_failed_folder(task, "OCR Processing")
                
                return False
            
            logger.info(f"? OCR processing successful (took {ocr_duration:.2f}s)")
            logger.info(f"?? Request ID: {ocr_response.request_id}")
            
            # Step 5: Move to processed folder (handles UUID renaming)
            logger.info("?? Step 5: Moving to processed folder...")
            final_filename = task.filename  # Default to original
            
            try:
                with self.sftp_manager as sftp_client:
                    move_success, final_filename = sftp_client.move_to_processed(task.file_path)
                
                if move_success:
                    # ?? Check if filename was changed due to UUID
                    if final_filename != task.filename:
                        logger.info(f"?? File renamed to avoid conflict:")
                        logger.info(f"   Original: {task.filename}")
                        logger.info(f"   Final: {final_filename}")
                        task.moved_filename = final_filename
                        
                        # ?? RECORD UUID RENAME IN STATISTICS
                        self.stats.record_uuid_rename()
                        logger.debug("?? UUID rename recorded in statistics")
                    
                    logger.info(f"? Moved to: {self.sftp_config.moved_folder}")
                    task.mark_moved()
                else:
                    logger.warning("?? Failed to move file, but OCR completed")
            except SFTPOperationError as e:
                logger.warning(f"?? Failed to move file: {e}")
                # Don't fail the task - OCR was successful
            
            # Mark task as completed
            task.mark_completed(ocr_response.request_id)
            self.tracker.mark_completed(task.file_path)
            self.stats.record_success()
            self.stats.record_moved()
            
            logger.info("=" * 80)
            logger.info(f"? SUCCESSFULLY PROCESSED: {task.filename}")
            logger.info(f"?? Document Type: {task.document_type}")
            if final_filename != task.filename:
                logger.info(f"?? Final filename in moved_folder: {final_filename}")
            logger.info(f"?? Processing time: {task.get_processing_duration():.2f}s")
            logger.info("=" * 80)
            
            return True
        
        except Exception as e:
            logger.error(f"? Unexpected error processing PDF: {e}", exc_info=True)
            task.mark_failed(f"Unexpected error: {str(e)}")
            self.tracker.mark_failed(task.file_path)
            self.stats.record_failure()
            
            # ? NEW: Move failed file to Failed_folder + send alert
            self._move_failed_file_to_failed_folder(task, "Unknown")
            
            return False
    
    @retry_on_failure(max_attempts=3, delay_seconds=5)
    def process_pdf_with_retry(self, task: PDFTask) -> bool:
        return self.process_pdf(task)
    
    def process_batch(self, tasks: List[PDFTask]) -> Tuple[int, int]:

        if not tasks:
            logger.info("? No PDFs to process")
            return 0, 0
        
        # ============================================================================
        # STEP 1: SHOW TOTAL COUNT BEFORE PROCESSING
        # ============================================================================
        total_files = len(tasks)
        
        logger.info("=" * 80)
        logger.info(f"?? PROCESSING BATCH: {total_files} PDFs DISCOVERED")
        logger.info("=" * 80)
        logger.info(f"?? Total files to process: {total_files}")
        logger.info(f"?? Mode: STRICT SEQUENTIAL (one at a time)")
        logger.info(f"?? Document types will be auto-detected from PDF text")
        logger.info("=" * 80)
        
        # ============================================================================
        # STEP 2: PROCESS EACH PDF ONE BY ONE (STRICTLY SEQUENTIAL)
        # ============================================================================
        successful = 0
        failed = 0
        
        for idx, task in enumerate(tasks, start=1):
            logger.info("")
            logger.info("=" * 80)
            logger.info(f"?? PROCESSING FILE {idx}/{total_files}")
            logger.info("=" * 80)
            logger.info(f"?? Progress: {(idx / total_files * 100):.1f}%")
            logger.info(f"?? Running stats: {successful} successful, {failed} failed")
            logger.info("=" * 80)

            result = self.process_pdf(task)
            
            if result:
                successful += 1
                logger.info(f"? File {idx}/{total_files} completed successfully")
            else:
                failed += 1
                logger.error(f"? File {idx}/{total_files} failed")
                
                # ? If file failed, it was already moved to Failed_folder by process_pdf()
                # ?? Email alert was already sent by process_pdf()
                try:
                    # Double-check if file needs to be moved (in case process_pdf didn't handle it)
                    if task.status == TaskStatus.FAILED and not task.moved_to_failed:
                        logger.warning(f"?? File not moved to Failed_folder, attempting now...")
                        self._move_failed_file_to_failed_folder(task, "Post-Processing Check")
                except:
                    logger.error(f"?? Could not move failed file to Failed_folder")
            
            # Show remaining count
            remaining = total_files - idx
            if remaining > 0:
                logger.info(f"? Remaining files: {remaining}")
                
                # ?? Add explicit delay between files to ensure cleanup
                logger.debug("?? Pausing 2 seconds before next file...")
                time.sleep(2)  # 2 second pause between files
        
        # ============================================================================
        # STEP 3: SHOW FINAL SUMMARY
        # ============================================================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("?? BATCH PROCESSING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"?? Total processed: {total_files} PDFs")
        logger.info(f"? Successful: {successful}")
        logger.info(f"? Failed: {failed}")
        logger.info(f"?? Success rate: {(successful / total_files * 100):.1f}%")
        
        # Show UUID rename statistics if any occurred
        if self.stats.total_files_renamed_with_uuid > 0:
            logger.info("")
            logger.info(f"?? Files renamed with UUID: {self.stats.total_files_renamed_with_uuid}")
            logger.info(f"?? UUID rename rate: {self.stats.get_uuid_rename_rate():.1f}%")
        
        # ? Show failed file movement statistics
        if self.stats.total_pdfs_moved_to_failed > 0:
            logger.info("")
            logger.info(f"? Files moved to Failed_folder: {self.stats.total_pdfs_moved_to_failed}")
            logger.info(f"?? Email alerts sent: {self.file_alert_tracker.get_status()['alerted_count']}")
        
        # Show failed files if any
        if failed > 0:
            logger.info("")
            logger.info("=" * 80)
            logger.info("? FAILED FILES:")
            for task in tasks:
                if task.status == TaskStatus.FAILED or task.status == TaskStatus.MOVED_TO_FAILED:
                    logger.info(f"    {task.filename}")
                    if task.error_message:
                        logger.info(f"     Error: {task.error_message}")
                    if task.moved_to_failed:
                        logger.info(f"     ?? Location: Failed_folder/{task.get_final_filename()}")
                        logger.info(f"     ?? Alert sent to: developers only")
        
        logger.info("=" * 80)
        
        # Clear completed files and alert tracker to prevent memory bloat
        self.tracker.clear_completed()
        self.file_alert_tracker.clear_alerted()
        
        return successful, failed
    
    def run_once(self) -> bool:

        # ?? CHECK IF ALREADY PROCESSING (NON-BLOCKING CHECK)
        with PipelineCore._global_processing_lock:
            if PipelineCore._is_processing:
                logger.warning("=" * 80)
                logger.warning("?? PROCESSING ALREADY IN PROGRESS")
                logger.warning("=" * 80)
                logger.warning("?? Another batch is currently being processed")
                logger.warning("?? Skipping this scan cycle to prevent parallel processing")
                logger.warning("? The next scan will process any remaining files")
                logger.warning("=" * 80)
                return True  # Return True - this is expected behavior
            
            # Mark as processing
            PipelineCore._is_processing = True
            logger.info("?? Global processing lock ACQUIRED")
        
        try:
            logger.info("=" * 80)
            logger.info("?? STARTING PIPELINE CYCLE")
            logger.info("=" * 80)
            
            # Log tracker status
            tracker_status = self.tracker.get_status()
            if tracker_status['processing_count'] > 0:
                logger.info(f"?? Files currently processing: {tracker_status['processing_count']}")
            
            # Log alert manager status
            if self.alert_manager.is_failing():
                logger.warning("?? SFTP is in failure state - will attempt reconnection")
            
            cycle_start_time = time.time()
            
            # Discover new PDFs (filters duplicates automatically)
            # ?? This will send failure/recovery alerts if SFTP state changes
            tasks = self.discover_pdfs()
            
            if not tasks:
                if self.alert_manager.is_failing():
                    logger.warning("?? No PDFs discovered - SFTP connection is down")
                else:
                    logger.info("? No new PDFs found in this cycle")
                logger.info("=" * 80)
                return True

            successful, failed = self.process_batch(tasks)
            
            # Calculate cycle statistics
            cycle_duration = time.time() - cycle_start_time
            
            logger.info("")
            logger.info("=" * 80)
            logger.info("?? PIPELINE CYCLE COMPLETE")
            logger.info("=" * 80)
            logger.info(f"?? Cycle duration: {cycle_duration:.2f}s")
            logger.info(f"?? Processed: {successful + failed} PDFs")
            logger.info(f"? Success: {successful}")
            logger.info(f"? Failed: {failed}")
            
            # ?? Log UUID rename statistics if any occurred
            if self.stats.total_files_renamed_with_uuid > 0:
                logger.info(f"?? Files renamed with UUID: {self.stats.total_files_renamed_with_uuid}")
                logger.info(f"?? UUID rename rate: {self.stats.get_uuid_rename_rate():.1f}%")
            
            # ? Log failed file movement statistics
            if self.stats.total_pdfs_moved_to_failed > 0:
                logger.info(f"? Files moved to Failed_folder: {self.stats.total_pdfs_moved_to_failed}")
                logger.info(f"?? Failure alerts sent: {self.file_alert_tracker.get_status()['alerted_count']}")
            
            logger.info("=" * 80)
            
            return True
        
        except Exception as e:
            logger.error(f"? Pipeline cycle failed: {e}", exc_info=True)
            return False
        
        finally:
            # ?? ALWAYS RELEASE THE LOCK
            with PipelineCore._global_processing_lock:
                PipelineCore._is_processing = False
                logger.info("?? Global processing lock RELEASED")
    
    def get_statistics(self) -> dict:

        stats_dict = self.stats.to_dict()
        stats_dict['tracker_status'] = self.tracker.get_status()
        stats_dict['alert_status'] = self.alert_manager.get_status()
        stats_dict['file_alert_status'] = self.file_alert_tracker.get_status()
        stats_dict['is_processing'] = PipelineCore._is_processing
        return stats_dict
    
    def get_auth_status(self) -> dict:
        return self.auth_manager.get_status()
    
    def get_alert_status(self) -> dict:
        return self.alert_manager.get_status()
    
    @classmethod
    def is_currently_processing(cls) -> bool:

        with cls._global_processing_lock:
            return cls._is_processing