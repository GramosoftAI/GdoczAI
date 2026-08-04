# -*- coding: utf-8 -*-

"""
Core orchestration logic for Gmail to OCR email fetcher.

Coordinates authentication, IMAP, and OCR operations. Implements main fetcher flow,
handles errors and retries, provides duplicate prevention, updates statistics,
and sends email alerts for failures.
"""

import logging
import time
import requests
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import List, Optional, Set, Tuple

from src.services.smtp_fetch.smtp_fetcher_config import IMAPConnectorConfig
from src.services.sftp_fetch.sftp_fetch_config import SlimPipelineConfig
from src.services.smtp_fetch.smtp_fetcher_models import (
    EmailAttachmentTask,
    EmailFetchStats,
    InboxCheckResult,
    InboxEmail,
    OCRRequest,
    OCRResponse,
    TaskStatus,
    OCRResult,
    IMAPError,
)
from src.services.smtp_fetch.smtp_fetcher_imap import (
    IMAPManager,
    IMAPConnectionError,
    IMAPOperationError,
)
from src.services.smtp_fetch.smtp_fetcher_notifier import (
    EmailFetcherNotifier,
    IMAPFailureContext,
    IMAPRecoveryContext,
    FileFailureContext,
    EmailSendError,
)
from src.services.sftp_fetch.sftp_fetch_auth import AuthenticationManager
from src.services.sftp_fetch.sftp_fetch_utils import (
    retry_on_failure,
    log_execution_time,
    extract_pdf_text,
    detect_document_type_with_logging,
)

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------
class EmailFetcherError(Exception):
    pass

class OCRAPIError(Exception):
    pass
# ---------------------------------------------------------------------------
# AttachmentTracker  (replaces ProcessingTracker from document_pipeline_core.py)
# ---------------------------------------------------------------------------
class AttachmentTracker:

    def __init__(self):
        """Initialize attachment tracker"""
        self._processing_hashes: Set[str] = set()   # SHA-256 hashes in-flight
        self._completed_hashes: Set[str] = set()    # SHA-256 hashes done this run
        self._completed_uids: Set[str] = set()      # IMAP UIDs fully processed
        self._lock = Lock()

        logger.info("?? AttachmentTracker initialized")
    # ----------------------------------------------------------------
    # Hash-based tracking  (parallel to file_path tracking in SFTP core)
    # ----------------------------------------------------------------

    def is_processing(self, sha256_hash: str) -> bool:
        """Return True if this hash is currently in-flight"""
        with self._lock:
            return sha256_hash in self._processing_hashes

    def is_completed(self, sha256_hash: str) -> bool:
        """Return True if this hash was already processed this run"""
        with self._lock:
            return sha256_hash in self._completed_hashes

    def mark_processing(self, sha256_hash: str) -> bool:

        with self._lock:
            if sha256_hash in self._processing_hashes:
                logger.warning(f"??  Attachment already in-flight (hash: {sha256_hash[:16]})")
                return False
            self._processing_hashes.add(sha256_hash)
            logger.debug(f"?? Marked as processing (hash: {sha256_hash[:16]})")
            return True

    def mark_completed(self, sha256_hash: str):
        """Move hash from in-flight to completed set"""
        with self._lock:
            self._processing_hashes.discard(sha256_hash)
            self._completed_hashes.add(sha256_hash)
            logger.debug(f"? Marked as completed (hash: {sha256_hash[:16]})")

    def mark_failed(self, sha256_hash: str):
        """Remove hash from in-flight set (no retry tracking needed at this level)"""
        with self._lock:
            self._processing_hashes.discard(sha256_hash)
            logger.debug(f"? Marked as failed (hash: {sha256_hash[:16]})")

    # ----------------------------------------------------------------
    # UID-level tracking  (no equivalent in SFTP core)
    # ----------------------------------------------------------------

    def mark_uid_completed(self, imap_uid: str):
        """Record that all attachments from this email UID are fully processed"""
        with self._lock:
            self._completed_uids.add(imap_uid)
            logger.debug(f"?? UID fully processed: {imap_uid}")

    def is_uid_completed(self, imap_uid: str) -> bool:
        """Return True if all attachments from this UID were processed"""
        with self._lock:
            return imap_uid in self._completed_uids

    def get_completed_uids(self) -> List[str]:
        """Return list of all fully processed UIDs (for mark-as-seen step)"""
        with self._lock:
            return list(self._completed_uids)

    # ----------------------------------------------------------------
    # Cleanup
    # ----------------------------------------------------------------

    def clear_completed(self):
        """Clear completed hashes and UIDs to prevent memory bloat"""
        with self._lock:
            hash_count = len(self._completed_hashes)
            uid_count = len(self._completed_uids)
            self._completed_hashes.clear()
            self._completed_uids.clear()
            if hash_count or uid_count:
                logger.debug(
                    f"???  Cleared tracker: {hash_count} hashes, {uid_count} UIDs"
                )

    def get_status(self) -> dict:
        """Return tracker status dictionary"""
        with self._lock:
            return {
                "processing_count": len(self._processing_hashes),
                "completed_count": len(self._completed_hashes),
                "completed_uid_count": len(self._completed_uids),
                "processing_hashes": [h[:16] for h in self._processing_hashes],
            }
# ---------------------------------------------------------------------------
# IMAPAlertManager  (replaces SFTPAlertManager from document_pipeline_core.py)
# ---------------------------------------------------------------------------
class IMAPAlertManager:

    def __init__(self, cooldown_minutes: int = 30):

        self.cooldown_minutes = cooldown_minutes
        self._is_failing: bool = False
        self._failure_start_time: Optional[datetime] = None
        self._last_alert_time: Optional[datetime] = None
        self._failure_notified: bool = False
        self._lock = Lock()

        logger.info(
            f"?? IMAPAlertManager initialized (cooldown: {cooldown_minutes} minutes)"
        )

    def should_send_failure_alert(self) -> bool:

        with self._lock:
            if not self._failure_notified:
                return True  # First failure  always alert

            if self._last_alert_time:
                minutes_since = (
                    datetime.now() - self._last_alert_time
                ).total_seconds() / 60
                if minutes_since < self.cooldown_minutes:
                    logger.debug(
                        f"? Alert cooldown active "
                        f"({minutes_since:.1f}/{self.cooldown_minutes} min)"
                    )
                    return False

            return True

    def mark_failure(self, send_alert: bool = True):

        with self._lock:
            now = datetime.now()
            if not self._is_failing:
                self._is_failing = True
                self._failure_start_time = now
                logger.warning(
                    f"?? IMAP marked as FAILING (started at {now.isoformat()})"
                )
            if send_alert:
                self._failure_notified = True
                self._last_alert_time = now
                logger.info(f"?? Failure alert recorded at {now.isoformat()}")

    def mark_recovery(self) -> bool:

        with self._lock:
            if self._is_failing:
                if self._failure_start_time:
                    downtime = (
                        datetime.now() - self._failure_start_time
                    ).total_seconds() / 60
                    logger.info(
                        f"? IMAP RECOVERED after {downtime:.1f} minutes downtime"
                    )
                self._is_failing = False
                self._failure_start_time = None
                self._failure_notified = False
                return True  # Send recovery alert
            return False  # Was not failing

    def is_failing(self) -> bool:
        """Return True if IMAP is currently in failure state"""
        with self._lock:
            return self._is_failing

    def get_downtime_minutes(self) -> float:
        """Return minutes since IMAP first failed (0 if not failing)"""
        with self._lock:
            if self._is_failing and self._failure_start_time:
                return (
                    datetime.now() - self._failure_start_time
                ).total_seconds() / 60
            return 0.0

    def get_status(self) -> dict:
        """Return alert manager state as dictionary"""
        with self._lock:
            status = {
                "is_failing": self._is_failing,
                "failure_notified": self._failure_notified,
                "cooldown_minutes": self.cooldown_minutes,
            }
            if self._failure_start_time:
                status["failure_start_time"] = self._failure_start_time.isoformat()
                status["downtime_minutes"] = round(self.get_downtime_minutes(), 1)
            if self._last_alert_time:
                status["last_alert_time"] = self._last_alert_time.isoformat()
                minutes_since = (
                    datetime.now() - self._last_alert_time
                ).total_seconds() / 60
                status["minutes_since_last_alert"] = round(minutes_since, 1)
            return status

# ---------------------------------------------------------------------------
# FileAlertTracker  (identical to document_pipeline_core.py)
# ---------------------------------------------------------------------------
class FileAlertTracker:

    def __init__(self):
        """Initialize file alert tracker"""
        self._alerted_paths: Set[str] = set()
        self._lock = Lock()
        logger.info("?? FileAlertTracker initialized")

    def should_send_alert(self, local_path: str) -> bool:
        """Return True if an alert has not yet been sent for this path"""
        with self._lock:
            return local_path not in self._alerted_paths

    def mark_alerted(self, local_path: str):
        """Record that an alert was sent for this path"""
        with self._lock:
            self._alerted_paths.add(local_path)
            logger.debug(f"?? File alert sent: {Path(local_path).name}")

    def clear_alerted(self):
        """Clear all alerted paths (call after each batch)"""
        with self._lock:
            count = len(self._alerted_paths)
            self._alerted_paths.clear()
            if count:
                logger.debug(f"???  Cleared {count} file alert entries")

    def get_status(self) -> dict:
        """Return tracker status"""
        with self._lock:
            return {
                "alerted_count": len(self._alerted_paths),
                "alerted_files": [Path(p).name for p in self._alerted_paths],
            }
# ---------------------------------------------------------------------------
# EmailFetcherCore  (replaces PipelineCore from document_pipeline_core.py)
# ---------------------------------------------------------------------------
class EmailFetcherCore:

    _global_processing_lock = Lock()
    _is_processing: bool = False

    def __init__(self, imap_config: 'IMAPConnectorConfig', slim_config: 'SlimPipelineConfig'):

        self.imap_config = imap_config
        self.slim_config = slim_config
        self.stats = EmailFetchStats()

        # Duplicate-prevention tracker (hash-based, replaces path-based SFTP tracker)
        self.tracker = AttachmentTracker()

        # IMAP failure alert state manager
        self.alert_manager = IMAPAlertManager(
            cooldown_minutes=30
        )

        # Per-file failure alert dedup tracker
        self.file_alert_tracker = FileAlertTracker()

        # Email notifier (SMTP sender for alerts)
        from src.services.smtp_fetch.smtp_fetcher_config import EmailNotificationConfig
        dummy_notif = EmailNotificationConfig(enabled=False, smtp_host='localhost', smtp_port=25, smtp_username='', smtp_password='', from_email='', from_name='', use_tls=False, developer_recipients=['dummy@example.com'], client_recipients=[])
        self.notifier = EmailFetcherNotifier(dummy_notif)

        # JWT auth manager  shared with SFTP pipeline (same backend)
        self.auth_manager = AuthenticationManager(slim_config.auth)

        # IMAP manager (replaces SFTPManager)
        self.imap_manager = IMAPManager(imap_config)

        logger.info("?? EmailFetcherCore initialized")
        logger.info(f"?? Max retry attempts : {slim_config.max_retry_attempts}")
        logger.info(f"??  Retry delay        : {slim_config.retry_delay_seconds}s")
        logger.info("?? Email alerts      : DISABLED (Not configured for DB connectors)")

    # ----------------------------------------------------------------
    # Initialization  (mirrors PipelineCore.initialize())
    # ----------------------------------------------------------------

    def initialize(self) -> bool:

        logger.info("=" * 80)
        logger.info("?? INITIALIZING EMAIL FETCHER")
        logger.info("=" * 80)

        try:
            logger.info("?? Obtaining initial JWT token...")
            try:
                auth_token = self.auth_manager.get_auth_header()
                logger.info("? Initial token obtained successfully")
            except Exception as token_error:
                logger.error(f"? Failed to obtain initial token: {token_error}")
                return False

            token_status = self.auth_manager.get_status()
            if token_status.get("token_valid"):
                hours_left = token_status.get("time_until_expiry_hours", 0)
                logger.info(f"?? Token expires in {hours_left:.2f} hours")
            else:
                logger.warning("??  Token status returned invalid state after login")

            logger.info("=" * 80)
            logger.info("? EMAIL FETCHER INITIALIZED SUCCESSFULLY")
            logger.info("=" * 80)
            return True

        except Exception as e:
            logger.error(f"? Failed to initialize fetcher: {e}", exc_info=True)
            return False

    # ----------------------------------------------------------------
    # Alert senders  (mirror _send_sftp_failure/recovery/file_failure_alert)
    # ----------------------------------------------------------------

    def _send_imap_failure_alert(self, error_message: str, exception: Exception):

        if not self.alert_manager.should_send_failure_alert():
            logger.debug("? IMAP failure alert skipped (cooldown active)")
            return

        try:
            logger.info("?? Sending IMAP failure alert email...")

            context = IMAPFailureContext(
                imap_server=self.imap_config.imap_server,
                imap_port=self.imap_config.imap_port,
                email_id=self.imap_config.email_id,
                mailbox=self.imap_config.mailbox,
                error_message=error_message,
                exception_type=type(exception).__name__,
                exception_details=str(exception),
                timestamp=datetime.now(),
            )

            success = self.notifier.send_imap_failure_alert(context)

            if success:
                logger.info("? IMAP failure alert sent successfully")
                self.alert_manager.mark_failure(send_alert=True)
            else:
                logger.warning("??  IMAP failure alert send failed")
                self.alert_manager.mark_failure(send_alert=False)

        except EmailSendError as e:
            logger.error(f"? Email send error: {e}")
            self.alert_manager.mark_failure(send_alert=False)
        except Exception as e:
            logger.error(f"? Unexpected error sending IMAP failure alert: {e}")
            self.alert_manager.mark_failure(send_alert=False)

    def _send_imap_recovery_alert(self, downtime_minutes: float):

        try:
            logger.info("?? Sending IMAP recovery alert email...")

            context = IMAPRecoveryContext(
                imap_server=self.imap_config.imap_server,
                imap_port=self.imap_config.imap_port,
                email_id=self.imap_config.email_id,
                downtime_minutes=downtime_minutes,
                timestamp=datetime.now(),
            )

            success = self.notifier.send_imap_recovery_alert(context)

            if success:
                logger.info("? IMAP recovery alert sent successfully")
            else:
                logger.warning("??  IMAP recovery alert send failed")

        except EmailSendError as e:
            logger.error(f"? Email send error: {e}")
        except Exception as e:
            logger.error(f"? Unexpected error sending IMAP recovery alert: {e}")

    def _send_file_failure_alert(
        self, task: EmailAttachmentTask, failure_stage: str
    ):

        # Use local_path as dedup key (replaces SFTP file_path)
        alert_key = task.local_path or f"{task.imap_uid}_{task.attachment_filename}"

        if not self.file_alert_tracker.should_send_alert(alert_key):
            logger.debug(
                f"? File failure alert skipped (already sent): "
                f"{task.attachment_filename}"
            )
            return

        try:
            logger.info(
                f"?? Sending file failure alert for: {task.attachment_filename}"
            )

            context = FileFailureContext(
                attachment_filename=task.attachment_filename,
                sender_address=task.sender_address,
                email_subject=task.email_subject,
                imap_uid=task.imap_uid,
                document_type=task.document_type or "Unknown",
                failure_stage=failure_stage,
                error_message=task.error_message or "Unknown error",
                file_size_mb=(
                    task.file_size_bytes / (1024 * 1024)
                    if task.file_size_bytes
                    else 0.0
                ),
                failed_dir_path=self.imap_config.failed_dir,
                local_path=task.local_path or "Not saved",
                retry_count=task.retry_count,
                timestamp=datetime.now(),
            )

            success = self.notifier.send_file_failure_alert(context)

            if success:
                logger.info(
                    f"? File failure alert sent: {task.attachment_filename}"
                )
                self.file_alert_tracker.mark_alerted(alert_key)
            else:
                logger.warning(
                    f"??  File failure alert send failed: {task.attachment_filename}"
                )

        except EmailSendError as e:
            logger.error(f"? Email send error: {e}")
        except Exception as e:
            logger.error(
                f"? Unexpected error sending file failure alert: {e}",
                exc_info=True,
            )
    # ----------------------------------------------------------------
    # Attachment discovery  (mirrors PipelineCore.discover_pdfs())
    # ----------------------------------------------------------------
    def discover_attachments(self) -> Tuple[List[EmailAttachmentTask], InboxCheckResult]:

        check_result = InboxCheckResult()

        try:
            logger.info("=" * 80)
            logger.info("?? DISCOVERING PDF ATTACHMENTS IN GMAIL INBOX")
            logger.info("=" * 80)
            logger.info(f"?? Account : {self.imap_config.email_id}")
            logger.info(f"?? Mailbox : {self.imap_config.mailbox}")
            logger.info(
                f"??  Approved senders: "
                f"{', '.join(self.imap_config.approved_senders)}"
            )

            # ----------------------------------------------------------------
            # STEP 1: Search inbox for UNSEEN emails from approved senders
            # ----------------------------------------------------------------
            try:
                with self.imap_manager as imap_client:
                    uids = imap_client.search_unseen_from_approved_senders()
            except IMAPConnectionError as e:
                # -- IMAP connection failed ? alert + return empty --
                logger.error("=" * 80)
                logger.error("? IMAP CONNECTION FAILED")
                logger.error("=" * 80)
                logger.error(f"?? Server  : {self.imap_config.imap_server}:{self.imap_config.imap_port}")
                logger.error(f"?? Account : {self.imap_config.email_id}")
                logger.error(f"? Error   : {str(e)}")
                logger.error("=" * 80)

                check_result.record_imap_error(IMAPError.CONNECTION_FAILED, str(e))
                self._send_imap_failure_alert(
                    "IMAP connection failed during inbox search", e
                )
                self.stats.record_imap_error()
                logger.warning(
                    "??  Attachment discovery paused until IMAP is restored"
                )
                return [], check_result

            # -- IMAP succeeded ? check for recovery --
            if self.alert_manager.is_failing():
                downtime = self.alert_manager.get_downtime_minutes()
                logger.info("=" * 80)
                logger.info("? IMAP CONNECTION RECOVERED")
                logger.info("=" * 80)
                logger.info(f"??  Downtime: {downtime:.1f} minutes")
                logger.info("=" * 80)
                should_send = self.alert_manager.mark_recovery()
                if should_send:
                    self._send_imap_recovery_alert(downtime)

            if not uids:
                logger.info("?? No UNSEEN emails from approved senders")
                check_result.emails_found = 0
                self.stats.record_inbox_check(0, 0)
                return [], check_result

            check_result.emails_found = len(uids)
            logger.info(f"?? Found {len(uids)} UNSEEN email(s) to process")

            # ----------------------------------------------------------------
            # STEP 2: For each UID ? fetch envelope + extract attachments
            # ----------------------------------------------------------------
            all_tasks: List[EmailAttachmentTask] = []
            total_attachments_found = 0

            for uid in uids:
                logger.info("-" * 60)
                logger.info(f"?? Processing email UID: {uid}")

                # Fetch envelope (headers only  cheap)
                try:
                    with self.imap_manager as imap_client:
                        inbox_email = imap_client.fetch_email_envelope(uid)
                except (IMAPConnectionError, IMAPOperationError) as e:
                    logger.error(f"? Failed to fetch envelope for UID {uid}: {e}")
                    check_result.attachments_sender_rejected += 1
                    continue

                if inbox_email is None:
                    logger.warning(f"??  Empty envelope for UID {uid}  skipping")
                    continue

                logger.info(f"   From    : {inbox_email.sender_address}")
                logger.info(f"   Subject : {inbox_email.subject[:80]}")

                # Double-check sender (IMAP SEARCH FROM is fuzzy  re-verify)
                approved_lower = self.imap_config.get_approved_senders_lower()
                if not inbox_email.is_approved_sender(approved_lower):
                    logger.warning(
                        f"? Sender not approved: {inbox_email.sender_address}  skipping"
                    )
                    check_result.attachments_sender_rejected += 1
                    self.stats.record_sender_rejection()
                    continue

                # Extract and save all PDF attachments from this email
                try:
                    saved_attachments = self.imap_manager.download_attachment_and_save(
                        uid, inbox_email
                    )
                except (IMAPConnectionError, IMAPOperationError) as e:
                    logger.error(
                        f"? Failed to extract attachments for UID {uid}: {e}"
                    )
                    self.stats.record_imap_error()
                    continue

                # saved_attachments: List[(local_path, sha256, filename)]
                pdf_count = inbox_email.attachment_count
                total_attachments_found += pdf_count
                check_result.attachments_found += pdf_count

                if not saved_attachments:
                    logger.info(
                        f"   ?? No new PDF attachments saved (all duplicate or empty)"
                    )
                    check_result.attachments_duplicate += pdf_count
                    self.stats.record_inbox_check(1, pdf_count)
                    # Mark email as seen even if all attachments were duplicates
                    # to avoid re-checking it on every scan
                    if self.imap_config.mark_as_seen:
                        self._safe_mark_email_seen(uid)
                    continue

                # ----------------------------------------------------------------
                # STEP 3: Build EmailAttachmentTask per saved attachment
                # ----------------------------------------------------------------
                for local_path, sha256, filename in saved_attachments:
                    file_size = Path(local_path).stat().st_size if Path(local_path).exists() else 0

                    # Skip if hash already in-flight or completed this run
                    if self.tracker.is_processing(sha256):
                        logger.debug(
                            f"??  Hash already in-flight, skipping: {filename}"
                        )
                        check_result.attachments_duplicate += 1
                        self.stats.record_duplicate()
                        continue

                    if self.tracker.is_completed(sha256):
                        logger.debug(
                            f"??  Hash already completed this run, skipping: {filename}"
                        )
                        check_result.attachments_duplicate += 1
                        self.stats.record_duplicate()
                        continue

                    task = EmailAttachmentTask(
                        imap_uid=uid,
                        attachment_filename=filename,
                        sender_address=inbox_email.sender_address,
                        email_subject=inbox_email.subject,
                        sha256_hash=sha256,
                        file_size_bytes=file_size,
                        local_path=local_path,
                    )
                    task.mark_download_complete(local_path, sha256, file_size)

                    all_tasks.append(task)
                    check_result.attachments_downloaded += 1
                    self.stats.record_download()

                    logger.info(
                        f"   ? Task created: {filename} "
                        f"({file_size / 1024:.1f} KB) hash:{sha256[:12]}"
                    )

                self.stats.record_inbox_check(1, pdf_count)

            # ----------------------------------------------------------------
            # STEP 4: Log discovery summary
            # ----------------------------------------------------------------
            logger.info("=" * 80)
            logger.info("?? DISCOVERY SUMMARY")
            logger.info("=" * 80)
            logger.info(f"   Emails scanned       : {len(uids)}")
            logger.info(f"   Attachments found    : {total_attachments_found}")
            logger.info(f"   Duplicates skipped   : {check_result.attachments_duplicate}")
            logger.info(f"   Sender rejections    : {check_result.attachments_sender_rejected}")
            logger.info(f"   New tasks created    : {len(all_tasks)}")
            logger.info("=" * 80)

            if not all_tasks:
                logger.info("?? No new PDF attachments to process this cycle")
            else:
                logger.info(f"?? {len(all_tasks)} attachment(s) ready for processing")

            return all_tasks, check_result

        except Exception as e:
            logger.error(f"? Unexpected error during discovery: {e}", exc_info=True)
            self.stats.record_imap_error()
            check_result.record_imap_error(IMAPError.SEARCH_FAILED, str(e))
            return [], check_result

    # ----------------------------------------------------------------
    # OCR sender  (identical to PipelineCore.send_to_ocr())
    # ----------------------------------------------------------------

    def send_to_ocr(self, ocr_request: OCRRequest) -> OCRResponse:
        try:
            logger.debug(f"?? Sending to OCR API: {ocr_request.filename}")
            logger.debug(f"?? Document type: {ocr_request.document_type}")
            logger.debug(f"?? File size: {ocr_request.get_file_size_mb():.2f} MB")

            files = {
                "file": (
                    ocr_request.filename,
                    ocr_request.file_bytes,
                    "application/pdf",
                )
            }
            data = {"document_type": ocr_request.document_type}

            if ocr_request.schema_json:
                data["schema_json"] = ocr_request.schema_json

            headers = {"Authorization": ocr_request.authorization_token}

            response = requests.post(
                self.slim_config.ocr.endpoint_url,
                files=files,
                data=data,
                headers=headers,
                timeout=self.slim_config.ocr.timeout_seconds,
            )

            logger.debug(f"?? OCR API Response Status: {response.status_code}")

            if response.status_code == 200:
                try:
                    response_data = response.json()
                    return OCRResponse(
                        success=response_data.get("success", False),
                        request_id=response_data.get("request_id"),
                        markdown=response_data.get("markdown"),
                        json_output=response_data.get("json_output"),
                        metadata=response_data.get("metadata"),
                        status_code=response.status_code,
                    )
                except Exception as e:
                    logger.error(f"? Failed to parse OCR response JSON: {e}")
                    return OCRResponse(
                        success=False,
                        error=f"Failed to parse response: {str(e)}",
                        status_code=response.status_code,
                    )
            else:
                error_msg = f"OCR API returned status {response.status_code}"
                try:
                    error_data = response.json()
                    detail = error_data.get("error") or error_data.get("message")
                    if detail:
                        error_msg = f"{error_msg}: {detail}"
                except Exception:
                    error_msg = f"{error_msg}: {response.text[:200]}"

                return OCRResponse(
                    success=False,
                    error=error_msg,
                    status_code=response.status_code,
                )

        except requests.exceptions.Timeout:
            error_msg = (
                f"OCR request timed out after "
                f"{self.slim_config.ocr.timeout_seconds} seconds"
            )
            logger.error(f"? {error_msg}")
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
    # ----------------------------------------------------------------
    # Failed file handler  (mirrors PipelineCore._move_failed_file_to_failed_folder)
    # ----------------------------------------------------------------
    def _move_failed_attachment_to_failed_dir(
        self, task: EmailAttachmentTask, failure_stage: str
    ):

        if not task.local_path:
            logger.warning(
                f"??  No local_path on task  cannot move to failed_dir: "
                f"{task.attachment_filename}"
            )
            return

        try:
            logger.info("=" * 80)
            logger.info("? MOVING FAILED ATTACHMENT TO failed_dir")
            logger.info("=" * 80)
            logger.info(f"?? File   : {task.attachment_filename}")
            logger.info(f"??  Sender : {task.sender_address}")
            logger.info(f"? Reason : {task.error_message or 'Unknown error'}")
            logger.info(f"?? Stage  : {failure_stage}")

            if not Path(task.local_path).exists():
                logger.warning(
                    f"??  File no longer on disk: {task.local_path}  "
                    f"may have already been moved"
                )
                return

            with self.imap_manager as imap_client:
                move_success, final_path = imap_client.move_to_failed(task.local_path)

            if move_success:
                task.mark_moved_to_failed(final_path)
                self.stats.record_moved_to_failed()

                logger.info(f"? Moved to failed_dir: {Path(final_path).name}")
                logger.info("=" * 80)

                # Send developer alert after moving
                self._send_file_failure_alert(task, failure_stage)
            else:
                logger.error("? Failed to move attachment to failed_dir")

        except IMAPOperationError as e:
            logger.error(f"? Local move error: {e}")
            logger.error(f"??  File remains in download_dir: {task.local_path}")
        except Exception as e:
            logger.error(
                f"? Unexpected error moving failed attachment: {e}", exc_info=True
            )
            logger.error(f"??  File remains in download_dir: {task.local_path}")

    # ----------------------------------------------------------------
    # Safe mark-seen helper
    # ----------------------------------------------------------------

    def _safe_mark_email_seen(self, uid: str) -> bool:

        try:
            with self.imap_manager as imap_client:
                return imap_client.mark_email_as_seen(uid)
        except Exception as e:
            logger.warning(
                f"??  Could not mark email UID {uid} as Seen: {e} "
                f" it will be picked up again next scan"
            )
            return False

    # ----------------------------------------------------------------
    # process_attachment  (mirrors PipelineCore.process_pdf())
    # ----------------------------------------------------------------

    @log_execution_time
    def process_attachment(self, task: EmailAttachmentTask) -> bool:

        logger.info("=" * 80)
        logger.info(f"?? PROCESSING ATTACHMENT: {task.attachment_filename}")
        logger.info("=" * 80)
        logger.info(f"??  Sender  : {task.sender_address}")
        logger.info(f"?? Subject : {task.email_subject[:80]}")
        logger.info(f"?? Size    : {task.file_size_bytes / (1024 * 1024):.2f} MB")
        logger.info(f"?? Hash    : {(task.sha256_hash or '')[:16]}...")

        # -- STEP 0: Claim hash in tracker --
        if not task.sha256_hash or not self.tracker.mark_processing(task.sha256_hash):
            logger.warning(
                f"??  Hash already being processed  skipping: "
                f"{task.attachment_filename}"
            )
            return False

        task.mark_processing()

        try:
            # -- STEP 1: Get valid JWT token --
            logger.info("?? Step 1: Obtaining valid JWT token...")
            try:
                auth_token = self.auth_manager.get_auth_header()
                logger.info("? Token obtained successfully")
            except Exception as e:
                logger.error(f"? Failed to get auth token: {e}")
                task.mark_failed(f"Authentication failed: {str(e)}")
                self.tracker.mark_failed(task.sha256_hash)
                self.stats.record_failure()
                self._move_failed_attachment_to_failed_dir(task, "Authentication")
                return False

            # -- STEP 2: Read PDF bytes from local disk --
            logger.info("?? Step 2: Reading PDF from local disk...")
            try:
                if not task.local_path or not Path(task.local_path).exists():
                    raise FileNotFoundError(
                        f"PDF not found on disk: {task.local_path}"
                    )
                file_bytes = Path(task.local_path).read_bytes()
                logger.info(
                    f"? Read {len(file_bytes) / (1024 * 1024):.2f} MB "
                    f"from {Path(task.local_path).name}"
                )
            except Exception as e:
                logger.error(f"? Failed to read PDF from disk: {e}")
                task.mark_failed(f"Local file read failed: {str(e)}")
                self.tracker.mark_failed(task.sha256_hash)
                self.stats.record_failure()
                self._move_failed_attachment_to_failed_dir(task, "File Read")
                return False

            # -- STEP 3: Extract PDF text ? detect document_type --
            logger.info("?? Step 3: Extracting text and detecting document type...")
            try:
                success, result = extract_pdf_text(file_bytes, max_pages=5)

                if not success:
                    logger.error(f"? PDF text extraction failed: {result}")
                    task.mark_failed(f"PDF text extraction failed: {result}")
                    self.tracker.mark_failed(task.sha256_hash)
                    self.stats.record_failure()
                    self.stats.record_text_extraction_failure()
                    self._move_failed_attachment_to_failed_dir(
                        task, "Text Extraction"
                    )
                    return False

                pdf_text = result

                # Store extracted text on task (for debugging)
                # Determine which extraction method was used
                extraction_method = "pymupdf"
                try:
                    import fitz  # noqa: F401  check if pymupdf available
                except ImportError:
                    extraction_method = "pdfplumber"

                task.set_extracted_text(pdf_text, extraction_method)
                self.stats.record_text_extraction_success(extraction_method)

                logger.info(
                    f"? Extracted {len(pdf_text)} characters "
                    f"via {extraction_method}"
                )

                # Detect document type from text content
                document_type = detect_document_type_with_logging(
                    pdf_text, task.attachment_filename
                )

                if not document_type:
                    logger.error(
                        "? Document type detection failed: no matching keywords"
                    )
                    task.mark_failed(
                        "Document type detection failed: no matching keywords in PDF"
                    )
                    self.tracker.mark_failed(task.sha256_hash)
                    self.stats.record_failure()
                    self.stats.record_document_type_detection_failure()
                    self._move_failed_attachment_to_failed_dir(
                        task, "Document Type Detection"
                    )
                    return False

                task.set_document_type(document_type)
                self.stats.record_document_type_detection_success()
                logger.info(f"? Document type detected: '{document_type}'")

            except Exception as e:
                logger.error(
                    f"? Error during text extraction / detection: {e}",
                    exc_info=True,
                )
                task.mark_failed(f"Document type detection error: {str(e)}")
                self.tracker.mark_failed(task.sha256_hash)
                self.stats.record_failure()
                self._move_failed_attachment_to_failed_dir(
                    task, "Document Type Detection"
                )
                return False

            # -- STEP 4: Send to OCR API (BLOCKS) --
            logger.info("?? Step 4: Sending to OCR API...")
            logger.info("? WAITING FOR OCR PROCESSING TO COMPLETE...")

            ocr_request = OCRRequest(
                file_bytes=file_bytes,
                filename=task.attachment_filename,
                document_type=task.document_type,
                authorization_token=auth_token,
            )

            ocr_start = time.time()
            ocr_response = self.send_to_ocr(ocr_request)
            ocr_duration = time.time() - ocr_start

            if not ocr_response.is_successful():
                logger.error(
                    f"? OCR processing failed: {ocr_response.get_error_message()}"
                )
                task.mark_failed(
                    f"OCR failed: {ocr_response.get_error_message()}"
                )
                self.tracker.mark_failed(task.sha256_hash)
                self.stats.record_failure()
                self.stats.record_api_error()
                self._move_failed_attachment_to_failed_dir(task, "OCR Processing")
                return False

            logger.info(
                f"? OCR processing successful ({ocr_duration:.2f}s)"
            )
            logger.info(f"?? Request ID: {ocr_response.request_id}")

            # -- STEP 5: Move to processed_dir --
            logger.info("?? Step 5: Moving to processed_dir...")
            final_path = task.local_path  # default  may change after move

            try:
                with self.imap_manager as imap_client:
                    move_success, final_path = imap_client.move_to_processed(
                        task.local_path
                    )

                if move_success:
                    task.mark_moved(final_path)
                    self.stats.record_moved()
                    logger.info(
                        f"? Moved to processed_dir: {Path(final_path).name}"
                    )
                else:
                    logger.warning(
                        "??  Failed to move to processed_dir, "
                        "but OCR completed successfully"
                    )

            except IMAPOperationError as e:
                logger.warning(f"??  Move to processed_dir failed: {e}")
                # Do NOT fail the task  OCR was successful

            # Mark as fully completed
            task.mark_completed(ocr_response.request_id)
            self.tracker.mark_completed(task.sha256_hash)
            self.tracker.mark_uid_completed(task.imap_uid)
            self.stats.record_success()

            logger.info("=" * 80)
            logger.info(
                f"? SUCCESSFULLY PROCESSED: {task.attachment_filename}"
            )
            logger.info(f"?? Document Type : {task.document_type}")
            logger.info(f"??  Sender        : {task.sender_address}")
            logger.info(
                f"??  Processing time: "
                f"{task.get_processing_duration() or 0:.2f}s"
            )
            logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error(
                f"? Unexpected error processing attachment: {e}", exc_info=True
            )
            task.mark_failed(f"Unexpected error: {str(e)}")
            self.tracker.mark_failed(task.sha256_hash or "unknown")
            self.stats.record_failure()
            self._move_failed_attachment_to_failed_dir(task, "Unknown")
            return False

    @retry_on_failure(max_attempts=3, delay_seconds=5)
    def process_attachment_with_retry(self, task: EmailAttachmentTask) -> bool:
        return self.process_attachment(task)
    # ----------------------------------------------------------------
    # process_batch  (mirrors PipelineCore.process_batch())
    # ----------------------------------------------------------------
    def process_batch(
        self, tasks: List[EmailAttachmentTask]
    ) -> Tuple[int, int]:

        if not tasks:
            logger.info("?? No attachments to process")
            return 0, 0

        total = len(tasks)

        logger.info("=" * 80)
        logger.info(f"?? PROCESSING BATCH: {total} PDF ATTACHMENT(S)")
        logger.info("=" * 80)
        logger.info(f"?? Total attachments  : {total}")
        logger.info(f"?? Mode               : STRICT SEQUENTIAL (one at a time)")
        logger.info(f"?? Document types     : auto-detected from PDF text")
        logger.info("=" * 80)

        successful = 0
        failed = 0

        for idx, task in enumerate(tasks, start=1):
            logger.info("")
            logger.info("=" * 80)
            logger.info(f"?? PROCESSING ATTACHMENT {idx}/{total}")
            logger.info("=" * 80)
            logger.info(f"?? Progress      : {(idx / total * 100):.1f}%")
            logger.info(f"?? Running stats : {successful} ?  {failed} ?")
            logger.info("=" * 80)

            # PROCESS THIS ATTACHMENT COMPLETELY BEFORE MOVING TO NEXT
            result = self.process_attachment(task)

            if result:
                successful += 1
                logger.info(f"? Attachment {idx}/{total} completed successfully")
            else:
                failed += 1
                logger.error(f"? Attachment {idx}/{total} failed")

                # Safety net: if process_attachment didn't move to failed_dir, do it now
                if task.status == TaskStatus.FAILED and not task.moved_to_failed:
                    try:
                        logger.warning(
                            "??  File not moved to failed_dir  attempting now..."
                        )
                        self._move_failed_attachment_to_failed_dir(
                            task, "Post-Processing Check"
                        )
                    except Exception:
                        logger.error(
                            "? Could not move failed attachment to failed_dir"
                        )

            remaining = total - idx
            if remaining > 0:
                logger.info(f"? Remaining: {remaining} attachment(s)")
                logger.debug("??  Pausing 2 seconds before next file...")
                time.sleep(2)

        # ----------------------------------------------------------------
        # Mark parent emails as Seen for all completed UIDs
        # ----------------------------------------------------------------
        completed_uids = self.tracker.get_completed_uids()
        if completed_uids:
            logger.info("")
            logger.info("=" * 80)
            logger.info(f"???  MARKING {len(completed_uids)} EMAIL(S) AS SEEN")
            logger.info("=" * 80)
            for uid in completed_uids:
                self._safe_mark_email_seen(uid)

        # ----------------------------------------------------------------
        # Batch summary log
        # ----------------------------------------------------------------
        logger.info("")
        logger.info("=" * 80)
        logger.info("?? BATCH PROCESSING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"?? Total processed   : {total}")
        logger.info(f"? Successful        : {successful}")
        logger.info(f"? Failed            : {failed}")
        logger.info(
            f"?? Success rate      : "
            f"{(successful / total * 100):.1f}%"
        )

        if self.stats.total_attachments_duplicate > 0:
            logger.info(
                f"??  Duplicates skipped: "
                f"{self.stats.total_attachments_duplicate}"
            )

        if self.stats.total_attachments_moved_to_failed > 0:
            logger.info(
                f"? Moved to failed_dir: "
                f"{self.stats.total_attachments_moved_to_failed}"
            )
            logger.info(
                f"?? Failure alerts sent: "
                f"{self.file_alert_tracker.get_status()['alerted_count']}"
            )

        if failed > 0:
            logger.info("")
            logger.info("=" * 80)
            logger.info("? FAILED ATTACHMENTS:")
            for task in tasks:
                if task.status in (
                    TaskStatus.FAILED,
                    TaskStatus.MOVED_TO_FAILED,
                ):
                    logger.info(f"    {task.attachment_filename}")
                    logger.info(f"     Sender : {task.sender_address}")
                    if task.error_message:
                        logger.info(f"     Error  : {task.error_message}")
                    if task.moved_to_failed:
                        logger.info(
                            f"     ?? Location: "
                            f"{self.imap_config.failed_dir}/"
                            f"{Path(task.moved_path or '').name}"
                        )

        logger.info("=" * 80)
        self.tracker.clear_completed()
        self.file_alert_tracker.clear_alerted()

        return successful, failed
    # ----------------------------------------------------------------
    # run_once  (mirrors PipelineCore.run_once())
    # ----------------------------------------------------------------
    def run_once(self) -> bool:

        with EmailFetcherCore._global_processing_lock:
            if EmailFetcherCore._is_processing:
                logger.warning("=" * 80)
                logger.warning("??  PROCESSING ALREADY IN PROGRESS")
                logger.warning("=" * 80)
                logger.warning("   Another batch is currently running.")
                logger.warning(
                    "   Skipping this scan cycle to prevent parallel processing."
                )
                logger.warning(
                    "   The next scheduled scan will pick up any remaining files."
                )
                logger.warning("=" * 80)
                return True  # Expected behaviour  not an error

            EmailFetcherCore._is_processing = True
            logger.info("?? Global processing lock ACQUIRED")

        try:
            logger.info("=" * 80)
            logger.info("?? STARTING INBOX SCAN CYCLE")
            logger.info("=" * 80)

            tracker_status = self.tracker.get_status()
            if tracker_status["processing_count"] > 0:
                logger.info(
                    f"? Attachments currently in-flight: "
                    f"{tracker_status['processing_count']}"
                )

            if self.alert_manager.is_failing():
                logger.warning(
                    "??  IMAP is in failure state  will attempt reconnection"
                )

            cycle_start = time.time()
            tasks, check_result = self.discover_attachments()

            if not tasks:
                if self.alert_manager.is_failing():
                    logger.warning(
                        "??  No attachments discovered  IMAP connection is down"
                    )
                else:
                    logger.info("?? No new PDF attachments found this cycle")
                logger.info("=" * 80)
                return True
            successful, failed = self.process_batch(tasks)
            cycle_duration = time.time() - cycle_start

            logger.info("")
            logger.info("=" * 80)
            logger.info("? INBOX SCAN CYCLE COMPLETE")
            logger.info("=" * 80)
            logger.info(f"??  Cycle duration : {cycle_duration:.2f}s")
            logger.info(f"?? Processed      : {successful + failed} attachment(s)")
            logger.info(f"? Successful      : {successful}")
            logger.info(f"? Failed          : {failed}")

            if self.stats.total_attachments_moved_to_failed > 0:
                logger.info(
                    f"?? Moved to failed_dir: "
                    f"{self.stats.total_attachments_moved_to_failed}"
                )
            logger.info("=" * 80)
            return True

        except Exception as e:
            logger.error(f"? Inbox scan cycle failed: {e}", exc_info=True)
            return False

        finally:
            # -- ALWAYS release the lock --
            with EmailFetcherCore._global_processing_lock:
                EmailFetcherCore._is_processing = False
                logger.info("?? Global processing lock RELEASED")
    # ----------------------------------------------------------------
    # Status / statistics  (mirrors PipelineCore.get_statistics / get_auth_status)
    # ----------------------------------------------------------------
    def get_statistics(self) -> dict:
        stats_dict = self.stats.to_dict()
        stats_dict["tracker_status"] = self.tracker.get_status()
        stats_dict["alert_status"] = self.alert_manager.get_status()
        stats_dict["file_alert_status"] = self.file_alert_tracker.get_status()
        stats_dict["is_processing"] = EmailFetcherCore._is_processing
        return stats_dict

    def get_auth_status(self) -> dict:
        return self.auth_manager.get_status()

    def get_alert_status(self) -> dict:
        return self.alert_manager.get_status()

    @classmethod
    def is_currently_processing(cls) -> bool:
        with cls._global_processing_lock:
            return cls._is_processing