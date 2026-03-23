# -*- coding: utf-8 -*-
"""
IMAP operations for Gmail to OCR email fetcher.
Connects to Gmail via IMAP SSL, searches for emails from approved senders,
extracts PDF attachments, handles deduplication, and manages local file operations.
"""

import imaplib
import email
import email.utils
import hashlib
import logging
import os
import re
import shutil
from datetime import datetime
from email.header import decode_header
from pathlib import Path
from typing import List, Optional, Tuple

from src.services.smtp_fetch.smtp_fetcher_config import IMAPConnectorConfig
from src.services.smtp_fetch.smtp_fetcher_models import InboxEmail, EmailAttachmentTask, TaskStatus

logger = logging.getLogger(__name__)

class IMAPConnectionError(Exception):
    pass

class IMAPOperationError(Exception):
    pass

def compute_sha256_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()

def decode_mime_header(header_value: str) -> str:
    if not header_value:
        return ""
    try:
        parts = decode_header(header_value)
        decoded_parts = []

        for part_bytes, charset in parts:
            if isinstance(part_bytes, bytes):
                charset = charset or "utf-8"
                try:
                    decoded_parts.append(part_bytes.decode(charset, errors="replace"))
                except (LookupError, UnicodeDecodeError):
                    decoded_parts.append(part_bytes.decode("utf-8", errors="replace"))
            else:
                decoded_parts.append(part_bytes)

        return " ".join(decoded_parts).strip()

    except Exception as e:
        logger.warning(f"  Failed to decode MIME header '{header_value}': {e}")
        return header_value

def extract_sender_address(from_header: str) -> str:
    if not from_header:
        return ""
    try:
        _display_name, address = email.utils.parseaddr(from_header)
        return address.lower().strip()
    except Exception as e:
        logger.warning(f"  Failed to extract sender from '{from_header}': {e}")
        # Last resort: simple regex
        match = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", from_header)
        return match.group(0).lower() if match else ""

def sanitize_filename(filename: str) -> str:
    if not filename:
        return f"attachment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

    filename = decode_mime_header(filename)
    illegal_chars = r'[/\\:*?"<>|\x00-\x1f]'
    safe_name = re.sub(illegal_chars, "_", filename)
    safe_name = re.sub(r"_+", "_", safe_name).strip("_")
    path = Path(safe_name)
    if path.suffix.lower() != ".pdf":
        safe_name = safe_name + ".pdf"

    stem = Path(safe_name).stem
    if len(safe_name.encode("utf-8")) > 240:
        safe_name = stem[:200] + ".pdf"

    return safe_name or f"attachment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

def ensure_local_directory(dir_path: str) -> bool:
    try:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        logger.debug(f" Directory ready: {dir_path}")
        return True
    except OSError as e:
        logger.error(f" Failed to create directory '{dir_path}': {e}")
        return False

class IMAPClient:

    def __init__(self, config: IMAPConnectorConfig):
        self.config = config
        self.mail: Optional[imaplib.IMAP4_SSL] = None
        self.is_connected: bool = False
        logger.info(" IMAPClient initialized")
        logger.info(f" IMAP Server  : {config.imap_server}:{config.imap_port}")
        logger.info(f" Account      : {config.email_id}")
        logger.info(f" Mailbox      : {config.mailbox}")
        logger.info(f"  Approved Senders: {len(config.approved_senders)}")
        logger.info(f" Download Dir : {config.download_dir}")
        logger.info(f" Processed Dir: {config.processed_dir}")
        logger.info(f" Failed Dir   : {config.failed_dir}")
    # ----------------------------------------------------------------
    # Connection management
    # ----------------------------------------------------------------
    def connect(self) -> bool:
        logger.info("=" * 80)
        logger.info(" CONNECTING TO GMAIL IMAP")
        logger.info("=" * 80)
        logger.info(f" Server  : {self.config.imap_server}:{self.config.imap_port}")
        logger.info(f" Account : {self.config.email_id}")
        logger.info(f" Mailbox : {self.config.mailbox}")

        try:
            try:
                self.mail = imaplib.IMAP4_SSL(
                    host=self.config.imap_server,
                    port=self.config.imap_port,
                )
                logger.debug(" IMAP4_SSL socket opened")
            except ConnectionRefusedError as e:
                raise IMAPConnectionError(
                    f"Connection refused to {self.config.imap_server}:{self.config.imap_port}  "
                    f"check server address and port: {e}"
                )
            except OSError as e:
                raise IMAPConnectionError(
                    f"Network error connecting to {self.config.imap_server}:{self.config.imap_port}: {e}"
                )

            try:
                status, response = self.mail.login(
                    self.config.email_id,
                    self.config.app_password
                )
                if status != "OK":
                    raise IMAPConnectionError(
                        f"IMAP login rejected for {self.config.email_id} "
                        f" status: {status}, response: {response}"
                    )
                logger.info(f" IMAP login successful: {self.config.email_id}")
            except imaplib.IMAP4.error as e:
                raise IMAPConnectionError(
                    f"IMAP authentication failed for {self.config.email_id}: {e}. "
                    f"Ensure Gmail App Password is correct and IMAP is enabled in Gmail settings."
                )
            # Select mailbox
            try:
                status, data = self.mail.select(self.config.mailbox)
                if status != "OK":
                    raise IMAPConnectionError(
                        f"Failed to select mailbox '{self.config.mailbox}' "
                        f" status: {status}, data: {data}"
                    )
                message_count = int(data[0]) if data and data[0] else 0
                logger.info(f" Mailbox '{self.config.mailbox}' selected  {message_count} messages total")
            except imaplib.IMAP4.error as e:
                raise IMAPConnectionError(
                    f"Failed to SELECT mailbox '{self.config.mailbox}': {e}"
                )

            self.is_connected = True
            logger.info(" IMAP connection established successfully")
            logger.info("=" * 80)
            return True

        except IMAPConnectionError:
            # Re-raise as-is (for email alert triggering in core)
            self.is_connected = False
            raise

        except Exception as e:
            self.is_connected = False
            error_msg = (
                f"Unexpected error connecting to {self.config.imap_server}: {e}"
            )
            logger.error(f" {error_msg}", exc_info=True)
            raise IMAPConnectionError(error_msg)

    def disconnect(self):
        try:
            if self.mail and self.is_connected:
                try:
                    self.mail.close()   # Closes selected mailbox
                    logger.debug(" IMAP mailbox closed")
                except Exception:
                    pass

                try:
                    self.mail.logout()
                    logger.debug(" IMAP logout complete")
                except Exception:
                    pass

            self.is_connected = False
            logger.info(" IMAP connection closed")

        except Exception as e:
            logger.warning(f"  Error closing IMAP connection: {e}")

    def ensure_connected(self):
        if not self.is_connected or self.mail is None:
            logger.info(" IMAP not connected  reconnecting...")
            self.connect()

    def search_unseen_from_approved_senders(self) -> List[str]:

        self.ensure_connected()
        logger.info("=" * 80)
        logger.info(" SEARCHING INBOX FOR UNSEEN EMAILS FROM APPROVED SENDERS")
        logger.info("=" * 80)
        approved = self.config.get_approved_senders_lower()
        seen_uids: set = set()
        all_uids: List[str] = []

        for sender in approved:
            try:
                logger.debug(f" Searching UNSEEN FROM {sender}")

                # IMAP4 search: UNSEEN FROM "sender@example.com"
                status, data = self.mail.uid(
                    "SEARCH", None,
                    "UNSEEN", f'FROM "{sender}"'
                )

                if status != "OK":
                    logger.warning(
                        f"  SEARCH returned non-OK status for sender {sender}: "
                        f"{status}  {data}"
                    )
                    continue

                # data[0] is space-separated byte string of UIDs
                raw = data[0]
                if not raw:
                    logger.debug(f" No UNSEEN emails from {sender}")
                    continue

                uids = raw.decode("utf-8").split()
                new_uids = [u for u in uids if u not in seen_uids]
                seen_uids.update(new_uids)
                all_uids.extend(new_uids)

                logger.info(f" Found {len(new_uids)} UNSEEN email(s) from {sender}")

            except imaplib.IMAP4.error as e:
                error_msg = f"SEARCH command failed for sender {sender}: {e}"
                logger.error(f" {error_msg}")
                raise IMAPOperationError(error_msg)

            except IMAPConnectionError:
                raise

            except Exception as e:
                error_msg = f"Unexpected error searching for sender {sender}: {e}"
                logger.error(f" {error_msg}", exc_info=True)
                raise IMAPOperationError(error_msg)

        logger.info(f" Total UNSEEN emails found: {len(all_uids)}")
        logger.info("=" * 80)
        return all_uids

    def fetch_email_envelope(self, uid: str) -> Optional[InboxEmail]:

        self.ensure_connected()

        try:
            logger.debug(f" Fetching envelope for UID {uid}")

            status, data = self.mail.uid("FETCH", uid, "(RFC822.HEADER)")

            if status != "OK" or not data or data[0] is None:
                logger.warning(f"  FETCH envelope failed for UID {uid}: {status}")
                return None

            # data[0] is a tuple: (b'uid (RFC822.HEADER {size}', b'header bytes')
            raw_headers = data[0][1]
            if not isinstance(raw_headers, bytes):
                logger.warning(f"  Unexpected header format for UID {uid}")
                return None

            msg = email.message_from_bytes(raw_headers)

            from_header = msg.get("From", "")
            subject_header = msg.get("Subject", "(no subject)")
            date_header = msg.get("Date", "")
            message_id = msg.get("Message-ID", "")

            sender_address = extract_sender_address(from_header)
            subject = decode_mime_header(subject_header)

            # Parse date
            received_at: Optional[datetime] = None
            if date_header:
                try:
                    parsed_tuple = email.utils.parsedate_tz(date_header)
                    if parsed_tuple:
                        timestamp = email.utils.mktime_tz(parsed_tuple)
                        received_at = datetime.fromtimestamp(timestamp)
                except Exception:
                    pass

            inbox_email = InboxEmail(
                imap_uid=uid,
                subject=subject,
                sender=from_header,
                sender_address=sender_address,
                received_at=received_at,
                message_id=message_id.strip() if message_id else None,
            )

            logger.debug(
                f" Envelope parsed  UID {uid}: "
                f"from={sender_address}, subject='{subject[:60]}'"
            )
            return inbox_email

        except IMAPConnectionError:
            raise
        except IMAPOperationError:
            raise
        except imaplib.IMAP4.error as e:
            error_msg = f"FETCH envelope command failed for UID {uid}: {e}"
            logger.error(f" {error_msg}")
            raise IMAPOperationError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error fetching envelope for UID {uid}: {e}"
            logger.error(f" {error_msg}", exc_info=True)
            raise IMAPOperationError(error_msg)

    def fetch_full_message(self, uid: str) -> Optional[email.message.Message]:

        self.ensure_connected()
        try:
            status, data = self.mail.uid("FETCH", uid, "(RFC822)")

            if status != "OK" or not data or data[0] is None:
                logger.warning(f"  FETCH RFC822 failed for UID {uid}: {status}")
                return None

            raw_email = data[0][1]
            if not isinstance(raw_email, bytes):
                logger.warning(f"  Unexpected message format for UID {uid}")
                return None

            return email.message_from_bytes(raw_email)

        except imaplib.IMAP4.error as e:
            error_msg = f"FETCH RFC822 command failed for UID {uid}: {e}"
            logger.error(f" {error_msg}")
            raise IMAPOperationError(error_msg)
        except IMAPConnectionError:
            raise
        except Exception as e:
            error_msg = f"Unexpected error fetching full message for UID {uid}: {e}"
            logger.error(f" {error_msg}", exc_info=True)
            raise IMAPOperationError(error_msg)

    def extract_pdf_attachments(
        self,
        uid: str,
        inbox_email: InboxEmail,
    ) -> List[Tuple[str, bytes]]:

        msg = self.fetch_full_message(uid)
        if msg is None:
            logger.warning(f"  Could not fetch full message for UID {uid}")
            return []

        attachments: List[Tuple[str, bytes]] = []

        for part in msg.walk():
            # Only process attachments, not inline parts
            content_disposition = part.get_content_disposition()
            if content_disposition not in ("attachment", "inline"):
                continue

            raw_filename = part.get_filename()
            if not raw_filename:
                continue

            decoded_filename = decode_mime_header(raw_filename)
            if not decoded_filename.lower().endswith(".pdf"):
                logger.debug(
                    f"  Skipping non-PDF attachment: {decoded_filename} (UID {uid})"
                )
                continue

            file_bytes = part.get_payload(decode=True)
            if not file_bytes:
                logger.warning(
                    f"  PDF attachment '{decoded_filename}' has empty payload (UID {uid})"
                )
                continue

            safe_filename = sanitize_filename(decoded_filename)

            logger.info(
                f" Found PDF attachment: {safe_filename} "
                f"({len(file_bytes) / 1024:.1f} KB)  UID {uid}"
            )
            attachments.append((safe_filename, file_bytes))

        # Update attachment_count on the envelope model
        inbox_email.attachment_count = len(attachments)

        if not attachments:
            logger.info(f" No PDF attachments in email UID {uid} ('{inbox_email.subject[:60]}')")
        else:
            logger.info(
                f" Total PDF attachments in UID {uid}: {len(attachments)}"
            )

        return attachments
    # ----------------------------------------------------------------
    # Deduplication  (replaces UUID rename logic from SFTP module)
    # ----------------------------------------------------------------
    def is_duplicate(self, file_bytes: bytes) -> Tuple[bool, str]:

        sha256 = compute_sha256_hash(file_bytes)

        download_dir = Path(self.config.download_dir)
        if not download_dir.exists():
            return False, sha256

        # Check if any existing file name starts with the hash prefix (first 16 chars)
        hash_prefix = sha256[:16]
        for existing in download_dir.iterdir():
            if existing.is_file() and existing.name.startswith(hash_prefix):
                logger.warning(
                    f"  Duplicate detected (SHA-256 prefix: {hash_prefix}): "
                    f"matches existing file '{existing.name}'  skipping"
                )
                return True, sha256

        return False, sha256
    # ----------------------------------------------------------------
    # Local file I/O  (local disk replaces remote SFTP moves)
    # ----------------------------------------------------------------
    def save_attachment_to_disk(
        self,
        filename: str,
        file_bytes: bytes,
        sha256_hash: str,
    ) -> Optional[str]:

        try:
            if not ensure_local_directory(self.config.download_dir):
                logger.error(f" Cannot create download dir: {self.config.download_dir}")
                return None

            # Prefix filename with first 16 chars of hash for dedup tracking
            hash_prefix = sha256_hash[:16]
            saved_filename = f"{hash_prefix}_{filename}"
            local_path = Path(self.config.download_dir) / saved_filename

            with open(local_path, "wb") as f:
                f.write(file_bytes)

            file_size_mb = len(file_bytes) / (1024 * 1024)
            logger.info(
                f" Saved: {saved_filename} ({file_size_mb:.2f} MB) "
                f" {self.config.download_dir}"
            )
            return str(local_path.resolve())

        except OSError as e:
            logger.error(f" Failed to save '{filename}' to disk: {e}")
            return None
        except Exception as e:
            logger.error(f" Unexpected error saving '{filename}': {e}", exc_info=True)
            return None

    def move_to_processed(self, local_path: str) -> Tuple[bool, str]:
        return self._move_local_file(local_path, self.config.processed_dir)

    def move_to_failed(self, local_path: str) -> Tuple[bool, str]:
        return self._move_local_file(local_path, self.config.failed_dir)

    def _move_local_file(
        self, source_path: str, destination_dir: str
    ) -> Tuple[bool, str]:

        try:
            source = Path(source_path)

            if not source.exists():
                error_msg = f"Source file not found for move: {source_path}"
                logger.error(f" {error_msg}")
                raise IMAPOperationError(error_msg)

            if not ensure_local_directory(destination_dir):
                error_msg = f"Cannot create destination dir: {destination_dir}"
                logger.error(f" {error_msg}")
                raise IMAPOperationError(error_msg)

            dest_dir = Path(destination_dir)
            dest_path = dest_dir / source.name

            # Handle name collision with timestamp suffix
            if dest_path.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                stem = source.stem
                new_name = f"{stem}_{timestamp}{source.suffix}"
                dest_path = dest_dir / new_name
                logger.info(
                    f"  Name collision  renaming to avoid overwrite: {new_name}"
                )

            shutil.move(str(source), str(dest_path))
            final_path = str(dest_path.resolve())

            logger.info(
                f" Moved: {source.name}  {destination_dir} "
                f"(final: {dest_path.name})"
            )
            return True, final_path

        except IMAPOperationError:
            raise
        except Exception as e:
            error_msg = f"Failed to move '{source_path}' to '{destination_dir}': {e}"
            logger.error(f" {error_msg}", exc_info=True)
            raise IMAPOperationError(error_msg)
    # ----------------------------------------------------------------
    # Mark as seen  (no SFTP equivalent  email-specific post-process)
    # ----------------------------------------------------------------
    def mark_email_as_seen(self, uid: str) -> bool:

        if not self.config.mark_as_seen:
            logger.debug(f"  mark_as_seen=False  leaving UID {uid} unread")
            return True
        self.ensure_connected()
        try:
            status, data = self.mail.uid("STORE", uid, "+FLAGS", "\\Seen")

            if status != "OK":
                logger.warning(
                    f"  STORE +FLAGS \\Seen returned non-OK for UID {uid}: "
                    f"{status}  {data}"
                )
                return False
            logger.info(f"  Marked as Seen: UID {uid}")
            return True

        except imaplib.IMAP4.error as e:
            logger.error(f" STORE \\Seen failed for UID {uid}: {e}")
            return False
        except Exception as e:
            logger.error(
                f" Unexpected error marking UID {uid} as seen: {e}", exc_info=True
            )
            return False
# ---------------------------------------------------------------------------
# IMAPManager  (mirrors SFTPManager  high-level context manager)
# ---------------------------------------------------------------------------
class IMAPManager:

    def __init__(self, config: IMAPConnectorConfig):
        self.config = config
        self.client: Optional[IMAPClient] = None
        logger.info(" IMAPManager initialized")

    def __enter__(self) -> IMAPClient:
        self.client = IMAPClient(self.config)
        self.client.connect()   # May raise IMAPConnectionError
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit  close IMAP connection regardless of outcome"""
        if self.client:
            self.client.disconnect()
        return False    # Do not suppress exceptions

    def get_unseen_email_uids(self) -> List[str]:
        with self as client:
            return client.search_unseen_from_approved_senders()

    def download_attachment_and_save(
        self,
        uid: str,
        inbox_email: InboxEmail,
    ) -> List[Tuple[str, str, str]]:

        with self as client:
            raw_attachments = client.extract_pdf_attachments(uid, inbox_email)

            saved: List[Tuple[str, str, str]] = []

            for filename, file_bytes in raw_attachments:
                is_dup, sha256 = client.is_duplicate(file_bytes)

                if is_dup:
                    logger.info(
                        f"  Skipping duplicate PDF: {filename} "
                        f"(hash prefix: {sha256[:16]})"
                    )
                    continue
                local_path = client.save_attachment_to_disk(filename, file_bytes, sha256)
                if local_path:
                    saved.append((local_path, sha256, filename))
                else:
                    logger.error(f" Failed to save attachment: {filename}")

            return saved

    def move_processed_file(self, local_path: str) -> Tuple[bool, str]:
        with self as client:
            return client.move_to_processed(local_path)

    def move_failed_file(self, local_path: str) -> Tuple[bool, str]:
        with self as client:
            return client.move_to_failed(local_path)

    def mark_seen(self, uid: str) -> bool:
        with self as client:
            return client.mark_email_as_seen(uid)