# -*- coding: utf-8 -*-

"""
Configuration management for Gmail -> OCR Email Fetcher.
Provides typed configuration objects for IMAP connections from the database.
"""

import re
from typing import List
from dataclasses import dataclass, field
import logging

from src.services.sftp_fetch.sftp_fetch_config import (
    SlimPipelineConfig,
    get_slim_config,
    AuthConfig,
    OCRConfig,
    SlimSchedulerConfig,
)

logger = logging.getLogger(__name__)

@dataclass
class IMAPConnectorConfig:

    email_id: str               # Email address to log in as (IMAP username)
    app_password: str           # App password for IMAP authentication
    imap_server: str            # e.g. "imap.gmail.com" or "imap.hostinger.com"
    imap_port: int = 993        # Always 993 for SSL
    approved_senders: List[str] = field(default_factory=list)
    email_method: str = "gmail" # "gmail" or "hostinger" (source of truth from DB)
    download_dir: str = "downloads/email_pdfs"        # Local dir to save attachments
    processed_dir: str = "downloads/email_pdfs/processed"
    failed_dir: str = "downloads/email_pdfs/failed"
    mailbox: str = "INBOX"
    mark_as_seen: bool = True
    deduplicate_by_hash: bool = True

    def __post_init__(self):
        if not self.email_id:
            raise ValueError("IMAP email_id cannot be empty")
        if not self.app_password:
            raise ValueError("IMAP app_password cannot be empty")
        if not self.imap_server:
            raise ValueError("IMAP server cannot be empty")
        if not self.approved_senders:
            raise ValueError("At least one approved sender must be specified")

    def get_approved_senders_lower(self) -> List[str]:
        return [s.lower() for s in self.approved_senders]


@dataclass
class EmailNotificationConfig:
    """SMTP alert configuration for email notifications."""
    enabled: bool
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    from_email: str
    from_name: str
    use_tls: bool
    developer_recipients: List[str]
    client_recipients: List[str]
    alert_cooldown_minutes: int = 30

    def __post_init__(self):
        if not self.enabled:
            return

        if not self.smtp_host:
            raise ValueError("SMTP host cannot be empty when email alerts are enabled")
        if not (1 <= self.smtp_port <= 65535):
            raise ValueError("SMTP port must be between 1 and 65535")
        if not self.smtp_username:
            raise ValueError("SMTP username cannot be empty when email alerts are enabled")
        if not self.smtp_password:
            raise ValueError("SMTP password cannot be empty when email alerts are enabled")
        if not self.from_email:
            raise ValueError("From email cannot be empty when email alerts are enabled")
        if not self.from_name:
            raise ValueError("From name cannot be empty when email alerts are enabled")

        all_recipients = self.developer_recipients + self.client_recipients
        if not all_recipients:
            raise ValueError("At least one recipient (developer or client) must be configured")

        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        for addr in all_recipients:
            if not re.match(email_pattern, addr):
                raise ValueError(f"Invalid recipient email format: {addr}")

        if self.alert_cooldown_minutes < 0:
            raise ValueError("Alert cooldown minutes cannot be negative")

    def get_all_recipients(self) -> List[str]:
        return self.developer_recipients + self.client_recipients

    def has_recipients(self) -> bool:
        return len(self.get_all_recipients()) > 0