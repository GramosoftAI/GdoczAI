# -*- coding: utf-8 -*-

import logging
import smtplib
from dataclasses import dataclass
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional
from src.services.smtp_fetch.smtp_fetcher_config import EmailNotificationConfig
logger = logging.getLogger(__name__)

class EmailSendError(Exception):
    pass

@dataclass
class IMAPFailureContext:
    imap_server: str
    imap_port: int
    email_id: str
    mailbox: str
    error_message: str
    exception_type: str
    exception_details: str
    timestamp: datetime
    approved_senders: Optional[List[str]] = None

    def get_approved_senders_display(self) -> str:
        """Return approved senders as a comma-separated string, or 'Not configured'"""
        if not self.approved_senders:
            return "Not configured"
        return ", ".join(self.approved_senders)

@dataclass
class IMAPRecoveryContext:
    imap_server: str
    imap_port: int
    email_id: str
    downtime_minutes: float
    timestamp: datetime

    def get_downtime_display(self) -> str:
        total = int(self.downtime_minutes)
        if total < 1:
            return "Less than 1 minute"
        if total < 60:
            return f"{total} minute{'s' if total != 1 else ''}"
        hours = total // 60
        mins = total % 60
        return f"{hours}h {mins}m"

@dataclass
class FileFailureContext:
    attachment_filename: str
    sender_address: str
    email_subject: str
    imap_uid: str
    document_type: str
    failure_stage: str
    error_message: str
    file_size_mb: float
    failed_dir_path: str
    local_path: str
    retry_count: int
    timestamp: datetime

    def get_failure_stage_emoji(self) -> str:
        """Return an emoji that visually categorises the failure stage"""
        stage_emojis = {
            "Authentication":          "",
            "File Read":               "",
            "Text Extraction":         "",
            "Document Type Detection": "",
            "OCR Processing":          "",
            "Post-Processing Check":   "",
            "Unknown":                 "",
        }
        return stage_emojis.get(self.failure_stage, "")

    def get_subject_truncated(self, max_len: int = 80) -> str:
        if len(self.email_subject) <= max_len:
            return self.email_subject
        return self.email_subject[:max_len - 3] + "..."

class EmailFetcherNotifier:

    def __init__(self, config: EmailNotificationConfig):
        self.config = config
        self.is_enabled = config.enabled

        if self.is_enabled:
            logger.info(" EmailFetcherNotifier initialised")
            logger.info(f"   SMTP     : {config.smtp_host}:{config.smtp_port}")
            logger.info(
                f"   From     : {config.from_name} <{config.from_email}>"
            )
            logger.info(
                f"   Developer recipients : {len(config.developer_recipients)}"
            )
            logger.info(
                f"   Client recipients    : {len(config.client_recipients)}"
            )
            logger.info(
                f"   All recipients       : {len(config.get_all_recipients())}"
            )
        else:
            logger.info(
                " EmailFetcherNotifier initialised (DISABLED  no alerts will be sent)"
            )

    def _create_smtp_connection(self) -> smtplib.SMTP:

        try:
            logger.debug(
                f" Connecting to SMTP: "
                f"{self.config.smtp_host}:{self.config.smtp_port}"
            )

            smtp = smtplib.SMTP(
                self.config.smtp_host,
                self.config.smtp_port,
                timeout=30,
            )

            if self.config.use_tls:
                logger.debug(" Starting TLS")
                smtp.starttls()

            logger.debug(f" Authenticating as: {self.config.smtp_username}")
            smtp.login(self.config.smtp_username, self.config.smtp_password)

            logger.debug(" SMTP connection established")
            return smtp

        except smtplib.SMTPAuthenticationError as e:
            raise EmailSendError(f"SMTP authentication failed: {e}")
        except smtplib.SMTPConnectError as e:
            raise EmailSendError(f"Failed to connect to SMTP server: {e}")
        except smtplib.SMTPException as e:
            raise EmailSendError(f"SMTP error: {e}")
        except Exception as e:
            raise EmailSendError(
                f"Unexpected error connecting to SMTP: {e}"
            )

    def _send_email(
        self,
        subject: str,
        html_body: str,
        plain_body: str,
        recipients: List[str],
    ) -> bool:

        if not recipients:
            logger.warning("  No recipients specified  skipping email")
            return False

        try:
            logger.info(f" Sending: '{subject}'")
            logger.info(f"   To: {', '.join(recipients)}")

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{self.config.from_name} <{self.config.from_email}>"
            msg["To"] = ", ".join(recipients)

            msg.attach(MIMEText(plain_body, "plain", "utf-8"))
            msg.attach(MIMEText(html_body, "html", "utf-8"))

            with self._create_smtp_connection() as smtp:
                smtp.send_message(msg)

            logger.info(
                f" Email sent to {len(recipients)} recipient(s)"
            )
            return True

        except EmailSendError:
            raise
        except Exception as e:
            raise EmailSendError(f"Failed to send email: {e}")

    def _format_imap_failure_html(self, ctx: IMAPFailureContext) -> str:

        senders_html = ""
        if ctx.approved_senders:
            rows = "".join(
                f"<li>{s}</li>" for s in ctx.approved_senders
            )
            senders_html = f"<h3>Approved Senders (affected):</h3><ul>{rows}</ul>"

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }}
        .container {{ max-width: 620px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #fd7e14; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
        .header h1 {{ margin: 0; font-size: 22px; }}
        .header p {{ margin: 6px 0 0; font-size: 13px; opacity: 0.9; }}
        .content {{ background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; }}
        .alert-box {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0; }}
        .detail-row {{ margin: 10px 0; }}
        .detail-label {{ font-weight: bold; color: #495057; min-width: 150px; display: inline-block; }}
        .detail-value {{ color: #212529; }}
        .error-box {{ background-color: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; margin: 15px 0;
                      border-radius: 4px; color: #721c24; font-family: monospace; font-size: 12px;
                      word-break: break-all; white-space: pre-wrap; }}
        .impact-box {{ background-color: #d1ecf1; border-left: 4px solid #0c5460; padding: 15px;
                       margin: 15px 0; color: #0c5460; }}
        .footer {{ background-color: #e9ecef; padding: 15px; border-radius: 0 0 5px 5px;
                   text-align: center; font-size: 12px; color: #6c757d; }}
        ul {{ padding-left: 20px; }} li {{ margin: 5px 0; }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1> CRITICAL: Gmail IMAP Connection Failed</h1>
        <p>Gmail  OCR Document Pipeline</p>
    </div>
    <div class="content">

        <div class="alert-box">
            <strong>Alert:</strong> The Gmail inbox monitor cannot connect to the IMAP server.
            PDF attachment processing has been paused until the connection is restored.
        </div>

        <h3>Connection Details:</h3>
        <div class="detail-row">
            <span class="detail-label">IMAP Server:</span>
            <span class="detail-value">{ctx.imap_server}:{ctx.imap_port}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Gmail Account:</span>
            <span class="detail-value">{ctx.email_id}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Mailbox:</span>
            <span class="detail-value">{ctx.mailbox}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Failure Time:</span>
            <span class="detail-value">{ctx.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Exception Type:</span>
            <span class="detail-value">{ctx.exception_type}</span>
        </div>

        <h3>Error Message:</h3>
        <div class="error-box">{ctx.error_message}

Exception detail:
{ctx.exception_details}</div>

        {senders_html}

        <div class="impact-box">
            <strong>Impact:</strong> All PDF attachment processing from Gmail is paused.
            Emails from approved senders will accumulate as UNSEEN in the inbox
            and will be automatically processed once the connection is restored.
        </div>

        <h3>Recommended Actions:</h3>
        <ul>
            <li>Verify Gmail IMAP is enabled in Gmail Settings  See all settings  Forwarding and POP/IMAP</li>
            <li>Check that the Gmail App Password is still valid (Settings  Google Account  Security  App passwords)</li>
            <li>Verify network connectivity from the server to <code>imap.gmail.com:993</code></li>
            <li>Check that the Gmail account has not been locked or suspended</li>
            <li>Review server logs for the full exception traceback</li>
            <li>Monitor for the automatic recovery notification</li>
        </ul>

    </div>
    <div class="footer">
        <p>Automated alert  Gmail  OCR Document Pipeline</p>
        <p>A recovery notification will be sent once the IMAP connection is restored.</p>
    </div>
</div>
</body>
</html>"""

    def _format_imap_failure_plain(self, ctx: IMAPFailureContext) -> str:

        senders_text = ""
        if ctx.approved_senders:
            senders_text = (
                "\nAPPROVED SENDERS (affected):\n"
                + "\n".join(f"  * {s}" for s in ctx.approved_senders)
                + "\n"
            )

        return f"""CRITICAL: Gmail IMAP Connection Failed
Gmail  OCR Document Pipeline

ALERT: The Gmail inbox monitor cannot connect to the IMAP server.
PDF attachment processing has been paused until the connection is restored.

================================================================================

CONNECTION DETAILS:

IMAP Server   : {ctx.imap_server}:{ctx.imap_port}
Gmail Account : {ctx.email_id}
Mailbox       : {ctx.mailbox}
Failure Time  : {ctx.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
Exception Type: {ctx.exception_type}

================================================================================

ERROR MESSAGE:

{ctx.error_message}

Exception detail:
{ctx.exception_details}

================================================================================
{senders_text}
IMPACT:

All PDF attachment processing from Gmail is paused.
Emails from approved senders will accumulate as UNSEEN in the inbox
and will be automatically processed once the connection is restored.

================================================================================

RECOMMENDED ACTIONS:

  * Verify Gmail IMAP is enabled:
    Gmail Settings  See all settings  Forwarding and POP/IMAP  Enable IMAP
  * Check that the Gmail App Password is still valid:
    Google Account  Security  App passwords
  * Verify network connectivity from server to imap.gmail.com:993
  * Check that the Gmail account has not been locked or suspended
  * Review server logs for the full exception traceback
  * Monitor for the automatic recovery notification

================================================================================

Automated alert  Gmail  OCR Document Pipeline
A recovery notification will be sent once the IMAP connection is restored.
"""

    def _format_imap_recovery_html(self, ctx: IMAPRecoveryContext) -> str:

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }}
        .container {{ max-width: 620px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #28a745; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
        .header h1 {{ margin: 0; font-size: 22px; }}
        .header p {{ margin: 6px 0 0; font-size: 13px; opacity: 0.9; }}
        .content {{ background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; }}
        .success-box {{ background-color: #d4edda; border-left: 4px solid #28a745; padding: 15px;
                        margin: 15px 0; color: #155724; }}
        .detail-row {{ margin: 10px 0; }}
        .detail-label {{ font-weight: bold; color: #495057; min-width: 150px; display: inline-block; }}
        .detail-value {{ color: #212529; }}
        .info-box {{ background-color: #d1ecf1; border-left: 4px solid #17a2b8; padding: 15px;
                     margin: 15px 0; color: #0c5460; }}
        .footer {{ background-color: #e9ecef; padding: 15px; border-radius: 0 0 5px 5px;
                   text-align: center; font-size: 12px; color: #6c757d; }}
        ul {{ padding-left: 20px; }} li {{ margin: 5px 0; }}
        .status-ok {{ color: #28a745; font-weight: bold; }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1> RESOLVED: Gmail IMAP Connection Restored</h1>
        <p>Gmail  OCR Document Pipeline</p>
    </div>
    <div class="content">

        <div class="success-box">
            <strong>Good News:</strong> The Gmail IMAP connection has been successfully restored.
            The OCR document pipeline has resumed normal operations.
        </div>

        <h3>Connection Details:</h3>
        <div class="detail-row">
            <span class="detail-label">IMAP Server:</span>
            <span class="detail-value">{ctx.imap_server}:{ctx.imap_port}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Gmail Account:</span>
            <span class="detail-value">{ctx.email_id}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Recovery Time:</span>
            <span class="detail-value">{ctx.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Total Downtime:</span>
            <span class="detail-value">{ctx.get_downtime_display()}</span>
        </div>

        <div class="info-box">
            <strong>Status:</strong> PDF attachment processing has automatically resumed.
            Any emails that arrived during the outage remain UNSEEN in the inbox
            and will be picked up during the next scheduled scan.
        </div>

        <h3>System Status:</h3>
        <ul>
            <li>IMAP connection: <span class="status-ok">OPERATIONAL</span></li>
            <li>Inbox monitoring: <span class="status-ok">ACTIVE</span></li>
            <li>PDF attachment processing: <span class="status-ok">RUNNING</span></li>
            <li>OCR pipeline: <span class="status-ok">RUNNING</span></li>
        </ul>

    </div>
    <div class="footer">
        <p>Automated recovery notification  Gmail  OCR Document Pipeline</p>
        <p>No action is required. Normal operations have resumed.</p>
    </div>
</div>
</body>
</html>"""

    def _format_imap_recovery_plain(self, ctx: IMAPRecoveryContext) -> str:

        return f"""RESOLVED: Gmail IMAP Connection Restored
Gmail  OCR Document Pipeline

GOOD NEWS: The Gmail IMAP connection has been successfully restored.
The OCR document pipeline has resumed normal operations.

================================================================================

CONNECTION DETAILS:

IMAP Server   : {ctx.imap_server}:{ctx.imap_port}
Gmail Account : {ctx.email_id}
Recovery Time : {ctx.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
Total Downtime: {ctx.get_downtime_display()}

================================================================================

STATUS:

PDF attachment processing has automatically resumed. Any emails that arrived
during the outage remain UNSEEN in the inbox and will be picked up during
the next scheduled scan.

SYSTEM STATUS:
  * IMAP connection         : OPERATIONAL
  * Inbox monitoring        : ACTIVE
  * PDF attachment processing: RUNNING
  * OCR pipeline            : RUNNING

================================================================================

Automated recovery notification  Gmail  OCR Document Pipeline
No action is required. Normal operations have resumed.
"""

    def _format_file_failure_html(self, ctx: FileFailureContext) -> str:

        stage_emoji = ctx.get_failure_stage_emoji()
        subject_display = ctx.get_subject_truncated(80)

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }}
        .container {{ max-width: 620px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #dc3545; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
        .header h1 {{ margin: 0; font-size: 22px; }}
        .header p {{ margin: 6px 0 0; font-size: 13px; opacity: 0.9; }}
        .content {{ background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; }}
        .alert-box {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0; }}
        .detail-row {{ margin: 10px 0; }}
        .detail-label {{ font-weight: bold; color: #495057; min-width: 180px; display: inline-block;
                         vertical-align: top; }}
        .detail-value {{ color: #212529; }}
        .error-box {{ background-color: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; margin: 15px 0;
                      border-radius: 4px; color: #721c24; font-family: monospace; font-size: 12px;
                      word-break: break-all; white-space: pre-wrap; }}
        .email-provenance {{ background-color: #e7f3ff; border-left: 4px solid #2196F3; padding: 15px;
                             margin: 15px 0; color: #0d47a1; }}
        .location-box {{ background-color: #e8f5e9; border-left: 4px solid #4caf50; padding: 15px;
                         margin: 15px 0; color: #1b5e20; }}
        .stage-badge {{ display: inline-block; padding: 5px 12px; border-radius: 4px;
                        background-color: #dc3545; color: white; font-size: 14px; margin: 8px 0; }}
        .footer {{ background-color: #e9ecef; padding: 15px; border-radius: 0 0 5px 5px;
                   text-align: center; font-size: 12px; color: #6c757d; }}
        ul {{ padding-left: 20px; }} li {{ margin: 5px 0; }}
        code {{ background: #f4f4f4; padding: 2px 5px; border-radius: 3px; font-size: 12px; }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1> PDF Attachment Processing Failed</h1>
        <p>Gmail  OCR Document Pipeline  Developer Alert</p>
    </div>
    <div class="content">

        <div class="alert-box">
            <strong>Developer Alert:</strong> A PDF attachment failed during processing and has been
            moved to the <code>failed_dir</code>. This alert is sent to developers only for investigation.
            Clients have not been notified.
        </div>

        <h3>Attachment Details:</h3>
        <div class="detail-row">
            <span class="detail-label">Filename:</span>
            <span class="detail-value">{ctx.attachment_filename}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">File Size:</span>
            <span class="detail-value">{ctx.file_size_mb:.2f} MB</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Document Type:</span>
            <span class="detail-value">{ctx.document_type}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Failure Time:</span>
            <span class="detail-value">{ctx.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</span>
        </div>

        <div class="email-provenance">
            <strong> Email Provenance</strong>  use these details to locate the email in Gmail:<br/><br/>
            <strong>From:</strong> {ctx.sender_address}<br/>
            <strong>Subject:</strong> {subject_display}<br/>
            <strong>IMAP UID:</strong> <code>{ctx.imap_uid}</code>
            <em style="font-size:11px;"> (search Gmail with: in:inbox uid:{ctx.imap_uid})</em>
        </div>

        <h3>Failure Information:</h3>
        <div class="stage-badge">{stage_emoji} Stage: {ctx.failure_stage}</div>
        <div class="detail-row">
            <span class="detail-label">Retry Count:</span>
            <span class="detail-value">{ctx.retry_count} (max retries exhausted)</span>
        </div>
        <div class="error-box">{ctx.error_message}</div>

        <div class="location-box">
            <strong> File Location:</strong><br/>
            Failed directory: <code>{ctx.failed_dir_path}</code><br/>
            Local path: <code>{ctx.local_path}</code>
        </div>

        <h3>Recommended Actions:</h3>
        <ul>
            <li>Locate the email in Gmail using the sender and subject above</li>
            <li>Open the attachment manually to check for corruption or invalid PDF format</li>
            <li>Review the error message above for the specific failure cause</li>
            <li>Verify the <strong>{ctx.failure_stage}</strong> stage is functioning correctly</li>
            <li>Check PDF text extraction  the file may be a scanned image without OCR layer</li>
            <li>Confirm the document type keywords are present in the PDF content</li>
            <li>Consider manual reprocessing if the file appears valid</li>
            <li>Review server logs for the full stack trace</li>
        </ul>

        <h3>System Context:</h3>
        <ul>
            <li>Processing Stage: {ctx.failure_stage}</li>
            <li>Retry Attempts: {ctx.retry_count}</li>
            <li>File Preserved: Yes (moved to failed_dir)</li>
            <li>Client Notification: No (this is a developer-only alert)</li>
            <li>Pipeline: Gmail  OCR (separate from SFTP pipeline)</li>
        </ul>

    </div>
    <div class="footer">
        <p><strong> Developer-only alert</strong>  clients have not been notified.</p>
        <p>Automated alert  Gmail  OCR Document Pipeline</p>
    </div>
</div>
</body>
</html>"""

    def _format_file_failure_plain(self, ctx: FileFailureContext) -> str:

        subject_display = ctx.get_subject_truncated(80)
        stage_emoji = ctx.get_failure_stage_emoji()

        return f"""PDF ATTACHMENT PROCESSING FAILURE ALERT
Gmail  OCR Document Pipeline  Developer Alert

DEVELOPER ALERT: A PDF attachment failed during processing and has been moved
to the failed_dir. This alert is sent to developers only. Clients have NOT
been notified.

================================================================================

ATTACHMENT DETAILS:

Filename       : {ctx.attachment_filename}
File Size      : {ctx.file_size_mb:.2f} MB
Document Type  : {ctx.document_type}
Failure Time   : {ctx.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

================================================================================

EMAIL PROVENANCE (use to locate the email in Gmail):

Sender         : {ctx.sender_address}
Subject        : {subject_display}
IMAP UID       : {ctx.imap_uid}

Tip: In Gmail search, use  from:{ctx.sender_address}  to find the email.

================================================================================

FAILURE INFORMATION:

Stage          : {stage_emoji} {ctx.failure_stage}
Retry Count    : {ctx.retry_count} (max retries exhausted)

Error Message:
{ctx.error_message}

================================================================================

FILE LOCATION:

Failed directory : {ctx.failed_dir_path}
Local path       : {ctx.local_path}

================================================================================

RECOMMENDED ACTIONS:

  * Locate the email in Gmail using the sender/subject above
  * Open the attachment to check for corruption or invalid PDF format
  * Review the error message for the specific failure cause
  * Verify the '{ctx.failure_stage}' stage is functioning correctly
  * Check if PDF text extraction works  file may be scanned image without OCR layer
  * Confirm document type keywords are present in the PDF content
  * Consider manual reprocessing if the file appears valid
  * Review server logs for the full stack trace

SYSTEM CONTEXT:

  * Processing Stage       : {ctx.failure_stage}
  * Retry Attempts         : {ctx.retry_count}
  * File Preserved         : Yes (moved to failed_dir)
  * Client Notification    : No (developers only)
  * Pipeline               : Gmail  OCR (separate from SFTP pipeline)

================================================================================

 DEVELOPER-ONLY ALERT  clients have not been notified.
Automated alert  Gmail  OCR Document Pipeline
"""

    # ----------------------------------------------------------------
    # Public send methods  (mirror EmailNotifier send_* methods)
    # ----------------------------------------------------------------

    def send_imap_failure_alert(self, ctx: IMAPFailureContext) -> bool:

        if not self.is_enabled:
            logger.info(" Email alerts disabled  skipping IMAP failure notification")
            return False

        try:
            logger.info("=" * 80)
            logger.info(" SENDING IMAP FAILURE ALERT")
            logger.info("=" * 80)
            logger.info(f"   IMAP   : {ctx.imap_server}:{ctx.imap_port}")
            logger.info(f"   Account: {ctx.email_id}")
            logger.info(f"   Error  : {ctx.exception_type}")

            subject = "[CRITICAL] Gmail IMAP Connection Failed  OCR Pipeline"
            html_body = self._format_imap_failure_html(ctx)
            plain_body = self._format_imap_failure_plain(ctx)
            recipients = self.config.get_all_recipients()

            success = self._send_email(subject, html_body, plain_body, recipients)

            if success:
                logger.info("=" * 80)
                logger.info(" IMAP FAILURE ALERT SENT SUCCESSFULLY")
                logger.info(f"   Sent to: {', '.join(recipients)}")
                logger.info("=" * 80)

            return success

        except EmailSendError as e:
            logger.error(f" Failed to send IMAP failure alert: {e}")
            return False
        except Exception as e:
            logger.error(
                f" Unexpected error sending IMAP failure alert: {e}",
                exc_info=True,
            )
            return False

    def send_imap_recovery_alert(self, ctx: IMAPRecoveryContext) -> bool:

        if not self.is_enabled:
            logger.info(" Email alerts disabled  skipping IMAP recovery notification")
            return False

        try:
            logger.info("=" * 80)
            logger.info(" SENDING IMAP RECOVERY ALERT")
            logger.info("=" * 80)
            logger.info(f"   Account   : {ctx.email_id}")
            logger.info(f"   Downtime  : {ctx.get_downtime_display()}")

            subject = "[RESOLVED] Gmail IMAP Connection Restored  OCR Pipeline"
            html_body = self._format_imap_recovery_html(ctx)
            plain_body = self._format_imap_recovery_plain(ctx)
            recipients = self.config.get_all_recipients()

            success = self._send_email(subject, html_body, plain_body, recipients)

            if success:
                logger.info("=" * 80)
                logger.info(" IMAP RECOVERY ALERT SENT SUCCESSFULLY")
                logger.info(f"   Sent to: {', '.join(recipients)}")
                logger.info("=" * 80)

            return success

        except EmailSendError as e:
            logger.error(f" Failed to send IMAP recovery alert: {e}")
            return False
        except Exception as e:
            logger.error(
                f" Unexpected error sending IMAP recovery alert: {e}",
                exc_info=True,
            )
            return False

    def send_file_failure_alert(self, ctx: FileFailureContext) -> bool:

        if not self.is_enabled:
            logger.info(" Email alerts disabled  skipping file failure notification")
            return False

        if not self.config.developer_recipients:
            logger.warning(
                "  No developer recipients configured  "
                "skipping file failure alert"
            )
            return False

        try:
            logger.info("=" * 80)
            logger.info(" SENDING FILE FAILURE ALERT (DEVELOPERS ONLY)")
            logger.info("=" * 80)
            logger.info(f"   File  : {ctx.attachment_filename}")
            logger.info(f"   Sender: {ctx.sender_address}")
            logger.info(f"   Stage : {ctx.failure_stage}")

            subject = (
                f"[ALERT] PDF Attachment Failed: {ctx.attachment_filename} "
                f"(from {ctx.sender_address})"
            )
            html_body = self._format_file_failure_html(ctx)
            plain_body = self._format_file_failure_plain(ctx)

            # Developers only  never send to clients
            recipients = self.config.developer_recipients

            logger.info(
                f"   Sending to {len(recipients)} developer(s) only"
            )
            logger.info("   Clients will NOT be notified")

            success = self._send_email(subject, html_body, plain_body, recipients)

            if success:
                logger.info("=" * 80)
                logger.info(" FILE FAILURE ALERT SENT SUCCESSFULLY")
                logger.info(f"   Developers: {', '.join(recipients)}")
                logger.info("   Clients excluded from this alert")
                logger.info("=" * 80)

            return success

        except EmailSendError as e:
            logger.error(f" Failed to send file failure alert: {e}")
            return False
        except Exception as e:
            logger.error(
                f" Unexpected error sending file failure alert: {e}",
                exc_info=True,
            )
            return False