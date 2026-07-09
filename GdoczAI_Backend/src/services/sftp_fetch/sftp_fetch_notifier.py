# -*- coding: utf-8 -*-

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass

from src.services.sftp_fetch.sftp_fetch_config import EmailConfig

logger = logging.getLogger(__name__)


class EmailSendError(Exception):
    pass

@dataclass
class SFTPFailureContext:
    """Context information for SFTP failure alert"""
    host: str
    port: int
    username: str
    auth_method: str  # "password" or "ssh_key"
    error_message: str
    timestamp: datetime
    monitored_folders: List[str]
    
    def get_auth_display(self) -> str:
        """Get human-readable authentication method"""
        if self.auth_method == "ssh_key":
            return "SSH Key Authentication"
        else:
            return "Password Authentication"


@dataclass
class SFTPRecoveryContext:
    """Context information for SFTP recovery alert"""
    host: str
    port: int
    username: str
    timestamp: datetime
    downtime_duration: Optional[str] = None  # e.g., "15 minutes"


@dataclass
class FileFailureContext:

    filename: str
    original_filename: str  # Original name before UUID rename
    source_folder: str
    document_type: str
    file_size_mb: float
    error_message: str
    failure_stage: str  # "Authentication", "Download", "OCR Processing", "Unknown"
    timestamp: datetime
    retry_count: int
    failed_folder_path: str
    was_renamed: bool  # True if UUID was appended
    
    def get_failure_stage_emoji(self) -> str:
        """Get emoji for failure stage"""
        stage_emojis = {
            "Authentication": "??",
            "Download": "??",
            "OCR Processing": "??",
            "File Movement": "??",
            "Unknown": "?"
        }
        return stage_emojis.get(self.failure_stage, "?")


class EmailNotifier:

    def __init__(self, email_config: EmailConfig):

        self.config = email_config
        self.is_enabled = email_config.enabled
        
        if self.is_enabled:
            logger.info("?? EmailNotifier initialized")
            logger.info(f"   SMTP: {email_config.smtp_host}:{email_config.smtp_port}")
            logger.info(f"   From: {email_config.from_name} <{email_config.from_email}>")
            logger.info(f"   Recipients: {len(email_config.get_all_recipients())}")
            logger.info(f"   Developer recipients: {len(email_config.developer_recipients)}")
            logger.info(f"   Client recipients: {len(email_config.client_recipients)}")
        else:
            logger.info("?? EmailNotifier initialized (DISABLED - no emails will be sent)")
    
    def _create_smtp_connection(self) -> smtplib.SMTP:

        try:
            logger.debug(f"?? Connecting to SMTP server: {self.config.smtp_host}:{self.config.smtp_port}")
            
            # Create SMTP connection
            smtp_server = smtplib.SMTP(
                self.config.smtp_host,
                self.config.smtp_port,
                timeout=30
            )
            
            # Enable TLS if configured
            if self.config.use_tls:
                logger.debug("?? Starting TLS encryption")
                smtp_server.starttls()
            
            # Authenticate
            logger.debug(f"?? Authenticating as: {self.config.smtp_username}")
            smtp_server.login(self.config.smtp_username, self.config.smtp_password)
            
            logger.debug("? SMTP connection established")
            return smtp_server
        
        except smtplib.SMTPAuthenticationError as e:
            error_msg = f"SMTP authentication failed: {str(e)}"
            logger.error(f"? {error_msg}")
            raise EmailSendError(error_msg)
        
        except smtplib.SMTPConnectError as e:
            error_msg = f"Failed to connect to SMTP server: {str(e)}"
            logger.error(f"? {error_msg}")
            raise EmailSendError(error_msg)
        
        except smtplib.SMTPException as e:
            error_msg = f"SMTP error: {str(e)}"
            logger.error(f"? {error_msg}")
            raise EmailSendError(error_msg)
        
        except Exception as e:
            error_msg = f"Unexpected error connecting to SMTP: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            raise EmailSendError(error_msg)
    
    def _send_email(
        self,
        subject: str,
        html_body: str,
        plain_body: str,
        recipients: List[str]
    ) -> bool:

        if not recipients:
            logger.warning("?? No recipients specified, skipping email")
            return False
        
        try:
            logger.info(f"?? Sending email: '{subject}'")
            logger.info(f"   To: {', '.join(recipients)}")
            
            # Create message
            message = MIMEMultipart('alternative')
            message['Subject'] = subject
            message['From'] = f"{self.config.from_name} <{self.config.from_email}>"
            message['To'] = ', '.join(recipients)
            
            # Attach plain text and HTML versions
            part_plain = MIMEText(plain_body, 'plain', 'utf-8')
            part_html = MIMEText(html_body, 'html', 'utf-8')
            
            message.attach(part_plain)
            message.attach(part_html)
            
            # Send email
            with self._create_smtp_connection() as smtp_server:
                smtp_server.send_message(message)
            
            logger.info(f"? Email sent successfully to {len(recipients)} recipient(s)")
            return True
        
        except EmailSendError:
            raise
        
        except Exception as e:
            error_msg = f"Failed to send email: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            raise EmailSendError(error_msg)
    
    def _format_failure_email_html(self, context: SFTPFailureContext) -> str:

        monitored_folders_html = "<br/>".join([
            f"&nbsp;&nbsp;* {folder}" for folder in context.monitored_folders
        ])
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #dc3545; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .content {{ background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; }}
        .alert-box {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0; }}
        .detail-row {{ margin: 10px 0; }}
        .detail-label {{ font-weight: bold; color: #495057; }}
        .detail-value {{ color: #212529; margin-left: 10px; }}
        .error-message {{ background-color: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; margin: 15px 0; border-radius: 4px; color: #721c24; font-family: monospace; font-size: 12px; }}
        .footer {{ background-color: #e9ecef; padding: 15px; border-radius: 0 0 5px 5px; text-align: center; font-size: 12px; color: #6c757d; }}
        .impact {{ background-color: #d1ecf1; border-left: 4px solid #0c5460; padding: 15px; margin: 15px 0; color: #0c5460; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>?? CRITICAL: SFTP Connection Failed</h1>
        </div>
        
        <div class="content">
            <div class="alert-box">
                <strong>Alert:</strong> The OCR document pipeline cannot connect to the SFTP server. 
                Document processing has been paused until the connection is restored.
            </div>
            
            <h3>Connection Details:</h3>
            <div class="detail-row">
                <span class="detail-label">Host:</span>
                <span class="detail-value">{context.host}:{context.port}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Username:</span>
                <span class="detail-value">{context.username}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Authentication:</span>
                <span class="detail-value">{context.get_auth_display()}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Failure Time:</span>
                <span class="detail-value">{context.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</span>
            </div>
            
            <h3>Error Message:</h3>
            <div class="error-message">
                {context.error_message}
            </div>
            
            <h3>Affected Folders:</h3>
            <div style="margin-left: 20px; color: #495057;">
                {monitored_folders_html}
            </div>
            
            <div class="impact">
                <strong>Impact:</strong> All PDF document processing is paused. 
                New documents in monitored folders will not be processed until SFTP connection is restored.
            </div>
            
            <h3>Recommended Actions:</h3>
            <ul>
                <li>Verify SFTP server is running and accessible</li>
                <li>Check network connectivity and firewall rules</li>
                <li>Validate authentication credentials (password or SSH key)</li>
                <li>Review SFTP server logs for connection attempts</li>
                <li>Monitor for automatic recovery notification</li>
            </ul>
        </div>
        
        <div class="footer">
            <p>This is an automated alert from the OCR Document Pipeline System.</p>
            <p>You will receive a recovery notification once the SFTP connection is restored.</p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def _format_failure_email_plain(self, context: SFTPFailureContext) -> str:
        """
        Format plain text body for SFTP failure alert
        
        Args:
            context: Failure context information
            
        Returns:
            str: Plain text formatted email body
        """
        monitored_folders_text = "\n".join([
            f"  * {folder}" for folder in context.monitored_folders
        ])
        
        text = f"""
CRITICAL: SFTP Connection Failed

ALERT: The OCR document pipeline cannot connect to the SFTP server.
Document processing has been paused until the connection is restored.

================================================================================

CONNECTION DETAILS:

Host: {context.host}:{context.port}
Username: {context.username}
Authentication: {context.get_auth_display()}
Failure Time: {context.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

ERROR MESSAGE:

{context.error_message}

================================================================================

AFFECTED FOLDERS:

{monitored_folders_text}

================================================================================

IMPACT:
All PDF document processing is paused. New documents in monitored folders 
will not be processed until SFTP connection is restored.

RECOMMENDED ACTIONS:
  * Verify SFTP server is running and accessible
  * Check network connectivity and firewall rules
  * Validate authentication credentials (password or SSH key)
  * Review SFTP server logs for connection attempts
  * Monitor for automatic recovery notification

================================================================================

This is an automated alert from the OCR Document Pipeline System.
You will receive a recovery notification once the SFTP connection is restored.
"""
        return text
    
    def _format_recovery_email_html(self, context: SFTPRecoveryContext) -> str:
        """
        Format HTML body for SFTP recovery alert
        
        Args:
            context: Recovery context information
            
        Returns:
            str: HTML formatted email body
        """
        downtime_html = ""
        if context.downtime_duration:
            downtime_html = f"""
            <div class="detail-row">
                <span class="detail-label">Downtime:</span>
                <span class="detail-value">{context.downtime_duration}</span>
            </div>
            """
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #28a745; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .content {{ background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; }}
        .success-box {{ background-color: #d4edda; border-left: 4px solid #28a745; padding: 15px; margin: 15px 0; color: #155724; }}
        .detail-row {{ margin: 10px 0; }}
        .detail-label {{ font-weight: bold; color: #495057; }}
        .detail-value {{ color: #212529; margin-left: 10px; }}
        .footer {{ background-color: #e9ecef; padding: 15px; border-radius: 0 0 5px 5px; text-align: center; font-size: 12px; color: #6c757d; }}
        .info-box {{ background-color: #d1ecf1; border-left: 4px solid #17a2b8; padding: 15px; margin: 15px 0; color: #0c5460; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>?? RESOLVED: SFTP Connection Restored</h1>
        </div>
        
        <div class="content">
            <div class="success-box">
                <strong>Good News:</strong> The SFTP connection has been successfully restored. 
                The OCR document pipeline has resumed normal operations.
            </div>
            
            <h3>Connection Details:</h3>
            <div class="detail-row">
                <span class="detail-label">Host:</span>
                <span class="detail-value">{context.host}:{context.port}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Username:</span>
                <span class="detail-value">{context.username}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Recovery Time:</span>
                <span class="detail-value">{context.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</span>
            </div>
            {downtime_html}
            
            <div class="info-box">
                <strong>Status:</strong> Document processing has automatically resumed. 
                The pipeline will now process any PDFs that accumulated during the outage.
            </div>
            
            <h3>System Status:</h3>
            <ul>
                <li>SFTP connection: <strong style="color: #28a745;">OPERATIONAL</strong></li>
                <li>File monitoring: <strong style="color: #28a745;">ACTIVE</strong></li>
                <li>OCR processing: <strong style="color: #28a745;">RUNNING</strong></li>
            </ul>
        </div>
        
        <div class="footer">
            <p>This is an automated recovery notification from the OCR Document Pipeline System.</p>
            <p>No action is required. Normal operations have resumed.</p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def _format_recovery_email_plain(self, context: SFTPRecoveryContext) -> str:

        downtime_text = ""
        if context.downtime_duration:
            downtime_text = f"Downtime: {context.downtime_duration}\n"
        
        text = f"""
RESOLVED: SFTP Connection Restored

GOOD NEWS: The SFTP connection has been successfully restored.
The OCR document pipeline has resumed normal operations.

================================================================================

CONNECTION DETAILS:

Host: {context.host}:{context.port}
Username: {context.username}
Recovery Time: {context.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
{downtime_text}
================================================================================

STATUS:
Document processing has automatically resumed. The pipeline will now 
process any PDFs that accumulated during the outage.

SYSTEM STATUS:
  * SFTP connection: OPERATIONAL
  * File monitoring: ACTIVE
  * OCR processing: RUNNING

================================================================================

This is an automated recovery notification from the OCR Document Pipeline System.
No action is required. Normal operations have resumed.
"""
        return text
    
    def _format_file_failure_email_html(self, context: FileFailureContext) -> str:

        # Determine rename status
        rename_info = ""
        if context.was_renamed:
            rename_info = f"""
            <div class="detail-row">
                <span class="detail-label">Original Filename:</span>
                <span class="detail-value">{context.original_filename}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Renamed To:</span>
                <span class="detail-value">{context.filename}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Rename Reason:</span>
                <span class="detail-value">Duplicate filename existed in Failed_folder</span>
            </div>
            """
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #ff6b6b; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .content {{ background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; }}
        .alert-box {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0; }}
        .detail-row {{ margin: 10px 0; }}
        .detail-label {{ font-weight: bold; color: #495057; }}
        .detail-value {{ color: #212529; margin-left: 10px; }}
        .error-message {{ background-color: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; margin: 15px 0; border-radius: 4px; color: #721c24; font-family: monospace; font-size: 12px; word-wrap: break-word; }}
        .footer {{ background-color: #e9ecef; padding: 15px; border-radius: 0 0 5px 5px; text-align: center; font-size: 12px; color: #6c757d; }}
        .location-box {{ background-color: #e7f3ff; border-left: 4px solid #2196F3; padding: 15px; margin: 15px 0; color: #0d47a1; }}
        .stage-badge {{ display: inline-block; padding: 5px 10px; border-radius: 3px; background-color: #dc3545; color: white; font-size: 14px; margin: 10px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>?? PDF Processing Failure Alert</h1>
        </div>
        
        <div class="content">
            <div class="alert-box">
                <strong>Developer Alert:</strong> A PDF file failed during processing and has been moved to the Failed_folder. 
                This alert is sent to developers only for investigation.
            </div>
            
            <h3>File Details:</h3>
            <div class="detail-row">
                <span class="detail-label">Filename:</span>
                <span class="detail-value">{context.filename}</span>
            </div>
            {rename_info}
            <div class="detail-row">
                <span class="detail-label">Source Folder:</span>
                <span class="detail-value">{context.source_folder}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Document Type:</span>
                <span class="detail-value">{context.document_type}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">File Size:</span>
                <span class="detail-value">{context.file_size_mb:.2f} MB</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Failure Time:</span>
                <span class="detail-value">{context.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</span>
            </div>
            
            <h3>Error Information:</h3>
            <div class="stage-badge">
                {context.get_failure_stage_emoji()} Stage: {context.failure_stage}
            </div>
            <div class="error-message">
                {context.error_message}
            </div>
            <div class="detail-row">
                <span class="detail-label">Retry Count:</span>
                <span class="detail-value">{context.retry_count} (max retries exhausted)</span>
            </div>
            
            <div class="location-box">
                <strong>?? File Location:</strong><br/>
                Failed Folder: <code>{context.failed_folder_path}</code><br/>
                {'<em>Note: File was renamed with UUID due to duplicate name</em>' if context.was_renamed else ''}
            </div>
            
            <h3>Recommended Actions:</h3>
            <ul>
                <li>Check the file in Failed_folder for corruption or invalid format</li>
                <li>Review error message above for specific failure cause</li>
                <li>Verify {context.failure_stage} stage is functioning correctly</li>
                <li>Check if document type mapping is correct for this folder</li>
                <li>Consider manual reprocessing if file appears valid</li>
                <li>Review logs for additional context and stack traces</li>
            </ul>
            
            <h3>System Context:</h3>
            <ul>
                <li>Processing Stage: {context.failure_stage}</li>
                <li>Retry Attempts: {context.retry_count} times</li>
                <li>File Preserved: Yes (in Failed_folder)</li>
                <li>Client Notification: No (developers only)</li>
            </ul>
        </div>
        
        <div class="footer">
            <p><strong>?? This is a developer-only alert</strong></p>
            <p>Clients have not been notified of this file-level failure.</p>
            <p>This is an automated alert from the OCR Document Pipeline System.</p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def _format_file_failure_email_plain(self, context: FileFailureContext) -> str:

        # Determine rename status
        rename_info = ""
        if context.was_renamed:
            rename_info = f"""
Original Filename: {context.original_filename}
Renamed To: {context.filename}
Rename Reason: Duplicate filename existed in Failed_folder
"""
        
        text = f"""
PDF PROCESSING FAILURE ALERT

DEVELOPER ALERT: A PDF file failed during processing and has been moved to 
the Failed_folder. This alert is sent to developers only for investigation.

================================================================================

FILE DETAILS:

Filename: {context.filename}
{rename_info}Source Folder: {context.source_folder}
Document Type: {context.document_type}
File Size: {context.file_size_mb:.2f} MB
Failure Time: {context.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

================================================================================

ERROR INFORMATION:

Stage: {context.failure_stage}
Retry Count: {context.retry_count} (max retries exhausted)

Error Message:
{context.error_message}

================================================================================

FILE LOCATION:

Failed Folder: {context.failed_folder_path}
{'Note: File was renamed with UUID due to duplicate name' if context.was_renamed else ''}

================================================================================

RECOMMENDED ACTIONS:

  * Check the file in Failed_folder for corruption or invalid format
  * Review error message above for specific failure cause
  * Verify {context.failure_stage} stage is functioning correctly
  * Check if document type mapping is correct for this folder
  * Consider manual reprocessing if file appears valid
  * Review logs for additional context and stack traces

SYSTEM CONTEXT:

  * Processing Stage: {context.failure_stage}
  * Retry Attempts: {context.retry_count} times
  * File Preserved: Yes (in Failed_folder)
  * Client Notification: No (developers only)

================================================================================

?? THIS IS A DEVELOPER-ONLY ALERT

Clients have not been notified of this file-level failure.
This is an automated alert from the OCR Document Pipeline System.
"""
        return text
    
    def send_sftp_failure_alert(self, context: SFTPFailureContext) -> bool:
        
        if not self.is_enabled:
            logger.info("?? Email alerts disabled, skipping failure notification")
            return False
        
        try:
            logger.info("=" * 80)
            logger.info("?? SENDING SFTP FAILURE ALERT")
            logger.info("=" * 80)
            
            # Format email content
            subject = "[CRITICAL] SFTP Connection Failed - OCR Pipeline"
            html_body = self._format_failure_email_html(context)
            plain_body = self._format_failure_email_plain(context)
            
            # Get all recipients (developers + clients)
            recipients = self.config.get_all_recipients()
            
            # Send email
            success = self._send_email(subject, html_body, plain_body, recipients)
            
            if success:
                logger.info("=" * 80)
                logger.info("? SFTP FAILURE ALERT SENT SUCCESSFULLY")
                logger.info("=" * 80)
            
            return success
        
        except EmailSendError as e:
            logger.error(f"? Failed to send SFTP failure alert: {e}")
            return False
        
        except Exception as e:
            logger.error(f"? Unexpected error sending failure alert: {e}", exc_info=True)
            return False
    
    def send_sftp_recovery_alert(self, context: SFTPRecoveryContext) -> bool:

        if not self.is_enabled:
            logger.info("?? Email alerts disabled, skipping recovery notification")
            return False
        
        try:
            logger.info("=" * 80)
            logger.info("?? SENDING SFTP RECOVERY ALERT")
            logger.info("=" * 80)
            
            # Format email content
            subject = "[RESOLVED] SFTP Connection Restored - OCR Pipeline"
            html_body = self._format_recovery_email_html(context)
            plain_body = self._format_recovery_email_plain(context)
            
            # Get all recipients (developers + clients)
            recipients = self.config.get_all_recipients()
            
            # Send email
            success = self._send_email(subject, html_body, plain_body, recipients)
            
            if success:
                logger.info("=" * 80)
                logger.info("? SFTP RECOVERY ALERT SENT SUCCESSFULLY")
                logger.info("=" * 80)
            
            return success
        
        except EmailSendError as e:
            logger.error(f"? Failed to send SFTP recovery alert: {e}")
            return False
        
        except Exception as e:
            logger.error(f"? Unexpected error sending recovery alert: {e}", exc_info=True)
            return False
    
    def send_file_failure_alert(self, context: FileFailureContext) -> bool:

        if not self.is_enabled:
            logger.info("?? Email alerts disabled, skipping file failure notification")
            return False
        
        # Check if developer recipients are configured
        if not self.config.developer_recipients:
            logger.warning("?? No developer recipients configured, skipping file failure alert")
            return False
        
        try:
            logger.info("=" * 80)
            logger.info("?? SENDING FILE FAILURE ALERT (DEVELOPERS ONLY)")
            logger.info("=" * 80)
            logger.info(f"?? File: {context.filename}")
            logger.info(f"? Stage: {context.failure_stage}")
            
            # Format email content
            subject = f"[ALERT] PDF Processing Failed: {context.original_filename}"
            html_body = self._format_file_failure_email_html(context)
            plain_body = self._format_file_failure_email_plain(context)
            
            # Get ONLY developer recipients (NOT clients)
            recipients = self.config.developer_recipients
            
            logger.info(f"????? Sending to {len(recipients)} developer(s) only")
            logger.info(f"?? Clients will NOT be notified")
            
            # Send email
            success = self._send_email(subject, html_body, plain_body, recipients)
            
            if success:
                logger.info("=" * 80)
                logger.info("? FILE FAILURE ALERT SENT SUCCESSFULLY")
                logger.info("=" * 80)
                logger.info(f"?? Sent to developers: {', '.join(recipients)}")
                logger.info(f"?? Clients excluded from this alert")
            
            return success
        
        except EmailSendError as e:
            logger.error(f"? Failed to send file failure alert: {e}")
            return False
        
        except Exception as e:
            logger.error(f"? Unexpected error sending file failure alert: {e}", exc_info=True)
            return False