"""
Email notification service for OCR failure alerts.

Sends email notifications when OCR processing fails.
Supports user's primary email plus CC recipients from database configuration.
Includes processing metrics, error details, and missing keys information.
"""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Tuple
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


def send_ocr_failure_email(
    config,
    filename: str,
    user_id: int,
    document_type: str,
    processing_time: float,
    page_count: int,
    request_id: str,
    error_details: str,
    ocr_engine: str = "GDocz",
    missed_keys: Optional[List[str]] = None
) -> bool:

    try:
        # STEP 1: Check if email is enabled in config
        email_config = config.config.get('fallback_notifications', {}).get('email', {})
        
        if not email_config.get('enabled', False):
            logger.info("?? Email notifications disabled in config")
            return False
        
        logger.info("=" * 80)
        logger.info("?? STEP 1: Email notifications ENABLED")
        logger.info("=" * 80)
        
        # STEP 2: Get SMTP configuration
        smtp_server = email_config.get('smtp_server')
        smtp_port = email_config.get('smtp_port')
        smtp_username = email_config.get('smtp_username')
        smtp_password = email_config.get('smtp_password')
        sender_email = email_config.get('sender_email')
        
        if not all([smtp_server, smtp_port, smtp_username, smtp_password, sender_email]):
            logger.error("? Incomplete SMTP configuration")
            return False
        
        smtp_config = {
            'smtp_server': smtp_server,
            'smtp_port': smtp_port,
            'smtp_username': smtp_username,
            'smtp_password': smtp_password,
            'sender_email': sender_email
        }
        
        logger.info(f"? SMTP Configuration loaded:")
        logger.info(f"   ?? Server: {smtp_server}:{smtp_port}")
        logger.info(f"   ?? Username: {smtp_username}")
        logger.info(f"   ?? Sender: {sender_email}")
        
        # STEP 3: Get user's email addresses from database
        logger.info("=" * 80)
        logger.info(f"?? STEP 2: Fetching email addresses for user_id={user_id}")
        logger.info("=" * 80)
        
        user_email, cc_emails = _get_user_emails(user_id, config.pg_config)
        
        if user_email is None:
            logger.warning(f"?? User {user_id} not found in database")
            return False
        
        # STEP 4: Build email subject
        subject = f"? OCR Processing Failed: {filename} ({document_type})"
        
        # STEP 5: Build email body
        body = f"""OCR Processing Failure Notification

Status: FAILED

Document Details:
--------------------------------------------------
- File: {filename}
- Document Type: {document_type}
- User ID: {user_id}
- Request ID: {request_id}
- OCR Engine: {ocr_engine}
- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
--------------------------------------------------

Processing Metrics:
--------------------------------------------------
- Pages Attempted: {page_count}
- Processing Time: {processing_time:.2f} seconds
- Status: ? Failed
--------------------------------------------------
"""
        
        # Add missed keys section if available
        if missed_keys is not None and len(missed_keys) > 0:
            body += f"""
?? MISSING CONDITIONAL KEYS:
--------------------------------------------------
"""
            for idx, key in enumerate(missed_keys, 1):
                body += f"  {idx}. {key}\n"
            
            body += f"Total Missing Keys: {len(missed_keys)}\n"
            body += "--------------------------------------------------\n"
        
        # Add error details
        body += f"""
Error Details:
--------------------------------------------------
{error_details}
--------------------------------------------------

Action Required: Please review the document and try again.

This is an automated notification from the GDocz OCR Server.

---
? Enhanced OCR Tracking System
?? For questions, contact your system administrator.
"""
        
        # STEP 6: Send email
        logger.info("=" * 80)
        logger.info("?? STEP 3: Sending email notification")
        logger.info("=" * 80)
        
        success = _send_email_with_cc(
            smtp_config=smtp_config,
            subject=subject,
            body=body,
            to_email=user_email,
            cc_emails=cc_emails
        )
        
        if success:
            logger.info("? Email notification sent successfully")
        else:
            logger.error("? Failed to send email notification")
        
        return success
    
    except Exception as e:
        logger.error(f"? Error in send_ocr_failure_email: {e}", exc_info=True)
        return False


def _get_user_emails(user_id: int, pg_config: dict) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    Get user's primary email and CC emails from database
    
    Args:
        user_id: User ID to lookup
        pg_config: PostgreSQL config dict with keys: host, port, database, user, password
        
    Returns:
        Tuple of (user_email, cc_emails_list)
        
    Examples:
        (None, None) - User not found
        ("john@example.com", None) - User found, no CC emails
        ("john@example.com", ["admin@example.com", "manager@example.com"]) - User + CC emails
    """
    
    try:
        # STEP 1: Connect to PostgreSQL database
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=pg_config.get('database'),
            user=pg_config.get('user'),
            password=pg_config.get('password')
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # STEP 2: Query users table for primary email
        cursor.execute("SELECT email FROM users WHERE user_id = %s", (user_id,))
        user_result = cursor.fetchone()
        user_email = user_result['email'] if user_result else None
        
        if not user_email:
            cursor.close()
            conn.close()
            logger.warning(f"?? User {user_id} not found in database")
            return (None, None)
        
        # STEP 3: Query alert_mail table for CC emails
        cursor.execute("SELECT cc_mail FROM alert_mail WHERE user_id = %s", (user_id,))
        alert_result = cursor.fetchone()
        
        cc_emails = None
        if alert_result and alert_result['cc_mail']:
            # Parse comma-separated emails
            cc_emails = [
                email.strip() 
                for email in alert_result['cc_mail'].split(',') 
                if email.strip()
            ]
        
        # STEP 4: Close database connection
        cursor.close()
        conn.close()
        
        # STEP 5: Log results
        logger.info(f"?? Fetched emails for user {user_id}")
        logger.info(f"   TO: {user_email}")
        if cc_emails:
            logger.info(f"   CC: {', '.join(cc_emails)} ({len(cc_emails)} recipients)")
        else:
            logger.info(f"   CC: None")
        
        return (user_email, cc_emails)
    
    except Exception as e:
        logger.error(f"? Failed to fetch user emails: {e}", exc_info=True)
        return (None, None)


def _send_email_with_cc(
    smtp_config: dict,
    subject: str,
    body: str,
    to_email: str,
    cc_emails: Optional[List[str]] = None
) -> bool:
    """
    Send email with CC support using SMTP
    
    Args:
        smtp_config: Dictionary with SMTP configuration
            - smtp_server: SMTP server address
            - smtp_port: SMTP port number
            - smtp_username: SMTP username
            - smtp_password: SMTP password
            - sender_email: Sender email address
        subject: Email subject line
        body: Email body (plain text)
        to_email: Primary recipient email address
        cc_emails: List of CC recipient email addresses (optional)
    
    Returns:
        True if email sent successfully, False otherwise
    """
    
    try:
        # STEP 1: Create email message
        msg = MIMEMultipart()
        msg['From'] = smtp_config['sender_email']
        msg['To'] = to_email
        msg['Subject'] = subject
        
        # Add CC if provided
        if cc_emails and len(cc_emails) > 0:
            msg['Cc'] = ', '.join(cc_emails)
        
        # Attach body
        msg.attach(MIMEText(body, 'plain'))
        
        # STEP 2: Prepare recipient list
        recipients = [to_email]
        if cc_emails:
            recipients.extend(cc_emails)
        
        logger.info(f"?? Preparing to send email to {len(recipients)} recipient(s):")
        logger.info(f"   TO: {to_email}")
        if cc_emails:
            logger.info(f"   CC: {', '.join(cc_emails)}")
        
        # STEP 3: Connect to SMTP server
        server = smtplib.SMTP(smtp_config['smtp_server'], smtp_config['smtp_port'])
        server.starttls()  # Enable TLS encryption
        server.login(smtp_config['smtp_username'], smtp_config['smtp_password'])
        
        # STEP 4: Send email
        server.sendmail(smtp_config['sender_email'], recipients, msg.as_string())
        server.quit()
        
        # STEP 5: Log success
        logger.info(f"? Email sent successfully to {len(recipients)} recipient(s)")
        
        return True
    
    except Exception as e:
        logger.error(f"? Failed to send email: {e}", exc_info=True)
        return False