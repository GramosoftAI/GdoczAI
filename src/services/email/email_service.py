#!/usr/bin/env python3

"""
Email Service for Document Processing Pipeline.
Provides email sending capabilities for password resets, welcome messages, and OTP verification.
"""
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, Any
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmailService:
    """Email service for sending notifications"""
    def __init__(self, smtp_config: Dict[str, Any]):

        self.smtp_host = smtp_config.get('host', 'smtp.gmail.com')
        self.smtp_port = smtp_config.get('port', 587)
        self.smtp_username = smtp_config.get('username')
        self.smtp_password = smtp_config.get('password')
        self.from_email = smtp_config.get('from_email', self.smtp_username)
        self.from_name = smtp_config.get('from_name', 'Document Pipeline')
        self.use_tls = smtp_config.get('use_tls', True)
        self.reset_url_base = smtp_config.get('reset_url_base')
        
        # Check if SMTP is properly configured
        self.is_configured = bool(
            self.smtp_host and 
            self.smtp_username and 
            self.smtp_password
        )
        
        if not self.is_configured:
            logger.warning("Email service not fully configured. Email sending will be disabled.")
    
    def send_email(self, to_email: str, subject: str, 
                   html_content: str, text_content: Optional[str] = None) -> bool:

        if not self.is_configured:
            logger.warning(f"Email service not configured. Skipping email to {to_email}")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = to_email
            msg['Subject'] = subject
            msg['Date'] = datetime.now().strftime("%a, %d %b %Y %H:%M:%S %z")
            
            # Add plain text version if provided
            if text_content:
                part1 = MIMEText(text_content, 'plain')
                msg.attach(part1)
            
            # Add HTML version
            part2 = MIMEText(html_content, 'html')
            msg.attach(part2)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {to_email}: {e}", exc_info=True)
            return False
    
    def send_password_reset_email(self, to_email: str, to_name: str, 
                                  reset_url_base: str = None,
                                  encrypted_token: str = None) -> bool:

        if not self.is_configured:
            logger.warning(f"Email not sent to {to_email}. SMTP not configured.")
            logger.info(f"Password reset requested for: {to_email}")
            if encrypted_token:
                logger.info(f"Encrypted token: {encrypted_token}")
            return False
        
        # Validate that encrypted_token is provided
        if not encrypted_token:
            logger.error("Cannot send password reset email: encrypted_token is required")
            return False
        
        # Use provided reset_url_base or fall back to instance variable from config
        if not reset_url_base:
            reset_url_base = self.reset_url_base
        
        # Validate reset_url_base is available
        if not reset_url_base:
            logger.error("Cannot send password reset email: reset_url_base not configured")
            return False
        
        # Build reset link with encrypted token as query parameter
        reset_link = f"{reset_url_base}?token={encrypted_token}"
        
        # Create HTML content matching reference image style
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                }}
                .container {{
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .header {{
                    background-color: #4CAF50;
                    color: white;
                    padding: 20px;
                    text-align: center;
                }}
                .content {{
                    background-color: #f9f9f9;
                    padding: 30px;
                    border-radius: 5px;
                    margin-top: 20px;
                }}
                .button {{
                    display: inline-block;
                    padding: 12px 30px;
                    background-color: #4CAF50;
                    color: white;
                    text-decoration: none;
                    border-radius: 5px;
                    margin: 20px 0;
                }}
                .footer {{
                    text-align: center;
                    margin-top: 20px;
                    color: #666;
                    font-size: 12px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Password Reset Request</h1>
                </div>
                <div class="content">
                    <h2>Hello {to_name},</h2>
                    <p>We received a request to reset your password. Click the button below to reset it:</p>
                    
                    <a href="{reset_link}" class="button">Reset Password</a>
                    
                    <p>If you didn't request a password reset, please ignore this email or contact support if you have concerns.</p>
                    
                    <p>This link will remain valid for 24 hours.</p>
                </div>
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                    <p>&copy; 2025 Document Processing Pipeline. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Plain text version
        text_content = f"""
        Hello {to_name},
        
        We received a request to reset your password.
        
        Please click the following link to reset your password:
        {reset_link}
        
        If you didn't request a password reset, please ignore this email.
        
        This link will remain valid for 24 hours.
        
        ---
        Document Processing Pipeline Team
        """
        
        subject = "Password Reset Request - Document Processing Pipeline"
        
        return self.send_email(to_email, subject, html_content, text_content)
    
    def send_welcome_email(self, to_email: str, to_name: str) -> bool:

        if not self.is_configured:
            logger.warning(f"Email not sent to {to_email}. SMTP not configured.")
            return False
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                }}
                .container {{
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .header {{
                    background-color: #4CAF50;
                    color: white;
                    padding: 20px;
                    text-align: center;
                }}
                .content {{
                    background-color: #f9f9f9;
                    padding: 30px;
                    border-radius: 5px;
                    margin-top: 20px;
                }}
                .footer {{
                    text-align: center;
                    margin-top: 20px;
                    color: #666;
                    font-size: 12px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Welcome to Document Processing Pipeline!</h1>
                </div>
                <div class="content">
                    <h2>Hello {to_name},</h2>
                    <p>Welcome! Your account has been successfully created.</p>
                    <p>Get started by logging into the system and uploading your first document!</p>
                    <p>If you have any questions, please contact your system administrator.</p>
                    <p>Best regards,</p>
                    <p><strong>Document Processing Pipeline Team</strong></p>
                </div>
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                    <p>&copy; 2025 Document Processing Pipeline. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_content = f"""
        Hello {to_name},
        
        Welcome! Your account has been successfully created.
        
        Get started by logging into the system and uploading your first document!
        
        If you have any questions, please contact your system administrator.
        
        Best regards,
        
        Document Processing Pipeline Team
        """
        
        subject = "Welcome to Document Processing Pipeline!"
        
        return self.send_email(to_email, subject, html_content, text_content)
    
    def send_signup_otp_email(self, to_email: str, to_name: str, otp: str) -> bool:

        if not self.is_configured:
            logger.warning(f"Email not sent to {to_email}. SMTP not configured.")
            logger.info(f"Signup OTP for {to_email}: {otp}")
            return False
        
        # Create HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                }}
                .container {{
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .header {{
                    background-color: #4CAF50;
                    color: white;
                    padding: 20px;
                    text-align: center;
                }}
                .content {{
                    background-color: #f9f9f9;
                    padding: 30px;
                    border-radius: 5px;
                    margin-top: 20px;
                }}
                .otp-box {{
                    background-color: #fff;
                    padding: 20px;
                    text-align: center;
                    font-size: 32px;
                    font-weight: bold;
                    letter-spacing: 8px;
                    color: #4CAF50;
                    border: 2px dashed #4CAF50;
                    margin: 20px 0;
                    border-radius: 5px;
                }}
                .footer {{
                    text-align: center;
                    margin-top: 20px;
                    color: #666;
                    font-size: 12px;
                }}
                .warning {{
                    color: #f44336;
                    font-weight: bold;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Email Verification</h1>
                </div>
                <div class="content">
                    <h2>Hello {to_name},</h2>
                    <p>Thank you for signing up! Please use the following OTP to complete your registration:</p>
                    
                    <div class="otp-box">
                        {otp}
                    </div>
                    
                    <p><strong>This OTP is valid for 5 minutes only.</strong></p>
                    
                    <p>If you didn't request this, please ignore this email.</p>
                    
                    <p class="warning">Do not share this OTP with anyone.</p>
                </div>
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                    <p>&copy; 2025 Document Processing Pipeline. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Plain text version
        text_content = f"""
        Hello {to_name},
        
        Thank you for signing up! Your OTP for email verification is:
        
        {otp}
        
        This OTP is valid for 5 minutes only.
        
        If you didn't request this, please ignore this email.
        
        Do not share this OTP with anyone.
        
        ---
        Document Processing Pipeline Team
        """
        
        subject = f"Your Signup OTP: {otp}"
        
        return self.send_email(to_email, subject, html_content, text_content)