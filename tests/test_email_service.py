"""
Test Suite: Email Service
Covers: Email sending, password reset, and welcome email logic
"""
import pytest
from unittest.mock import patch

@pytest.fixture
def smtp_config():
    return {
        'host': 'smtp.example.com',
        'port': 587,
        'username': 'user@example.com',
        'password': 'password',
        'from_email': 'noreply@example.com',
        'from_name': 'Test Pipeline',
        'use_tls': True,
        'reset_url_base': 'https://example.com/reset'
    }

def test_email_service_init(smtp_config):
    from src.services.email.email_service import EmailService
    es = EmailService(smtp_config)
    assert es.is_configured

def test_send_email(monkeypatch, smtp_config):
    from src.services.email.email_service import EmailService
    es = EmailService(smtp_config)
    with patch('smtplib.SMTP') as mock_smtp:
        ok = es.send_email('to@example.com', 'Subject', '<b>HTML</b>', 'Text')
        assert ok is True
        assert mock_smtp.called

def test_send_password_reset_email(monkeypatch, smtp_config):
    from src.services.email.email_service import EmailService
    es = EmailService(smtp_config)
    with patch('smtplib.SMTP') as mock_smtp:
        ok = es.send_password_reset_email('to@example.com', 'User', encrypted_token='abc123')
        assert ok is True
        assert mock_smtp.called

def test_send_welcome_email(monkeypatch, smtp_config):
    from src.services.email.email_service import EmailService
    es = EmailService(smtp_config)
    with patch('smtplib.SMTP') as mock_smtp:
        ok = es.send_welcome_email('to@example.com', 'User')
        assert ok is True
        assert mock_smtp.called
