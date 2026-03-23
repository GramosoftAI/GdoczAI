# -*- coding: utf-8 -*-

"""
?? Slim Configuration Management for Multi-Connector SFTP ? OCR Pipeline

Responsibilities:
- Load and parse config.yaml (ONLY auth, ocr, and token refresh settings)
- Expose typed configuration objects
- Validate configuration completeness
- SFTP credentials are now sourced from database
- Email settings are now sourced from database

NEW APPROACH:
- config.yaml contains ONLY:
  * authentication (signin_url, username, password, token_refresh_interval_hours)
  * ocr (endpoint_url, timeout_seconds)
  * scheduler.token_refresh_check_interval_minutes
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)
from dataclasses import dataclass as dataclass_decorator

@dataclass_decorator
class SFTPConfig:

    host: str
    port: int
    username: str
    password: str
    monitored_folders: List[str]
    moved_folder: str
    failed_folder: str
    private_key_path: Optional[str] = None
    key_password: Optional[str] = None 
    
    def __post_init__(self):
        """Validate SFTP configuration"""
        if not self.host:
            raise ValueError("SFTP host cannot be empty")
        if not self.username:
            raise ValueError("SFTP username cannot be empty")
        if not self.monitored_folders:
            raise ValueError("At least one monitored folder must be specified")
        if not self.moved_folder:
            raise ValueError("Moved folder path cannot be empty")
        if not self.failed_folder:
            raise ValueError("Failed folder path cannot be empty")


@dataclass
class AuthConfig:
    """Authentication API configuration (from config.yaml)"""
    signin_url: str
    username: str
    password: str
    token_refresh_interval_hours: int = 20
    
    def __post_init__(self):
        """Validate auth configuration"""
        if not self.signin_url:
            raise ValueError("Auth signin URL cannot be empty")
        if not self.username:
            raise ValueError("Auth username cannot be empty")
        if not self.password:
            raise ValueError("Auth password cannot be empty")
        if self.token_refresh_interval_hours <= 0 or self.token_refresh_interval_hours > 24:
            raise ValueError("Token refresh interval must be between 1 and 24 hours")


@dataclass
class OCRConfig:
    """OCR API configuration (from config.yaml)"""
    endpoint_url: str
    timeout_seconds: int = 300
    
    def __post_init__(self):
        """Validate OCR configuration"""
        if not self.endpoint_url:
            raise ValueError("OCR endpoint URL cannot be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("OCR timeout must be positive")


@dataclass
class SlimSchedulerConfig:
    """    
    Folder scan interval is now per-connector (in database)
    """
    token_refresh_check_interval_minutes: int = 60
    
    def __post_init__(self):
        """Validate scheduler configuration"""
        if self.token_refresh_check_interval_minutes <= 0:
            raise ValueError("Token refresh check interval must be positive")


@dataclass
class SlimPipelineConfig:
    """SFTP credentials come from database."""
    auth: AuthConfig
    ocr: OCRConfig
    scheduler: SlimSchedulerConfig
    log_level: str = "INFO"
    max_retry_attempts: int = 3
    retry_delay_seconds: int = 5
    
    def __post_init__(self):
        """Validate pipeline configuration"""
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(f"Invalid log level: {self.log_level}. Must be one of {valid_log_levels}")
        if self.max_retry_attempts < 0:
            raise ValueError("Max retry attempts cannot be negative")
        if self.retry_delay_seconds <= 0:
            raise ValueError("Retry delay must be positive")

# ============================================================================
# BACKWARD COMPATIBILITY: Keep full PipelineConfig for existing code
# ============================================================================
@dataclass
class SchedulerConfig:
    folder_scan_interval_minutes: int = 5
    token_refresh_check_interval_minutes: int = 60
    
    def __post_init__(self):
        if self.folder_scan_interval_minutes <= 0:
            raise ValueError("Folder scan interval must be positive")
        if self.token_refresh_check_interval_minutes <= 0:
            raise ValueError("Token refresh check interval must be positive")


@dataclass
class EmailConfig:
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
        if self.smtp_port <= 0 or self.smtp_port > 65535:
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
        
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        for email in all_recipients:
            if not re.match(email_pattern, email):
                raise ValueError(f"Invalid email format: {email}")
        
        if self.alert_cooldown_minutes < 0:
            raise ValueError("Alert cooldown minutes cannot be negative")
    
    def get_all_recipients(self) -> List[str]:
        """Get combined list of all recipients"""
        return self.developer_recipients + self.client_recipients
    
    def has_recipients(self) -> bool:
        """Check if any recipients are configured"""
        return len(self.get_all_recipients()) > 0


# Keep PipelineConfig for backward compatibility
@dataclass
class PipelineConfig:
    sftp: SFTPConfig
    auth: AuthConfig
    ocr: OCRConfig
    scheduler: SchedulerConfig
    email: EmailConfig
    log_level: str = "INFO"
    max_retry_attempts: int = 3
    retry_delay_seconds: int = 5
    
    def __post_init__(self):
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(f"Invalid log level: {self.log_level}. Must be one of {valid_log_levels}")
        if self.max_retry_attempts < 0:
            raise ValueError("Max retry attempts cannot be negative")
        if self.retry_delay_seconds <= 0:
            raise ValueError("Retry delay must be positive")


class SlimConfigLoader:
    
    @staticmethod
    def load_config(config_path: str = "config/config.yaml") -> SlimPipelineConfig:

        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        logger.info("=" * 80)
        logger.info("?? LOADING SLIM CONFIGURATION (DB-DRIVEN SFTP)")
        logger.info("=" * 80)
        logger.info(f"?? Config file: {config_path}")
        logger.info("=" * 80)
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            if not config_data:
                raise ValueError("Configuration file is empty")
            
            # Parse authentication configuration
            auth_data = config_data.get('authentication', {})
            auth_config = AuthConfig(
                signin_url=auth_data.get('signin_url', ''),
                username=auth_data.get('username', ''),
                password=auth_data.get('password', ''),
                token_refresh_interval_hours=auth_data.get('token_refresh_interval_hours', 20)
            )
            
            logger.info("?? Authentication configuration:")
            logger.info(f"   Signin URL: {auth_config.signin_url}")
            logger.info(f"   Username: {auth_config.username}")
            logger.info(f"   Token Refresh Interval: {auth_config.token_refresh_interval_hours} hours")
            
            # Parse OCR configuration
            ocr_data = config_data.get('ocr', {})
            ocr_config = OCRConfig(
                endpoint_url=ocr_data.get('endpoint_url', ''),
                timeout_seconds=ocr_data.get('timeout_seconds', 300)
            )
            
            logger.info("?? OCR configuration:")
            logger.info(f"   Endpoint: {ocr_config.endpoint_url}")
            logger.info(f"   Timeout: {ocr_config.timeout_seconds} seconds")
            
            # Parse slim scheduler configuration
            scheduler_data = config_data.get('scheduler', {})
            scheduler_config = SlimSchedulerConfig(
                token_refresh_check_interval_minutes=scheduler_data.get('token_refresh_check_interval_minutes', 60)
            )
            
            logger.info("?? Scheduler configuration:")
            logger.info(f"   Token Refresh Check Interval: {scheduler_config.token_refresh_check_interval_minutes} minutes")
            logger.info("   ⚠️  Folder scan interval is now PER-CONNECTOR from database")
            
            # Create slim pipeline configuration
            slim_config = SlimPipelineConfig(
                auth=auth_config,
                ocr=ocr_config,
                scheduler=scheduler_config,
                log_level=config_data.get('log_level', 'INFO'),
                max_retry_attempts=config_data.get('max_retry_attempts', 3),
                retry_delay_seconds=config_data.get('retry_delay_seconds', 5)
            )
            
            logger.info("?? Configuration loaded and validated successfully")
            logger.info("=" * 80)
            logger.info("?? IMPORTANT: NEW MULTI-CONNECTOR APPROACH")
            logger.info("?? SFTP credentials are now loaded from database dynamically")
            logger.info("?? Multiple independent pipelines can run simultaneously")
            logger.info("=" * 80)
            
            return slim_config
            
        except yaml.YAMLError as e:
            logger.error(f"? Failed to parse YAML configuration: {e}")
            raise
        except ValueError as e:
            logger.error(f"? Invalid configuration: {e}")
            raise
        except Exception as e:
            logger.error(f"? Unexpected error loading configuration: {e}")
            raise


# ============================================================================
# Global configuration instances
# ============================================================================

_slim_config_instance: Optional[SlimPipelineConfig] = None


def get_slim_config(config_path: str = "config/config.yaml") -> SlimPipelineConfig:

    global _slim_config_instance
    
    if _slim_config_instance is None:
        _slim_config_instance = SlimConfigLoader.load_config(config_path)
    
    return _slim_config_instance


def reload_slim_config(config_path: str = "config/config.yaml") -> SlimPipelineConfig:

    global _slim_config_instance
    
    logger.info("?? Reloading slim configuration...")
    _slim_config_instance = SlimConfigLoader.load_config(config_path)
    
    return _slim_config_instance