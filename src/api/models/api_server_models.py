#!/usr/bin/env python3

import base64
import json
import logging
import secrets
import shutil
import boto3
import psycopg2
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, EmailStr, validator
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIG MANAGER - STANDALONE (NO PIPELINE DEPENDENCIES)
# ============================================================================

class ConfigManager:

    def __init__(self, config_path: str = "config/config.yaml"):

        self.config_path = config_path
        
        # Load YAML configuration
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            logger.info(f"? ConfigManager loaded: {config_path}")
        except Exception as e:
            logger.error(f"? Failed to load config: {e}")
            raise
    
    def get(self, key: str, default=None):

        try:
            # Split key by dots
            keys = key.split('.')
            
            # Navigate through dictionary
            value = self.config
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k)
                    if value is None:
                        return default
                else:
                    return default
            
            return value if value is not None else default
        
        except Exception as e:
            logger.debug(f"Config key not found: {key}, using default: {default}")
            return default

# ============================================================================
# HELPER FUNCTIONS FOR NEW FEATURES
# ============================================================================

def generate_unique_request_id() -> str:

    random_bytes = secrets.token_bytes(16)
    request_id = base64.urlsafe_b64encode(random_bytes).decode('utf-8').rstrip('=')
    logger.debug(f"?? Generated request_id: {request_id}")
    return request_id

def generate_timestamped_filename(original_filename: str) -> str:

    path = Path(original_filename)
    stem = path.stem
    extension = path.suffix
    timestamp = datetime.now().strftime('%H%M%S')
    timestamped_name = f"{stem}_{timestamp}{extension}"
    logger.debug(f"?? Generated timestamped filename: {original_filename} ? {timestamped_name}")
    return timestamped_name

def get_db_connection():

    try:
        config_manager = ConfigManager("config/config.yaml")
        
        # Get postgres config from YAML
        pg_config = config_manager.config.get('postgres', {})
        
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=pg_config.get('database', 'document_pipeline'),
            user=pg_config.get('user'),
            password=pg_config.get('password')
        )
        
        logger.debug("? Database connection established")
        return conn
    
    except Exception as e:
        logger.error(f"? Database connection failed: {e}")
        raise

# ============================================================================
# STORAGE MANAGER CLASS
# ============================================================================

class StorageManager:

    def __init__(self, config_manager: ConfigManager):

        self.config = config_manager
        self.storage_type = config_manager.get('storage.storage_type', 'local')
        
        # Local storage configuration
        self.local_base_path = Path(
            config_manager.get('storage.local_storage.base_path', './stored_pdfs/')
        )
        self.create_date_folders = config_manager.get(
            'storage.local_storage.create_date_folders', 
            True
        )
        
        # S3 storage configuration
        if self.storage_type == 's3':
            aws_access_key = config_manager.get('storage.s3_storage.aws_access_key_id')
            aws_secret_key = config_manager.get('storage.s3_storage.aws_secret_access_key')
            aws_region = config_manager.get('storage.s3_storage.aws_region', 'us-east-1')
            
            if not aws_access_key or not aws_secret_key:
                logger.error("? AWS credentials not configured")
                raise ValueError("AWS credentials required for S3 storage")
            
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
            
            self.bucket_name = config_manager.get('storage.s3_storage.bucket_name')
            self.bucket_prefix = config_manager.get('storage.s3_storage.bucket_prefix', '')
            
            if not self.bucket_name:
                logger.error("? S3 bucket name not configured")
                raise ValueError("S3 bucket name required for S3 storage")
            
            logger.info(f"? S3 Storage configured: {self.bucket_name}")
        
        logger.info(f"? Storage Manager initialized: {self.storage_type.upper()} storage")
    
    def store_file(self, source_path: Path, filename_with_timestamp: str) -> Optional[str]:

        try:
            if self.storage_type == 'local':
                return self._store_local(source_path, filename_with_timestamp)
            elif self.storage_type == 's3':
                return self._store_s3(source_path, filename_with_timestamp)
            else:
                logger.error(f"? Unknown storage type: {self.storage_type}")
                return None
        
        except Exception as e:
            logger.error(f"? Failed to store file: {e}", exc_info=True)
            return None
    
    def _store_local(self, source_path: Path, filename: str) -> str:
        """Store file to local filesystem"""
        try:
            # Create base directory
            self.local_base_path.mkdir(parents=True, exist_ok=True)
            
            # Create date-based subfolder if enabled
            if self.create_date_folders:
                date_folder = datetime.now().strftime('%Y-%m-%d')
                target_dir = self.local_base_path / date_folder
                target_dir.mkdir(parents=True, exist_ok=True)
                relative_path = f"/{date_folder}/{filename}"
            else:
                target_dir = self.local_base_path
                relative_path = f"/{filename}"
            
            # Copy file to target directory
            target_path = target_dir / filename
            shutil.copy2(source_path, target_path)
            
            logger.info(f"?? File stored locally: {relative_path}")
            return relative_path
        
        except Exception as e:
            logger.error(f"? Local storage failed: {e}", exc_info=True)
            raise
    
    def _store_s3(self, source_path: Path, filename: str) -> str:
        """Store file to S3"""
        try:
            # Build S3 key with optional prefix and date folder
            date_folder = datetime.now().strftime('%Y-%m-%d')
            
            if self.bucket_prefix:
                s3_key = f"{self.bucket_prefix}/{date_folder}/{filename}"
            else:
                s3_key = f"{date_folder}/{filename}"
            
            # Upload to S3
            self.s3_client.upload_file(
                str(source_path),
                self.bucket_name,
                s3_key
            )
            
            # Return S3 URI
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"?? File stored to S3: {s3_uri}")
            return s3_uri
        
        except ClientError as e:
            logger.error(f"? S3 storage failed: {e}", exc_info=True)
            raise
    
    def get_file_url(self, storage_path: str, expiration: int = 3600) -> Optional[str]:

        try:
            if self.storage_type != 's3':
                logger.warning("? Presigned URLs only available for S3 storage")
                return None
            
            # Extract S3 key from URI
            if storage_path.startswith('s3://'):
                # Format: s3://bucket/key
                parts = storage_path.replace('s3://', '').split('/', 1)
                if len(parts) == 2:
                    s3_key = parts[1]
                else:
                    logger.error(f"? Invalid S3 URI: {storage_path}")
                    return None
            else:
                s3_key = storage_path
            
            # Generate presigned URL
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': s3_key
                },
                ExpiresIn=expiration
            )
            
            logger.debug(f"?? Generated presigned URL for: {s3_key}")
            return url
        
        except Exception as e:
            logger.error(f"? Failed to generate presigned URL: {e}", exc_info=True)
            return None
    
    def get_file_path(self, relative_path: str) -> Optional[Path]:

        if self.storage_type != 'local':
            logger.warning("?? get_file_path only works with local storage")
            return None
        
        full_path = self.local_base_path / relative_path.lstrip('/')
        return full_path if full_path.exists() else None

# ============================================================================
# PYDANTIC MODELS FOR API REQUESTS/RESPONSES
# ============================================================================

class PipelineStatus(BaseModel):
    """Pipeline status response model"""
    status: str
    uptime_seconds: float
    total_files_processed: int
    files_in_queue: int
    active_processing: int
    last_activity: Optional[str]
    api_health: Dict[str, Any]

class DocumentTypeCreate(BaseModel):
    """Request model for creating document type"""
    document_type: str = Field(
        ..., 
        min_length=1, 
        max_length=255, 
        description="Document type name"
    )

class DocumentTypeResponse(BaseModel):
    """Response model for document type"""
    doc_type_id: int
    document_type: str
    user_id: int
    created_at: str
    updated_at: str

class DocumentSchemaCreate(BaseModel):
    """Request model for creating document schema with prompt_field validation"""
    doc_type_id: int = Field(..., description="Document type ID")
    prompt_field: str = Field(..., description="Type of prompt: 'text' or 'json'")
    logic_type_id: Optional[int] = Field(default=None, description="Document logic ID referencing document_logics table")
    extraction_schema: Union[str, Dict[str, Any], List[Any]] = Field(
        ..., 
        description="Extraction schema - must be text string if prompt_field='text', or JSON object/string if prompt_field='json'"
    )
    
    @validator('prompt_field')
    def validate_prompt_field(cls, v):
        """Validate prompt_field is either 'text' or 'json'"""
        if v not in ['text', 'json']:
            raise ValueError("prompt_field must be either 'text' or 'json'")
        return v
    
    @validator('extraction_schema')
    def validate_extraction_schema(cls, v, values):
        """Validate extraction_schema matches the prompt_field type"""
        prompt_field = values.get('prompt_field')
        
        if prompt_field == 'text':
            # For text, extraction_schema must be a plain string (not JSON)
            if isinstance(v, (dict, list)):
                raise ValueError("When prompt_field='text', extraction_schema must be a text string, not a JSON object or array")
            
            # If it's a string, try to parse it to see if it's JSON
            if isinstance(v, str):
                try:
                    json.loads(v)
                    raise ValueError("When prompt_field='text', extraction_schema must be plain text, not a JSON string")
                except json.JSONDecodeError:
                    # This is good - it's plain text
                    pass
        
        elif prompt_field == 'json':
            # For json, extraction_schema should be valid JSON (dict, list, or JSON string)
            if isinstance(v, str):
                try:
                    json.loads(v)
                    # Valid JSON string
                except json.JSONDecodeError:
                    raise ValueError("When prompt_field='json', extraction_schema must be valid JSON (object, array, or JSON string)")
            elif not isinstance(v, (dict, list)):
                raise ValueError("When prompt_field='json', extraction_schema must be a JSON object, array, or valid JSON string")
        
        return v

class DocumentSchemaResponse(BaseModel):
    """Response model for document schema"""
    id: int
    doc_type_id: int
    extraction_schema: Any
    prompt_field: Optional[str] = None
    logic_type_id: Optional[int] = None
    logic_name: Optional[str] = None
    user_id: int
    created_at: str
    updated_at: str

class FileProcessingRequest(BaseModel):
    """Request model for file processing"""
    custom_prompt: Optional[str] = Field(
        default=None, 
        description="Custom OCR prompt"
    )
    priority: Optional[int] = Field(
        default=5, 
        ge=1, 
        le=10, 
        description="Processing priority (1-10)"
    )
    user_id: Optional[int] = Field(
        default=None, 
        description="User ID for tracking (optional)"
    )
    extraction_schema: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="Optional JSON schema for extraction"
    )

class FileProcessingResponse(BaseModel):
    """Response model for file processing"""
    success: bool
    job_id: str
    request_id: str
    filename: str
    status: str
    message: Optional[str] = None
    ocr_output_path: Optional[str] = None
    json_output_path: Optional[str] = None
    processing_time_seconds: Optional[float] = None
    file_path: Optional[str] = None

class BatchProcessingRequest(BaseModel):
    """Request model for batch processing"""
    file_ids: List[str]
    custom_prompt: Optional[str] = None
    priority: Optional[int] = 5
    user_id: Optional[int] = None
    extraction_schema: Optional[Dict[str, Any]] = None

class PipelineConfig(BaseModel):
    """Pipeline configuration model"""
    max_concurrent_files: int
    polling_interval_minutes: int
    supported_extensions: List[str]
    auto_retry_failed: bool
    max_retries: int

class FileStatistics(BaseModel):
    """File statistics response model"""
    total_files: int
    completed: int
    failed: int
    pending: int
    processing: int
    average_processing_time_minutes: float
    success_rate_percent: float

class JobStatus(BaseModel):
    """Job status response model"""
    job_id: str
    request_id: Optional[str] = None
    filename: str
    status: str
    created_at: str
    updated_at: str
    progress_percent: int
    error_message: Optional[str] = None
    result_urls: Optional[Dict[str, str]] = None
    file_path: Optional[str] = None

# ============================================================================
# ADDITIONAL HELPER FUNCTIONS
# ============================================================================

def validate_file_format(filename: str, supported_formats: List[str]) -> bool:

    extension = Path(filename).suffix.lower()
    return extension in supported_formats

def get_file_size_mb(file_path: Path) -> float:

    try:
        size_bytes = file_path.stat().st_size
        return size_bytes / (1024 * 1024)
    except Exception as e:
        logger.error(f"? Failed to get file size: {e}")
        return 0.0

def sanitize_filename(filename: str) -> str:

    # Remove path separators
    filename = filename.replace('/', '_').replace('\\', '_')
    
    # Remove null bytes
    filename = filename.replace('\x00', '')
    
    # Remove control characters
    import re
    filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)
    
    # Limit length (keep extension)
    if len(filename) > 255:
        path = Path(filename)
        name = path.stem
        ext = path.suffix
        max_name_length = 255 - len(ext)
        filename = name[:max_name_length] + ext
    
    return filename

def format_processing_time(seconds: float) -> str:

    if seconds < 60:
        return f"{seconds:.1f}s"
    
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    
    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"
    
    hours = minutes // 60
    remaining_minutes = minutes % 60
    
    return f"{hours}h {remaining_minutes}m"

def create_error_response(error_message: str, status_code: int = 500) -> Dict[str, Any]:

    return {
        "success": False,
        "error": error_message,
        "status_code": status_code,
        "timestamp": datetime.now().isoformat()
    }

# ============================================================================
# DATABASE HELPER FUNCTIONS
# ============================================================================

def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            "SELECT user_id, email, full_name FROM users WHERE email = %s",
            (email.lower(),)
        )
        
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return dict(user) if user else None
    
    except Exception as e:
        logger.error(f"? Failed to get user by email: {e}")
        return None

def get_file_by_request_id(request_id: str) -> Optional[Dict[str, Any]]:

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            """
            SELECT * FROM processed_files 
            WHERE request_id = %s
            ORDER BY created_on DESC
            LIMIT 1
            """,
            (request_id,)
        )
        
        file_record = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return dict(file_record) if file_record else None
    
    except Exception as e:
        logger.error(f"? Failed to get file by request_id: {e}")
        return None

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_api_logging(log_level: str = "INFO", log_file: Optional[str] = None):

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        )
        logging.getLogger().addHandler(file_handler)
        logger.info(f"? File logging enabled: {log_file}")

logger.info("API Server Models module loaded")