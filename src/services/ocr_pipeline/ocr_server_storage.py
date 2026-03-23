# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
Storage management and utilities for OCR Server.

Provides:
- Local and S3 file storage management
- Database connection helpers
- JWT token verification
- Document type configuration retrieval from database
"""

import os
import jwt
import json
import boto3
import psycopg2
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple
from psycopg2.extras import RealDictCursor
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# ============================================================================
# STORAGE MANAGER CLASS
# ============================================================================
class StorageManager:
    """
    ?? Storage manager for handling local and S3 file storage
    """
    
    def __init__(self, config_obj):
        self.config = config_obj
        self.storage_type = config_obj.storage_type
        
        self.local_base_path = config_obj.local_base_path
        self.create_date_folders = config_obj.create_date_folders
        
        # S3 storage configuration
        if self.storage_type == 's3':
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=config_obj.s3_config.get('aws_access_key_id'),
                aws_secret_access_key=config_obj.s3_config.get('aws_secret_access_key'),
                region_name=config_obj.s3_config.get('aws_region', 'us-east-1')
            )
            self.bucket_name = config_obj.s3_config.get('bucket_name')
            self.bucket_prefix = config_obj.s3_config.get('bucket_prefix', '')
        
        logger.info(f"?? Storage Manager initialized: {self.storage_type.upper()} storage")
    
    def store_file(self, source_bytes: bytes, filename_with_timestamp: str) -> Optional[str]:

        try:
            if self.storage_type == 'local':
                return self._store_local(source_bytes, filename_with_timestamp)
            elif self.storage_type == 's3':
                return self._store_s3(source_bytes, filename_with_timestamp)
            else:
                logger.error(f"? Unknown storage type: {self.storage_type}")
                return None
        except Exception as e:
            logger.error(f"? Storage error: {e}")
            return None
    
    def _store_local(self, source_bytes: bytes, filename_with_timestamp: str) -> Optional[str]:

        try:
            # Create base directory
            base_dir = self.local_base_path
            base_dir.mkdir(parents=True, exist_ok=True)
            
            # Optionally create date-based subdirectories
            if self.create_date_folders:
                date_folder = datetime.now().strftime('%Y-%m-%d')
                storage_dir = base_dir / date_folder
                storage_dir.mkdir(parents=True, exist_ok=True)
            else:
                storage_dir = base_dir
            
            # Write file to storage location
            dest_path = storage_dir / filename_with_timestamp
            with open(dest_path, 'wb') as f:
                f.write(source_bytes)
            
            # Return relative path (without base_path)
            if self.create_date_folders:
                date_folder = datetime.now().strftime('%Y-%m-%d')
                relative_path = f"/{date_folder}/{filename_with_timestamp}"
            else:
                relative_path = f"/{filename_with_timestamp}"

            absolute_path = str(dest_path.resolve())
            logger.info(f"? File stored locally: {absolute_path}")
            logger.info(f"?? Relative path for DB: {relative_path}")
            return relative_path  # Return relative path instead of absolute
            
        except Exception as e:
            logger.error(f"? Local storage error: {e}")
            return None
    
    def _store_s3(self, source_bytes: bytes, filename_with_timestamp: str) -> Optional[str]:

        try:
            # Build S3 key
            s3_key = self.bucket_prefix.rstrip('/') + '/'
            
            if self.create_date_folders:
                date_folder = datetime.now().strftime('%Y-%m-%d')
                s3_key += date_folder + '/'
            
            s3_key += filename_with_timestamp
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=source_bytes,
                ServerSideEncryption=self.config.config.get('storage', {}).get('s3_storage', {}).get('server_side_encryption', 'AES256'),
                ACL=self.config.config.get('storage', {}).get('s3_storage', {}).get('acl', 'private')
            )
            
            # Return S3 URI
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"? File stored in S3: {s3_uri}")
            return s3_uri
            
        except ClientError as e:
            logger.error(f"? S3 storage error: {e}")
            return None

# ============================================================================
# FILENAME UTILITY FUNCTIONS
# ============================================================================
def generate_timestamped_filename(original_filename: str) -> str:

    path = Path(original_filename)
    stem = path.stem
    extension = path.suffix
    timestamp = datetime.now().strftime('%H%M%S')
    return f"{stem}_{timestamp}{extension}"

# ============================================================================
# DATABASE CONNECTION HELPER
# ============================================================================
def get_db_connection(pg_config: Dict):
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=pg_config.get('database', 'document_pipeline'),
            user=pg_config.get('user'),
            password=pg_config.get('password')
        )
        return conn
    except Exception as e:
        logger.error(f"? Database connection error: {e}")
        return None

# ============================================================================
# ?? IMPORT CONFIGURATION FUNCTIONS FROM storage2
# ============================================================================
from src.services.ocr_pipeline.ocr_server_storage2 import (
    get_document_type_id,
    get_schema_for_document_type,
    get_conditional_keys,
    get_langchain_keys,
    get_document_config,
    get_document_config_or_fallback,
    verify_jwt_token,
    validate_document_config,
    should_use_langchain_chunking,
    should_validate_markdown
)

__all__ = [
    'StorageManager',
    'generate_timestamped_filename',
    'get_db_connection',
    'get_document_type_id',
    'get_schema_for_document_type',
    'get_conditional_keys',
    'get_langchain_keys',
    'get_document_config',
    'get_document_config_or_fallback',
    'verify_jwt_token',
    'validate_document_config',
    'should_use_langchain_chunking',
    'should_validate_markdown'
]
