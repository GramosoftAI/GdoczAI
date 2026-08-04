# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
Configuration management and initialization for OCR Server.

Handles:
- Loading YAML configuration
- Logging setup
- Gemini API availability check
- Mistral OCR configuration
- Database storage initialization
- Manual splitting configuration for oversized chunks
"""

import os
import warnings

# Suppress oneDNN custom operations warnings and absl messages from TensorFlow
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

# Suppress Python warnings from tensorflow and tf_keras
warnings.filterwarnings("ignore", category=DeprecationWarning, module="tensorflow")
warnings.filterwarnings("ignore", category=UserWarning, module="tensorflow")
warnings.filterwarnings("ignore", category=FutureWarning, module="tensorflow")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="tf_keras")
warnings.filterwarnings("ignore", category=UserWarning, module="tf_keras")
warnings.filterwarnings("ignore", category=FutureWarning, module="tf_keras")

import yaml
import logging
import psycopg2
from pathlib import Path
from typing import Dict, Optional, List
from logging.handlers import RotatingFileHandler

# Database storage utility
from src.core.database.db_storage_util import DatabaseStorage

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
# Create logs directory if it doesn't exist
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Remove any existing handlers
logger.handlers.clear()

# Create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# File handler with rotation (10MB max, keep 5 backup files)
file_handler = RotatingFileHandler(
    'logs/olmocr_server.log',
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console handler (optional - keep for debugging)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger = logging.getLogger(__name__)
logger.info("OCR SERVER - LOGGING INITIALIZED")
logger.info(f"Log file: logs/olmocr_server.log")

# ============================================================================
# CONFIGURATION CLASS
# ============================================================================
class Config:
    """Configuration management for the OCR server"""
    
    def __init__(self):
        self.config = self._load_config()
        
        # Chunking settings (for Unstructured and LangChain)
        self.chunking_enabled = self.config.get('chunking', {}).get('enabled', True)
        self.chunk_size = self.config.get('chunking', {}).get('chunk_size', 20000)
        self.chunk_overlap = self.config.get('chunking', {}).get('overlap', 500)
        # if self.chunking_enabled:
        
        # ? Manual splitting settings (NEW)
        manual_split_config = self.config.get('manual_splitting', {})
        self.manual_split_enabled = manual_split_config.get('enabled', True)
        self.manual_split_threshold = manual_split_config.get('threshold_characters', 7000)
        self.manual_split_max_rows = manual_split_config.get('max_rows_per_chunk', 10)
        
        # Mistral OCR settings - REQUIRED
        self.mistral_ocr_api_key = self.config.get('mistral_ocr', {}).get('api_key', os.getenv('MISTRAL_API_KEY'))
        self.mistral_ocr_model = self.config.get('mistral_ocr', {}).get('model', 'mistral-ocr-latest')
        self.mistral_ocr_timeout = self.config.get('mistral_ocr', {}).get('timeout', 300)
        
        # if self.mistral_ocr_api_key:
        # else:

        # QWENOCR settings (DeepInfra) - REQUIRED
        self.qwenocr_deepinfra_api_key = self.config.get('qwenocr_deepinfra', {}).get('api_key', os.getenv('DEEPINFRA_API_KEY'))
        self.qwenocr_deepinfra_model = self.config.get('qwenocr_deepinfra', {}).get('model', 'Qwen/Qwen3-VL-235B-A22B-Instruct')
        self.qwenocr_deepinfra_timeout = self.config.get('qwenocr_deepinfra', {}).get('timeout', 300)
        
        # if self.qwenocr_deepinfra_api_key:
        # else:

        # CHANDRA settings (Datalab Marker API) - NEW
        chandra_cfg = self.config.get('chandra_datalab', {})
        self.chandra_datalab_api_key = chandra_cfg.get('api_key')
        self.chandra_datalab_output_format = chandra_cfg.get('output_format', 'html')
        self.chandra_datalab_mode = chandra_cfg.get('mode', 'accurate')
        self.chandra_datalab_timeout = chandra_cfg.get('timeout', 600)
        self.chandra_datalab_poll_interval = chandra_cfg.get('poll_interval', 3)
        self.chandra_datalab_max_retries = chandra_cfg.get('max_retries', 2)


        
        # JWT settings
        self.jwt_secret = self.config.get('security', {}).get('jwt_secret_key', 'your-secret-key-change-this-in-production')
        
        # PostgreSQL settings
        self.pg_config = self.config.get('postgres', {})
        
        # ? Storage settings
        self.storage_type = self.config.get('storage', {}).get('storage_type', 'local')
        self.local_base_path = Path(self.config.get('storage', {}).get('local_storage', {}).get('base_path', './stored_pdfs/'))
        self.create_date_folders = self.config.get('storage', {}).get('local_storage', {}).get('create_date_folders', True)
        
        # S3 settings (if using S3)
        if self.storage_type == 's3':
            self.s3_config = self.config.get('storage', {}).get('s3_storage', {})
        
        # if self.storage_type == 'local':
        
        # ? GENERIC: LangChain chunking settings (used when langchain_keys exist)
        self.langchain_chunk_size = self.config.get('langchain_chunking', {}).get('chunk_size', 15000)
        self.langchain_chunk_overlap = self.config.get('langchain_chunking', {}).get('chunk_overlap', 500)
        
        # ? Unstructured chunking settings (used when langchain_keys do NOT exist)
        self.unstructured_max_chars = self.config.get('unstructured_chunking', {}).get('max_characters', 20000)
        self.unstructured_combine_chars = self.config.get('unstructured_chunking', {}).get('combine_text_under_n_chars', 1000)
        self.unstructured_new_after_chars = self.config.get('unstructured_chunking', {}).get('new_after_n_chars', 19000)
        
    def _load_config(self) -> Dict:
        """Load configuration from sundarams_config.yaml ONLY (client-specific)"""
        import os
        
        # SUNDARAMS USES ITS OWN CONFIG ONLY - No fallback to global config
        # This ensures all credentials come only from sundarams_config.yaml
        
        config_path = None
        
        # Resolution order:
        # 1. SUNDARAMS_CONFIG_PATH environment variable
        # 2. Same directory as this file (src/services/sundarams/)
        
        # Check environment variable first
        env_path = os.environ.get("SUNDARAMS_CONFIG_PATH")
        if env_path:
            config_path = env_path
        else:
            # Use config in same directory as this file
            this_dir = Path(__file__).parent
            config_path = str(this_dir / "sundarams_config.yaml")
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info("?? [SUNDARAMS CLIENT] CONFIGURATION LOADED")
                logger.info(f"? Config file: {config_path}")
                logger.info("? Status: Using DEDICATED CLIENT-SPECIFIC config")
                logger.info("? Credentials: Loaded from sundarams_config.yaml ONLY")
                return config if config else {}
        except FileNotFoundError:
            error_msg = (
                f"??? CRITICAL ERROR: Sundarams config not found!\n"
                f"? Expected path: {config_path}\n"
                f"? Sundarams requires its own dedicated config file.\n"
                f"? Set SUNDARAMS_CONFIG_PATH environment variable or place "
                f"sundarams_config.yaml in src/services/sundarams/\n"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        except Exception as e:
            logger.error(f"? Failed to load Sundarams config: {e}", exc_info=True)
            raise

# Initialize global config
config = Config()

# ============================================================================
# CHUNKER COMPATIBILITY
# ============================================================================
# Chonkie/Recursive chunker is no longer used in the pipeline
# Replaced by Unstructured semantic chunker for unknown document types
# This variable is kept for backward compatibility with existing code
chunker = None

# ============================================================================
# GEMINI PROCESSOR INITIALIZATION
# ============================================================================
# ============================================================================
# GEMINI PROCESSOR INITIALIZATION
# ============================================================================
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# ============================================================================
# UNSTRUCTURED CHUNKER AVAILABILITY CHECK
# ============================================================================
UNSTRUCTURED_AVAILABLE = False
try:
    from unstructured.partition.text import partition_text
    from unstructured.chunking.title import chunk_by_title
    UNSTRUCTURED_AVAILABLE = True
except ImportError:
    pass

# ============================================================================
# LANGCHAIN TEXT SPLITTER AVAILABILITY CHECK
# ============================================================================
LANGCHAIN_AVAILABLE = False
try:
    from langchain_text_splitters import MarkdownHeaderTextSplitter
    LANGCHAIN_AVAILABLE = True
except ImportError:
    pass

# ============================================================================
# ? MANUAL SPLITTER AVAILABILITY CHECK
# ============================================================================
MANUAL_SPLITTER_AVAILABLE = False
try:
    from src.services.sundarams.sundarams_ocr_server_manual_splitter import ManualMarkdownSplitter, process_oversized_chunks
    MANUAL_SPLITTER_AVAILABLE = True
except ImportError:
    pass

# ============================================================================
# DATABASE STORAGE INITIALIZATION
# ============================================================================
db_storage: Optional[DatabaseStorage] = None
try:
    pg_config = config.config.get('postgres', {})
    if pg_config.get('host') and pg_config.get('user'):
        db_storage = DatabaseStorage(pg_config)
except Exception as e:
    pass

# ============================================================================
# MISTRAL AVAILABILITY CHECK
# ============================================================================
MISTRAL_AVAILABLE = False
if config.mistral_ocr_api_key:
    MISTRAL_AVAILABLE = True

# ============================================================================
# QWEN AVAILABILITY CHECK
# ============================================================================
QWEN_AVAILABLE = False
if config.qwenocr_deepinfra_api_key:
    QWEN_AVAILABLE = True

# ============================================================================
# CHANDRA (DATALAB MARKER) AVAILABILITY CHECK
# ============================================================================
CHANDRA_AVAILABLE = False
if config.chandra_datalab_api_key:
    CHANDRA_AVAILABLE = True

# ============================================================================
# STARTUP SUMMARY PRINT FUNCTION
# ============================================================================
def print_startup_summary():
    """Prints a beautiful and consolidated status of all models and components."""
    status_msg = [
        "",
        "=" * 60,
        "           SUNDARAMS OCR SERVER SYSTEM STATUS",
        "=" * 60,
        "OCR Backends Status:",
        f"  [+] Mistral OCR: {'AVAILABLE' if MISTRAL_AVAILABLE else 'NOT AVAILABLE'}",
        f"  [+] Qwen VL  : {'AVAILABLE' if QWEN_AVAILABLE else 'NOT AVAILABLE'}",
        f"  [+] Chandra  : {'AVAILABLE' if CHANDRA_AVAILABLE else 'NOT AVAILABLE'}",
        "",
        "LLM JSON Extractor Status:",
        f"  [+] Gemini   : {'AVAILABLE' if GEMINI_AVAILABLE else 'NOT AVAILABLE'}",
        "",
        "Database Status:",
        f"  [+] Database : {'CONNECTED' if db_storage is not None else 'NOT CONNECTED (Running in Local Mode)'}",
        "",
        "Utility Libraries Status:",
        f"  [+] LangChain text splitter      : {'AVAILABLE' if LANGCHAIN_AVAILABLE else 'NOT AVAILABLE'}",
        f"  [+] Unstructured semantic chunker: {'AVAILABLE' if UNSTRUCTURED_AVAILABLE else 'NOT AVAILABLE'}",
        f"  [+] Manual Splitter              : {'AVAILABLE' if MANUAL_SPLITTER_AVAILABLE else 'NOT AVAILABLE'}",
        "",
        "Storage Configuration:",
        f"  [+] Type     : {config.storage_type.upper()}",
        f"  [+] Path     : {config.local_base_path if config.storage_type == 'local' else 'S3 Bucket'}",
        "=" * 60,
        ""
    ]
    for line in status_msg:
        logger.info(line)