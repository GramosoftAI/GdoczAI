# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
Configuration management and initialization for OLMOCR Server.

Handles:
- Loading YAML configuration
- Logging setup
- Gemini API availability check
- OLMOCR (DeepInfra) configuration
- Qwen VL (DeepInfra) configuration
- Chandra (Datalab Marker) configuration          ? NEW
- Database storage initialization
- Manual splitting configuration for oversized chunks
"""

import os
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
logger.info("=" * 80)
logger.info("OLMOCR SERVER - LOGGING INITIALIZED")
logger.info("=" * 80)
logger.info(f"Log file: logs/olmocr_server.log")
logger.info("=" * 80)

# ============================================================================
# CONFIGURATION CLASS
# ============================================================================
class Config:
    """Configuration management for the OLMOCR server"""

    def __init__(self):
        self.config = self._load_config()

        # ----------------------------------------------------------------
        # Chunking settings (for Unstructured and LangChain)
        # ----------------------------------------------------------------
        self.chunking_enabled = self.config.get('chunking', {}).get('enabled')
        self.chunk_size       = self.config.get('chunking', {}).get('chunk_size')
        self.chunk_overlap    = self.config.get('chunking', {}).get('overlap')
        logger.info(f"? Chunking: {'ENABLED' if self.chunking_enabled else 'DISABLED'}")
        if self.chunking_enabled:
            logger.info(f"? Chunk size: {self.chunk_size} chars, overlap: {self.chunk_overlap}")

        # ----------------------------------------------------------------
        # Manual splitting settings
        # ----------------------------------------------------------------
        manual_split_config = self.config.get('manual_splitting', {})
        self.manual_split_enabled   = manual_split_config.get('enabled')
        self.manual_split_threshold = manual_split_config.get('threshold_characters')
        self.manual_split_max_rows  = manual_split_config.get('max_rows_per_chunk')

        logger.info("=" * 60)
        logger.info("? MANUAL SPLITTING CONFIGURATION:")
        logger.info(f"   Status    : {'ENABLED' if self.manual_split_enabled else 'DISABLED'}")
        logger.info(f"   Threshold : {self.manual_split_threshold} characters")
        logger.info(f"   Max rows  : {self.manual_split_max_rows} per chunk")
        logger.info(f"   Applies to: LangChain chunks ONLY")
        logger.info(f"   Skips     : Unstructured semantic chunks")
        logger.info("=" * 60)

        # ----------------------------------------------------------------
        # OLMOCR settings (DeepInfra) - REQUIRED
        # ----------------------------------------------------------------
        self.olmocr_deepinfra_api_key = self.config.get('olmocr_deepinfra', {}).get('api_key')
        self.olmocr_deepinfra_model   = self.config.get('olmocr_deepinfra', {}).get('model')
        self.olmocr_deepinfra_timeout = self.config.get('olmocr_deepinfra', {}).get('timeout', 300)

        if self.olmocr_deepinfra_api_key:
            logger.info(f"? OLMOCR: ENABLED (DeepInfra)")
            logger.info(f"  Model  : {self.olmocr_deepinfra_model}")
            logger.info(f"  Timeout: {self.olmocr_deepinfra_timeout}s")
        else:
            logger.error(f"? OLMOCR: DISABLED (No DeepInfra API key)")
            logger.error("   OLMOCR Server requires OLMOCR to be configured!")

        # ----------------------------------------------------------------
        # QWENOCR settings (DeepInfra) - REQUIRED
        # ----------------------------------------------------------------
        self.qwenocr_deepinfra_api_key = self.config.get('qwenocr_deepinfra', {}).get('api_key')
        self.qwenocr_deepinfra_model   = self.config.get('qwenocr_deepinfra', {}).get('model')
        self.qwenocr_deepinfra_timeout = self.config.get('qwenocr_deepinfra', {}).get('timeout')

        if self.qwenocr_deepinfra_api_key:
            logger.info(f"? QWENOCR: ENABLED (DeepInfra)")
            logger.info(f"  Model  : {self.qwenocr_deepinfra_model}")
            logger.info(f"  Timeout: {self.qwenocr_deepinfra_timeout}s")
        else:
            logger.error(f"? QWENOCR: DISABLED (No DeepInfra API key)")
            logger.error("   QWENOCR Server requires QWENOCR to be configured!")

        # ----------------------------------------------------------------
        # CHANDRA settings (Datalab Marker API) - NEW
        # ----------------------------------------------------------------
        chandra_cfg = self.config.get('chandra_datalab', {})

        self.chandra_datalab_api_key       = chandra_cfg.get('api_key')
        self.chandra_datalab_output_format = chandra_cfg.get('output_format')
        self.chandra_datalab_mode          = chandra_cfg.get('mode')
        self.chandra_datalab_timeout       = chandra_cfg.get('timeout')
        self.chandra_datalab_poll_interval = chandra_cfg.get('poll_interval')
        self.chandra_datalab_max_retries   = chandra_cfg.get('max_retries')

        logger.info("=" * 60)
        if self.chandra_datalab_api_key:
            masked_key = (
                self.chandra_datalab_api_key[:6]
                + '*' * max(0, len(self.chandra_datalab_api_key) - 6)
            )
            logger.info(f"? CHANDRA: ENABLED (Datalab Marker API)")
            logger.info(f"  API key      : {masked_key}")
            logger.info(f"  Output format: {self.chandra_datalab_output_format}")
            logger.info(f"  Mode         : {self.chandra_datalab_mode}")
            logger.info(f"  Timeout      : {self.chandra_datalab_timeout}s")
            logger.info(f"  Poll interval: {self.chandra_datalab_poll_interval}s")
            logger.info(f"  Max retries  : {self.chandra_datalab_max_retries}")
        else:
            logger.warning(f"? CHANDRA: DISABLED (No Datalab API key)")
            logger.warning("   Set 'chandra_datalab.api_key' in config.yaml")
            logger.warning("   or export DATALAB_API_KEY=<your-key>")
        logger.info("=" * 60)

        # ----------------------------------------------------------------
        # JWT settings
        # ----------------------------------------------------------------
        self.jwt_secret = self.config.get('security', {}).get('jwt_secret_key')
        logger.info(f"? JWT Authentication: ENABLED")

        # ----------------------------------------------------------------
        # PostgreSQL settings
        # ----------------------------------------------------------------
        self.pg_config = self.config.get('postgres', {})

        # ----------------------------------------------------------------
        # Storage settings
        # ----------------------------------------------------------------
        self.storage_type       = self.config.get('storage', {}).get('storage_type', 'local')
        self.local_base_path    = Path(
            self.config.get('storage', {}).get('local_storage', {}).get('base_path', './stored_pdfs/')
        )
        self.create_date_folders = self.config.get('storage', {}).get(
            'local_storage', {}
        ).get('create_date_folders', True)

        # S3 settings (if using S3)
        if self.storage_type == 's3':
            self.s3_config = self.config.get('storage', {}).get('s3_storage', {})

        logger.info(f"? Storage Type: {self.storage_type.upper()}")
        if self.storage_type == 'local':
            logger.info(f"? Local Storage Path: {self.local_base_path}")

        # ----------------------------------------------------------------
        # GENERIC: LangChain chunking settings
        # ----------------------------------------------------------------
        self.langchain_chunk_size    = self.config.get('langchain_chunking', {}).get('chunk_size')
        self.langchain_chunk_overlap = self.config.get('langchain_chunking', {}).get('chunk_overlap')

        logger.info(
            f"? LangChain chunker settings: "
            f"{self.langchain_chunk_size} chars, {self.langchain_chunk_overlap} overlap"
        )

        # ----------------------------------------------------------------
        # Unstructured chunking settings
        # ----------------------------------------------------------------
        self.unstructured_max_chars      = self.config.get('unstructured_chunking', {}).get('max_characters', 20000)
        self.unstructured_combine_chars  = self.config.get('unstructured_chunking', {}).get('combine_text_under_n_chars', 1000)
        self.unstructured_new_after_chars = self.config.get('unstructured_chunking', {}).get('new_after_n_chars', 19000)

        logger.info(
            f"? Unstructured chunker settings: "
            f"{self.unstructured_max_chars} max chars, combine under {self.unstructured_combine_chars}"
        )

        logger.info("=" * 60)
        logger.info("? GENERIC CONFIGURATION LOADED")
        logger.info("=" * 60)
        logger.info("? All document type configurations loaded from database:")
        logger.info("   ? conditional_keys -> document_types.conditional_keys column")
        logger.info("   ? langchain_keys   -> document_types.langchain_keys column")
        logger.info("   ? schema_json      -> document_schemas table (unchanged)")
        logger.info("=" * 60)
        logger.info("? SIMPLIFIED DESIGN:")
        logger.info("   ? All keys stored in document_types table")
        logger.info("   ? No separate tables for conditional_keys or langchain_keys")
        logger.info("   ? Single query to fetch all document configuration")
        logger.info("   ? Improved performance with consolidated storage")
        logger.info("=" * 60)
        logger.info("? CHUNKING STRATEGY:")
        logger.info("   ? LangChain   : Used when langchain_keys exist (configured types)")
        logger.info("   ? Manual Split: Applied to LangChain chunks > 7000 chars")
        logger.info("   ? Unstructured: Used when langchain_keys NOT exist (unknown types)")
        logger.info("   ? Chonkie/Recursive: REMOVED from pipeline")
        logger.info("=" * 60)

    def _load_config(self) -> Dict:
        """Load configuration from config.yaml"""
        try:
            with open('config/config.yaml', 'r') as f:
                config = yaml.safe_load(f)
                logger.info("? Configuration loaded from config.yaml")
                return config if config else {}
        except FileNotFoundError:
            logger.warning("? config.yaml not found, using defaults")
            return {}
        except Exception as e:
            logger.warning(f"? Failed to load config.yaml: {e}")
            return {}


# Initialize global config
config = Config()

# ============================================================================
# CHUNKER COMPATIBILITY
# ============================================================================
# Chonkie/Recursive chunker is no longer used in the pipeline.
# Replaced by Unstructured semantic chunker for unknown document types.
# This variable is kept for backward compatibility with existing code.
chunker = None

# ============================================================================
# GEMINI PROCESSOR INITIALIZATION
# ============================================================================
try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
    GEMINI_AVAILABLE = True
    logger.info("? Google Generative AI library available")
except ImportError:
    GEMINI_AVAILABLE = False
    logger.error("? Google Generative AI not available. JSON generation will fail!")
    logger.error("   Install with: pip install google-generativeai")

# ============================================================================
# UNSTRUCTURED CHUNKER AVAILABILITY CHECK
# ============================================================================
UNSTRUCTURED_AVAILABLE = False
try:
    from unstructured.partition.text import partition_text
    from unstructured.chunking.title import chunk_by_title
    UNSTRUCTURED_AVAILABLE = True
    logger.info("? Unstructured library available for semantic chunking")
    logger.info(f"   Max chars   : {config.unstructured_max_chars}")
    logger.info(f"   Combine under: {config.unstructured_combine_chars} chars")
except ImportError:
    logger.warning("? Unstructured library not available")
    logger.warning("   Install with: pip install unstructured")
    logger.warning("   Unknown document types will NOT be processed")

# ============================================================================
# LANGCHAIN TEXT SPLITTER AVAILABILITY CHECK
# ============================================================================
LANGCHAIN_AVAILABLE = False
try:
    from langchain_text_splitters import MarkdownHeaderTextSplitter
    LANGCHAIN_AVAILABLE = True
    logger.info("? LangChain text splitter available")
    logger.info(f"   Chunk size: {config.langchain_chunk_size} chars")
    logger.info(f"   Overlap   : {config.langchain_chunk_overlap} chars")
except ImportError:
    logger.warning("? LangChain text splitter not available")
    logger.warning("   Install with: pip install langchain-text-splitters")
    logger.warning("   Configured document types (with langchain_keys) will fail")

# ============================================================================
# MANUAL SPLITTER AVAILABILITY CHECK
# ============================================================================
MANUAL_SPLITTER_AVAILABLE = False
try:
    from src.services.ocr_pipeline.ocr_server_manual_splitter import (
        ManualMarkdownSplitter, process_oversized_chunks
    )
    MANUAL_SPLITTER_AVAILABLE = True
    logger.info("? Manual splitter available for oversized LangChain chunks")
    logger.info(f"   Threshold: {config.manual_split_threshold} characters")
    logger.info(f"   Max rows : {config.manual_split_max_rows} per chunk")
except ImportError:
    logger.warning("? Manual splitter not available")
    logger.warning("   Ensure ocr_server_manual_splitter.py is present")
    logger.warning("   LangChain chunks over 7000 chars may cause extraction issues")

# ============================================================================
# DATABASE STORAGE INITIALIZATION
# ============================================================================
db_storage: Optional[DatabaseStorage] = None
try:
    pg_config = config.config.get('postgres', {})
    if pg_config.get('host') and pg_config.get('user'):
        db_storage = DatabaseStorage(pg_config)
        logger.info("? Database storage initialized successfully")
    else:
        logger.warning("? PostgreSQL not configured - database storage disabled")
except Exception as e:
    logger.error(f"? Failed to initialize database storage: {e}")
    logger.warning("? Continuing without database storage")

# ============================================================================
# OLMOCR AVAILABILITY CHECK
# ============================================================================
OLMOCR_AVAILABLE = False
if config.olmocr_deepinfra_api_key:
    OLMOCR_AVAILABLE = True
    logger.info("? OLMOCR available (DeepInfra API configured)")
else:
    logger.error("? OLMOCR not available - No DeepInfra API key")
    logger.error("   OLMOCR Server cannot function without OLMOCR!")

# ============================================================================
# QWEN VL AVAILABILITY CHECK
# ============================================================================
QWEN_VL_AVAILABLE = False
if config.qwenocr_deepinfra_api_key:
    QWEN_VL_AVAILABLE = True
    logger.info("? QWEN VL available (DeepInfra API configured)")
else:
    logger.error("? QWEN VL not available - No DeepInfra API key")
    logger.error("   QWEN VL Server cannot function without QWEN VL!")

# ============================================================================
# CHANDRA (DATALAB MARKER) AVAILABILITY CHECK                        ? NEW
# ============================================================================
CHANDRA_AVAILABLE = False
if config.chandra_datalab_api_key:
    CHANDRA_AVAILABLE = True
    logger.info("? CHANDRA available (Datalab Marker API configured)")
else:
    logger.warning("? CHANDRA not available - No Datalab API key")
    logger.warning("   Set chandra_datalab.api_key in config.yaml to enable Chandra")

# ============================================================================
# STARTUP SUMMARY
# ============================================================================
logger.info("=" * 60)
logger.info("? CONFIGURATION SUMMARY")
logger.info("=" * 60)
logger.info(f"? OLMOCR  : {'AVAILABLE' if OLMOCR_AVAILABLE else 'NOT AVAILABLE'}")
logger.info(f"? QWEN VL : {'AVAILABLE' if QWEN_VL_AVAILABLE else 'NOT AVAILABLE'}")
logger.info(f"? CHANDRA : {'AVAILABLE' if CHANDRA_AVAILABLE else 'NOT AVAILABLE'}")  # NEW
logger.info(f"? Gemini  : {'AVAILABLE' if GEMINI_AVAILABLE else 'NOT AVAILABLE'}")
logger.info(f"? Chonkie : REMOVED (no longer used)")
logger.info(f"? Unstructured   : {'AVAILABLE' if UNSTRUCTURED_AVAILABLE else 'NOT AVAILABLE'}")
logger.info(f"? LangChain      : {'AVAILABLE' if LANGCHAIN_AVAILABLE else 'NOT AVAILABLE'}")
logger.info(f"? Manual Splitter: {'AVAILABLE' if MANUAL_SPLITTER_AVAILABLE else 'NOT AVAILABLE'}")
logger.info(f"? Database       : {'CONNECTED' if db_storage else 'NOT CONNECTED'}")
logger.info("=" * 60)
logger.info("? GENERIC MODE: All document configurations from database")
logger.info("=" * 60)
logger.info("?? CONFIGURATION STORAGE:")
logger.info("   ? conditional_keys -> document_types.conditional_keys")
logger.info("   ? langchain_keys   -> document_types.langchain_keys")
logger.info("   ? schema_json      -> document_schemas.schema_json")
logger.info("=" * 60)
logger.info("? ARCHITECTURE BENEFITS:")
logger.info("   ? Consolidated storage - all keys in one table")
logger.info("   ? Faster queries - single database lookup")
logger.info("   ? Simplified management - no separate tables to maintain")
logger.info("   ? Better data integrity - fewer tables to keep in sync")
logger.info("   ? Easier migrations - centralized key storage")
logger.info("=" * 60)
logger.info("? UPDATED CHUNKING WORKFLOW:")
logger.info("=" * 60)
logger.info("? CONFIGURED TYPES (langchain_keys present):")
logger.info("   ? Use LangChain section-based splitter")
logger.info("   ? Split by database-defined section markers")
logger.info("   ? Preserves document structure")
logger.info("   ? Apply manual splitting if chunks > 7000 chars")
logger.info("   ? Split tables: 1 header + max 10 data rows")
logger.info("")
logger.info("? UNKNOWN TYPES (langchain_keys NOT present):")
logger.info("   ? Use Unstructured semantic chunker")
logger.info("   ? Context-aware, intelligent chunking")
logger.info("   ? Preserves headings, sections, tables")
logger.info("   ? Manual splitting NOT applied (already optimal)")
logger.info("")
logger.info("? MERGE STRATEGY:")
logger.info("   ? Post-processing merge for ALL document types")
logger.info("   ? LLM merge REMOVED from final step")
logger.info("   ? Deterministic, rule-based merge")
logger.info("   ? Token savings: ~2000-4000 per document")
logger.info("=" * 60)
logger.info("? MANUAL SPLITTING FEATURES:")
logger.info("   ? Table-aware splitting (preserves headers)")
logger.info(f"   ? Configurable threshold (default: {config.manual_split_threshold} chars)")
logger.info(f"   ? Configurable row limit (default: {config.manual_split_max_rows} rows)")
logger.info("   ? Only for LangChain chunks")
logger.info("   ? Improves Gemini extraction accuracy")
logger.info("=" * 60)

# ============================================================================
# CRITICAL WARNINGS
# ============================================================================
if not OLMOCR_AVAILABLE:
    logger.error("=" * 60)
    logger.error("??? CRITICAL ERROR ???")
    logger.error("=" * 60)
    logger.error("OLMOCR is NOT configured!")
    logger.error("OLMOCR Server cannot function without OLMOCR.")
    logger.error("")
    logger.error("To fix:")
    logger.error("  1. Set DEEPINFRA_API_KEY in config.yaml or environment")
    logger.error("  2. Restart the server")
    logger.error("=" * 60)

if not GEMINI_AVAILABLE:
    logger.error("=" * 60)
    logger.error("??? CRITICAL ERROR ???")
    logger.error("=" * 60)
    logger.error("Google Generative AI is NOT installed!")
    logger.error("OLMOCR Server cannot function without Gemini.")
    logger.error("")
    logger.error("To fix:")
    logger.error("  1. Install: pip install google-generativeai")
    logger.error("  2. Restart the server")
    logger.error("=" * 60)

if not UNSTRUCTURED_AVAILABLE:
    logger.error("=" * 60)
    logger.error("??? CRITICAL WARNING ???")
    logger.error("=" * 60)
    logger.error("Unstructured library is NOT installed!")
    logger.error("")
    logger.error("This means:")
    logger.error("  ? Unknown document types CANNOT be processed")
    logger.error("  ? Only configured types (with langchain_keys) will work")
    logger.error("")
    logger.error("To fix:")
    logger.error("  1. Install: pip install unstructured")
    logger.error("  2. Restart the server")
    logger.error("=" * 60)

if not LANGCHAIN_AVAILABLE:
    logger.warning("=" * 60)
    logger.warning("? WARNING: LangChain not available")
    logger.warning("=" * 60)
    logger.warning("Configured document types (with langchain_keys) will fail!")
    logger.warning("")
    logger.warning("To fix:")
    logger.warning("  1. Install: pip install langchain-text-splitters")
    logger.warning("  2. Restart the server")
    logger.warning("=" * 60)

if not MANUAL_SPLITTER_AVAILABLE:
    logger.warning("=" * 60)
    logger.warning("? WARNING: Manual splitter not available")
    logger.warning("=" * 60)
    logger.warning("LangChain chunks over 7000 characters may cause:")
    logger.warning("  ? Reduced Gemini extraction accuracy")
    logger.warning("  ? Inconsistent JSON output")
    logger.warning("  ? Token limit issues")
    logger.warning("")
    logger.warning("To fix:")
    logger.warning("  1. Ensure ocr_server_manual_splitter.py exists")
    logger.warning("  2. Restart the server")
    logger.warning("=" * 60)

# Non-critical warning for Chandra (optional model)
if not CHANDRA_AVAILABLE:
    logger.warning("=" * 60)
    logger.warning("? INFO: Chandra (Datalab Marker) not configured")
    logger.warning("=" * 60)
    logger.warning("model=chandra requests will be rejected.")
    logger.warning("")
    logger.warning("To enable Chandra:")
    logger.warning("  Add to config.yaml:")
    logger.warning("    chandra_datalab:")
    logger.warning("      api_key: '<your-datalab-api-key>'")
    logger.warning("      output_format: 'html'    # or 'markdown'")
    logger.warning("      mode: 'accurate'         # or 'fast'")
    logger.warning("      timeout: 300")
    logger.warning("      poll_interval: 3")
    logger.warning("      max_retries: 2")
    logger.warning("  Or: export DATALAB_API_KEY=<your-key>")
    logger.warning("=" * 60)

if (
    OLMOCR_AVAILABLE
    and GEMINI_AVAILABLE
    and UNSTRUCTURED_AVAILABLE
    and LANGCHAIN_AVAILABLE
    and MANUAL_SPLITTER_AVAILABLE
):
    logger.info("=" * 60)
    logger.info("??? OLMOCR SERVER READY ???")
    logger.info("=" * 60)
    logger.info("System is fully operational:")
    logger.info("  ? OLMOCR   for PDF -> Markdown conversion")
    logger.info("  ? Qwen VL  for PDF -> Markdown conversion")
    if CHANDRA_AVAILABLE:
        logger.info("  ? Chandra  for PDF -> HTML/Markdown conversion (Datalab Marker)")
    logger.info("  ? Gemini   for JSON extraction")
    logger.info("  ? Configured types  -> LangChain splitter + Manual split")
    logger.info("  ? Unknown types     -> Unstructured semantic chunker")
    logger.info("  ? Oversized chunks  -> Table-aware manual splitting")
    logger.info("=" * 60)