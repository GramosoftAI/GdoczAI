# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import os
import sys
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

import logging

# Configure logging IMMEDIATELY (before importing other modules)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/olmocr_server.log', mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

logging.getLogger('absl').setLevel(logging.ERROR)
logging.getLogger('tensorflow').setLevel(logging.ERROR)

# Create logger for this module
logger = logging.getLogger(__name__)

logger.info("?? LOGGING SYSTEM INITIALIZED - Writing to logs/olmocr_server.log")

import time
import asyncio
import secrets
import base64
import mimetypes
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, File, UploadFile, Form, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

# Import all components
from src.services.sundarams.sundarams_ocr_server_config import config, db_storage, chunker, GEMINI_AVAILABLE, CHANDRA_AVAILABLE, QWEN_AVAILABLE, print_startup_summary
from src.services.sundarams.sundarams_ocr_server_storage import (
    StorageManager, generate_timestamped_filename,
    get_document_config, get_document_config_or_fallback,
    verify_jwt_token, validate_document_config,
    should_use_langchain_chunking, should_validate_markdown
)
from src.services.sundarams.sundarams_ocr_server_gemini import GeminiJSONGenerator
from src.services.sundarams.sundarams_ocr_server_mistral_processor import MistralProcessor
from src.services.sundarams.sundarams_ocr_server_qwen_processor import QwenProcessor
from src.services.sundarams.sundarams_ocr_server_chandra_processor import ChandraProcessor
from src.services.sundarams.sundarams_ocr_server_email import send_ocr_failure_email

# Import API key validation function
from src.api.routes.api_server_apikey_generate import validate_api_key_from_header

import threading
from collections import deque

# Global state for queue management
ocr_processing_lock = threading.Lock()
ocr_queue = deque()
queue_lock = threading.Lock()

# Initialize components
storage_manager = StorageManager(config)
gemini_generator = GeminiJSONGenerator(config, chunker, GEMINI_AVAILABLE)
mistral_processor = MistralProcessor()
qwen_processor = QwenProcessor()
chandra_processor = ChandraProcessor()

# ============================================================================
# FASTAPI APPLICATION
# ============================================================================
app = FastAPI(
    title="OCR API Server",
    description="PDF to Markdown extraction and JSON generation using Mistral OCR or Qwen3-VL (DeepInfra)",
    version="15.1.0",
    docs_url="/ocr/docs",
    redoc_url="/ocr/redoc",
    openapi_url="/ocr/openapi.json"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """Server startup event."""
    logger.info("OCR SERVER STARTING...")
    print_startup_summary()
    
@app.get("/")
def root():
    """Root endpoint with API information."""
    return {
        "service": "OCR API (Model-Based Routing + Dynamic Gemini Selection)",
        "version": "15.1.0",
        "status": "running",
        "processing_modes": {
            "mistral": "Mistral OCR ? Strict Validation ? Conditional Split ? Smart Gemini",
            "qwen": "Qwen3-VL ? Strict Validation ? Conditional Split ? Smart Gemini",
            "chandra": "Chandra ? Strict Validation ? Conditional Split ? Smart Gemini"
        },
        "mistral_features": {
            "strict_validation": "No fallback - throws error if validation fails",
            "conditional_chunking": "LangChain if keys exist, full markdown otherwise",
            "dynamic_gemini": "2.0 Flash (<20K, 8K out) or 2.5 Flash (>=20K, 65K out)",
            "post_processing": "Only applied when content is chunked",
            "unstructured_chunking": "Disabled in Mistral mode"
        },
        "qwen_features": {
            "model": "Qwen/Qwen3-VL-235B-A22B-Instruct",
            "strict_validation": "No fallback - throws error if validation fails",
            "conditional_chunking": "LangChain if keys exist, full markdown otherwise",
            "dynamic_gemini": "2.0 Flash (<20K, 8K out) or 2.5 Flash (>=20K, 65K out)",
            "post_processing": "Only applied when content is chunked",
            "unstructured_chunking": "Disabled in Qwen mode"
        },
        "chandra_features": {
            "model": "Datalab Marker API",
            "strict_validation": "No fallback - throws error if validation fails",
            "conditional_chunking": "LangChain if keys exist, full markdown/HTML otherwise",
            "dynamic_gemini": "Gemini 2.5 Flash (65K out)",
            "post_processing": "Only applied when content is chunked",
            "unstructured_chunking": "Disabled in Chandra mode"
        },
        "api_type": "Official Python API",
        "json_generation": "enabled" if gemini_generator.enabled else "disabled",
        "authentication": "JWT token or API key (optional)",
        "mandatory_fields": ["file (PDF)", "model (mistral/qwen/chandra)", "document_type (text)"],
        "optional_fields": ["page_range", "schema_json (JSON string)", "output_format (string)"],
        "schema_support": "database_lookup_or_dynamic",
        "storage_type": config.storage_type.upper(),
        "timestamped_filenames": "enabled",
        "generic_processing": "all_document_types_supported",
        "configuration": "database_driven_consolidated_keys",
        "invoice_extraction": "enabled",
        "api_key_validation": "enabled",
        "key_storage": {
            "conditional_keys": "document_types.conditional_keys",
            "langchain_keys": "document_types.langchain_keys",
            "schema_json": "document_schemas.schema_json"
        },
        "enhanced_tracking": "enabled",
        "tracking_features": [
            "separate_markdown_storage",
            "missed_keys_recording",
            "email_notifications",
            "invoice_number_extraction"
        ],
        "merge_strategy": "conditional_post_processing",
        "merge_features": [
            "deterministic_rule_based_merge",
            "no_llm_merge",
            "post_processing_only_when_chunked"
        ],
        "manual_splitting": {
            "enabled": config.manual_split_enabled,
            "threshold_characters": config.manual_split_threshold,
            "max_rows_per_chunk": config.manual_split_max_rows,
            "applies_to": "langchain_chunks_only",
            "features": [
                "table_aware_splitting",
                "header_preservation",
                "configurable_thresholds",
                "improved_extraction_accuracy"
            ]
        },
        "email_notifications": {
            "enabled": True,
            "features": [
                "ocr_failure_alerts",
                "validation_failure_alerts",
                "cc_support",
                "missed_keys_reporting",
                "processing_metrics"
            ]
        },
        "endpoints": {
            "health": "/health",
            "ocr_markdown_only": "/ocr/markdown-only",
            "extract_markdown": "/extract/markdown",
            "models": "/v1/models",
            "docs": "/ocr/docs"
        }
    }

@app.get("/health")
def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "ocr_engines": {
            "mistral": "Mistral OCR API",
            "qwen": "Qwen/Qwen3-VL-235B-A22B-Instruct (DeepInfra)",
            "chandra": "Datalab Marker API"
        },
        "backend": "Mistral AI / DeepInfra / Datalab API",
        "processing_mode": "model_based_routing",
        "supported_models": ["mistral", "qwen", "chandra"],
        "model_loading": "api_based",
        "supported_formats": ["pdf", "jpg", "jpeg", "png", "webp", "bmp"],
        "output_format": "markdown + json",
        "json_generation": "enabled" if gemini_generator.enabled else "disabled",
        "authentication": "optional",
        "storage_type": config.storage_type.upper()
    }

def generate_unique_request_id() -> str:

    random_bytes = secrets.token_bytes(16)
    request_id = base64.urlsafe_b64encode(random_bytes).decode('utf-8').rstrip('=')
    return request_id
# ============================================================================
# SHARED PROCESSING LOGIC
# ============================================================================

async def _run_ocr_processing(
    model_label: str,
    processor,
    file_bytes: bytes,
    timestamped_filename: str,
    file_extension: str,
    page_range: Optional[str]
):

    if file_extension == '.pdf':
        logger.info(f"Processing PDF with {model_label}" + (f" (page range: {page_range})" if page_range else ""))

        success, markdown_content, page_count, error = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: processor.process_pdf(file_bytes, timestamped_filename, page_range=page_range)
        )
    else:
        logger.info(f"Processing image with {model_label} ({file_extension})")

        success, markdown_content, page_count, error = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: processor.process_image(file_bytes, timestamped_filename, file_extension)
        )

    return success, markdown_content, page_count, error

from src.services.sundarams.sundarams_ocr_server_vendor_endpoint import router as vendor_router
app.include_router(vendor_router)

@app.get("/v1/models")
def list_models():
    return {
        "object": "list",
        "data": [
            {
                "id": "mistral",
                "object": "model",
                "created": 1234567890,
                "owned_by": "mistral",
                "description": "Mistral OCR (mistral-ocr-latest) - PDF extraction with strict validation, conditional LangChain chunking, manual splitting, Gemini JSON extraction, and email failure notifications."
            },
            {
                "id": "qwen",
                "object": "model",
                "created": 1234567890,
                "owned_by": "deepinfra",
                "description": "Qwen3-VL (Qwen/Qwen3-VL-235B-A22B-Instruct) - PDF extraction with strict validation, conditional LangChain chunking, manual splitting, Gemini JSON extraction, and email failure notifications."
            },
            {
                "id": "chandra",
                "object": "model",
                "created": 1234567890,
                "owned_by": "datalab",
                "description": "Chandra (Datalab Marker API) - PDF extraction with strict validation, conditional LangChain chunking, manual splitting, Gemini JSON extraction, and email failure notifications."
            }
        ]
    }

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    import uvicorn

    logger.info("STARTING OCR SERVER (MISTRAL + QWEN + CHANDRA MODEL ROUTING)")
    logger.info("Server will be available at: http://0.0.0.0:4433")
    logger.info("API documentation at: http://0.0.0.0:4433/ocr/docs")
    print_startup_summary()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=4433,
        log_level="info"
    )