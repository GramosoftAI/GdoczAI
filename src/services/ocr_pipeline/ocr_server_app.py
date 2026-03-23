# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import logging
import sys

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

# Create logger for this module
logger = logging.getLogger(__name__)

logger.info("=" * 80)
logger.info("[BOOT] LOGGING SYSTEM INITIALIZED - Writing to logs/olmocr_server.log")
logger.info("=" * 80)

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

# ============================================================================
# IMPORT ALL COMPONENTS
# ============================================================================
from src.services.ocr_pipeline.ocr_server_config import (
    config, db_storage, chunker,
    GEMINI_AVAILABLE,
    OLMOCR_AVAILABLE,
    QWEN_VL_AVAILABLE,
    CHANDRA_AVAILABLE,
)
from src.services.ocr_pipeline.ocr_server_storage import (
    StorageManager, generate_timestamped_filename,
    get_document_config, get_document_config_or_fallback,
    verify_jwt_token, validate_document_config,
    should_use_langchain_chunking, should_validate_markdown
)
from src.services.ocr_pipeline.ocr_server_gemini    import GeminiJSONGenerator
from src.services.ocr_pipeline.ocr_server_qwen      import QwenJSONGenerator
from src.services.ocr_pipeline.ocr_server_processor  import OlmocrProcessor
from src.services.ocr_pipeline.ocr_server_processor2 import QwenProcessor
from src.services.ocr_pipeline.ocr_server_processor3 import ChandraProcessor
from src.services.ocr_pipeline.ocr_server_webhook   import WebhookHandler, trigger_webhook_if_needed
from src.services.ocr_pipeline.ocr_server_email     import send_ocr_failure_email

# Import API key validation function
from src.api.routes.api_server_apikey_generate import validate_api_key_from_header

import threading
from collections import deque

# ============================================================================
# GLOBAL STATE FOR QUEUE MANAGEMENT
# ============================================================================
ocr_processing_lock = threading.Lock()
ocr_queue  = deque()
queue_lock = threading.Lock()

# ============================================================================
# INITIALIZE PROCESSORS & GENERATORS
# ============================================================================
storage_manager   = StorageManager(config)
gemini_generator  = GeminiJSONGenerator(config, chunker, GEMINI_AVAILABLE)
olmocr_processor  = OlmocrProcessor()
qwen_processor    = QwenProcessor()
chandra_processor = ChandraProcessor()
qwen_generator    = QwenJSONGenerator(config)


# ============================================================================
# UTILITY: EXTRACT INVOICE NUMBER FROM JSON OUTPUT
# ============================================================================
def extract_invoice_number(json_output: dict) -> Optional[str]:

    try:
        if not isinstance(json_output, dict):
            logger.warning("[WARN] json_output is not a dict: %s", type(json_output))
            return None

        if "Header" in json_output:
            header = json_output["Header"]

            if isinstance(header, list) and len(header) > 0:
                header_obj = header[0]

                if isinstance(header_obj, dict) and "InvoiceDetails" in header_obj:
                    invoice_details = header_obj["InvoiceDetails"]

                    if isinstance(invoice_details, dict) and "Invoice_No" in invoice_details:
                        invoice_no = invoice_details["Invoice_No"]

                        if invoice_no and str(invoice_no).strip().lower() != "null":
                            logger.info("[OK] Invoice Number extracted: %s", invoice_no)
                            return str(invoice_no).strip()

        logger.warning("[WARN] Invoice_No not found in expected structure")
        return None

    except Exception as e:
        logger.error("[ERR] Error extracting invoice number: %s", e, exc_info=True)
        return None


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================
app = FastAPI(
    title="OCR API Server",
    description=(
        "PDF to Markdown extraction and JSON generation using "
        "OLMOCR or Qwen3-VL (DeepInfra) or Chandra (Datalab Marker)"
    ),
    version="15.2.0",
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


# ============================================================================
# STARTUP EVENT
# ============================================================================
@app.on_event("startup")
async def startup_event():
    logger.info("=" * 60)
    logger.info("OCR SERVER STARTING...")
    logger.info("=" * 60)

    logger.info("[OK] OLMOCR ready (no warmup required)")
    logger.info("     OLMOCR uses API-based processing (DeepInfra)")
    logger.info("[OK] Qwen VL ready (no warmup required)")
    logger.info("     Qwen VL uses API-based processing (DeepInfra)")

    if CHANDRA_AVAILABLE:
        logger.info("[OK] Chandra ready (no warmup required)")
        logger.info("     Chandra uses Datalab Marker API (async submit + poll)")
        logger.info("     Output format : %s", config.chandra_datalab_output_format)
        logger.info("     Mode          : %s", config.chandra_datalab_mode)
        logger.info("     Timeout       : %ss", config.chandra_datalab_timeout)
        logger.info("     Poll interval : %ss", config.chandra_datalab_poll_interval)
    else:
        logger.warning("[WARN] Chandra NOT configured (Datalab API key missing)")
        logger.warning("       model=chandra requests will be rejected with HTTP 503")

    logger.info("=" * 60)
    logger.info("OCR SERVER STARTED")
    logger.info("=" * 60)
    logger.info("[OK] Server ready for processing!")
    logger.info(
        "[>>] OCR Backends: OLMOCR (allenai/olmOCR-2-7B-1025) | "
        "Qwen (Qwen/Qwen3-VL-235B-A22B-Instruct) | "
        "Chandra (Datalab Marker API)"
    )
    logger.info("[>>] Processing Mode   : PDF -> Images -> OCR API")
    logger.info("[>>] Invoice Number    : Auto-extracted to unique_id column")
    logger.info("[>>] API Key Auth      : Optional (validates if provided)")
    logger.info(
        "[>>] JSON generation   : %s",
        "ENABLED" if gemini_generator.enabled else "DISABLED"
    )
    logger.info("[>>] Chunking Configuration:")
    logger.info("     Enabled      : %s", config.chunking_enabled)
    logger.info("     Chunk size   : %s chars", config.chunk_size)
    logger.info("     Chunk overlap: %s chars", config.chunk_overlap)
    logger.info(
        "     Chunker status: %s",
        "[OK] READY" if chunker is not None else "[ERR] FAILED TO INITIALIZE"
    )
    logger.info("=" * 60)
    logger.info("[>>] MANUAL SPLITTING CONFIGURATION:")
    logger.info(
        "     Status    : %s",
        "ENABLED" if config.manual_split_enabled else "DISABLED"
    )
    logger.info("     Threshold : %s characters", config.manual_split_threshold)
    logger.info("     Max rows  : %s per chunk",  config.manual_split_max_rows)
    logger.info("     Applies to: LangChain chunks ONLY")
    logger.info("     Skips     : Unstructured semantic chunks")
    logger.info("=" * 60)
    logger.info("[>>] MODEL-BASED ROUTING: ENABLED")
    logger.info("     OLMOCR MODE:")
    logger.info("       OLMOCR -> Strict Validation -> Conditional Split -> Smart Gemini")
    logger.info("       Fallback OCR    : DISABLED (OLMOCR only)")
    logger.info("       Unstructured    : DISABLED")
    logger.info("       Validation fail : HTTP 400 error (no retry)")
    logger.info("     QWEN MODE:")
    logger.info("       Qwen3-VL -> Strict Validation -> Conditional Split -> Smart Gemini")
    logger.info("       Fallback OCR    : DISABLED (Qwen only)")
    logger.info("       Unstructured    : DISABLED")
    logger.info("       Validation fail : HTTP 400 error (no retry)")
    logger.info("     CHANDRA MODE:")
    logger.info("       Datalab Marker -> Strict Validation -> Conditional Split -> Smart Gemini")
    logger.info("       Submit + Poll   : Async Datalab Marker API")
    logger.info("       Output format   : HTML or Markdown (configurable)")
    logger.info("       Fallback OCR    : DISABLED (Chandra only)")
    logger.info("       Unstructured    : DISABLED")
    logger.info("       Validation fail : HTTP 400 error (no retry)")
    logger.info("=" * 60)
    logger.info("[>>] DYNAMIC GEMINI MODEL SELECTION:")
    logger.info("     Content > 25,000 chars  -> Gemini 2.5 Flash (65K output)")
    logger.info("     Content <= 25,000 chars -> Gemini 2.0 Flash (8K output)")
    logger.info("     Applied only when NO chunking occurs")
    logger.info("=" * 60)
    logger.info("[>>] CONSOLIDATED KEY STORAGE:")
    logger.info("     conditional_keys -> document_types.conditional_keys")
    logger.info("     langchain_keys   -> document_types.langchain_keys")
    logger.info("     schema_json      -> document_schemas.schema_json")
    logger.info("=" * 60)
    logger.info("[>>] ENHANCED OCR TRACKING: ENABLED")
    logger.info("     Separate Markdown Storage")
    logger.info("     Strict Validation (no fallback)")
    logger.info("     Missing Keys Recording (missed_keys)")
    logger.info("     Email Notifications (with missed keys details)")
    logger.info("=" * 60)
    logger.info("[OK] CONDITIONAL POST-PROCESSING: ENABLED")
    logger.info("     Post-processing merge ONLY when content is chunked")
    logger.info("     No post-processing for single-shot Gemini calls")
    logger.info("     LLM merge REMOVED from final step")
    logger.info("     Deterministic rule-based merge")
    logger.info("=" * 60)
    logger.info("[>>] WEBHOOK HANDLER: ENABLED")
    logger.info("     Dynamic per-user webhook configuration")
    logger.info("     Token-based authentication")
    logger.info("     Automatic retry on failure (3 attempts)")
    logger.info("     30-second timeout")
    logger.info("=" * 60)


# ============================================================================
# ROOT INFO ENDPOINT
# ============================================================================
@app.get("/")
def root():
    return {
        "service": "OCR API Server",
        "version": "15.2.0",
        "status": "running",
        "ocr_engines": {
            "olmocr":  "allenai/olmOCR-2-7B-1025 (DeepInfra)",
            "qwen":    "Qwen/Qwen3-VL-235B-A22B-Instruct (DeepInfra)",
            "chandra": "Datalab Marker API (async submit + poll)",
        },
        "supported_models": ["olmocr", "qwen", "chandra"],
        "processing_features": [
            "pdf_to_markdown",
            "image_to_markdown",
            "conditional_key_validation",
            "langchain_chunking",
            "manual_table_splitting",
            "gemini_json_extraction",
            "webhook_delivery",
            "email_failure_notifications",
            "invoice_number_extraction"
        ],
        "merge_strategy": "conditional_post_processing",
        "merge_features": [
            "deterministic_rule_based_merge",
            "no_llm_merge",
            "post_processing_only_when_chunked"
        ],
        "manual_splitting": {
            "enabled":              config.manual_split_enabled,
            "threshold_characters": config.manual_split_threshold,
            "max_rows_per_chunk":   config.manual_split_max_rows,
            "applies_to":           "langchain_chunks_only",
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
            "health":            "/health",
            "ocr_pdf":           "/ocr/pdf",
            "ocr_markdown_only": "/ocr/markdown-only",
            "extract_markdown":  "/extract/markdown",
            "models":            "/v1/models",
            "docs":              "/ocr/docs"
        }
    }


# ============================================================================
# HEALTH CHECK ENDPOINT
# ============================================================================
@app.get("/health")
def health():
    return {
        "status": "healthy",
        "ocr_engines": {
            "olmocr":  "allenai/olmOCR-2-7B-1025 (DeepInfra)",
            "qwen":    "Qwen/Qwen3-VL-235B-A22B-Instruct (DeepInfra)",
            "chandra": "Datalab Marker API ({})".format(
                "configured" if CHANDRA_AVAILABLE else "not configured"
            ),
        },
        "availability": {
            "olmocr":  OLMOCR_AVAILABLE,
            "qwen":    QWEN_VL_AVAILABLE,
            "chandra": CHANDRA_AVAILABLE,
        },
        "backend": {
            "olmocr":  "DeepInfra",
            "qwen":    "DeepInfra",
            "chandra": "Datalab Marker API",
        },
        "processing_mode":   "model_based_routing",
        "supported_models":  ["olmocr", "qwen", "chandra"],
        "model_loading":     "api_based",
        "supported_formats": ["pdf", "jpg", "jpeg", "png", "webp", "bmp"],
        "output_format":     "markdown + json",
        "json_generation":   "enabled" if gemini_generator.enabled else "disabled",
        "authentication":    "optional",
        "storage_type":      config.storage_type.upper()
    }


# ============================================================================
# REQUEST ID GENERATOR
# ============================================================================
def generate_unique_request_id() -> str:
    random_bytes = secrets.token_bytes(16)
    request_id   = base64.urlsafe_b64encode(random_bytes).decode('utf-8').rstrip('=')
    return request_id


# ============================================================================
# SHARED OCR PROCESSING LOGIC
# ============================================================================
async def _run_ocr_processing(
    model_label,
    processor,
    file_bytes,
    timestamped_filename,
    file_extension,
    page_range
):
    # Run OCR processing (PDF or image) using the given processor.
    # Compatible with OlmocrProcessor, QwenProcessor, and ChandraProcessor.
    # All three expose the same 4-tuple interface:
    #   process_pdf(bytes, filename, page_range=None)
    #       -> (success, markdown, page_count, error)
    #   process_image(bytes, filename, extension)
    #       -> (success, markdown, page_count, error)

    if file_extension == '.pdf':
        if page_range:
            logger.info(
                "[>>] STEP 2: Processing PDF with %s - Page Range: %s",
                model_label, page_range
            )
        else:
            logger.info("[>>] STEP 2: Processing entire PDF with %s...", model_label)

        logger.info("[OK] %s API-based processing (no local model)", model_label)

        if model_label == "Chandra":
            logger.info(
                "[>>] Chandra: async submit -> poll workflow (Datalab Marker API)"
            )
        else:
            logger.info("[OK] Empty page detection: ENABLED (skips blank pages)")

        success, markdown_content, page_count, error = \
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: processor.process_pdf(
                    file_bytes, timestamped_filename, page_range=page_range
                )
            )
    else:
        logger.info(
            "[>>] STEP 2: Processing image with %s (%s)...",
            model_label, file_extension
        )

        success, markdown_content, page_count, error = \
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: processor.process_image(
                    file_bytes, timestamped_filename, file_extension
                )
            )

    return success, markdown_content, page_count, error


# ============================================================================
# ROUTER INCLUDES
# ============================================================================
# Import routers AFTER app initialization and global component initialization
from src.services.ocr_pipeline.ocr_server_endpoint        import router as pdf_router
from src.services.ocr_pipeline.ocr_server_helper_endpoint import router as helper_router
from src.services.sundarams.sundarams_ocr_server_vendor_endpoint import router as vendor_router

app.include_router(pdf_router)
app.include_router(helper_router)
app.include_router(vendor_router)


# ============================================================================
# MODELS LIST ENDPOINT
# ============================================================================
@app.get("/v1/models")
def list_models():
    return {
        "object": "list",
        "data": [
            {
                "id":          "olmocr",
                "object":      "model",
                "created":     1234567890,
                "owned_by":    "deepinfra",
                "description": (
                    "OLMOCR (allenai/olmOCR-2-7B-1025) - PDF extraction with strict "
                    "validation, conditional LangChain chunking, manual splitting, "
                    "Gemini JSON extraction, webhook delivery, and email failure notifications."
                )
            },
            {
                "id":          "qwen",
                "object":      "model",
                "created":     1234567890,
                "owned_by":    "deepinfra",
                "description": (
                    "Qwen3-VL (Qwen/Qwen3-VL-235B-A22B-Instruct) - PDF extraction with "
                    "strict validation, conditional LangChain chunking, manual splitting, "
                    "Gemini JSON extraction, webhook delivery, and email failure notifications."
                )
            },
            {
                "id":          "chandra",
                "object":      "model",
                "created":     1234567890,
                "owned_by":    "datalab",
                "available":   CHANDRA_AVAILABLE,
                "description": (
                    "Chandra (Datalab Marker API) - PDF/image extraction via async "
                    "submit-and-poll workflow. Supports HTML and Markdown output formats. "
                    "Includes strict validation, conditional LangChain chunking, manual "
                    "splitting, Gemini JSON extraction, webhook delivery, and email "
                    "failure notifications."
                ),
                "config": {
                    "output_format":   config.chandra_datalab_output_format,
                    "mode":            config.chandra_datalab_mode,
                    "timeout_seconds": config.chandra_datalab_timeout,
                    "poll_interval":   config.chandra_datalab_poll_interval,
                }
            },
        ]
    }


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    import uvicorn

    logger.info("=" * 60)
    logger.info("STARTING OCR SERVER (OLMOCR + QWEN + CHANDRA MODEL ROUTING)")
    logger.info("=" * 60)
    logger.info("Server will be available at : http://0.0.0.0:3545")
    logger.info("API documentation at        : http://0.0.0.0:3545/ocr/docs")
    logger.info(
        "OCR Backends: OLMOCR (allenai/olmOCR-2-7B-1025) | "
        "Qwen (Qwen/Qwen3-VL-235B-A22B-Instruct) | "
        "Chandra (Datalab Marker API)"
    )
    logger.info("Processing Mode  : PDF -> Images -> OCR API")
    logger.info(
        "Chunking         : %s",
        "ENABLED" if config.chunking_enabled else "DISABLED"
    )
    logger.info(
        "JSON generation  : %s",
        "ENABLED" if gemini_generator.enabled else "DISABLED"
    )
    logger.info(
        "Chandra          : %s",
        "ENABLED" if CHANDRA_AVAILABLE else "DISABLED (no API key)"
    )
    logger.info("=" * 60)
    logger.info("Authentication   : OPTIONAL")
    logger.info("  JWT Bearer token (Authorization header) -> extracts user_id")
    logger.info("  API Key (X-API-Key header)              -> validates only (no user_id)")
    logger.info("  Both optional - processes without auth if not provided")
    logger.info("  User ID: Extracted ONLY from JWT token")
    logger.info("=" * 60)
    logger.info("Mandatory Fields : PDF file + model + document_type")
    logger.info("Optional Fields  : page_range, schema_json, output_format")
    logger.info("Supported models : olmocr | qwen | chandra")
    logger.info("Request ID       : Auto-generated for all uploads")
    logger.info("User ID          : Extracted from JWT token")
    logger.info("=" * 60)
    logger.info("GENERIC PROCESSING: All document types supported")
    logger.info("Configuration    : Database-driven with consolidated keys")
    logger.info("=" * 60)
    logger.info("Storage Type     : %s", config.storage_type.upper())
    if config.storage_type == 'local':
        logger.info("Local Storage Path: %s", config.local_base_path)
    logger.info("Timestamped Filenames: ENABLED (format: filename_HHMMSS.ext)")
    logger.info("=" * 60)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=3545,
        log_level="info"
    )