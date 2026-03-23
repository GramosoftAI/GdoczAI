# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
Helper Endpoints for OCR Server.

Contains:
- /ocr/markdown-only: Pure OCR markdown extraction (no queue, true concurrency)
- /extract/markdown : Markdown-to-JSON extraction with nested schema support

Supported models for /ocr/markdown-only:
  - olmocr  : OLMOCR via DeepInfra API
  - qwen    : Qwen3-VL via DeepInfra API
  - chandra : Datalab Marker API              <- NEW
"""

import logging
import time
import asyncio
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, File, UploadFile, Form, Header, HTTPException
from fastapi.responses import JSONResponse
from src.services.ocr_pipeline.ocr_server_extract import MarkdownExtractionRequest, extract_from_markdown_endpoint

logger = logging.getLogger(__name__)

# Router definition using APIRouter
router = APIRouter(prefix="/ocr", tags=["ocr-helpers"])


@router.post("/markdown-only")
async def ocr_markdown_only(
    file: UploadFile = File(...),
    model: str = Form(...),
    page_range: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):

    # Import shared components from app module
    from src.services.ocr_pipeline.ocr_server_app import (
        storage_manager, olmocr_processor, qwen_processor, chandra_processor,
        generate_unique_request_id, _run_ocr_processing, config
    )
    from src.services.ocr_pipeline.ocr_server_storage import (
        generate_timestamped_filename, verify_jwt_token
    )
    from src.services.ocr_pipeline.ocr_server_config import (
        db_storage, CHANDRA_AVAILABLE
    )
    from src.api.routes.api_server_apikey_generate import validate_api_key_from_header

    request_id          = None
    file_path           = None
    timestamped_filename = None

    try:
        # Generate unique request ID (no queue, no lock)
        request_id = generate_unique_request_id()
        total_start_time = time.time()

        logger.info("=" * 80)
        logger.info(f"?? /ocr/markdown-only request received | request_id={request_id}")
        logger.info("=" * 80)

        # ============================================================
        # VALIDATE FILE FORMAT
        # ============================================================
        if not file.filename:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "No filename provided",
                    "request_id": request_id
                }
            )

        file_extension    = Path(file.filename).suffix.lower()
        supported_formats = {'.pdf', '.jpg', '.jpeg', '.png', '.webp', '.bmp'}

        if file_extension not in supported_formats:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "Unsupported file format. Supported: PDF, JPG, JPEG, PNG, WEBP, BMP",
                    "request_id": request_id
                }
            )

        # ============================================================
        # VALIDATE MODEL & ASSIGN PROCESSOR
        # ============================================================
        model_lower = model.lower()

        if model_lower == "olmocr":
            active_processor = olmocr_processor
            model_label      = "OLMOCR"
            logger.info(f"?? [markdown-only] OLMOCR mode activated | request_id={request_id}")

        elif model_lower == "qwen":
            active_processor = qwen_processor
            model_label      = "Qwen VL"
            logger.info(f"?? [markdown-only] Qwen VL mode activated | request_id={request_id}")

        # ----------------------------------------------------------------
        # CHANDRA model routing                                  <- NEW
        # ----------------------------------------------------------------
        elif model_lower == "chandra":
            if not CHANDRA_AVAILABLE:
                logger.error(
                    f"? [markdown-only] Chandra requested but Datalab API key "
                    f"not configured | request_id={request_id}"
                )
                return JSONResponse(
                    status_code=503,
                    content={
                        "success": False,
                        "error": (
                            "Chandra model is not available - "
                            "Datalab API key not configured"
                        ),
                        "message": (
                            "Add 'chandra_datalab.api_key' to config.yaml "
                            "or set DATALAB_API_KEY environment variable"
                        ),
                        "request_id": request_id
                    }
                )
            active_processor = chandra_processor
            model_label      = "Chandra"
            logger.info(
                f"?? [markdown-only] Chandra mode activated "
                f"(Datalab Marker API) | request_id={request_id}"
            )
            logger.info(
                f"   Output format: {config.chandra_datalab_output_format} | "
                f"Mode: {config.chandra_datalab_mode}"
            )

        else:
            logger.error(
                f"? [markdown-only] Invalid model: {model} | request_id={request_id}"
            )
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": (
                        f"Invalid model: {model}. "
                        "Supported models: olmocr, qwen, chandra"
                    ),
                    "request_id": request_id
                }
            )

        # ============================================================
        # AUTHENTICATION (optional) - same as /ocr/pdf
        # ============================================================
        user_id = None

        if x_api_key:
            logger.info(
                f"?? [markdown-only] API Key provided, validating... "
                f"| request_id={request_id}"
            )
            api_key_data = validate_api_key_from_header(x_api_key)
            if not api_key_data:
                logger.warning(
                    f"? [markdown-only] Invalid API key | request_id={request_id}"
                )
                return JSONResponse(
                    status_code=401,
                    content={
                        "success": False,
                        "error": "Invalid API key",
                        "message": "API key authentication failed",
                        "request_id": request_id
                    }
                )
            logger.info(f"? [markdown-only] Valid API key | request_id={request_id}")

        if authorization:
            logger.info(
                f"?? [markdown-only] JWT token provided, validating... "
                f"| request_id={request_id}"
            )
            token_data = verify_jwt_token(authorization, config.jwt_secret)
            if token_data:
                user_id = token_data.get('user_id')
                logger.info(
                    f"? [markdown-only] Authenticated user_id={user_id} "
                    f"| request_id={request_id}"
                )
            else:
                logger.error(
                    f"? [markdown-only] Invalid or expired JWT token "
                    f"| request_id={request_id}"
                )
                return JSONResponse(
                    status_code=401,
                    content={
                        "success": False,
                        "error": "Invalid or expired token",
                        "message": "Authentication failed. Please login again.",
                        "request_id": request_id
                    }
                )
        else:
            logger.info(
                f"?? [markdown-only] No JWT token - processing without user_id "
                f"| request_id={request_id}"
            )

        # ============================================================
        # READ FILE WITH 50 MB LIMIT
        # ============================================================
        max_size   = 50 * 1024 * 1024
        contents   = bytearray()
        chunk_size = 1024 * 1024

        try:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                contents.extend(chunk)
                if len(contents) > max_size:
                    return JSONResponse(
                        status_code=413,
                        content={
                            "success": False,
                            "error": "File size exceeds maximum allowed size of 50 MB",
                            "request_id": request_id
                        }
                    )
        except Exception as read_error:
            logger.error(
                f"? [markdown-only] Failed to read file: {read_error} "
                f"| request_id={request_id}"
            )
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": f"Failed to read uploaded file: {str(read_error)}",
                    "request_id": request_id
                }
            )

        file_bytes = bytes(contents)
        logger.info(
            f"?? [markdown-only] File read: {len(file_bytes)} bytes "
            f"| request_id={request_id}"
        )

        # ============================================================
        # GENERATE TIMESTAMPED FILENAME AND STORE FILE
        # ============================================================
        timestamped_filename = generate_timestamped_filename(file.filename)
        logger.info(
            f"? [markdown-only] Timestamped filename: "
            f"{file.filename} -> {timestamped_filename} | request_id={request_id}"
        )

        file_path = storage_manager.store_file(file_bytes, timestamped_filename)
        logger.info(
            f"?? [markdown-only] File stored at: {file_path} | request_id={request_id}"
        )

        # ============================================================
        # RUN OCR PROCESSING (reusing _run_ocr_processing from app)
        # ============================================================
        logger.info(
            f"?? [markdown-only] Starting OCR with {model_label} "
            f"| request_id={request_id}"
        )
        ocr_start_time = time.time()

        success, markdown_content, page_count, ocr_error = await _run_ocr_processing(
            model_label=model_label,
            processor=active_processor,
            file_bytes=file_bytes,
            timestamped_filename=timestamped_filename,
            file_extension=file_extension,
            page_range=page_range
        )

        ocr_elapsed = time.time() - ocr_start_time
        logger.info(
            f"?? [markdown-only] OCR completed in {ocr_elapsed:.2f}s "
            f"| success={success} | request_id={request_id}"
        )

        # ============================================================
        # IF OCR FAILED - return 500
        # ============================================================
        if not success or not markdown_content:
            error_msg = ocr_error or "OCR processing failed - no markdown generated"
            logger.error(
                f"? [markdown-only] OCR failed: {error_msg} "
                f"| request_id={request_id}"
            )
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": error_msg,
                    "request_id": request_id,
                    "file_path": file_path
                }
            )

        # ============================================================
        # SAVE TO DATABASE (Gemini fields empty/zero for markdown-only)
        # ============================================================
        try:
            db_storage.store_ocr_result(
                file_name=timestamped_filename,
                markdown_output=markdown_content,
                json_output={},
                page_count=page_count,
                processing_duration=round(time.time() - total_start_time, 2),
                token_usage=0,
                unique_id=None,
                error_details=None,
                request_id=request_id,
                user_id=user_id,
                file_path=file_path,
                olmocr_markdown=markdown_content,
                olmocr_used="Yes",
                missed_keys=[]
            )
            logger.info(
                f"?? [markdown-only] DB record stored | request_id={request_id}"
            )
        except Exception as db_error:
            logger.error(
                f"?? [markdown-only] DB storage failed (non-fatal): {db_error} "
                f"| request_id={request_id}"
            )

        # ============================================================
        # RETURN MARKDOWN-ONLY RESPONSE
        # ============================================================
        total_processing_time = time.time() - total_start_time
        logger.info(
            f"? [markdown-only] Done in {total_processing_time:.2f}s "
            f"| pages={page_count} | request_id={request_id}"
        )

        return {
            "success": True,
            "request_id": request_id,
            "markdown": markdown_content,
            "metadata": {
                "filename":                timestamped_filename,
                "model":                   model_lower,
                "backend": (
                    "Datalab Marker API"
                    if model_lower == "chandra"
                    else "DeepInfra"
                ),
                "page_count":              page_count,
                "processing_time_seconds": round(total_processing_time, 2),
                "file_path":               file_path
            }
        }

    except Exception as e:
        logger.error(
            f"? [markdown-only] Unexpected error: {e} "
            f"| request_id={request_id}",
            exc_info=True
        )
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": f"Unexpected error: {str(e)}",
                "request_id": request_id if request_id else None,
                "file_path": file_path if file_path else None
            }
        )


@router.post("/extract/markdown")
async def extract_markdown(
    request: MarkdownExtractionRequest,
    authorization: Optional[str] = Header(None)
):
    # Import shared components from app module
    from src.services.ocr_pipeline.ocr_server_app import config, gemini_generator
    from src.services.ocr_pipeline.ocr_server_storage import verify_jwt_token

    user_id = None
    if authorization:
        token_data = verify_jwt_token(authorization, config.jwt_secret)
        if not token_data:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        user_id = token_data.get("user_id")
    else:
        raise HTTPException(status_code=401, detail="Authorization token required")

    if not gemini_generator.enabled:
        return JSONResponse(
            status_code=503,
            content={
                "success": False,
                "error": "Gemini API is not available",
                "message": "JSON extraction requires Gemini API to be configured"
            }
        )

    if not request.markdown_content or not request.markdown_content.strip():
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "error": "markdown_content is required and cannot be empty"
            }
        )

    if not request.fields or len(request.fields) == 0:
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "error": "At least one field definition is required"
            }
        )

    try:
        result = await extract_from_markdown_endpoint(request, gemini_generator, user_id)
        return result

    except HTTPException as he:
        return JSONResponse(
            status_code=he.status_code,
            content={
                "success": False,
                "error": he.detail
            }
        )

    except Exception as e:
        logger.error(
            f"Unexpected error in extract_markdown endpoint: {e}", exc_info=True
        )
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": f"Unexpected error: {str(e)}"
            }
        )