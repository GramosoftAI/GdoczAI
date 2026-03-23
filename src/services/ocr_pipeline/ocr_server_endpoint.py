# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
/ocr/pdf Endpoint for OCR Server.

Handles PDF and image processing with model-based routing:
  - olmocr  : OLMOCR via DeepInfra API
  - qwen    : Qwen3-VL via DeepInfra API
  - chandra : Datalab Marker API              <- NEW

Includes queue management, validation, JSON generation,
webhook delivery, and email failure notifications.
"""

import logging
import time
import asyncio
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, File, UploadFile, Form, Header, HTTPException
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

# Router definition using APIRouter
router = APIRouter(prefix="/ocr", tags=["ocr"])


@router.post("/pdf")
async def ocr_pdf(
    file: UploadFile = File(...),
    document_type: Optional[str] = Form(None),
    model: str = Form(...),
    schema_json: Optional[str] = Form(None),
    output_format: Optional[str] = Form(None),
    page_range: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):

    from src.services.ocr_pipeline.ocr_server_app import (
        ocr_processing_lock, ocr_queue, queue_lock,
        storage_manager, gemini_generator,
        olmocr_processor, qwen_processor, chandra_processor,
        generate_unique_request_id, _run_ocr_processing,
        extract_invoice_number, config
    )
    from src.services.ocr_pipeline.ocr_server_storage import (
        generate_timestamped_filename, get_document_config_or_fallback,
        verify_jwt_token, should_validate_markdown, should_use_langchain_chunking
    )
    from src.services.ocr_pipeline.ocr_server_webhook import WebhookHandler, trigger_webhook_if_needed
    from src.services.ocr_pipeline.ocr_server_email import send_ocr_failure_email
    from src.api.routes.api_server_apikey_generate import validate_api_key_from_header
    from src.core.database.db_storage_util import DatabaseStorage

    # Import db_storage from config
    from src.services.ocr_pipeline.ocr_server_config import db_storage

    temp_pdf_path = None
    request_id = None
    timestamped_filename = None
    file_path = None

    # OCR tracking variables
    olmocr_markdown = None
    olmocr_used = "Yes"
    missed_keys_list = []
    final_markdown = None
    manual_split_applied = False
    chunks_manually_split = 0
    ocr_success = True
    error_message = None
    invoice_number = None

    try:
        # STEP 0: Generate request ID and add to queue
        request_id = generate_unique_request_id()

        with queue_lock:
            ocr_queue.append(request_id)
            queue_position = len(ocr_queue)

        logger.info(f"?? Request {request_id} added to queue (Position: {queue_position})")

        # Wait until this request is at the front of queue
        while True:
            with queue_lock:
                if ocr_queue and ocr_queue[0] == request_id:
                    break
            await asyncio.sleep(0.5)

        # Acquire processing lock
        with ocr_processing_lock:
            logger.info(f"?? Processing started for request {request_id}")

            # ============================================================
            # VALIDATE FILE
            # ============================================================
            if not file.filename:
                return JSONResponse(
                    status_code=400,
                    content={"success": False, "error": "No filename provided"}
                )

            file_extension = Path(file.filename).suffix.lower()
            supported_formats = {'.pdf', '.jpg', '.jpeg', '.png', '.webp', '.bmp'}

            if file_extension not in supported_formats:
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": "Unsupported file format. Supported: PDF, JPG, JPEG, PNG, WEBP, BMP"
                    }
                )

            # Validate document_type
            if not document_type or not document_type.strip():
                return JSONResponse(
                    status_code=400,
                    content={"success": False, "error": "document_type is required"}
                )

            # Generate timestamped filename
            timestamped_filename = generate_timestamped_filename(file.filename)
            logger.info(f"? Generated timestamped filename: {file.filename} -> {timestamped_filename}")

            # ============================================================
            # AUTHENTICATION - JWT for user_id, API key for validation only
            # ============================================================
            user_id = None

            if x_api_key:
                logger.info("?? API Key provided, validating...")
                api_key_data = validate_api_key_from_header(x_api_key)

                if not api_key_data:
                    logger.warning("? Invalid API key")
                    return JSONResponse(
                        status_code=401,
                        content={
                            "success": False,
                            "error": "Invalid API key",
                            "message": "API key authentication failed"
                        }
                    )
                logger.info("? Valid API key")

            if authorization:
                logger.info("?? JWT token provided, validating and extracting user_id...")
                token_data = verify_jwt_token(authorization, config.jwt_secret)
                if token_data:
                    user_id = token_data.get('user_id')
                    logger.info(f"? Authenticated user_id: {user_id}")
                else:
                    logger.error("? Invalid or expired JWT token")
                    return JSONResponse(
                        status_code=401,
                        content={
                            "success": False,
                            "error": "Invalid or expired token",
                            "message": "Authentication failed. Please login again."
                        }
                    )
            else:
                logger.info("?? No JWT token - processing without user_id")

            # ============================================================
            # MODEL-BASED ROUTING
            # ============================================================
            logger.info("=" * 80)
            logger.info(f"?? MODEL-BASED ROUTING: {model}")
            logger.info("=" * 80)

            model_lower = model.lower()

            if model_lower == "olmocr":
                active_processor = olmocr_processor
                model_label = "OLMOCR"
                ocr_engine_label = "OLMOCR (DeepInfra)"
                logger.info("?? OLMOCR mode activated - proceeding with OLMOCR processing")
                logger.info("? Fallback OCR: DISABLED (strict validation mode)")
                logger.info("? Unstructured chunking: DISABLED (LangChain or full markdown only)")

            elif model_lower == "qwen":
                active_processor = qwen_processor
                model_label = "Qwen VL"
                ocr_engine_label = "Qwen3-VL (DeepInfra)"
                logger.info("?? Qwen VL mode activated - proceeding with Qwen3-VL processing")
                logger.info("? Fallback OCR: DISABLED (strict validation mode)")
                logger.info("? Unstructured chunking: DISABLED (LangChain or full markdown only)")

            # ----------------------------------------------------------------
            # CHANDRA model routing                                  <- NEW
            # ----------------------------------------------------------------
            elif model_lower == "chandra":
                # Guard: check whether Chandra is configured
                from src.services.ocr_pipeline.ocr_server_config import CHANDRA_AVAILABLE
                if not CHANDRA_AVAILABLE:
                    logger.error("? Chandra requested but Datalab API key not configured")
                    return JSONResponse(
                        status_code=503,
                        content={
                            "success": False,
                            "error": "Chandra model is not available - Datalab API key not configured",
                            "message": (
                                "Add 'chandra_datalab.api_key' to config.yaml "
                                "or set DATALAB_API_KEY environment variable"
                            ),
                            "request_id": request_id
                        }
                    )

                active_processor = chandra_processor
                model_label = "Chandra"
                ocr_engine_label = "Chandra (Datalab Marker API)"
                logger.info("?? Chandra mode activated - proceeding with Datalab Marker processing")
                logger.info(f"   Output format : {config.chandra_datalab_output_format}")
                logger.info(f"   Mode          : {config.chandra_datalab_mode}")
                logger.info(f"   Timeout       : {config.chandra_datalab_timeout}s")
                logger.info("? Fallback OCR: DISABLED (strict validation mode)")
                logger.info("? Unstructured chunking: DISABLED (LangChain or full markdown only)")

            else:
                logger.error(f"? Invalid model: {model}")
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": (
                            f"Invalid model: {model}. "
                            "Supported models: olmocr, qwen, chandra"
                        )
                    }
                )

            # ============================================================
            # STEP 1: FETCH DOCUMENT TYPE CONFIGURATION FROM DATABASE
            # ============================================================
            logger.info("=" * 80)
            logger.info("?? STEP 1: FETCHING DOCUMENT TYPE CONFIGURATION")
            logger.info("=" * 80)

            import json as json_module
            dynamic_schema = None
            if schema_json:
                try:
                    dynamic_schema = json_module.loads(schema_json)
                    logger.info(f"?? Received dynamic schema_json with {len(dynamic_schema)} fields")
                except json_module.JSONDecodeError as e:
                    logger.error(f"? Invalid schema_json format: {e}")
                    return JSONResponse(
                        status_code=400,
                        content={"success": False, "error": f"Invalid schema_json format: {str(e)}"}
                    )

            doc_config = get_document_config_or_fallback(
                document_type=document_type,
                user_id=user_id,
                pg_config=config.pg_config,
                fallback_schema=dynamic_schema
            )

            conditional_keys = doc_config.get('conditional_keys', [])
            langchain_keys   = doc_config.get('langchain_keys', [])
            resolved_schema  = doc_config.get('schema_json')
            config_status    = doc_config.get('status', 'unknown')

            logger.info("=" * 80)
            logger.info(f"?? CONFIGURATION SUMMARY:")
            logger.info("=" * 80)
            logger.info(f"?? Document Type   : {document_type}")
            logger.info(f"?? User ID         : {user_id if user_id else 'Not authenticated'}")
            logger.info(f"? Config Status   : {config_status}")
            logger.info(f"? Conditional Keys: {len(conditional_keys)} keys")
            if conditional_keys:
                logger.info(f"   Keys: {', '.join(conditional_keys[:5])}{'...' if len(conditional_keys) > 5 else ''}")
            logger.info(f"?? LangChain Keys  : {len(langchain_keys)} keys")
            if langchain_keys:
                logger.info(f"   Keys: {', '.join(langchain_keys)}")
                logger.info(f"   Manual splitting: WILL BE APPLIED if chunks > {config.manual_split_threshold} chars")
            else:
                logger.info(f"   No LangChain keys - will send full markdown to Gemini")
            logger.info(f"?? Schema          : {'? Available' if resolved_schema else '?? None (dynamic extraction)'}")
            logger.info("=" * 80)

            # ============================================================
            # READ UPLOADED FILE WITH 50 MB LIMIT
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
            except Exception as e:
                logger.error(f"Error reading uploaded file: {e}", exc_info=True)
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": f"Failed to read uploaded file: {str(e)}",
                        "request_id": request_id
                    }
                )

            file_bytes    = bytes(contents)
            file_size_mb  = len(file_bytes) / 1024 / 1024
            logger.info(f"?? File size: {file_size_mb:.2f} MB")

            # Store file to local/S3 storage BEFORE processing
            logger.info(f"?? Storing file to {config.storage_type.upper()} storage...")
            file_path = storage_manager.store_file(file_bytes, timestamped_filename)

            if file_path:
                logger.info(f"? File stored successfully: {file_path}")
            else:
                logger.warning("?? File storage failed, continuing without file_path")

            # START TOTAL PROCESSING TIMER
            total_start_time = time.time()

            # ============================================================
            # STEP 2: PROCESS PDF/IMAGE WITH SELECTED OCR ENGINE
            # ============================================================
            logger.info("=" * 80)

            success, markdown_content, page_count, error = await _run_ocr_processing(
                model_label=model_label,
                processor=active_processor,
                file_bytes=file_bytes,
                timestamped_filename=timestamped_filename,
                file_extension=file_extension,
                page_range=page_range
            )

            if not success:
                logger.error(f"? Processing failed: {error}")
                ocr_success  = False
                error_message = error

                if db_storage and user_id:
                    try:
                        db_storage.store_ocr_result(
                            file_name=timestamped_filename,
                            markdown_output="",
                            json_output={},
                            page_count=0,
                            processing_duration=time.time() - total_start_time,
                            token_usage=0,
                            unique_id=None,
                            error_details=error,
                            request_id=request_id,
                            user_id=user_id,
                            file_path=file_path,
                            olmocr_markdown=None,
                            olmocr_used="Yes",
                            missed_keys=[]
                        )
                        logger.info("?? Error details stored in database")
                    except Exception as db_error:
                        logger.error(f"Failed to store error in database: {db_error}")

                if user_id:
                    try:
                        logger.info("=" * 80)
                        logger.info(f"?? SENDING EMAIL NOTIFICATION FOR {model_label} OCR FAILURE")
                        logger.info("=" * 80)

                        email_sent = send_ocr_failure_email(
                            config=config,
                            filename=timestamped_filename,
                            user_id=user_id,
                            document_type=document_type,
                            processing_time=time.time() - total_start_time,
                            page_count=page_count if page_count else 0,
                            request_id=request_id,
                            error_details=error,
                            ocr_engine=model_label,
                            missed_keys=None
                        )

                        if email_sent:
                            logger.info("? Failure notification email sent successfully")
                        else:
                            logger.warning("?? Failed to send email notification")

                    except Exception as email_error:
                        logger.error(f"? Email notification error: {email_error}")

                return JSONResponse(
                    status_code=500,
                    content={
                        "success": False,
                        "error": error,
                        "request_id": request_id,
                        "file_path": file_path,
                        "email_sent": email_sent if 'email_sent' in locals() else False
                    }
                )

            logger.info(f"? {model_label} processing complete")
            logger.info(f"?? Processed {page_count} pages with {model_label}")

            # STEP 2.5: STORE OCR MARKDOWN SEPARATELY
            logger.info("=" * 80)
            logger.info(f"?? STEP 2.5: STORING {model_label} MARKDOWN SEPARATELY")
            logger.info("=" * 80)
            olmocr_markdown = markdown_content
            logger.info(f"? {model_label} markdown stored: {len(olmocr_markdown)} characters")

            # ============================================================
            # STEP 3: VALIDATE MARKDOWN (IF CONDITIONAL KEYS EXIST)
            # ============================================================
            if should_validate_markdown(doc_config):
                logger.info("=" * 80)
                logger.info("?? STEP 3: VALIDATING MARKDOWN WITH CONDITIONAL KEYS")
                logger.info("=" * 80)

                from src.services.ocr_pipeline.ocr_server_validator import MarkdownValidator

                validator = MarkdownValidator(conditional_keys)
                is_valid, missing_keywords = validator.validate_markdown(markdown_content)

                if not is_valid:
                    logger.error(
                        f"? {model_label} MODE: Validation FAILED - "
                        f"Missing keywords: {missing_keywords}"
                    )
                    logger.error("? STRICT MODE: No fallback allowed - returning error")

                    missed_keys_list = missing_keywords

                    if db_storage and user_id:
                        try:
                            db_storage.store_ocr_result(
                                file_name=timestamped_filename,
                                markdown_output=markdown_content,
                                json_output={},
                                page_count=page_count,
                                processing_duration=time.time() - total_start_time,
                                token_usage=0,
                                unique_id=None,
                                error_details=(
                                    f"Validation failed - Missing keys: "
                                    f"{', '.join(missing_keywords)}"
                                ),
                                request_id=request_id,
                                user_id=user_id,
                                file_path=file_path,
                                olmocr_markdown=olmocr_markdown,
                                olmocr_used="Yes",
                                missed_keys=missed_keys_list
                            )
                            logger.info("?? Validation failure stored in database")
                        except Exception as db_error:
                            logger.error(f"Failed to store validation error in database: {db_error}")

                    if user_id:
                        try:
                            logger.info("=" * 80)
                            logger.info("?? SENDING EMAIL NOTIFICATION FOR VALIDATION FAILURE")
                            logger.info("=" * 80)

                            email_sent = send_ocr_failure_email(
                                config=config,
                                filename=timestamped_filename,
                                user_id=user_id,
                                document_type=document_type,
                                processing_time=time.time() - total_start_time,
                                page_count=page_count,
                                request_id=request_id,
                                error_details="Conditional key validation failed",
                                ocr_engine=model_label,
                                missed_keys=missed_keys_list
                            )

                            if email_sent:
                                logger.info("? Validation failure notification email sent")
                            else:
                                logger.warning("?? Failed to send email notification")

                        except Exception as email_error:
                            logger.error(f"? Email notification error: {email_error}")

                    return JSONResponse(
                        status_code=400,
                        content={
                            "success": False,
                            "error": "Conditional key validation failed",
                            "missing_keys": missing_keywords,
                            "message": f"Missing required keys: {', '.join(missing_keywords)}",
                            "request_id": request_id,
                            "file_path": file_path,
                            "email_sent": email_sent if 'email_sent' in locals() else False,
                            "mode": f"{model_lower}_strict_validation"
                        }
                    )
                else:
                    logger.info("? Markdown validation PASSED. All keywords found.")
                    final_markdown = olmocr_markdown
            else:
                logger.info("?? STEP 3: SKIPPED - No conditional keys configured for validation")
                final_markdown = olmocr_markdown

            # ============================================================
            # STEP 4: CHUNKING DECISION & GEMINI MODEL SELECTION
            # ============================================================
            logger.info("=" * 80)
            logger.info(f"?? STEP 4: CHUNKING DECISION & GEMINI MODEL SELECTION")
            logger.info(f"?? Markdown length: {len(markdown_content)} characters")
            logger.info(f"? Using config status: {config_status}")

            has_langchain_keys   = doc_config.get('has_langchain_keys', False)
            will_chunk           = False
            selected_gemini_model = None

            if has_langchain_keys and len(doc_config.get('langchain_keys', [])) > 0:
                logger.info("? LangChain keys exist -> Will chunk markdown")
                will_chunk = True
                selected_gemini_model = None  # Will use default
            else:
                logger.info("? No LangChain keys -> Will send full markdown to Gemini")
                will_chunk = False

                content_length = len(markdown_content)
                if content_length > 25000:
                    selected_gemini_model = "gemini-2.5-flash"
                    logger.info(
                        f"? Content > 25,000 chars ({content_length}) "
                        f"-> Using Gemini 2.5 Flash (65k output)"
                    )
                else:
                    selected_gemini_model = "gemini-2.0-flash"
                    logger.info(
                        f"? Content <= 25,000 chars ({content_length}) "
                        f"-> Using Gemini 2.0 Flash (8k output)"
                    )

            if will_chunk and config.manual_split_enabled:
                logger.info(f"?? Manual splitting: ENABLED for this document type")
                logger.info(f"   ?? Threshold: {config.manual_split_threshold} characters")
                logger.info(f"   ?? Max rows : {config.manual_split_max_rows} per chunk")
            elif will_chunk:
                logger.info(f"?? Manual splitting: DISABLED in config")
            else:
                logger.info(f"?? Manual splitting: NOT APPLICABLE (no chunking)")

            logger.info("=" * 80)

            json_output, gemini_prompt_tokens, gemini_response_tokens, gemini_total_tokens = \
                await gemini_generator.generate_json_from_markdown_async(
                    markdown_content=markdown_content,
                    document_type=document_type,
                    doc_config=doc_config,
                    original_file_bytes=file_bytes,
                    original_filename=timestamped_filename,
                    gemini_model=selected_gemini_model,
                    disable_unstructured_chunking=True
                )

            if isinstance(json_output, dict):
                manual_split_applied  = json_output.get('_manual_split_applied', False)
                chunks_manually_split = json_output.get('_chunks_manually_split', 0)
                json_output.pop('_manual_split_applied', None)
                json_output.pop('_chunks_manually_split', None)

            logger.info("? JSON generation complete")
            logger.info(f"?? Gemini Token Usage:")
            logger.info(f"   ?? Prompt tokens  : {gemini_prompt_tokens}")
            logger.info(f"   ?? Response tokens : {gemini_response_tokens}")
            logger.info(f"   ?? Total tokens    : {gemini_total_tokens}")

            if manual_split_applied:
                logger.info(f"?? Manual splitting was applied:")
                logger.info(f"   ? Chunks manually split: {chunks_manually_split}")

            # ============================================================
            # STEP 4.5: EXTRACT INVOICE NUMBER FROM JSON OUTPUT
            # ============================================================
            logger.info("=" * 80)
            logger.info("?? STEP 4.5: EXTRACTING INVOICE NUMBER FROM JSON OUTPUT")
            logger.info("=" * 80)

            invoice_number = extract_invoice_number(json_output)

            if invoice_number:
                logger.info(f"? Invoice Number extracted: {invoice_number}")
                logger.info(f"?? Will be stored in unique_id column")
            else:
                logger.warning("?? Invoice Number not found in JSON output")
                logger.info("?? unique_id will remain NULL")

            logger.info("=" * 80)

            # END TOTAL PROCESSING TIMER
            total_processing_time = time.time() - total_start_time

            # ============================================================
            # STEP 5: STORE RESULTS IN DATABASE WITH ENHANCED TRACKING
            # ============================================================
            if db_storage:
                try:
                    logger.info(f"?? STEP 5: Storing results in database with enhanced tracking...")
                    logger.info(f"   ?? Request ID    : {request_id}")
                    logger.info(f"   ?? User ID       : {user_id if user_id else 'None'}")
                    logger.info(f"   ?? Gemini Tokens : {gemini_total_tokens}")
                    logger.info(f"   ?? File Path     : {file_path}")
                    logger.info(
                        f"   ?? {model_label} Markdown: "
                        f"{len(olmocr_markdown) if olmocr_markdown else 0} chars"
                    )
                    logger.info(f"   ?? OCR Used      : {olmocr_used}")
                    logger.info(f"   ? Missed Keys  : {len(missed_keys_list)} keys")
                    logger.info(f"   ?? Manual Split  : {manual_split_applied}")
                    logger.info(f"   ?? Invoice Number: {invoice_number if invoice_number else 'None'}")

                    stored = db_storage.store_ocr_result(
                        file_name=timestamped_filename,
                        markdown_output=final_markdown,
                        json_output=json_output,
                        page_count=page_count,
                        processing_duration=total_processing_time,
                        token_usage=gemini_total_tokens,
                        unique_id=invoice_number,
                        error_details=None,
                        request_id=request_id,
                        user_id=user_id,
                        file_path=file_path,
                        olmocr_markdown=olmocr_markdown,
                        olmocr_used=olmocr_used,
                        missed_keys=missed_keys_list
                    )

                    if stored:
                        logger.info(f"? Results stored in database successfully")
                        logger.info(f"?? Enhanced tracking data included:")
                        logger.info(f"   ? Separate markdown storage")
                        logger.info(f"   ? Fallback tracking  : {olmocr_used}")
                        logger.info(f"   ? Missed keys        : {len(missed_keys_list)}")
                        logger.info(f"   ?? Manual split       : {manual_split_applied}")
                        logger.info(
                            f"   ?? Invoice number     : "
                            f"{invoice_number if invoice_number else 'NULL'}"
                        )
                    else:
                        logger.warning("?? Failed to store results in database")

                except Exception as e:
                    logger.error(f"? Database storage error: {e}")
            else:
                logger.warning("?? Database storage not available")

            # ============================================================
            # STEP 5.5: TRIGGER WEBHOOK (IF CONFIGURED)
            # ============================================================
            logger.info("=" * 80)
            logger.info("?? STEP 5.5: CHECKING WEBHOOK CONFIGURATION")
            logger.info("=" * 80)

            webhook_handler = WebhookHandler(config)

            webhook_success = trigger_webhook_if_needed(
                webhook_handler=webhook_handler,
                user_id=user_id,
                json_output=json_output,
                request_id=request_id,
                document_type=document_type
            )

            if webhook_success:
                logger.info("? Webhook delivered successfully")
            else:
                logger.info("?? Webhook not sent (not configured or delivery failed)")

            logger.info("=" * 80)

            # ============================================================
            # FINAL SUMMARY LOG
            # ============================================================
            logger.info("=" * 80)
            logger.info("? PROCESSING COMPLETE")
            logger.info(f"?? Request ID           : {request_id}")
            logger.info(f"?? Total processing time: {total_processing_time:.2f}s")
            logger.info(f"?? Gemini Tokens Used   : {gemini_total_tokens}")
            logger.info(f"?? OCR Engine Used      : {ocr_engine_label}")
            if missed_keys_list:
                logger.info(f"? Missed Keys Count  : {len(missed_keys_list)}")
            if manual_split_applied:
                logger.info(f"?? Manual Splitting    : {chunks_manually_split} chunks split")
            if invoice_number:
                logger.info(f"?? Invoice Number      : {invoice_number}")
            logger.info("=" * 80)

            # ============================================================
            # CONDITIONAL RESPONSE BASED ON output_format PARAMETER
            # ============================================================

            if output_format == "markdown":
                logger.info("?? Returning MARKDOWN-ONLY response (output_format='markdown')")
                return {
                    "status": "complete",
                    "success": True,
                    "request_id": request_id,
                    "markdown": final_markdown,
                    "metadata": {
                        "filename": timestamped_filename,
                        "content_type": file.content_type or "application/octet-stream"
                    }
                }

            elif output_format == "json":
                logger.info("?? Returning JSON-ONLY response (output_format='json')")
                return {
                    "status": "complete",
                    "success": True,
                    "request_id": request_id,
                    "json": json_output,
                    "metadata": {
                        "filename": timestamped_filename,
                        "content_type": file.content_type or "application/octet-stream"
                    }
                }

            else:
                logger.info("?? Returning FULL response (output_format=None/empty)")
                return {
                    "success": True,
                    "request_id": request_id,
                    "markdown": final_markdown,
                    "json_output": json_output,
                    "metadata": {
                        "request_id": request_id,
                        "user_id": user_id,
                        "document_type": document_type,
                        "config_status": config_status,
                        "has_conditional_keys": len(conditional_keys) > 0,
                        "has_langchain_keys": len(langchain_keys) > 0,
                        "has_schema": resolved_schema is not None,
                        "invoice_number": invoice_number,
                        "key_storage_design": {
                            "conditional_keys_source": "document_types.conditional_keys",
                            "langchain_keys_source":   "document_types.langchain_keys",
                            "schema_source":           "document_schemas.schema_json"
                        },
                        "ocr_engine": ocr_engine_label,
                        "model": model_lower,
                        "backend": (
                            "Datalab Marker API"
                            if model_lower == "chandra"
                            else "DeepInfra"
                        ),
                        "processing_mode": "entire_pdf_at_once",
                        "page_count": page_count,
                        "processing_time_seconds": round(total_processing_time, 2),
                        "file_path": file_path,
                        "merge_strategy": "unified_post_processing",
                        "manual_splitting": {
                            "enabled":      config.manual_split_enabled,
                            "applied":      manual_split_applied,
                            "chunks_split": chunks_manually_split,
                            "threshold":    config.manual_split_threshold,
                            "max_rows":     config.manual_split_max_rows
                        },
                        "token_usage": {
                            "gemini_prompt_tokens":   gemini_prompt_tokens,
                            "gemini_response_tokens": gemini_response_tokens,
                            "gemini_total_tokens":    gemini_total_tokens
                        }
                    }
                }

    except Exception as e:
        logger.error(f"Unexpected error in OCR PDF processing: {e}", exc_info=True)

        if user_id and 'timestamped_filename' in locals():
            try:
                logger.info("?? Sending email for unexpected error...")
                email_sent = send_ocr_failure_email(
                    config=config,
                    filename=timestamped_filename,
                    user_id=user_id,
                    document_type=document_type if 'document_type' in locals() else 'unknown',
                    processing_time=time.time() - total_start_time if 'total_start_time' in locals() else 0,
                    page_count=0,
                    request_id=request_id if request_id else 'unknown',
                    error_details=f"Unexpected error: {str(e)}",
                    ocr_engine=model_label if 'model_label' in locals() else model,
                    missed_keys=None
                )
                if email_sent:
                    logger.info("? Error notification email sent")
            except Exception as email_error:
                logger.error(f"? Failed to send error email: {email_error}")

        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": f"Unexpected error: {str(e)}",
                "request_id": request_id if 'request_id' in locals() else None,
                "file_path": file_path if 'file_path' in locals() else None
            }
        )

    finally:
        if request_id:
            with queue_lock:
                if ocr_queue and ocr_queue[0] == request_id:
                    ocr_queue.popleft()
                    logger.info(f"? Request {request_id} removed from queue")