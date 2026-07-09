# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
/ocr/vendor Endpoint for OCR Server.

Handles PDF and image processing with model-based routing (Mistral/Qwen).
Includes queue management, validation, and JSON generation.
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

@router.post("/vendor")
async def ocr_vendor(
    file: UploadFile = File(...),
    document_type: Optional[str] = Form(None),
    model: str = Form(...),
    schema_json_str: Optional[str] = Form(None, alias="schema_json"),
    output_format: Optional[str] = Form(None),
    page_range: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    ?? MODEL-BASED PDF/Image Processing with Strict Routing + Dynamic Gemini Selection

    ?? MISTRAL MODE: MISTRAL ? Validate ? Conditional Split ? Smart Gemini ? Optional Merge
    ?? QWEN MODE:    Qwen3-VL ? Validate ? Conditional Split ? Smart Gemini ? Optional Merge

    Mandatory Parameters:
    - file: PDF/image file to process
    - model: Processing model ("mistral" or "qwen")

    Optional Parameters:
    - document_type: Type of document (e.g., "invoice", "receipt", "contract")
    - page_range: Page range to process (e.g., "1-5", "1,3,5")
    - schema_json: JSON string with custom schema (if not found in database)
    - output_format: Response format ("markdown", "json", or None for full response)
    - authorization: JWT Bearer token for user identification (extracts user_id)
    - X-API-Key: API key authentication (validates only, no user_id)

    Model Behavior:

    MISTRAL MODE (model="mistral"):
    1. Run Mistral OCR on uploaded PDF (using API)
    2. Validate markdown against conditional_keys (if configured)
       - If ANY key missing ? HTTP 400 error with missing_keys list
    3. Chunking Decision:
       - If langchain_keys exist ? LangChain split + manual split if chunk > threshold
       - If NO langchain_keys ? Send full markdown to Gemini
    4. Gemini Model Selection (dynamic):
       - Content >= 20,000 chars ? Gemini 2.5 Flash
       - Content < 20,000 chars ? Gemini 2.0 Flash
    5. Post-Processing: If chunked ? Run post-processor merge
    6. Return JSON output

    QWEN MODE (model="qwen"):
    1. Run Qwen3-VL on uploaded PDF (converts to images first)
    2-6. Same pipeline as MISTRAL MODE above

    Authentication:
    - JWT token: Validates and extracts user_id
    - API key: Validates only (true/false), no user_id
    - Both optional
    - User_id comes ONLY from JWT token

    Response Formats:
    - output_format="markdown": Returns only markdown output with minimal metadata
    - output_format="json": Returns only JSON output with minimal metadata
    - output_format=None: Returns full response with all metadata (default)
    """
    schema_json = schema_json_str

    # Import shared components from app module
    # Using delayed import to avoid circular imports
    from src.services.sundarams.sundarams_ocr_server_app import (
        ocr_processing_lock,
        ocr_queue,
        queue_lock,
        storage_manager,
        gemini_generator,
        mistral_processor,
        qwen_processor,
        chandra_processor,
        generate_unique_request_id,
        _run_ocr_processing,
        config,
    )
    from src.services.sundarams.sundarams_json_postprocessor_netsuite import (
        post_process_invoice_to_netsuite,
    )
    from src.services.sundarams.sundarams_ocr_server_storage import (
        generate_timestamped_filename,
        get_document_config_or_fallback,
        verify_jwt_token,
        should_validate_markdown,
        should_use_langchain_chunking,
    )
    from src.services.sundarams.sundarams_ocr_server_email import send_ocr_failure_email
    from src.api.routes.api_server_apikey_generate import validate_api_key_from_header
    from src.core.database.db_storage_util import DatabaseStorage

    # Import db_storage from config
    from src.services.sundarams.sundarams_ocr_server_config import db_storage

    temp_pdf_path = None
    request_id = None
    timestamped_filename = None
    file_path = None

    # OCR tracking variables
    missed_keys_list = []
    final_markdown = None
    manual_split_applied = False
    chunks_manually_split = 0
    ocr_success = True
    error_message = None
    invoice_number = None
    netsuite_bill = None
    netsuite_conversion_error = None

    try:
        # STEP 0: Generate request ID and add to queue
        request_id = generate_unique_request_id()

        with queue_lock:
            ocr_queue.append(request_id)
            queue_position = len(ocr_queue)

        logger.info(
            f"?? Request {request_id} added to queue (Position: {queue_position})"
        )

        # Wait until this request is at the front of queue
        while True:
            with queue_lock:
                if ocr_queue and ocr_queue[0] == request_id:
                    break
            await asyncio.sleep(0.5)

        # Acquire processing lock
        with ocr_processing_lock:
            logger.info(f"?? Processing started for request {request_id}")

            # Validate file upload
            if not file.filename:
                return JSONResponse(
                    status_code=400,
                    content={"success": False, "error": "No filename provided"},
                )

            file_extension = Path(file.filename).suffix.lower()
            supported_formats = {".pdf", ".jpg", ".jpeg", ".png", ".webp", ".bmp"}

            if file_extension not in supported_formats:
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": f"Unsupported file format. Supported: PDF, JPG, JPEG, PNG, WEBP, BMP",
                    },
                )

            # Validate document_type
            if not document_type or not document_type.strip():
                return JSONResponse(
                    status_code=400,
                    content={"success": False, "error": "document_type is required"},
                )

            # Generate timestamped filename
            timestamped_filename = generate_timestamped_filename(file.filename)
            logger.info(
                f"? Generated timestamped filename: {file.filename} ? {timestamped_filename}"
            )

            # ============================================================
            # AUTHENTICATION - JWT/API key validation and user_id extraction
            # ============================================================
            user_id = None

            if x_api_key:
                api_key_data = validate_api_key_from_header(x_api_key)

                if not api_key_data:
                    logger.warning("? Invalid API key")
                    return JSONResponse(
                        status_code=401,
                        content={
                            "success": False,
                            "error": "Invalid API key",
                            "message": "API key authentication failed",
                        },
                    )
                if isinstance(api_key_data, dict):
                    user_id = api_key_data.get("user_id")
                logger.info(f"? Valid API key, user_id: {user_id}")

            if authorization:
                logger.info(
                    "?? JWT token provided, validating and extracting user_id..."
                )
                token_data = verify_jwt_token(authorization, config.jwt_secret)
                if token_data:
                    user_id = token_data.get("user_id")
                    logger.info(f"? Authenticated user_id: {user_id}")
                else:
                    logger.error("? Invalid or expired JWT token")
                    return JSONResponse(
                        status_code=401,
                        content={
                            "success": False,
                            "error": "Invalid or expired token",
                            "message": "Authentication failed. Please login again.",
                        },
                    )
            elif not x_api_key:
                logger.info("?? No JWT token and no API key - processing without user_id")

            # ============================================================
            # MODEL-BASED ROUTING
            # ============================================================

            model_lower = model.lower()

            if model_lower == "mistral":
                active_processor = mistral_processor
                model_label = "MISTRAL"
                ocr_engine_label = "MISTRAL (Mistral AI)"
                logger.info(
                    "?? MISTRAL mode activated - proceeding with MISTRAL processing"
                )
                logger.info(
                    "? Unstructured chunking: DISABLED (LangChain or full markdown only)"
                )

            elif model_lower == "qwen":
                active_processor = qwen_processor
                model_label = "Qwen VL"
                ocr_engine_label = "Qwen3-VL (DeepInfra)"
                logger.info(
                    "?? Qwen VL mode activated - proceeding with Qwen3-VL processing"
                )
                logger.info(
                    "? Unstructured chunking: DISABLED (LangChain or full markdown only)"
                )

            elif model_lower == "chandra":
                # Guard: check whether Chandra is configured
                from src.services.sundarams.sundarams_ocr_server_config import CHANDRA_AVAILABLE
                if not CHANDRA_AVAILABLE:
                    logger.error("? Chandra requested but Datalab API key not configured")
                    return JSONResponse(
                        status_code=503,
                        content={
                            "success": False,
                            "error": "Chandra model is not available - Datalab API key not configured",
                            "message": (
                                "Add 'chandra_datalab.api_key' to sundarams_config.yaml "
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

            else:
                logger.error(f"? Invalid model: {model}")
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": f"Invalid model: {model}. Supported models: mistral, qwen, chandra",
                    },
                )

            # ============================================================
            # STEP 1: FETCH DOCUMENT TYPE CONFIGURATION FROM DATABASE
            # ============================================================

            import json as json_module

            dynamic_schema = None
            if schema_json:
                try:
                    dynamic_schema = json_module.loads(schema_json)
                    logger.info(
                        f"?? Received dynamic schema_json with {len(dynamic_schema)} fields"
                    )
                except json_module.JSONDecodeError as e:
                    logger.error(f"? Invalid schema_json format: {e}")
                    return JSONResponse(
                        status_code=400,
                        content={
                            "success": False,
                            "error": f"Invalid schema_json format: {str(e)}",
                        },
                    )

            doc_config = get_document_config_or_fallback(
                document_type=document_type,
                user_id=user_id,
                pg_config=config.pg_config,
                fallback_schema=dynamic_schema,
            )

            conditional_keys = doc_config.get("conditional_keys", [])
            langchain_keys = doc_config.get("langchain_keys", [])
            resolved_schema = doc_config.get("schema_json")
            config_status = doc_config.get("status", "unknown")

            logger.info(f"?? Document Type: {document_type}")
            logger.info(f"? Config Status: {config_status}")
            if conditional_keys:
                logger.info(
                    f"   Keys: {', '.join(conditional_keys[:5])}{'...' if len(conditional_keys) > 5 else ''}"
                )
            logger.info(f"?? LangChain Keys: {len(langchain_keys)} keys")
            if langchain_keys:
                logger.info(f"   Keys: {', '.join(langchain_keys)}")
                logger.info(
                    f"   Manual splitting: WILL BE APPLIED if chunks > {config.manual_split_threshold} chars"
                )
            else:
                logger.info(f"   No LangChain keys - will send full markdown to Gemini")
            logger.info(
                f"?? Schema: {'? Available' if resolved_schema else '?? None (dynamic extraction)'}"
            )

            # Read uploaded file with size limit (50 MB)
            max_size = 50 * 1024 * 1024
            contents = bytearray()
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
                                "request_id": request_id,
                            },
                        )
            except Exception as e:
                logger.error(f"Error reading uploaded file: {e}", exc_info=True)
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": f"Failed to read uploaded file: {str(e)}",
                        "request_id": request_id,
                    },
                )

            file_bytes = bytes(contents)
            file_size_mb = len(file_bytes) / 1024 / 1024
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

            success, markdown_content, page_count, error = await _run_ocr_processing(
                model_label=model_label,
                processor=active_processor,
                file_bytes=file_bytes,
                timestamped_filename=timestamped_filename,
                file_extension=file_extension,
                page_range=page_range,
            )

            if not success:
                logger.error(f"? Processing failed: {error}")
                ocr_success = False
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
                            model=model_lower,
                            missed_keys=[],
                        )
                        logger.info("?? Error details stored in database")
                    except Exception as db_error:
                        logger.error(f"Failed to store error in database: {db_error}")

                if user_id:
                    try:
                        logger.info(
                            f"?? SENDING EMAIL NOTIFICATION FOR {model_label} OCR FAILURE"
                        )

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
                            missed_keys=None,
                        )

                        if email_sent:
                            logger.info(
                                "? Failure notification email sent successfully"
                            )
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
                        "email_sent": email_sent if "email_sent" in locals() else False,
                    },
                )

            logger.info(f"? {model_label} processing complete")
            logger.info(f"?? Processed {page_count} pages with {model_label} API calls")

            final_markdown = markdown_content

            # ============================================================
            # STEP 3: VALIDATE MARKDOWN (IF CONDITIONAL KEYS EXIST)
            # ============================================================
            if should_validate_markdown(doc_config):

                from src.services.sundarams.sundarams_ocr_server_validator import (
                    MarkdownValidator,
                )

                validator = MarkdownValidator(conditional_keys)
                is_valid, missing_keywords = validator.validate_markdown(
                    markdown_content
                )

                if not is_valid:
                    logger.error(
                        f"? {model_label} MODE: Validation FAILED - Missing keywords: {missing_keywords}"
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
                                error_details=f"Validation failed - Missing keys: {', '.join(missing_keywords)}",
                                request_id=request_id,
                                user_id=user_id,
                                file_path=file_path,
                                model=model_lower,
                                missed_keys=missed_keys_list,
                            )
                        except Exception as db_error:
                            logger.error(
                                f"Failed to store validation error in database: {db_error}"
                            )

                    if user_id:
                        try:
                            logger.info(
                                "?? SENDING EMAIL NOTIFICATION FOR VALIDATION FAILURE"
                            )

                            email_sent = send_ocr_failure_email(
                                config=config,
                                filename=timestamped_filename,
                                user_id=user_id,
                                document_type=document_type,
                                processing_time=time.time() - total_start_time,
                                page_count=page_count,
                                request_id=request_id,
                                error_details=f"Conditional key validation failed",
                                ocr_engine=model_label,
                                missed_keys=missed_keys_list,
                            )

                            if email_sent:
                                logger.info(
                                    "? Validation failure notification email sent"
                                )
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
                            "email_sent": (
                                email_sent if "email_sent" in locals() else False
                            ),
                            "mode": f"{model_lower}_strict_validation",
                        },
                    )
                else:
                    logger.info("? Markdown validation PASSED. All keywords found.")
                    final_markdown = markdown_content
            else:
                logger.info(
                    "?? STEP 3: SKIPPED - No conditional keys configured for validation"
                )
                final_markdown = markdown_content

            # ============================================================
            # STEP 4: CHUNKING DECISION & GEMINI MODEL SELECTION
            # ============================================================
            logger.info(f"?? Markdown length: {len(markdown_content)} characters")
            logger.info(f"? Using config status: {config_status}")

            has_langchain_keys = doc_config.get("has_langchain_keys", False)
            if has_langchain_keys and len(doc_config.get("langchain_keys", [])) > 0:
                logger.info("? LangChain keys exist ? Will chunk markdown")
                will_chunk = True
            else:
                logger.info("? No LangChain keys ? Will send full markdown to Gemini")
                will_chunk = False

            content_length = len(markdown_content)
            selected_gemini_model = "gemini-2.5-flash"
            logger.info(
                f"? Using Gemini 2.5 Flash (65k output) for JSON conversion (Markdown length: {content_length} chars)"
            )

            if will_chunk:
                logger.info(f"Chunking enabled. Manual splitting: {'ENABLED' if config.manual_split_enabled else 'DISABLED'}")
            else:
                logger.info("Chunking not applicable.")

            (
                json_output,
                gemini_prompt_tokens,
                gemini_response_tokens,
                gemini_total_tokens,
            ) = await gemini_generator.generate_json_from_markdown_async(
                markdown_content=markdown_content,
                document_type=document_type,
                doc_config=doc_config,
                original_file_bytes=file_bytes,
                original_filename=timestamped_filename,
                gemini_model=selected_gemini_model,
                disable_unstructured_chunking=True,
            )

            if isinstance(json_output, dict):
                manual_split_applied = json_output.get("_manual_split_applied", False)
                chunks_manually_split = json_output.get("_chunks_manually_split", 0)
                json_output.pop("_manual_split_applied", None)
                json_output.pop("_chunks_manually_split", None)

            logger.info(f"Gemini Token Usage: prompt={gemini_prompt_tokens}, response={gemini_response_tokens}, total={gemini_total_tokens}")

            if manual_split_applied:
                logger.info(f"Manual splitting applied: {chunks_manually_split} chunks.")

            # ============================================================
            # STEP 4.5: NETSUITE VENDOR BILL CONVERSION
            # ============================================================

            netsuite_bill = None
            netsuite_conversion_error = None

            try:
                # ---------------------------------------------------------
                # Build extracted_data for the NetSuite post-processor.
                #
                # CRITICAL RULES:
                #   1. vendor_name MUST come from document_type (the form
                #      parameter), NOT from json_output.  The system
                #      guarantees document_type == vendor_name.  Parsing
                #      vendor from memo or OCR output is explicitly forbidden.
                #
                #   2. Expense lines live under the "expense" key in the
                #      Gemini JSON output.  We fall back to "line_items" for
                #      safety, but the primary key is "expense".
                #
                #   3. All other header fields are passed through from
                #      json_output so the post-processor can populate memo,
                #      subsidiary, sm_location, round-off fields, etc.
                # ---------------------------------------------------------
                extracted_data = {
                    # --- Vendor identification (MUST be document_type) ---
                    "vendor_name":                    document_type,

                    # --- Transaction identity ---
                    "custbody_sm_ori_docu_no":        json_output.get("custbody_sm_ori_docu_no"),

                    # --- Memo: pass-through from Gemini extraction ---
                    "memo":                           json_output.get("memo"),

                    # --- Date fields ---
                    "trandate":                       json_output.get("trandate"),
                    "custbody_entrydate":             json_output.get("custbody_entrydate"),

                    # --- GST state fields (drive IGST vs CGST+SGST logic) ---
                    "custbody_in_gst_pos":            json_output.get("custbody_in_gst_pos"),
                    "shippingaddress":                json_output.get("shippingaddress"),

                    # --- Financial fields ---
                    # NOTE: custbody_cardtype is NOT passed here -- it is always
                    # injected as hardcoded "C1135" via STATIC_DEFAULTS in the
                    # post-processor and must not be overridden by Gemini output.
                    "custbody_actual_bill_amount":    json_output.get("custbody_actual_bill_amount"),
                    "custbody_tds_taxamount":         json_output.get("custbody_tds_taxamount"),
                    "custbody_tds_taxrate":           json_output.get("custbody_tds_taxrate"),

                    # --- LR / GRN reference fields ---
                    "custbody_lr_no":                 json_output.get("custbody_lr_no"),
                    "custbody_lr_date":               json_output.get("custbody_lr_date"),
                    "custbody_grnvrn_no":             json_output.get("custbody_grnvrn_no"),
                    "custbody_grnvrn_date":           json_output.get("custbody_grnvrn_date"),

                    # --- Vehicle / transport fields ---
                    "custbody_vin_no":                json_output.get("custbody_vin_no"),
                    "custbody_registration_no":       json_output.get("custbody_registration_no"),

                    # --- Source document fields ---
                    "createdfrom":                    json_output.get("createdfrom"),
                    "createddate":                    json_output.get("createddate"),

                    # --- Insurer field ---
                    "custbody_insurercd":             json_output.get("custbody_insurercd"),

                    # --- Round-off fields ---
                    "custbody_round_off_val":         json_output.get("custbody_round_off_val"),
                    "custbody_round_off_acc":         json_output.get("custbody_round_off_acc"),
                    "custbody_round_off_sub_gl":      json_output.get("custbody_round_off_sub_gl"),

                    # --- Expense lines ---
                    # Gemini extracts lines under "expense"; fall back to
                    # "line_items" in case schema naming differs.
                    "expense":                        json_output.get(
                                                          "expense",
                                                          json_output.get("line_items", [])
                                                      ),
                }

                logger.info(f"OK Prepared extracted data for post-processor")
                logger.info(f"  - Vendor Name  : {extracted_data.get('vendor_name')}  <- from document_type")
                logger.info(f"  - Document No  : {extracted_data.get('custbody_sm_ori_docu_no')}")
                logger.info(f"  - Entry Date   : {extracted_data.get('custbody_entrydate')}")
                logger.info(f"  - Tran Date    : {extracted_data.get('trandate')}")
                logger.info(f"  - Amount       : {extracted_data.get('custbody_actual_bill_amount')}")
                logger.info(f"  - GST POS      : {extracted_data.get('custbody_in_gst_pos')}")
                logger.info(f"  - Ship Addr    : {extracted_data.get('shippingaddress')}")
                logger.info(f"  - GRN No       : {extracted_data.get('custbody_grnvrn_no')}")
                logger.info(f"  - Insurer      : {extracted_data.get('custbody_insurercd')}")
                logger.info(f"  - Expense Lines: {len(extracted_data.get('expense', []))}")

                # Call post-processor to generate NetSuite JSON
                netsuite_bill = await post_process_invoice_to_netsuite(
                    extracted_data=extracted_data, request_id=request_id
                )

                logger.info("? NetSuite Vendor Bill JSON generated successfully")
                logger.info(f"  - Entity        : {netsuite_bill.get('entity')}")
                logger.info(f"  - Department    : {netsuite_bill.get('department')}")
                logger.info(f"  - Location      : {netsuite_bill.get('location')}")
                logger.info(f"  - Due Date      : {netsuite_bill.get('duedate')}")
                logger.info(
                    f"  - Expense Lines : {len(netsuite_bill.get('expense', []))}"
                )
                logger.info(f"  - Status        : READY for NetSuite API")

            except ValueError as e:
                logger.warning(f"? NetSuite conversion failed: {e}")
                netsuite_conversion_error = str(e)
                netsuite_bill = None
            except Exception as e:
                logger.error(
                    f"? Unexpected error in NetSuite conversion: {e}", exc_info=True
                )
                netsuite_conversion_error = str(e)
                netsuite_bill = None

            # END TOTAL PROCESSING TIMER
            total_processing_time = time.time() - total_start_time

            # ============================================================
            # STEP 5: STORE RESULTS IN DATABASE WITH ENHANCED TRACKING
            # ============================================================
            if db_storage:
                try:
                    logger.info(
                        f"?? STEP 5: Storing results in database with enhanced tracking..."
                    )
                    logger.info(
                        f"   ?? {model_label} Markdown: {len(mistral_markdown) if mistral_markdown else 0} chars"
                    )
                    logger.info(
                        f"   ?? Invoice Number: {invoice_number if invoice_number else 'None'}"
                    )

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
                        model=model_lower,
                        missed_keys=missed_keys_list,
                    )

                    if stored:
                        logger.info(
                            f"   ?? Invoice number: {invoice_number if invoice_number else 'NULL'}"
                        )
                    else:
                        logger.warning("?? Failed to store results in database")

                except Exception as e:
                    logger.error(f"? Database storage error: {e}")
            else:
                logger.warning("?? Database storage not available")

            logger.info(f"Request {request_id} complete: time={total_processing_time:.2f}s, tokens={gemini_total_tokens}, engine={ocr_engine_label}")
            if missed_keys_list:
                logger.info(f"Missed keys: {len(missed_keys_list)}")
            if manual_split_applied:
                logger.info(f"Manual splitting applied: {chunks_manually_split} chunks.")
            if invoice_number:
                logger.info(f"Extracted invoice number: {invoice_number}")

            # ============================================================
            # CONDITIONAL RESPONSE BASED ON output_format PARAMETER
            # ============================================================

            # json_output in all responses below is the final NetSuite bill.
            # If NetSuite conversion succeeded, use netsuite_bill.
            # If it failed, fall back to raw Gemini output so the response
            # is never empty.
            final_json_output = netsuite_bill if netsuite_bill else json_output

            if output_format == "markdown":
                logger.info(
                    "?? Returning MARKDOWN-ONLY response (output_format='markdown')"
                )
                return {
                    "status": "complete",
                    "success": True,
                    "request_id": request_id,
                    "markdown": final_markdown,
                    "metadata": {
                        "filename": timestamped_filename,
                        "content_type": file.content_type or "application/octet-stream",
                    },
                }

            elif output_format == "json":
                logger.info("?? Returning JSON-ONLY response (output_format='json')")
                return {
                    "status": "complete",
                    "success": True,
                    "request_id": request_id,
                    "json": final_json_output,
                    "metadata": {
                        "filename": timestamped_filename,
                        "content_type": file.content_type or "application/octet-stream",
                    },
                }

            else:
                logger.info("?? Returning FULL response (output_format=None/empty)")
                return {
                    "success": True,
                    "request_id": request_id,
                    "markdown": final_markdown,
                    "json_output": final_json_output,
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
                            "langchain_keys_source": "document_types.langchain_keys",
                            "schema_source": "document_schemas.schema_json",
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
                            "enabled": config.manual_split_enabled,
                            "applied": manual_split_applied,
                            "chunks_split": chunks_manually_split,
                            "threshold": config.manual_split_threshold,
                            "max_rows": config.manual_split_max_rows,
                        },
                        "token_usage": {
                            "gemini_prompt_tokens": gemini_prompt_tokens,
                            "gemini_response_tokens": gemini_response_tokens,
                            "gemini_total_tokens": gemini_total_tokens,
                        },
                        "netsuite_conversion": {
                            "status": "success" if netsuite_bill else "failed",
                            "bill_generated": netsuite_bill is not None,
                            "error": (
                                netsuite_conversion_error
                                if netsuite_conversion_error
                                else None
                            ),
                            "expense_lines": (
                                len(netsuite_bill.get("expense", []))
                                if netsuite_bill
                                else 0
                            ),
                        },
                    },
                }

    except Exception as e:
        logger.error(f"Unexpected error in OCR vendor processing: {e}", exc_info=True)

        if user_id and "timestamped_filename" in locals():
            try:
                logger.info("?? Sending email for unexpected error...")
                email_sent = send_ocr_failure_email(
                    config=config,
                    filename=timestamped_filename,
                    user_id=user_id,
                    document_type=(
                        document_type if "document_type" in locals() else "unknown"
                    ),
                    processing_time=(
                        time.time() - total_start_time
                        if "total_start_time" in locals()
                        else 0
                    ),
                    page_count=0,
                    request_id=request_id if request_id else "unknown",
                    error_details=f"Unexpected error: {str(e)}",
                    ocr_engine=model_label if "model_label" in locals() else model,
                    missed_keys=None,
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
                "request_id": request_id if "request_id" in locals() else None,
                "file_path": file_path if "file_path" in locals() else None,
            },
        )

    finally:
        if request_id:
            with queue_lock:
                if ocr_queue and ocr_queue[0] == request_id:
                    ocr_queue.popleft()
