# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
/ocr/segment Endpoint for OCR Server.

Provides document segmentation in two modes:
  - auto   : Gemini auto-detects logical document boundaries across pages
  - guided : User supplies keywords + description to guide segmentation

Processing flow:
  1. Validate file (PDF only) and authentication (JWT + API Key)
  2. Convert PDF -> page-wise Markdown using ChandraProcessor
  3. Parse <---- Page N ----> delimiters to build page index map
  4. Call DocumentSegmentAnalyzer (Gemini 2.5 Flash) with auto or guided prompt
  5. Validate segment coverage (no missing / duplicate pages)
  6. Return structured JSON response

No queue / no processing lock -> full concurrency allowed.
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, Header, UploadFile
from fastapi.responses import JSONResponse

from src.services.ocr_pipeline.ocr_server_segment_analyzer import DocumentSegmentAnalyzer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
router = APIRouter(prefix="/ocr", tags=["ocr-segment"])


# ===========================================================================
# POST /ocr/segment
# ===========================================================================
@router.post("/segment")
async def ocr_segment(
    file: UploadFile = File(...),
    mode: str = Form(...),
    segments: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    Segment a PDF document into logical sections.

    Form Data
    ---------
    file        : PDF file (required, max 50 MB)
    mode        : "auto" or "guided"  (required)
    keywords    : JSON array of strings, e.g. ["invoice","packing list"]
                  Required when mode == "guided"
    description : Plain-text context hint for guided mode (optional)

    Headers
    -------
    Authorization : Bearer <jwt_token>  (optional -- extracts user_id)
    X-API-Key     : <api_key>           (optional -- validates key only)

    Returns
    -------
    {
      "success": true,
      "segments": [...],
      "metadata": {...},
      "request_id": "...",
      "processing_time": 12.34
    }
    """

    # ------------------------------------------------------------------
    # Lazy imports -- identical pattern to ocr_server_helper_endpoint.py
    # ------------------------------------------------------------------
    from src.services.ocr_pipeline.ocr_server_app import (
        chandra_processor,
        generate_unique_request_id,
        config,
    )
    from src.services.ocr_pipeline.ocr_server_storage import (
        generate_timestamped_filename,
        verify_jwt_token,
    )
    from src.services.ocr_pipeline.ocr_server_config import (
        CHANDRA_AVAILABLE,
        GEMINI_AVAILABLE,
    )
    from src.api.routes.api_server_apikey_generate import validate_api_key_from_header

    request_id = None
    total_start_time = time.time()

    try:
        # ----------------------------------------------------------------
        # STEP 0: Generate request ID (no queue -- full concurrency)
        # ----------------------------------------------------------------
        request_id = generate_unique_request_id()

        logger.info("=" * 80)
        logger.info(
            "[SEGMENT] /ocr/segment request received | request_id=%s", request_id
        )
        logger.info("=" * 80)

        # ----------------------------------------------------------------
        # STEP 1a: Validate file presence and extension
        # ----------------------------------------------------------------
        if not file or not file.filename:
            logger.warning(
                "[SEGMENT] No file provided | request_id=%s", request_id
            )
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "No file provided",
                    "request_id": request_id,
                },
            )

        file_extension = Path(file.filename).suffix.lower()

        if file_extension != ".pdf":
            logger.warning(
                "[SEGMENT] Non-PDF file rejected: %s | request_id=%s",
                file.filename,
                request_id,
            )
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": (
                        "Only PDF files are supported for segmentation. "
                        f"Received: {file_extension}"
                    ),
                    "request_id": request_id,
                },
            )

        # ----------------------------------------------------------------
        # STEP 1b: Validate mode
        # ----------------------------------------------------------------
        mode_lower = mode.strip().lower() if mode else ""

        if mode_lower not in ("auto", "guided"):
            logger.warning(
                "[SEGMENT] Invalid mode '%s' | request_id=%s", mode, request_id
            )
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": (
                        f"Invalid mode '{mode}'. Supported modes: 'auto', 'guided'"
                    ),
                    "request_id": request_id,
                },
            )

        # ----------------------------------------------------------------
        # STEP 1c: Validate guided-mode inputs
        # ----------------------------------------------------------------
        parsed_segments: Optional[list] = None

        if mode_lower == "guided":
            if not segments or not segments.strip():
                logger.warning(
                    "[SEGMENT] guided mode missing segments | request_id=%s",
                    request_id,
                )
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": (
                            "segments is required for guided mode. "
                            'Provide a JSON array, e.g. [{"name": "Invoice", "description": "..."}]'
                        ),
                        "request_id": request_id,
                    },
                )

            try:
                parsed_segments = json.loads(segments)
                if not isinstance(parsed_segments, list) or len(parsed_segments) == 0:
                    raise ValueError("segments must be a non-empty JSON array")

                for i, seg in enumerate(parsed_segments):
                    if not isinstance(seg, dict):
                        raise ValueError(f"segments[{i}] must be an object")
                    seg_name = str(seg.get("name") or "").strip()
                    seg_desc = str(seg.get("description") or "").strip()
                    if not seg_name:
                        raise ValueError(f"segments[{i}] is missing a non-empty 'name'")
                    if not seg_desc:
                        raise ValueError(f"segments[{i}] is missing a non-empty 'description'")

            except (json.JSONDecodeError, ValueError) as seg_err:
                logger.warning(
                    "[SEGMENT] Invalid segments input: %s | request_id=%s",
                    seg_err,
                    request_id,
                )
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": f"Invalid segments format: {str(seg_err)}",
                        "hint": '[{"name": "Invoice", "description": "Contains invoice details"}, ...]',
                        "request_id": request_id,
                    },
                )

        # ----------------------------------------------------------------
        # STEP 1d: Validate Gemini availability
        # ----------------------------------------------------------------
        if not GEMINI_AVAILABLE:
            logger.error(
                "[SEGMENT] Gemini not available | request_id=%s", request_id
            )
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "error": "Gemini API is not available -- segmentation requires Gemini",
                    "message": "Install google-generativeai and configure a Gemini API key",
                    "request_id": request_id,
                },
            )

        # ----------------------------------------------------------------
        # STEP 1e: Validate Chandra availability
        # ----------------------------------------------------------------
        if not CHANDRA_AVAILABLE:
            logger.error(
                "[SEGMENT] Chandra not available | request_id=%s", request_id
            )
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "error": (
                        "Chandra (Datalab Marker API) is not configured -- "
                        "segmentation uses Chandra for PDF-to-Markdown conversion"
                    ),
                    "message": (
                        "Add 'chandra_datalab.api_key' to config.yaml "
                        "or set DATALAB_API_KEY environment variable"
                    ),
                    "request_id": request_id,
                },
            )

        # ----------------------------------------------------------------
        # STEP 2: Authentication -- JWT (user_id) + API Key
        # ----------------------------------------------------------------
        user_id: Optional[str] = None

        if x_api_key:
            logger.info(
                "[SEGMENT] Validating API key | request_id=%s", request_id
            )
            api_key_data = validate_api_key_from_header(x_api_key)
            if not api_key_data:
                logger.warning(
                    "[SEGMENT] Invalid API key | request_id=%s", request_id
                )
                return JSONResponse(
                    status_code=401,
                    content={
                        "success": False,
                        "error": "Invalid API key",
                        "message": "API key authentication failed",
                        "request_id": request_id,
                    },
                )
            if isinstance(api_key_data, dict):
                user_id = api_key_data.get("user_id")
            logger.info(
                "[SEGMENT] API key valid (user_id=%s) | request_id=%s", user_id, request_id
            )

        if authorization:
            logger.info(
                "[SEGMENT] Validating JWT token | request_id=%s", request_id
            )
            token_data = verify_jwt_token(authorization, config.jwt_secret)
            if not token_data:
                logger.error(
                    "[SEGMENT] Invalid/expired JWT | request_id=%s", request_id
                )
                return JSONResponse(
                    status_code=401,
                    content={
                        "success": False,
                        "error": "Invalid or expired token",
                        "message": "Authentication failed. Please login again.",
                        "request_id": request_id,
                    },
                )
            user_id = token_data.get("user_id")
            logger.info(
                "[SEGMENT] Authenticated user_id=%s | request_id=%s",
                user_id,
                request_id,
            )
        else:
            logger.info(
                "[SEGMENT] No JWT -- processing without user_id | request_id=%s",
                request_id,
            )

        # ----------------------------------------------------------------
        # STEP 3: Read file (50 MB limit)
        # ----------------------------------------------------------------
        logger.info(
            "[SEGMENT] Reading file: %s | request_id=%s",
            file.filename,
            request_id,
        )

        max_size = 50 * 1024 * 1024  # 50 MB
        contents = bytearray()
        chunk_size = 1024 * 1024  # 1 MB read chunks

        try:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                contents.extend(chunk)
                if len(contents) > max_size:
                    logger.warning(
                        "[SEGMENT] File exceeds 50 MB | request_id=%s", request_id
                    )
                    return JSONResponse(
                        status_code=413,
                        content={
                            "success": False,
                            "error": "File size exceeds maximum allowed size of 50 MB",
                            "request_id": request_id,
                        },
                    )
        except Exception as read_err:
            logger.error(
                "[SEGMENT] Failed to read file: %s | request_id=%s",
                read_err,
                request_id,
                exc_info=True,
            )
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": f"Failed to read uploaded file: {str(read_err)}",
                    "request_id": request_id,
                },
            )

        file_bytes = bytes(contents)
        file_size_mb = len(file_bytes) / 1024 / 1024
        logger.info(
            "[SEGMENT] File read: %.2f MB | request_id=%s", file_size_mb, request_id
        )

        # ----------------------------------------------------------------
        # STEP 4: Generate timestamped filename
        # ----------------------------------------------------------------
        timestamped_filename = generate_timestamped_filename(file.filename)
        logger.info(
            "[SEGMENT] Timestamped filename: %s -> %s | request_id=%s",
            file.filename,
            timestamped_filename,
            request_id,
        )

        # ----------------------------------------------------------------
        # STEP 5: PDF -> page-wise Markdown via ChandraProcessor
        # ----------------------------------------------------------------
        logger.info("=" * 80)
        logger.info(
            "[SEGMENT] STEP 5: Converting PDF to Markdown via ChandraProcessor | request_id=%s",
            request_id,
        )
        logger.info("=" * 80)

        ocr_start = time.time()

        import asyncio

        success, markdown_content, page_count, ocr_error = (
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: chandra_processor.process_pdf(
                    file_bytes, timestamped_filename, page_range=None
                ),
            )
        )

        ocr_elapsed = time.time() - ocr_start
        logger.info(
            "[SEGMENT] ChandraProcessor done in %.2fs | success=%s | pages=%s | request_id=%s",
            ocr_elapsed,
            success,
            page_count,
            request_id,
        )

        if not success or not markdown_content or not markdown_content.strip():
            error_msg = ocr_error or "ChandraProcessor returned empty content"
            logger.error(
                "[SEGMENT] OCR failed: %s | request_id=%s", error_msg, request_id
            )
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": f"PDF to Markdown conversion failed: {error_msg}",
                    "request_id": request_id,
                },
            )

        logger.info(
            "[SEGMENT] Markdown produced: %d chars, %d pages | request_id=%s",
            len(markdown_content),
            page_count,
            request_id,
        )

        # ----------------------------------------------------------------
        # STEP 6: Parse page-wise Markdown into {page_number -> content}
        # ----------------------------------------------------------------
        logger.info(
            "[SEGMENT] STEP 6: Parsing page index map | request_id=%s", request_id
        )

        page_map = _parse_page_map(markdown_content)

        if not page_map:
            logger.warning(
                "[SEGMENT] No <---- Page N ----> delimiters found -- "
                "treating entire markdown as page 1 | request_id=%s",
                request_id,
            )
            page_map = {1: markdown_content.strip()}

        actual_page_count = len(page_map)
        logger.info(
            "[SEGMENT] Page index map built: %d pages | request_id=%s",
            actual_page_count,
            request_id,
        )

        for pnum, pcontent in page_map.items():
            logger.info(
                "[SEGMENT]   Page %d: %d chars",
                pnum,
                len(pcontent),
            )

        # ----------------------------------------------------------------
        # STEP 7: Segmentation via DocumentSegmentAnalyzer (Gemini 2.5 Flash)
        # ----------------------------------------------------------------
        logger.info("=" * 80)
        logger.info(
            "[SEGMENT] STEP 7: Running Gemini segmentation | mode=%s | request_id=%s",
            mode_lower,
            request_id,
        )
        logger.info("=" * 80)

        analyzer = DocumentSegmentAnalyzer(config)

        seg_start = time.time()

        segmentation_result = await analyzer.segment_document(
            page_map=page_map,
            mode=mode_lower,
            segments=parsed_segments,
            request_id=request_id,
        )

        seg_elapsed = time.time() - seg_start
        logger.info(
            "[SEGMENT] Gemini segmentation done in %.2fs | request_id=%s",
            seg_elapsed,
            request_id,
        )

        # ----------------------------------------------------------------
        # STEP 8: Validate segment coverage
        # ----------------------------------------------------------------
        logger.info(
            "[SEGMENT] STEP 8: Validating segment page coverage | request_id=%s",
            request_id,
        )

        segments = segmentation_result.get("segments", [])
        coverage_ok, coverage_error = _validate_coverage(segments, actual_page_count)

        if not coverage_ok:
            logger.warning(
                "[SEGMENT] Coverage validation warning: %s | request_id=%s",
                coverage_error,
                request_id,
            )
            # Non-fatal -- return result with a warning flag so caller is aware
            segmentation_result["coverage_warning"] = coverage_error
        else:
            logger.info(
                "[SEGMENT] Coverage validation passed -- all %d pages accounted for | request_id=%s",
                actual_page_count,
                request_id,
            )

        # ----------------------------------------------------------------
        # STEP 9: Build final response
        # ----------------------------------------------------------------
        total_processing_time = round(time.time() - total_start_time, 2)

        # Enrich metadata returned by analyzer
        meta = segmentation_result.get("metadata", {})
        meta.update(
            {
                "total_pages": actual_page_count,
                "ocr_time_seconds": round(ocr_elapsed, 2),
                "segmentation_time_seconds": round(seg_elapsed, 2),
                "filename": timestamped_filename,
                "mode": mode_lower,
                "user_id": user_id,
                "request_id": request_id,
            }
        )

        if mode_lower == "guided":
            meta["segments_used"] = parsed_segments

        logger.info("=" * 80)
        logger.info("[SEGMENT] COMPLETE | request_id=%s", request_id)
        logger.info(
            "[SEGMENT] Segments found  : %d", len(segments)
        )
        logger.info(
            "[SEGMENT] Total pages     : %d", actual_page_count
        )
        logger.info(
            "[SEGMENT] Total time      : %.2fs", total_processing_time
        )
        logger.info("=" * 80)

        response_body = {
            "success": True,
            "segments": segments,
            "metadata": meta,
            "request_id": request_id,
            "processing_time": total_processing_time,
        }

        if "coverage_warning" in segmentation_result:
            response_body["coverage_warning"] = segmentation_result["coverage_warning"]

        return JSONResponse(status_code=200, content=response_body)

    # ----------------------------------------------------------------------
    # Global exception handler -- never let an unhandled error crash the app
    # ----------------------------------------------------------------------
    except Exception as exc:
        logger.error(
            "[SEGMENT] Unexpected error: %s | request_id=%s",
            exc,
            request_id,
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": f"Unexpected error: {str(exc)}",
                "request_id": request_id,
            },
        )


# ===========================================================================
# HELPER: Parse <---- Page N ----> delimited markdown into page map
# ===========================================================================
def _parse_page_map(markdown_content: str) -> dict:
    """
    Parse the page-delimited markdown produced by ChandraProcessor into a
    dict mapping page_number (int, 1-based) -> page_content (str).

    ChandraProcessor emits delimiters in the form:
        <---- Page 1 ---->
        ...content...
        <---- Page 2 ---->
        ...content...

    Parameters
    ----------
    markdown_content : str
        Raw markdown string from ChandraProcessor.process_pdf()

    Returns
    -------
    dict
        {1: "page 1 content", 2: "page 2 content", ...}
        Returns empty dict if no delimiters found.
    """
    import re

    # Pattern matches "<---- Page N ---->" with any amount of whitespace
    delimiter_pattern = re.compile(
        r"<[-]+\s*Page\s+(\d+)\s*[-]+>", re.IGNORECASE
    )

    matches = list(delimiter_pattern.finditer(markdown_content))

    if not matches:
        logger.debug("[SEGMENT] _parse_page_map: no page delimiters found")
        return {}

    page_map: dict = {}

    for idx, match in enumerate(matches):
        page_num = int(match.group(1))

        content_start = match.end()
        content_end = (
            matches[idx + 1].start() if idx + 1 < len(matches) else len(markdown_content)
        )

        page_content = markdown_content[content_start:content_end].strip()
        page_map[page_num] = page_content

    logger.debug(
        "[SEGMENT] _parse_page_map: parsed %d pages", len(page_map)
    )
    return page_map


# ===========================================================================
# HELPER: Validate that every page 1..N appears exactly once across segments
# ===========================================================================
def _validate_coverage(segments: list, total_pages: int) -> tuple:
    """
    Check that all pages from 1 to total_pages appear exactly once across
    all segments and that no page appears more than once.

    Parameters
    ----------
    segments    : list of segment dicts (each has a "pages" key)
    total_pages : int -- total number of pages in the document

    Returns
    -------
    (is_valid: bool, error_message: str | None)
        (True, None) when coverage is perfect.
    """
    all_pages: list = []

    for seg in segments:
        pages = seg.get("pages", [])
        all_pages.extend(pages)

    all_pages_set = set(all_pages)
    expected_pages = set(range(1, total_pages + 1))

    # Check for duplicates
    seen: set = set()
    duplicates: set = set()
    for p in all_pages:
        if p in seen:
            duplicates.add(p)
        seen.add(p)

    if duplicates:
        return (
            False,
            f"Duplicate pages found in segments: {sorted(duplicates)}",
        )

    missing = expected_pages - all_pages_set
    if missing:
        return (
            False,
            f"Pages missing from segments: {sorted(missing)}",
        )

    extra = all_pages_set - expected_pages
    if extra:
        return (
            False,
            f"Segments reference pages outside document range (1-{total_pages}): {sorted(extra)}",
        )

    return True, None