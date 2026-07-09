# -*- coding: utf-8 -*-
"""
royal_tech_app.py � FastAPI application for the Royal Tech Invoice Extraction System.

Endpoint: POST /royal_tech/pdf  (ApiKey header + form fields + PDF upload)
Run:      uvicorn royal_tech_app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import logging
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, File, Form, Header, Request, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.services.royal.royal_tech_config import cfg
from src.services.royal.royal_tech_schema_loader import (
    DocumentTypeNotFoundError,
    SchemaLoaderError,
    load_schema,
)

# ---------------------------------------------------------------------------
# Lazy imports � pipeline modules imported inside worker threads only
# ---------------------------------------------------------------------------
# royal_tech_processor imports are deferred to _get_pipeline() which runs
# in the ThreadPoolExecutor, keeping the event loop free during startup.

# ============================================================================
# Logging setup
# ============================================================================


def _setup_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    lc = cfg.logging
    root.setLevel(getattr(logging, lc.level, logging.INFO))
    fmt = logging.Formatter(fmt=lc.format, datefmt=lc.datefmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)
    if lc.log_file:
        fh = logging.FileHandler(lc.log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)


_setup_logging()
logger = logging.getLogger(__name__)

# ============================================================================
# Thread pool  (pipeline is fully synchronous / blocking)
# ============================================================================

_THREAD_POOL = ThreadPoolExecutor(
    max_workers=cfg.pipeline.pipeline_workers,
    thread_name_prefix="royal_pipeline",
)

# ============================================================================
# FastAPI app
# ============================================================================

app = FastAPI(
    title="Royal Tech Invoice Extraction API",
    description=(
        "Production-grade invoice extraction system powered by "
        "Gemini 2.5 Flash + OLMOCR. "
        "Upload a PDF invoice and receive structured JSON output."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cfg.api.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Pipeline singleton
# ============================================================================

_pipeline = None
_pipeline_error: Optional[str] = None


def _get_pipeline():
    """
    Lazy-initialise the InvoicePipeline singleton.
    Must only be called from within the ThreadPoolExecutor � blocking I/O
    is safe there.
    """
    global _pipeline, _pipeline_error

    if _pipeline is not None:
        return _pipeline

    try:
        # Deferred import � keeps event-loop startup clean
        from src.services.royal.royal_tech_processor import RoyalInvoicePipeline  # noqa: PLC0415

        cfg.validate()
        _pipeline = RoyalInvoicePipeline()
        _pipeline_error = None
        logger.info("RoyalInvoicePipeline singleton initialised")
        return _pipeline
    except ValueError as exc:
        _pipeline_error = str(exc)
        raise
    except Exception as exc:
        _pipeline_error = str(exc)
        raise RuntimeError(f"Pipeline init failed: {exc}") from exc


# ============================================================================
# App lifecycle
# ============================================================================


@app.on_event("startup")
async def _on_startup() -> None:
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(_THREAD_POOL, _get_pipeline)
        logger.info("Startup pre-warm: pipeline ready")
    except Exception as exc:
        logger.error("Startup pre-warm failed (will init on first request): %s", exc)


@app.on_event("shutdown")
async def _on_shutdown() -> None:
    _THREAD_POOL.shutdown(wait=False)
    logger.info("Shutdown: thread pool released")


# ============================================================================
# API-key validation
# ============================================================================


def _validate_api_key(api_key: str) -> dict:
    """
    Validate the ApiKey header using the project's shared validator.

    Returns
    -------
    dict
        {"user_id": <int>, "api_key_id": <int>}

    Raises
    ------
    PermissionError
        If the key is invalid, inactive, or expired.
    """
    from src.api.routes.api_server_apikey_generate import (  # noqa: PLC0415
        validate_api_key_from_header,
    )

    result = validate_api_key_from_header(api_key)
    if not result:
        raise PermissionError("Invalid, inactive, or expired API key.")
    return result


# ============================================================================
# Temp-file helpers
# ============================================================================


def _write_temp_pdf(pdf_bytes: bytes, request_id: str) -> Path:
    with tempfile.NamedTemporaryFile(
        suffix=".pdf",
        prefix=f"royal_{request_id}_",
        delete=False,
    ) as fh:
        fh.write(pdf_bytes)
        return Path(fh.name)


def _delete_temp(path: Optional[Path], request_id: str) -> None:
    if path is None:
        return
    try:
        if path.exists():
            path.unlink()
    except Exception as exc:
        logger.warning("[%s] Could not delete temp file %s: %s", request_id, path, exc)


# ============================================================================
# Response builders
# ============================================================================


def _success_response(result, filename: str, request_id: str) -> dict:
    ocr_meta = None
    if result.ocr_result:
        ocr_meta = {
            "total_pages": result.ocr_result.total_pages,
            "processed_pages": result.ocr_result.processed_pages,
            "empty_pages": result.ocr_result.empty_pages,
            "failed_pages": result.ocr_result.failed_pages,
            "ocr_time_seconds": round(result.ocr_result.processing_time_seconds, 2),
        }

    token_usage = None
    if result.gemini_token_usage:
        token_usage = result.gemini_token_usage.as_dict()

    return {
        "status": "success",
        "request_id": request_id,
        "filename": filename,
        "total_time_seconds": round(result.total_time_seconds, 2),
        "step_timings": {k: round(v, 3) for k, v in result.step_timings.items()},
        "pipeline_meta": {
            "line_item_pages": result.line_item_pages,
            "identifier_count": result.identifier_count,
            "batch_plan": result.batch_plan_summary,
            "failed_batches": result.failed_batches,
            "warnings": result.warnings,
            "ocr_summary": ocr_meta,
            "gemini_token_usage": token_usage,
        },
        "data": result.final_output,
    }


def _error_response(
    detail: str,
    filename: str,
    request_id: str,
    result=None,
) -> dict:
    meta: dict = {}
    if result:
        token_usage = None
        if result.gemini_token_usage:
            token_usage = result.gemini_token_usage.as_dict()
        meta = {
            "line_item_pages": result.line_item_pages,
            "identifier_count": result.identifier_count,
            "failed_batches": result.failed_batches,
            "warnings": result.warnings,
            "step_timings": {k: round(v, 3) for k, v in result.step_timings.items()},
            "gemini_token_usage": token_usage,
        }
    return {
        "status": "error",
        "request_id": request_id,
        "filename": filename,
        "detail": detail,
        "pipeline_meta": meta,
        "data": None,
    }


# ============================================================================
# Blocking pipeline runner  (runs inside the ThreadPoolExecutor)
# ============================================================================


def _run_pipeline_sync(
    tmp_path: Path,
    schema: dict,
    metadata: dict,
    request_id: str,
):
    """
    Synchronous wrapper executed by run_in_executor().

    1. Gets (or inits) the pipeline singleton.
    2. Runs extraction with the dynamically loaded schema.
    3. Returns a PipelineResult.
    """
    logger.info(
        "[%s] Worker thread: starting pipeline on %s", request_id, tmp_path
    )
    pipeline = _get_pipeline()
    result = pipeline.run(
        pdf_path=tmp_path,
        schema=schema,
        metadata=metadata,
    )
    logger.info(
        "[%s] Worker thread: pipeline finished success=%s time=%.2fs",
        request_id,
        result.success,
        result.total_time_seconds,
    )
    return result


# ============================================================================
# Main endpoint: POST /royal_tech/pdf
# ============================================================================


@app.post("/royal_tech/pdf", tags=["Extraction"])
async def extract_invoice(
    request: Request,
    # -- File ------------------------------------------------------------
    File: UploadFile = File(..., description="PDF invoice file"),  # noqa: N803
    # -- Mandatory form fields --------------------------------------------
    CompanyID: str = Form(...),
    UserID: str = Form(...),
    BranchCode: str = Form(...),
    JobNo: str = Form(...),
    JobType: str = Form(...),
    WorkingPeriod: str = Form(...),
    PageCount: str = Form(...),
    PdfCount: str = Form(...),
    JobStatus: str = Form(...),
    FileName: str = Form(...),
    InvoiceStartTime: str = Form(...),
    PdfClientName: str = Form(...),
    # -- Auth header ------------------------------------------------------
    ApiKey: str = Header(..., alias="ApiKey"),
) -> JSONResponse:
    """
    Extract structured data from an invoice PDF.

    **Headers:**
    - `ApiKey` *(required)* � authentication token

    **Form fields (all required):**
    CompanyID, UserID, BranchCode, JobNo, JobType, WorkingPeriod,
    PageCount, PdfCount, JobStatus, FileName, InvoiceStartTime, PdfClientName

    **File:**
    - `File` � PDF upload

    **Response `data`** follows the Document 2 schema:
    `Header`, `ItemsDetails[]`, `ShipmentContainerDetails`
    """
    request_id = str(uuid.uuid4())[:8]
    filename = File.filename or FileName or "upload.pdf"
    tmp_path: Optional[Path] = None

    logger.info(
        "[%s] POST /royal_tech/pdf  file=%r  PdfClientName=%r",
        request_id,
        filename,
        PdfClientName,
    )

    # -- 1. API-key validation -------------------------------------------
    logger.info("[%s] Validating API key...", request_id)
    try:
        auth_info = _validate_api_key(ApiKey)
    except PermissionError as exc:
        logger.warning("[%s] API key rejected: %s", request_id, exc)
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content=_error_response(str(exc), filename, request_id),
        )
    except Exception as exc:
        logger.error("[%s] API key validation error: %s", request_id, exc)
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content=_error_response(
                "API key validation failed.", filename, request_id
            ),
        )

    user_id: int = auth_info["user_id"]
    logger.info("[%s] API key valid � user_id=%s", request_id, user_id)

    # -- 2. Dynamic schema loading ---------------------------------------
    logger.info(
        "[%s] Loading schema for PdfClientName=%r user_id=%s",
        request_id,
        PdfClientName,
        user_id,
    )
    try:
        schema = load_schema(user_id=user_id, pdf_client_name=PdfClientName)
    except DocumentTypeNotFoundError as exc:
        logger.warning("[%s] %s", request_id, exc)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=_error_response(
                "Document type not configured.", filename, request_id
            ),
        )
    except SchemaLoaderError as exc:
        logger.error("[%s] Schema load error: %s", request_id, exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_response(
                f"Schema loading failed: {exc}", filename, request_id
            ),
        )

    logger.info("[%s] Schema loaded successfully", request_id)

    # -- 3. Validate file type -------------------------------------------
    if not filename.lower().endswith(".pdf"):
        return JSONResponse(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            content=_error_response(
                f"Only PDF files are accepted. Got: '{filename}'",
                filename,
                request_id,
            ),
        )

    # -- 4. Read uploaded bytes (async) ----------------------------------
    try:
        pdf_bytes = await File.read()
    except Exception as exc:
        logger.error("[%s] File read error: %s", request_id, exc)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=_error_response(
                f"Could not read file: {exc}", filename, request_id
            ),
        )

    if not pdf_bytes:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=_error_response("Uploaded file is empty.", filename, request_id),
        )

    logger.info("[%s] Received %d bytes", request_id, len(pdf_bytes))

    # -- 5. Write temp file ----------------------------------------------
    try:
        tmp_path = _write_temp_pdf(pdf_bytes, request_id)
    except Exception as exc:
        logger.error("[%s] Temp file error: %s", request_id, exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_response(
                f"Server error preparing file: {exc}", filename, request_id
            ),
        )

    # -- 6. Collect metadata for injection ------------------------------
    metadata = {
        "CompanyID": CompanyID,
        "UserID": UserID,
        "BranchCode": BranchCode,
        "JobNo": JobNo,
        "JobType": JobType,
        "WorkingPeriod": WorkingPeriod,
        "PageCount": PageCount,
        "PdfCount": PdfCount,
        "JobStatus": JobStatus,
        "FileName": FileName,
        "InvoiceStartTime": InvoiceStartTime,
        "PdfClientName": PdfClientName,
    }

    # -- 7. Run pipeline in thread pool ----------------------------------
    result = None
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _THREAD_POOL,
            _run_pipeline_sync,
            tmp_path,
            schema,
            metadata,
            request_id,
        )
    except ValueError as exc:
        logger.error("[%s] Config error: %s", request_id, exc)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=_error_response(
                f"Service configuration error: {exc}", filename, request_id
            ),
        )
    except Exception as exc:
        logger.exception("[%s] Unexpected pipeline exception: %s", request_id, exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_response(
                f"Internal error: {exc}", filename, request_id, result
            ),
        )
    finally:
        _delete_temp(tmp_path, request_id)

    # -- 8. Return response ----------------------------------------------
    if result and result.success:
        item_count = (
            len(result.final_output.get("ItemsDetails", []))
            if isinstance(result.final_output, dict)
            else 0
        )
        token_total = (
            result.gemini_token_usage.total_tokens
            if result.gemini_token_usage
            else 0
        )
        logger.info(
            "[%s] Success � %d item(s) in %.2fs | Gemini tokens: %d",
            request_id,
            item_count,
            result.total_time_seconds,
            token_total,
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=_success_response(result, filename, request_id),
        )
    else:
        error_detail = (
            (result.error or "Unknown pipeline failure") if result else "No result"
        )
        logger.warning("[%s] Extraction failed: %s", request_id, error_detail)
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=_error_response(error_detail, filename, request_id, result),
        )


# ============================================================================
# Info / health routes
# ============================================================================


@app.get("/", tags=["Info"])
async def root() -> dict:
    return {
        "name": "Royal Tech Invoice Extraction API",
        "version": "2.0.0",
        "description": "Upload a PDF invoice to extract structured data.",
        "endpoints": {
            "POST /royal_tech/pdf": "Upload a PDF and extract invoice data",
            "GET  /royal_tech/health": "Liveness check",
            "GET  /docs": "Swagger UI",
        },
        "example": (
            "curl -X POST http://localhost:8000/royal_tech/pdf "
            '-H "ApiKey: <your-key>" '
            '-F "File=@invoice.pdf" '
            '-F "CompanyID=C001" '
            '-F "UserID=U001" '
            '-F "PdfClientName=RoyalInvoice" ...'
        ),
    }


@app.get("/royal_tech/health", tags=["Info"])
async def health() -> dict:
    return {
        "status": "ok",
        "pipeline_ready": _pipeline is not None,
        "pipeline_error": _pipeline_error,
    }


# ============================================================================
# Global exception handler
# ============================================================================


@app.exception_handler(Exception)
async def _global_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception %s %s", request.method, request.url)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"status": "error", "detail": "Internal server error", "data": None},
    )


# ============================================================================
# Direct run
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "royal_tech_app:app",
        host=cfg.api.host,
        port=cfg.api.port,
        reload=cfg.api.reload,
        workers=cfg.api.workers,
        log_level=cfg.logging.level.lower(),
    )