# -*- coding: utf-8 -*-
"""
royal_tech_processor_helpers.py  -  Shared helpers for the Royal Tech pipeline.

Contains:
  * GeminiTokenUsage / GeminiTokenTracker  -  thread-safe token accumulator
  * wrap_call_gemini / restore_call_gemini  -  monkey-patch helpers
  * extract_page_markdown_map  -  slices full OCR markdown by page
  * PipelineResult  -  full result dataclass for one pipeline run

Imported by royal_tech_processor.py.

Step ordering (for reference):
  Step 1   OCR                          (common)
  Step 2   Invoice-type detection       (common  -  NORMAL or CROSS_PAGE)
  NORMAL path:
    NI-3   Parallel per-page extraction
    NI-4   Merge
  CROSS_PAGE path:
    Step 2b  Line-item page detection
    Step 3   Identifier extraction
    Step 4   Batch planning
    Step 5   Batch extraction
    Step 6   Merge
  Both paths:
    Step 7   Metadata injection
    Step 8   Validation + Rule 49
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from src.services.royal.royal_tech_config import cfg

# Imported for type hints only  -  no circular dependency
from src.services.royal.royal_tech_ocr_service import OcrResult
from src.services.royal.royal_tech_batch_extractor import BatchResult
from src.services.royal.royal_tech_validator import ValidationResult

logger = logging.getLogger(__name__)


# ============================================================================
# Gemini token usage dataclass
# ============================================================================

@dataclass
class GeminiTokenUsage:
    """Accumulated Gemini token counts for one pipeline run."""

    input_tokens:  int = 0
    output_tokens: int = 0
    total_tokens:  int = 0
    call_count:    int = 0

    def as_dict(self) -> dict:
        return {
            "input_tokens":  self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens":  self.total_tokens,
            "call_count":    self.call_count,
        }


# ============================================================================
# Gemini token tracker  (thread-safe)
# ============================================================================

class GeminiTokenTracker:
    """
    Thread-safe accumulator for Gemini token usage across all parallel calls.

    One instance per pipeline run. Reset before each run(), snapshot() after.
    """

    def __init__(self) -> None:
        self._lock  = threading.Lock()
        self._usage = GeminiTokenUsage()

    def record(self, usage_metadata: dict) -> None:
        """
        Accumulate token counts from one Gemini response's usageMetadata dict.

        Missing keys are treated as 0.
        """
        inp = usage_metadata.get("promptTokenCount",     0) or 0
        out = usage_metadata.get("candidatesTokenCount", 0) or 0
        tot = usage_metadata.get("totalTokenCount",      0) or 0
        with self._lock:
            self._usage.input_tokens  += inp
            self._usage.output_tokens += out
            self._usage.total_tokens  += tot
            self._usage.call_count    += 1

    def snapshot(self) -> GeminiTokenUsage:
        """Return a copy of the current accumulated usage (thread-safe)."""
        with self._lock:
            return GeminiTokenUsage(
                input_tokens  = self._usage.input_tokens,
                output_tokens = self._usage.output_tokens,
                total_tokens  = self._usage.total_tokens,
                call_count    = self._usage.call_count,
            )

    def reset(self) -> None:
        """Reset all counters to zero  -  call before each pipeline.run()."""
        with self._lock:
            self._usage = GeminiTokenUsage()


# ============================================================================
# Token-hook installer / restorer
# ============================================================================

def wrap_call_gemini(module: Any, tracker: GeminiTokenTracker) -> Callable:
    """
    Monkey-patch module._call_gemini so every successful Gemini HTTP response
    has its usageMetadata recorded by tracker before the text is returned.

    Requires zero changes to any extractor or detector module  -  their
    internal logic is completely untouched.

    Returns the original (unwrapped) function so it can be restored later.
    """
    import requests as _requests  # noqa: PLC0415

    original_fn: Callable = module._call_gemini

    def _wrapped(prompt: str, max_output_tokens: int) -> Optional[str]:
        gcfg = cfg.gemini
        url = (
            f"{gcfg.api_base_url}/{gcfg.model}"
            f":generateContent?key={gcfg.api_key}"
        )
        payload = {
            "system_instruction": {
                "parts": [{"text": getattr(module, "_SYSTEM_INSTRUCTION", "")}]
            },
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature":      gcfg.temperature,
                "topP":             gcfg.top_p,
                "topK":             gcfg.top_k,
                "maxOutputTokens":  max_output_tokens,
                "responseMimeType": "application/json",
            },
        }
        headers = {"Content-Type": "application/json"}

        try:
            response = _requests.post(
                url, headers=headers, json=payload, timeout=gcfg.timeout
            )
        except _requests.exceptions.Timeout:
            logger.error("GeminiTokenTracker/wrapped: request timed out")
            return None
        except _requests.exceptions.RequestException as exc:
            logger.error("GeminiTokenTracker/wrapped: request failed  -  %s", exc)
            return None

        if response.status_code != 200:
            logger.error(
                "GeminiTokenTracker/wrapped: HTTP %d  -  %s",
                response.status_code, response.text[:400],
            )
            return None

        try:
            data       = response.json()
            usage_meta = data.get("usageMetadata", {})
            tracker.record(usage_meta if usage_meta else {})
            text = (
                data
                .get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
            )
            return text.strip() if text else None
        except (KeyError, IndexError, ValueError) as exc:
            logger.error("GeminiTokenTracker/wrapped: parse error  -  %s", exc)
            return None

    module._call_gemini = _wrapped
    logger.debug(
        "GeminiTokenTracker: patched _call_gemini in module '%s'",
        module.__name__,
    )
    return original_fn


def restore_call_gemini(module: Any, original_fn: Callable) -> None:
    """Restore a module's original _call_gemini after the pipeline run."""
    module._call_gemini = original_fn
    logger.debug(
        "GeminiTokenTracker: restored _call_gemini in module '%s'",
        module.__name__,
    )


# ============================================================================
# Page markdown extractor
# ============================================================================

def extract_page_markdown_map(
    full_markdown: str,
    page_numbers: list[int],
) -> dict[int, str]:
    """
    Slice the full assembled markdown (with ---PAGE N--- separators) into a
    dict keyed by page number, restricted to the requested page_numbers.

    The separator format produced by ocr_service._page_separator() is:
        \\n\\n---PAGE N---\\n\\n

    Algorithm
    ---------
    1. Find every separator match and record (page_num, match.start(), match.end()).
    2. Content for page N runs from end-of-separator-N to start-of-separator-(N+1).
    3. Last page's content runs to EOF.
    """
    sep_pattern = re.compile(r"---PAGE\s+(\d+)---", re.IGNORECASE)

    matches: list[tuple[int, int, int]] = []
    for m in sep_pattern.finditer(full_markdown):
        matches.append((int(m.group(1)), m.start(), m.end()))

    if not matches:
        logger.warning(
            "extract_page_markdown_map: No ---PAGE N--- separators found. "
            "Treating entire document as page 1."
        )
        if 1 in page_numbers:
            return {1: full_markdown.strip()}
        return {}

    page_map: dict[int, str] = {}
    page_numbers_set = set(page_numbers)

    for i, (page_num, _sep_start, sep_end) in enumerate(matches):
        content_start = sep_end
        content_end   = (
            matches[i + 1][1] if i + 1 < len(matches) else len(full_markdown)
        )
        if page_num in page_numbers_set:
            page_map[page_num] = full_markdown[content_start:content_end].strip()

    missing = page_numbers_set - set(page_map.keys())
    if missing:
        logger.warning(
            "extract_page_markdown_map: Pages %s not found in markdown",
            sorted(missing),
        )
    return page_map


# ============================================================================
# Pipeline result dataclass
# ============================================================================

@dataclass
class PipelineResult:
    """
    Full result of one RoyalInvoicePipeline.run() execution.

    Attributes
    ----------
    success : bool
        True only when all steps completed without a fatal error.
    final_output : dict | None
        The merged invoice JSON (Document 2 schema) when success is True.
    pdf_path : str
        Path to the source PDF.
    step_timings : dict[str, float]
        Wall-clock seconds per step name.
        Common keys: step1_ocr, step2_invoice_type_detection,
          step7_metadata_injection, step8_validation.
        NORMAL path adds: ni_extraction_and_merge.
        CROSS_PAGE path adds: step2b_page_detection, step3_identifier_extraction,
          step4_batch_planning, step5_batch_extraction, step6_merge.
    total_time_seconds : float
        End-to-end elapsed time.
    full_markdown : str | None
        Full OCR markdown (all pages).
    ocr_result : OcrResult | None
        Step 1 artefact.
    line_item_pages : list[int]
        Step 2b artefact (CROSS_PAGE path only).  Empty for NORMAL path.
    invoice_type : str | None
        Step 2 artefact  -  "NORMAL_INVOICE" or "CROSS_PAGE_INVOICE".
    identifier_count : int
        Number of identifiers extracted (CROSS_PAGE) or final item count (NORMAL).
    batch_plan_summary : dict | None
        Step 4 artefact (CROSS_PAGE path only).
    batch_results : list[BatchResult]
        Step 5 artefacts (CROSS_PAGE path only).
    failed_batches : list[int]
        Failed batch indices (CROSS_PAGE) or failed page numbers (NORMAL).
    error : str | None
        Fatal error message if success is False.
    warnings : list[str]
        Non-fatal warnings accumulated during the run.
    gemini_token_usage : GeminiTokenUsage | None
        Accumulated Gemini token counts across all steps.
    validation_result : ValidationResult | None
        Step 8 validation + Rule 49 output.
    """

    success:             bool
    final_output:        Optional[dict]
    pdf_path:            str
    step_timings:        dict[str, float]            = field(default_factory=dict)
    total_time_seconds:  float                       = 0.0
    full_markdown:       Optional[str]               = None
    ocr_result:          Optional[OcrResult]         = None
    line_item_pages:     list[int]                   = field(default_factory=list)
    invoice_type:        Optional[str]               = None
    identifier_count:    int                         = 0
    batch_plan_summary:  Optional[dict]              = None
    batch_results:       list[BatchResult]           = field(default_factory=list)
    failed_batches:      list[int]                   = field(default_factory=list)
    error:               Optional[str]               = None
    warnings:            list[str]                   = field(default_factory=list)
    gemini_token_usage:  Optional[GeminiTokenUsage]  = None
    validation_result:   Optional[ValidationResult]  = None

    def log_summary(self) -> None:
        """Write a structured summary to the logger."""
        status = "SUCCESS" if self.success else "FAILED"
        logger.info("=" * 65)
        logger.info(
            "RoyalInvoicePipeline %s  -  %s  (%.2fs)",
            status, self.pdf_path, self.total_time_seconds,
        )
        for step, secs in self.step_timings.items():
            logger.info("  %-34s %.3fs", step, secs)
        if self.ocr_result:
            logger.info("  OCR pages        : %d", self.ocr_result.total_pages)
        logger.info(
            "  Invoice type     : %s", self.invoice_type or "unknown"
        )
        if self.invoice_type == "CROSS_PAGE_INVOICE" and self.line_item_pages:
            logger.info(
                "  Line-item pages  : %s", self.line_item_pages,
            )
        logger.info(
            "  Identifiers      : %d  |  Failed: %s",
            self.identifier_count, self.failed_batches,
        )
        if self.gemini_token_usage:
            tok = self.gemini_token_usage
            logger.info(
                "  Gemini tokens    : calls=%d  in=%d  out=%d  total=%d",
                tok.call_count, tok.input_tokens,
                tok.output_tokens, tok.total_tokens,
            )
        if self.validation_result:
            vr = self.validation_result
            logger.info(
                "  Validation       : %s  errors=%d  warnings=%d",
                "OK" if vr.is_valid else "FAIL",
                len(vr.errors), len(vr.warnings),
            )
        for w in self.warnings:
            logger.info("  ! %s", w)
        if self.error:
            logger.error("  Fatal error      : %s", self.error)
        if self.final_output:
            items = self.final_output.get("ItemsDetails", [])
            logger.info(
                "  Final line items : %d",
                len(items) if isinstance(items, list) else 1,
            )
        logger.info("=" * 65)