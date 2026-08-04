# -*- coding: utf-8 -*-
"""
royal_tech_normal_invoice_extractor.py  -  NI-Step 3: Parallel per-page full
extraction for the NORMAL_INVOICE pipeline path.

NORMAL_INVOICE invoices repeat the full header on every page and contain
complete, self-contained rows  -  every field needed (HSNCode, IGST data,
NetWeight, etc.) is present on the same page as the line item.  There is
therefore no need for cross-page linking or the serial/batch two-pass
approach used by the CROSS_PAGE_INVOICE path.

Strategy
--------
One Gemini 2.0 Flash call per line-item page, all fired in parallel via
ThreadPoolExecutor.  Each call returns the full header + all line items
found on that page, plus the all-null container block.

The output of each call is a PageExtractionResult.  The caller
(royal_tech_normal_invoice_merger.py) is responsible for merging them.

Public API
----------
    extractor = RoyalNormalInvoiceExtractor(schema=schema_dict)
    results   = extractor.extract_all_pages(page_markdown_map)
    # -> list[PageExtractionResult]  one per page, ordered by page number
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests

from src.services.royal.royal_tech_config import cfg
from src.services.royal.royal_tech_batch_extractor_helpers import (
    build_null_container,
    coerce_numeric_fields,
    extract_json,
    inject_null_and_default_header_fields,
    inject_null_item_fields,
    resolve_schema_fields,
    build_header_field_spec,
    build_item_field_spec,
    build_header_json_skeleton,
    build_item_json_skeleton,
    build_container_json_skeleton,
)

logger = logging.getLogger(__name__)


# ============================================================================
# System instruction (referenced by token-tracker monkey-patch in processor)
# ============================================================================

_SYSTEM_INSTRUCTION = """\
You are a precise structured-data extraction engine for commercial invoices.
You output ONLY valid JSON. No prose. No explanation. No markdown fences.\
"""


# ============================================================================
# Prompt template
# ============================================================================

_PAGE_PROMPT_TEMPLATE = """\
You are extracting structured data from ONE PAGE of a commercial invoice
(already converted to Markdown).  This invoice is a NORMAL INVOICE: every
page is fully self-contained  -  the complete header and all data needed for
each line item appear on this page only.

#######################################################
PAGE SCOPE  (Page {page_num})
#######################################################
Extract ALL line items visible on this page.
Do NOT reach outside this page's content for any value.

#######################################################
HEADER EXTRACTION RULES
#######################################################
Extract all header fields from the invoice header on this page.
The header repeats on every page of a NORMAL INVOICE  -  extract it fully.

#######################################################
FIELD-LEVEL RULES
#######################################################
Header fields to extract:
{header_field_spec}

Line item fields to extract per item:
{item_field_spec}

General rules:
  * String values: return as plain string, no surrounding quotes in value.
  * Numeric fields (Amount, Rate, Quantity, Itemslno, NetWeight,
    TaxableAmount, IGSTAmount, IGSTRate): return as JSON number, not a string.
    Strip currency symbols (Rs $ EUR GBP) and commas before returning.
  * PaymentPeriod: extract only the numeric day count (e.g. "60" from "60 Days Net").
  * ItemDesc: description text printed directly below the part number in the invoice table (e.g. "Hydraulic Trailer Brake Kit, FIK, TRAILE"). Always populate; never return null.
  * ItemQTYCode: unit string only (PC, NOS, KG, etc.)  -  NOT the numeric quantity.
  * TotalCarton: numeric digits only, no text.
  * If a value is not found on this page: return JSON null (not the string "null", not "").
  * Do NOT add fields not listed in the schema.
  * Do NOT infer, guess, or hallucinate any value.

#######################################################
REQUIRED OUTPUT FORMAT  -  STRICT JSON
#######################################################
Return ONLY this JSON object. No markdown fences. No commentary.

{{
  "header": {{
{header_json_skeleton}
  }},
  "container_details": {{
{container_json_skeleton}
  }},
  "line_items": [
    {{
{item_json_skeleton}
    }}
  ]
}}

One object in "line_items" per line item visible on this page.
Return an empty array if no line items are present.

#######################################################
PAGE MARKDOWN FOLLOWS
#######################################################
{page_markdown}
"""


# ============================================================================
# PageExtractionResult dataclass
# ============================================================================

@dataclass
class PageExtractionResult:
    """
    Output of RoyalNormalInvoiceExtractor.extract_page() for one page.

    Attributes
    ----------
    page_num : int
        1-indexed page number this result came from.
    success : bool
        True if Gemini returned a parseable, schema-valid response.
    header : dict
        Extracted header fields (fully populated with nulls/defaults).
    container_details : dict
        All-null container block (Document 2 schema).
    line_items : list[dict]
        All line items extracted from this page.
    raw_response : str | None
        Raw Gemini text before parsing  -  preserved for debug.
    error : str | None
        Human-readable error message when success is False.
    """

    page_num:          int
    success:           bool
    header:            dict       = field(default_factory=dict)
    container_details: dict       = field(default_factory=dict)
    line_items:        list[dict] = field(default_factory=list)
    raw_response:      Optional[str] = None
    error:             Optional[str] = None

    def summary(self) -> dict:
        return {
            "page_num":         self.page_num,
            "success":          self.success,
            "line_items_count": len(self.line_items),
            "error":            self.error,
        }


# ============================================================================
# Gemini HTTP caller
# ============================================================================

def _build_gemini_url() -> str:
    gcfg = cfg.gemini
    return f"{gcfg.api_base_url}/{gcfg.model}:generateContent?key={gcfg.api_key}"


def _call_gemini(prompt: str, max_output_tokens: int) -> Optional[str]:
    """POST to Gemini and return the raw text, or None on any failure."""
    gcfg = cfg.gemini
    url  = _build_gemini_url()

    payload = {
        "system_instruction": {"parts": [{"text": _SYSTEM_INSTRUCTION}]},
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
        response = requests.post(
            url, headers=headers, json=payload, timeout=gcfg.timeout
        )
    except requests.exceptions.Timeout:
        logger.error(
            "NormalInvoiceExtractor: Gemini timeout after %ds", gcfg.timeout
        )
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("NormalInvoiceExtractor: Gemini request failed  -  %s", exc)
        return None

    if response.status_code != 200:
        logger.error(
            "NormalInvoiceExtractor: Gemini HTTP %d  -  %s",
            response.status_code, response.text[:400],
        )
        return None

    try:
        data = response.json()
        text = (
            data
            .get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        return text.strip() if text else None
    except (KeyError, IndexError, ValueError) as exc:
        logger.error(
            "NormalInvoiceExtractor: failed to parse Gemini response  -  %s", exc
        )
        return None


# ============================================================================
# RoyalNormalInvoiceExtractor
# ============================================================================

class RoyalNormalInvoiceExtractor:
    """
    NI-Step 3  -  Extracts full header + line items from each NORMAL_INVOICE
    page independently, all pages fired in parallel.

    Each page is self-contained: the complete header and all lookup data
    (HSNCode, IGST, NetWeight, etc.) appear on the same page as the items,
    so no cross-page correlation is needed.

    Usage
    -----
        extractor = RoyalNormalInvoiceExtractor(schema=schema_dict)
        results   = extractor.extract_all_pages(page_markdown_map)
        # -> list[PageExtractionResult], len == len(page_markdown_map)
    """

    def __init__(self, schema: dict[str, Any]) -> None:
        self._cfg_gemini = cfg.gemini
        self._cfg_step   = cfg.batch_extractor   # reuse max_output_tokens
        self._schema     = schema

        self._header_fields, self._item_fields = resolve_schema_fields(schema)

        logger.info(
            "RoyalNormalInvoiceExtractor initialised (model=%s, "
            "max_output_tokens=%d, header_fields=%d, item_fields=%d)",
            self._cfg_gemini.model,
            self._cfg_step.max_output_tokens,
            len(self._header_fields),
            len(self._item_fields),
        )

    # ------------------------------------------------------------------
    # Parallel public method  (NI-Step 3 entry point)
    # ------------------------------------------------------------------

    def extract_all_pages(
        self,
        page_markdown_map: dict[int, str],
    ) -> list[PageExtractionResult]:
        """
        Process every page in page_markdown_map IN PARALLEL.

        Parameters
        ----------
        page_markdown_map : dict[int, str]
            Keys = 1-indexed page numbers from page_detector.
            Values = markdown content for that page only (already sliced
            by extract_page_markdown_map in the processor).

        Returns
        -------
        list[PageExtractionResult]
            Ordered ascending by page_num.
            Always length == len(page_markdown_map).
            Individual entries may have success=False.
        """
        if not page_markdown_map:
            logger.warning(
                "RoyalNormalInvoiceExtractor: page_markdown_map is empty  -  "
                "nothing to extract"
            )
            return []

        sorted_pages = sorted(page_markdown_map.keys())
        total_pages  = len(sorted_pages)

        logger.info(
            "RoyalNormalInvoiceExtractor: submitting %d page(s) to Gemini "
            "IN PARALLEL: %s",
            total_pages, sorted_pages,
        )

        start_time = time.time()
        results: list[PageExtractionResult] = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=total_pages
        ) as executor:
            future_to_page = {
                executor.submit(
                    self.extract_page,
                    page_markdown_map[page_num],
                    page_num,
                ): page_num
                for page_num in sorted_pages
            }

            for future in concurrent.futures.as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(
                        "RoyalNormalInvoiceExtractor: page %d done  -  "
                        "success=%s items=%d",
                        page_num, result.success, len(result.line_items),
                    )
                except Exception as exc:
                    logger.error(
                        "RoyalNormalInvoiceExtractor: page %d raised "
                        "exception: %s",
                        page_num, exc,
                    )
                    results.append(PageExtractionResult(
                        page_num=page_num,
                        success=False,
                        error=f"Unhandled exception on page {page_num}: {exc}",
                    ))

        elapsed = time.time() - start_time
        results.sort(key=lambda r: r.page_num)

        succeeded = sum(1 for r in results if r.success)
        failed    = total_pages - succeeded
        logger.info(
            "RoyalNormalInvoiceExtractor: parallel complete in %.2fs  -  "
            "%d succeeded, %d failed",
            elapsed, succeeded, failed,
        )
        for r in results:
            if not r.success:
                logger.error(
                    "  FAIL Page %d failed: %s", r.page_num, r.error
                )

        return results

    # ------------------------------------------------------------------
    # Single-page public method
    # ------------------------------------------------------------------

    def extract_page(
        self,
        page_markdown: str,
        page_num: int,
    ) -> PageExtractionResult:
        """
        Extract header + all line items from a single page's markdown.

        Parameters
        ----------
        page_markdown : str
            Markdown for this page only (no other pages included).
        page_num : int
            1-indexed page number  -  used in logging and result.

        Returns
        -------
        PageExtractionResult  -  always returned; check .success before using.
        """
        if not page_markdown or not page_markdown.strip():
            return PageExtractionResult(
                page_num=page_num, success=False,
                error=f"page {page_num}: empty markdown",
            )

        logger.info(
            "RoyalNormalInvoiceExtractor: page %d  -  extracting "
            "(%d markdown chars)",
            page_num, len(page_markdown),
        )

        prompt   = self._build_prompt(page_markdown, page_num)
        raw_text = self._call_with_retry(prompt, page_num)

        if raw_text is None:
            return PageExtractionResult(
                page_num=page_num, success=False,
                error=f"All Gemini attempts failed for page {page_num}",
            )

        logger.debug(
            "RoyalNormalInvoiceExtractor: raw response page %d (%d chars):\n%s",
            page_num, len(raw_text), raw_text[:500],
        )

        parsed = extract_json(raw_text)
        if parsed is None:
            return PageExtractionResult(
                page_num=page_num, success=False,
                raw_response=raw_text,
                error=f"JSON parse failed for page {page_num}",
            )

        structure_error = self._validate_structure(parsed, page_num)
        if structure_error:
            return PageExtractionResult(
                page_num=page_num, success=False,
                raw_response=raw_text,
                error=structure_error,
            )

        raw_header     = parsed.get("header",            {}) or {}
        raw_line_items = parsed.get("line_items",        []) or []

        header     = self._process_header(raw_header)
        container  = build_null_container()
        line_items = self._process_line_items(raw_line_items, page_num)

        logger.info(
            "RoyalNormalInvoiceExtractor: page %d  -  extracted %d item(s)",
            page_num, len(line_items),
        )

        return PageExtractionResult(
            page_num=page_num,
            success=True,
            header=header,
            container_details=container,
            line_items=line_items,
            raw_response=raw_text,
        )

    # ------------------------------------------------------------------
    # Prompt builder
    # ------------------------------------------------------------------

    def _build_prompt(self, page_markdown: str, page_num: int) -> str:
        return _PAGE_PROMPT_TEMPLATE.format(
            page_num=page_num,
            header_field_spec=build_header_field_spec(self._header_fields),
            item_field_spec=build_item_field_spec(self._item_fields),
            header_json_skeleton=build_header_json_skeleton(self._header_fields),
            container_json_skeleton=build_container_json_skeleton(),
            item_json_skeleton=build_item_json_skeleton(self._item_fields),
            page_markdown=page_markdown,
        )

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def _validate_structure(self, parsed: Any, page_num: int) -> Optional[str]:
        """
        Return an error string if the top-level JSON structure is invalid,
        or None if it is acceptable.
        """
        if not isinstance(parsed, dict):
            return (
                f"page {page_num}: response is not a JSON object "
                f"(got {type(parsed).__name__})"
            )
        if "header" not in parsed:
            return f"page {page_num}: missing 'header' key"
        if "line_items" not in parsed:
            return f"page {page_num}: missing 'line_items' key"
        if not isinstance(parsed["line_items"], list):
            return (
                f"page {page_num}: 'line_items' is not a list "
                f"(got {type(parsed['line_items']).__name__})"
            )
        return None

    # ------------------------------------------------------------------
    # Post-processing helpers
    # ------------------------------------------------------------------

    def _process_header(self, raw_header: dict) -> dict:
        """Inject always-null and always-default header fields from schema."""
        return inject_null_and_default_header_fields(raw_header, self._schema)

    def _process_line_items(
        self,
        raw_items: list,
        page_num: int,
    ) -> list[dict]:
        """
        Validate, inject null fields, and coerce numeric fields for every
        raw line-item dict returned by Gemini.
        """
        processed: list[dict] = []
        for idx, item in enumerate(raw_items):
            if not isinstance(item, dict):
                logger.warning(
                    "RoyalNormalInvoiceExtractor: page %d item[%d] not a "
                    "dict  -  skipped",
                    page_num, idx,
                )
                continue
            item = inject_null_item_fields(item, self._schema)
            item = coerce_numeric_fields(item)
            processed.append(item)
        return processed

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    def _call_with_retry(self, prompt: str, page_num: int) -> Optional[str]:
        """
        Call Gemini up to cfg.gemini.max_retries times with exponential
        back-off (retry_backoff_base ** attempt seconds).
        """
        gcfg       = self._cfg_gemini
        max_tokens = self._cfg_step.max_output_tokens

        for attempt in range(gcfg.max_retries):
            logger.info(
                "RoyalNormalInvoiceExtractor: page %d  -  Gemini attempt %d/%d",
                page_num, attempt + 1, gcfg.max_retries,
            )
            result = _call_gemini(prompt, max_tokens)
            if result is not None:
                return result

            if attempt < gcfg.max_retries - 1:
                wait = gcfg.retry_backoff_base ** attempt
                logger.warning(
                    "RoyalNormalInvoiceExtractor: page %d attempt %d failed "
                    " -  retrying in %.1fs",
                    page_num, attempt + 1, wait,
                )
                time.sleep(wait)

        logger.error(
            "RoyalNormalInvoiceExtractor: page %d  -  all %d attempts exhausted",
            page_num, gcfg.max_retries,
        )
        return None