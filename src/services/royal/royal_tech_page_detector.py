# -*- coding: utf-8 -*-
"""
royal_tech_page_detector.py  -  STEP 2b (CROSS_PAGE path only): Detect which
pages contain the MAIN invoice line-item table.

This step runs ONLY for CROSS_PAGE_INVOICE documents.  Invoice-type detection
(Step 2, common to both paths) is performed earlier by InvoiceTypeDetector
directly in the processor and is no longer part of this module.

Direct refactor of page_detector.py:
  * config  -> royal_tech_config.cfg
  * Class renamed RoyalPageDetector
  * All module-level constants, helpers, and method logic preserved exactly
  * _SYSTEM_INSTRUCTION and _DETECT_PROMPT kept verbatim  -  prompt engineering
    must never change silently

Public API
----------
    detector = RoyalPageDetector()
    pages    = detector.detect(full_markdown, total_pages)            -> list[int]
    debug    = detector.detect_with_debug(full_markdown, total_pages) -> dict
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Optional

import requests

from src.services.royal.royal_tech_config import cfg

logger = logging.getLogger(__name__)

# ============================================================================
# Prompt constants  (verbatim  -  do not alter)
# ============================================================================

_SYSTEM_INSTRUCTION = """\
You are a precise document-structure analyser for complex commercial invoices.
You output ONLY valid JSON. No prose. No explanation. No markdown fences.\
"""

_DETECT_PROMPT = """\
You are analysing a multi-page commercial invoice converted to Markdown.
Page boundaries are marked with:  ---PAGE <number>---

Your ONLY task: return the page numbers that contain the MAIN EXPORT INVOICE line-item table.

#######################################################
WHAT THE MAIN LINE-ITEM TABLE LOOKS LIKE  -  INCLUDE THESE PAGES
#######################################################
A page qualifies ONLY if ALL of the following are true:

  1. The page header says "EXPORT INVOICE" (or the same invoice header repeats).
  2. There is a table whose columns include:
       * Part No / Material ID / Catalogue No  (e.g. "SU26573", "RE282287")
       * Description of Goods
       * Qty / UOM
       * Rate (USD / EUR / local currency)   -  a UNIT PRICE per row
       * Amount / Total                      -  a LINE TOTAL per row
  3. Each data row represents ONE individual part being sold/shipped,
     with its own Rate and Amount values.

#######################################################
PAGES TO EXCLUDE  -  HARD RULES, NO EXCEPTIONS
#######################################################
EXCLUDE any page that matches ANY of the following patterns:

  >  TOTALS / SUMMARY PAGE
       The page shows only grand totals, sub-totals, charges (freight, insurance,
       packing), or "AMOUNT CHARGEABLE" with a total in words.
       It does NOT list individual parts with their own Rate and Amount.

  >  EXAMINATION REPORT
       The page is titled "Examination Report".
       It shows Material No, HSN Code, and IGST Tax Rate columns only  -
       there is NO Rate (USD) column and NO per-row Amount (USD) column.

  >  ANNEXURE / TAX BREAKDOWN  (titled "Annexure I", "Annexure", or similar)
       The page shows INR assessable values, CGST Amt, SGST Amt, IGST Amt columns.
       Even though it has Material No and Qty, it is a TAX document, not the
       billing invoice. The currency is INR and amounts are domestic tax figures.
       EXCLUDE ALL Annexure pages without exception.

  >  PACKING LIST
       The page is titled "Packing List" or "Total Packing List".
       It shows SR NO, PART, DESCRIPTION, QTY, PALLET NO, No. of BOXES,
       NET WT, GROSS WEIGHT columns. There is no Rate or Amount column.

  >  INVOICE SUMMARY / TOTALS-ONLY
       The page shows a summary table with Total NDP, Total Discount,
       Total Freight, Total Assessable Value, Total IGST, etc.
       No individual line items appear on this page.

#######################################################
CROSS-PAGE AWARENESS
#######################################################
The MAIN line-item table often spans multiple consecutive pages.
Each continuation page repeats the invoice header ("EXPORT INVOICE", exporter
name, invoice number) followed by more part rows. Include ALL such pages.
Stop including pages the moment the content no longer shows part rows with
Rate (USD) and Amount (USD) per row.

#######################################################
DECISION CHECKLIST  -  apply to every page
#######################################################
For each page, ask in order:

  Q1. Does the page say "Annexure" anywhere? -> EXCLUDE immediately.
  Q2. Does the page say "Examination Report"? -> EXCLUDE immediately.
  Q3. Does the page say "Packing List"?       -> EXCLUDE immediately.
  Q4. Does the page show only totals / grand total / AMOUNT CHARGEABLE
      with no individual part rows?            -> EXCLUDE immediately.
  Q5. Does the page show individual part rows with Part No, Description,
      Qty, Rate (USD), and Amount (USD)?       -> INCLUDE.
  Q6. None of the above match?                -> EXCLUDE (default safe).

#######################################################
OUTPUT FORMAT  -  STRICT JSON, NOTHING ELSE
#######################################################
Return ONLY this JSON object. No prose. No markdown fences. No backticks.

{{
  "line_items_pages": [<integer page numbers in ascending order>]
}}

If no qualifying pages are found:
{{
  "line_items_pages": []
}}

#######################################################
DOCUMENT MARKDOWN FOLLOWS
#######################################################
{full_markdown}
"""

# ============================================================================
# Gemini HTTP helper
# ============================================================================

def _build_gemini_url() -> str:
    gcfg = cfg.gemini
    return (
        f"{gcfg.api_base_url}/{gcfg.model}"
        f":generateContent?key={gcfg.api_key}"
    )


def _call_gemini(prompt: str, max_output_tokens: int) -> Optional[str]:
    """
    POST to Gemini and return the raw text response.
    Returns None on any failure so the caller can decide on retry / fallback.
    """
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
            "RoyalPageDetector: Gemini request timed out after %ds", gcfg.timeout
        )
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("RoyalPageDetector: Gemini request failed  -  %s", exc)
        return None

    if response.status_code != 200:
        logger.error(
            "RoyalPageDetector: Gemini HTTP %d  -  %s",
            response.status_code, response.text[:500],
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
            "RoyalPageDetector: failed to parse Gemini response  -  %s", exc
        )
        return None


# ============================================================================
# JSON extraction / repair
# ============================================================================

def _extract_json(raw: str) -> Optional[dict]:
    """
    Attempt to extract a JSON object from Gemini's raw text output.

    Strategy (in order):
    1. Direct json.loads on the full string.
    2. Strip markdown code fences then parse.
    3. Regex-search for the first {...} block.
    4. Repair truncated JSON  -  recover partial integer array from a cut response.
    """
    if not raw:
        return None

    # Attempt 1  -  direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Attempt 2  -  strip markdown fences
    stripped = (
        re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE)
        .strip()
        .rstrip("`")
        .strip()
    )
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    # Attempt 3  -  find first {...} block
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Attempt 4  -  repair truncated JSON (max_output_tokens cut mid-stream)
    # e.g.  {"line_items_pages": [1, 2, 3   <-- response cut here
    partial = re.search(
        r'"line_items_pages"\s*:\s*\[([^\]]*)', raw, re.DOTALL
    )
    if partial:
        numbers = re.findall(r"\d+", partial.group(1))
        if numbers:
            logger.warning(
                "RoyalPageDetector: repaired truncated JSON  -  "
                "recovered %d page number(s) from partial response",
                len(numbers),
            )
            return {"line_items_pages": [int(n) for n in numbers]}

    logger.warning(
        "RoyalPageDetector: could not parse JSON from Gemini output:\n%s",
        raw[:300],
    )
    return None


# ============================================================================
# Page-list validation
# ============================================================================

def _validate_pages(raw_pages: list, total_pages: int) -> list[int]:
    """
    Sanitise the raw page list returned by Gemini:
    * Keep only integers.
    * Keep only values in range [1, total_pages].
    * Sort ascending and deduplicate.
    """
    if not isinstance(raw_pages, list):
        logger.warning(
            "RoyalPageDetector: 'line_items_pages' is not a list  -  got %s",
            type(raw_pages),
        )
        return []

    valid: list[int] = []
    for item in raw_pages:
        try:
            page_num = int(item)
        except (TypeError, ValueError):
            logger.warning(
                "RoyalPageDetector: ignoring non-integer page entry: %r", item
            )
            continue

        if 1 <= page_num <= total_pages:
            valid.append(page_num)
        else:
            logger.warning(
                "RoyalPageDetector: page %d out of range [1, %d]  -  ignored",
                page_num, total_pages,
            )

    return sorted(set(valid))


# ============================================================================
# RoyalPageDetector
# ============================================================================

class RoyalPageDetector:
    """
    STEP 2b (CROSS_PAGE_INVOICE path only)  -  Detects which pages of the
    OCR'd markdown contain the MAIN invoice line-item table.

    This class is used exclusively in the CROSS_PAGE_INVOICE path.
    Invoice-type detection (Step 2) is handled upstream by InvoiceTypeDetector
    before this class is ever called.

    Usage
    -----
        detector = RoyalPageDetector()
        pages    = detector.detect(full_markdown, total_pages=12)
        # -> [2, 3, 4]
    """

    def __init__(self) -> None:
        self._cfg_gemini = cfg.gemini
        self._cfg_step   = cfg.page_detector
        logger.info(
            "RoyalPageDetector initialised (model=%s, max_output_tokens=%d, "
            "fallback=%s)",
            self._cfg_gemini.model,
            self._cfg_step.max_output_tokens,
            self._cfg_step.fallback_to_all_pages_on_empty,
        )

    # ------------------------------------------------------------------
    # Primary public method
    # ------------------------------------------------------------------

    def detect(self, full_markdown: str, total_pages: int) -> list[int]:
        """
        Detect MAIN line-item pages from the full OCR markdown.

        Called only for CROSS_PAGE_INVOICE documents (Step 2b).

        Parameters
        ----------
        full_markdown : str
            Complete assembled markdown for all pages, with ---PAGE N---
            separators as produced by royal_tech_ocr_service.py.
        total_pages : int
            Total number of pages in the source PDF.

        Returns
        -------
        list[int]
            Ascending list of 1-indexed page numbers containing the MAIN
            line-item table. Never empty (falls back to all pages if needed).
        """
        if not full_markdown or not full_markdown.strip():
            logger.error(
                "RoyalPageDetector: empty markdown  -  returning all pages as fallback"
            )
            return self._fallback_all_pages(total_pages, reason="empty markdown")

        if total_pages < 1:
            logger.error(
                "RoyalPageDetector: total_pages=%d is invalid", total_pages
            )
            raise ValueError(f"total_pages must be >= 1, got {total_pages}")

        logger.info(
            "RoyalPageDetector: starting detection on %d-page document "
            "(%d chars of markdown)",
            total_pages, len(full_markdown),
        )

        prompt   = _DETECT_PROMPT.format(full_markdown=full_markdown)
        raw_text = self._call_with_retry(prompt)

        if raw_text is None:
            return self._fallback_all_pages(
                total_pages, reason="all Gemini attempts failed"
            )

        logger.debug(
            "RoyalPageDetector: raw Gemini response:\n%s", raw_text[:500]
        )

        parsed = _extract_json(raw_text)
        if parsed is None:
            return self._fallback_all_pages(
                total_pages, reason="JSON parse failed"
            )

        raw_pages = parsed.get("line_items_pages")
        if raw_pages is None:
            logger.warning(
                "RoyalPageDetector: 'line_items_pages' key missing from "
                "response: %s", parsed,
            )
            return self._fallback_all_pages(
                total_pages, reason="missing 'line_items_pages' key"
            )

        pages = _validate_pages(raw_pages, total_pages)

        if not pages:
            logger.warning(
                "RoyalPageDetector: Gemini returned zero valid line-item pages "
                "(raw=%s, total_pages=%d)",
                raw_pages, total_pages,
            )
            return self._fallback_all_pages(
                total_pages,
                reason="Gemini returned empty or all-invalid page list",
            )

        logger.info(
            "RoyalPageDetector: detected %d line-item page(s): %s",
            len(pages), pages,
        )
        return pages

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    def _call_with_retry(self, prompt: str) -> Optional[str]:
        """
        Call Gemini with up to cfg.gemini.max_retries attempts.
        Uses exponential back-off: wait = retry_backoff_base ** attempt seconds.
        """
        gcfg              = self._cfg_gemini
        max_output_tokens = self._cfg_step.max_output_tokens

        for attempt in range(gcfg.max_retries):
            logger.info(
                "RoyalPageDetector: Gemini call attempt %d/%d",
                attempt + 1, gcfg.max_retries,
            )
            result = _call_gemini(prompt, max_output_tokens)

            if result is not None:
                return result

            if attempt < gcfg.max_retries - 1:
                wait = gcfg.retry_backoff_base ** attempt
                logger.warning(
                    "RoyalPageDetector: attempt %d failed  -  retrying in %.1fs",
                    attempt + 1, wait,
                )
                time.sleep(wait)

        logger.error(
            "RoyalPageDetector: all %d Gemini attempts exhausted",
            gcfg.max_retries,
        )
        return None

    # ------------------------------------------------------------------
    # Fallback
    # ------------------------------------------------------------------

    def _fallback_all_pages(self, total_pages: int, reason: str) -> list[int]:
        """
        Return all pages [1 - total_pages] as a safe fallback.

        Behaviour is controlled by
        cfg.page_detector.fallback_to_all_pages_on_empty.
        If that flag is False, raises RuntimeError to surface the failure.
        """
        if not self._cfg_step.fallback_to_all_pages_on_empty:
            raise RuntimeError(
                f"RoyalPageDetector: could not detect line-item pages "
                f"({reason}). fallback_to_all_pages_on_empty is False  -  "
                "aborting."
            )

        all_pages = list(range(1, total_pages + 1))
        logger.warning(
            "RoyalPageDetector: FALLBACK  -  treating ALL %d pages as "
            "line-item pages. Reason: %s",
            total_pages, reason,
        )
        return all_pages

    # ------------------------------------------------------------------
    # Debug helper
    # ------------------------------------------------------------------

    def detect_with_debug(
        self,
        full_markdown: str,
        total_pages:   int,
    ) -> dict:
        """
        Same as detect() but returns a rich debug dict instead of just the
        page list.  Useful when cfg.pipeline.debug_save_intermediate is True.

        Returns
        -------
        dict with keys:
            pages          -  list[int]   final validated page list
            raw_response   -  str | None  raw Gemini text before parsing
            parsed_json    -  dict | None parsed JSON object
            fallback_used  -  bool        whether fallback was triggered
        """
        debug: dict = {
            "pages":         [],
            "raw_response":  None,
            "parsed_json":   None,
            "fallback_used": False,
        }

        if not full_markdown or not full_markdown.strip():
            debug["pages"]         = self._fallback_all_pages(
                total_pages, "empty markdown"
            )
            debug["fallback_used"] = True
            return debug

        prompt   = _DETECT_PROMPT.format(full_markdown=full_markdown)
        raw_text = self._call_with_retry(prompt)
        debug["raw_response"] = raw_text

        if raw_text is None:
            debug["pages"]         = self._fallback_all_pages(
                total_pages, "all Gemini attempts failed"
            )
            debug["fallback_used"] = True
            return debug

        parsed = _extract_json(raw_text)
        debug["parsed_json"] = parsed

        if parsed is None:
            debug["pages"]         = self._fallback_all_pages(
                total_pages, "JSON parse failed"
            )
            debug["fallback_used"] = True
            return debug

        raw_pages = parsed.get("line_items_pages", [])
        pages     = _validate_pages(raw_pages, total_pages)

        if not pages:
            debug["pages"]         = self._fallback_all_pages(
                total_pages, "empty or all-invalid page list"
            )
            debug["fallback_used"] = True
        else:
            debug["pages"] = pages

        return debug