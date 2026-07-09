# -*- coding: utf-8 -*-
"""
royal_tech_identifier_extractor.py � STEP 3: Per-page line-item identifier extraction.

Direct refactor of identifier_extractor.py:
  � config      ? royal_tech_config.cfg
  � All helpers moved to royal_tech_identifier_extractor_helpers.py
  � Class renamed RoyalIdentifierExtractor; original IdentifierRecord unchanged
  � All extraction logic, parallel strategy, and retry behaviour preserved exactly

Public API
----------
    extractor = RoyalIdentifierExtractor()

    # Preferred � all detected pages in parallel:
    all_records = extractor.extract_all_pages(
        page_markdown_map={2: "...", 3: "..."}
    )

    # Page-by-page with manual serial control:
    records_p2 = extractor.extract_page("...", page_num=2, serial_offset=0)
    records_p3 = extractor.extract_page("...", page_num=3, serial_offset=len(records_p2))
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
from dataclasses import dataclass
from typing import Optional

from src.services.royal.royal_tech_config import cfg
from src.services.royal.royal_tech_identifier_extractor_helpers import (
    _call_gemini,
    _extract_json,
    _parse_record_components,
    _sanitise_value_string,
    build_page_extract_prompt,
)

logger = logging.getLogger(__name__)


# ============================================================================
# IdentifierRecord dataclass
# ============================================================================

@dataclass
class IdentifierRecord:
    """
    A single unique composite identifier for one invoice line-item row.

    Attributes
    ----------
    serial : str
        Zero-padded global serial number, e.g. "00001".
    identifier_type : str
        Either cfg.identifier_extractor.identifier_type_material
        or  cfg.identifier_extractor.identifier_type_description.
    value : str
        Full composite string in canonical grammar:
        "material id SU26573 have qty 2 and amount 4.38"
        "description STEEL BOLT M6x20 have qty 100 and amount 350.00"
    raw_material_id : str | None
        Bare material_id / part-number (when identifier_type is material_id).
    raw_description : str | None
        Bare description text (when identifier_type is description).
    raw_qty : str
        Numeric qty string extracted from value.
    raw_amount : str
        Numeric amount string extracted from value.
    source_page : int
        1-indexed page number this identifier came from.
    """

    serial: str
    identifier_type: str
    value: str
    raw_material_id: Optional[str]
    raw_description: Optional[str]
    raw_qty: str
    raw_amount: str
    source_page: int

    def to_prompt_line(self) -> str:
        """One-line representation embedded in batch extraction prompts."""
        return f"[{self.serial}] {self.value}"

    def to_dict(self) -> dict:
        return {
            "serial":          self.serial,
            "type":            self.identifier_type,
            "value":           self.value,
            "raw_material_id": self.raw_material_id,
            "raw_description": self.raw_description,
            "raw_qty":         self.raw_qty,
            "raw_amount":      self.raw_amount,
            "source_page":     self.source_page,
        }


# ============================================================================
# RoyalIdentifierExtractor
# ============================================================================

class RoyalIdentifierExtractor:
    """
    STEP 3 � Extracts composite line-item identifiers from individual pages
    of the OCR markdown.

    Two-phase parallel strategy (identical to original identifier_extractor.py):

    Phase 1 � All page Gemini calls fired simultaneously via ThreadPoolExecutor.
              Each call uses serial_start=1 (placeholder); real serials assigned
              in Phase 2 once we know how many records each page produced.

    Phase 2 � Walk pages in sorted order, assign globally unique zero-padded
              serial numbers, build final IdentifierRecord list.
    """

    def __init__(self) -> None:
        self._cfg_gemini = cfg.gemini
        self._cfg_step   = cfg.identifier_extractor
        logger.info(
            "RoyalIdentifierExtractor initialised (model=%s, max_output_tokens=%d, "
            "zero_pad=%d, skip_unidentifiable=%s, mode=PARALLEL)",
            self._cfg_gemini.model,
            self._cfg_step.max_output_tokens,
            self._cfg_step.serial_zero_pad,
            self._cfg_step.skip_unidentifiable_rows,
        )

    # ------------------------------------------------------------------
    # Primary public methods
    # ------------------------------------------------------------------

    def extract_all_pages(
        self,
        page_markdown_map: dict[int, str],
    ) -> list[IdentifierRecord]:
        """
        Process every page in page_markdown_map IN PARALLEL and return the
        fully serialised master identifier list.

        Parameters
        ----------
        page_markdown_map : dict[int, str]
            Keys = 1-indexed page numbers from page_detector.
            Values = markdown content for that page only.

        Returns
        -------
        list[IdentifierRecord]
            Ordered by page then row within page.
            Serial numbers are globally unique and consecutive.
        """
        if not page_markdown_map:
            logger.warning(
                "RoyalIdentifierExtractor: page_markdown_map is empty � "
                "nothing to extract"
            )
            return []

        sorted_pages = sorted(page_markdown_map.keys())
        total_pages  = len(sorted_pages)

        logger.info(
            "RoyalIdentifierExtractor: submitting %d page(s) to Gemini "
            "IN PARALLEL: %s",
            total_pages, sorted_pages,
        )

        start_time = time.time()

        # Phase 1 � parallel Gemini calls; serials are placeholders
        page_raw_items: dict[int, list[dict]] = {}

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=total_pages
        ) as executor:
            future_to_page = {
                executor.submit(
                    self._extract_page_raw_items,
                    page_markdown_map[page_num],
                    page_num,
                ): page_num
                for page_num in sorted_pages
            }

            for future in concurrent.futures.as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    raw_items = future.result()
                    page_raw_items[page_num] = raw_items
                    logger.info(
                        "RoyalIdentifierExtractor: page %d done � "
                        "%d raw item(s)",
                        page_num, len(raw_items),
                    )
                except Exception as exc:
                    logger.error(
                        "RoyalIdentifierExtractor: page %d raised exception: "
                        "%s � treating as empty",
                        page_num, exc,
                    )
                    page_raw_items[page_num] = []

        elapsed = time.time() - start_time
        logger.info(
            "RoyalIdentifierExtractor: all %d parallel call(s) complete "
            "in %.2fs",
            total_pages, elapsed,
        )

        # Phase 2 � assign globally unique serials in page order
        all_records: list[IdentifierRecord] = []

        for page_num in sorted_pages:
            raw_items     = page_raw_items.get(page_num, [])
            serial_offset = len(all_records)

            page_records = self._build_records(
                raw_items=raw_items,
                page_num=page_num,
                serial_offset=serial_offset,
            )
            all_records.extend(page_records)

            logger.info(
                "RoyalIdentifierExtractor: page %d ? %d identifier(s) accepted "
                "| running total: %d",
                page_num, len(page_records), len(all_records),
            )

        logger.info(
            "RoyalIdentifierExtractor: master list complete � %d total identifier(s)",
            len(all_records),
        )

        for rec in all_records:
            logger.info(
                "RoyalIdentifierExtractor: [%s] page=%-2d type=%-12s value=%s",
                rec.serial, rec.source_page, rec.identifier_type, rec.value,
            )

        return all_records

    def extract_page(
        self,
        page_markdown: str,
        page_num: int,
        serial_offset: int = 0,
    ) -> list[IdentifierRecord]:
        """
        Extract line-item identifiers from a single page's markdown.

        Parameters
        ----------
        page_markdown : str
            Markdown for this page only.
        page_num : int
            1-indexed page number.
        serial_offset : int
            Global counter before this page's records begin.
            First record on this page gets serial serial_offset + 1.
        """
        raw_items = self._extract_page_raw_items(page_markdown, page_num)
        return self._build_records(
            raw_items=raw_items,
            page_num=page_num,
            serial_offset=serial_offset,
        )

    # ------------------------------------------------------------------
    # Internal: call Gemini for one page ? raw item dicts
    # ------------------------------------------------------------------

    def _extract_page_raw_items(
        self,
        page_markdown: str,
        page_num: int,
    ) -> list[dict]:
        """
        Send one page's markdown to Gemini and return the raw list of item
        dicts from the JSON response.

        Serial numbers in Gemini's response are IGNORED � they are re-assigned
        in Phase 2 of extract_all_pages() so they are globally correct.

        Returns an empty list on any failure.
        """
        if not page_markdown or not page_markdown.strip():
            logger.warning(
                "RoyalIdentifierExtractor: empty markdown for page %d � skipping",
                page_num,
            )
            return []

        logger.info(
            "RoyalIdentifierExtractor: extracting identifiers from page %d "
            "(markdown_chars=%d)",
            page_num, len(page_markdown),
        )

        prompt   = build_page_extract_prompt(page_markdown, page_num, serial_start=1)
        raw_text = self._call_with_retry(prompt, page_num)

        if raw_text is None:
            logger.error(
                "RoyalIdentifierExtractor: all Gemini attempts failed for page %d",
                page_num,
            )
            return []

        logger.debug(
            "RoyalIdentifierExtractor: raw response page %d:\n%s",
            page_num, raw_text[:400],
        )

        parsed = _extract_json(raw_text)
        if parsed is None:
            logger.error(
                "RoyalIdentifierExtractor: JSON parse failed for page %d � "
                "skipping page",
                page_num,
            )
            return []

        if not isinstance(parsed, dict):
            logger.error(
                "RoyalIdentifierExtractor: non-dict JSON for page %d: %s",
                page_num, type(parsed),
            )
            return []

        raw_items      = parsed.get("items", [])
        reported_count = parsed.get("line_items_count_this_page", 0)

        if not isinstance(raw_items, list):
            logger.error(
                "RoyalIdentifierExtractor: 'items' is not a list on page %d",
                page_num,
            )
            return []

        if reported_count == 0 or not raw_items:
            logger.info(
                "RoyalIdentifierExtractor: page %d has no main line-item rows",
                page_num,
            )
            return []

        logger.info(
            "RoyalIdentifierExtractor: page %d � Gemini reported %d item(s), "
            "%d in response",
            page_num, reported_count, len(raw_items),
        )
        return raw_items

    # ------------------------------------------------------------------
    # Record construction
    # ------------------------------------------------------------------

    def _build_records(
        self,
        raw_items: list,
        page_num: int,
        serial_offset: int,
    ) -> list[IdentifierRecord]:
        """
        Validate each raw Gemini item and build IdentifierRecord objects
        with globally unique zero-padded serial numbers.
        """
        pad                 = self._cfg_step.serial_zero_pad
        skip_unidentifiable = self._cfg_step.skip_unidentifiable_rows
        valid_types         = {
            self._cfg_step.identifier_type_material,
            self._cfg_step.identifier_type_description,
        }

        records: list[IdentifierRecord] = []
        local_serial = serial_offset  # incremented only on accepted items

        for idx, raw_item in enumerate(raw_items):
            if not isinstance(raw_item, dict):
                logger.warning(
                    "RoyalIdentifierExtractor: page %d item[%d] not a dict � skipped",
                    page_num, idx,
                )
                continue

            raw_type  = raw_item.get("type",  "")
            raw_value = raw_item.get("value", "")

            # Validate / infer type
            if raw_type not in valid_types:
                logger.warning(
                    "RoyalIdentifierExtractor: page %d item[%d] unknown type %r � "
                    "attempting to infer from value",
                    page_num, idx, raw_type,
                )
                if str(raw_value).lower().startswith("material id"):
                    raw_type = self._cfg_step.identifier_type_material
                elif str(raw_value).lower().startswith("description"):
                    raw_type = self._cfg_step.identifier_type_description
                elif skip_unidentifiable:
                    logger.warning(
                        "RoyalIdentifierExtractor: page %d item[%d] � cannot "
                        "infer type, skipping",
                        page_num, idx,
                    )
                    continue

            # Sanitise value string ? canonical grammar
            canonical_value = _sanitise_value_string(raw_value)

            if canonical_value is None:
                if skip_unidentifiable:
                    logger.warning(
                        "RoyalIdentifierExtractor: page %d item[%d] value %r "
                        "cannot be sanitised � skipped",
                        page_num, idx, str(raw_value)[:80],
                    )
                    continue
                else:
                    canonical_value = str(raw_value).strip()
                    logger.warning(
                        "RoyalIdentifierExtractor: page %d item[%d] � using "
                        "raw value without sanitisation: %r",
                        page_num, idx, canonical_value[:80],
                    )

            # Parse components
            id_type, raw_mid, raw_desc, raw_qty, raw_amt = (
                _parse_record_components(canonical_value)
            )

            # Assign globally unique serial
            local_serial += 1
            serial_str    = str(local_serial).zfill(pad)

            record = IdentifierRecord(
                serial=serial_str,
                identifier_type=id_type,
                value=canonical_value,
                raw_material_id=raw_mid,
                raw_description=raw_desc,
                raw_qty=raw_qty,
                raw_amount=raw_amt,
                source_page=page_num,
            )
            records.append(record)

            logger.debug(
                "RoyalIdentifierExtractor: accepted [%s] page=%d type=%s value=%r",
                serial_str, page_num, id_type, canonical_value[:80],
            )

        return records

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    def _call_with_retry(self, prompt: str, page_num: int) -> Optional[str]:
        """
        Retry _call_gemini up to cfg.gemini.max_retries times with
        exponential back-off (retry_backoff_base ** attempt seconds).
        """
        gcfg       = self._cfg_gemini
        max_tokens = self._cfg_step.max_output_tokens

        for attempt in range(gcfg.max_retries):
            logger.info(
                "RoyalIdentifierExtractor: page %d � Gemini attempt %d/%d",
                page_num, attempt + 1, gcfg.max_retries,
            )
            result = _call_gemini(prompt, max_tokens)
            if result is not None:
                return result

            if attempt < gcfg.max_retries - 1:
                wait = gcfg.retry_backoff_base ** attempt
                logger.warning(
                    "RoyalIdentifierExtractor: page %d attempt %d failed � "
                    "retrying in %.1fs",
                    page_num, attempt + 1, wait,
                )
                time.sleep(wait)

        logger.error(
            "RoyalIdentifierExtractor: page %d � all %d attempts exhausted",
            page_num, gcfg.max_retries,
        )
        return None

    # ------------------------------------------------------------------
    # Duplicate audit (called by pipeline after all pages complete)
    # ------------------------------------------------------------------

    def audit_duplicates(
        self, records: list[IdentifierRecord]
    ) -> dict[str, list[str]]:
        """
        Scan the master identifier list for duplicate composite value strings.

        Does NOT remove duplicates � reports them for pipeline logging.
        Genuine duplicates (same material_id + qty + amount on different pages)
        are expected in some invoice formats.

        Returns
        -------
        dict[str, list[str]]
            Maps duplicate value ? list of serials.  Empty = no duplicates.
        """
        seen: dict[str, list[str]] = {}
        for rec in records:
            seen.setdefault(rec.value, []).append(rec.serial)

        duplicates = {v: s for v, s in seen.items() if len(s) > 1}

        if duplicates:
            logger.warning(
                "RoyalIdentifierExtractor.audit_duplicates: %d duplicate(s)",
                len(duplicates),
            )
            for val, serials in duplicates.items():
                logger.warning("  Duplicate: %r ? serials %s", val[:80], serials)
        else:
            logger.info(
                "RoyalIdentifierExtractor.audit_duplicates: no duplicates in "
                "%d identifier(s)",
                len(records),
            )

        return duplicates

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def records_to_dict_list(records: list[IdentifierRecord]) -> list[dict]:
        """Convert list[IdentifierRecord] ? list[dict]."""
        return [r.to_dict() for r in records]

    @staticmethod
    def records_to_prompt_lines(records: list[IdentifierRecord]) -> list[str]:
        """Return one prompt-ready line per record.
        e.g. ["[00001] material id SU26573 have qty 2 and amount 4.38", ...]
        """
        return [r.to_prompt_line() for r in records]