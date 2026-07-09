# -*- coding: utf-8 -*-
"""
royal_tech_batch_extractor.py ? STEP 5: Controlled batch extraction via Gemini 2.5 Flash.

Key change from original batch_extractor.py
--------------------------------------------
? Schema fields (header_fields, item_fields) are no longer hardcoded.
  They are resolved at runtime from the dynamic DB schema dict passed in
  from royal_tech_processor.py via the plan or directly per extract() call.
? All helper functions live in royal_tech_batch_extractor_helpers.py.

Public API
----------
    extractor = RoyalBatchExtractor(schema=schema_dict)

    # Single batch:
    result = extractor.extract(batch, full_markdown, total_batches)

    # All batches in parallel (preferred):
    results = extractor.extract_all_batches(plan, full_markdown)
"""

from __future__ import annotations

import json
import logging
import re
import time
import concurrent.futures
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from src.services.royal.royal_tech_config import cfg
from src.services.royal.royal_tech_batch_manager import BatchSlice, BatchPlan
from src.services.royal.royal_tech_identifier_extractor import IdentifierRecord

from src.services.royal.royal_tech_batch_extractor_helpers import (
    _call_gemini,
    build_full_prompt,
    build_null_container,
    coerce_numeric_fields,
    cross_validate,
    extract_json,
    inject_null_and_default_header_fields,
    inject_null_item_fields,
    resolve_schema_fields,
)

logger = logging.getLogger(__name__)


# ============================================================================
# BatchResult dataclass
# ============================================================================

@dataclass
class BatchResult:
    """
    Output of RoyalBatchExtractor.extract() for one BatchSlice.

    Attributes
    ----------
    batch_index : int
        0-based batch index (mirrors BatchSlice.index).
    success : bool
        True if Gemini returned a parseable, schema-valid response.
    header : dict
        Extracted header fields (fully populated with nulls/defaults).
        Only batch 0 is used by merger.py.
    container_details : dict
        Shipment container details (all-null per schema).
    line_items : list[dict]
        Extracted line items for this batch only.
    missing_serials : list[str]
        Serials from the batch that produced no line item.
    extra_serials : list[str]
        Serials in the response not present in the batch (hallucinations).
    raw_response : str | None
        Raw Gemini text before parsing ? preserved for debug saves.
    error : str | None
        Human-readable error message when success is False.
    """

    batch_index: int
    success: bool
    header: dict = field(default_factory=dict)
    container_details: dict = field(default_factory=dict)
    line_items: list[dict] = field(default_factory=list)
    missing_serials: list[str] = field(default_factory=list)
    extra_serials: list[str] = field(default_factory=list)
    raw_response: Optional[str] = None
    error: Optional[str] = None

    def summary(self) -> dict:
        return {
            "batch_index":      self.batch_index,
            "success":          self.success,
            "line_items_count": len(self.line_items),
            "missing_serials":  self.missing_serials,
            "extra_serials":    self.extra_serials,
            "error":            self.error,
        }


# ============================================================================
# Main class
# ============================================================================

class RoyalBatchExtractor:
    """
    STEP 5 ? Sends one BatchSlice at a time to Gemini 2.5 Flash and returns
    a fully populated BatchResult.

    Schema is resolved once at construction time from the dynamic DB schema
    dict loaded by royal_tech_schema_loader.

    Usage
    -----
        extractor = RoyalBatchExtractor(schema=schema_dict)

        # All batches in parallel (preferred):
        results = extractor.extract_all_batches(plan, full_markdown)

        # Single batch:
        result = extractor.extract(batch, full_markdown, plan.total_batches)
    """

    def __init__(self, schema: dict[str, Any]) -> None:
        self._cfg_gemini = cfg.gemini
        self._cfg_step   = cfg.batch_extractor
        self._debug      = cfg.pipeline.debug_save_intermediate
        self._work_dir   = cfg.pipeline.work_dir
        self._schema     = schema

        # Resolve header/item field lists from DB schema (with fallback to defaults)
        self._header_fields, self._item_fields = resolve_schema_fields(schema)

        logger.info(
            "RoyalBatchExtractor initialised (model=%s, max_tokens=%d, "
            "header_fields=%d, item_fields=%d)",
            self._cfg_gemini.model,
            self._cfg_step.max_output_tokens,
            len(self._header_fields),
            len(self._item_fields),
        )

    # ------------------------------------------------------------------
    # Parallel public method  (STEP 5 entry point)
    # ------------------------------------------------------------------

    def extract_all_batches(
        self,
        plan: BatchPlan,
        full_markdown: str,
    ) -> list[BatchResult]:
        """
        Process ALL batches in parallel and return results ordered by batch index.

        Every batch's Gemini call is fired simultaneously via ThreadPoolExecutor.
        Results are re-sorted by batch_index so merger receives them in order.

        Returns
        -------
        list[BatchResult]
            Always length == plan.total_batches. Entries may have success=False.
        """
        if plan.is_empty:
            logger.warning("RoyalBatchExtractor.extract_all_batches: plan is empty")
            return []

        total_batches = plan.total_batches
        logger.info(
            "RoyalBatchExtractor: submitting %d batch(es) to Gemini IN PARALLEL...",
            total_batches,
        )

        start_time = time.time()
        results: list[BatchResult] = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=total_batches
        ) as executor:
            future_to_batch = {
                executor.submit(
                    self.extract, batch, full_markdown, total_batches
                ): batch
                for batch in plan.batches
            }

            for future in concurrent.futures.as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(
                        "RoyalBatchExtractor: batch %d/%d done ? "
                        "success=%s items=%d",
                        batch.batch_number, total_batches,
                        result.success, len(result.line_items),
                    )
                except Exception as exc:
                    logger.error(
                        "RoyalBatchExtractor: batch %d/%d raised exception: %s",
                        batch.batch_number, total_batches, exc,
                    )
                    results.append(BatchResult(
                        batch_index=batch.index,
                        success=False,
                        error=f"Unhandled exception in batch {batch.batch_number}: {exc}",
                    ))

        elapsed = time.time() - start_time
        results.sort(key=lambda r: r.batch_index)

        succeeded = sum(1 for r in results if r.success)
        failed    = total_batches - succeeded
        logger.info(
            "RoyalBatchExtractor: parallel complete in %.2fs ? "
            "%d succeeded, %d failed",
            elapsed, succeeded, failed,
        )
        for r in results:
            if not r.success:
                logger.error(
                    "  ? Batch index %d failed: %s", r.batch_index, r.error
                )

        return results

    # ------------------------------------------------------------------
    # Single-batch public method
    # ------------------------------------------------------------------

    def extract(
        self,
        batch: BatchSlice,
        full_markdown: str,
        total_batches: int,
    ) -> BatchResult:
        """
        Extract structured invoice data for the identifiers in batch.

        Parameters
        ----------
        batch : BatchSlice
            One slice from BatchPlan.batches.
        full_markdown : str
            Complete assembled OCR markdown (all pages).
        total_batches : int
            Used in prompt context ("Batch 2 of 5").

        Returns
        -------
        BatchResult  ? always returned; check .success before using.
        """
        if not full_markdown or not full_markdown.strip():
            return BatchResult(
                batch_index=batch.index, success=False,
                error="full_markdown is empty",
            )
        if batch.size == 0:
            return BatchResult(
                batch_index=batch.index, success=False,
                error="BatchSlice has no records",
            )

        logger.info(
            "RoyalBatchExtractor: batch %d/%d ? %d identifier(s) [serials %s?%s]",
            batch.batch_number, total_batches,
            batch.size, batch.serial_range[0], batch.serial_range[1],
        )

        prompt = build_full_prompt(
            batch, full_markdown, total_batches,
            self._header_fields, self._item_fields,
        )

        raw_text = self._call_with_retry(prompt, batch)
        if raw_text is None:
            return BatchResult(
                batch_index=batch.index, success=False,
                error=f"All Gemini attempts failed for batch {batch.batch_number}",
            )

        logger.debug(
            "RoyalBatchExtractor: raw response batch %d (%d chars):\n%s",
            batch.batch_number, len(raw_text), raw_text[:500],
        )

        parsed = extract_json(raw_text)
        if parsed is None:
            return BatchResult(
                batch_index=batch.index, success=False,
                raw_response=raw_text,
                error=f"JSON parse failed for batch {batch.batch_number}",
            )

        structure_error = self._validate_structure(parsed, batch)
        if structure_error:
            logger.warning(
                "RoyalBatchExtractor: batch %d structure invalid (%s) -- "
                "attempting structure recovery...",
                batch.batch_number, structure_error,
            )
            recovered = self._try_recover_structure(batch, full_markdown, total_batches)
            if recovered is not None:
                parsed = recovered
                logger.info(
                    "RoyalBatchExtractor: batch %d structure recovered successfully",
                    batch.batch_number,
                )
            else:
                return BatchResult(
                    batch_index=batch.index, success=False,
                    raw_response=raw_text, error=structure_error,
                )

        raw_header     = parsed.get("header",            {}) or {}
        raw_container  = parsed.get("container_details", {}) or {}
        raw_line_items = parsed.get("line_items",        []) or []

        header     = self._process_header(raw_header)
        container  = build_null_container()
        line_items = self._process_line_items(raw_line_items, batch)

        missing, extra = cross_validate(line_items, batch)

        if missing:
            logger.warning(
                "RoyalBatchExtractor: batch %d ? %d missing serial(s): %s",
                batch.batch_number, len(missing), missing,
            )
            line_items, missing, extra = self._retry_missing(
                batch, full_markdown, total_batches, missing, line_items, extra
            )

        if extra:
            logger.warning(
                "RoyalBatchExtractor: batch %d ? %d extra item(s): %s",
                batch.batch_number, len(extra), extra,
            )

        if self._debug:
            self._save_debug(batch, raw_text, parsed, header, container, line_items)

        result = BatchResult(
            batch_index=batch.index,
            success=True,
            header=header,
            container_details=container,
            line_items=line_items,
            missing_serials=missing,
            extra_serials=extra,
            raw_response=raw_text if self._debug else None,
        )
        logger.info(
            "RoyalBatchExtractor: batch %d complete ? %d item(s) "
            "(missing=%d, extra=%d)",
            batch.batch_number, len(line_items), len(missing), len(extra),
        )
        return result

    # ------------------------------------------------------------------
    # Post-processing
    # ------------------------------------------------------------------

    def _validate_structure(
        self, parsed: Any, batch: BatchSlice
    ) -> Optional[str]:
        if not isinstance(parsed, dict):
            return f"Gemini returned non-dict JSON (type={type(parsed).__name__})"
        if "line_items" not in parsed:
            return "Missing 'line_items' key in Gemini response"
        if not isinstance(parsed.get("line_items"), list):
            return "'line_items' is not a list"
        if "header" not in parsed:
            logger.warning(
                "RoyalBatchExtractor: batch %d 'header' key missing ? using empty dict",
                batch.batch_number,
            )
        return None

    def _try_recover_structure(
        self,
        batch: BatchSlice,
        full_markdown: str,
        total_batches: int,
    ) -> Optional[dict]:
        """
        Called when Gemini returned malformed JSON (missing/invalid line_items).
        Retries up to cfg.gemini.max_retries times with a stripped-down prompt
        that demands ONLY the line_items array.
        Returns a valid parsed dict on success, or None on failure.
        """
        serials_block = "\n".join(
            f"  {r.to_prompt_line()}" for r in batch.records
        )
        recovery_prompt = (
            "You previously returned malformed JSON missing the line_items list.\n"
            "Return ONLY valid JSON with this exact structure -- no commentary:\n"
            "{\n"
            '  "header": {},\n'
            '  "line_items": [\n'
            "    {\n"
            '      "serial": "NNNNN",\n'
            "      <all item fields as key: value>\n"
            "    }\n"
            "  ],\n"
            '  "container_details": {}\n'
            "}\n\n"
            f"You MUST include ALL {batch.size} line items for these identifiers:\n"
            f"{serials_block}\n\n"
            "Extract from the invoice markdown below.\n"
            + full_markdown[:8000]
        )

        gcfg = self._cfg_gemini
        for attempt in range(gcfg.max_retries):
            logger.info(
                "RoyalBatchExtractor: batch %d -- structure recovery attempt %d/%d",
                batch.batch_number, attempt + 1, gcfg.max_retries,
            )
            raw = _call_gemini(recovery_prompt, self._cfg_step.max_output_tokens)
            if raw is None:
                if attempt < gcfg.max_retries - 1:
                    time.sleep(gcfg.retry_backoff_base ** attempt)
                continue
            parsed = extract_json(raw)
            if parsed is None:
                continue
            err = self._validate_structure(parsed, batch)
            if err is None:
                logger.info(
                    "RoyalBatchExtractor: batch %d -- structure recovery succeeded "
                    "on attempt %d",
                    batch.batch_number, attempt + 1,
                )
                return parsed
            logger.warning(
                "RoyalBatchExtractor: batch %d -- recovery attempt %d still invalid: %s",
                batch.batch_number, attempt + 1, err,
            )
            if attempt < gcfg.max_retries - 1:
                time.sleep(gcfg.retry_backoff_base ** attempt)

        logger.error(
            "RoyalBatchExtractor: batch %d -- all structure recovery attempts failed",
            batch.batch_number,
        )
        return None

    def _process_header(self, raw_header: dict) -> dict:
        """Merge Gemini header output with always-null and always-default fields."""
        header = dict(raw_header)
        pp = header.get("PaymentPeriod")
        if pp and isinstance(pp, str):
            m = re.search(r"(\d+)", pp)
            if m:
                header["PaymentPeriod"] = m.group(1)
        return inject_null_and_default_header_fields(header, self._schema)

    def _process_line_items(
        self, raw_items: list, batch: BatchSlice
    ) -> list[dict]:
        """Validate, coerce, assign Itemslno, and inject null item fields."""
        if len(raw_items) > batch.size:
            logger.warning(
                "RoyalBatchExtractor: batch %d ? Gemini returned %d items, "
                "batch has %d; truncating",
                batch.batch_number, len(raw_items), batch.size,
            )
            raw_items = raw_items[: batch.size]

        processed: list[dict] = []
        for idx, raw_item in enumerate(raw_items):
            if not isinstance(raw_item, dict):
                logger.warning(
                    "RoyalBatchExtractor: batch %d item[%d] not a dict ? skipped",
                    batch.batch_number, idx,
                )
                continue
            item = coerce_numeric_fields(raw_item)
            item["Itemslno"] = (
                int(batch.records[idx].serial)
                if idx < len(batch.records)
                else idx + 1
            )
            item = inject_null_item_fields(item, self._schema)
            processed.append(item)

        return processed

    # ------------------------------------------------------------------
    # Missing-serial retry
    # ------------------------------------------------------------------

    def _retry_missing(
        self,
        batch: BatchSlice,
        full_markdown: str,
        total_batches: int,
        missing: list[str],
        line_items: list[dict],
        extra: list[str],
    ) -> tuple[list[dict], list[str], list[str]]:
        """
        Iteratively retry for missing serials up to cfg.gemini.max_retries
        times.  Each attempt focuses ONLY on the serials still missing at
        that point, merging any recovered items into the accumulating list.

        Strategy per attempt
        --------------------
        Attempt 1: full batch prompt + RETRY NOTE listing missing serials.
        Attempt 2+: minimal focused prompt asking only for the remaining
                    missing serials, with a tighter instruction to avoid
                    Gemini drifting back to already-extracted items.

        Merge logic
        -----------
        After each attempt we replace only the items for serials that were
        previously missing AND are now present in the retry response.
        Items for serials that were already in line_items are preserved
        untouched.  This means partial progress (e.g. 1-of-2 recovered)
        is accumulated across attempts.
        """
        gcfg       = self._cfg_gemini
        max_rounds = gcfg.max_retries

        current_items   = list(line_items)
        current_missing = list(missing)
        current_extra   = list(extra)

        # Build a serial -> item index map for fast replacement
        def _serial_map(items: list[dict]) -> dict[str, dict]:
            result = {}
            for it in items:
                sl = str(it.get("Itemslno", "")).zfill(5)
                result[sl] = it
            return result

        for attempt in range(1, max_rounds + 1):
            if not current_missing:
                break

            logger.info(
                "RoyalBatchExtractor: batch %d -- missing-serial retry "
                "attempt %d/%d for serials %s",
                batch.batch_number, attempt, max_rounds, current_missing,
            )

            missing_records = [
                r for r in batch.records if r.serial in current_missing
            ]
            missing_lines = "\n".join(
                r.to_prompt_line() for r in missing_records
            )

            if attempt == 1:
                # First retry: full prompt + appended note
                retry_note = (
                    f"\n\n[RETRY NOTE] Your previous response was missing "
                    f"line item(s) for the following identifier(s):\n"
                    f"{missing_lines}\n"
                    f"You MUST include ALL of them in your response.\n"
                )
                retry_prompt = (
                    build_full_prompt(
                        batch, full_markdown, total_batches,
                        self._header_fields, self._item_fields,
                    )
                    + retry_note
                )
            else:
                # Later retries: focused minimal prompt for remaining serials
                serials_block = "\n".join(
                    f"  {r.to_prompt_line()}" for r in missing_records
                )
                retry_prompt = (
                    f"[FOCUSED RETRY - attempt {attempt}]\n"
                    f"Extract ONLY the following {len(missing_records)} "
                    f"line item(s) from the invoice markdown.\n"
                    f"Do NOT repeat items already extracted in a prior response.\n"
                    f"Return ONLY valid JSON with this exact structure:\n"
                    "{\n"
                    '  "header": {},\n'
                    '  "line_items": [\n'
                    "    {<all item fields>}\n"
                    "  ],\n"
                    '  "container_details": {}\n'
                    "}\n\n"
                    f"You MUST include ALL {len(missing_records)} item(s) "
                    f"for these identifiers:\n"
                    f"{serials_block}\n\n"
                    "Invoice markdown:\n"
                    + full_markdown[:8000]
                )

            retry_text = self._call_with_retry(retry_prompt, batch)
            if not retry_text:
                logger.warning(
                    "RoyalBatchExtractor: batch %d -- retry attempt %d "
                    "Gemini call failed",
                    batch.batch_number, attempt,
                )
                if attempt < max_rounds:
                    time.sleep(gcfg.retry_backoff_base ** (attempt - 1))
                continue

            retry_parsed = extract_json(retry_text)
            if not retry_parsed:
                logger.warning(
                    "RoyalBatchExtractor: batch %d -- retry attempt %d "
                    "JSON parse failed",
                    batch.batch_number, attempt,
                )
                continue

            # Process only the items for the currently-missing serials
            retry_raw_items = retry_parsed.get("line_items", []) or []
            retry_items_all = self._process_line_items(retry_raw_items, batch)

            # Keep only items whose serial was in current_missing
            missing_set   = set(current_missing)
            recovered_map = {
                str(it.get("Itemslno", "")).zfill(5): it
                for it in retry_items_all
                if str(it.get("Itemslno", "")).zfill(5) in missing_set
            }

            if not recovered_map:
                logger.warning(
                    "RoyalBatchExtractor: batch %d -- retry attempt %d "
                    "returned no items for missing serials %s",
                    batch.batch_number, attempt, current_missing,
                )
                continue

            # Merge recovered items into current_items
            existing_map = _serial_map(current_items)
            existing_map.update(recovered_map)
            current_items = list(existing_map.values())

            # Recompute missing / extra against the full batch
            current_missing, current_extra = cross_validate(current_items, batch)

            logger.info(
                "RoyalBatchExtractor: batch %d -- retry attempt %d "
                "recovered %d serial(s), still missing: %s",
                batch.batch_number, attempt,
                len(recovered_map), current_missing,
            )

            if not current_missing:
                logger.info(
                    "RoyalBatchExtractor: batch %d -- all missing serials "
                    "recovered after attempt %d",
                    batch.batch_number, attempt,
                )
                break

            if attempt < max_rounds:
                time.sleep(gcfg.retry_backoff_base ** (attempt - 1))

        if current_missing:
            logger.warning(
                "RoyalBatchExtractor: batch %d -- %d serial(s) still "
                "missing after %d retry attempt(s): %s",
                batch.batch_number, len(current_missing),
                max_rounds, current_missing,
            )

        return current_items, current_missing, current_extra

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    def _call_with_retry(
        self, prompt: str, batch: BatchSlice
    ) -> Optional[str]:
        gcfg       = self._cfg_gemini
        max_tokens = self._cfg_step.max_output_tokens

        for attempt in range(gcfg.max_retries):
            logger.info(
                "RoyalBatchExtractor: batch %d ? Gemini attempt %d/%d",
                batch.batch_number, attempt + 1, gcfg.max_retries,
            )
            result = _call_gemini(prompt, max_tokens)
            if result is not None:
                return result
            if attempt < gcfg.max_retries - 1:
                wait = gcfg.retry_backoff_base ** attempt
                logger.warning(
                    "RoyalBatchExtractor: batch %d attempt %d failed ? "
                    "retrying in %.1fs",
                    batch.batch_number, attempt + 1, wait,
                )
                time.sleep(wait)

        logger.error(
            "RoyalBatchExtractor: batch %d ? all %d attempts exhausted",
            batch.batch_number, gcfg.max_retries,
        )
        return None

    # ------------------------------------------------------------------
    # Debug save
    # ------------------------------------------------------------------

    def _save_debug(
        self,
        batch: BatchSlice,
        raw_text: str,
        parsed: dict,
        header: dict,
        container: dict,
        line_items: list[dict],
    ) -> None:
        try:
            work_dir = Path(self._work_dir)
            work_dir.mkdir(parents=True, exist_ok=True)
            prefix = f"batch_{batch.batch_number:02d}"

            (work_dir / f"{prefix}_raw.txt").write_text(raw_text, encoding="utf-8")

            payload = {
                "batch_index":       batch.index,
                "batch_number":      batch.batch_number,
                "serial_range":      list(batch.serial_range),
                "header":            header,
                "container_details": container,
                "line_items":        line_items,
            }
            (work_dir / f"{prefix}_result.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info(
                "RoyalBatchExtractor [debug]: saved batch %d artefacts ? %s",
                batch.batch_number, work_dir,
            )
        except Exception as exc:
            logger.warning(
                "RoyalBatchExtractor [debug]: could not save batch %d ? %s",
                batch.batch_number, exc,
            )