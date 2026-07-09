# -*- coding: utf-8 -*-
"""
royal_tech_batch_manager.py � STEP 4: Group identifiers into extraction batches.

Direct refactor of batch_manager.py:
  � config  ? royal_tech_config.cfg
  � IdentifierRecord imported from royal_tech_identifier_extractor
  � All logic, dataclasses, and method signatures unchanged.

Public API
----------
    manager = RoyalBatchManager()
    plan    = manager.create_plan(records: list[IdentifierRecord]) -> BatchPlan

    for batch in plan.batches:
        batch.index          # 0-based
        batch.records        # list[IdentifierRecord]
        batch.prompt_lines   # pre-formatted for Gemini prompt
        batch.serial_range   # (first_serial, last_serial)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from src.services.royal.royal_tech_config import cfg
from src.services.royal.royal_tech_identifier_extractor import IdentifierRecord

logger = logging.getLogger(__name__)


# ============================================================================
# BatchSlice
# ============================================================================

@dataclass
class BatchSlice:
    """
    One batch of IdentifierRecords to be sent to royal_tech_batch_extractor.

    Attributes
    ----------
    index : int
        0-based position.  Batch 0 is authoritative for Header / container.
    records : list[IdentifierRecord]
    prompt_lines : list[str]
        Pre-formatted prompt lines � one per record.
    serial_range : tuple[str, str]
        (first_serial, last_serial) for logging and prompt context.
    is_single_shot : bool
        True when the entire master list fits in one batch.
    """

    index: int
    records: list[IdentifierRecord]
    prompt_lines: list[str]
    serial_range: tuple[str, str]
    is_single_shot: bool = False

    @property
    def batch_number(self) -> int:
        """1-based batch number for human-readable logging."""
        return self.index + 1

    @property
    def size(self) -> int:
        return len(self.records)

    @property
    def serials(self) -> list[str]:
        return [r.serial for r in self.records]

    @property
    def source_pages(self) -> list[int]:
        return sorted(set(r.source_page for r in self.records))

    def to_dict(self) -> dict:
        return {
            "batch_index":    self.index,
            "batch_number":   self.batch_number,
            "size":           self.size,
            "serial_range":   list(self.serial_range),
            "serials":        self.serials,
            "source_pages":   self.source_pages,
            "is_single_shot": self.is_single_shot,
            "prompt_lines":   self.prompt_lines,
        }


# ============================================================================
# BatchPlan
# ============================================================================

@dataclass
class BatchPlan:
    """
    The complete batching plan produced by RoyalBatchManager.create_plan().

    Attributes
    ----------
    total_identifiers : int
    total_batches : int
    batch_size_used : int
        The batch_size actually applied (config default or single-shot size).
    single_shot_triggered : bool
        True when total_identifiers = cfg.batch_manager.single_shot_threshold.
    batches : list[BatchSlice]
        Ordered by batch index.  Always non-empty when total_identifiers > 0.
    """

    total_identifiers: int
    total_batches: int
    batch_size_used: int
    single_shot_triggered: bool
    batches: list[BatchSlice] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return self.total_identifiers == 0

    def get_batch(self, index: int) -> Optional[BatchSlice]:
        if 0 <= index < len(self.batches):
            return self.batches[index]
        return None

    def summary(self) -> dict:
        return {
            "total_identifiers":    self.total_identifiers,
            "total_batches":        self.total_batches,
            "batch_size_used":      self.batch_size_used,
            "single_shot_triggered": self.single_shot_triggered,
            "batches": [
                {
                    "batch_number": b.batch_number,
                    "size":         b.size,
                    "serial_range": list(b.serial_range),
                    "source_pages": b.source_pages,
                }
                for b in self.batches
            ],
        }

    def log_plan(self) -> None:
        logger.info(
            "BatchPlan: %d identifier(s) ? %d batch(es) "
            "[batch_size=%d, single_shot=%s]",
            self.total_identifiers, self.total_batches,
            self.batch_size_used, self.single_shot_triggered,
        )
        for b in self.batches:
            logger.info(
                "  Batch %d/%d: %d item(s) | serials %s?%s | pages %s",
                b.batch_number, self.total_batches,
                b.size, b.serial_range[0], b.serial_range[1],
                b.source_pages,
            )


# ============================================================================
# RoyalBatchManager
# ============================================================================

class RoyalBatchManager:
    """
    STEP 4 � Slices the master identifier list into BatchSlice objects
    ready for royal_tech_batch_extractor (STEP 5).

    Pure, deterministic data-partitioning � no Gemini calls.

    Usage
    -----
        manager = RoyalBatchManager()
        plan    = manager.create_plan(all_records)
        plan.log_plan()

        for batch in plan.batches:
            result = extractor.extract(batch, full_markdown)
    """

    def __init__(self) -> None:
        bcfg = cfg.batch_manager
        self._batch_size: int            = bcfg.batch_size
        self._single_shot_threshold: int = bcfg.single_shot_threshold
        self._debug: bool                = cfg.pipeline.debug_save_intermediate
        self._work_dir: str              = cfg.pipeline.work_dir

        if self._batch_size < 1:
            raise ValueError(
                f"BatchManagerConfig.batch_size must be >= 1, got {self._batch_size}"
            )

        logger.info(
            "RoyalBatchManager initialised (batch_size=%d, single_shot_threshold=%d)",
            self._batch_size, self._single_shot_threshold,
        )

    # ------------------------------------------------------------------
    # Primary public method
    # ------------------------------------------------------------------

    def create_plan(self, records: list[IdentifierRecord]) -> BatchPlan:
        """
        Partition records into an ordered list of BatchSlice objects.

        Parameters
        ----------
        records : list[IdentifierRecord]
            Complete master identifier list � already in serial order.

        Returns
        -------
        BatchPlan  (plan.is_empty == True when records is empty)
        """
        total = len(records)

        if total == 0:
            logger.warning("RoyalBatchManager.create_plan: received empty records list")
            return BatchPlan(
                total_identifiers=0,
                total_batches=0,
                batch_size_used=self._batch_size,
                single_shot_triggered=False,
                batches=[],
            )

        single_shot = (
            self._single_shot_threshold > 0
            and total <= self._single_shot_threshold
        ) or total <= self._batch_size

        if single_shot:
            logger.info(
                "RoyalBatchManager: single-shot mode � %d identifier(s) in one batch "
                "(threshold=%d, batch_size=%d)",
                total, self._single_shot_threshold, self._batch_size,
            )
            batch_size_used = total
            slices = [self._make_slice(0, records, is_single_shot=True)]
        else:
            batch_size_used = self._batch_size
            slices = self._slice_records(records)

        plan = BatchPlan(
            total_identifiers=total,
            total_batches=len(slices),
            batch_size_used=batch_size_used,
            single_shot_triggered=single_shot,
            batches=slices,
        )

        plan.log_plan()

        if self._debug:
            self._save_debug(plan)

        return plan

    # ------------------------------------------------------------------
    # Slicing internals
    # ------------------------------------------------------------------

    def _slice_records(self, records: list[IdentifierRecord]) -> list[BatchSlice]:
        """
        Divide records into consecutive non-overlapping chunks of
        size self._batch_size.  Last chunk may be smaller.
        """
        slices: list[BatchSlice] = []
        total = len(records)

        for batch_index, start in enumerate(range(0, total, self._batch_size)):
            end   = min(start + self._batch_size, total)
            chunk = records[start:end]
            slices.append(self._make_slice(batch_index, chunk, is_single_shot=False))

        logger.debug(
            "RoyalBatchManager._slice_records: %d record(s) ? %d batch(es) of =%d",
            total, len(slices), self._batch_size,
        )
        return slices

    def _make_slice(
        self,
        index: int,
        records: list[IdentifierRecord],
        is_single_shot: bool,
    ) -> BatchSlice:
        """Build a BatchSlice from a chunk of IdentifierRecord objects."""
        prompt_lines = [r.to_prompt_line() for r in records]
        first_serial = records[0].serial  if records else "00000"
        last_serial  = records[-1].serial if records else "00000"

        return BatchSlice(
            index=index,
            records=records,
            prompt_lines=prompt_lines,
            serial_range=(first_serial, last_serial),
            is_single_shot=is_single_shot,
        )

    # ------------------------------------------------------------------
    # Runtime batch-size override
    # ------------------------------------------------------------------

    def create_plan_with_size(
        self,
        records: list[IdentifierRecord],
        batch_size: int,
    ) -> BatchPlan:
        """
        Same as create_plan() but uses batch_size instead of the configured
        default.  Useful for adaptive retry with smaller batches on token-overflow.
        Original batch_size is always restored after the call.
        """
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")

        original = self._batch_size
        self._batch_size = batch_size

        logger.info(
            "RoyalBatchManager.create_plan_with_size: temporary batch_size=%d "
            "(config default=%d)",
            batch_size, original,
        )
        try:
            plan = self.create_plan(records)
        finally:
            self._batch_size = original

        return plan

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_plan(
        self, plan: BatchPlan, records: list[IdentifierRecord]
    ) -> bool:
        """
        Sanity-check that the plan covers all records exactly once.

        Checks:
        1. Total items across all batches == len(records).
        2. No serial appears in more than one batch (no duplicates, no missing).
        3. Batch ordering is preserved (serials strictly increasing across batches).

        Returns True if valid; logs errors and returns False on any violation.
        """
        if plan.is_empty and not records:
            return True

        errors: list[str] = []

        plan_total = sum(b.size for b in plan.batches)
        if plan_total != len(records):
            errors.append(
                f"Total items mismatch: plan={plan_total}, records={len(records)}"
            )

        expected_serials = {r.serial for r in records}
        plan_serials: list[str] = [s for b in plan.batches for s in b.serials]
        plan_serial_set = set(plan_serials)

        missing = expected_serials - plan_serial_set
        extra   = plan_serial_set  - expected_serials

        if missing:
            errors.append(f"Missing serials in plan: {sorted(missing)}")
        if extra:
            errors.append(f"Extra serials not in records: {sorted(extra)}")

        for i in range(len(plan.batches) - 1):
            last_cur  = plan.batches[i].serials[-1]
            first_nxt = plan.batches[i + 1].serials[0]
            if last_cur >= first_nxt:
                errors.append(
                    f"Ordering violation: batch {i+1} ends at {last_cur} "
                    f"but batch {i+2} starts at {first_nxt}"
                )

        if errors:
            for err in errors:
                logger.error("RoyalBatchManager.validate_plan: %s", err)
            return False

        logger.info(
            "RoyalBatchManager.validate_plan: OK � %d batch(es), %d identifier(s)",
            plan.total_batches, plan.total_identifiers,
        )
        return True

    # ------------------------------------------------------------------
    # Debug save
    # ------------------------------------------------------------------

    def _save_debug(self, plan: BatchPlan) -> None:
        try:
            work_dir = Path(self._work_dir)
            work_dir.mkdir(parents=True, exist_ok=True)
            out_path = work_dir / "batch_plan.json"
            out_path.write_text(
                json.dumps(plan.summary(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info("RoyalBatchManager [debug]: batch plan saved ? %s", out_path)
        except Exception as exc:
            logger.warning(
                "RoyalBatchManager [debug]: could not save batch plan � %s", exc
            )