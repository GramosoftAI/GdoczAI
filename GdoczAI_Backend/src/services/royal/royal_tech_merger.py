# -*- coding: utf-8 -*-
"""
royal_tech_merger.py ? STEP 6: Merge all BatchResults into the final unified JSON.

Direct refactor of merger.py:
  ? config      ? royal_tech_config.cfg
  ? BatchResult imported from royal_tech_batch_extractor
  ? Schema field lists (null/defaults/manifests) loaded from cfg at import time
    with the same module-level singleton pattern as the original
  ? All logic, dataclasses, and method signatures preserved exactly

Public API
----------
    merger = RoyalMerger()
    final  = merger.merge(batch_results: list[BatchResult]) -> dict | None
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from src.services.royal.royal_tech_config import cfg
from src.services.royal.royal_tech_batch_extractor import BatchResult

logger = logging.getLogger(__name__)


# ============================================================================
# Complete field manifests (Document 2 schema)
# ============================================================================

_ALL_HEADER_FIELDS: tuple[str, ...] = (
    "Branch", "BranchCode", "BuyerAdd1", "BuyerAdd2", "BuyerAdd3",
    "BuyerName", "BuyerOrderNo", "CartonNo", "Client", "Commision",
    "CommonItem", "CompanyID", "ConsigneeAdd1", "ConsigneeAdd2",
    "ConsigneeAdd3", "ConsigneeAdd4", "ConsigneeName", "CountryOfDischarge",
    "Currency", "Discount", "ExporterName", "Freight", "IEC", "Insurance",
    "InvoiceDate", "InvoiceNo", "InvoiceResponseTime", "InvoiceStartTime",
    "InvoiceValue", "JobNo", "JobStatus", "JobType", "KimballNo",
    "NotifyAddress1", "NotifyAddress2", "NotifyAddress3", "NotifyPartyName",
    "OrderNo", "OtherDed", "OtherReference", "Packingcharges", "PageCount",
    "PaymentPeriod", "PdfClientName", "PdfCount", "PortOfDischarge",
    "PortOfFinalDestination", "QtyCode", "SchemeCode", "Status", "Terms",
    "TermsOfPayment", "TotalCBM", "TotalCarton", "TotalGrossWeight",
    "TotalNetWeight", "UserID", "WorkingPeriod",
)

_ALL_ITEM_FIELDS: tuple[str, ...] = (
    "Amount", "DBK_SL_NO", "ExtraItemDesc", "ExtraQuantity",
    "HSNCode", "IGSTAmount", "IGSTRate", "InfoQty", "InfoUnitPrice",
    "ItemDesc", "ItemQTYCode", "Itemslno", "NetWeight",
    "Quantity", "Rate", "TaxableAmount",
)

_ALL_CONTAINER_FIELDS: tuple[str, ...] = (
    "ContainerNo", "ContainerSealDate", "ContainerSealNo",
    "ContainerSize", "Containerslno",
)

# Loaded from cfg.schema (SchemaConfig) -- populated from yaml schema: section
_HEADER_FORCE_NULL: frozenset[str] = frozenset(cfg.schema.header_always_null_fields)
_ITEM_FORCE_NULL:   frozenset[str] = frozenset(cfg.schema.items_always_null_fields)
_HEADER_DEFAULTS:   dict[str, Any] = dict(cfg.schema.header_default_fields)


# ============================================================================
# MergeReport  (internal diagnostics ? not returned to caller)
# ============================================================================

@dataclass
class MergeReport:
    """Internal diagnostics produced during merge. Attached to log output."""

    primary_batch_index: int = -1
    primary_batch_was_fallback: bool = False
    successful_batch_indices: list[int] = field(default_factory=list)
    failed_batch_indices: list[int] = field(default_factory=list)
    total_line_items_before_dedup: int = 0
    total_line_items_after_dedup: int = 0
    duplicates_removed: int = 0
    renumbered_items: bool = False
    header_field_gaps: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def log(self) -> None:
        logger.info("MergeReport:")
        logger.info(
            "  Primary batch : %d%s",
            self.primary_batch_index,
            " (fallback)" if self.primary_batch_was_fallback else "",
        )
        logger.info("  Successful    : %s", self.successful_batch_indices)
        logger.info("  Failed        : %s", self.failed_batch_indices)
        logger.info("  Items pre-dedup: %d", self.total_line_items_before_dedup)
        logger.info("  Duplicates removed: %d", self.duplicates_removed)
        logger.info("  Items final   : %d", self.total_line_items_after_dedup)
        if self.header_field_gaps:
            logger.warning("  Null header fields: %s", self.header_field_gaps)
        for w in self.warnings:
            logger.warning("  ? %s", w)

    def to_dict(self) -> dict:
        return {
            "primary_batch_index":          self.primary_batch_index,
            "primary_batch_was_fallback":   self.primary_batch_was_fallback,
            "successful_batch_indices":     self.successful_batch_indices,
            "failed_batch_indices":         self.failed_batch_indices,
            "total_line_items_before_dedup": self.total_line_items_before_dedup,
            "duplicates_removed":           self.duplicates_removed,
            "total_line_items_after_dedup": self.total_line_items_after_dedup,
            "renumbered_items":             self.renumbered_items,
            "header_field_gaps":            self.header_field_gaps,
            "warnings":                     self.warnings,
        }


# ============================================================================
# Schema enforcement helpers
# ============================================================================

def _enforce_header(raw: dict, report: MergeReport) -> dict:
    """
    Produce a header dict that:
    ? Contains exactly the fields in _ALL_HEADER_FIELDS.
    ? Forces always-null fields to None regardless of Gemini's value.
    ? Applies always-default values (CartonNo="1", Status="Success", etc.).
    ? Fills absent fields with None.
    ? Reports null fields to MergeReport for visibility.
    """
    out: dict = {}

    for fname in _ALL_HEADER_FIELDS:
        if fname in _HEADER_FORCE_NULL:
            out[fname] = None
        elif fname in _HEADER_DEFAULTS:
            out[fname] = _HEADER_DEFAULTS[fname]
        else:
            out[fname] = raw.get(fname)

    gaps = [
        f for f in _ALL_HEADER_FIELDS
        if out.get(f) is None
        and f not in _HEADER_FORCE_NULL
        and f not in _HEADER_DEFAULTS
    ]
    if gaps:
        report.header_field_gaps.extend(gaps)

    return out


def _enforce_item(raw: dict, new_itemslno: int) -> dict:
    """
    Produce a line-item dict that:
    ? Contains exactly the fields in _ALL_ITEM_FIELDS.
    ? Forces always-null item fields to None.
    ? Sets Itemslno to new_itemslno (re-numbered by merger).
    ? Fills absent fields with None.
    """
    out: dict = {}
    for fname in _ALL_ITEM_FIELDS:
        if fname in _ITEM_FORCE_NULL:
            out[fname] = None
        elif fname == "Itemslno":
            out[fname] = new_itemslno
        else:
            out[fname] = raw.get(fname)
    return out


def _enforce_container(raw: dict) -> dict:
    """All container fields are always null per Document 2 schema."""
    return {f: None for f in _ALL_CONTAINER_FIELDS}


# ============================================================================
# Deduplication helpers
# ============================================================================

def _item_identity_key(item: dict) -> tuple:
    """
    Hashable identity key for a line item.
    Uses (Quantity, Amount, HSNCode, Rate) ? same logic as original merger.
    """
    return (
        str(item.get("Quantity", "") or ""),
        str(item.get("Amount",   "") or ""),
        str(item.get("HSNCode",  "") or ""),
        str(item.get("Rate",     "") or ""),
    )


def _deduplicate_items(
    items: list[dict], report: MergeReport
) -> list[dict]:
    """Remove duplicate line items (first-seen wins)."""
    seen: dict[tuple, int] = {}
    unique: list[dict] = []

    for item in items:
        key = _item_identity_key(item)
        if key not in seen:
            seen[key] = len(unique)
            unique.append(item)
        else:
            report.duplicates_removed += 1
            logger.debug(
                "RoyalMerger: duplicate item removed "
                "(qty=%s, amount=%s, hsn=%s)",
                item.get("Quantity"), item.get("Amount"), item.get("HSNCode"),
            )

    return unique


# ============================================================================
# Primary batch selection
# ============================================================================

def _select_primary_batch(
    batch_results: list[BatchResult],
    preferred_index: int,
    report: MergeReport,
) -> Optional[BatchResult]:
    """
    Return the BatchResult to use as source for Header and
    ShipmentContainerDetails.

    Priority:
    1. batch_results[preferred_index] if it succeeded.
    2. First successful batch (fallback).
    3. None if no successful batch exists.
    """
    for r in batch_results:
        if r.batch_index == preferred_index and r.success:
            report.primary_batch_index = preferred_index
            return r

    for r in sorted(batch_results, key=lambda x: x.batch_index):
        if r.success:
            report.primary_batch_index = r.batch_index
            report.primary_batch_was_fallback = True
            logger.warning(
                "RoyalMerger: primary batch %d failed ? falling back to batch %d",
                preferred_index, r.batch_index,
            )
            return r

    logger.error("RoyalMerger: no successful batch ? cannot produce header")
    return None


# ============================================================================
# Main class
# ============================================================================

class RoyalMerger:
    """
    STEP 6 ? Combines all BatchResult objects into the final unified JSON
    matching the Document 2 schema exactly.

    Usage
    -----
        merger = RoyalMerger()
        final  = merger.merge(batch_results)

        if final is None:
            # Fatal ? no usable data from any batch
        else:
            print(json.dumps(final, indent=2))
    """

    def __init__(self) -> None:
        self._cfg      = cfg.merger
        self._debug    = cfg.pipeline.debug_save_intermediate
        self._work_dir = cfg.pipeline.work_dir

        logger.info(
            "RoyalMerger initialised (primary_batch=%d, dedup=%s)",
            self._cfg.primary_batch_index,
            self._cfg.deduplicate_on_exact_identifier,
        )

    # ------------------------------------------------------------------
    # Primary public method
    # ------------------------------------------------------------------

    def merge(self, batch_results: list[BatchResult]) -> Optional[dict]:
        """
        Merge all BatchResult objects into the final invoice JSON.

        Parameters
        ----------
        batch_results : list[BatchResult]
            Ordered list from royal_tech_batch_extractor.
            May contain failed results (result.success == False).
            Must have at least one successful result.

        Returns
        -------
        dict | None  ? None only when no batch succeeded at all.
        """
        if not batch_results:
            logger.error("RoyalMerger.merge: received empty batch_results list")
            return None

        report = MergeReport()

        for r in batch_results:
            if r.success:
                report.successful_batch_indices.append(r.batch_index)
            else:
                report.failed_batch_indices.append(r.batch_index)

        logger.info(
            "RoyalMerger: %d successful / %d failed batch(es)",
            len(report.successful_batch_indices),
            len(report.failed_batch_indices),
        )

        if not report.successful_batch_indices:
            logger.error("RoyalMerger: all batches failed ? cannot produce output")
            return None

        primary = _select_primary_batch(
            batch_results,
            preferred_index=self._cfg.primary_batch_index,
            report=report,
        )
        if primary is None:
            return None

        header    = _enforce_header(primary.header, report)
        container = _enforce_container(primary.container_details)

        raw_items = self._collect_line_items(batch_results, report)

        if not raw_items:
            report.warnings.append(
                "No line items collected from any batch ? ItemsDetails will be empty"
            )
            logger.warning("RoyalMerger: no line items in any successful batch")

        report.total_line_items_before_dedup = len(raw_items)

        if self._cfg.deduplicate_on_exact_identifier and raw_items:
            raw_items = _deduplicate_items(raw_items, report)
            if report.duplicates_removed:
                logger.info(
                    "RoyalMerger: deduplication removed %d item(s)",
                    report.duplicates_removed,
                )

        report.total_line_items_after_dedup = len(raw_items)

        final_items = self._enforce_items(raw_items, report)

        final_output = {
            "Header":                   header,
            "ItemsDetails":             final_items,
            "ShipmentContainerDetails": container,
        }

        report.log()

        if self._debug:
            self._save_debug(final_output, report)

        logger.info(
            "RoyalMerger complete ? %d line item(s), %d header field(s) null, "
            "%d batch(es) failed",
            len(final_items),
            len(report.header_field_gaps),
            len(report.failed_batch_indices),
        )

        return final_output

    # ------------------------------------------------------------------
    # Line item collection
    # ------------------------------------------------------------------

    def _collect_line_items(
        self,
        batch_results: list[BatchResult],
        report: MergeReport,
    ) -> list[dict]:
        """Concatenate line_items from all successful batches in index order."""
        all_items: list[dict] = []

        for r in sorted(batch_results, key=lambda x: x.batch_index):
            if not r.success:
                report.warnings.append(
                    f"Batch {r.batch_index} skipped (failed): "
                    f"{len(r.missing_serials)} item(s) absent from output"
                )
                logger.warning(
                    "RoyalMerger: skipping failed batch %d ? %s",
                    r.batch_index, r.error or "unknown error",
                )
                continue

            if not r.line_items:
                logger.warning(
                    "RoyalMerger: successful batch %d has no line items",
                    r.batch_index,
                )
                report.warnings.append(
                    f"Batch {r.batch_index} succeeded but returned 0 line items"
                )
                continue

            logger.debug(
                "RoyalMerger: appending %d item(s) from batch %d",
                len(r.line_items), r.batch_index,
            )
            all_items.extend(r.line_items)

        return all_items

    # ------------------------------------------------------------------
    # Item schema enforcement + re-numbering
    # ------------------------------------------------------------------

    def _enforce_items(
        self,
        raw_items: list[dict],
        report: MergeReport,
    ) -> list[dict]:
        """Apply _enforce_item() to every raw item with sequential Itemslno."""
        final: list[dict] = []

        for new_slno, raw_item in enumerate(raw_items, start=1):
            if not isinstance(raw_item, dict):
                logger.warning(
                    "RoyalMerger: skipping non-dict item at position %d", new_slno
                )
                report.warnings.append(
                    f"Non-dict item at merged position {new_slno} ? skipped"
                )
                continue
            final.append(_enforce_item(raw_item, new_itemslno=new_slno))

        report.renumbered_items = True
        return final

    # ------------------------------------------------------------------
    # Debug save
    # ------------------------------------------------------------------

    def _save_debug(self, final_output: dict, report: MergeReport) -> None:
        try:
            work_dir = Path(self._work_dir)
            work_dir.mkdir(parents=True, exist_ok=True)

            (work_dir / "merged_output.json").write_text(
                json.dumps(final_output, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (work_dir / "merge_report.json").write_text(
                json.dumps(report.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info("RoyalMerger [debug]: artefacts saved ? %s", work_dir)
        except Exception as exc:
            logger.warning("RoyalMerger [debug]: could not save debug files ? %s", exc)

    # ------------------------------------------------------------------
    # Output validation (convenience)
    # ------------------------------------------------------------------

    def validate_output(self, final_output: dict) -> list[str]:
        """
        Lightweight structural validation of the merged output.

        Returns list[str] of error strings ? empty means valid.
        """
        errors: list[str] = []

        for key in ("Header", "ItemsDetails", "ShipmentContainerDetails"):
            if key not in final_output:
                errors.append(f"Missing top-level key: '{key}'")
        if errors:
            return errors

        header = final_output.get("Header", {})
        if not isinstance(header, dict):
            errors.append(f"'Header' must be dict, got {type(header).__name__}")
        else:
            for fname in _ALL_HEADER_FIELDS:
                if fname not in header:
                    errors.append(f"Header missing field: '{fname}'")

        items = final_output.get("ItemsDetails")
        if not isinstance(items, list):
            errors.append(f"'ItemsDetails' must be list, got {type(items).__name__}")
        else:
            for i, item in enumerate(items):
                if not isinstance(item, dict):
                    errors.append(f"ItemsDetails[{i}] is not a dict")
                    continue
                for fname in _ALL_ITEM_FIELDS:
                    if fname not in item:
                        errors.append(f"ItemsDetails[{i}] missing field: '{fname}'")

        container = final_output.get("ShipmentContainerDetails", {})
        if not isinstance(container, dict):
            errors.append(
                f"'ShipmentContainerDetails' must be dict, "
                f"got {type(container).__name__}"
            )
        else:
            for fname in _ALL_CONTAINER_FIELDS:
                if fname not in container:
                    errors.append(
                        f"ShipmentContainerDetails missing field: '{fname}'"
                    )

        if errors:
            for err in errors:
                logger.error("RoyalMerger.validate_output: %s", err)
        else:
            logger.info(
                "RoyalMerger.validate_output: OK ? %d header fields, "
                "%d items, container valid",
                len(header), len(items),
            )

        return errors