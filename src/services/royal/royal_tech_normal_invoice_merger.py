# -*- coding: utf-8 -*-
"""
royal_tech_normal_invoice_merger.py  -  NI-Step 4: Merge all PageExtractionResults
from the NORMAL_INVOICE path into the final unified Document 2 JSON.

NORMAL_INVOICE merger differences vs. RoyalMerger (CROSS_PAGE path)
--------------------------------------------------------------------
* Input is list[PageExtractionResult] keyed by page_num, not batch_index.
* Header source: lowest-numbered successful page (page 1 equivalent).
  On a NORMAL_INVOICE the header repeats identically on every page, so
  any successful page would do  -  lowest page_num is the most reliable.
* Line items: concatenated in ascending page_num order; no serial-based
  identity key is available, so deduplication uses the same
  (Quantity, Amount, HSNCode, Rate) tuple used by RoyalMerger.
* Container: all-null per Document 2 schema (same as CROSS_PAGE path).
* Schema enforcement (_enforce_header, _enforce_item, _enforce_container)
  is shared via the same module-level constants as royal_tech_merger.py.

Public API
----------
    merger = RoyalNormalInvoiceMerger()
    final  = merger.merge(page_results)  -> dict | None
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from src.services.royal.royal_tech_config import cfg
from src.services.royal.royal_tech_normal_invoice_extractor import PageExtractionResult

logger = logging.getLogger(__name__)


# ============================================================================
# Complete field manifests (Document 2 schema)  -  identical to royal_tech_merger
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

# Loaded from cfg.schema  -  same singleton values used by RoyalMerger
_HEADER_FORCE_NULL: frozenset[str] = frozenset(cfg.schema.header_always_null_fields)
_ITEM_FORCE_NULL:   frozenset[str] = frozenset(cfg.schema.items_always_null_fields)
_HEADER_DEFAULTS:   dict[str, Any] = dict(cfg.schema.header_default_fields)


# ============================================================================
# NormalMergeReport  (internal diagnostics)
# ============================================================================

@dataclass
class NormalMergeReport:
    """Internal diagnostics produced during NORMAL_INVOICE merge."""

    primary_page_num:              int       = -1
    successful_page_nums:          list[int] = field(default_factory=list)
    failed_page_nums:              list[int] = field(default_factory=list)
    total_line_items_before_dedup: int       = 0
    total_line_items_after_dedup:  int       = 0
    duplicates_removed:            int       = 0
    renumbered_items:              bool      = False
    header_field_gaps:             list[str] = field(default_factory=list)
    warnings:                      list[str] = field(default_factory=list)

    def log(self) -> None:
        logger.info("NormalMergeReport:")
        logger.info("  Primary page     : %d", self.primary_page_num)
        logger.info("  Successful pages : %s", self.successful_page_nums)
        logger.info("  Failed pages     : %s", self.failed_page_nums)
        logger.info("  Items pre-dedup  : %d", self.total_line_items_before_dedup)
        logger.info("  Duplicates removed: %d", self.duplicates_removed)
        logger.info("  Items final      : %d", self.total_line_items_after_dedup)
        if self.header_field_gaps:
            logger.warning("  Null header fields: %s", self.header_field_gaps)
        for w in self.warnings:
            logger.warning("  ! %s", w)

    def to_dict(self) -> dict:
        return {
            "primary_page_num":             self.primary_page_num,
            "successful_page_nums":         self.successful_page_nums,
            "failed_page_nums":             self.failed_page_nums,
            "total_line_items_before_dedup": self.total_line_items_before_dedup,
            "duplicates_removed":           self.duplicates_removed,
            "total_line_items_after_dedup": self.total_line_items_after_dedup,
            "renumbered_items":             self.renumbered_items,
            "header_field_gaps":            self.header_field_gaps,
            "warnings":                     self.warnings,
        }


# ============================================================================
# Schema enforcement helpers  (identical logic to royal_tech_merger.py)
# ============================================================================

def _enforce_header(raw: dict, report: NormalMergeReport) -> dict:
    """
    Produce a header dict that:
    * Contains exactly the fields in _ALL_HEADER_FIELDS.
    * Forces always-null fields to None regardless of Gemini's value.
    * Applies always-default values (CartonNo="1", Status="Success", etc.).
    * Fills absent fields with None.
    * Reports null fields to NormalMergeReport for visibility.
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
    * Contains exactly the fields in _ALL_ITEM_FIELDS.
    * Forces always-null item fields to None.
    * Sets Itemslno to new_itemslno (re-numbered by merger).
    * Fills absent fields with None.
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


def _enforce_container() -> dict:
    """All container fields are always null per Document 2 schema."""
    return {f: None for f in _ALL_CONTAINER_FIELDS}


# ============================================================================
# Deduplication helpers
# ============================================================================

def _item_identity_key(item: dict) -> tuple:
    """
    Hashable identity key for a line item.
    Uses (Quantity, Amount, HSNCode, Rate)  -  same logic as RoyalMerger.
    """
    return (
        str(item.get("Quantity", "") or ""),
        str(item.get("Amount",   "") or ""),
        str(item.get("HSNCode",  "") or ""),
        str(item.get("Rate",     "") or ""),
    )


def _deduplicate_items(
    items: list[dict], report: NormalMergeReport
) -> list[dict]:
    """Remove duplicate line items (first-seen wins)."""
    seen:   dict[tuple, int] = {}
    unique: list[dict]       = []

    for item in items:
        key = _item_identity_key(item)
        if key not in seen:
            seen[key] = len(unique)
            unique.append(item)
        else:
            report.duplicates_removed += 1
            logger.debug(
                "NormalInvoiceMerger: duplicate item removed "
                "(qty=%s, amount=%s, hsn=%s)",
                item.get("Quantity"), item.get("Amount"), item.get("HSNCode"),
            )
    return unique


# ============================================================================
# Header source selection
# ============================================================================

def _select_primary_page(
    page_results: list[PageExtractionResult],
    report: NormalMergeReport,
) -> Optional[PageExtractionResult]:
    """
    Return the PageExtractionResult to use as the header source.

    Priority: lowest page_num among successful results.
    The header repeats identically on every NORMAL_INVOICE page;
    using page 1 (or the lowest available) is the safest choice.

    Returns None only if no successful page exists.
    """
    successful = sorted(
        (r for r in page_results if r.success),
        key=lambda r: r.page_num,
    )
    if not successful:
        logger.error(
            "NormalInvoiceMerger: no successful page results  -  "
            "cannot produce header"
        )
        return None

    primary = successful[0]
    report.primary_page_num = primary.page_num

    if primary.page_num != min(r.page_num for r in page_results):
        logger.warning(
            "NormalInvoiceMerger: lowest page (%d) failed  -  "
            "using page %d as header source (fallback)",
            min(r.page_num for r in page_results),
            primary.page_num,
        )
    return primary


# ============================================================================
# RoyalNormalInvoiceMerger
# ============================================================================

class RoyalNormalInvoiceMerger:
    """
    NI-Step 4  -  Combines all PageExtractionResult objects from the
    NORMAL_INVOICE extractor into the final unified Document 2 JSON.

    Usage
    -----
        merger = RoyalNormalInvoiceMerger()
        final  = merger.merge(page_results)

        if final is None:
            # Fatal  -  no usable data from any page
        else:
            print(json.dumps(final, indent=2))
    """

    def __init__(self) -> None:
        self._cfg      = cfg.merger
        self._debug    = cfg.pipeline.debug_save_intermediate
        self._work_dir = cfg.pipeline.work_dir

        logger.info(
            "RoyalNormalInvoiceMerger initialised (dedup=%s)",
            self._cfg.deduplicate_on_exact_identifier,
        )

    # ------------------------------------------------------------------
    # Primary public method
    # ------------------------------------------------------------------

    def merge(
        self, page_results: list[PageExtractionResult]
    ) -> Optional[dict]:
        """
        Merge all PageExtractionResult objects into the final invoice JSON.

        Parameters
        ----------
        page_results : list[PageExtractionResult]
            Ordered list from RoyalNormalInvoiceExtractor.extract_all_pages().
            May contain failed results (result.success == False).
            Must have at least one successful result.

        Returns
        -------
        dict | None   -  None only when no page succeeded at all.
        """
        if not page_results:
            logger.error(
                "RoyalNormalInvoiceMerger.merge: received empty page_results"
            )
            return None

        report = NormalMergeReport()

        for r in page_results:
            if r.success:
                report.successful_page_nums.append(r.page_num)
            else:
                report.failed_page_nums.append(r.page_num)

        logger.info(
            "NormalInvoiceMerger: %d successful / %d failed page(s)",
            len(report.successful_page_nums),
            len(report.failed_page_nums),
        )

        if not report.successful_page_nums:
            logger.error(
                "NormalInvoiceMerger: all pages failed  -  cannot produce output"
            )
            return None

        primary = _select_primary_page(page_results, report)
        if primary is None:
            return None

        header    = _enforce_header(primary.header, report)
        container = _enforce_container()

        raw_items = self._collect_line_items(page_results, report)

        if not raw_items:
            report.warnings.append(
                "No line items collected from any page  -  ItemsDetails will be empty"
            )
            logger.warning(
                "NormalInvoiceMerger: no line items in any successful page"
            )

        report.total_line_items_before_dedup = len(raw_items)

        if self._cfg.deduplicate_on_exact_identifier and raw_items:
            raw_items = _deduplicate_items(raw_items, report)
            if report.duplicates_removed:
                logger.info(
                    "NormalInvoiceMerger: deduplication removed %d item(s)",
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
            "NormalInvoiceMerger complete  -  %d line item(s), "
            "%d header field(s) null, %d page(s) failed",
            len(final_items),
            len(report.header_field_gaps),
            len(report.failed_page_nums),
        )
        return final_output

    # ------------------------------------------------------------------
    # Line item collection
    # ------------------------------------------------------------------

    def _collect_line_items(
        self,
        page_results: list[PageExtractionResult],
        report: NormalMergeReport,
    ) -> list[dict]:
        """Concatenate line_items from all successful pages in page_num order."""
        all_items: list[dict] = []

        for r in sorted(page_results, key=lambda x: x.page_num):
            if not r.success:
                report.warnings.append(
                    f"Page {r.page_num} skipped (failed): {r.error or 'unknown error'}"
                )
                logger.warning(
                    "NormalInvoiceMerger: skipping failed page %d  -  %s",
                    r.page_num, r.error or "unknown error",
                )
                continue

            if not r.line_items:
                logger.warning(
                    "NormalInvoiceMerger: successful page %d has no line items",
                    r.page_num,
                )
                report.warnings.append(
                    f"Page {r.page_num} succeeded but returned 0 line items"
                )
                continue

            logger.debug(
                "NormalInvoiceMerger: appending %d item(s) from page %d",
                len(r.line_items), r.page_num,
            )
            all_items.extend(r.line_items)

        return all_items

    # ------------------------------------------------------------------
    # Item schema enforcement + re-numbering
    # ------------------------------------------------------------------

    def _enforce_items(
        self,
        raw_items: list[dict],
        report: NormalMergeReport,
    ) -> list[dict]:
        """Apply _enforce_item() to every raw item with sequential Itemslno."""
        final: list[dict] = []
        for new_slno, raw_item in enumerate(raw_items, start=1):
            if not isinstance(raw_item, dict):
                logger.warning(
                    "NormalInvoiceMerger: skipping non-dict item at "
                    "position %d",
                    new_slno,
                )
                report.warnings.append(
                    f"Non-dict item at merged position {new_slno}  -  skipped"
                )
                continue
            final.append(_enforce_item(raw_item, new_itemslno=new_slno))
        report.renumbered_items = True
        return final

    # ------------------------------------------------------------------
    # Debug save
    # ------------------------------------------------------------------

    def _save_debug(
        self, final_output: dict, report: NormalMergeReport
    ) -> None:
        try:
            work_dir = Path(self._work_dir)
            work_dir.mkdir(parents=True, exist_ok=True)
            (work_dir / "ni_merged_output.json").write_text(
                json.dumps(final_output, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (work_dir / "ni_merge_report.json").write_text(
                json.dumps(report.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info(
                "NormalInvoiceMerger [debug]: artefacts saved  -  %s", work_dir
            )
        except Exception as exc:
            logger.warning(
                "NormalInvoiceMerger [debug]: could not save debug files  -  %s",
                exc,
            )

    # ------------------------------------------------------------------
    # Output validation (convenience  -  mirrors RoyalMerger.validate_output)
    # ------------------------------------------------------------------

    def validate_output(self, final_output: dict) -> list[str]:
        """
        Lightweight structural validation of the merged output.
        Returns list[str] of error strings  -  empty means valid.
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
            errors.append(
                f"'ItemsDetails' must be list, got {type(items).__name__}"
            )
        else:
            for i, item in enumerate(items):
                if not isinstance(item, dict):
                    errors.append(f"ItemsDetails[{i}] is not a dict")
                    continue
                for fname in _ALL_ITEM_FIELDS:
                    if fname not in item:
                        errors.append(
                            f"ItemsDetails[{i}] missing field: '{fname}'"
                        )

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
                logger.error("NormalInvoiceMerger.validate_output: %s", err)
        else:
            logger.info(
                "NormalInvoiceMerger.validate_output: OK  -  %d header fields, "
                "%d items, container valid",
                len(header), len(items),
            )
        return errors