# -*- coding: utf-8 -*-
"""
royal_tech_normal_invoice_pipeline.py  -  Orchestrator for the NORMAL_INVOICE
extraction sub-pipeline (NI-Steps 3 and 4).

Called by royal_tech_processor.py after Step 2 confirms NORMAL_INVOICE.
Receives the raw full_markdown and total_pages directly from the OCR result
(Step 1).  No line-item page detection is performed  -  all OCR pages are
passed to the extractor.

Runs inside the same token-tracker monkey-patch context as the main pipeline,
so all Gemini token usage is automatically recorded.

Steps
-----
NI-3  RoyalNormalInvoiceExtractor.extract_all_pages()
        Builds a per-page markdown map from all OCR pages, then fires one
        Gemini call per page in parallel.
        Each call returns header + all line items for that page.

NI-4  RoyalNormalInvoiceMerger.merge()
        Selects header from the lowest-numbered successful page,
        concatenates all line items in page-number order,
        deduplicates (Qty, Amount, HSNCode, Rate) if configured,
        enforces the full Document 2 field manifest,
        and renumbers Itemslno sequentially.

Public API
----------
    pipeline = RoyalNormalInvoicePipeline()
    result   = pipeline.run(full_markdown, total_pages, schema)
    # -> NormalInvoicePipelineResult
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from src.services.royal.royal_tech_normal_invoice_extractor import (
    RoyalNormalInvoiceExtractor,
    PageExtractionResult,
)
from src.services.royal.royal_tech_normal_invoice_merger import (
    RoyalNormalInvoiceMerger,
)
from src.services.royal.royal_tech_processor_helpers import (
    extract_page_markdown_map,
)

logger = logging.getLogger(__name__)


# ============================================================================
# NormalInvoicePipelineResult
# ============================================================================

@dataclass
class NormalInvoicePipelineResult:
    """
    Result of one RoyalNormalInvoicePipeline.run() execution.

    Attributes
    ----------
    success : bool
        True only when extraction + merge completed without a fatal error.
    merged_output : dict | None
        The merged invoice JSON (Document 2 schema) when success is True.
    failed_pages : list[int]
        Page numbers whose extraction failed.  Empty on full success.
    warnings : list[str]
        Non-fatal warnings accumulated during NI-3 and NI-4.
    error : str | None
        Fatal error message when success is False.
    page_results : list[PageExtractionResult]
        Raw per-page extraction outputs from NI-3 (preserved for debug).
    elapsed_ni3 : float
        Wall-clock seconds spent in NI-3 (extraction).
    elapsed_ni4 : float
        Wall-clock seconds spent in NI-4 (merge).
    """

    success:       bool
    merged_output: Optional[dict]
    failed_pages:  list[int]                   = field(default_factory=list)
    warnings:      list[str]                   = field(default_factory=list)
    error:         Optional[str]               = None
    page_results:  list[PageExtractionResult]  = field(default_factory=list)
    elapsed_ni3:   float                       = 0.0
    elapsed_ni4:   float                       = 0.0


# ============================================================================
# RoyalNormalInvoicePipeline
# ============================================================================

class RoyalNormalInvoicePipeline:
    """
    Thin orchestrator that wires NI-3 (extraction) -> NI-4 (merge) and
    returns a NormalInvoicePipelineResult to the main pipeline.

    Receives full_markdown and total_pages from the processor and builds the
    per-page markdown map internally from ALL OCR pages  -  no line-item
    page detection step is involved.

    A new RoyalNormalInvoiceExtractor is instantiated per run() call so the
    schema is always fresh.  RoyalNormalInvoiceMerger is stateless and shared.

    Usage
    -----
        pipeline = RoyalNormalInvoicePipeline()
        result   = pipeline.run(
            full_markdown=ocr_result.markdown,
            total_pages=ocr_result.total_pages,
            schema=schema_dict,
        )
        if result.success:
            final_json = result.merged_output
    """

    def __init__(self) -> None:
        self._merger = RoyalNormalInvoiceMerger()
        logger.info("RoyalNormalInvoicePipeline ready")

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        full_markdown: str,
        total_pages:   int,
        schema:        dict[str, Any],
    ) -> NormalInvoicePipelineResult:
        """
        Execute NI-3 (parallel per-page extraction) then NI-4 (merge).

        Parameters
        ----------
        full_markdown : str
            Complete OCR markdown with ---PAGE N--- separators, exactly as
            returned by royal_tech_ocr_service.  All pages are included.
        total_pages : int
            Total number of pages in the source PDF (from OCR result).
            Used to build the full list of page numbers [1 .. total_pages].
        schema : dict[str, Any]
            Dynamic DB schema dict  -  passed straight through to the extractor
            so field lists and null/default overrides are applied correctly.

        Returns
        -------
        NormalInvoicePipelineResult  -  never raises.
        """
        if not full_markdown or not full_markdown.strip():
            return NormalInvoicePipelineResult(
                success=False,
                merged_output=None,
                error="full_markdown is empty  -  nothing to extract",
            )

        if total_pages < 1:
            return NormalInvoicePipelineResult(
                success=False,
                merged_output=None,
                error=f"total_pages={total_pages} is invalid",
            )

        all_page_numbers = list(range(1, total_pages + 1))

        # Build per-page markdown map from ALL pages (no filtering)
        page_markdown_map = extract_page_markdown_map(
            full_markdown, all_page_numbers
        )

        if not page_markdown_map:
            return NormalInvoicePipelineResult(
                success=False,
                merged_output=None,
                error=(
                    "Could not extract per-page markdown from full_markdown  -  "
                    "check ---PAGE N--- separators"
                ),
            )

        sorted_pages = sorted(page_markdown_map.keys())
        logger.info(
            "RoyalNormalInvoicePipeline.run: %d page(s) %s",
            len(sorted_pages), sorted_pages,
        )

        # NI-3  -  Parallel per-page extraction
        t0        = time.time()
        extractor = RoyalNormalInvoiceExtractor(schema=schema)
        page_results: list[PageExtractionResult] = \
            extractor.extract_all_pages(page_markdown_map)
        elapsed_ni3 = time.time() - t0

        failed_pages = [r.page_num for r in page_results if not r.success]
        warnings: list[str] = []

        if failed_pages:
            warnings.append(
                f"NI-3: {len(failed_pages)} page(s) failed extraction: "
                f"{failed_pages}"
            )
            logger.warning(
                "RoyalNormalInvoicePipeline: NI-3 partial failure  -  "
                "failed pages: %s",
                failed_pages,
            )

        successful_results = [r for r in page_results if r.success]
        if not successful_results:
            return NormalInvoicePipelineResult(
                success=False,
                merged_output=None,
                failed_pages=failed_pages,
                warnings=warnings,
                error="NI-3: every page failed  -  no data to merge",
                page_results=page_results,
                elapsed_ni3=elapsed_ni3,
            )

        logger.info(
            "RoyalNormalInvoicePipeline: NI-3 complete  -  %d/%d page(s) "
            "succeeded (%.2fs)  items per page: %s",
            len(successful_results),
            len(page_results),
            elapsed_ni3,
            {r.page_num: len(r.line_items) for r in successful_results},
        )

        # NI-4  -  Merge
        t0          = time.time()
        merged      = self._merger.merge(page_results)
        elapsed_ni4 = time.time() - t0

        if merged is None:
            return NormalInvoicePipelineResult(
                success=False,
                merged_output=None,
                failed_pages=failed_pages,
                warnings=warnings,
                error="NI-4: merger returned None  -  check merger logs",
                page_results=page_results,
                elapsed_ni3=elapsed_ni3,
                elapsed_ni4=elapsed_ni4,
            )

        item_count = len(merged.get("ItemsDetails", []))
        logger.info(
            "RoyalNormalInvoicePipeline: NI-4 complete  -  %d line item(s) "
            "(%.2fs)",
            item_count, elapsed_ni4,
        )

        return NormalInvoicePipelineResult(
            success=True,
            merged_output=merged,
            failed_pages=failed_pages,
            warnings=warnings,
            page_results=page_results,
            elapsed_ni3=elapsed_ni3,
            elapsed_ni4=elapsed_ni4,
        )