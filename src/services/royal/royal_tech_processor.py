# -*- coding: utf-8 -*-
"""
royal_tech_processor.py  -  Pipeline orchestrator for Royal Tech Invoice Extraction.

Step 1   OCR                  (common to both paths)
Step 2   Invoice-type detect  (common to both paths  -  on full markdown, no page detect yet)

CROSS_PAGE_INVOICE path:
  Step 2b  Line-item page detection -> Step 3  Identifiers -> Step 4  Batch Plan
  -> Step 5  Batch Extract -> Step 6  Merge
  -> Step 7  Metadata Inject -> Step 8  Validate + Rule 49

NORMAL_INVOICE path:
  NI-3  Parallel per-page extraction (all OCR pages passed directly)
  -> NI-4  Merge
  -> Step 7  Metadata Inject -> Step 8  Validate + Rule 49
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

from src.services.royal.royal_tech_config import cfg

# Helpers  -  token tracker, page-markdown slicer, PipelineResult
from src.services.royal.royal_tech_processor_helpers import (
    GeminiTokenUsage,
    GeminiTokenTracker,
    wrap_call_gemini,
    restore_call_gemini,
    extract_page_markdown_map,
    PipelineResult,
)

# Pipeline components  -  common
from src.services.royal.royal_tech_ocr_service import RoyalOcrService as OcrService, OcrResult
from src.services.royal.royal_tech_invoice_type_detector import InvoiceTypeDetector

# Pipeline components  -  CROSS_PAGE_INVOICE path (original)
from src.services.royal.royal_tech_page_detector import RoyalPageDetector as PageDetector
from src.services.royal.royal_tech_identifier_extractor import (
    RoyalIdentifierExtractor as IdentifierExtractor, IdentifierRecord,
)
from src.services.royal.royal_tech_batch_manager import RoyalBatchManager as BatchManager, BatchPlan
from src.services.royal.royal_tech_batch_extractor import RoyalBatchExtractor as BatchExtractor, BatchResult
from src.services.royal.royal_tech_merger import RoyalMerger as Merger

# Pipeline components  -  NORMAL_INVOICE path
from src.services.royal.royal_tech_normal_invoice_pipeline import (
    RoyalNormalInvoicePipeline, NormalInvoicePipelineResult,
)

# Shared post-processing
from src.services.royal.royal_tech_metadata_injector import MetadataInjector
from src.services.royal.royal_tech_validator import InvoiceValidator, ValidationResult

# Module references for token-tracker monkey-patching
import src.services.royal.royal_tech_invoice_type_detector as _it_module
import src.services.royal.royal_tech_page_detector as _pd_module
import src.services.royal.royal_tech_identifier_extractor as _ie_module
import src.services.royal.royal_tech_batch_extractor as _be_module
import src.services.royal.royal_tech_normal_invoice_extractor as _ni_module

logger = logging.getLogger(__name__)


# ============================================================================
# RoyalInvoicePipeline
# ============================================================================

class RoyalInvoicePipeline:
    """
    Orchestrates the Royal Tech invoice extraction pipeline.

    Step 1 (OCR) and Step 2 (invoice-type detection) are common to both paths.
    After Step 2 the pipeline routes:
      NORMAL_INVOICE     -> all OCR pages passed directly to NI extractor
      CROSS_PAGE_INVOICE -> line-item page detection then original Steps 3-8
    Both paths converge at Step 7 (metadata injection) and Step 8 (validation).
    """

    def __init__(self) -> None:
        cfg.validate()

        self._ocr               = OcrService()
        self._type_detector     = InvoiceTypeDetector()
        self._metadata_injector = MetadataInjector()
        self._validator         = InvoiceValidator()
        self._token_tracker     = GeminiTokenTracker()

        # CROSS_PAGE_INVOICE components (original)
        self._page_detector        = PageDetector()
        self._identifier_extractor = IdentifierExtractor()
        self._batch_manager        = BatchManager()
        self._merger               = Merger()

        # NORMAL_INVOICE components
        self._normal_pipeline = RoyalNormalInvoicePipeline()

        logger.info("RoyalInvoicePipeline ready  -  all components initialised")

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        pdf_path:    str | os.PathLike,
        schema:      dict[str, Any],
        metadata:    dict[str, str],
        page_range:  Optional[str] = None,
        output_path: Optional[str | os.PathLike] = None,
    ) -> PipelineResult:
        """Run the full pipeline. Returns PipelineResult  -  never raises."""
        pdf_path       = Path(pdf_path)
        pipeline_start = time.time()
        timings:  dict[str, float] = {}
        warnings: list[str]        = []

        logger.info("=" * 65)
        logger.info("RoyalInvoicePipeline.run: %s", pdf_path)
        logger.info("=" * 65)

        self._token_tracker.reset()
        orig_it = wrap_call_gemini(_it_module, self._token_tracker)
        orig_pd = wrap_call_gemini(_pd_module, self._token_tracker)
        orig_ie = wrap_call_gemini(_ie_module, self._token_tracker)
        orig_be = wrap_call_gemini(_be_module, self._token_tracker)
        orig_ni = wrap_call_gemini(_ni_module, self._token_tracker)

        try:
            result = self._run_pipeline(
                pdf_path=pdf_path, schema=schema, metadata=metadata,
                page_range=page_range, output_path=output_path,
                pipeline_start=pipeline_start, timings=timings, warnings=warnings,
            )
        finally:
            restore_call_gemini(_it_module, orig_it)
            restore_call_gemini(_pd_module, orig_pd)
            restore_call_gemini(_ie_module, orig_ie)
            restore_call_gemini(_be_module, orig_be)
            restore_call_gemini(_ni_module, orig_ni)

        token_usage = self._token_tracker.snapshot()
        result.gemini_token_usage = token_usage
        logger.info(
            "Gemini totals  -  calls=%d  input=%d  output=%d  total=%d",
            token_usage.call_count, token_usage.input_tokens,
            token_usage.output_tokens, token_usage.total_tokens,
        )
        return result

    # ------------------------------------------------------------------
    # Steps 1 & 2  (common)  then route
    # ------------------------------------------------------------------

    def _run_pipeline(
        self,
        pdf_path: Path, schema: dict[str, Any], metadata: dict[str, str],
        page_range: Optional[str], output_path: Optional[str | os.PathLike],
        pipeline_start: float, timings: dict, warnings: list,
    ) -> PipelineResult:

        def fatal(msg: str, **kw) -> PipelineResult:
            return self._fatal(msg, pdf_path=str(pdf_path), timings=timings,
                               warnings=warnings, pipeline_start=pipeline_start, **kw)

        # ---- STEP 1  -  OCR ----
        t0 = time.time()
        logger.info("STEP 1  -  OCR: converting PDF to Markdown...")
        ocr_result = self._ocr.process_file(pdf_path, page_range=page_range)
        timings["step1_ocr"] = time.time() - t0

        if not ocr_result.success:
            return fatal(f"STEP 1 OCR failed: {ocr_result.error}",
                         ocr_result=ocr_result)
        if ocr_result.failed_pages:
            warnings.append(
                f"OCR failed on pages {ocr_result.failed_pages}  -  "
                "those pages will have placeholder content"
            )
        logger.info("STEP 1 complete  -  %d page(s), %.2fs",
                    ocr_result.total_pages, timings["step1_ocr"])

        full_markdown = ocr_result.markdown
        total_pages   = ocr_result.total_pages

        # ---- STEP 2  -  Invoice-type detection (on full markdown) ----
        t0 = time.time()
        logger.info("STEP 2  -  Invoice-type detection...")
        invoice_type = self._type_detector.detect(full_markdown)
        timings["step2_invoice_type_detection"] = time.time() - t0
        logger.info("STEP 2 complete  -  invoice_type=%s (%.2fs)",
                    invoice_type, timings["step2_invoice_type_detection"])

        # ---- ROUTE ----
        ctx = dict(
            pdf_path=pdf_path, schema=schema, metadata=metadata,
            output_path=output_path, pipeline_start=pipeline_start,
            timings=timings, warnings=warnings,
            ocr_result=ocr_result, full_markdown=full_markdown,
            total_pages=total_pages, invoice_type=invoice_type,
            fatal=fatal,
        )
        if invoice_type == "NORMAL_INVOICE":
            return self._normal_path(**ctx)
        return self._cross_page_path(**ctx)

    # ------------------------------------------------------------------
    # NORMAL_INVOICE path  (NI-3 -> NI-4 -> Steps 7 & 8)
    # All OCR pages are passed directly  -  no line-item page detection.
    # ------------------------------------------------------------------

    def _normal_path(
        self, pdf_path, schema, metadata, output_path, pipeline_start,
        timings, warnings, ocr_result, full_markdown, total_pages,
        invoice_type, fatal,
    ) -> PipelineResult:
        logger.info("ROUTING -> NORMAL_INVOICE (%d page(s))", total_pages)

        t0 = time.time()
        logger.info("NI-3/4  -  Normal invoice extraction + merge: %d page(s)...",
                    total_pages)
        ni_result: NormalInvoicePipelineResult = self._normal_pipeline.run(
            full_markdown=full_markdown,
            total_pages=total_pages,
            schema=schema,
        )
        timings["ni_extraction_and_merge"] = time.time() - t0

        if not ni_result.success:
            return fatal(
                f"NORMAL_INVOICE: extraction/merge failed  -  {ni_result.error}",
                ocr_result=ocr_result, invoice_type=invoice_type,
            )

        warnings.extend(ni_result.warnings)
        item_count = len(ni_result.merged_output.get("ItemsDetails", []))
        logger.info("NI-3/4 complete  -  %d item(s) (%.2fs)",
                    item_count, timings["ni_extraction_and_merge"])

        final_output, validation_result = self._shared_steps_7_8(
            ni_result.merged_output, metadata, timings, warnings
        )
        if output_path:
            self._write_output(final_output, Path(output_path))

        result = PipelineResult(
            success=True, final_output=final_output, pdf_path=str(pdf_path),
            step_timings=timings, total_time_seconds=time.time() - pipeline_start,
            full_markdown=full_markdown, ocr_result=ocr_result,
            invoice_type=invoice_type,
            identifier_count=item_count,
            failed_batches=ni_result.failed_pages,
            warnings=warnings, validation_result=validation_result,
        )
        result.log_summary()
        return result

    # ------------------------------------------------------------------
    # CROSS_PAGE_INVOICE path  (Steps 2b -> 3 -> 4 -> 5 -> 6 -> 7 -> 8)
    # ------------------------------------------------------------------

    def _cross_page_path(
        self, pdf_path, schema, metadata, output_path, pipeline_start,
        timings, warnings, ocr_result, full_markdown, total_pages,
        invoice_type, fatal,
    ) -> PipelineResult:
        logger.info("ROUTING -> CROSS_PAGE_INVOICE")

        # ---- STEP 2b  -  Line-item page detection ----
        t0 = time.time()
        logger.info("STEP 2b  -  Page detection: finding main line-item pages...")
        line_item_pages = self._page_detector.detect(full_markdown, total_pages)
        timings["step2b_page_detection"] = time.time() - t0

        if not line_item_pages:
            return fatal(
                "STEP 2b: No line-item pages detected and fallback is disabled",
                ocr_result=ocr_result, invoice_type=invoice_type,
            )
        logger.info("STEP 2b complete  -  line-item pages: %s (%.2fs)",
                    line_item_pages, timings["step2b_page_detection"])

        page_markdown_map = extract_page_markdown_map(full_markdown, line_item_pages)
        if not page_markdown_map:
            return fatal(
                "Could not extract per-page markdown for line-item pages",
                ocr_result=ocr_result, line_item_pages=line_item_pages,
                invoice_type=invoice_type,
            )

        # ---- STEP 3  -  Identifier Extraction ----
        t0 = time.time()
        logger.info("STEP 3  -  Identifier extraction [PARALLEL]: %d page(s)...",
                    len(page_markdown_map))
        all_records: list[IdentifierRecord] = \
            self._identifier_extractor.extract_all_pages(page_markdown_map)
        timings["step3_identifier_extraction"] = time.time() - t0

        if not all_records:
            return fatal("STEP 3: No line-item identifiers extracted",
                         ocr_result=ocr_result, line_item_pages=line_item_pages,
                         invoice_type=invoice_type)
        duplicates = self._identifier_extractor.audit_duplicates(all_records)
        if duplicates:
            warnings.append(
                f"{len(duplicates)} duplicate composite identifier(s) detected"
            )
        logger.info("STEP 3 complete  -  %d identifier(s) (%.2fs)",
                    len(all_records), timings["step3_identifier_extraction"])

        # ---- STEP 4  -  Batch Planning ----
        t0 = time.time()
        logger.info("STEP 4  -  Batch planning: partitioning %d identifier(s)...",
                    len(all_records))
        plan: BatchPlan = self._batch_manager.create_plan(all_records)
        timings["step4_batch_planning"] = time.time() - t0

        if plan.is_empty:
            return fatal("STEP 4: BatchManager produced an empty plan",
                         ocr_result=ocr_result, line_item_pages=line_item_pages,
                         invoice_type=invoice_type, identifier_count=len(all_records))
        if not self._batch_manager.validate_plan(plan, all_records):
            warnings.append(
                "BatchPlan validation found inconsistencies  -  proceeding with caution"
            )
        logger.info("STEP 4 complete  -  %d batch(es) of <=%d (%.2fs)",
                    plan.total_batches, plan.batch_size_used,
                    timings["step4_batch_planning"])

        # ---- STEP 5  -  Batch Extraction ----
        t0 = time.time()
        logger.info("STEP 5  -  Batch extraction [PARALLEL]: %d batch(es) -> Gemini...",
                    plan.total_batches)
        batch_results: list[BatchResult] = \
            BatchExtractor(schema=schema).extract_all_batches(
                plan=plan, full_markdown=full_markdown,
            )
        timings["step5_batch_extraction"] = time.time() - t0

        failed_batches: list[int] = []
        for r in batch_results:
            batch = plan.batches[r.batch_index]
            if not r.success:
                failed_batches.append(r.batch_index)
                warnings.append(f"Batch {batch.batch_number} failed: {r.error}")
                logger.error("  FAIL Batch %d: %s", batch.batch_number, r.error)
            else:
                if r.missing_serials:
                    warnings.append(
                        f"Batch {batch.batch_number}: missing serials "
                        f"{r.missing_serials}"
                    )
                if r.extra_serials:
                    warnings.append(
                        f"Batch {batch.batch_number}: extra items {r.extra_serials}"
                    )
                logger.info("  OK Batch %d: %d item(s)",
                            batch.batch_number, len(r.line_items))

        successful_results = [r for r in batch_results if r.success]
        if not successful_results:
            return fatal("STEP 5: Every batch failed  -  no data to merge",
                         ocr_result=ocr_result, line_item_pages=line_item_pages,
                         invoice_type=invoice_type, identifier_count=len(all_records),
                         batch_plan_summary=plan.summary(),
                         batch_results=batch_results, failed_batches=failed_batches)
        if failed_batches:
            warnings.append(
                f"{len(failed_batches)} of {plan.total_batches} batch(es) failed  -  "
                f"output may be incomplete (indices: {failed_batches})"
            )
        logger.info("STEP 5 complete  -  %d/%d batch(es) succeeded (%.2fs)",
                    len(successful_results), plan.total_batches,
                    timings["step5_batch_extraction"])

        # ---- STEP 6  -  Merge ----
        t0 = time.time()
        logger.info("STEP 6  -  Merger: combining %d batch result(s)...",
                    len(batch_results))
        final_output = self._merger.merge(batch_results)
        timings["step6_merge"] = time.time() - t0

        if final_output is None:
            return fatal("STEP 6: Merger returned None  -  check merger logs",
                         ocr_result=ocr_result, line_item_pages=line_item_pages,
                         invoice_type=invoice_type, identifier_count=len(all_records),
                         batch_plan_summary=plan.summary(),
                         batch_results=batch_results, failed_batches=failed_batches)
        logger.info("STEP 6 complete  -  %d line item(s) (%.2fs)",
                    len(final_output.get("ItemsDetails", [])),
                    timings["step6_merge"])

        final_output, validation_result = self._shared_steps_7_8(
            final_output, metadata, timings, warnings
        )
        if output_path:
            self._write_output(final_output, Path(output_path))

        result = PipelineResult(
            success=True, final_output=final_output, pdf_path=str(pdf_path),
            step_timings=timings, total_time_seconds=time.time() - pipeline_start,
            full_markdown=full_markdown, ocr_result=ocr_result,
            line_item_pages=line_item_pages, invoice_type=invoice_type,
            identifier_count=len(all_records), batch_plan_summary=plan.summary(),
            batch_results=batch_results, failed_batches=failed_batches,
            warnings=warnings, validation_result=validation_result,
        )
        result.log_summary()
        return result

    # ------------------------------------------------------------------
    # Shared Steps 7 & 8
    # ------------------------------------------------------------------

    def _shared_steps_7_8(
        self,
        final_output: dict,
        metadata:     dict[str, str],
        timings:      dict,
        warnings:     list[str],
    ) -> tuple[dict, ValidationResult]:
        """Metadata injection (Step 7) + validation/Rule-49 (Step 8)."""
        t0 = time.time()
        logger.info("STEP 7  -  Metadata injection...")
        final_output = self._metadata_injector.inject(final_output, metadata)
        timings["step7_metadata_injection"] = time.time() - t0
        logger.info("STEP 7 complete (%.3fs)", timings["step7_metadata_injection"])

        t0 = time.time()
        logger.info("STEP 8  -  Validation + Rule 49 QtyCode normalisation...")
        validation_result = self._validator.validate_and_normalise(final_output)
        timings["step8_validation"] = time.time() - t0

        if not validation_result.is_valid:
            for err in validation_result.errors:
                warnings.append(f"Validation: {err}")
            logger.warning("STEP 8: validation errors  -  output returned with warnings")
        else:
            logger.info("STEP 8 complete  -  valid (%.3fs)",
                        timings["step8_validation"])

        return validation_result.invoice_json, validation_result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fatal(
        self, error: str, pdf_path: str, timings: dict, warnings: list[str],
        pipeline_start: float, ocr_result: Optional[OcrResult] = None,
        line_item_pages: Optional[list[int]] = None,
        invoice_type: Optional[str] = None,
        identifier_count: int = 0,
        batch_plan_summary: Optional[dict] = None,
        batch_results: Optional[list[BatchResult]] = None,
        failed_batches: Optional[list[int]] = None,
    ) -> PipelineResult:
        logger.error("RoyalInvoicePipeline FATAL: %s", error)
        result = PipelineResult(
            success=False, final_output=None, pdf_path=pdf_path,
            step_timings=timings, total_time_seconds=time.time() - pipeline_start,
            ocr_result=ocr_result, line_item_pages=line_item_pages or [],
            invoice_type=invoice_type, identifier_count=identifier_count,
            batch_plan_summary=batch_plan_summary,
            batch_results=batch_results or [], failed_batches=failed_batches or [],
            error=error, warnings=warnings,
        )
        result.log_summary()
        return result

    def _write_output(self, output: dict, path: Path) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(output, ensure_ascii=False, indent=2),
                            encoding="utf-8")
            logger.info("Output written to: %s", path)
        except Exception as exc:
            logger.error("Could not write output to %s: %s", path, exc)