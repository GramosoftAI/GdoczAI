# -*- coding: utf-8 -*-
"""
royal_tech_ocr_service.py � STEP 1: PDF ? page-separated Markdown via OLMOCR.

Direct refactor of ocr_service.py:
  � config  ? royal_tech_config.cfg
  � All logic, dataclasses, helper functions, and method signatures preserved
  � OcrResult, _page_separator, _parse_processor_markdown, _assemble_markdown
    all unchanged � only config source swapped

Public API
----------
    service = RoyalOcrService()
    result  = service.process(pdf_bytes, filename="invoice.pdf") -> OcrResult
    result  = service.process_file(pdf_path)                     -> OcrResult
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from src.services.royal.royal_tech_config import cfg

try:
    from src.services.royal.royal_tech_ocr_processor import MinerUProcessor
    _PROCESSOR_AVAILABLE = True
except ImportError:
    _PROCESSOR_AVAILABLE = False
    MinerUProcessor = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


# ============================================================================
# OcrResult dataclass
# ============================================================================

@dataclass
class OcrResult:
    """
    The output of RoyalOcrService.process().

    Attributes
    ----------
    success : bool
        True if OCR completed without a fatal error.
    markdown : str
        Full assembled markdown with ---PAGE N--- separators.
        Empty string on failure.
    total_pages : int
        Total pages found in the PDF (before any page-range filtering).
    processed_pages : list[int]
        1-indexed page numbers actually sent to OLMOCR.
    empty_pages : list[int]
        Pages detected as blank and skipped.
    failed_pages : list[int]
        Pages where OLMOCR returned nothing after all retries.
    processing_time_seconds : float
        Wall-clock seconds for the OCR phase.
    error : str | None
        Human-readable error message if success is False.
    """

    success: bool
    markdown: str
    total_pages: int
    processed_pages: list[int] = field(default_factory=list)
    empty_pages: list[int] = field(default_factory=list)
    failed_pages: list[int] = field(default_factory=list)
    processing_time_seconds: float = 0.0
    error: Optional[str] = None

    @property
    def page_count(self) -> int:
        """Number of pages that produced usable markdown content."""
        return len(self.processed_pages) - len(self.failed_pages)

    def summary(self) -> dict:
        return {
            "success":                   self.success,
            "total_pages":               self.total_pages,
            "processed_pages":           self.processed_pages,
            "empty_pages":               self.empty_pages,
            "failed_pages":              self.failed_pages,
            "markdown_chars":            len(self.markdown),
            "processing_time_seconds":   round(self.processing_time_seconds, 2),
            "error":                     self.error,
        }


# ============================================================================
# Page-separator utilities
# ============================================================================

def _page_separator(page_num: int) -> str:
    """
    Return the separator string for page_num using the template in config.
    Template token understood: {page_num}
    Default: "\\n\\n---PAGE {page_num}---\\n\\n"
    """
    return cfg.olmocr.page_separator.format(page_num=page_num)


def _extract_page_number_from_header(header_line: str) -> Optional[int]:
    """
    MinerUProcessor uses "# Page N" as each section header.
    Parse that and return N, or None if the line does not match.
    """
    match = re.match(r"^#+\s*[Pp]age\s+(\d+)", header_line.strip())
    if match:
        return int(match.group(1))
    return None


def _parse_processor_markdown(raw_markdown: str) -> dict[int, str]:
    """
    MinerUProcessor.process_pdf() assembles markdown as:

        # Page 1
        <content>
        ---
        # Page 2
        <content>
        ---
        ...

    Split this into a dict {page_num: content_without_header}.
    Falls back gracefully if the format is different.
    """
    page_map: dict[int, str] = {}
    sections = re.split(r"\n\s*---\s*\n", raw_markdown)

    for section in sections:
        lines = section.strip().splitlines()
        if not lines:
            continue

        page_num = _extract_page_number_from_header(lines[0])
        if page_num is None:
            logger.debug(
                "RoyalOcrService: skipping section with unrecognised header: %r",
                lines[0][:60],
            )
            continue

        content = "\n".join(lines[1:]).strip()

        if page_num in page_map:
            logger.warning(
                "RoyalOcrService: duplicate section for page %d � "
                "keeping first occurrence",
                page_num,
            )
        else:
            page_map[page_num] = content

    return page_map


def _assemble_markdown(page_map: dict[int, str], total_pages: int) -> str:
    """
    Build the canonical pipeline markdown from a {page_num: content} map.

    Format per page:
        \\n\\n---PAGE N---\\n\\n
        <content>

    Pages missing from page_map get a placeholder so downstream steps can
    still see the separator and know the page exists.
    """
    parts: list[str] = []
    for page_num in range(1, total_pages + 1):
        sep     = _page_separator(page_num)
        content = page_map.get(
            page_num, "[OCR extraction failed for this page]"
        )
        parts.append(f"{sep}{content}")
    return "".join(parts)


# ============================================================================
# RoyalOcrService
# ============================================================================

class RoyalOcrService:
    """
    STEP 1 � Converts a PDF to page-separated markdown using OLMOCR.

    Wraps MinerUProcessor and adds:
    � Unified OcrResult return type.
    � Re-assembly into the canonical ---PAGE N--- format.
    � Page cap enforcement (cfg.pipeline.max_pages).
    � Debug intermediate saving (cfg.pipeline.debug_save_intermediate).
    � Clear error propagation without silent swallowing.

    Usage
    -----
        service = RoyalOcrService()
        result  = service.process(pdf_bytes, filename="invoice.pdf")

        if not result.success:
            raise RuntimeError(result.error)

        markdown    = result.markdown
        total_pages = result.total_pages
    """

    def __init__(self) -> None:
        if not _PROCESSOR_AVAILABLE:
            raise ImportError(
                "RoyalOcrService requires 'mineru_ocr_server_processor.py' "
                "to be importable. Ensure it exists in the Python path."
            )

        ocfg = cfg.olmocr
        self._processor = MinerUProcessor(
            api_key=ocfg.api_key,
            model=ocfg.model,
            timeout=ocfg.timeout,
            batch_size=ocfg.batch_size,
        )

        self._max_pages: Optional[int] = cfg.pipeline.max_pages
        self._debug: bool              = cfg.pipeline.debug_save_intermediate
        self._work_dir: str            = cfg.pipeline.work_dir

        logger.info(
            "RoyalOcrService initialised (model=%s, batch_size=%d, "
            "dpi=%d, max_pages=%s)",
            ocfg.model,
            ocfg.batch_size,
            ocfg.pdf_dpi,
            self._max_pages if self._max_pages else "unlimited",
        )

    # ------------------------------------------------------------------
    # Public entry point � bytes
    # ------------------------------------------------------------------

    def process(
        self,
        pdf_bytes: bytes,
        filename: str,
        page_range: Optional[str] = None,
    ) -> OcrResult:
        """
        Run OLMOCR on pdf_bytes and return an OcrResult.

        Parameters
        ----------
        pdf_bytes : bytes
            Raw bytes of the PDF file.
        filename : str
            Original filename � used only for logging inside MinerUProcessor.
        page_range : str | None
            Optional page-range string, e.g. "1-3", "1,3,5", "2-4,7".
            None = process all pages.

        Returns
        -------
        OcrResult � always returned; never raises. Check result.success.
        """
        if not pdf_bytes:
            return OcrResult(
                success=False, markdown="", total_pages=0,
                error="pdf_bytes is empty",
            )

        if not filename:
            filename = "invoice.pdf"

        page_range = self._apply_page_cap(page_range)

        logger.info(
            "RoyalOcrService.process: filename=%r  size=%d bytes  page_range=%r",
            filename, len(pdf_bytes), page_range,
        )

        start = time.time()

        try:
            success, raw_markdown, total_pages, error_msg = (
                self._processor.process_pdf(
                    pdf_bytes=pdf_bytes,
                    filename=filename,
                    page_range=page_range,
                )
            )
        except Exception as exc:
            elapsed = time.time() - start
            logger.exception(
                "RoyalOcrService: MinerUProcessor raised unexpected exception"
            )
            return OcrResult(
                success=False, markdown="", total_pages=0,
                processing_time_seconds=elapsed,
                error=f"MinerUProcessor exception: {exc}",
            )

        elapsed = time.time() - start

        if not success:
            logger.error(
                "RoyalOcrService: MinerUProcessor failure � %s", error_msg
            )
            return OcrResult(
                success=False, markdown="", total_pages=total_pages,
                processing_time_seconds=elapsed,
                error=error_msg or "MinerUProcessor returned success=False",
            )

        if not raw_markdown or not raw_markdown.strip():
            logger.warning("RoyalOcrService: MinerUProcessor returned empty markdown")
            return OcrResult(
                success=False, markdown="", total_pages=total_pages,
                processing_time_seconds=elapsed,
                error="MinerUProcessor returned empty markdown",
            )

        page_map = _parse_processor_markdown(raw_markdown)

        if not page_map:
            logger.warning(
                "RoyalOcrService: could not parse pages from raw_markdown "
                "(%d chars) � using raw as page 1",
                len(raw_markdown),
            )
            page_map   = {1: raw_markdown.strip()}
            total_pages = max(total_pages, 1)

        processed_pages, empty_pages, failed_pages = self._classify_pages(
            page_map, total_pages, raw_markdown
        )

        final_markdown = _assemble_markdown(page_map, total_pages)

        if self._debug:
            self._save_debug(
                filename, raw_markdown, page_map, final_markdown, total_pages
            )

        result = OcrResult(
            success=True,
            markdown=final_markdown,
            total_pages=total_pages,
            processed_pages=processed_pages,
            empty_pages=empty_pages,
            failed_pages=failed_pages,
            processing_time_seconds=elapsed,
        )

        logger.info("RoyalOcrService.process complete � %s", result.summary())
        return result

    # ------------------------------------------------------------------
    # Public entry point � file path
    # ------------------------------------------------------------------

    def process_file(
        self,
        pdf_path: str | os.PathLike,
        page_range: Optional[str] = None,
    ) -> OcrResult:
        """
        Convenience wrapper � reads the file at pdf_path and calls process().

        Parameters
        ----------
        pdf_path : str | Path
            Filesystem path to the PDF.
        page_range : str | None
            Optional page range forwarded to process().
        """
        pdf_path = Path(pdf_path)

        if not pdf_path.exists():
            return OcrResult(
                success=False, markdown="", total_pages=0,
                error=f"File not found: {pdf_path}",
            )

        if pdf_path.suffix.lower() not in cfg.pipeline.supported_extensions:
            return OcrResult(
                success=False, markdown="", total_pages=0,
                error=(
                    f"Unsupported file type '{pdf_path.suffix}'. "
                    f"Supported: {cfg.pipeline.supported_extensions}"
                ),
            )

        logger.info("RoyalOcrService.process_file: reading %s", pdf_path)

        try:
            pdf_bytes = pdf_path.read_bytes()
        except OSError as exc:
            return OcrResult(
                success=False, markdown="", total_pages=0,
                error=f"Could not read file: {exc}",
            )

        return self.process(
            pdf_bytes=pdf_bytes,
            filename=pdf_path.name,
            page_range=page_range,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_page_cap(self, page_range: Optional[str]) -> Optional[str]:
        """
        If cfg.pipeline.max_pages is set and no page_range was supplied,
        generate a capping range "1-{max_pages}".
        Explicit page_range is left untouched.
        """
        if self._max_pages is None:
            return page_range

        if page_range:
            logger.info(
                "RoyalOcrService: max_pages=%d but explicit page_range=%r "
                "supplied � not overriding",
                self._max_pages, page_range,
            )
            return page_range

        capped = f"1-{self._max_pages}"
        logger.info(
            "RoyalOcrService: applying page cap � restricting to %s", capped
        )
        return capped

    def _classify_pages(
        self,
        page_map: dict[int, str],
        total_pages: int,
        raw_markdown: str,
    ) -> tuple[list[int], list[int], list[int]]:
        """
        Return (processed_pages, empty_pages, failed_pages) by inspecting
        the content of each entry in page_map and the raw_markdown.
        """
        empty_marker  = "[Empty page - no content]"
        failed_marker = "[OCR extraction failed"

        processed: list[int] = []
        empty:     list[int] = []
        failed:    list[int] = []

        for page_num in range(1, total_pages + 1):
            content = page_map.get(page_num)
            if content is None:
                failed.append(page_num)
                continue
            if empty_marker in content:
                empty.append(page_num)
            elif failed_marker in content:
                failed.append(page_num)
            else:
                processed.append(page_num)

        # Re-scan raw markdown for empty-page markers with different wording
        raw_empty = re.findall(
            r"#\s*[Pp]age\s+(\d+)\s*\n+\[Empty page", raw_markdown
        )
        for n in raw_empty:
            pg = int(n)
            if pg not in empty:
                empty.append(pg)
                if pg in processed:
                    processed.remove(pg)

        empty.sort()
        failed.sort()
        processed.sort()

        logger.info(
            "RoyalOcrService: pages classified � "
            "processed=%s  empty=%s  failed=%s",
            processed, empty, failed,
        )
        return processed, empty, failed

    def _save_debug(
        self,
        filename: str,
        raw_markdown: str,
        page_map: dict[int, str],
        final_markdown: str,
        total_pages: int,
    ) -> None:
        try:
            work_dir = Path(self._work_dir)
            work_dir.mkdir(parents=True, exist_ok=True)
            stem = Path(filename).stem

            (work_dir / f"{stem}_ocr_raw.md").write_text(
                raw_markdown, encoding="utf-8"
            )
            (work_dir / f"{stem}_ocr_page_map.json").write_text(
                json.dumps(
                    {str(k): v for k, v in page_map.items()},
                    ensure_ascii=False, indent=2,
                ),
                encoding="utf-8",
            )
            (work_dir / f"{stem}_ocr_final.md").write_text(
                final_markdown, encoding="utf-8"
            )
            logger.info(
                "RoyalOcrService [debug]: artefacts saved ? %s", work_dir
            )
        except Exception as exc:
            logger.warning(
                "RoyalOcrService [debug]: could not save debug files � %s", exc
            )