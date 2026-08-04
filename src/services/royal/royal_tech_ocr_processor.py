# -*- coding: utf-8 -*-
"""
royal_tech_ocr_processor.py � OLMOCR PDF processor for the Royal Tech pipeline.

Direct refactor of mineru_ocr_server_processor.py:
  � Config-driven DPI, retries, tokens, temperature, top_p, empty-page
    thresholds � all read from royal_tech_config.cfg.olmocr
  � Class name MinerUProcessor kept for backward compatibility with
    royal_tech_ocr_service.py which imports it by that name
  � All OCR logic, anti-hallucination prompt, parallel strategy, and
    image-processing code preserved exactly

Public API (unchanged)
----------
    from royal_tech_ocr_processor import MinerUProcessor
    proc = MinerUProcessor(api_key, model, timeout, batch_size)
    ok, md, pages, err = proc.process_pdf(pdf_bytes, filename, page_range)
    ok, md, pages, err = proc.process_image(image_bytes, filename, ext)
"""

import base64
import concurrent.futures
import io
import logging
import time
from typing import Optional, Tuple

import requests
from pdf2image import convert_from_bytes
from PIL import Image

from src.services.royal.royal_tech_config import cfg

logger = logging.getLogger(__name__)

# Anti-hallucination rules body � shared by both single-page and multi-page calls
_ANTI_HALLUCINATION_RULES = """\
ABSOLUTE RULES - VIOLATIONS WILL CAUSE FAILURE:

1. EXTRACT ONLY WHAT YOU SEE
   - Extract ONLY text that is PHYSICALLY VISIBLE in this image
   - DO NOT add any text, numbers, or content that is not in the image
   - DO NOT invent, create, or fabricate ANY content
   - DO NOT add example data or placeholder content
   - DO NOT continue patterns beyond what is visible

2. ANTI-HALLUCINATION REQUIREMENTS
   - If a table has 3 rows visible, extract ONLY those 3 rows
   - If a section is empty, output NOTHING for that section
   - DO NOT generate sample data or examples
   - DO NOT fill in missing information with assumptions
   - DO NOT create content based on document type expectations
   - STOP extracting when the visible content ends

3. COMPLETE EXTRACTION OF VISIBLE CONTENT
   - Extract EVERY visible word, number, and symbol
   - Include ALL headers, column names, and labels
   - Capture ALL table rows that are actually present
   - Include ALL dates, amounts, codes, and identifiers
   - Preserve ALL formatting (bold, italic, structure)

4. TABLE EXTRACTION RULES
   - Extract table headers exactly as shown
   - Extract ONLY the rows that are visible in the image
   - DO NOT add empty rows or sample rows
   - DO NOT extend tables beyond visible content
   - Use proper markdown table format: | column1 | column2 |

5. TEXT ORGANIZATION
   - Use # for main headers visible in image
   - Use ## for subheaders visible in image
   - Use * or - for bullet points that exist
   - Use **bold** for emphasized text (if visible)
   - Use > for quotes (if present)

6. WHAT NOT TO DO (CRITICAL)
   - DO NOT invent product names or data
   - DO NOT create fictional examples
   - DO NOT add explanatory text not in image
   - DO NOT generate template content
   - DO NOT fill tables with made-up data
   - DO NOT add "example" or "sample" entries
   - DO NOT continue numbered lists beyond what exists
   - DO NOT create symmetric patterns that don't exist

7. VALIDATION CHECK
   - Before outputting, verify EVERY line exists in the image
   - Remove any content you are not 100% certain is visible
   - If unsure about text, DO NOT include it
   - Better to miss minor text than to hallucinate content

8. OUTPUT FORMAT
   - Clean markdown with ONLY visible content
   - NO additional commentary or explanations
   - NO example data or placeholders
   - ONLY what is physically in the image

REMEMBER: Extract EXACTLY what you see. Nothing more, nothing less. Hallucination = FAILURE.\
"""


class MinerUProcessor:
    """
    OLMOCR processor using DeepInfra API for PDF ? Markdown conversion.
    Class name kept as 'MinerUProcessor' for backward compatibility.

    All non-empty pages are submitted to OLMOCR simultaneously via
    ThreadPoolExecutor (full parallel, no intermediate batch grouping).
    Config-driven tuning: DPI, retries, token limit, temperature, top_p,
    and empty-page detection thresholds all come from cfg.olmocr.
    """

    def __init__(
        self,
        api_key: str = None,
        model: str = None,
        timeout: int = None,
        gpu_id: int = 0,
        batch_size: int = None,
    ) -> None:
        ocfg = cfg.olmocr
        self.api_key    = api_key    if api_key    is not None else ocfg.api_key
        self.model      = model      if model      is not None else ocfg.model
        self.timeout    = timeout    if timeout    is not None else ocfg.timeout
        self.gpu_id     = gpu_id
        self.batch_size = batch_size if batch_size is not None else ocfg.batch_size

        self._pdf_dpi          = ocfg.pdf_dpi
        self._max_retries      = ocfg.max_retries_per_page
        self._max_tokens       = ocfg.max_tokens_per_page
        self._temperature      = ocfg.temperature
        self._top_p            = ocfg.top_p
        self._var_threshold    = ocfg.empty_page_variance_threshold
        self._bright_threshold = ocfg.empty_page_bright_threshold
        self._dark_threshold   = ocfg.empty_page_dark_threshold
        self.api_url = "https://api.deepinfra.com/v1/openai/chat/completions"

        logger.info(
            "MinerUProcessor (OLMOCR) initialised � model=%s  dpi=%d  "
            "retries=%d  mode=FULL_PARALLEL",
            self.model, self._pdf_dpi, self._max_retries,
        )

    # Backward-compat stubs
    def warmup(self) -> None:
        logger.info("OLMOCR warmup skipped (API-based)")

    def _build_content_list(self, pdf_info) -> list:
        return []

    def _save_output_files(self, *args, **kwargs) -> None:
        pass

    # Empty-page detection
    def _is_empty_page(self, image) -> bool:
        try:
            import numpy as np
            arr  = np.array(image.convert("L"))
            var  = float(np.var(arr))
            mean = float(np.mean(arr))
            empty = var < self._var_threshold and (
                mean > self._bright_threshold or mean < self._dark_threshold
            )
            if empty:
                logger.info(
                    "  Empty page detected (variance=%.2f brightness=%.2f)", var, mean
                )
            return empty
        except Exception as exc:
            logger.warning("  Empty-page detection failed: %s � assuming content", exc)
            return False

    # Page-range parser
    def _parse_page_range(self, page_range_str: str, total_pages: int) -> list[int]:
        if not page_range_str or not page_range_str.strip():
            return list(range(1, total_pages + 1))
        try:
            pages: set[int] = set()
            for part in page_range_str.split(","):
                part = part.strip()
                if "-" in part:
                    s, e = part.split("-")
                    pages.update(range(int(s.strip()), int(e.strip()) + 1))
                else:
                    pages.add(int(part))
            valid   = sorted(p for p in pages if 1 <= p <= total_pages)
            invalid = sorted(p for p in pages if p < 1 or p > total_pages)
            if invalid:
                logger.warning(
                    "Invalid page numbers %s (total=%d) � falling back to ALL",
                    invalid, total_pages,
                )
                return list(range(1, total_pages + 1))
            return valid or list(range(1, total_pages + 1))
        except Exception as exc:
            logger.error(
                "Failed to parse page range '%s': %s � falling back to ALL",
                page_range_str, exc,
            )
            return list(range(1, total_pages + 1))

    # Single-page OCR (called in parallel)
    def _process_single_page(
        self, image, page_num: int, total_pages: int, filename: str
    ) -> Tuple[int, Optional[str]]:
        try:
            buf = io.BytesIO()
            image.save(buf, format="PNG", optimize=False)
            img_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            markdown = None
            for attempt in range(self._max_retries):
                markdown = self._call_olmocr_api(img_b64, page_num, total_pages, filename)
                if markdown and len(markdown.strip()) > 10:
                    break
                if attempt < self._max_retries - 1:
                    logger.warning(
                        "  Page %d attempt %d: insufficient content � retrying",
                        page_num, attempt + 1,
                    )
                    time.sleep(1)
            if markdown and len(markdown.strip()) > 10:
                logger.info("  Page %d: %d chars", page_num, len(markdown))
                return page_num, markdown
            logger.error("  Page %d failed after %d attempt(s)", page_num, self._max_retries)
            return page_num, None
        except Exception as exc:
            logger.error("  Page %d error: %s", page_num, exc)
            return page_num, None

    # Public API � PDF
    def process_pdf(
        self,
        pdf_bytes: bytes,
        filename: str,
        page_range: str = None,
    ) -> Tuple[bool, str, int, Optional[str]]:
        """
        Convert a PDF to Markdown using OLMOCR with full parallel processing.
        Returns (success, markdown_content, page_count, error_message).
        """
        try:
            logger.info("Processing PDF (FULL PARALLEL): %s  %d bytes", filename, len(pdf_bytes))
            images      = convert_from_bytes(pdf_bytes, dpi=self._pdf_dpi)
            total_pages = len(images)
            logger.info("PDF converted: %d page(s) at DPI=%d", total_pages, self._pdf_dpi)

            pages_to_process = (
                self._parse_page_range(page_range, total_pages)
                if page_range
                else list(range(1, total_pages + 1))
            )
            logger.info("Pages to process: %s", pages_to_process)

            non_empty: list[int] = []
            empty:     list[int] = []
            for pn in pages_to_process:
                if self._is_empty_page(images[pn - 1]):
                    empty.append(pn)
                    logger.info("  Page %d: EMPTY � skipping", pn)
                else:
                    non_empty.append(pn)

            if empty:
                logger.info("Empty: %s  Content: %s", empty, non_empty)
            if not non_empty:
                logger.warning("All pages empty � nothing to extract")
                return True, "# Document\n\n[All pages are empty]", total_pages, None

            logger.info("Submitting %d page(s) in PARALLEL...", len(non_empty))
            start = time.time()
            results: list[Tuple[int, Optional[str]]] = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(non_empty)) as ex:
                futures = {
                    ex.submit(
                        self._process_single_page,
                        images[pn - 1], pn, total_pages, filename,
                    ): pn
                    for pn in non_empty
                }
                for future in concurrent.futures.as_completed(futures):
                    pn = futures[future]
                    try:
                        results.append(future.result())
                        logger.info("  Page %d complete", pn)
                    except Exception as exc:
                        logger.error("  Page %d exception: %s", pn, exc)
                        results.append((pn, None))

            elapsed = time.time() - start
            logger.info("Parallel OCR complete in %.2fs", elapsed)

            results.sort(key=lambda x: x[0])
            md_blocks:    list[str] = []
            failed_pages: list[int] = []
            success_count = 0

            for pn, md in results:
                if md:
                    md_blocks.append(f"# Page {pn}\n\n{md}")
                    success_count += 1
                else:
                    failed_pages.append(pn)
                    md_blocks.append(f"# Page {pn}\n\n[OCR extraction failed after retries]")

            for ep in empty:
                md_blocks.append(f"# Page {ep}\n\n[Empty page - no content]")

            def _key(t: str) -> int:
                try:
                    return int(t.split("\n")[0].replace("# Page ", ""))
                except Exception:
                    return 999_999

            md_blocks.sort(key=_key)
            final = "\n\n---\n\n".join(md_blocks)

            logger.info(
                "OLMOCR done � total=%d requested=%d empty=%d ocr=%d "
                "ok=%d failed=%d time=%.2fs chars=%d",
                total_pages, len(pages_to_process), len(empty), len(non_empty),
                success_count, len(failed_pages), elapsed, len(final),
            )
            return True, final, total_pages, None

        except Exception as exc:
            msg = f"OLMOCR processing failed: {exc}"
            logger.error("%s", msg, exc_info=True)
            return False, "", 0, msg

    # Public API � single image
    def process_image(
        self,
        image_bytes: bytes,
        filename: str,
        file_extension: str,
    ) -> Tuple[bool, str, int, Optional[str]]:
        """OCR a single image file via OLMOCR. Returns (success, md, 1, error)."""
        try:
            logger.info(
                "Image processing: %s  %d bytes  %s",
                filename, len(image_bytes), file_extension,
            )
            image = Image.open(io.BytesIO(image_bytes))
            if image.mode in ("RGBA", "LA", "P"):
                bg = Image.new("RGB", image.size, (255, 255, 255))
                if image.mode == "P":
                    image = image.convert("RGBA")
                bg.paste(
                    image,
                    mask=image.split()[-1] if image.mode in ("RGBA", "LA") else None,
                )
                image = bg
            elif image.mode != "RGB":
                image = image.convert("RGB")

            buf = io.BytesIO()
            image.save(buf, format="PNG", optimize=False)
            img_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

            markdown = None
            for attempt in range(self._max_retries):
                markdown = self._call_olmocr_api(img_b64, 1, 1, filename)
                if markdown and len(markdown.strip()) > 10:
                    break
                if attempt < self._max_retries - 1:
                    logger.warning(
                        "  Image attempt %d: insufficient content � retrying",
                        attempt + 1,
                    )
                    time.sleep(1)

            if markdown and len(markdown.strip()) > 10:
                logger.info("Image processing successful: %d chars", len(markdown))
                return True, markdown, 1, None
            logger.error("Image processing failed after %d attempt(s)", self._max_retries)
            return True, "[OCR extraction failed after retries]", 1, None

        except Exception as exc:
            msg = f"Image processing error: {exc}"
            logger.error("%s", msg, exc_info=True)
            return False, "", 0, msg

    # Internal � OLMOCR API call
    def _call_olmocr_api(
        self,
        image_base64: str,
        page_num: int,
        total_pages: int,
        filename: str = "",
    ) -> str:
        """POST one page image to DeepInfra OLMOCR. Returns markdown or ""."""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type":  "application/json",
            }
            prefix = (
                f"CRITICAL OCR TASK - Extract text from image {page_num} of {total_pages}\n\n"
                if total_pages > 1
                else "CRITICAL OCR TASK - Extract text from this image\n\n"
            )
            payload = {
                "model": self.model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{image_base64}"
                                },
                            },
                            {"type": "text", "text": prefix + _ANTI_HALLUCINATION_RULES},
                        ],
                    }
                ],
                "max_tokens":        self._max_tokens,
                "temperature":       self._temperature,
                "top_p":             self._top_p,
                "presence_penalty":  0.0,
                "frequency_penalty": 0.0,
            }
            logger.info("OLMOCR API call (page %d/%d)...", page_num, total_pages)
            resp = requests.post(
                self.api_url, headers=headers, json=payload, timeout=self.timeout
            )
            if resp.status_code == 200:
                md = (
                    resp.json()
                    .get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                )
                if md:
                    logger.info(
                        "OLMOCR success (page %d): %d chars", page_num, len(md)
                    )
                else:
                    logger.warning("OLMOCR returned empty content (page %d)", page_num)
                return md.strip()
            logger.error(
                "OLMOCR HTTP %d (page %d): %s",
                resp.status_code, page_num, resp.text[:500],
            )
            return ""
        except requests.exceptions.Timeout:
            logger.error("OLMOCR timeout after %ds (page %d)", self.timeout, page_num)
            return ""
        except requests.exceptions.RequestException as exc:
            logger.error("OLMOCR request failed (page %d): %s", page_num, exc)
            return ""
        except Exception as exc:
            logger.error("OLMOCR call failed (page %d): %s", page_num, exc)
            return ""


__all__ = ["MinerUProcessor"]