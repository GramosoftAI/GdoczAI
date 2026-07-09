# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
Qwen VL PDF processor for OCR Server.

Provides:
- Qwen3-VL PDF to Markdown conversion using DeepInfra API
- Image processing support with PDF conversion
- Empty page detection
- Page range support
- Parallel batch processing
"""

import os
import io
import json
import base64
import logging
import requests
from pathlib import Path
from datetime import datetime
from typing import Tuple, Optional
from pdf2image import convert_from_bytes
from PIL import Image

logger = logging.getLogger(__name__)

# ============================================================================
# QWEN VL PROCESSOR CLASS
# ============================================================================
class QwenProcessor:

    PARALLEL_PAGE_THRESHOLD = 20  # pages <= this value -> full parallel (no batching)

    def __init__(self, api_key: str = None, model: str = None, timeout: int = 600, gpu_id: int = 0, batch_size: int = 3):

        if api_key is None:
            from src.services.ocr_pipeline.ocr_server_config import config
            api_key = config.qwenocr_deepinfra_api_key
            model = config.qwenocr_deepinfra_model
            timeout = config.qwenocr_deepinfra_timeout

        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.gpu_id = gpu_id
        self.batch_size = batch_size
        self.api_url = "https://api.deepinfra.com/v1/openai/chat/completions"

        logger.info(f"[OK] Qwen VL Processor initialized")
        logger.info(f"  [>>] Model: {self.model}")
        logger.info(f"  [T] Timeout: {self.timeout}s")
        logger.info(f"  [~] Parallel threshold: <= {self.PARALLEL_PAGE_THRESHOLD} pages -> full parallel (no batching)")
        logger.info(f"  [B] Batch size: {self.batch_size} pages/batch (used only when page count > {self.PARALLEL_PAGE_THRESHOLD})")
        logger.info(f"  [GPU] GPU ID: {self.gpu_id} (not used by Qwen API)")
        logger.info(f"  [*] Features: Empty page detection, Page range support, Smart parallel processing")

    def _is_empty_page(self, image) -> bool:
    
        try:
            import numpy as np

            img_array = np.array(image.convert('L'))
            variance = np.var(img_array)
            mean_brightness = np.mean(img_array)

            is_empty = (variance < 100 and (mean_brightness > 250 or mean_brightness < 5))

            if is_empty:
                logger.info(f"  ?? Empty page detected (variance: {variance:.2f}, brightness: {mean_brightness:.2f})")

            return is_empty

        except Exception as e:
            logger.warning(f"  ?? Empty page detection failed: {e}, assuming page has content")
            return False

    def _parse_page_range(self, page_range_str: str, total_pages: int) -> list:

        if not page_range_str or not page_range_str.strip():
            return list(range(1, total_pages + 1))

        try:
            pages = set()
            parts = page_range_str.split(',')

            for part in parts:
                part = part.strip()
                if '-' in part:
                    start, end = part.split('-')
                    start = int(start.strip())
                    end = int(end.strip())
                    pages.update(range(start, end + 1))
                else:
                    pages.add(int(part))

            valid_pages = sorted([p for p in pages if 1 <= p <= total_pages])
            invalid_pages = sorted([p for p in pages if p < 1 or p > total_pages])

            if invalid_pages:
                logger.warning(f"?? Invalid page numbers detected: {invalid_pages}")
                logger.warning(f"  ?? These pages don't exist in PDF (total pages: {total_pages})")
                logger.warning(f"  ?? Falling back to process ALL pages")
                return list(range(1, total_pages + 1))

            if valid_pages:
                logger.info(f"? Page range parsed: {valid_pages}")
                return valid_pages
            else:
                logger.warning(f"?? No valid pages in range, processing all pages")
                return list(range(1, total_pages + 1))

        except Exception as e:
            logger.error(f"? Failed to parse page range '{page_range_str}': {e}")
            logger.info(f"  ?? Falling back to process ALL pages")
            return list(range(1, total_pages + 1))

    def _process_single_page(self, image, page_num: int, total_pages: int, filename: str) -> Tuple[int, Optional[str]]:

        try:
            img_buffer = io.BytesIO()
            image.save(img_buffer, format='PNG', optimize=False)
            img_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')

            markdown = None
            max_retries = 2

            for attempt in range(max_retries):
                markdown = self._call_qwen_api(img_base64, page_num, total_pages, filename)

                if markdown and len(markdown.strip()) > 10:
                    break
                elif attempt < max_retries - 1:
                    logger.warning(f"?? Page {page_num} attempt {attempt + 1} returned insufficient content, retrying...")
                    import time
                    time.sleep(1)

            if markdown and len(markdown.strip()) > 10:
                logger.info(f"? Page {page_num} processed successfully: {len(markdown)} chars")
                return (page_num, markdown)
            else:
                logger.error(f"? Page {page_num} failed after {max_retries} attempts")
                return (page_num, None)

        except Exception as e:
            logger.error(f"? Error processing page {page_num}: {e}")
            return (page_num, None)

    def _process_batch_parallel(self, batch_pages: list, images: list, total_pages: int, filename: str) -> list:

        import concurrent.futures

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch_pages)) as executor:
            future_to_page = {}
            for page_num in batch_pages:
                image = images[page_num - 1]
                future = executor.submit(self._process_single_page, image, page_num, total_pages, filename)
                future_to_page[future] = page_num

            for future in concurrent.futures.as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"? Batch processing error for page {page_num}: {e}")
                    results.append((page_num, None))

        return results

    def process_pdf(self, pdf_bytes: bytes, filename: str, page_range: str = None) -> Tuple[bool, str, int, Optional[str]]:
  
        try:
            logger.info(f"[>>] Processing PDF with Qwen VL SMART PARALLEL mode: {filename}")
            logger.info(f"[sz] PDF size: {len(pdf_bytes)} bytes")
            logger.info(f"[~] Parallel threshold: {self.PARALLEL_PAGE_THRESHOLD} pages | Batch size (if needed): {self.batch_size}")

            logger.info("[>>] Converting PDF to high-resolution images...")
            logger.info("  [DPI] DPI: 300 (high quality for better text extraction)")
            images = convert_from_bytes(pdf_bytes, dpi=300)
            total_pages = len(images)
            logger.info(f"[OK] Converted PDF to {total_pages} high-resolution images")

            if page_range:
                logger.info(f"[rng] Page range specified: {page_range}")
                pages_to_process = self._parse_page_range(page_range, total_pages)
            else:
                logger.info(f"[rng] No page range specified, processing all {total_pages} pages")
                pages_to_process = list(range(1, total_pages + 1))

            logger.info(f"[rng] Pages to process: {pages_to_process}")

            logger.info("[>>] Detecting empty pages...")
            non_empty_pages = []
            empty_page_numbers = []

            for page_num in pages_to_process:
                image = images[page_num - 1]

                if self._is_empty_page(image):
                    empty_page_numbers.append(page_num)
                    logger.info(f"  [EMPTY] Page {page_num}: EMPTY - Skipping Qwen VL")
                else:
                    non_empty_pages.append(page_num)
                    logger.info(f"  [OK] Page {page_num}: Has content")

            if empty_page_numbers:
                logger.info(f"[OK] Empty page detection complete")
                logger.info(f"  [EMPTY] Empty pages: {empty_page_numbers} (skipped)")
                logger.info(f"  [OK] Valid pages: {non_empty_pages}")
            else:
                logger.info(f"[OK] No empty pages detected, all pages have content")

            if not non_empty_pages:
                logger.warning("[WARN] All pages are empty, nothing to extract")
                return True, "# Document\n\n[All pages are empty]", total_pages, None

            import concurrent.futures
            import time

            start_time = time.time()
            all_results = []

            if len(non_empty_pages) <= self.PARALLEL_PAGE_THRESHOLD:
                # ----------------------------------------------------------------
                # FULL PARALLEL MODE: all pages fired at once (no batching)
                # ----------------------------------------------------------------
                logger.info(
                    f"[>>] FULL PARALLEL MODE: {len(non_empty_pages)} pages "
                    f"(<= threshold {self.PARALLEL_PAGE_THRESHOLD}) -- processing ALL pages simultaneously"
                )

                with concurrent.futures.ThreadPoolExecutor(max_workers=len(non_empty_pages)) as executor:
                    future_to_page = {}
                    for page_num in non_empty_pages:
                        image = images[page_num - 1]
                        future = executor.submit(self._process_single_page, image, page_num, total_pages, filename)
                        future_to_page[future] = page_num
                        logger.info(f"  [>>] Submitted page {page_num} for parallel processing")

                    for future in concurrent.futures.as_completed(future_to_page):
                        page_num = future_to_page[future]
                        try:
                            result = future.result()
                            all_results.append(result)
                            logger.info(f"[OK] Page {page_num} completed")
                        except Exception as e:
                            logger.error(f"[ERR] Page {page_num} parallel error: {e}")
                            all_results.append((page_num, None))

                processing_time = time.time() - start_time
                logger.info(f"[OK] Full parallel processing complete in {processing_time:.2f}s")

            else:
                # ----------------------------------------------------------------
                # BATCH PARALLEL MODE: split into batches, run ALL batches in parallel
                # ----------------------------------------------------------------
                batches = []
                for i in range(0, len(non_empty_pages), self.batch_size):
                    batch = non_empty_pages[i:i + self.batch_size]
                    batches.append(batch)

                logger.info(
                    f"[>>] BATCH PARALLEL MODE: {len(non_empty_pages)} pages "
                    f"(> threshold {self.PARALLEL_PAGE_THRESHOLD}) -- "
                    f"split into {len(batches)} batches of up to {self.batch_size} pages each"
                )
                for idx, batch in enumerate(batches, 1):
                    logger.info(f"  [B{idx}] Batch {idx}: Pages {batch}")

                logger.info(f"[>>] Submitting ALL {len(batches)} batches in PARALLEL...")

                with concurrent.futures.ThreadPoolExecutor(max_workers=len(batches)) as executor:
                    future_to_batch = {}
                    for idx, batch in enumerate(batches, 1):
                        logger.info(f"  [>>] Submitting Batch {idx} ({len(batch)} pages)...")
                        future = executor.submit(self._process_batch_parallel, batch, images, total_pages, filename)
                        future_to_batch[future] = (idx, batch)

                    for future in concurrent.futures.as_completed(future_to_batch):
                        batch_idx, batch_pages = future_to_batch[future]
                        try:
                            batch_results = future.result()
                            all_results.extend(batch_results)
                            logger.info(f"[OK] Batch {batch_idx} completed: Pages {batch_pages}")
                        except Exception as e:
                            logger.error(f"[ERR] Batch {batch_idx} failed: {e}")
                            for page_num in batch_pages:
                                all_results.append((page_num, None))

                processing_time = time.time() - start_time
                logger.info(f"[OK] Batch parallel processing complete in {processing_time:.2f}s (batches: {len(batches)})")

            all_results.sort(key=lambda x: x[0])

            all_markdown = []
            failed_pages = []
            processed_count = 0

            for page_num, markdown in all_results:
                if markdown:
                    all_markdown.append(f"# Page {page_num}\n\n{markdown}")
                    processed_count += 1
                else:
                    failed_pages.append(page_num)
                    all_markdown.append(f"# Page {page_num}\n\n[OCR extraction failed after retries]")

            if empty_page_numbers:
                for empty_page in empty_page_numbers:
                    all_markdown.append(f"# Page {empty_page}\n\n[Empty page - no content]")

            def get_page_num(md_text):
                try:
                    return int(md_text.split('\n')[0].replace('# Page ', ''))
                except:
                    return 999999

            all_markdown.sort(key=get_page_num)

            final_markdown = "\n\n---\n\n".join(all_markdown)

            processing_mode = (
                f"FULL PARALLEL ({len(non_empty_pages)} pages at once)"
                if len(non_empty_pages) <= self.PARALLEL_PAGE_THRESHOLD
                else f"BATCH PARALLEL ({len(non_empty_pages)} pages / batch_size={self.batch_size})"
            )

            logger.info(f"[OK] Qwen VL processing complete")
            logger.info(f"  [mode] Processing mode: {processing_mode}")
            logger.info(f"  [pg] Total pages in PDF: {total_pages}")
            logger.info(f"  [pg] Pages requested: {len(pages_to_process)}")
            logger.info(f"  [pg] Empty pages skipped: {len(empty_page_numbers)}")
            logger.info(f"  [pg] Pages sent to Qwen VL: {len(non_empty_pages)}")
            logger.info(f"  [OK] Successfully extracted: {processed_count}")
            logger.info(f"  [ERR] Failed pages: {len(failed_pages)}")
            if failed_pages:
                logger.warning(f"  [WARN] Failed page numbers: {failed_pages}")
            if empty_page_numbers:
                logger.info(f"  [EMPTY] Empty page numbers: {empty_page_numbers}")
            logger.info(f"  [T] Processing time: {processing_time:.2f}s")
            logger.info(f"  [sz] Total markdown: {len(final_markdown)} chars")

            return True, final_markdown, total_pages, None

        except Exception as e:
            error_msg = f"Qwen VL processing failed: {str(e)}"
            logger.error(f"[ERR] {error_msg}", exc_info=True)
            return False, "", 0, error_msg

    def process_image(self, image_bytes: bytes, filename: str, file_extension: str) -> Tuple[bool, str, int, Optional[str]]:

        try:
            logger.info(f"??? Starting STRICT image processing: {filename}")
            logger.info(f"?? Image size: {len(image_bytes)} bytes")
            logger.info(f"?? Format: {file_extension}")

            logger.info("?? Converting image to high-quality PNG format...")
            image = Image.open(io.BytesIO(image_bytes))

            if image.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', image.size, (255, 255, 255))
                if image.mode == 'P':
                    image = image.convert('RGBA')
                background.paste(image, mask=image.split()[-1] if image.mode in ('RGBA', 'LA') else None)
                image = background
            elif image.mode != 'RGB':
                image = image.convert('RGB')

            img_buffer = io.BytesIO()
            image.save(img_buffer, format='PNG', optimize=False)
            img_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')

            logger.info(f"? Image converted to high-quality PNG: {len(img_base64)} chars (base64)")

            logger.info("?? Processing image with Qwen VL STRICT extraction...")
            markdown = None
            max_retries = 2

            for attempt in range(max_retries):
                markdown = self._call_qwen_api(img_base64, 1, 1, filename)

                if markdown and len(markdown.strip()) > 10:
                    break
                elif attempt < max_retries - 1:
                    logger.warning(f"?? Image attempt {attempt + 1} returned insufficient content, retrying...")
                    import time
                    time.sleep(1)

            if markdown and len(markdown.strip()) > 10:
                logger.info(f"? Image processing successful: {len(markdown)} chars extracted")
                logger.info(f"  ?? STRICT mode: Complete content capture")
                return True, markdown, 1, None
            else:
                logger.error(f"? Image processing failed after {max_retries} attempts")
                return True, "[OCR extraction failed after retries]", 1, None

        except Exception as e:
            error_msg = f"Image processing error: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            return False, "", 0, error_msg

    def _call_qwen_api(self, image_base64: str, page_num: int, total_pages: int, filename: str = "") -> str:

        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            if total_pages > 1:
                prompt = f"""CRITICAL OCR TASK - Extract ALL text from image {page_num} of {total_pages}

THIS IS PAGE {page_num} OF A MULTI-PAGE DOCUMENT.

ABSOLUTE RULES - VIOLATIONS WILL CAUSE FAILURE:

0. PAGE CONTINUATION RULE (MOST IMPORTANT - READ FIRST)
   - This page may BEGIN with table rows or content that are a continuation from the PREVIOUS page
   - A table row at the very TOP of the image with NO header above it means the header is on a previous page
   - You MUST extract these continuation rows - they are REAL DATA, not invented content
   - DO NOT skip rows just because the table header is not visible on this page
   - DO NOT skip the first rows of this page even if they appear to be mid-table continuations
   - Wrap continuation rows in a <table> tag even without headers: <table><tr><td>data</td></tr></table>
   - SCAN FROM THE ABSOLUTE TOP PIXEL OF THE IMAGE - extract everything from line 1

1. EXTRACT ONLY WHAT YOU SEE
   - Extract ONLY text that is PHYSICALLY VISIBLE in this image
   - DO NOT add any text, numbers, or content that is not visible
   - DO NOT invent, create, or fabricate ANY content
   - DO NOT add example data or placeholder content
   - DO NOT extend or continue content beyond what is physically visible on this page

2. ANTI-HALLUCINATION REQUIREMENTS
   - If a table has 3 data rows visible, extract ONLY those 3 rows
   - DO NOT generate sample data or examples
   - DO NOT fill in missing information with assumptions
   - DO NOT create content based on document type expectations
   - STOP extracting when the visible content ends at the BOTTOM of the image

3. COMPLETE EXTRACTION OF VISIBLE CONTENT - NOTHING MUST BE SKIPPED
   - Extract EVERY visible word, number, and symbol from TOP to BOTTOM
   - START extraction from the very first visible content at the top of the image
   - Include ALL section headings, labels, and titles (e.g. PARTS, SUBLET, LABOUR)
   - Include ALL table structures even if they have NO data rows
   - Capture ALL table rows that are actually present INCLUDING rows at the very top of the page
   - Include ALL dates, amounts, codes, and identifiers
   - Preserve ALL formatting (bold, italic, structure)
   - A table with only header row and no data rows is VALID - extract it with just the header row

4. TABLE EXTRACTION RULES
   - Extract table headers exactly as shown
   - If a table is visible with headers but ZERO data rows, still extract the table with its headers
   - Extract ONLY the data rows that are visible in the image - DO NOT invent data rows
   - DO NOT extend tables beyond visible content at the bottom
   - If rows appear at the top of the page WITHOUT a visible header (continuation from prior page), still extract them in a <table>
   - Use HTML table format ONLY: <table><tr><th>header</th></tr><tr><td>data</td></tr></table>
   - Use <th> for header cells and <td> for data cells
   - Use <br> inside cells when a cell contains multiple lines of text
   - DO NOT use markdown pipe table format (| column | column |)

5. TEXT ORGANIZATION
   - Use # for main headers visible in image
   - Use ## for subheaders visible in image
   - Use * or - for bullet points that exist
   - Use **bold** for emphasized text (if visible)
   - Use > for quotes (if present)

6. WHAT NOT TO DO (CRITICAL)
   - DO NOT skip content at the TOP of the page even if it looks like a mid-table continuation
   - DO NOT skip any section heading or table visible in the image
   - DO NOT skip a table just because it has no data rows
   - DO NOT invent product names or data
   - DO NOT create fictional examples
   - DO NOT add explanatory text not in image
   - DO NOT generate template content
   - DO NOT fill tables with made-up data rows
   - DO NOT add "example" or "sample" entries
   - DO NOT continue numbered lists beyond what exists
   - DO NOT create symmetric patterns that don't exist
   - DO NOT use markdown pipe tables (| col | col |) under any circumstances

7. VALIDATION CHECK
   - Before outputting, scan the ENTIRE image from TOP pixel to BOTTOM pixel
   - Confirm the very first content at the top of the image is captured
   - Confirm every section heading is captured
   - Confirm every table structure (even header-only) is captured
   - Confirm every text block is captured
   - Confirm continuation rows at page top are NOT skipped
   - Remove any content you invented that is not physically in the image

8. OUTPUT FORMAT
   - Clean markdown with ONLY visible content
   - Tables MUST be in HTML format (<table>, <tr>, <th>, <td>)
   - NO additional commentary or explanations
   - NO example data or placeholders
   - ONLY what is physically in the image

REMEMBER: Extract EVERYTHING you see from TOP to BOTTOM. Skip NOTHING including page-top continuations. Do not invent data. Header-only tables must still be extracted. Missing top-of-page content = FAILURE. Hallucination = FAILURE."""
            else:
                prompt = """CRITICAL OCR TASK - Extract ALL text from this image

ABSOLUTE RULES - VIOLATIONS WILL CAUSE FAILURE:

0. SCAN FROM TOP TO BOTTOM - START AT LINE 1
   - Begin extraction from the ABSOLUTE TOP of the image - do not skip anything at the top
   - Extract EVERY piece of content from the very first visible line to the very last

1. EXTRACT ONLY WHAT YOU SEE
   - Extract ONLY text that is PHYSICALLY VISIBLE in this image
   - DO NOT add any text, numbers, or content that is not in the image
   - DO NOT invent, create, or fabricate ANY content
   - DO NOT add example data or placeholder content
   - DO NOT extend or continue content beyond what is physically visible

2. ANTI-HALLUCINATION REQUIREMENTS
   - If a table has 3 data rows visible, extract ONLY those 3 rows
   - DO NOT generate sample data or examples
   - DO NOT fill in missing information with assumptions
   - DO NOT create content based on document type expectations
   - STOP extracting when the visible content ends at the BOTTOM of the image

3. COMPLETE EXTRACTION OF VISIBLE CONTENT - NOTHING MUST BE SKIPPED
   - Extract EVERY visible word, number, and symbol from TOP to BOTTOM
   - Include ALL section headings, labels, and titles (e.g. PARTS, SUBLET, LABOUR)
   - Include ALL table structures even if they have NO data rows - header-only tables are REAL and MUST be extracted
   - Capture ALL table rows that are actually present
   - Include ALL dates, amounts, codes, and identifiers
   - Preserve ALL formatting (bold, italic, structure)
   - A table with only header row and no data rows is VALID - extract it with just the header row

4. TABLE EXTRACTION RULES
   - Extract table headers exactly as shown
   - If a table is visible with headers but ZERO data rows, still extract the table with its headers
   - Extract ONLY the data rows that are visible in the image - DO NOT invent data rows
   - DO NOT extend tables beyond visible content
   - Use HTML table format ONLY: <table><tr><th>header</th></tr><tr><td>data</td></tr></table>
   - Use <th> for header cells and <td> for data cells
   - Use <br> inside cells when a cell contains multiple lines of text
   - DO NOT use markdown pipe table format (| column | column |)

5. TEXT ORGANIZATION
   - Use # for main headers visible in image
   - Use ## for subheaders visible in image
   - Use * or - for bullet points that exist
   - Use **bold** for emphasized text (if visible)
   - Use > for quotes (if present)

6. WHAT NOT TO DO (CRITICAL)
   - DO NOT skip any section heading or table visible in the image
   - DO NOT skip a table just because it has no data rows
   - DO NOT invent product names or data
   - DO NOT create fictional examples
   - DO NOT add explanatory text not in image
   - DO NOT generate template content
   - DO NOT fill tables with made-up data rows
   - DO NOT add "example" or "sample" entries
   - DO NOT continue numbered lists beyond what exists
   - DO NOT create symmetric patterns that don't exist
   - DO NOT use markdown pipe tables (| col | col |) under any circumstances

7. VALIDATION CHECK
   - Before outputting, scan the ENTIRE image from TOP pixel to BOTTOM pixel
   - Confirm the very first content at the top of the image is captured
   - Confirm every section heading is captured
   - Confirm every table structure (even header-only) is captured
   - Confirm every text block is captured
   - Remove any content you invented that is not physically in the image

8. OUTPUT FORMAT
   - Clean markdown with ONLY visible content
   - Tables MUST be in HTML format (<table>, <tr>, <th>, <td>)
   - NO additional commentary or explanations
   - NO example data or placeholders
   - ONLY what is physically in the image

REMEMBER: Extract EVERYTHING you see from TOP to BOTTOM. Skip NOTHING. Do not invent data. Header-only tables must still be extracted. Hallucination = FAILURE. Missing content = FAILURE."""

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
                                }
                            },
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ],
                "max_tokens": 8192,
                "temperature": 0.0,
                "top_p": 0.95,
                "presence_penalty": 0.0,
                "frequency_penalty": 0.0
            }

            logger.info(f"?? Calling Qwen VL API with ANTI-HALLUCINATION prompt (page {page_num}/{total_pages})...")

            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=self.timeout
            )

            if response.status_code == 200:
                result = response.json()
                markdown = result.get('choices', [{}])[0].get('message', {}).get('content', '')

                if markdown:
                    logger.info(f"? Qwen VL API success (page {page_num}): {len(markdown)} chars extracted")
                    logger.info(f"  ?? ANTI-HALLUCINATION mode: Extract ONLY visible content")
                else:
                    logger.warning(f"?? Qwen VL API returned empty content (page {page_num})")

                return markdown.strip()
            else:
                logger.error(f"? Qwen VL API error: {response.status_code}")
                logger.error(f"  ?? Response: {response.text[:500]}")
                return ""

        except requests.exceptions.Timeout:
            logger.error(f"? Qwen VL API timeout after {self.timeout}s (page {page_num})")
            return ""
        except requests.exceptions.RequestException as e:
            logger.error(f"? Qwen VL API request failed (page {page_num}): {e}")
            return ""
        except Exception as e:
            logger.error(f"? Qwen VL API call failed (page {page_num}): {e}")
            return ""

    def _build_content_list(self, pdf_info) -> list:
        """Backward compatibility method - not used by Qwen VL"""
        logger.debug("?? _build_content_list called (not used by Qwen VL)")
        return []

    def _save_output_files(self, *args, **kwargs):
        """Backward compatibility method - not used by Qwen VL"""
        logger.debug("?? _save_output_files called (not used by Qwen VL)")
        pass


# ============================================================================
# EXPORT
# ============================================================================
__all__ = ['QwenProcessor']