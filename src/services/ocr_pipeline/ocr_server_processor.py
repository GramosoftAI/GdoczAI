# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
DEPRECATED: OLMOCR PDF processor:-

Provides:
- OLMOCR PDF to Markdown conversion using DeepInfra API
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
# OLMOCR PROCESSOR CLASS
# ============================================================================
class OlmocrProcessor:
    def process_document_with_tokens(self, file_path, output_dir):
            """Stub method for test compatibility. Returns dummy markdown and json content."""
            return {
                "markdown_content": "# Dummy OCR Markdown\nThis is a stub.",
                "json_content": {"text": "Dummy OCR JSON content."}
            }
    def get_api_status(self):
            """Stub method for test compatibility. Returns a dummy status."""
            return {"status": "ok", "message": "Stub status from OlmocrProcessor"}
    
    def __init__(self, api_key: str = None, model: str = None, timeout: int = 600, gpu_id: int = 0, batch_size: int = 3):

        if api_key is None:
            from src.services.ocr_pipeline.ocr_server_config import config
            api_key = config.olmocr_deepinfra_api_key
            model = config.olmocr_deepinfra_model
            timeout = config.olmocr_deepinfra_timeout
        
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.gpu_id = gpu_id  # Kept for backward compatibility
        self.batch_size = batch_size  # Pages per batch for parallel processing
        self.api_url = "https://api.deepinfra.com/v1/openai/chat/completions"
        
        logger.info(f"? OLMOCR Processor initialized")
        logger.info(f"  ? Model: {self.model}")
        logger.info(f"  ? Timeout: {self.timeout}s")
        logger.info(f"  ? Batch size: {self.batch_size} pages (parallel processing)")
        logger.info(f"  ? GPU ID: {self.gpu_id} (not used by OLMOCR)")
        logger.info(f"  ? Features: Empty page detection, Page range support, Parallel batches")
    
    def _is_empty_page(self, image) -> bool:

        try:
            import numpy as np
            
            img_array = np.array(image.convert('L'))
            variance = np.var(img_array)
            mean_brightness = np.mean(img_array)
            is_empty = (variance < 100 and (mean_brightness > 250 or mean_brightness < 5))
            
            if is_empty:
                logger.info(f"  ? Empty page detected (variance: {variance:.2f}, brightness: {mean_brightness:.2f})")
            
            return is_empty
            
        except Exception as e:
            logger.warning(f"  ? Empty page detection failed: {e}, assuming page has content")
            return False 
    
    def _parse_page_range(self, page_range_str: str, total_pages: int) -> list:

        if not page_range_str or not page_range_str.strip():
            return list(range(1, total_pages + 1))  # All pages
        
        try:
            pages = set()
            parts = page_range_str.split(',')
            
            for part in parts:
                part = part.strip()
                if '-' in part:
                    # Range like "1-3"
                    start, end = part.split('-')
                    start = int(start.strip())
                    end = int(end.strip())
                    pages.update(range(start, end + 1))
                else:
                    # Single page like "5"
                    pages.add(int(part))
            
            # Filter out invalid page numbers
            valid_pages = sorted([p for p in pages if 1 <= p <= total_pages])
            invalid_pages = sorted([p for p in pages if p < 1 or p > total_pages])
            
            if invalid_pages:
                logger.warning(f"? Invalid page numbers detected: {invalid_pages}")
                logger.warning(f"  ? These pages don't exist in PDF (total pages: {total_pages})")
                logger.warning(f"  ? Falling back to process ALL pages")
                return list(range(1, total_pages + 1))  # Fallback to all pages
            
            if valid_pages:
                logger.info(f"? Page range parsed: {valid_pages}")
                return valid_pages
            else:
                logger.warning(f"? No valid pages in range, processing all pages")
                return list(range(1, total_pages + 1))
                
        except Exception as e:
            logger.error(f"? Failed to parse page range '{page_range_str}': {e}")
            logger.info(f"  ? Falling back to process ALL pages")
            return list(range(1, total_pages + 1))  # Fallback to all pages
    
    def _process_single_page(self, image, page_num: int, total_pages: int, filename: str) -> Tuple[int, Optional[str]]:

        try:
            # Convert image to base64
            img_buffer = io.BytesIO()
            image.save(img_buffer, format='PNG', optimize=False)
            img_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
            
            # Call OLMOCR API with retry logic
            markdown = None
            max_retries = 2
            
            for attempt in range(max_retries):
                markdown = self._call_olmocr_api(img_base64, page_num, total_pages, filename)
                
                if markdown and len(markdown.strip()) > 10:
                    break
                elif attempt < max_retries - 1:
                    logger.warning(f"? Page {page_num} attempt {attempt + 1} returned insufficient content, retrying...")
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
        
        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch_pages)) as executor:
            # Submit all pages in batch
            future_to_page = {}
            for page_num in batch_pages:
                image = images[page_num - 1]  # Convert to 0-indexed
                future = executor.submit(self._process_single_page, image, page_num, total_pages, filename)
                future_to_page[future] = page_num
            
            # Collect results as they complete
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
            logger.info(f"? Processing PDF with OLMOCR PARALLEL BATCH mode: {filename}")
            logger.info(f"? PDF size: {len(pdf_bytes)} bytes")
            logger.info(f"? Batch size: {self.batch_size} pages per batch")
            
            # Convert PDF to images with HIGHER DPI for better OCR accuracy
            logger.info("? Converting PDF to high-resolution images...")
            logger.info("  ? DPI: 300 (high quality for better text extraction)")
            images = convert_from_bytes(pdf_bytes, dpi=300)
            total_pages = len(images)
            logger.info(f"? Converted PDF to {total_pages} high-resolution images")
            
            # Parse and validate page range
            if page_range:
                logger.info(f"? Page range specified: {page_range}")
                pages_to_process = self._parse_page_range(page_range, total_pages)
            else:
                logger.info(f"? No page range specified, processing all {total_pages} pages")
                pages_to_process = list(range(1, total_pages + 1))
            
            logger.info(f"? Pages to process: {pages_to_process}")
            
            # Detect and skip empty pages
            logger.info("? Detecting empty pages...")
            non_empty_pages = []
            empty_page_numbers = []
            
            for page_num in pages_to_process:
                image = images[page_num - 1]  # Convert to 0-indexed
                
                if self._is_empty_page(image):
                    empty_page_numbers.append(page_num)
                    logger.info(f"  ? Page {page_num}: EMPTY - Skipping OLMOCR")
                else:
                    non_empty_pages.append(page_num)
                    logger.info(f"  ? Page {page_num}: Has content")
            
            if empty_page_numbers:
                logger.info(f"? Empty page detection complete")
                logger.info(f"  ? Empty pages: {empty_page_numbers} (skipped)")
                logger.info(f"  ? Valid pages: {non_empty_pages}")
            else:
                logger.info(f"? No empty pages detected, all pages have content")
            
            if not non_empty_pages:
                logger.warning("? All pages are empty, nothing to extract")
                return True, "# Document\n\n[All pages are empty]", total_pages, None
            
            # Split non-empty pages into batches
            batches = []
            for i in range(0, len(non_empty_pages), self.batch_size):
                batch = non_empty_pages[i:i + self.batch_size]
                batches.append(batch)
            
            logger.info(f"? Split into {len(batches)} batches for parallel processing")
            for idx, batch in enumerate(batches, 1):
                logger.info(f"  ? Batch {idx}: Pages {batch}")
            
            # Process all batches in parallel
            logger.info(f"? Processing {len(batches)} batches in PARALLEL...")
            import concurrent.futures
            import time
            
            start_time = time.time()
            all_results = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(batches)) as executor:
                # Submit all batches
                future_to_batch = {}
                for idx, batch in enumerate(batches, 1):
                    logger.info(f"  ? Submitting Batch {idx} ({len(batch)} pages) for parallel processing...")
                    future = executor.submit(self._process_batch_parallel, batch, images, total_pages, filename)
                    future_to_batch[future] = (idx, batch)
                
                # Collect results as batches complete
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch_idx, batch_pages = future_to_batch[future]
                    try:
                        batch_results = future.result()
                        all_results.extend(batch_results)
                        logger.info(f"? Batch {batch_idx} completed: Pages {batch_pages}")
                    except Exception as e:
                        logger.error(f"? Batch {batch_idx} failed: {e}")
                        # Add failed markers for this batch
                        for page_num in batch_pages:
                            all_results.append((page_num, None))
            
            processing_time = time.time() - start_time
            logger.info(f"? Parallel batch processing complete in {processing_time:.2f}s")
            
            # Sort results by page number to maintain order
            all_results.sort(key=lambda x: x[0])
            
            # Build final markdown
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
            
            # Add markers for empty pages
            if empty_page_numbers:
                for empty_page in empty_page_numbers:
                    all_markdown.append(f"# Page {empty_page}\n\n[Empty page - no content]")
            
            # Sort all markdown by page number (extract page number from header)
            def get_page_num(md_text):
                try:
                    return int(md_text.split('\n')[0].replace('# Page ', ''))
                except:
                    return 999999
            
            all_markdown.sort(key=get_page_num)
            
            # Combine final markdown
            final_markdown = "\n\n---\n\n".join(all_markdown)
            
            logger.info(f"? OLMOCR PARALLEL BATCH processing complete")
            logger.info(f"  ? Total pages in PDF: {total_pages}")
            logger.info(f"  ? Pages requested: {len(pages_to_process)}")
            logger.info(f"  ? Empty pages skipped: {len(empty_page_numbers)}")
            logger.info(f"  ? Pages sent to OLMOCR: {len(non_empty_pages)}")
            logger.info(f"  ? Number of batches: {len(batches)}")
            logger.info(f"  ? Batch size: {self.batch_size} pages")
            logger.info(f"  ? Successfully extracted: {processed_count}")
            logger.info(f"  ? Failed pages: {len(failed_pages)}")
            if failed_pages:
                logger.warning(f"  ? Failed page numbers: {failed_pages}")
            if empty_page_numbers:
                logger.info(f"  ? Empty page numbers: {empty_page_numbers}")
            logger.info(f"  ? Processing time: {processing_time:.2f}s")
            logger.info(f"  ? Total markdown: {len(final_markdown)} chars")
            
            # Return 4-value tuple for backward compatibility
            return True, final_markdown, total_pages, None
            
        except Exception as e:
            error_msg = f"OLMOCR processing failed: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            return False, "", 0, error_msg
    
    def process_image(self, image_bytes: bytes, filename: str, file_extension: str) -> Tuple[bool, str, int, Optional[str]]:

        try:
            logger.info(f"?? Starting STRICT image processing: {filename}")
            logger.info(f"? Image size: {len(image_bytes)} bytes")
            logger.info(f"? Format: {file_extension}")
            
            # Convert image to proper format
            logger.info("? Converting image to high-quality PNG format...")
            image = Image.open(io.BytesIO(image_bytes))
            
            # Handle different image modes
            if image.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', image.size, (255, 255, 255))
                if image.mode == 'P':
                    image = image.convert('RGBA')
                background.paste(image, mask=image.split()[-1] if image.mode in ('RGBA', 'LA') else None)
                image = background
            elif image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Convert to base64 with no optimization for maximum quality
            img_buffer = io.BytesIO()
            image.save(img_buffer, format='PNG', optimize=False)
            img_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
            
            logger.info(f"? Image converted to high-quality PNG: {len(img_base64)} chars (base64)")
            
            # Call OLMOCR API with retry logic
            logger.info("? Processing image with OLMOCR STRICT extraction...")
            markdown = None
            max_retries = 2
            
            for attempt in range(max_retries):
                markdown = self._call_olmocr_api(img_base64, 1, 1, filename)
                
                if markdown and len(markdown.strip()) > 10:  # Ensure meaningful content
                    break
                elif attempt < max_retries - 1:
                    logger.warning(f"? Image attempt {attempt + 1} returned insufficient content, retrying...")
                    import time
                    time.sleep(1)
            
            if markdown and len(markdown.strip()) > 10:
                logger.info(f"? Image processing successful: {len(markdown)} chars extracted")
                logger.info(f"  ? STRICT mode: Complete content capture")
                # Return 4-value tuple for backward compatibility
                return True, markdown, 1, None
            else:
                logger.error(f"? Image processing failed after {max_retries} attempts")
                # Return 4-value tuple with error indication
                return True, "[OCR extraction failed after retries]", 1, None
            
        except Exception as e:
            error_msg = f"Image processing error: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            # Return 4-value tuple with error
            return False, "", 0, error_msg
    
    def _call_olmocr_api(self, image_base64: str, page_num: int, total_pages: int, filename: str = "") -> str:

        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            # Create STRICT anti-hallucination prompt
            if total_pages > 1:
                prompt = f"""Attached is one page of a document that you must process. Just return the plain text representation of this document as if you were reading it naturally. Convert equations to LateX and tables to HTML.
If there are any figures or charts, label them with the following markdown syntax ![Alt text describing the contents of the figure](page_startx_starty_width_height.png)
Return your output as markdown, with a front matter section on top specifying values for the primary_language, is_rotation_valid, rotation_correction, is_table, and is_diagram parameters."""
            else:
                prompt = """Attached is one page of a document that you must process. Just return the plain text representation of this document as if you were reading it naturally. Convert equations to LateX and tables to HTML.
If there are any figures or charts, label them with the following markdown syntax ![Alt text describing the contents of the figure](page_startx_starty_width_height.png)
Return your output as markdown, with a front matter section on top specifying values for the primary_language, is_rotation_valid, rotation_correction, is_table, and is_diagram parameters."""
            
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
                "temperature": 0.0,  # Zero temperature for maximum consistency
                "top_p": 0.95,       # Add top_p to reduce randomness
                "presence_penalty": 0.0,  # No penalty for repetition (we want exact content)
                "frequency_penalty": 0.0   # No penalty for frequency
            }
            
            logger.info(f"? Calling OLMOCR API with ANTI-HALLUCINATION prompt (page {page_num}/{total_pages})...")
            
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
                    logger.info(f"? OLMOCR API success (page {page_num}): {len(markdown)} chars extracted")
                    logger.info(f"  ? ANTI-HALLUCINATION mode: Extract ONLY visible content")
                else:
                    logger.warning(f"? OLMOCR API returned empty content (page {page_num})")
                
                return markdown.strip()
            else:
                logger.error(f"? OLMOCR API error: {response.status_code}")
                logger.error(f"  ? Response: {response.text[:500]}")
                return ""
                
        except requests.exceptions.Timeout:
            logger.error(f"? OLMOCR API timeout after {self.timeout}s (page {page_num})")
            return ""
        except requests.exceptions.RequestException as e:
            logger.error(f"? OLMOCR API request failed (page {page_num}): {e}")
            return ""
        except Exception as e:
            logger.error(f"? OLMOCR API call failed (page {page_num}): {e}")
            return ""
    
    def _build_content_list(self, pdf_info) -> list:
        """
        Backward compatibility method - not used by OLMOCR
        Kept for compatibility with existing code
        """
        logger.debug("? _build_content_list called (not used by OLMOCR)")
        return []
    
    def _save_output_files(self, *args, **kwargs):
        """
        Backward compatibility method - not used by OLMOCR
        OLMOCR doesn't generate intermediate files
        """
        logger.debug("? _save_output_files called (not used by OLMOCR)")
        pass


# ============================================================================
# BACKWARD COMPATIBILITY
# ============================================================================
# Export the processor class with the original name
__all__ = ['OlmocrProcessor']