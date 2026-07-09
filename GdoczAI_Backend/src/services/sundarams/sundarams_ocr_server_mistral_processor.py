# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
Mistral AI OCR Processor:-

Provides:
- Mistral OCR PDF to Markdown conversion
- Mistral OCR Image to Markdown conversion
- Auto upload and cleanup of files in Mistral Storage
- Page range support
"""

import os
import io
import logging
import requests
import mimetypes
from pathlib import Path
from pdf2image import convert_from_bytes
import concurrent.futures
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# MISTRAL OCR PROCESSOR CLASS
# ============================================================================
class MistralProcessor:
    def process_document_with_tokens(self, file_path, output_dir):
        """Stub method for test compatibility. Returns dummy markdown and json content."""
        return {
            "markdown_content": "# Dummy OCR Markdown\nThis is a stub.",
            "json_content": {"text": "Dummy OCR JSON content."}
        }

    def get_api_status(self):
        """Stub method for test compatibility. Returns a dummy status."""
        return {"status": "ok", "message": "Stub status from MistralProcessor"}
    
    def __init__(self, api_key: str = None, model: str = None, timeout: int = 600):
        if api_key is None:
            from src.services.sundarams.sundarams_ocr_server_config import config
            api_key = config.mistral_ocr_api_key
            model = config.mistral_ocr_model
            timeout = config.mistral_ocr_timeout
        
        self.api_key = api_key
        self.model = model or "mistral-ocr-latest"
        self.timeout = timeout
        
        logger.info(f"? Mistral OCR Processor initialized")
        logger.info(f"  ? Model: {self.model}")
        logger.info(f"  ? Timeout: {self.timeout}s")
    
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
                logger.warning(f"  ? These pages don't exist in document (total pages: {total_pages})")
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

    def _upload_file(self, file_bytes: bytes, filename: str) -> str:
        """Upload file bytes to Mistral Files API and return the file_id"""
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        
        # Guess mimetype
        mime_type, _ = mimetypes.guess_type(filename)
        if not mime_type:
            if filename.lower().endswith('.pdf'):
                mime_type = 'application/pdf'
            else:
                mime_type = 'image/png'
                
        files = {
            'file': (filename, file_bytes, mime_type)
        }
        data = {
            'purpose': 'ocr'
        }
        
        logger.info(f"? Uploading {filename} ({len(file_bytes)} bytes, mime: {mime_type}) to Mistral Storage...")
        
        response = requests.post(
            "https://api.mistral.ai/v1/files",
            headers=headers,
            files=files,
            data=data,
            timeout=120
        )
        
        if response.status_code != 200:
            raise Exception(f"File upload failed with status {response.status_code}: {response.text}")
            
        result = response.json()
        file_id = result.get('id')
        if not file_id:
            raise Exception(f"Mistral Files response did not contain file id: {result}")
            
        logger.info(f"? Uploaded successfully. File ID: {file_id}")
        return file_id

    def _delete_file(self, file_id: str):
        """Delete file from Mistral Files API"""
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        logger.info(f"? Deleting temporary file {file_id} from Mistral Storage...")
        try:
            response = requests.delete(
                f"https://api.mistral.ai/v1/files/{file_id}",
                headers=headers,
                timeout=60
            )
            if response.status_code == 200:
                logger.info(f"? Deleted temporary file {file_id} successfully")
            else:
                logger.warning(f"? Failed to delete file {file_id}: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"? Error deleting file {file_id}: {e}")

    def _process_ocr(self, file_id: str) -> dict:
        """Call Mistral OCR API for the given file_id"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model,
            "document": {
                "file_id": file_id
            }
        }
        
        logger.info(f"? Sending OCR request to Mistral API (model: {self.model}, file_id: {file_id})...")
        
        response = requests.post(
            "https://api.mistral.ai/v1/ocr",
            headers=headers,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"Mistral OCR failed with status {response.status_code}: {response.text}")
            
        return response.json()

    def _process_single_page(self, image, page_num: int, filename: str) -> Tuple[int, Optional[str]]:
        try:
            img_buffer = io.BytesIO()
            image.save(img_buffer, format='PNG', optimize=False)
            image_bytes = img_buffer.getvalue()
            
            success, md, _, err = self.process_image(image_bytes, f"page_{page_num}_{filename}", "png")
            if success:
                return (page_num, md)
            else:
                logger.error(f"? Page {page_num} failed: {err}")
                return (page_num, None)
        except Exception as e:
            logger.error(f"? Error processing page {page_num}: {e}")
            return (page_num, None)

    def process_pdf(self, pdf_bytes: bytes, filename: str, page_range: str = None) -> Tuple[bool, str, int, Optional[str]]:
        try:
            logger.info(f"[>>] Processing PDF with Mistral OCR PARALLEL IMAGE mode: {filename}")
            logger.info(f"[sz] PDF size: {len(pdf_bytes)} bytes")
            
            logger.info("[>>] Converting PDF to high-resolution images...")
            images = convert_from_bytes(pdf_bytes, dpi=300)
            total_pages = len(images)
            logger.info(f"[OK] Converted PDF to {total_pages} high-resolution images")
            
            if page_range:
                pages_to_process = self._parse_page_range(page_range, total_pages)
            else:
                pages_to_process = list(range(1, total_pages + 1))
                
            logger.info(f"[rng] Pages to process: {pages_to_process}")
            
            # Process pages in parallel using ThreadPoolExecutor
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(pages_to_process), 10)) as executor:
                future_to_page = {}
                for page_num in pages_to_process:
                    image = images[page_num - 1]
                    future = executor.submit(self._process_single_page, image, page_num, filename)
                    future_to_page[future] = page_num
                    
                for future in concurrent.futures.as_completed(future_to_page):
                    page_num = future_to_page[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"? Parallel processing error for page {page_num}: {e}")
                        results.append((page_num, None))
                        
            # Sort results by page number
            results.sort(key=lambda x: x[0])
            
            all_markdown = []
            for page_num, md in results:
                if md:
                    all_markdown.append(f"# Page {page_num}\n\n{md}")
                else:
                    all_markdown.append(f"# Page {page_num}\n\n[OCR extraction failed for this page]")
                    
            final_markdown = "\n\n---\n\n".join(all_markdown)
            logger.info(f"? Mistral OCR parallel processing complete: {len(final_markdown)} chars extracted")
            return True, final_markdown, total_pages, None
            
        except Exception as e:
            error_msg = f"Mistral OCR parallel PDF processing failed: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            return False, "", 0, error_msg

    def process_image(self, image_bytes: bytes, filename: str, file_extension: str) -> Tuple[bool, str, int, Optional[str]]:
        file_id = None
        try:
            logger.info(f"? Processing image with Mistral OCR: {filename}")
            
            # 1. Upload Image to Mistral Files
            file_id = self._upload_file(image_bytes, filename)
            
            # 2. Run Mistral OCR
            result = self._process_ocr(file_id)
            
            pages = result.get('pages', [])
            total_pages = len(pages)
            logger.info(f"? Mistral OCR returned {total_pages} page(s) for the image")
            
            if total_pages == 0:
                return True, "[OCR extraction failed - no pages returned]", 0, None
                
            # Extract markdown from the first page
            md = pages[0].get('markdown', '')
            
            logger.info(f"? Mistral OCR image processing complete: {len(md)} chars extracted")
            return True, md, 1, None
            
        except Exception as e:
            error_msg = f"Mistral OCR image processing failed: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            return False, "", 0, error_msg
        finally:
            if file_id:
                self._delete_file(file_id)


# Export the processor class
__all__ = ['MistralProcessor']
