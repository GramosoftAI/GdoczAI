#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Chandra PDF processor for OCR Server.

Provides:
- Chandra (Datalab Marker API) PDF to HTML/Markdown conversion
- Async submit -> poll -> retrieve workflow
- Image processing support
- Empty page detection
- Page range support (via pypdf page slicing)
- Configurable polling interval and timeout
- Full compatibility with OlmocrProcessor / QwenProcessor interface:
    process_pdf(pdf_bytes, filename, page_range=None)
        -> (success: bool, markdown: str, page_count: int, error: str or None)
    process_image(image_bytes, filename, file_extension)
        -> (success: bool, markdown: str, page_count: int, error: str or None)

API Flow (Datalab Marker):
    POST  https://www.datalab.to/api/v1/marker   <- submit job
    GET   <request_check_url>                     <- poll until complete or failed
    Output: poll_response["html"] or poll_response["markdown"]

Config keys read from config.yaml under 'chandra_datalab' block:
    api_key          : str   (required)
    output_format    : str   (default: "html")     "html" or "markdown"
    mode             : str   (default: "accurate") "accurate" or "fast"
    timeout          : int   (default: 300)  total seconds to wait for job
    poll_interval    : int   (default: 3)    seconds between polls
    max_retries      : int   (default: 2)    per-image retry count
"""

import io
import time
import logging
import requests
from pathlib import Path
from typing import Tuple, Optional

from PIL import Image

logger = logging.getLogger(__name__)


# ============================================================================
# CHANDRA (DATALAB MARKER) PROCESSOR CLASS
# ============================================================================
class ChandraProcessor:
    """
    OCR processor backed by the Datalab Marker API.

    Implements the same 4-tuple interface as OlmocrProcessor and QwenProcessor
    so it can be dropped in anywhere those processors are used:

        (success: bool, markdown: str, page_count: int, error: str or None)
    """

    DATALAB_SUBMIT_URL = "https://www.datalab.to/api/v1/marker"

    def __init__(
        self,
        api_key=None,
        output_format=None,
        mode=None,
        timeout=None,
        poll_interval=None,
        max_retries=None,
    ):
        cfg = self._load_config()

        self.api_key       = api_key       or cfg.get('api_key')
        self.output_format = output_format or cfg.get('output_format', 'html')
        self.mode          = mode          or cfg.get('mode', 'accurate')
        self.timeout       = timeout       or cfg.get('timeout', 300)
        self.poll_interval = poll_interval or cfg.get('poll_interval', 3)
        self.max_retries   = max_retries   or cfg.get('max_retries', 2)

        logger.info("=" * 70)
        logger.info("[OK] Chandra (Datalab Marker) Processor initialized")
        logger.info("  [>>] Output format  : %s", self.output_format)
        logger.info("  [>>] Mode           : %s", self.mode)
        logger.info("  [T]  Timeout        : %ss", self.timeout)
        logger.info("  [~]  Poll interval  : %ss", self.poll_interval)
        logger.info("  [R]  Max retries    : %s", self.max_retries)

        if not self.api_key:
            logger.error("[ERR] Chandra: No Datalab API key found!")
            logger.error("      Add 'chandra_datalab.api_key' to config.yaml")
        else:
            masked = self.api_key[:6] + '*' * max(0, len(self.api_key) - 6)
            logger.info("  [KEY] API key       : %s", masked)

        logger.info("=" * 70)

    # ------------------------------------------------------------------
    # INTERNAL: config loader
    # ------------------------------------------------------------------
    def _load_config(self):
        try:
            import yaml
            with open('config/config.yaml', 'r') as f:
                full_cfg = yaml.safe_load(f) or {}
            cfg = full_cfg.get('chandra_datalab', {})
            logger.info("[OK] Chandra config loaded from config/config.yaml")
            return cfg
        except FileNotFoundError:
            logger.warning("[WARN] config/config.yaml not found - using defaults / env vars")
            return {}
        except Exception as e:
            logger.warning("[WARN] Failed to load config.yaml: %s", e)
            return {}

    # ------------------------------------------------------------------
    # INTERNAL: page range parser
    # ------------------------------------------------------------------
    def _parse_page_range(self, page_range_str, total_pages):
        if not page_range_str or not page_range_str.strip():
            return list(range(1, total_pages + 1))

        try:
            pages = set()
            for part in page_range_str.split(','):
                part = part.strip()
                if '-' in part:
                    start, end = part.split('-')
                    pages.update(range(int(start.strip()), int(end.strip()) + 1))
                else:
                    pages.add(int(part))

            valid   = sorted(p for p in pages if 1 <= p <= total_pages)
            invalid = sorted(p for p in pages if p < 1 or p > total_pages)

            if invalid:
                logger.warning(
                    "[WARN] Invalid page numbers %s - falling back to ALL pages", invalid
                )
                return list(range(1, total_pages + 1))

            return valid if valid else list(range(1, total_pages + 1))

        except Exception as e:
            logger.error("[ERR] Failed to parse page range '%s': %s", page_range_str, e)
            return list(range(1, total_pages + 1))

    # ------------------------------------------------------------------
    # INTERNAL: empty page detector
    # ------------------------------------------------------------------
    def _is_empty_page(self, image):
        try:
            import numpy as np
            arr        = np.array(image.convert('L'))
            variance   = float(np.var(arr))
            brightness = float(np.mean(arr))
            is_empty   = variance < 100 and (brightness > 250 or brightness < 5)
            if is_empty:
                logger.info(
                    "  [SKIP] Empty page detected (var=%.1f, bright=%.1f)",
                    variance, brightness
                )
            return is_empty
        except Exception as e:
            logger.warning("[WARN] Empty page detection failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # INTERNAL: submit job to Datalab
    # ------------------------------------------------------------------
    def _submit_job(self, file_bytes, filename, content_type):
        headers = {"X-Api-Key": self.api_key}
        files   = {"file": (filename, file_bytes, content_type)}
        data    = {
            "output_format": self.output_format,
            "mode"         : self.mode,
        }

        logger.info("[>>] Submitting to Datalab Marker API...")
        logger.info("     filename=%s  format=%s  mode=%s",
                    filename, self.output_format, self.mode)

        try:
            resp = requests.post(
                self.DATALAB_SUBMIT_URL,
                headers=headers,
                files=files,
                data=data,
                timeout=60,
            )

            if resp.status_code != 200:
                logger.error(
                    "[ERR] Datalab submit HTTP %s: %s",
                    resp.status_code, resp.text[:400]
                )
                return None

            result    = resp.json()
            status    = result.get("status", "unknown")
            check_url = result.get("request_check_url")

            logger.info("[OK]  Job submitted. status=%s  check_url=%s", status, check_url)
            return result

        except requests.exceptions.Timeout:
            logger.error("[ERR] Datalab submit timed out (60s)")
            return None
        except Exception as e:
            logger.error("[ERR] Datalab submit error: %s", e)
            return None

    # ------------------------------------------------------------------
    # INTERNAL: poll until complete / failed / timeout
    # ------------------------------------------------------------------
    def _poll_job(self, check_url):
        headers  = {"X-Api-Key": self.api_key}
        deadline = time.time() + self.timeout
        attempt  = 0

        logger.info(
            "[~] Polling Datalab job (timeout=%ss, interval=%ss)...",
            self.timeout, self.poll_interval
        )

        while time.time() < deadline:
            attempt += 1
            try:
                time.sleep(self.poll_interval)
                resp = requests.get(check_url, headers=headers, timeout=30)

                if resp.status_code != 200:
                    logger.warning("[WARN] Poll attempt %s: HTTP %s", attempt, resp.status_code)
                    continue

                poll   = resp.json()
                status = poll.get("status", "unknown")

                logger.info("[~] Poll #%s: status=%s", attempt, status)

                if status == "complete":
                    logger.info("[OK]  Job complete after %s polls", attempt)
                    return poll

                if status == "failed":
                    err = poll.get("error", "unknown error")
                    logger.error("[ERR] Datalab job failed: %s", err)
                    return None

                # still processing - continue loop

            except requests.exceptions.Timeout:
                logger.warning("[WARN] Poll attempt %s timed out, retrying...", attempt)
            except Exception as e:
                logger.warning("[WARN] Poll attempt %s error: %s", attempt, e)

        logger.error(
            "[ERR] Datalab job timed out after %ss (%s polls)", self.timeout, attempt
        )
        return None

    # ------------------------------------------------------------------
    # INTERNAL: extract content from completed poll response
    # ------------------------------------------------------------------
    def _extract_content(self, poll_response):
        if self.output_format == "html":
            content = poll_response.get("html") or poll_response.get("markdown") or ""
        else:
            content = poll_response.get("markdown") or poll_response.get("html") or ""

        page_count = poll_response.get("pages") or poll_response.get("page_count") or 1

        quality = poll_response.get("parse_quality_score")
        if quality is not None:
            logger.info("[OK]  Datalab parse_quality_score = %s", quality)

        return content, int(page_count)

    # ------------------------------------------------------------------
    # INTERNAL: extract page subset from PDF bytes using pypdf
    # ------------------------------------------------------------------
    def _extract_pdf_pages(self, pdf_bytes, page_numbers):
        try:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
            writer = pypdf.PdfWriter()
            for pn in page_numbers:
                writer.add_page(reader.pages[pn - 1])
            out = io.BytesIO()
            writer.write(out)
            logger.info(
                "[OK] Extracted pages %s into new PDF (%s bytes)",
                page_numbers, out.tell()
            )
            return out.getvalue()
        except ImportError:
            logger.warning(
                "[WARN] pypdf not installed - sending full PDF (page_range ignored)"
            )
            return pdf_bytes
        except Exception as e:
            logger.warning(
                "[WARN] Page extraction failed (%s) - sending full PDF", e
            )
            return pdf_bytes

    # ==================================================================
    # PUBLIC: process_pdf
    # ==================================================================
    def process_pdf(self, pdf_bytes, filename, page_range=None):
        """
        Convert a PDF to markdown/HTML via Datalab Marker.

        Args:
            pdf_bytes  : Raw PDF bytes
            filename   : Original filename
            page_range : Optional page range string e.g. "1-3,5"

        Returns:
            (success, content_str, page_count, error_message)
        """
        logger.info("=" * 70)
        logger.info("[>>] Chandra process_pdf: %s", filename)
        logger.info("     size=%s bytes  page_range=%s",
                    len(pdf_bytes), page_range or "ALL")
        logger.info("=" * 70)

        start = time.time()

        if not self.api_key:
            err = "Chandra: Datalab API key not configured"
            logger.error("[ERR] %s", err)
            return False, "", 0, err

        # Detect total pages for page-range handling
        total_pages = 1
        if page_range:
            try:
                import pypdf
                reader      = pypdf.PdfReader(io.BytesIO(pdf_bytes))
                total_pages = len(reader.pages)
                logger.info("[OK] PDF has %s pages", total_pages)
            except ImportError:
                logger.warning(
                    "[WARN] pypdf not installed - page_range ignored; sending full PDF"
                )
                page_range = None
            except Exception as e:
                logger.warning(
                    "[WARN] Could not read PDF page count: %s - sending full PDF", e
                )
                page_range = None

        # Slice PDF to requested pages if page_range specified
        submit_bytes   = pdf_bytes
        submit_name    = filename
        selected_pages = None

        if page_range and total_pages > 0:
            selected_pages = self._parse_page_range(page_range, total_pages)
            logger.info("[OK] Processing pages: %s", selected_pages)

            if len(selected_pages) < total_pages:
                logger.info("[>>] Extracting page subset from PDF...")
                submit_bytes = self._extract_pdf_pages(pdf_bytes, selected_pages)
                stem         = Path(filename).stem
                submit_name  = "{}_pages{}.pdf".format(
                    stem, "_".join(str(p) for p in selected_pages)
                )

        # Submit job
        submit_result = self._submit_job(submit_bytes, submit_name, "application/pdf")
        if not submit_result:
            err = "Chandra: Failed to submit PDF to Datalab Marker API"
            logger.error("[ERR] %s", err)
            return False, "", 0, err

        check_url = submit_result.get("request_check_url")
        if not check_url:
            err = "Chandra: Datalab submit response missing request_check_url"
            logger.error("[ERR] %s", err)
            return False, "", 0, err

        # Poll until done
        poll_result = self._poll_job(check_url)
        if not poll_result:
            err = "Chandra: Datalab job failed or timed out after {}s".format(self.timeout)
            logger.error("[ERR] %s", err)
            return False, "", 0, err

        # Extract content
        content, page_count = self._extract_content(poll_result)

        if not content or not content.strip():
            err = "Chandra: Datalab returned empty content"
            logger.error("[ERR] %s", err)
            return False, "", page_count, err

        if selected_pages:
            page_count = len(selected_pages)

        elapsed = time.time() - start
        logger.info("=" * 70)
        logger.info("[OK]  Chandra process_pdf complete")
        logger.info("      pages=%s  content_len=%s chars  elapsed=%.2fs",
                    page_count, len(content), elapsed)
        logger.info("=" * 70)

        return True, content, page_count, None

    # ==================================================================
    # PUBLIC: process_image
    # ==================================================================
    def process_image(self, image_bytes, filename, file_extension):
        """
        Convert an image file to markdown/HTML via Datalab Marker.

        Args:
            image_bytes   : Raw image bytes
            filename      : Original filename
            file_extension: e.g. '.jpg', '.png'

        Returns:
            (success, content_str, page_count=1, error_message)
        """
        logger.info("=" * 70)
        logger.info("[>>] Chandra process_image: %s (%s)", filename, file_extension)
        logger.info("     size=%s bytes", len(image_bytes))
        logger.info("=" * 70)

        start = time.time()

        if not self.api_key:
            err = "Chandra: Datalab API key not configured"
            logger.error("[ERR] %s", err)
            return False, "", 0, err

        # Normalise image to RGB PNG
        try:
            img = Image.open(io.BytesIO(image_bytes))
            if img.mode not in ('RGB', 'L'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                if img.mode in ('RGBA', 'LA'):
                    background.paste(img, mask=img.split()[-1])
                else:
                    background.paste(img)
                img = background
            buf = io.BytesIO()
            img.save(buf, format='PNG', optimize=False)
            submit_bytes = buf.getvalue()
            submit_name  = Path(filename).stem + '.png'
            content_type = 'image/png'
            logger.info("[OK] Image normalised to PNG: %s bytes", len(submit_bytes))
        except Exception as e:
            logger.warning("[WARN] Image normalisation failed (%s) - sending original", e)
            submit_bytes = image_bytes
            submit_name  = filename
            ext_lower    = file_extension.lower().lstrip('.')
            mime_map     = {
                'jpg' : 'image/jpeg',
                'jpeg': 'image/jpeg',
                'png' : 'image/png',
                'webp': 'image/webp',
                'bmp' : 'image/bmp',
            }
            content_type = mime_map.get(ext_lower, 'image/jpeg')

        # Retry loop
        for attempt in range(1, self.max_retries + 1):
            logger.info("[>>] Image submit attempt %s/%s", attempt, self.max_retries)

            submit_result = self._submit_job(submit_bytes, submit_name, content_type)

            if not submit_result:
                if attempt < self.max_retries:
                    logger.warning("[WARN] Submit failed on attempt %s, retrying...", attempt)
                    time.sleep(2)
                    continue
                err = "Chandra: Failed to submit image to Datalab Marker API"
                logger.error("[ERR] %s", err)
                return False, "", 0, err

            check_url = submit_result.get("request_check_url")
            if not check_url:
                err = "Chandra: Datalab submit response missing request_check_url"
                logger.error("[ERR] %s", err)
                return False, "", 0, err

            poll_result = self._poll_job(check_url)
            if not poll_result:
                if attempt < self.max_retries:
                    logger.warning("[WARN] Poll failed on attempt %s, retrying...", attempt)
                    time.sleep(2)
                    continue
                err = "Chandra: Datalab job failed or timed out after {}s".format(
                    self.timeout
                )
                logger.error("[ERR] %s", err)
                return False, "", 0, err

            content, _ = self._extract_content(poll_result)

            if content and len(content.strip()) > 10:
                elapsed = time.time() - start
                logger.info("=" * 70)
                logger.info("[OK]  Chandra process_image complete (attempt %s)", attempt)
                logger.info("      content_len=%s chars  elapsed=%.2fs",
                            len(content), elapsed)
                logger.info("=" * 70)
                return True, content, 1, None

            if attempt < self.max_retries:
                logger.warning("[WARN] Empty content on attempt %s, retrying...", attempt)
                time.sleep(2)

        err = "Chandra: Datalab returned empty content after all retries"
        logger.error("[ERR] %s", err)
        return False, "", 0, err

    # ------------------------------------------------------------------
    # BACKWARD COMPATIBILITY STUBS
    # ------------------------------------------------------------------
    def _build_content_list(self, pdf_info):
        logger.debug("[DEBUG] _build_content_list called (not used by Chandra)")
        return []

    def _save_output_files(self, *args, **kwargs):
        logger.debug("[DEBUG] _save_output_files called (not used by Chandra)")


# ============================================================================
# EXPORT
# ============================================================================
__all__ = ['ChandraProcessor']