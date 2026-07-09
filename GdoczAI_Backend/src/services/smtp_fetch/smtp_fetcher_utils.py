# -*- coding: utf-8 -*-
"""
Utility functions for Gmail to OCR smtp fetcher.
Provides PDF extraction, document type detection, SHA-256 hashing,
filename sanitization, and disk management utilities.
"""
import functools
import hashlib
import logging
import os
import re
import shutil
import time
from datetime import datetime
from io import BytesIO
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

def extract_text_from_pdf(pdf_bytes: bytes, max_pages: int = 5) -> Tuple[bool, str]:

    try:
        import fitz  # PyMuPDF

        logger.debug(" Starting PDF text extraction with PyMuPDF")

        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = len(pdf_document)
        pages_to_extract = min(total_pages, max_pages)

        logger.debug(
            f" PDF has {total_pages} pages  "
            f"extracting first {pages_to_extract}"
        )

        extracted_text = ""
        for page_num in range(pages_to_extract):
            page = pdf_document[page_num]
            extracted_text += page.get_text() + "\n"

        pdf_document.close()
        extracted_text = extracted_text.strip()

        if not extracted_text:
            logger.warning("  PyMuPDF extraction returned empty text")
            return False, "PDF contains no extractable text"

        logger.info(
            f" Extracted {len(extracted_text)} characters "
            f"from {pages_to_extract} page(s) via PyMuPDF"
        )
        return True, extracted_text

    except ImportError:
        error_msg = (
            "PyMuPDF (fitz) not installed. "
            "Install with: pip install pymupdf --break-system-packages"
        )
        logger.error(f" {error_msg}")
        return False, error_msg

    except Exception as e:
        error_msg = f"Failed to extract text from PDF (PyMuPDF): {str(e)}"
        logger.error(f" {error_msg}", exc_info=True)
        return False, error_msg


def extract_text_from_pdf_fallback(
    pdf_bytes: bytes, max_pages: int = 5
) -> Tuple[bool, str]:

    try:
        import pdfplumber

        logger.debug(" Starting PDF text extraction with pdfplumber (fallback)")

        pdf_file = BytesIO(pdf_bytes)
        extracted_text = ""

        with pdfplumber.open(pdf_file) as pdf:
            total_pages = len(pdf.pages)
            pages_to_extract = min(total_pages, max_pages)

            logger.debug(
                f" PDF has {total_pages} pages  "
                f"extracting first {pages_to_extract}"
            )

            for page_num in range(pages_to_extract):
                page_text = pdf.pages[page_num].extract_text()
                if page_text:
                    extracted_text += page_text + "\n"

        extracted_text = extracted_text.strip()

        if not extracted_text:
            logger.warning("pdfplumber extraction returned empty text")
            return False, "PDF contains no extractable text"

        logger.info(
            f" Extracted {len(extracted_text)} characters "
            f"from {pages_to_extract} page(s) via pdfplumber"
        )
        return True, extracted_text

    except ImportError:
        error_msg = (
            "pdfplumber not installed. "
            "Install with: pip install pdfplumber --break-system-packages"
        )
        logger.error(f" {error_msg}")
        return False, error_msg

    except Exception as e:
        error_msg = f"Failed to extract text from PDF (pdfplumber): {str(e)}"
        logger.error(f" {error_msg}", exc_info=True)
        return False, error_msg

def extract_pdf_text(pdf_bytes: bytes, max_pages: int = 5) -> Tuple[bool, str]:

    success, result = extract_text_from_pdf(pdf_bytes, max_pages)
    if success:
        return True, result

    logger.warning("  PyMuPDF failed  trying pdfplumber fallback...")
    return extract_text_from_pdf_fallback(pdf_bytes, max_pages)

def detect_document_type_from_text(text: str) -> Optional[str]:

    if not text:
        logger.warning("  Empty text provided for document type detection")
        return None

    # Collapse whitespace and lowercase for reliable pattern matching
    normalised = re.sub(r"\s+", " ", text.lower())

    logger.debug(
        f" Detecting document type from text ({len(text)} chars)"
    )

    keyword_patterns = [
        (r"customer\s*e?invoice",    "Service Request",   "Customer Invoice/eInvoice"),
        (r"otc\s*invoice",           "OTC Invoice",        "OTC Invoice"),
        (r"tax\s*invoice",           "Co-Dealer Invoice",  "Tax Invoice"),
        (r"insurance\s*e?invoice",   "Insurance Invoice",  "Insurance eInvoice"),
        (r"mobilo\s*invoice",        "Mobilo",             "Mobilo Invoice"),
        (r"star\s*ease\s*invoice",   "Star Ease",          "Star Ease Invoice"),
    ]

    for pattern, document_type, keyword_desc in keyword_patterns:
        if re.search(pattern, normalised):
            logger.info(
                f" Document type detected: '{document_type}' "
                f"(matched: '{keyword_desc}')"
            )
            return document_type

    logger.warning("  No matching keywords found in PDF text")
    logger.debug(f" Text sample (first 200 chars): {text[:200]}")
    return None

def detect_document_type_with_logging(
    text: str, filename: str
) -> Optional[str]:

    logger.info(f" Analysing PDF for document type: {filename}")
    logger.debug(f" Text length: {len(text)} characters")

    document_type = detect_document_type_from_text(text)

    if document_type:
        logger.info(
            f" Document type '{document_type}' detected for: {filename}"
        )
    else:
        logger.warning(f"  Could not detect document type for: {filename}")
        logger.debug(f" First 500 chars:\n{text[:500]}")

    return document_type

def compute_sha256_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()

def is_duplicate_by_hash(
    file_bytes: bytes,
    search_dir: str,
    hash_prefix_length: int = 16,
) -> Tuple[bool, str]:

    sha256 = compute_sha256_hash(file_bytes)
    prefix = sha256[:hash_prefix_length]

    scan_dir = Path(search_dir)
    if not scan_dir.exists():
        return False, sha256

    for existing in scan_dir.iterdir():
        if existing.is_file() and existing.name.startswith(prefix):
            logger.warning(
                f"  Duplicate detected  hash prefix {prefix} "
                f"matches existing file: {existing.name}"
            )
            return True, sha256

    return False, sha256

def build_saved_filename(sha256_hash: str, original_filename: str) -> str:

    prefix = sha256_hash[:16]
    # Strip any leading hash prefix that might already be on the original
    clean_original = original_filename
    if re.match(r"^[0-9a-f]{16}_", original_filename):
        clean_original = original_filename[17:]  # strip existing prefix

    return f"{prefix}_{clean_original}"

def parse_saved_filename_hash(saved_filename: str) -> Optional[str]:

    match = re.match(r"^([0-9a-f]{16})_", saved_filename)
    return match.group(1) if match else None

def is_valid_pdf_filename(filename: str) -> bool:

    if not filename:
        return False
    if not filename.lower().endswith(".pdf"):
        return False
    if "\x00" in filename or "/" in filename or "\\" in filename:
        return False
    return True

def sanitize_filename(filename: str) -> str:

    if not filename:
        return f"attachment_{get_timestamp_string()}.pdf"

    # Strip MIME encoded-word syntax before anything else
    filename = re.sub(r"=\?[^?]+\?[BbQq]\?[^?]*\?=", "", filename).strip()

    # Replace path separators
    filename = filename.replace("/", "_").replace("\\", "_")

    # Remove null bytes and control characters
    filename = re.sub(r"[\x00-\x1f\x7f]", "", filename)

    # Replace shell-special and filesystem-illegal characters
    filename = re.sub(r'[*?\[\]|<>;:&"\'`!@#$%^(){}=+,~]', "_", filename)

    # Replace spaces with underscores
    filename = filename.replace(" ", "_")

    # Collapse consecutive underscores
    filename = re.sub(r"_+", "_", filename)

    # Strip leading/trailing dots and underscores
    filename = filename.strip("._")

    # Ensure .pdf extension (force lowercase)
    stem = Path(filename).stem
    if not stem:
        stem = f"attachment_{get_timestamp_string()}"

    filename = f"{stem}.pdf"

    # Enforce maximum byte length (ext4 limit = 255 bytes)
    while len(filename.encode("utf-8")) > 240:
        stem = stem[:-1]
        filename = f"{stem}.pdf"

    return filename or f"attachment_{get_timestamp_string()}.pdf"

def get_file_extension(filename: str) -> str:
    return Path(filename).suffix.lower()

def validate_sender_email(address: str) -> bool:

    if not address:
        return False
    pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, address.strip()))

def normalise_sender_address(raw_from_header: str) -> str:

    if not raw_from_header:
        return ""

    import email.utils as eu
    try:
        _display_name, address = eu.parseaddr(raw_from_header)
        return address.lower().strip()
    except Exception:
        # Fallback: regex extraction
        match = re.search(
            r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
            raw_from_header,
        )
        return match.group(0).lower() if match else ""

def is_approved_sender(
    raw_from_header: str, approved_senders: List[str]
) -> Tuple[bool, str]:

    sender = normalise_sender_address(raw_from_header)
    approved_lower = [s.lower().strip() for s in approved_senders]
    return (sender in approved_lower, sender)

def is_valid_local_dir_path(path: str) -> bool:

    if not path:
        return False
    if "\x00" in path:
        return False
    return True

def ensure_local_directory(dir_path: str) -> bool:

    try:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        logger.debug(f" Directory ready: {dir_path}")
        return True
    except OSError as e:
        logger.error(f" Failed to create directory '{dir_path}': {e}")
        return False

def get_disk_free_mb(path: str) -> float:

    try:
        stat = shutil.disk_usage(path)
        return stat.free / (1024 * 1024)
    except Exception as e:
        logger.warning(f"  Could not check disk space for '{path}': {e}")
        return -1.0

def is_safe_to_save(
    file_bytes: bytes,
    target_dir: str,
    minimum_free_mb: float = 100.0,
) -> Tuple[bool, str]:

    if not file_bytes:
        return False, "File bytes are empty  attachment may be corrupt"

    file_size_mb = len(file_bytes) / (1024 * 1024)

    if file_size_mb > 500:
        return False, f"File size {file_size_mb:.1f} MB exceeds 500 MB safety limit"

    if not Path(target_dir).exists():
        # Directory will be created  check parent
        check_path = str(Path(target_dir).parent)
    else:
        check_path = target_dir

    free_mb = get_disk_free_mb(check_path)

    if free_mb < 0:
        # Could not determine free space  allow save but log warning
        logger.warning(
            "  Could not verify disk space  proceeding with save"
        )
        return True, "ok"

    required_mb = file_size_mb + minimum_free_mb
    if free_mb < required_mb:
        return (
            False,
            f"Insufficient disk space: {free_mb:.1f} MB free, "
            f"need {required_mb:.1f} MB "
            f"({file_size_mb:.1f} MB file + {minimum_free_mb:.1f} MB buffer)",
        )

    return True, "ok"

def get_file_age_hours(file_path: str) -> float:

    try:
        mtime = Path(file_path).stat().st_mtime
        age_seconds = time.time() - mtime
        return age_seconds / 3600
    except FileNotFoundError:
        return -1.0
    except Exception as e:
        logger.warning(f"  Could not get age of '{file_path}': {e}")
        return -1.0

def list_orphaned_downloads(
    download_dir: str,
    older_than_hours: float = 24.0,
) -> List[str]:

    stale: List[str] = []
    scan_dir = Path(download_dir)

    if not scan_dir.exists():
        return stale

    for entry in scan_dir.iterdir():
        if not entry.is_file():
            continue
        if not entry.suffix.lower() == ".pdf":
            continue
        age = get_file_age_hours(str(entry))
        if age >= older_than_hours:
            stale.append(str(entry.resolve()))
            logger.debug(
                f"  Orphaned file ({age:.1f}h old): {entry.name}"
            )

    if stale:
        logger.warning(
            f"  Found {len(stale)} orphaned download(s) "
            f"older than {older_than_hours}h in {download_dir}"
        )

    return stale

def cleanup_orphaned_downloads(
    download_dir: str,
    failed_dir: str,
    older_than_hours: float = 24.0,
) -> Tuple[int, int]:

    orphans = list_orphaned_downloads(download_dir, older_than_hours)

    if not orphans:
        logger.info(f" No orphaned downloads found in {download_dir}")
        return 0, 0

    logger.info(
        f"  Moving {len(orphans)} orphaned file(s) to failed_dir: {failed_dir}"
    )

    ensure_local_directory(failed_dir)

    moved = 0
    errors = 0

    for orphan_path in orphans:
        try:
            source = Path(orphan_path)
            dest_dir = Path(failed_dir)
            dest = dest_dir / source.name

            # Timestamp suffix if name already exists in failed_dir
            if dest.exists():
                timestamp = get_timestamp_string()
                dest = dest_dir / f"{source.stem}_{timestamp}{source.suffix}"

            shutil.move(str(source), str(dest))
            logger.info(
                f"  Moved orphan to failed_dir: {source.name}  {dest.name}"
            )
            moved += 1

        except Exception as e:
            logger.error(
                f" Failed to move orphaned file '{orphan_path}': {e}"
            )
            errors += 1

    logger.info(
        f"  Orphan cleanup complete: {moved} moved, {errors} errors"
    )
    return moved, errors

def retry_on_failure(
    max_attempts: int = 3,
    delay_seconds: int = 5,
    backoff_multiplier: float = 1.0,
    exceptions: tuple = (Exception,),
):

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 1
            current_delay = delay_seconds

            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)

                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(
                            f" {func.__name__} failed after "
                            f"{max_attempts} attempt(s): {e}"
                        )
                        raise

                    logger.warning(
                        f"  Attempt {attempt}/{max_attempts} failed "
                        f"for {func.__name__}: {e}"
                    )
                    logger.info(f" Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff_multiplier
                    attempt += 1

            return None  # Unreachable but satisfies type checkers

        return wrapper
    return decorator

def log_execution_time(func: Callable) -> Callable:

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start = time.time()
        logger.debug(f"  Starting: {func.__name__}")
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start
            logger.info(f" Completed: {func.__name__} in {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start
            logger.error(
                f" Failed: {func.__name__} after {duration:.2f}s  {e}"
            )
            raise

    return wrapper

def format_duration(seconds: float) -> str:

    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)

    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"

    hours = minutes // 60
    remaining_minutes = minutes % 60
    return f"{hours}h {remaining_minutes}m {remaining_seconds}s"

def format_bytes(size_bytes: int) -> str:

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def bytes_to_mb(size_bytes: int) -> float:
    return size_bytes / (1024 * 1024)

def validate_document_type(document_type: str) -> bool:

    if not document_type:
        return False
    if len(document_type) > 100:
        return False

    if not re.match(r"^[a-zA-Z0-9 _\-]+$", document_type):
        return False
    return True

def setup_file_logger(
    log_file: str,
    level: str = "INFO",
    format_string: Optional[str] = None,
    logger_name: str = "email_fetch",
) -> logging.Logger:

    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    file_logger = logging.getLogger(logger_name)
    file_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Rotating: 10 MB per file, keep 5 backups
    handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    formatter = logging.Formatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    # Avoid duplicate handlers if called more than once
    if not any(
        isinstance(h, RotatingFileHandler) and h.baseFilename == handler.baseFilename
        for h in file_logger.handlers
    ):
        file_logger.addHandler(handler)

    logger.info(f" File logging enabled: {log_file} (level={level})")
    return file_logger

def log_separator(title: str = "", char: str = "=", width: int = 80):

    if title:
        logger.info(char * width)
        logger.info(title.center(width))
        logger.info(char * width)
    else:
        logger.info(char * width)

def safe_cleanup_temp_file(file_path: str):

    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"  Cleaned up temp file: {file_path}")
    except Exception as e:
        logger.warning(f"  Failed to clean up temp file '{file_path}': {e}")

def cleanup_temp_files(file_paths: List[str]):

    for file_path in file_paths:
        safe_cleanup_temp_file(file_path)

def get_timestamp_string(fmt: str = "%Y%m%d_%H%M%S") -> str:
    return datetime.now().strftime(fmt)

def parse_iso_datetime(datetime_string: str) -> Optional[datetime]:

    try:
        return datetime.fromisoformat(datetime_string)
    except (ValueError, TypeError):
        return None

def safe_get_nested(data: dict, keys: List[str], default: Any = None) -> Any:

    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def flatten_dict(
    data: dict, parent_key: str = "", separator: str = "."
) -> dict:

    items: List[Tuple[str, Any]] = []
    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    return dict(items)

def safe_execute(func: Callable, *args, **kwargs) -> Tuple[bool, Any]:

    try:
        result = func(*args, **kwargs)
        return True, result
    except Exception as e:
        logger.error(f" Error in {func.__name__}: {e}", exc_info=True)
        return False, str(e)


def get_error_details(exception: Exception) -> Dict[str, Any]:

    return {
        "type": type(exception).__name__,
        "message": str(exception),
        "args": exception.args,
    }