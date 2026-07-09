# -*- coding: utf-8 -*-

import os
import time
import logging
import functools
import re
from pathlib import Path
from typing import Callable, Any, Optional, List, Tuple
from datetime import datetime
from io import BytesIO

logger = logging.getLogger(__name__)
# ============================================================================
# PDF TEXT EXTRACTION UTILITIES
# ============================================================================

def extract_text_from_pdf(pdf_bytes: bytes, max_pages: int = 5) -> Tuple[bool, str]:

    try:
        import fitz  # PyMuPDF
        
        logger.debug("?? Starting PDF text extraction with PyMuPDF")
        
        # Open PDF from bytes
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        total_pages = len(pdf_document)
        pages_to_extract = min(total_pages, max_pages)
        
        logger.debug(f"?? PDF has {total_pages} pages, extracting first {pages_to_extract} pages")
        
        extracted_text = ""
        
        # Extract text from each page
        for page_num in range(pages_to_extract):
            page = pdf_document[page_num]
            page_text = page.get_text()
            extracted_text += page_text + "\n"
        
        # Close PDF document
        pdf_document.close()
        
        # Clean up extracted text
        extracted_text = extracted_text.strip()
        
        if not extracted_text:
            logger.warning("?? PDF text extraction returned empty text")
            return False, "PDF contains no extractable text"
        
        text_length = len(extracted_text)
        logger.info(f"? Successfully extracted {text_length} characters from {pages_to_extract} pages")
        
        return True, extracted_text
    
    except ImportError:
        error_msg = "PyMuPDF (fitz) library not installed. Install with: pip install pymupdf"
        logger.error(f"? {error_msg}")
        return False, error_msg
    
    except Exception as e:
        error_msg = f"Failed to extract text from PDF: {str(e)}"
        logger.error(f"? {error_msg}", exc_info=True)
        return False, error_msg


def extract_text_from_pdf_fallback(pdf_bytes: bytes, max_pages: int = 5) -> Tuple[bool, str]:

    try:
        import pdfplumber
        
        logger.debug("?? Starting PDF text extraction with pdfplumber (fallback)")
        
        # Open PDF from bytes
        pdf_file = BytesIO(pdf_bytes)
        
        extracted_text = ""
        
        with pdfplumber.open(pdf_file) as pdf:
            total_pages = len(pdf.pages)
            pages_to_extract = min(total_pages, max_pages)
            
            logger.debug(f"?? PDF has {total_pages} pages, extracting first {pages_to_extract} pages")
            
            # Extract text from each page
            for page_num in range(pages_to_extract):
                page = pdf.pages[page_num]
                page_text = page.extract_text()
                if page_text:
                    extracted_text += page_text + "\n"
        
        # Clean up extracted text
        extracted_text = extracted_text.strip()
        
        if not extracted_text:
            logger.warning("?? PDF text extraction returned empty text")
            return False, "PDF contains no extractable text"
        
        text_length = len(extracted_text)
        logger.info(f"? Successfully extracted {text_length} characters from {pages_to_extract} pages")
        
        return True, extracted_text
    
    except ImportError:
        error_msg = "pdfplumber library not installed. Install with: pip install pdfplumber"
        logger.error(f"? {error_msg}")
        return False, error_msg
    
    except Exception as e:
        error_msg = f"Failed to extract text from PDF: {str(e)}"
        logger.error(f"? {error_msg}", exc_info=True)
        return False, error_msg


def extract_pdf_text(pdf_bytes: bytes, max_pages: int = 5) -> Tuple[bool, str]:

    # Try PyMuPDF first (faster and more reliable)
    success, result = extract_text_from_pdf(pdf_bytes, max_pages)
    
    if success:
        return True, result
    
    # If PyMuPDF fails, try pdfplumber
    logger.warning("?? PyMuPDF extraction failed, trying pdfplumber fallback...")
    success, result = extract_text_from_pdf_fallback(pdf_bytes, max_pages)
    
    return success, result

# ============================================================================
# DOCUMENT TYPE DETECTION UTILITIES
# ============================================================================

def detect_document_type_from_text(text: str) -> Optional[str]:

    if not text:
        logger.warning("?? Empty text provided for document type detection")
        return None
    
    # Normalize text for case-insensitive matching
    # Remove extra spaces and make lowercase for pattern matching
    normalized_text = re.sub(r'\s+', ' ', text.lower())
    
    logger.debug(f"?? Detecting document type from text (length: {len(text)} chars)")
    
    # Define keyword patterns with their corresponding document types
    # Order matters: Check more specific patterns first
    keyword_patterns = [
        # Pattern 1: Customer Invoice or Customer eInvoice  Service Request
        (r'customer\s*e?invoice', 'Service Request', 'Customer Invoice/eInvoice'),
        
        # Pattern 2: OTC Invoice  OTC Invoice
        (r'otc\s*invoice', 'OTC Invoice', 'OTC Invoice'),
        
        # Pattern 3: Tax Invoice  Co-Dealer Invoice
        (r'tax\s*invoice', 'Co-Dealer Invoice', 'Tax Invoice'),
        
        # Pattern 4: Insurance eInvoice  Insurance Invoice
        (r'insurance\s*e?invoice', 'Insurance Invoice', 'Insurance eInvoice'),
        
        # Pattern 5: Mobilo Invoice  Mobilo
        (r'mobilo\s*invoice', 'Mobilo', 'Mobilo Invoice'),
        
        # Pattern 6: Star Ease Invoice  Star Ease
        (r'star\s*ease\s*invoice', 'Star Ease', 'Star Ease Invoice'),
    ]
    
    # Check each pattern
    for pattern, document_type, keyword_desc in keyword_patterns:
        if re.search(pattern, normalized_text):
            logger.info(f"? Document type detected: '{document_type}' (matched keyword: '{keyword_desc}')")
            return document_type
    
    # No matching keyword found
    logger.warning("?? No matching keywords found in PDF text for document type detection")
    logger.debug(f"?? Text sample (first 200 chars): {text[:200]}")
    
    return None


def detect_document_type_with_logging(text: str, filename: str) -> Optional[str]:

    logger.info(f"?? Analyzing PDF for document type: {filename}")
    logger.debug(f"?? Text length: {len(text)} characters")
    
    document_type = detect_document_type_from_text(text)
    
    if document_type:
        logger.info(f"? Document type detected: '{document_type}' for {filename}")
    else:
        logger.warning(f"?? Could not detect document type for {filename}")
        logger.debug(f"?? First 500 characters of text:\n{text[:500]}")
    
    return document_type

# ============================================================================
# FILENAME UTILITIES
# ============================================================================

def is_valid_pdf_filename(filename: str) -> bool:

    if not filename:
        return False
    
    # Check extension
    if not filename.lower().endswith('.pdf'):
        return False
    
    # Check for null bytes or path separators
    if '\x00' in filename or '/' in filename or '\\' in filename:
        return False
    
    return True


def sanitize_filename(filename: str) -> str:
    
    # Remove path separators
    filename = filename.replace('/', '_').replace('\\', '_')
    
    # Remove null bytes
    filename = filename.replace('\x00', '')
    
    # Remove control characters
    filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)
    
    # Limit length (keep extension)
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        max_name_length = 255 - len(ext)
        filename = name[:max_name_length] + ext
    
    return filename

def extract_folder_name(folder_path: str) -> str:
    return Path(folder_path).name

def get_file_extension(filename: str) -> str:
    return Path(filename).suffix.lower()
# ============================================================================
# RETRY DECORATOR
# ============================================================================

def retry_on_failure(
    max_attempts: int = 3,
    delay_seconds: int = 5,
    backoff_multiplier: float = 1.0,
    exceptions: tuple = (Exception,)
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
                            f"? Function {func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise
                    
                    logger.warning(
                        f"?? Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}"
                    )
                    logger.info(f"? Retrying in {current_delay} seconds...")
                    
                    time.sleep(current_delay)
                    
                    # Exponential backoff
                    current_delay *= backoff_multiplier
                    attempt += 1
            
            return None
        
        return wrapper
    return decorator


# ============================================================================
# TIMING UTILITIES
# ============================================================================

def log_execution_time(func: Callable) -> Callable:

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        
        logger.debug(f"?? Starting: {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            logger.info(f"? Completed: {func.__name__} in {duration:.2f}s")
            
            return result
        
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"? Failed: {func.__name__} after {duration:.2f}s: {e}")
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

# ============================================================================
# SIZE UTILITIES
# ============================================================================
def format_bytes(size_bytes: int) -> str:

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    
    return f"{size_bytes:.2f} PB"

def bytes_to_mb(size_bytes: int) -> float:
    return size_bytes / (1024 * 1024)

# ============================================================================
# VALIDATION UTILITIES
# ============================================================================

def is_valid_folder_path(path: str) -> bool:

    if not path:
        return False
    
    # Must start with /
    if not path.startswith('/'):
        return False
    
    # No null bytes
    if '\x00' in path:
        return False
    
    # No Windows-style paths
    if '\\' in path or ':' in path:
        return False
    
    return True

def validate_document_type(document_type: str) -> bool:

    if not document_type:
        return False
    
    # Must be alphanumeric with underscores only
    if not re.match(r'^[a-z0-9_]+$', document_type):
        return False
    
    # Reasonable length
    if len(document_type) > 100:
        return False
    
    return True
# ============================================================================
# LOGGING UTILITIES
# ============================================================================
def setup_file_logger(
    log_file: str,
    level: str = "INFO",
    format_string: Optional[str] = None
) -> logging.Logger:

    from logging.handlers import RotatingFileHandler
    
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create logger
    file_logger = logging.getLogger('pipeline_file_logger')
    file_logger.setLevel(getattr(logging, level.upper()))
    
    # Create rotating file handler (10MB max, keep 5 backups)
    handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    file_logger.addHandler(handler)
    
    logger.info(f"?? File logging enabled: {log_file}")
    
    return file_logger

def log_separator(title: str = "", char: str = "=", width: int = 80):

    if title:
        logger.info(char * width)
        logger.info(title.center(width))
        logger.info(char * width)
    else:
        logger.info(char * width)
# ============================================================================
# TEMPORARY FILE UTILITIES
# ============================================================================
def safe_cleanup_temp_file(file_path: str):

    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"??? Cleaned up temp file: {file_path}")
    except Exception as e:
        logger.warning(f"?? Failed to cleanup temp file {file_path}: {e}")

def cleanup_temp_files(file_paths: List[str]):

    for file_path in file_paths:
        safe_cleanup_temp_file(file_path)
# ============================================================================
# DATE/TIME UTILITIES
# ============================================================================
def get_timestamp_string(format: str = "%Y%m%d_%H%M%S") -> str:
    return datetime.now().strftime(format)

def parse_iso_datetime(datetime_string: str) -> Optional[datetime]:

    try:
        return datetime.fromisoformat(datetime_string)
    except (ValueError, TypeError):
        return None
# ============================================================================
# DICTIONARY UTILITIES
# ============================================================================
def safe_get_nested(data: dict, keys: List[str], default: Any = None) -> Any:

    current = data
    
    for key in keys:
        if not isinstance(current, dict):
            return default
        
        current = current.get(key)
        
        if current is None:
            return default
    
    return current

def flatten_dict(data: dict, parent_key: str = '', separator: str = '.') -> dict:

    items = []
    
    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    
    return dict(items)
# ============================================================================
# ERROR HANDLING UTILITIES
# ============================================================================
def safe_execute(func: Callable, *args, **kwargs) -> tuple:

    try:
        result = func(*args, **kwargs)
        return True, result
    except Exception as e:
        logger.error(f"Error executing {func.__name__}: {e}", exc_info=True)
        return False, str(e)

def get_error_details(exception: Exception) -> dict:

    return {
        'type': type(exception).__name__,
        'message': str(exception),
        'args': exception.args
    }