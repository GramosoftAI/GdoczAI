"""
Test Suite: Storage Layer (File & DB)
Covers: file_storage.py and db_storage_util.py
"""
import pytest
import tempfile
import shutil
from pathlib import Path
import os
import json

@pytest.fixture(scope="module")
def temp_storage_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)

def test_file_storage_basic(temp_storage_dir):
    from src.core.storage.file_storage import FileStorage
    fs = FileStorage(storage_dir=temp_storage_dir)
    result = fs.store_ocr_result(
        file_name="test.pdf",
        markdown_output="# Test", json_output={"a": 1},
        page_count=1, processing_duration=1.2, token_usage=10,
        unique_id="abc123", error_details=None, request_id="req1", user_id=1
    )
    assert result is True
    rec = fs.get_file_record(file_name="test.pdf")
    assert rec is not None
    assert rec["file_name"] == "test.pdf"
    stats = fs.get_statistics()
    assert stats["total_files"] >= 1

def test_file_storage_status_update(temp_storage_dir):
    from src.core.storage.file_storage import FileStorage
    fs = FileStorage(storage_dir=temp_storage_dir)
    fs.store_ocr_result("test2.pdf", "# MD", {}, 1, 1.0, 5, request_id="r2")
    ok = fs.update_processing_status("test2.pdf", "FAILED", error_details="err", request_id="r2")
    assert ok is True
    rec = fs.get_file_record(file_name="test2.pdf")
    assert rec["processing_status"] == "FAILED"

def test_db_storage_util(monkeypatch):
    # This is a stub; for real DB tests, use a test DB and patch psycopg2
    from src.core.database import db_storage_util
    assert hasattr(db_storage_util, "ProcessingStatus")
    assert hasattr(db_storage_util, "DatabaseStorage")
    # More detailed DB tests should use a test PostgreSQL instance
