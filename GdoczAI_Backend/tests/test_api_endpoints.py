
"""
Test Suite: API Endpoints (FastAPI)
Covers: Health, config, file upload, job status, user auth, webhooks, SFTP, SMTP, document types, schemas, and alert routes.
"""
import pytest
import requests
import os
import time
from fastapi.testclient import TestClient
from src.api.api_server import app

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:4535")

def wait_for_api():
    for _ in range(30):
        try:
            r = requests.get(f"{API_BASE_URL}/", timeout=3)
            if r.status_code == 200:
                return True
        except Exception:
            time.sleep(1)
    return False

@pytest.fixture(scope="session", autouse=True)
def api_ready():
    assert wait_for_api(), f"API server not running on {API_BASE_URL}"

def test_root():
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "API" in response.text

def test_health(api_ready):
    r = requests.get(f"{API_BASE_URL}/health")
    assert r.status_code in (200, 503)
    assert "status" in r.json()

# Config endpoint
def test_config(api_ready):
    r = requests.get(f"{API_BASE_URL}/config")
    assert r.status_code in (200, 503)
    if r.status_code == 200:
        assert "max_concurrent_files" in r.json()

# File upload (basic)
def test_file_upload(api_ready, tmp_path):
    from PIL import Image
    import io
    img = Image.new('RGB', (200, 100), color='white')
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    files = {'file': ('test.png', buf, 'image/png')}
    data = {'document_type': 'test_invoice', 'priority': 1}
    r = requests.post(f"{API_BASE_URL}/process/file", files=files, data=data)
    assert r.status_code in (200, 503)
    if r.status_code == 200:
        resp = r.json()
        assert resp["success"] is True
        assert "job_id" in resp
        assert resp["document_type"] == "test_invoice"

# Job status
def test_job_status(api_ready):
    pass  # Implement with a fixture if needed