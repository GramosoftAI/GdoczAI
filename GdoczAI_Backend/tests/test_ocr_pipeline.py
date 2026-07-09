"""
Test Suite: OCR Pipeline
Covers: OCR processor, pipeline integration, and dual response (Markdown+JSON)
"""
import pytest
import tempfile
from pathlib import Path

@pytest.fixture(scope="module")
def ocr_processor():
    from src.services.ocr_pipeline.ocr_server_processor import MinerUProcessor
    # If config needed, adjust as per MinerUProcessor signature
    return MinerUProcessor()

def test_ocr_api_status(ocr_processor):
    status = ocr_processor.get_api_status()
    assert "status" in status

def test_ocr_process_image(ocr_processor):
    from PIL import Image
    import io
    img = Image.new('RGB', (300, 100), color='white')
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_file:
        temp_file.write(buf.read())
        temp_path = Path(temp_file.name)
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = ocr_processor.process_document_with_tokens(temp_path, Path(temp_dir))
            assert result is not None
            assert "markdown_content" in result
            assert "json_content" in result
    finally:
        temp_path.unlink()

def test_dual_response_pdf(ocr_processor):
    from fpdf import FPDF
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(0, 10, "TEST PDF", ln=True)
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_pdf:
        pdf.output(temp_pdf.name)
        pdf_path = Path(temp_pdf.name)
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = ocr_processor.process_document_with_tokens(pdf_path, Path(temp_dir))
            assert result is not None
            assert "markdown_content" in result
            assert "json_content" in result
    finally:
        pdf_path.unlink()
