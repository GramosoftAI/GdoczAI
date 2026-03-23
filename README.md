<div align="center">
  <!-- TODO: Replace the 'logo.png' src with the actual path or URL of your uploaded logo -->
  <img src="./logo.png" alt="GdoczAI Logo" height="100" />
# GdoczAI

Advanced Enterprise Document Processing & OCR Pipeline
  
  [![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
  [![FastAPI](https://img.shields.io/badge/FastAPI-0.132.0-009688.svg?logo=fastapi)](https://fastapi.tiangolo.com/)
  [![Pydantic](https://img.shields.io/badge/Pydantic-2.12.5-e92063.svg?logo=pydantic)](https://docs.pydantic.dev/)
  [![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0.45-d71f00.svg)](https://www.sqlalchemy.org/)
  [![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-336791.svg?logo=postgresql)](https://www.postgresql.org/)
  [![Uvicorn](https://img.shields.io/badge/Uvicorn-0.41.0-499848.svg)](https://www.uvicorn.org/)
  [![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)


---

## Overview

**GdoczAI** is a highly scalable, AI-powered document processing pipeline built for precision OCR, structured JSON generation, and advanced text extraction. Leveraging state-of-the-art vision-language models—such as **OLMOCR (allenai/olmOCR-2-7B)** and **Qwen3-VL** via DeepInfra—coupled with **Google Gemini 2.0/2.5 Flash** for dynamic semantic parsing, GdoczAI transforms complex PDFs, invoices, and images into clean Markdown and structured, schema-validated JSON.

## Key Features

- **Model-Based Routing:** Seamlessly switch between OLMOCR and Qwen3-VL engines for PDF and image extraction.
- **Smart JSON Extraction (Gemini):** Dynamically scales between Gemini 2.0 Flash (for standard docs) and Gemini 2.5 Flash (for large docs > 25k chars).
- **Intelligent Chunking:** Employs LangChain and Unstructured for conditional document chunking and manual table-aware splitting.
- **Enterprise Connectors:** Enterprise Connectors (Having SFTP and SMTP):** Native support for SFTP polling and SMTP/Email notifications for fully automated workflows.
- **Real-Time Webhooks:** Trigger user-defined webhooks dynamically upon successful processing or failure.
- **Robust API & Auth:** Fast API server with JWT Bearer tokens, API keys management, and structured REST endpoints.
- **Automated Invoice Extraction:** Rule-based fallback and ML-driven automatic extraction of `Invoice_No` directly into the database.
- **Dual Storage Support:** Configurable local filesystem or AWS S3 object storage.

## Architecture

The architecture relies on multiple integrated microservices communicating asynchronously:

1. **API Server (Port 4535):** Standalone REST API for file uploads, auth routing, connector management, and user configurations.
2. **OCR Engine / worker (Port 3545):** Dedicated GPU-optimized instance running PDF parsing, deep vision extraction, and LLM-driven JSON restructuring.
3. **Pipeline Monitor Scheduler:** Watchdog running continuous SFTP syncs, validating rules, and feeding documents into the OCR engine.

## Tech Stack

- **Core:** Python 3.9+, FastAPI, Uvicorn, Pydantic, SQLAlchemy.
- **AI & ML:** Transformers, PyTorch, LlamaIndex, Google Gemini GenAI SDK, HuggingFace Hub.
- **Document Processing:** PDFPlumber, PyMuPDF, Unstructured, PDFMiner.
- **Database:** PostgreSQL (async asyncpg/aiosqlite).
- **Messaging/Tasks:** APScheduler, Asyncio.

## Repository Structure

```text
Mineru_project/
logs/                # Real-time streaming and rotated service logs
config/              # YAML configuration and environment setups
scripts/             # Shell/Bash executable scripts
doczocr_api.sh
doczocr_engine.sh
start_services.py
src/                 # Main Source Code
api/             # Core API Server & Routes
core/            # Database configurations & storage managers
services/        # Subsystems (OCR Pipeline, Email, Royal, Webhooks)
ocr_pipeline/# OLMOCR & Qwen processors, Gemini JSON parsers
requirements.txt     # Python Dependencies
```

## Installation

### 1. Prerequisites
- Python 3.9 or higher
- PostgreSQL Server
- Virtual Environment tool (venv/conda)

### 2. Setup Environment

```bash
# Clone the repository
git clone https://github.com/your-org/Mineru_project.git
cd Mineru_project

# Create a virtual environment
python -m venv mineru_env
source mineru_env/bin/activate  # On Windows use: mineru_env\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration

Configure your `config/config.yaml` file and `.env` securely before starting:
- **DB_HOST, DB_PORT**: Set PostgreSQL connection credentials.
- **GEMINI_API_KEY**: Provide your Google GenAI API secret.
- **JWT_SECRET_KEY**: Set application JWT secret.
- **STORAGE**: Choose `local` or `s3`.

## Running the Application

We provide a highly optimized startup script (`start_services.py`) handling auto-reloads, color-coded logging, and unbuffered stdout.

### Method 1: Using the Master Script (Recommended)

```bash
# Start all services (API Server, Pipeline Monitor, Dashboard)
python scripts/start_services.py --all

# Start only the API Server on a custom port
python scripts/start_services.py --api --api-port 4535

# Start only the Pipeline Monitor
python scripts/start_services.py --pipeline
```

### Method 2: Using Bash Scripts

For Unix environments, execute dedicated bash runners:

```bash
# Start Core API Server standalone
bash scripts/gdoczai_api.sh

# Start OCR Inference Engine
bash scripts/gdoczai_engine.sh

# Start SFTP background worker
bash scripts/gdoczai_sftp.sh
```

## Core API Endpoints

Once running, access the automatic interactive docs at:
- **Core API Swagger:** [http://localhost:4535/docs](http://localhost:4535/docs)
- **OCR Engine Swagger:** [http://localhost:3545/ocr/docs](http://localhost:3545/ocr/docs)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ocr/pdf` | Extract Markdown & JSON from PDF document (requires model type). |
| `POST` | `/ocr/markdown-only` | Rapid OCR without LLM schema parsing. |
| `GET`  | `/files/user-files` | Retrieve processed output logs for the authenticated user. |
| `POST` | `/api/v1/auth/login` | Authenticate and obtain a JWT token. |
| `GET`  | `/v1/models` | List available Vision-Language processing models (OLMOCR, Qwen). |

## Observability & Logging

- Service logs are written dynamically to the `logs/` directory using thread-safe log queues to prevent IO blocking.
- Inspect logs locally:
  ```bash
  tail -f logs/api_server.log
  tail -f logs/olmocr_server.log
  ```
- Error traces with processing metrics are emailed to admins via the `EmailService` module.

## Live Demo
Try the core functionality of GdoczAI in action here:
[https://gdocz.gramopro.ai/auth/demo](https://gdocz.gramopro.ai/auth/demo)

## Authors & Team
- **Ramkumar S** - GenAI/ML Expert
- **Girinath R** - GenAI Developer Supporting
- **Rajesh Kannan M** - Team Lead

## Acknowledgement
We would like to extend a special thanks to the team and contributors whose hard work, expertise, and dedication made the development of the **GdoczAI** document processing pipeline possible.


## Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE` for more information.

---
<div align="center">
  <b>Built with ?? for robust document intelligence.</b>
</div>
