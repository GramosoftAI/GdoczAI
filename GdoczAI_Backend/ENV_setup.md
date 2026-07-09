# Environment Configuration Guide

This document describes how to configure the environment settings and credentials for **GdoczAI** (formerly Mineru_project).

---

## 1. Setup Instructions

1. Copy `.env.example` in the root directory and rename it to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Open `.env` and fill in the values described below.

---

## 2. Configuration Settings

### PostgreSQL Settings
- **DB_HOST**: Hostname of the PostgreSQL database server (e.g. `localhost` or remote IP `75.119.145.112`).
- **DB_PORT**: Database connection port (default: `5432`).
- **DB_USER**: Database username.
- **DB_PASSWORD**: Database connection password.
- **DB_NAME**: Database name (e.g., `gdocz_ai`).

### Security Settings
- **JWT_SECRET_KEY**: A random secure string used to sign JWT session authentication tokens. **Do not expose this in production.**

### Email / SMTP Settings
Used to send automated email alerts on OCR processing or validation failures.
- **SMTP_SERVER**: SMTP host server (e.g., `smtp-relay.brevo.com`).
- **SMTP_PORT**: SMTP port (e.g., `587`).
- **SMTP_SENDER_EMAIL**: Sender email address.
- **SMTP_RECIPIENT_EMAIL**: Email address to receive fail notifications.
- **SMTP_USERNAME**: Authentication username for SMTP server.
- **SMTP_PASSWORD**: Password/API token for SMTP server.

### AI Engine & OCR API Keys
- **GEMINI_API_KEY**: Google GenAI API key for JSON structure extraction (Gemini 2.5 Flash).
- **MISTRAL_API_KEY**: Mistral AI API key for PDF/image to Markdown conversion.
- **DEEPINFRA_API_KEY**: DeepInfra API key used for Qwen3-VL processing.
- **CHANDRA_API_KEY**: Datalab Marker API key for ChandraProcessor conversion.

---

## 3. Applying the Configuration

The application is configured to load settings from both Environment variables (read via `.env` or system variables) and standard configuration. You can initialize the database using the setup script:

```bash
python src/core/database/setup_postgresql.py
```
This script will read postgres parameters, verify the database exists, run auto-migrations, and set up all the schema, triggers, and indexes.
