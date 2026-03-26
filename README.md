<h1 align="center">
  <a name="readme-top"></a>
  <br>
  <img
    src="./logo/logo-gdocz.png"
    height="220"
    alt="GDocZ AI Logo"
  >
  <br><br>
  <b>🧠 Intelligent Document Processing Platform</b>
  <br>
  <sub>Transform PDFs, Images & Documents into Structured Intelligence with Multi-Engine OCR</sub>
</h1>

<div align="center">
  <br>
  
  [![Live Demo](https://img.shields.io/badge/🌐%20View%20Demo-https://gdocz.gramopro.ai-blue?style=for-the-badge&logoColor=white)](https://gdocz.gramopro.ai/)
  [![Try Free](https://img.shields.io/badge/🚀%20Try%20Now-Free%20Access-brightgreen?style=for-the-badge&logoColor=white)](https://gdocz.gramopro.ai/auth/demo)
  [![MIT License](https://img.shields.io/badge/📜%20License-MIT-yellow?style=for-the-badge&logoColor=white)](LICENSE)
  [![Python 3.9+](https://img.shields.io/badge/Python-3.9+-3776ab?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
  [![FastAPI](https://img.shields.io/badge/FastAPI-0.132+-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
  [![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
  
  <br><br>
  
  <table>
    <tr>
      <td align="center"><a href="#-quickstart">⚡ Quick Start</a></td>
      <td align="center"><a href="#-key-features">✨ Features</a></td>
      <td align="center"><a href="#%EF%B8%8F-installation--setup">🛠️ Install</a></td>
      <td align="center"><a href="#-api-endpoints">📚 API</a></td>
      <td align="center"><a href="#-architecture--workflow">🏗️ Architecture</a></td>
    </tr>
  </table>
  
  <br>
</div>

---

## 🎯 What is GDocZ?

> **GDocZ** is an **intelligent document processing platform** that combines multi-engine OCR (OlmOCR, Qwen VL, Gemini, Chandra) with AI-powered data extraction. It transforms complex documents—invoices, contracts, forms, receipts, multi-page reports, and more—into structured, actionable data.
>
> From **raw PDFs and images** → **clean JSON** with intelligent schema validation  
> No complex integrations. No fragile CSS selectors. No maintenance nightmares. 🚀

<br>

### 💡 Why Choose GDocZ?

<table>
  <tr>
    <td width="50%">
      
#### 🎯 **Multi-Model OCR Harmony**
Intelligently routes documents to the best-fit engine (OlmOCR for speed, Qwen for charts, Gemini for complex layouts) based on document size and complexity.

#### 📄 **Cross-Page Intelligence**
Extract data that spans multiple pages—invoices split across sheets, contracts with continuation pages, multi-page forms. GDocZ understands document flow.
      
#### 🧠 **Schema-Driven Extraction**
Define what you want to extract once using intuitive schema definitions. Supports nested objects, arrays, and complex hierarchies.

#### 📧 **Automated Ingestion**
SMTP/IMAP email monitoring, SFTP file fetching, webhook notifications. Documents arrive, processing begins automatically.
      
#### 🔐 **Enterprise Ready**
JWT authentication, API keys, rate limiting, multi-user support, audit logs, and full error recovery.
      
#### ☁️ **Flexible Storage**
Local filesystem or AWS S3. Date-based organization, original PDF preservation, and comprehensive metadata.

---

## ✨ Key Features

<div align="start">

| 🧠 | 📄 | ⚡ |
|---|---|---|
| **Multi-Engine OCR** | **Schema-Based Extraction** | **Intelligent Chunking** |
| OlmOCR, Qwen2.5-7B VL, Gemini 2.0/2.5, <br>Chandra with intelligent routing | Recursive schema definitions <br> for complex nested data structures | Auto segmentation  + manual<br> splitting for edge cases |

| 📧 | 🌐 | 🔐 |
|---|---|---|
| **Email Integration** | **Dual Storage** | **JWT Security** |
| SMTP/IMAP monitoring for automatic<br> document ingestion | Local FS or AWS S3 with date-based <br> organization | Secure API with token <br>refresh & API keys |

| 🔄 | 🔔 | 📊 |
|---|---|---|
| **Conflict Resolution** | **Webhooks** | **Field Validation** |
| Multi-model result merging with confidence <br> scoring | Real-time processing notifications | Schema-based <br>validation with null handling |

| 🚀 | 📋 | 🔍 |
|---|---|---|
| **Background Processing** | **Comprehensive Logging** | **Cross-Page Extraction** |
| Asynchronous jobs with priority queues | Rotating logs, error tracking, audit trails | Seamless multi-page data <br>extraction |

</div>
<br>

---

## 🏗️ Architecture & Workflow

### 📊 Processing Pipeline

```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃            INPUT SOURCES (📥 Multiple Channels)            ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃   📧 Email (SMTP/IMAP)  │  📁 SFTP  │  🌐 API Upload   ┃
┗━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
          │
          ▼
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃     INTELLIGENT PROCESSING PIPELINE (⚙️ Multi-Step)       ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃  ① 📋 File Validation & Storage                           ┃
┃  ② 🖼️  PDF → Image Conversion (if needed)                 ┃
┃  ③ 🎯 Intelligent Model Routing (size/complexity)         ┃
┃  ④ 🧠 OCR Processing (Multi-engine execution)             ┃
┃  ⑤ 📝 Markdown Generation                                 ┃
┃  ⑥ ✂️  Smart Chunking & Segmentation                      ┃
┃  ⑦ 🔍 AI-Powered JSON Extraction (Schema-driven)          ┃
┃  ⑧ 🔀 Conflict Resolution (multi-model consolidation)     ┃
┃  ⑨ ✅ Field Validation & Post-processing                  ┃
┃  ⑩ 📊 Metadata & Audit Logging                            ┃
┗━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
          │
          ▼
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃          OUTPUT DESTINATIONS (📤 Multiple Formats)         ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ 📊 JSON  │  💾 Storage (Local/S3)  │  🔔 Webhooks        ┃
┃ 📧 Emails  │  📋 Database  │  📈 Analytics               ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
```

### 🔧 Core Components

<div align="left">

| Component | Purpose | Tech Stack |
|-----------|---------|-----------|
| **🧠 OCR Engine Hub** | Multi-model document understanding | OlmOCR, Qwen2.5-7B VL, Gemini 2.0/2.5, Chandra |
| **🚀 API Server** | RESTful interface for all operations | FastAPI + Python 3.9+ |
| **🔀 Schema Engine** | Recursive data extraction rules | Custom schema validation |
| **💾 Storage Manager** | File persistence & retrieval | Local FS / AWS S3 |
| **📧 Email Connector** | Automated email document fetching | SMTP/IMAP + OAuth2 |
| **📁 SFTP Connector** | Remote file synchronization | SSH-based transfer |
| **🗄️ Database Layer** | Persistent data & audit trails | PostgreSQL |
| **⏰ Background Scheduler** | Async processing & automation | APScheduler |
| **🔐 Authentication** | Security & access control | JWT + API Key + BCrypt |

</div>

---

## 📋 Prerequisites

### 💻 System Requirements

      
**🐍 Python**
3.9+ (3.12+ recommended)

**🗄️ PostgreSQL**
12+ (port 5432)

**🧠 RAM**
8GB min (16GB+ ideal)
 
**💾 Storage**
50GB+ for documents

> **📌 GPU Support** (Optional)  
> NVIDIA/AMD for acceleration • Apple Silicon for MLX • CPU-only possible with smaller models

</div>

### 🔑 Required Credentials

<table>
  <tr>
    <td width="33%">
      
**🧠 OCR API Keys**
- DeepInfra Key (OlmOCR, Qwen)
- Gemini API Key (Google)

    </td>
    <td width="33%">
      
**📧 Email Config** (SMTP/IMAP)
- Gmail / Hostinger / Custom
- OAuth2 Support
- App passwords

    </td>
    <td width="33%">
      
**☁️ AWS S3** (Optional)
- Access Key ID
- Secret Access Key
- Bucket Name

    </td>
  </tr>
</table>

---

## ⚙️ Installation & Setup

> 📌 **Estimated Time**: ~5-10 minutes for basic setup, ~20 minutes with optional services

### Step 1️⃣ Clone the Repository

```bash
git clone https://github.com/your-org/gdocz-ai.git
cd gdocz-ai
```

### Step 2️⃣ Create & Activate Virtual Environment

```bash
# Using Python venv (recommended)
python -m venv mineru_env

# Activate on Windows
mineru_env\Scripts\activate

# Activate on Linux/Mac
source mineru_env/bin/activate
```

### Step 3️⃣ Install Python Dependencies

```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

**📦 What Gets Installed:**
- FastAPI & Uvicorn (HTTP server)
- psycopg2 (PostgreSQL driver)
- pdf2image, pillow (PDF processing)
- requests (API calls)
- boto3 (AWS S3)
- PyYAML, python-dotenv, APScheduler

### Step 4️⃣ Environment Configuration

```bash
# Create .env from template
cp .env.example .env
```

**Edit `.env` with your credentials:**

```env
# 🔐 Authentication
AUTH_SIGNIN_URL=https://your-auth-endpoint.com/auth/signin
AUTH_USERNAME=your_email@example.com
AUTH_PASSWORD=your_password

# 🧠 OCR Services
DEEPINFRA_API_KEY=your_deepinfra_key
GEMINI_API_KEY=your_google_gemini_key

# 🗄️ Database
POSTGRES_HOST=localhost
POSTGRES_PASSWORD=your_secure_password

# 💾 Storage
STORAGE_TYPE=local  # or "s3"
```

📖 **Full Reference**: See [ENV_SETUP.md](ENV_SETUP.md)

### Step 5️⃣ Initialize PostgreSQL Database

```bash
# Create database
psql -U postgres -h localhost -c "CREATE DATABASE gdocz_db;"

# Run migrations
python -m src.core.database.db_migration

# Verify
psql -U postgres -d gdocz_db -c "\dt"
```

### Step 6️⃣ Start Required Services

<table>
  <tr>
    <th>Terminal</th>
    <th>Command</th>
    <th>Purpose</th>
    <th>URL</th>
  </tr>
  <tr>
    <td><b>1️⃣ API</b></td>
    <td><code>python src/api/start_api_server.py</code></td>
    <td>REST API Server</td>
    <td><a href="http://localhost:8000">http://localhost:8000</a></td>
  </tr>
  <tr>
    <td><b>2️⃣ OCR</b></td>
    <td><code>python src/services/ocr_pipeline/ocr_server_app.py</code></td>
    <td>OCR Pipeline</td>
    <td>Background service</td>
  </tr>
  <tr>
    <td><b>3️⃣ Email</b></td>
    <td><code>python src/services/smtp_fetch/start_smtp_fetcher.py</code></td>
    <td>Email Monitor (Optional)</td>
    <td>Background service</td>
  </tr>
  <tr>
    <td><b>4️⃣ SFTP</b></td>
    <td><code>python src/services/sftp_fetch/start_sftp_fetch.py</code></td>
    <td>SFTP Sync (Optional)</td>
    <td>Background service</td>
  </tr>
  <tr>
    <td><b>5️⃣ Jobs</b></td>
    <td><code>python scripts/start_services.py</code></td>
    <td>Scheduler</td>
    <td>Background service</td>
  </tr>
</table>

✅ **All services running?** Visit **http://localhost:8000/docs** for interactive API documentation!

---

## 📚 API Endpoints

<div align="left">

### 🔐 Authentication
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/auth/signin` | 🔓 Sign in with credentials |
| POST | `/auth/signup` | ✍️ Create new account |
| POST | `/auth/token/refresh` | 🔄 Refresh access token |
| POST | `/auth/forgot-password` | 🔑 Request password reset |
| POST | `/auth/reset-password` | ✅ Reset with token |

### 📋 Document Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/document/type/create` | ➕ Define document types |
| POST | `/document/schema/create` | 🎯 Create extraction schemas |
| GET | `/document/schema/list` | 📖 List all schemas |
| PUT | `/document/schema/update/{id}` | ✏️ Update schema |
| DELETE | `/document/schema/{id}` | ❌ Delete schema |

### 🔑 API Configuration
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/apikey/generate` | 🆕 Generate API key |
| GET | `/apikey/list` | 📋 List API keys |
| DELETE | `/apikey/{key_id}` | 🚫 Revoke API key |

### 🔔 Webhooks & Notifications
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/webhook/register` | ✍️ Register webhook |
| PUT | `/webhook/update/{id}` | ✏️ Update webhook |
| DELETE | `/webhook/{id}` | ❌ Remove webhook |
| POST | `/alert/email/configure` | 📧 Set alert emails |

### 📧 Email & File Integration
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/connector/smtp/add` | ➕ Add SMTP account |
| GET | `/connector/smtp/status` | 📊 Email status |
| POST | `/connector/sftp/add` | ➕ Add SFTP connection |
| GET | `/connector/sftp/status` | 📊 SFTP status |

</div>

📖 **Interactive Docs**: [http://localhost:8000/docs](http://localhost:8000/docs) when running

---

## 📁 Project Structure

<details open>
<summary><b>🗂️ Click to expand directory tree</b></summary>

```
gdocz-ai/                              
├── 📦 src/                             # Source code directory
│   ├── 🌐 api/                         # REST API Layer
│   │   ├── api_server.py               # FastAPI main app
│   │   ├── start_api_server.py         # Server entry point
│   │   ├── 📍 routes/                  # API endpoints
│   │   │   ├── api_server_auth.py      # Auth routes
│   │   │   ├── api_server_files.py     # File operations
│   │   │   ├── api_server_alert_mail.py # Email alerts
│   │   │   └── ...
│   │   ├── 🏗️ models/                  # Pydantic models
│   │   ├── 📋 schemas/                 # Document schemas
│   │   ├── ⚙️ processing/              # Request handlers
│   │   └── 🔗 connectors/              # SFTP/SMTP bridges
│   │
│   ├── 🔧 services/                    # Business Logic
│   │   ├── 🧠 ocr_pipeline/            # Multi-engine OCR
│   │   │   ├── ocr_server_processor.py # OlmOCR core
│   │   │   ├── ocr_server_qwen.py      # Qwen VL handler
│   │   │   ├── ocr_server_gemini.py    # Gemini bridge
│   │   │   ├── ocr_server_extract.py   # JSON extraction
│   │   │   ├── ocr_server_validator.py # Field validation
│   │   │   └── ...
│   │   ├── 📧 email/                   # Email service
│   │   ├── 📬 smtp_fetch/              # Email fetcher
│   │   └── 📁 sftp_fetch/              # SFTP sync
│   │
│   └── 💾 core/                        # Infrastructure
│       ├── 🗄️ database/                # DB schema & migrations
│       ├── 📋 schema/                  # Document schemas
│       ├── 💿 storage/                 # Storage abstraction
│       ├── 🔑 indexes/                 # DB indexes
│       └── 🔗 triggers/                # DB triggers
│
├── ⚙️ config/
│   └── config.yaml                     # Main configuration
│
├── 📊 data/
│   ├── 💾 storage/stored_documents/    # Document storage
│   ├── 📤 uploads/temp_uploads/        # Upload staging
│   ├── 🎯 jobs/temp_jobs/              # Job data
│   └── 🔐 pem_files/                   # SSH keys
│
├── 💬 prompts/                         # LLM Prompts
│   ├── UNIVERSAL_SINGLE_PROMPT.txt
│   └── UNIVERSAL_CHUNK_PROMPT.txt
│
├── 📝 logs/
│   └── olmocr_server.log               # Application logs
│
├── 🚀 scripts/
│   ├── start_services.py               # Service launcher
│   └── *.sh                            # Shell scripts
│
├── 🧪 tests/
│   ├── test_api_endpoints.py
│   ├── test_ocr_pipeline.py
│   └── ...
│
├── 📄 requirements.txt                 # Python dependencies
├── 🔐 .env.example                     # Environment template
├── 📖 ENV_SETUP.md                     # Config guide
└── 📘 README.md                        # This file
```

</details>

---

## 🔧 Configuration

### config.yaml Structure

<details open>
<summary><b>⚙️ Click to view configuration template</b></summary>

```yaml
# 🔐 AUTHENTICATION LAYER
authentication:
  signin_url: "${AUTH_SIGNIN_URL}"
  token_refresh_interval_hours: 20

# 🧠 OCR CONFIGURATION
ocr:
  endpoint_url: "${OCR_ENDPOINT_URL}"
  timeout_seconds: 900

# 🚀 OLMOCR (DeepInfra) - Fast, Cost-Effective
olmocr:
  deepinfra_api_key: "${DEEPINFRA_API_KEY}"
  deepinfra_model: "cognitivecomputations/OLMoE-1B-7B-0924"
  deepinfra_timeout: 600
  batch_size: 3  # Parallel pages

# 📊 QWEN VL (DeepInfra) - Charts & Visual Content
qwen:
  api_key: "${DEEPINFRA_API_KEY}"
  model: "Qwen/Qwen2.5-7B-Instruct"
  timeout: 120
  temperature: 0.1
  max_tokens: 8192

# ✨ GEMINI (Google) - Large, Complex Documents
gemini:
  api_key: "${GEMINI_API_KEY}"
  model_2_0: "gemini-2.0-flash"
  model_2_5: "gemini-2.5-flash"
  timeout: 300

# 💾 STORAGE CONFIGURATION
storage:
  storage_type: "local"  # or "s3"
  local_storage:
    base_path: "./data/storage/stored_documents/"
    create_date_folders: true
  s3_storage:
    bucket_name: "your-bucket"
    aws_region: "us-east-1"
    create_date_folders: true

# 🗄️ DATABASE
postgres:
  host: "${POSTGRES_HOST}"
  port: 5432
  database: "${POSTGRES_DB}"
  user: "${POSTGRES_USER}"
  password: "${POSTGRES_PASSWORD}"

# 📝 LOGGING
log_level: "${LOG_LEVEL:INFO}"
max_retry_attempts: 3
retry_delay_seconds: 5
```

</details>

### 📊 OCR Model Routing Strategy

<table align="">
  <tr>
    <th>Document Size</th>
    <th>Model Selected</th>
    <th>Speed</th>
    <th>Cost</th>
    <th>Best For</th>
  </tr>
  <tr>
    <td>≤ 20,000 chars</td>
    <td>🚀 <b>Qwen2.5-7B</b></td>
    <td>⚡ Fastest</td>
    <td>💰 Cheapest</td>
    <td>Simple forms, receipts</td>
  </tr>
  <tr>
    <td>20-30K chars</td>
    <td>⚖️ <b>Gemini 2.0</b></td>
    <td>⏱️ Balanced</td>
    <td>💵 Moderate</td>
    <td>Invoices, tables</td>
  </tr>
  <tr>
    <td>&gt; 30,000 chars</td>
    <td>🧠 <b>Gemini 2.5</b></td>
    <td>⏳ Thorough</td>
    <td>💳 Premium</td>
    <td>Contracts, reports</td>
  </tr>
</table>

---

## 🚨 Troubleshooting

<table>
  <tr>
    <th width="25%">Issue</th>
    <th width="75%">Solution</th>
  </tr>
  <tr>
    <td><b>🔴 API Won't Start</b></td>
    <td>
      <code>lsof -i :8000</code> (Linux/Mac) or <code>netstat -ano | findstr :8000</code> (Windows)<br>
      Check: <code>psql -U postgres -h localhost -d gdocz_db -c "SELECT 1;"</code>
    </td>
  </tr>
  <tr>
    <td><b>⏳ OCR Hangs</b></td>
    <td>
      Verify API keys: <code>echo $DEEPINFRA_API_KEY</code><br>
      Check logs: <code>tail -f logs/olmocr_server.log</code><br>
      Test connection: <code>curl -H "Authorization: Bearer KEY" https://api.deepinfra.com/v1/status</code>
    </td>
  </tr>
  <tr>
    <td><b>📧 Email Not Working</b></td>
    <td>
      Status: <code>curl -X GET http://localhost:8000/connector/smtp/status</code><br>
      Gmail? Use App Password, not regular password<br>
      Outlook? Enable "Less secure app access"
    </td>
  </tr>
  <tr>
    <td><b>💾 Storage Issues</b></td>
    <td>
      Local: <code>ls -la data/storage/stored_documents/</code><br>
      S3: <code>aws s3 ls --profile default</code>
    </td>
  </tr>
</table>

---

## 🔒 Security & Best Practices

### ✅ Secrets Management (DO's)

```yaml
- Store credentials in .env (NEVER in git)
- Use strong, unique API keys
- Rotate credentials regularly
- Enable HTTPS in production
- Use environment-specific configs
- Monitor access logs
```

### ❌ Security Anti-Patterns (DON'Ts)

```yaml
- Commit .env to version control
- Hardcode credentials in code
- Share API keys in logs
- Use weak passwords
- Store secrets in comments
- Log sensitive data
```

### 🔐 Database Security

```sql
-- 👤 Create restricted database user
CREATE USER gdocz_app WITH ENCRYPTED PASSWORD 'your_strong_password_here';
GRANT CONNECT ON DATABASE gdocz_db TO gdocz_app;
GRANT USAGE ON SCHEMA public TO gdocz_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO gdocz_app;

-- 🔒 Enable SSL connections
-- Edit postgresql.conf: ssl = on
```

### 🛡️ API Authentication Headers

| Feature | Details |
|---------|---------|
| **Token Expiry** | 24 hours |
| **Refresh** | Use `/auth/token/refresh` before expiry |
| **API Keys** | For programmatic access (more secure for automation) |
| **Rate Limiting** | Recommended for production |

---

## 📊 Performance Optimization

### ⚡ Large Document Handling

<table>
  <tr>
    <th>Strategy</th>
    <th>Benefit</th>
    <th>When to Use</th>
  </tr>
  <tr>
    <td>🧠 <b>Gemini 2.5 Flash</b></td>
    <td>Auto-routes for >30K chars</td>
    <td>Large contracts, reports</td>
  </tr>
  <tr>
    <td>✂️ <b>Manual Splitting</b></td>
    <td>Control chunk boundaries</td>
    <td>Over-sized PDFs</td>
  </tr>
  <tr>
    <td>🚀 <b>Batch Processing</b></td>
    <td>Parallel OCR (3-5 pages)</td>
    <td>High-volume workloads</td>
  </tr>
  <tr>
    <td>☁️ <b>S3 Storage</b></td>
    <td>Scale-on-demand</td>
    <td>Enterprise deployments</td>
  </tr>
</table>

### 🗄️ Database Optimization

```sql
-- ⚡ Add indexes for common queries
CREATE INDEX idx_job_status ON jobs(status);
CREATE INDEX idx_user_id ON documents(user_id);
CREATE INDEX idx_created_date ON documents(created_at);

-- 📊 Analyze query performance
EXPLAIN ANALYZE SELECT * FROM documents WHERE status = 'completed';
```

### 🎯 Model Routing Quick Reference

```
20K chars  ┌─────────────────┐
           │  Qwen2.5-7B     │  ⚡ Fastest
           └────────┬────────┘
                    │
30K chars  ┌────────▼────────┐
           │  Gemini 2.0     │  ⚖️ Balanced
           └────────┬────────┘
                    │
 >30K       ┌────────▼────────┐
           │  Gemini 2.5     │  🧠 Most Capable
           └─────────────────┘
```

---

## 🧪 Testing

### ▶️ Run Test Suite

```bash
# 🔍 All tests
pytest tests/ -v

# 🎯 Specific module
pytest tests/test_api_endpoints.py -v

# 📊 With coverage report
pytest --cov=src tests/
```

### ✅ Example Test Cases

```python
# tests/test_ocr_pipeline.py

def test_invoice_extraction():
    """Test OCR extraction on sample invoice"""
    result = ocr_processor.process("tests/samples/invoice.pdf")
    assert "invoice_number" in result
    assert result["total_amount"] > 0

def test_cross_page_extraction():
    """Test multi-page document handling"""
    result = ocr_processor.process("tests/samples/multi_page_contract.pdf")
    assert len(result["pages"]) > 1
    
def test_conflict_resolution():
    """Test multi-model result merging"""
    result = merge_ocr_results(qwen_result, gemini_result)
    assert "confidence_score" in result
```

---

## 🤝 Contributing

> ❤️ **Contributions are welcome!** Help us make GDocZ even better.

### 🔄 Contribution Workflow

<div align="left">

1️⃣ **Fork** → 2️⃣ **Branch** → 3️⃣ **Code** → 4️⃣ **Test** → 5️⃣ **PR** ✅

</div>

### 📝 Step-by-Step Guide

```bash
# 1. Fork the repository (on GitHub)

# 2. Clone your fork
git clone https://github.com/your-username/gdocz-ai.git
cd gdocz-ai

# 3. Create feature branch
git checkout -b feature/YourFeature

# 4. Setup dev environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate (Windows)
pip install -r requirements.txt
pip install pytest pytest-cov black flake8

# 5. Make your changes with clear commits
git commit -m "feat: add your feature description"

# 6. Run quality checks
black src/
flake8 src/
pytest tests/ -v

# 7. Push to your fork
git push origin feature/YourFeature

# 8. Open Pull Request on GitHub
```

### 📋 Code Style Guidelines

✅ Follow **PEP 8** (use `black` for formatting)  
✅ Add **docstrings** to all functions  
✅ Include **type hints**  
✅ Write **tests** for new features  
✅ Update **documentation** if needed  

---

## 📄 License

<div align="left">

**GDocZ** is released under the **MIT License** 📜

See [LICENSE](LICENSE) file for full details.

```
MIT License

Copyright (c) 2026 Gramosoft Private Limited

Permission is hereby granted, free of charge, to any person 
obtaining a copy of this software...
```

</div>

---

## 👥 Team & Authors

<div align="left">

Built with ❤️ by **Gramosoft Private Limited**

| 👤 | Role | Name |
|:-:|------|------|
| <b>👨‍💼</b> | **Team Lead** | Rajesh Kannan M |
| <b>🧠</b> | **GenAI/ML Developer** | Ramkumar S |
| <b>🔧</b> | **GenAI Developer (Supporting)** | Girinath R |

</div>

---

## 🙏 Acknowledgments

> GDocZ stands on the shoulders of incredible open-source projects and the amazing developer community.
 
**⚡ FastAPI**  
Async API excellence
     
**🐘 PostgreSQL**  
Reliable data foundation
     
**🎭 Playwright**  
Browser automation
     
**🌿 Celery**  
Distributed tasks
    
**☁️ AWS SDK**  
Cloud integration
    
**✔️ Pydantic**  
Data validation

---

## 📞 Support & Contact

<div align="left">

| 🌐 | 🚀 | 📧 | 🐛 | 💬 |
|---|---|---|---|---|
| [Website](https://gdocz.gramopro.ai/) | [Live Demo](https://gdocz.gramopro.ai/auth/demo) | [Email](mailto:support@gramopro.ai) | [Issues](https://github.com/your-org/gdocz-ai/issues) | [Discussions](https://github.com/your-org/gdocz-ai/discussions) |

**🌐 Website**: [https://gdocz.gramopro.ai/](https://gdocz.gramopro.ai/)  
**🚀 Try Demo**: [https://gdocz.gramopro.ai/auth/demo](https://gdocz.gramopro.ai/auth/demo)  
**📧 Email**: support@gramopro.ai  
**🐛 Issues**: [GitHub Issues](https://github.com/your-org/gdocz-ai/issues)  
**💬 Discussions**: [GitHub Discussions](https://github.com/your-org/gdocz-ai/discussions)

</div>

---

## 🌟 Show Your Support

<div align="left">

If GDocZ saves you time and frustration, please **⭐ star this repository**!  
It helps others discover the project and motivates us to keep improving.

```
⭐ Star GDocZ ⭐
```

### 🎯 You can also help by:
- 🐛 Reporting bugs
- 💡 Suggesting features  
- 📝 Writing documentation
- 🤝 Contributing code
- 📢 Sharing with others

</div>

---

<br>

<div align="center">

<h2>✨ Transform Documents Into Intelligence ✨</h2>

<p>
  Built with ❤️ by <a href="https://gramosoft.tech"><b>Gramosoft Private Limited</b></a>
  <br><br>
  <b>GDocZ: The Future of Document Processing</b>
  <br><br>
  <a href="#readme-top">
    <img src="https://img.shields.io/badge/↑%20Back%20to%20Top%20↑-gray?style=flat-square" alt="Back to Top">
  </a>
</p>

</div>

---

<div align="center">
  <sub>📅 Last Updated: March 23, 2026</sub>
  <br>
  <sub>📦 Version: 5.0.0</sub>
  <br>
  <sub>⚡ Built in 🔥</sub>
</div>
