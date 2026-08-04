<h3 align="center">
  <a name="readme-top"></a>
  <img
    src="https://raw.githubusercontent.com/GramosoftAI/GdoczAI/main/GdoczAI_Landingpage/public/integration/GdoczAI.svg"
    height="70"
    alt="GdoczAI Logo"
  >
</h3>


<div align="center">
  <a href="#">
    <img src="https://raw.githubusercontent.com/GramosoftAI/GdoczAI/main/img/mit-license.svg" alt="License" target="_blank">
  </a>
  <a href="https://gcrawlai.com" target="_blank">
    <img src="https://raw.githubusercontent.com/GramosoftAI/GdoczAI/main/img/visits.svg" alt="Visit gcrawlai.com">
  </a>
</div>

---

## ✨ Why GdoczAI?

Most document extraction tools dump raw, unformatted OCR text or chaotic JSON files on your lap, requiring massive amounts of manual cleanup and custom parsing scripts.

**GdoczAI** gives you clean, validated, structured data that integrates directly with your existing software stack. Teach it with just one sample document, and you're ready to automate.

Here's what you can build and automate with it:

🧾 **Accounts Payable Automation** — Extract invoice and purchase order data automatically, perform three-way matching, and post details to ERPs (Odoo, ERPNext, SAP, NetSuite, Tally).

✈️ **Aviation & Logistics Operations** — Automatically extract and normalize fuel receipts, cargo manifests, handling forms, and freight invoices across different currencies and carriers.

🚗 **Automotive Dealership Portals** — Streamline OEM statements, vehicle purchase invoices, job cards, and branch accounting without manual keying.

🏦 **Insurance & Claim Processing** — Convert physical claim forms, surveyor reports, bank statements, and KYC documents into clean databases for core systems.

📬 **Email Intake Workflows** — Monitor support or financial mailboxes, grab email attachments, and automatically extract, validate, and deliver content.

📁 **Secure SFTP Pipelines** — Drop PDF documents in a secure folder, and let GdoczAI automatically detect, extract, and log the results to your database.

No complex machine learning training. No manual dataset tagging. No brittle template rules that break on layout changes.

GdoczAI handles the messy documents so you can run your operations.

- ⚡ **Single-Shot Learning** — Upload just one sample document, specify fields, and GdoczAI learns the pattern instantly.
- 🧠 **Multi-Engine OCR Strategy** — Choose your preferred extraction engine per document type (Chandra/Marker API, Mistral OCR, Qwen3-VL, or Gemini).
- ⚙️ **End-to-End Delivery** — Deliver data to Google Sheets, ERP webhooks, custom endpoints, or downstream relational databases.
- 📬 **Native Email & SFTP Connectors** — Run background monitors that fetch files from IMAP/SMTP and SFTP paths automatically.
- 🔔 **Alert Notification System** — Get real-time alerts via email when validations fail or when OCR pipelines face parsing issues.
- 🌍 **Fully Open Source** — MIT licensed. Run it locally, host it on-premise, or start instantly on GdoczAI Cloud.

---

## 🚀 Features

| Feature                       | Description                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------- |
| **Single-Shot Templates**     | Teach GdoczAI to extract fields based on a single sample upload — no extensive model training |
| **Multi-Engine Extraction**   | Switch dynamically between Google Gemini, Mistral OCR, Qwen3-VL, and Datalab Marker (Chandra) |
| **Email IMAP Intake**         | Connect directly to mailboxes to monitor and auto-process incoming email attachments    |
| **SFTP Directory Watcher**    | Automated background scheduler checks SFTP directories, processes files, and archives them |
| **PostgreSQL Tracking**       | Audited relational storage tracking every processed file, state, retry, and log metadata |
| **Custom Schema Builder**     | Dynamically create data schemas (dates, numbers, strings, and tables/line items)       |
| **Webhook Notifications**     | Dispatch structured payloads to Odoo, ERPNext, NetSuite, or generic REST webhooks      |
| **Authentication & Auth Keys**| Secure access via JWT tokens, Email OTP verification, and developer API keys           |

---

## 🛠️ Technology Stack

* **Backend**: FastAPI, Python 3.9+
* **Database**: PostgreSQL
* **Schedulers & Tasks**: APScheduler
* **AI & OCR Engines**: Google Gemini (GenAI), Mistral OCR, Qwen3-VL (DeepInfra), Datalab Marker (Chandra API)
* **Connectors**: IMAP, SMTP, SFTP (Paramiko)
* **Frontend**: Next.js (App Router), TypeScript, Ant Design, Bootstrap 5

---

## 📋 Prerequisites

* **Python 3.9+**
* **Node.js 18+** & **npm**
* **PostgreSQL** (running on default port 5432)
* **Git**



## ⚙️ Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/GramosoftAI/GdoczAI.git
   cd GdoczAI
   ```

2. **Setup the Python Backend**
   ```bash
   cd GdoczAI_Backend
   python -m venv gdocz_env
   
   # Activate virtual environment
   # Linux/macOS:
   source gdocz_env/bin/activate
   # Windows:
   gdocz_env\Scripts\activate

   # Install dependencies
   pip install -r requirements.txt
   ```

3. **Setup the Next.js Frontend**
   ```bash
   cd ../GdoczAI_Landingpage
   npm install
   ```

---

## 🔧 Configuration

1. **Backend Environment Variables**:
   In `GdoczAI_Backend/`, copy `.env.example` to `.env`:
   ```bash
   cd ../GdoczAI_Backend
   cp .env.example .env
   ```
   Open `.env` and configure:
   * Database settings (`DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`)
   * JWT security key (`JWT_SECRET_KEY`)
   * SMTP credentials for alert mails (`SMTP_SERVER`, `SMTP_USERNAME`, `SMTP_PASSWORD`)
   * AI API Keys (`GEMINI_API_KEY`, `MISTRAL_API_KEY`, `DEEPINFRA_API_KEY`, `CHANDRA_API_KEY`)

2. **Core Settings File**:
   Update database credentials, storage type (`local` or `s3`), and processing schedules in `config/config.yaml`.

3. **Initialize Database**:
   Ensure PostgreSQL is running, then initialize database tables, triggers, and indexes:
   ```bash
   python -m src.core.database.setup_postgresql
   ```

4. **Frontend Environment**:
   Configure frontend URLs in `GdoczAI_Landingpage/.env`:
   ```env
   NEXT_PUBLIC_APP_URL=https://app.gdoczai.com
   ```

---

## 🏃 Running the Application

### 1. Start the API Backend
From the `GdoczAI_Backend` directory (with your virtual environment active):
```bash
python -m src.api.start_api_server --port 4535 --reload
```
Interactive Swagger API documentation will be available at: http://localhost:4535/docs

### 2. Start Background Schedulers (Optional)
To run background ingestion pipelines for SFTP folders and email attachments:
```bash
# Start SFTP watch folder checker
python -m src.services.sftp_fetch.start_sftp_fetch

# Start SMTP/IMAP mailbox email attachment checker
python -m src.services.smtp_fetch.start_smtp_fetcher
```

### 3. Start the Frontend
From the `GdoczAI_Landingpage` directory:
```bash
npm run dev
```
Open your browser and navigate to: http://localhost:3000

---

## 📁 Project Structure

```
GdoczAI/
├── GdoczAI_Backend/                  # FastAPI Python backend
│   ├── src/
│   │   ├── api/                      # Routes, authentication, webhooks & API models
│   │   │   ├── routes/               # API endpoints (auth, files, webhooks, api-keys)
│   │   │   ├── processing/           # Processing controllers & logic
│   │   │   └── schemas/              # Pydantic schemas (document types, custom logic)
│   │   ├── core/                     # Relational DB connection, schemas, migrations
│   │   └── services/                 # core pipelines & fetch schedulers
│   │       ├── ocr_pipeline/         # OCR pipeline processors (Gemini, Mistral, Qwen, Chandra)
│   │       ├── sftp_fetch/           # SFTP polling daemon
│   │       └── smtp_fetch/           # IMAP mailbox polling daemon
│   ├── config/                       # config.yaml settings
│   ├── logs/                         # File execution log files
│   └── requirements.txt              # Backend dependencies
│
└── GdoczAI_Landingpage/              # Next.js frontend
    ├── app/                          # Pages, layouts, metadata & global styles
    ├── components/                   # Nav, Hero, ROI calculator, FAQ accordion
    ├── public/                       # Image assets and integrations
    └── package.json                  # Frontend dependencies
```

---

## 🔐 Key API Endpoints

* `POST /auth/signup` - Register a new user account.
* `POST /auth/signin` - Secure email/password login returning a JWT.
* `POST /process/file` - Upload, parse, and process a document.
* `GET /files/user-files` - Query statuses and outputs of processed documents.
* `POST /document-types` - Create custom document templates.
* `POST /document-schemas` - Define dynamic validation and JSON schemas.
* `POST /user-webhooks` - Register delivery webhooks for real-time exports.
* `POST /user-apikeys` - Generate API tokens to authenticate REST integrations.
* `POST /user-sftp` & `POST /user-smtp` - Setup credentials for background extraction sources.

Full interactive docs are accessible at `/docs` (Swagger UI) when the backend is running.

---

## 🤝 Contributing

Contributions are welcome and appreciated! Here's how to get involved:

1. Fork the repository
2. Create a feature branch — `git checkout -b feature/YourFeature`
3. Commit your changes — `git commit -m 'Add YourFeature'`
4. Push to your branch — `git push origin feature/YourFeature`
5. Open a Pull Request

Please ensure your code follows the existing style and matches the project's code quality. For large changes, open an issue first to discuss your proposal.

---

## 🙌 Credits & Inspiration

GdoczAI was built by the team at **Gramosoft Private Limited**. We stand on the shoulders of giants in the AI and document intelligence ecosystem:

| Project                                                          | What We Learned                                                                            |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| [⚡ FastAPI](https://github.com/tiangolo/fastapi)                | High-performance asynchronous REST API pattern design.                                      |
| [♊ Google Gemini API](https://github.com/google/generative-ai)  | Advanced schema extraction, function calling, and structured JSON generation.               |
| [🐘 PostgreSQL](https://www.postgresql.org)                      | Bulletproof data structures, triggers, and relational tracking for pipeline jobs.         |
| [🍃 Mistral AI](https://github.com/mistralai)                    | Native PDF-to-Markdown processing configurations.                                          |

> **Disclaimer:** GdoczAI is an independent open-source project built by Gramosoft Private Limited. All referenced projects are the intellectual property of their respective owners and contributors. GdoczAI is not affiliated with or endorsed by any of the above projects. We simply admire their work and credit them accordingly.

---

## 📄 License

GdoczAI is released under the **MIT License**.

```
MIT License

Copyright (c) 2026 Gramosoft Private Limited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

See the [LICENSE](./LICENSE) file for full details.

---

## 🙏 Acknowledgements

* Thank you to all contributors and the open-source community for your support!
* GdoczAI is intended for legitimate document parsing, business automation, and research purposes.
* Users are responsible for complying with regulatory guidelines (HIPAA, GDPR) when extracting private document data.

---

<p align="center">
  Built with ❤️ by <a href="https://gramosoft.tech">Gramosoft Private Limited</a>
  <br><br>
  ⭐ If GdoczAI saves you time, please <strong>star the repo</strong> — it helps others discover it!
  <br><br>
  <a href="#readme-top">↑ Back to Top ↑</a>
</p>
