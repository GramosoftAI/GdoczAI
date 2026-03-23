# Environment Variables Configuration Guide

## Overview
This project uses environment variables stored in a `.env` file to manage sensitive data and configuration. The `.env` file is loaded automatically when the application starts, and variables are substituted into `config/config.yaml`.

All configuration for authentication, storage, database, email, AI/ML APIs, logging, and more is managed through environment variables. This allows for secure, flexible, and environment-specific configuration without hardcoding secrets or credentials in your codebase.

## Setup Instructions

### 1. Create Your .env File
Copy the `.env.example` file to `.env` and fill in your actual values:

```bash
cp .env.example .env
```

Then edit `.env` and replace all placeholder values (like `your_api_key_here`) with your actual credentials.
Each variable in `.env.example` is grouped and commented for clarity. Replace every placeholder (such as `your_api_key_here`) with your real value. If a variable is not needed for your deployment, you may leave it as the default or blank, but review the table below for required fields.

### 2. Important Security Notes

⚠️ **CRITICAL**:
- **Never commit `.env` to version control** – It contains sensitive data
- **Always use `.env.example`** as a template for new developers
- **.env.example** can be checked into version control (with dummy values only)
- Keep `.env` file with restricted permissions on production servers

### 3. Variable Format in config.yaml

Variables in `config/config.yaml` use the following format:

**Format with required variable:**
```yaml
password: "${POSTGRES_PASSWORD}"
```
This will fail if `POSTGRES_PASSWORD` is not defined.

**Format with default value:**
```yaml
host: "${POSTGRES_HOST:localhost}"
```
This will use `POSTGRES_HOST` if defined, otherwise default to `localhost`.

### 4. How It Works

When the application starts:

1. **Load Environment Variables**: The `python-dotenv` package loads all variables from `.env`
2. **Load YAML Config**: The `config/config.yaml` is loaded
3. **Variable Substitution**: All `${VAR_NAME}` or `${VAR_NAME:default}` patterns are replaced with environment variable values
4. **Use Configuration**: The application uses the resolved configuration

### 5. Environment Variables Reference


## Comprehensive Environment Variables Table

Below is a detailed table of all environment variables supported by this project. Each variable is grouped by function, with required status, default, example, and description.

### Authentication
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| AUTH_SIGNIN_URL | Yes | — | http://localhost:4535/auth/signin | Authentication endpoint URL |
| AUTH_USERNAME | Yes | — | your_email@example.com | Authentication username/email |
| AUTH_PASSWORD | Yes | — | your_password_here | Authentication password |
| AUTH_TOKEN_REFRESH_INTERVAL_HOURS | No | 20 | 20 | Token refresh interval (hours) |

### OCR
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| OCR_ENDPOINT_URL | Yes | — | https://your-ocr-endpoint.com | OCR API endpoint |
| OCR_TIMEOUT_SECONDS | No | 900 | 900 | OCR request timeout (seconds) |

### Scheduler
| Variable | Required | Default | Example | Description |
|-----------------------------|----------|---------|---------|-------------|
| SCHEDULER_TOKEN_REFRESH_CHECK_INTERVAL_MINUTES | No | 60 | 60 | How often to check token refresh (minutes) |
| EMAIL_FETCH_SCHEDULER_TOKEN_REFRESH_CHECK_INTERVAL_MINUTES | No | 60 | 60 | Email fetch scheduler token refresh interval (minutes) |

### Logging
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| LOG_LEVEL | No | INFO | INFO | Log level (DEBUG, INFO, WARNING, ERROR) |
| MAX_RETRY_ATTEMPTS | No | 3 | 3 | Max retry attempts for failed operations |
| RETRY_DELAY_SECONDS | No | 5 | 5 | Delay between retries (seconds) |

### Storage
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| STORAGE_TYPE | Yes | local | local | Storage backend type (local, s3) |
| LOCAL_STORAGE_ENABLED | No | true | true | Enable local storage |
| LOCAL_STORAGE_BASE_PATH | No | ./data/storage/stored_documents/ | ./data/storage/stored_documents/ | Local storage path |
| LOCAL_STORAGE_CREATE_DATE_FOLDERS | No | true | true | Create date-based folders in local storage |
| LOCAL_STORAGE_PRESERVE_ORIGINAL_NAME | No | true | true | Preserve original file name |
| S3_STORAGE_ENABLED | No | false | false | Enable S3 storage |
| S3_AWS_ACCESS_KEY_ID | No | — | your_aws_access_key_id | AWS access key for S3 |
| S3_AWS_SECRET_ACCESS_KEY | No | — | your_aws_secret_access_key | AWS secret key for S3 |
| S3_AWS_REGION | No | us-east-1 | us-east-1 | AWS region for S3 |
| S3_BUCKET_NAME | No | — | your_bucket_name | S3 bucket name |
| S3_BUCKET_PREFIX | No | input-pdfs/ | input-pdfs/ | S3 bucket prefix |
| S3_CREATE_DATE_FOLDERS | No | true | true | Create date-based folders in S3 |
| S3_STORAGE_CLASS | No | STANDARD | STANDARD | S3 storage class |
| S3_ENABLE_VERSIONING | No | false | false | Enable S3 versioning |
| S3_SERVER_SIDE_ENCRYPTION | No | AES256 | AES256 | S3 server-side encryption |
| S3_ACL | No | private | private | S3 ACL |
| SAVE_ORIGINAL_PDF | No | true | true | Save original PDF |
| INCLUDE_METADATA | No | true | true | Include metadata |
| CLEANUP_AFTER_STORAGE | No | false | false | Cleanup after storage |

### Google Gemini
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| GEMINI_API_KEY | Yes | — | your_gemini_api_key_here | Google Gemini API key |
| GEMINI_MODEL | No | gemini-2.0-flash | gemini-2.0-flash | Gemini model name |
| GEMINI_TIMEOUT_SECONDS | No | 60 | 60 | Gemini API timeout (seconds) |
| GEMINI_MAX_RETRIES | No | 3 | 3 | Gemini API max retries |
| GEMINI_RETRY_DELAY_SECONDS | No | 2 | 2 | Delay between Gemini API retries (seconds) |
| GEMINI_MAX_TOKENS | No | 8192 | 8192 | Gemini max tokens |
| GEMINI_TEMPERATURE | No | 0 | 0 | Gemini temperature |

### Chunking
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| CHUNKING_ENABLED | No | true | true | Enable chunking |
| CHUNK_SIZE | No | 6000 | 6000 | Chunk size |
| CHUNK_OVERLAP | No | 500 | 500 | Chunk overlap |

### Manual Splitting
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| MANUAL_SPLITTING_ENABLED | No | true | true | Enable manual splitting |
| MANUAL_SPLITTING_THRESHOLD_CHARACTERS | No | 7000 | 7000 | Manual splitting threshold (characters) |
| MANUAL_SPLITTING_MAX_ROWS_PER_CHUNK | No | 10 | 10 | Max rows per chunk |

### Database (PostgreSQL)
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| POSTGRES_HOST | Yes | localhost | localhost | PostgreSQL server hostname |
| POSTGRES_PORT | No | 5432 | 5432 | PostgreSQL server port |
| POSTGRES_DATABASE | Yes | — | your_db_name | Database name |
| POSTGRES_USER | Yes | — | your_db_user | Database user |
| POSTGRES_PASSWORD | Yes | — | your_db_password | Database password |

### Security
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| JWT_SECRET_KEY | Yes | — | your_jwt_secret_key | Secret key for JWT token signing |
| JWT_ALGORITHM | No | HS256 | HS256 | JWT algorithm |
| ACCESS_TOKEN_EXPIRE_MINUTES | No | 1440 | 1440 | Token expiration in minutes |
| PASSWORD_RESET_TOKEN_EXPIRE_HOURS | No | 24 | 24 | Password reset token expiration |
| ENCRYPTION_KEY | Yes | — | your_encryption_key | Encryption key for sensitive data |
| API_KEY_ENCRYPTION_KEY | No | — | your_api_key_encryption_key | API key encryption key |

### Email
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| EMAIL_HOST | Yes | smtp-relay.brevo.com | smtp-relay.brevo.com | SMTP server hostname |
| EMAIL_PORT | No | 587 | 587 | SMTP server port |
| EMAIL_USERNAME | Yes | — | your_email_username | SMTP authentication username |
| EMAIL_PASSWORD | Yes | — | your_email_password | SMTP authentication password |
| EMAIL_FROM_EMAIL | Yes | — | your_from_email@example.com | Sender email address |
| EMAIL_FROM_NAME | No | GdoczAI | GdoczAI | Sender display name |
| EMAIL_USE_TLS | No | true | true | Use TLS encryption |
| EMAIL_RESET_PASSWORD_URL | No | https://your-app.com/auth/reset_password | https://your-app.com/auth/reset_password | Password reset URL for email links |

### Email Notifications
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| EMAIL_NOTIFICATIONS_ENABLED | No | true | true | Enable email notifications |
| EMAIL_NOTIFICATIONS_SMTP_HOST | No | smtp-relay.brevo.com | smtp-relay.brevo.com | SMTP host for notifications |
| EMAIL_NOTIFICATIONS_SMTP_PORT | No | 587 | 587 | SMTP port for notifications |
| EMAIL_NOTIFICATIONS_SMTP_USERNAME | No | — | your_email_username | SMTP username for notifications |
| EMAIL_NOTIFICATIONS_SMTP_PASSWORD | No | — | your_email_password | SMTP password for notifications |
| EMAIL_NOTIFICATIONS_FROM_EMAIL | No | — | your_from_email@example.com | Sender email for notifications |
| EMAIL_NOTIFICATIONS_FROM_NAME | No | OCR Pipeline Alert System | OCR Pipeline Alert System | Sender name for notifications |
| EMAIL_NOTIFICATIONS_USE_TLS | No | true | true | Use TLS for notifications |
| EMAIL_NOTIFICATIONS_DEVELOPER_RECIPIENTS | No | dev1@example.com,dev2@example.com | dev1@example.com,dev2@example.com | Developer recipients |
| EMAIL_NOTIFICATIONS_CLIENT_RECIPIENTS | No | client@example.com | client@example.com | Client recipients |
| EMAIL_NOTIFICATIONS_ALERT_COOLDOWN_MINUTES | No | 30 | 30 | Alert cooldown (minutes) |

### File Tracking
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| FILE_TRACKING_BACKUP_PROCESSED_FILES | No | true | true | Backup processed files |
| FILE_TRACKING_CLEANUP_TEMP_FILES | No | true | true | Cleanup temp files |
| FILE_TRACKING_MAX_FILE_AGE_DAYS | No | 30 | 30 | Max file age (days) |

### Logging (Pipeline)
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| LOGGING_LEVEL | No | INFO | INFO | Logging level |
| LOGGING_LOG_FILE | No | ./logs/pipeline.log | ./logs/pipeline.log | Log file path |
| LOGGING_MAX_LOG_SIZE_MB | No | 100 | 100 | Max log size (MB) |
| LOGGING_BACKUP_COUNT | No | 5 | 5 | Log backup count |
| LOGGING_CONSOLE_OUTPUT | No | true | true | Console output enabled |

### Error Handling
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| ERROR_HANDLING_MAX_FILE_RETRIES | No | 3 | 3 | Max file retries |
| ERROR_HANDLING_RETRY_DELAY_MINUTES | No | 10 | 10 | Retry delay (minutes) |
| ERROR_HANDLING_CONTINUE_ON_ERROR | No | true | true | Continue on error |
| ERROR_HANDLING_SAVE_FAILED_FILES_LIST | No | true | true | Save failed files list |
| ERROR_HANDLING_FAILED_FILES_LOG | No | ./logs/failed_files.log | ./logs/failed_files.log | Failed files log path |

### Performance
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| PERFORMANCE_CHUNK_SIZE_BYTES | No | 8192 | 8192 | Chunk size (bytes) |
| PERFORMANCE_CONNECTION_POOL_SIZE | No | 10 | 10 | Connection pool size |
| PERFORMANCE_ENABLE_COMPRESSION | No | true | true | Enable compression |
| PERFORMANCE_PREFETCH_FILES | No | true | true | Prefetch files |

### OLMOCR Deepinfra
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| OLMOCR_DEEPINFRA_API_KEY | Yes | — | your_olmocr_api_key | OLMOCR Deepinfra API key |
| OLMOCR_DEEPINFRA_MODEL | No | allenai/olmOCR-2-7B-1025 | allenai/olmOCR-2-7B-1025 | OLMOCR model |
| OLMOCR_DEEPINFRA_TIMEOUT | No | 600 | 600 | OLMOCR timeout (seconds) |
| OLMOCR_DEEPINFRA_MAX_TOKENS | No | 8192 | 8192 | OLMOCR max tokens |

### QwenOCR Deepinfra
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| QWENOCR_DEEPINFRA_API_KEY | Yes | — | your_qwenocr_api_key | QwenOCR Deepinfra API key |
| QWENOCR_DEEPINFRA_MODEL | No | Qwen/Qwen3-VL-235B-A22B-Instruct | Qwen/Qwen3-VL-235B-A22B-Instruct | QwenOCR model |
| QWENOCR_DEEPINFRA_TIMEOUT | No | 600 | 600 | QwenOCR timeout (seconds) |
| QWENOCR_DEEPINFRA_MAX_TOKENS | No | 8192 | 8192 | QwenOCR max tokens |

### Chandra Datalab
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| CHANDRA_DATALAB_API_KEY | Yes | — | your_chandra_api_key | Chandra Datalab API key |
| CHANDRA_DATALAB_OUTPUT_FORMAT | No | html | html | Output format |
| CHANDRA_DATALAB_MODE | No | accurate | accurate | Mode |
| CHANDRA_DATALAB_TIMEOUT | No | 600 | 600 | Timeout (seconds) |
| CHANDRA_DATALAB_POLL_INTERVAL | No | 3 | 3 | Poll interval (seconds) |
| CHANDRA_DATALAB_MAX_RETRIES | No | 2 | 2 | Max retries |

### Benz Validation
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| BENZ_VALIDATION_ENABLED | No | true | true | Enable Benz validation |
| BENZ_VALIDATION_EMAIL_NOTIFICATIONS_ENABLED | No | true | true | Enable Benz email notifications |
| BENZ_VALIDATION_EMAIL_NOTIFICATIONS_SMTP_SERVER | No | smtp-relay.brevo.com | smtp-relay.brevo.com | SMTP server for Benz notifications |
| BENZ_VALIDATION_EMAIL_NOTIFICATIONS_SMTP_PORT | No | 587 | 587 | SMTP port for Benz notifications |
| BENZ_VALIDATION_EMAIL_NOTIFICATIONS_SENDER_EMAIL | No | — | your_sender_email@example.com | Sender email for Benz notifications |
| BENZ_VALIDATION_EMAIL_NOTIFICATIONS_RECIPIENT_EMAIL | No | — | your_recipient_email@example.com | Recipient email for Benz notifications |
| BENZ_VALIDATION_EMAIL_NOTIFICATIONS_SMTP_USERNAME | No | — | your_email_username | SMTP username for Benz notifications |
| BENZ_VALIDATION_EMAIL_NOTIFICATIONS_SMTP_PASSWORD | No | — | your_email_password | SMTP password for Benz notifications |

### Gemini (Override)
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| GEMINI_OVERRIDE_API_KEY | No | — | your_gemini_override_api_key | Gemini override API key |
| GEMINI_OVERRIDE_MODEL | No | gemini-2.0-flash | gemini-2.0-flash | Gemini override model |
| GEMINI_OVERRIDE_TIMEOUT_SECONDS | No | 60 | 60 | Gemini override timeout (seconds) |
| GEMINI_OVERRIDE_MAX_RETRIES | No | 3 | 3 | Gemini override max retries |
| GEMINI_OVERRIDE_RETRY_DELAY_SECONDS | No | 2 | 2 | Gemini override retry delay (seconds) |
| GEMINI_OVERRIDE_TEMPERATURE | No | 0.1 | 0.1 | Gemini override temperature |
| GEMINI_OVERRIDE_MAX_TOKENS | No | 8192 | 8192 | Gemini override max tokens |
| GEMINI_2_0_FLASH_TEMPERATURE | No | 0.1 | 0.1 | Gemini 2.0 flash temperature |
| GEMINI_2_0_FLASH_MAX_TOKENS | No | 8192 | 8192 | Gemini 2.0 flash max tokens |
| GEMINI_2_0_FLASH_TIMEOUT_SECONDS | No | 60 | 60 | Gemini 2.0 flash timeout (seconds) |
| GEMINI_2_5_FLASH_TEMPERATURE | No | 0.1 | 0.1 | Gemini 2.5 flash temperature |
| GEMINI_2_5_FLASH_MAX_TOKENS | No | 65536 | 65536 | Gemini 2.5 flash max tokens |
| GEMINI_2_5_FLASH_TIMEOUT_SECONDS | No | 90 | 90 | Gemini 2.5 flash timeout (seconds) |

### Qwen
| Variable | Required | Default | Example | Description |
|----------|----------|---------|---------|-------------|
| QWEN_API_KEY | Yes | — | your_qwen_api_key | Qwen API key |
| QWEN_MODEL | No | Qwen/Qwen2.5-7B-Instruct | Qwen/Qwen2.5-7B-Instruct | Qwen model |
| QWEN_TIMEOUT | No | 600 | 600 | Qwen timeout (seconds) |
| QWEN_TEMPERATURE | No | 0.1 | 0.1 | Qwen temperature |
| QWEN_MAX_TOKENS | No | 8192 | 8192 | Qwen max tokens |
| QWEN_MAX_RETRIES | No | 3 | 3 | Qwen max retries |
| QWEN_RETRY_DELAY_SECONDS | No | 2 | 2 | Qwen retry delay (seconds) |

---

> **Tip:** For the most up-to-date and complete list, always refer to `.env.example` in your project root. If you add new features or integrations, update both `.env.example` and this table.

### 6. Generating Secure Keys

#### Generate JWT Secret Key
```python
import secrets
jwt_secret = secrets.token_urlsafe(32)
print(jwt_secret)
```

#### Generate Encryption Key
```python
from cryptography.fernet import Fernet
encryption_key = Fernet.generate_key().decode()
print(encryption_key)
```

### 7. Deployment to Production

For production deployment:

1. **Set environment variables directly** on your server/container instead of using .env file:
   ```bash
   export POSTGRES_PASSWORD="secure_password"
   export JWT_SECRET_KEY="secure_key"
   # ... etc
   ```

2. **Using Docker**: Pass environment variables via `docker run -e`:
   ```bash
   docker run -e POSTGRES_PASSWORD="secure_password" -e JWT_SECRET_KEY="secure_key" your-app
   ```

3. **Using Docker Compose**: Define in `.env` for local development, use secrets for production

### 8. Troubleshooting

**Issue**: Configuration not loading correctly
- Make sure `.env` file exists in the project root
- Verify all required variables are set
- Check for typos in variable names

**Issue**: Database connection failing
- Verify `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD` are correct
- Ensure PostgreSQL server is running
- Test connection manually: `psql -h $POSTGRES_HOST -U $POSTGRES_USER`

**Issue**: Email not sending
- Verify `EMAIL_HOST`, `EMAIL_PORT`, `EMAIL_USERNAME`, `EMAIL_PASSWORD` are correct
- Check if SMTP server requires TLS/SSL
- Verify sender email address is authorized for SMTP server

## Files Modified

- `.env` - Contains your sensitive data (add to .gitignore)
- `.env.example` - Template file with dummy values (add to git)
- `config/config.yaml` - Updated to use environment variables
- `requirements.txt` - Should include `python-dotenv` dependency

See `.env.example` for the full list of supported environment variables.
