#!/usr/bin/env python3

"""
Core API Server for Document Processing
- Standalone REST API (NO pipeline dependencies)
- JWT authentication
- File upload/download
- Document management
- User management
"""
import sys
import argparse
import logging
import time
import uvicorn
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# API SERVER IMPORTS ONLY - NO PIPELINE IMPORTS
from src.api.processing.auth_processing import AuthManager as APIAuthManager
from src.services.email.email_service import EmailService
from src.api.models.api_server_models import (
    PipelineStatus, PipelineConfig as APIPipelineConfig, 
    StorageManager, get_db_connection, ConfigManager
)
from src.api.routes.api_server_auth import create_auth_routes, set_global_services as set_auth_services
from src.api.schemas.api_server_document_types import create_document_type_routes
from src.api.schemas.api_server_document_schemas import create_document_schema_routes
from src.api.schemas.api_server_document_logics import create_document_logic_routes
from src.api.routes.api_server_document_webhook import create_user_webhook_routes
from src.api.routes.api_server_alert_mail import create_alert_mail_routes
from src.api.routes.api_server_files import create_file_routes
from src.api.processing.api_server_processing import create_processing_routes
from src.api.routes.api_server_apikey_generate import create_user_apikey_routes
from src.api.connectors.api_server_sftp import create_user_sftp_routes
from src.api.connectors.api_server_smtp import create_user_smtp_routes


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== GLOBAL SERVICE INSTANCES ====================

# API Authentication
api_auth_manager: Optional[APIAuthManager] = None
email_service: Optional[EmailService] = None
storage_manager: Optional[StorageManager] = None
config_manager: Optional[ConfigManager] = None

app_start_time = time.time()

security = HTTPBearer(auto_error=False)

# ==================== AUTHENTICATION ====================

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify JWT token and get current user"""
    if not api_auth_manager:
        raise HTTPException(status_code=503, detail="Authentication service not initialized")
    
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = credentials.credentials
    token_data = api_auth_manager.verify_token(token)
    
    if not token_data:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
    return token_data

# ==================== FASTAPI APP ====================

app = FastAPI(
    title="Document Processing API",
    description="REST API for document processing and management",
    version="5.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== ROUTE REGISTRATION ====================

def register_routes():
    """Register all routes from sub-modules"""
    create_auth_routes(app, get_current_user)
    create_document_type_routes(app, get_current_user)
    create_document_schema_routes(app, get_current_user)
    create_document_logic_routes(app, get_current_user)
    create_user_webhook_routes(app, get_current_user)
    create_user_sftp_routes(app, get_current_user)
    create_user_smtp_routes(app, get_current_user)
    create_user_apikey_routes(app, get_current_user)
    create_alert_mail_routes(app, get_current_user)
    create_file_routes(app, api_auth_manager, get_current_user)
    create_processing_routes(app, api_auth_manager, storage_manager, get_current_user)

# ==================== STARTUP AND SHUTDOWN ====================

def setup_file_logging():
    """Setup file logging for the API server"""
    from logging.handlers import RotatingFileHandler
    
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # API Server Log
    api_handler = RotatingFileHandler(
        log_dir / 'api_server.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    api_handler.setLevel(logging.INFO)
    api_handler.setFormatter(log_format)
    
    # Error Log
    error_handler = RotatingFileHandler(
        log_dir / 'errors.log',
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(log_format)
    
    # Add handlers
    root_logger = logging.getLogger()
    root_logger.addHandler(api_handler)
    root_logger.addHandler(error_handler)
    
    logger.info("File logging enabled:")
    logger.info(f"API logs: {log_dir / 'api_server.log'}")
    logger.info(f"Error logs: {log_dir / 'errors.log'}")

@app.on_event("startup")
async def startup_event():
    global api_auth_manager, email_service, storage_manager, config_manager
    
    try:
        setup_file_logging()
        
        logger.info("=" * 80)
        logger.info("Starting API server (STANDALONE MODE)")
        logger.info("=" * 80)
        
        # Load configuration
        logger.info("Loading configuration...")
        config_manager = ConfigManager("config/config.yaml")
        logger.info("Configuration loaded")
        
        # Initialize Storage Manager
        logger.info("Initializing storage manager...")
        storage_manager = StorageManager(config_manager)
        logger.info(f"Storage manager initialized: {storage_manager.storage_type.upper()}")
        
        # Initialize API Authentication Manager
        logger.info("Initializing authentication manager...")
        config_dict = {
            'postgres': {
                'host': config_manager.get('postgres.host', 'localhost'),
                'port': config_manager.get('postgres.port', 5432),
                'database': config_manager.get('postgres.database', 'document_pipeline'),
                'user': config_manager.get('postgres.user'),
                'password': config_manager.get('postgres.password')
            },
            'security': {
                'jwt_secret_key': config_manager.get('security.jwt_secret_key', 'your-secret-key'),
                'encryption_key': config_manager.get('security.encryption_key')
            },
            'email': config_manager.get('email', {})
        }
        
        jwt_secret = config_dict['security']['jwt_secret_key']
        encryption_key = config_dict['security']['encryption_key']
        
        api_auth_manager = APIAuthManager(
            config_dict['postgres'], 
            jwt_secret, 
            encryption_key
        )
        logger.info("Authentication manager initialized")
        
        # Initialize Email Service
        logger.info("Initializing email service...")
        email_service = EmailService(config_dict['email'])
        logger.info("Email service initialized")
        
        # Register Routes
        logger.info("Registering API routes...")
        register_routes()
        logger.info("All routes registered")
        
        # Set Global Services
        set_auth_services(api_auth_manager, email_service)
        
        logger.info("=" * 80)
        logger.info("API server ready (STANDALONE MODE)")
        logger.info("=" * 80)
        logger.info("Features:")
        logger.info("- File upload/download")
        logger.info("- User authentication")
        logger.info("- API key management")
        logger.info("- Document types & schemas")
        logger.info("- Webhook management")
        logger.info("- Alert configuration")
        logger.info("- SFTP connector management")
        logger.info("- SMTP connector management")
        logger.info("- File storage (local/S3)")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}", exc_info=True)
        raise

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down API server...")
    logger.info("API server shutdown complete")

# ==================== HEALTH AND ROOT ENDPOINTS ====================

@app.get("/", tags=["Health"])
async def root():
    return {
        "message": "Document Processing API (Standalone)",
        "version": "5.0.0",
        "status": "healthy",
        "uptime_seconds": time.time() - app_start_time,
        "mode": "standalone",
        "features": [
            "File upload/download",
            "User authentication",
            "API key management",
            "Document types & schemas",
            "Webhook management",
            "Alert configuration",
            "File storage (local/S3)"
        ],
        "storage_type": storage_manager.storage_type.upper() if storage_manager else "Unknown",
        "endpoints": {
            "health": "/health",
            "docs": "/docs",
            "process_file": "/process/file",
            "user_files": "/files/user-files",
            "document_types": "/document-types",
            "document_schemas": "/document-schemas",
            "user_webhooks": "/user-webhooks",
            "user_apikeys": "/user-apikeys",
            "user_sftp": "/user-sftp",
            "user_smtp": "/user-smtp"
        }
    }

@app.get("/health", tags=["Health"])
async def health_check():
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": time.time() - app_start_time,
            "mode": "standalone",
            "dependencies": {
                "database": "healthy",
                "storage": storage_manager.storage_type.upper() if storage_manager else "Unknown",
                "authentication": "healthy" if api_auth_manager else "unhealthy"
            },
            "features": {
                "file_upload": "enabled",
                "user_authentication": "enabled",
                "document_management": "enabled",
                "file_storage": "enabled"
            }
        }
    
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")

# ==================== CONFIGURATION ENDPOINT ====================

@app.get("/config", response_model=APIPipelineConfig, tags=["Configuration"])
async def get_config(user = Depends(get_current_user)):
    """Get current API configuration"""
    try:
        return APIPipelineConfig(
            max_concurrent_files=5,
            polling_interval_minutes=5,
            supported_extensions=['.pdf', '.jpg', '.jpeg', '.png'],
            auto_retry_failed=True,
            max_retries=3
        )
    
    except Exception as e:
        logger.error(f"Failed to get configuration: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== MAIN ENTRY POINT ====================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Document Processing API Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=4535, help='Port to bind to')
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload for development')
    parser.add_argument('--log-level', default='info', help='Log level (debug, info, warning, error)')
    
    args = parser.parse_args()
    
    # Create required directories
    Path("logs").mkdir(exist_ok=True)
    Path("temp_uploads").mkdir(exist_ok=True)
    Path("temp_jobs").mkdir(exist_ok=True)
    Path("processed_output/ocr_output").mkdir(parents=True, exist_ok=True)
    Path("processed_output/json_output").mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 80)
    logger.info("Document Processing API Server (STANDALONE)")
    logger.info("=" * 80)
    logger.info(f"Starting server on {args.host}:{args.port}")
    logger.info(f"Log level: {args.log_level.upper()}")
    logger.info("=" * 80)
    
    uvicorn.run(
        "api_server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level
    )

if __name__ == "__main__":
    main()