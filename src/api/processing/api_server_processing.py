#!/usr/bin/env python3

import asyncio
import json
import logging
import uuid
import shutil
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from fastapi import HTTPException, UploadFile, File, BackgroundTasks, Depends, Query, Form

from src.api.models.api_server_models import (
    FileProcessingResponse, JobStatus, generate_unique_request_id,
    generate_timestamped_filename, ConfigManager
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== GLOBAL SERVICE INSTANCES ====================

auth_manager = None
storage_manager = None
config_manager = None

def set_global_services(auth_mgr, storage_mgr, config_mgr):
    """Set global services"""
    global auth_manager, storage_manager, config_manager
    auth_manager = auth_mgr
    storage_manager = storage_mgr
    config_manager = config_mgr
    logger.info("Global services set in processing module")

# ==================== FILE PROCESSING ENDPOINTS ====================

def create_processing_routes(app, auth_mgr, storage_mgr, get_current_user):
    """Create processing routes"""
    
    # Set global services
    set_global_services(auth_mgr, storage_mgr, ConfigManager("config/config.yaml"))

    @app.post("/process/file", response_model=FileProcessingResponse, tags=["Processing"])
    async def process_uploaded_file(
        background_tasks: BackgroundTasks,
        file: UploadFile = File(...),
        document_type: str = Form(...),
        schema_json: Optional[str] = Form(None),
        custom_prompt: Optional[str] = Form(None),
        priority: int = Form(5),
        user_id: Optional[int] = Form(None),
        user = Depends(get_current_user)
    ):

        try:
            if not storage_manager:
                raise HTTPException(status_code=503, detail="Storage manager not initialized")
            
            # Validate document_type
            if not document_type or not document_type.strip():
                raise HTTPException(status_code=400, detail="document_type is required and cannot be empty")
            
            document_type = document_type.lower()

            file_extension = Path(file.filename).suffix.lower()

            # Validate file format
            supported_formats = {'.pdf', '.jpg', '.jpeg', '.png', '.webp', '.bmp'}
            if file_extension not in supported_formats:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Unsupported file format. Supported: {', '.join(supported_formats)}"
                )

            # Generate unique identifiers
            request_id = generate_unique_request_id()
            timestamped_filename = generate_timestamped_filename(file.filename)
            job_id = str(uuid.uuid4())
            
            logger.info("=" * 80)
            logger.info(f"?? NEW FILE UPLOAD PROCESSING")
            logger.info("=" * 80)
            logger.info(f"?? Job ID: {job_id}")
            logger.info(f"?? Request ID: {request_id}")
            logger.info(f"?? Original filename: {file.filename}")
            logger.info(f"?? Timestamped filename: {timestamped_filename}")
            logger.info(f"?? Document type: {document_type}")
            logger.info(f"?? User ID: {user_id or user.get('user_id')}")
            
            # Extract user_id from token if not provided
            if not user_id:
                user_id = user.get('user_id')
            
            # Parse schema_json if provided
            schema_dict = None
            if schema_json:
                try:
                    schema_dict = json.loads(schema_json)
                    logger.info(f"?? Using dynamic schema_json parameter")
                except json.JSONDecodeError:
                    raise HTTPException(status_code=400, detail="Invalid schema_json format")
            
            # Save uploaded file to temp directory
            temp_dir = Path("temp_uploads")
            temp_dir.mkdir(exist_ok=True)
            
            temp_file_path = temp_dir / f"{job_id}_{timestamped_filename}"
            
            logger.info(f"?? Saving to temp: {temp_file_path}")
            with open(temp_file_path, "wb") as buffer:
                content = await file.read()
                buffer.write(content)
            
            logger.info(f"? File saved: {len(content) / (1024*1024):.2f} MB")
            
            # Store file to local/S3 storage
            logger.info(f"?? Storing file to {storage_manager.storage_type.upper()} storage...")
            storage_file_path = storage_manager.store_file(temp_file_path, timestamped_filename)
            
            if storage_file_path:
                logger.info(f"? File stored: {storage_file_path}")
            else:
                logger.warning("?? File storage failed, continuing without file_path")
            
            # Process file in background
            background_tasks.add_task(
                process_single_file_background,
                job_id,
                temp_file_path,
                file.filename,
                timestamped_filename,
                document_type,
                schema_dict,
                custom_prompt,
                request_id,
                user_id,
                storage_file_path
            )
            
            logger.info(f"?? Background processing queued")
            logger.info("=" * 80)
            
            return FileProcessingResponse(
                success=True,
                job_id=job_id,
                request_id=request_id,
                filename=timestamped_filename,
                status="queued",
                message=f"File queued for processing (request_id: {request_id}, document_type: {document_type}, user_id: {user_id})",
                file_path=storage_file_path
            )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"? Failed to process uploaded file: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/process/batch", tags=["Processing"])
    async def process_batch_files(
        background_tasks: BackgroundTasks,
        files: List[UploadFile] = File(...),
        document_type: str = Form(...),
        schema_json: Optional[str] = Form(None),
        custom_prompt: Optional[str] = Form(None),
        priority: int = Form(5),
        user_id: Optional[int] = Form(None),
        user = Depends(get_current_user)
    ):
        """
        ?? Process batch files using external OCR API
        """
        try:
            if not storage_manager:
                raise HTTPException(status_code=503, detail="Storage manager not initialized")
            
            # Validate document_type
            if not document_type or not document_type.strip():
                raise HTTPException(status_code=400, detail="document_type is required and cannot be empty")
            
            document_type = document_type.lower()
            
            if len(files) > 10:
                raise HTTPException(status_code=400, detail="Maximum 10 files per batch")
            
            # Extract user_id from token if not provided
            if not user_id:
                user_id = user.get('user_id')
            
            # Parse schema_json if provided
            schema_dict = None
            if schema_json:
                try:
                    schema_dict = json.loads(schema_json)
                except json.JSONDecodeError:
                    raise HTTPException(status_code=400, detail="Invalid schema_json format")
            
            batch_id = str(uuid.uuid4())
            job_details = []
            
            logger.info("=" * 80)
            logger.info(f"?? BATCH FILE UPLOAD PROCESSING")
            logger.info("=" * 80)
            logger.info(f"?? Batch ID: {batch_id}")
            logger.info(f"?? Total files: {len(files)}")
            logger.info(f"?? Document type: {document_type}")
            logger.info(f"?? User ID: {user_id}")
            
            for idx, file in enumerate(files, 1):
                file_extension = Path(file.filename).suffix.lower()
                supported_formats = {'.pdf', '.jpg', '.jpeg', '.png', '.webp', '.bmp'}
                
                if file_extension not in supported_formats:
                    logger.warning(f"?? Skipping unsupported file: {file.filename}")
                    continue
                
                job_id = str(uuid.uuid4())
                request_id = generate_unique_request_id()
                timestamped_filename = generate_timestamped_filename(file.filename)
                
                logger.info(f"?? [{idx}/{len(files)}] Processing: {file.filename}")
                logger.info(f"   ?? Job ID: {job_id}")
                logger.info(f"   ?? Request ID: {request_id}")
                
                temp_dir = Path("temp_uploads")
                temp_dir.mkdir(exist_ok=True)
                
                temp_file_path = temp_dir / f"{job_id}_{timestamped_filename}"
                with open(temp_file_path, "wb") as buffer:
                    content = await file.read()
                    buffer.write(content)
                
                # Store file
                storage_file_path = storage_manager.store_file(temp_file_path, timestamped_filename)
                
                # Queue for background processing
                background_tasks.add_task(
                    process_single_file_background,
                    job_id,
                    temp_file_path,
                    file.filename,
                    timestamped_filename,
                    document_type,
                    schema_dict,
                    custom_prompt,
                    request_id,
                    user_id,
                    storage_file_path
                )
                
                job_details.append({
                    "job_id": job_id,
                    "request_id": request_id,
                    "filename": timestamped_filename,
                    "file_path": storage_file_path
                })
            
            logger.info(f"? Batch processing queued: {len(job_details)} files")
            logger.info("=" * 80)
            
            return {
                "success": True,
                "batch_id": batch_id,
                "jobs": job_details,
                "total_files": len(job_details),
                "user_id": user_id,
                "message": f"Batch of {len(job_details)} files queued for processing with document_type: {document_type}"
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"? Failed to process batch files: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/process/job/{job_id}", response_model=JobStatus, tags=["Processing"])
    async def get_job_status(job_id: str, user = Depends(get_current_user)):
        """Get job status by job_id"""
        try:
            job_file = Path(f"temp_jobs/{job_id}.json")
            
            if not job_file.exists():
                raise HTTPException(status_code=404, detail="Job not found")
            
            with open(job_file, 'r') as f:
                job_data = json.load(f)
            
            return JobStatus(**job_data)
        
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Job not found")
        except Exception as e:
            logger.error(f"? Failed to get job status: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/process/jobs", tags=["Processing"])
    async def list_jobs(
        status: Optional[str] = Query(None, description="Filter by status"),
        limit: int = Query(50, ge=1, le=100),
        user = Depends(get_current_user)
    ):
        """List all processing jobs"""
        try:
            jobs_dir = Path("temp_jobs")
            if not jobs_dir.exists():
                return {"jobs": [], "total": 0}
            
            jobs = []
            for job_file in jobs_dir.glob("*.json"):
                try:
                    with open(job_file, 'r') as f:
                        job_data = json.load(f)
                    
                    if status and job_data.get("status") != status:
                        continue
                    
                    jobs.append(job_data)
                    if len(jobs) >= limit:
                        break
                
                except Exception as e:
                    logger.warning(f"?? Failed to read job file {job_file}: {e}")
                    continue
            
            jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
            
            return {
                "jobs": jobs,
                "total": len(jobs),
                "filtered_by_status": status
            }
        
        except Exception as e:
            logger.error(f"? Failed to list jobs: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# ==================== BACKGROUND PROCESSING TASKS ====================

async def process_single_file_background(
    job_id: str,
    temp_file_path: Path,
    original_filename: str,
    timestamped_filename: str,
    document_type: str,
    schema_json: Optional[Dict],
    custom_prompt: Optional[str],
    request_id: str,
    user_id: Optional[int] = None,
    storage_file_path: Optional[str] = None
):
    """Background file processing handler."""
    try:
        logger.info("=" * 80)
        logger.info(f"Background processing started")
        logger.info("=" * 80)
        logger.info(f"Job ID: {job_id}")
        logger.info(f"Request ID: {request_id}")
        logger.info(f"File: {timestamped_filename}")
        logger.info(f"Document Type: {document_type}")
        
        job_file = Path(f"temp_jobs/{job_id}.json")
        job_status = {
            "job_id": job_id,
            "status": "processing",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "progress_percent": 0,
            "error_message": None,
            "result_urls": None,
            "user_id": user_id,
            "file_path": storage_file_path
        }
        
        # Save initial job status
        with open(job_file, 'w') as f:
            json.dump(job_status, f)
        
        logger.info("Job status file created")
        
        # Update progress
        job_status["progress_percent"] = 25
        job_status["updated_at"] = datetime.now().isoformat()
        with open(job_file, 'w') as f:
            json.dump(job_status, f)
        
        # Call external OCR API
        logger.info("?? Calling external OCR API...")
        
        # Get OCR endpoint from config
        ocr_endpoint = config_manager.get('ocr.endpoint_url') if config_manager else None
        
        if not ocr_endpoint:
            raise Exception("OCR endpoint not configured")
        
        # Get authentication token (if needed)
        # For now, we'll use a simple approach
        auth_token = None  # TODO: Implement token retrieval
        
        # Prepare request
        with open(temp_file_path, 'rb') as f:
            files = {'file': (timestamped_filename, f, 'application/pdf')}
            data = {'document_type': document_type}
            
            if schema_json:
                data['schema_json'] = json.dumps(schema_json)
            
            headers = {}
            if auth_token:
                headers['Authorization'] = f'Bearer {auth_token}'
            
            # Make API request
            response = requests.post(
                ocr_endpoint,
                files=files,
                data=data,
                headers=headers,
                timeout=300  # 5 minutes timeout
            )
        
        if response.status_code != 200:
            error_msg = f"OCR API returned status {response.status_code}"
            logger.error(f"? {error_msg}")
            
            job_status["status"] = "failed"
            job_status["error_message"] = error_msg
            job_status["progress_percent"] = 0
            job_status["updated_at"] = datetime.now().isoformat()
            with open(job_file, 'w') as f:
                json.dump(job_status, f)
            return
        
        # Parse response
        result = response.json()
        
        logger.info(f"? OCR processing completed")
        logger.info(f"   ?? Request ID: {result.get('request_id', request_id)}")
        
        job_status["progress_percent"] = 75
        job_status["updated_at"] = datetime.now().isoformat()
        with open(job_file, 'w') as f:
            json.dump(job_status, f)
        
        # Update job status to completed
        job_status["status"] = "completed"
        job_status["progress_percent"] = 100
        job_status["request_id"] = result.get('request_id', request_id)
        job_status["result_urls"] = {
            "file_details": f"/files/file-details/{user_id}",
            "note": "Use /files/user-files to retrieve markdown and json output"
        }
        job_status["updated_at"] = datetime.now().isoformat()
        
        with open(job_file, 'w') as f:
            json.dump(job_status, f)
        
        logger.info("=" * 80)
        logger.info(f"? PROCESSING COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"?? Job ID: {job_id}")
        logger.info(f"?? Request ID: {request_id}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"? Background processing error for job {job_id}: {e}", exc_info=True)
        try:
            job_status["status"] = "failed"
            job_status["error_message"] = str(e)
            job_status["updated_at"] = datetime.now().isoformat()
            with open(job_file, 'w') as f:
                json.dump(job_status, f)
        except Exception as save_error:
            logger.error(f"? Failed to save error status: {save_error}")
    
    finally:
        # Cleanup temp file
        try:
            if temp_file_path.exists():
                temp_file_path.unlink()
                logger.info(f"??? Cleaned up temp file: {temp_file_path}")
        except Exception as cleanup_error:
            logger.warning(f"?? Failed to cleanup temp file: {cleanup_error}")