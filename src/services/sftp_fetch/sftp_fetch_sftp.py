#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import paramiko
import logging
import stat
import uuid
from typing import List, Optional, Tuple
from pathlib import Path
from io import BytesIO

from src.services.sftp_fetch.sftp_fetch_config import SFTPConfig
from src.services.sftp_fetch.sftp_fetch_models import SFTPFile

logger = logging.getLogger(__name__)


class SFTPConnectionError(Exception):
    pass

class SFTPOperationError(Exception):
    pass

def generate_unique_filename(original_filename: str) -> str:

    file_path = Path(original_filename)
    name_without_ext = file_path.stem
    extension = file_path.suffix
    
    # Generate short UUID (first 8 characters)
    unique_id = str(uuid.uuid4())[:8]
    
    # Construct new filename: name_uuid.extension
    new_filename = f"{name_without_ext}_{unique_id}{extension}"
    
    logger.debug(f"?? Generated unique filename: {original_filename} ? {new_filename}")
    
    return new_filename


class SFTPClient:

    def __init__(self, sftp_config: SFTPConfig):

        self.config = sftp_config
        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.sftp_client: Optional[paramiko.SFTPClient] = None
        self.is_connected = False
        
        logger.info("?? SFTPClient initialized")
        logger.info(f"??? Host: {sftp_config.host}:{sftp_config.port}")
        logger.info(f"?? Username: {sftp_config.username}")
        logger.info(f"?? Monitored folders: {len(sftp_config.monitored_folders)}")
        
        # Check authentication method
        if hasattr(sftp_config, 'private_key_path') and sftp_config.private_key_path:
            logger.info(f"?? Auth method: SSH Key ({sftp_config.private_key_path})")
        else:
            logger.info(f"?? Auth method: Password")
    
    def _load_private_key(self, key_path: str, password: Optional[str] = None) -> paramiko.PKey:

        try:
            key_file = Path(key_path)
            
            if not key_file.exists():
                error_msg = f"Private key file not found: {key_path}"
                logger.error(f"? {error_msg}")
                raise SFTPConnectionError(error_msg)
            
            logger.info(f"?? Loading private key: {key_path}")
            
            # Try different key types (DSS/DSA removed - deprecated and insecure)
            key_types = [
                (paramiko.RSAKey, "RSA"),
                (paramiko.Ed25519Key, "Ed25519"),
                (paramiko.ECDSAKey, "ECDSA"),
            ]
            
            last_error = None
            
            for key_class, key_type in key_types:
                try:
                    if password:
                        key = key_class.from_private_key_file(str(key_file), password=password)
                    else:
                        key = key_class.from_private_key_file(str(key_file))
                    
                    logger.info(f"? Successfully loaded {key_type} key")
                    return key
                
                except paramiko.SSHException as e:
                    last_error = e
                    continue
                except Exception as e:
                    last_error = e
                    continue
            
            # If we get here, no key type worked
            error_msg = (
                f"Failed to load private key from {key_path}. "
                f"Last error: {last_error}. "
                f"Supported key types: RSA, Ed25519, ECDSA. "
                f"Make sure the key is in OpenSSH format and not DSA/DSS (deprecated)."
            )
            logger.error(f"? {error_msg}")
            raise SFTPConnectionError(error_msg)
        
        except SFTPConnectionError:
            raise
        except Exception as e:
            error_msg = f"Error loading private key: {str(e)}"
            logger.error(f"? {error_msg}")
            raise SFTPConnectionError(error_msg)
    
    def connect(self) -> bool:

        try:
            logger.info("=" * 80)
            logger.info("?? CONNECTING TO SFTP SERVER")
            logger.info("=" * 80)
            logger.info(f"??? Host: {self.config.host}")
            logger.info(f"?? Port: {self.config.port}")
            logger.info(f"?? Username: {self.config.username}")
            
            # Initialize SSH client
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Determine authentication method
            use_key_auth = (
                hasattr(self.config, 'private_key_path') and 
                self.config.private_key_path
            )
            
            if use_key_auth:
                # SSH Key Authentication
                logger.info(f"?? Using SSH key authentication")
                logger.info(f"?? Key file: {self.config.private_key_path}")
                
                # Load private key (may raise SFTPConnectionError)
                key_password = getattr(self.config, 'key_password', None)
                private_key = self._load_private_key(
                    self.config.private_key_path, 
                    key_password
                )
                
                # Connect with key
                try:
                    self.ssh_client.connect(
                        hostname=self.config.host,
                        port=self.config.port,
                        username=self.config.username,
                        pkey=private_key,
                        timeout=30,
                        banner_timeout=30,
                        auth_timeout=30,
                        look_for_keys=False,  # Don't look for default keys
                        allow_agent=False     # Don't use SSH agent
                    )
                except paramiko.AuthenticationException as e:
                    error_msg = f"SSH key authentication failed for {self.config.username}@{self.config.host}: {str(e)}"
                    logger.error(f"? {error_msg}")
                    raise SFTPConnectionError(error_msg)
            else:
                # Password Authentication
                logger.info(f"?? Using password authentication")
                
                # Connect with password
                try:
                    self.ssh_client.connect(
                        hostname=self.config.host,
                        port=self.config.port,
                        username=self.config.username,
                        password=self.config.password,
                        timeout=30,
                        banner_timeout=30,
                        auth_timeout=30
                    )
                except paramiko.AuthenticationException as e:
                    error_msg = f"Password authentication failed for {self.config.username}@{self.config.host}: {str(e)}"
                    logger.error(f"? {error_msg}")
                    raise SFTPConnectionError(error_msg)
            
            # Open SFTP session
            try:
                self.sftp_client = self.ssh_client.open_sftp()
            except Exception as e:
                error_msg = f"Failed to open SFTP session: {str(e)}"
                logger.error(f"? {error_msg}")
                raise SFTPConnectionError(error_msg)
            
            self.is_connected = True
            
            logger.info("? SFTP connection established successfully")
            logger.info("=" * 80)
            
            return True
        
        except SFTPConnectionError:
            # Re-raise SFTPConnectionError as-is (for email alerts)
            raise
        
        except paramiko.SSHException as e:
            error_msg = f"SSH connection failed to {self.config.host}:{self.config.port}: {str(e)}"
            logger.error(f"? {error_msg}")
            raise SFTPConnectionError(error_msg)
        
        except FileNotFoundError as e:
            error_msg = f"Private key file not found: {str(e)}"
            logger.error(f"? {error_msg}")
            raise SFTPConnectionError(error_msg)
        
        except Exception as e:
            error_msg = f"Failed to connect to SFTP server {self.config.host}:{self.config.port}: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            raise SFTPConnectionError(error_msg)
    
    def disconnect(self):
        """Close SFTP connection safely"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
                logger.debug("?? SFTP client closed")
            
            if self.ssh_client:
                self.ssh_client.close()
                logger.debug("?? SSH client closed")
            
            self.is_connected = False
            logger.info("? SFTP connection closed")
        
        except Exception as e:
            logger.warning(f"?? Error closing SFTP connection: {e}")
    
    def ensure_connected(self):

        if not self.is_connected or self.sftp_client is None:
            logger.info("?? SFTP not connected, establishing connection...")
            self.connect()
    
    def list_files_in_folder(self, folder_path: str) -> List[SFTPFile]:

        self.ensure_connected()
        
        try:
            logger.debug(f"?? Scanning folder: {folder_path}")
            
            # List directory contents
            entries = self.sftp_client.listdir_attr(folder_path)
            
            files = []
            for entry in entries:
                # Skip directories - Use stat.S_ISDIR instead of paramiko.sftp_attr.S_ISDIR
                if not stat.S_ISDIR(entry.st_mode):
                    file_path = f"{folder_path}/{entry.filename}"
                    folder_name = Path(folder_path).name
                    
                    sftp_file = SFTPFile(
                        file_path=file_path,
                        filename=entry.filename,
                        folder_path=folder_path,
                        folder_name=folder_name,
                        size_bytes=entry.st_size
                    )
                    
                    files.append(sftp_file)
            
            logger.debug(f"? Found {len(files)} files in {folder_path}")
            return files
        
        except FileNotFoundError:
            logger.warning(f"?? Folder not found: {folder_path}")
            return []
        
        except Exception as e:
            error_msg = f"Failed to list files in {folder_path}: {str(e)}"
            logger.error(f"? {error_msg}")
            raise SFTPOperationError(error_msg)
    
    def scan_all_monitored_folders(self) -> List[SFTPFile]:

        self.ensure_connected()
        
        logger.info("=" * 80)
        logger.info("?? SCANNING ALL MONITORED FOLDERS")
        logger.info("=" * 80)
        
        all_files = []
        
        for folder_path in self.config.monitored_folders:
            try:
                files = self.list_files_in_folder(folder_path)
                all_files.extend(files)
                logger.info(f"?? {folder_path}: {len(files)} files")
            except SFTPOperationError as e:
                logger.error(f"? Failed to scan {folder_path}: {e}")
                # Continue with other folders
                continue
        
        logger.info(f"? Total files found: {len(all_files)}")
        logger.info("=" * 80)
        
        return all_files
    
    def filter_pdf_files(self, files: List[SFTPFile]) -> List[SFTPFile]:

        pdf_files = [f for f in files if f.is_pdf()]
        
        logger.info(f"?? Filtered {len(pdf_files)} PDF files from {len(files)} total files")
        
        return pdf_files
    
    def download_file(self, file_path: str) -> bytes:

        self.ensure_connected()
        
        try:
            logger.debug(f"?? Downloading: {file_path}")
            
            # Download to BytesIO buffer
            buffer = BytesIO()
            self.sftp_client.getfo(file_path, buffer)
            
            # Get bytes
            file_bytes = buffer.getvalue()
            buffer.close()
            
            file_size_mb = len(file_bytes) / (1024 * 1024)
            logger.debug(f"? Downloaded {file_size_mb:.2f} MB from {Path(file_path).name}")
            
            return file_bytes
        
        except FileNotFoundError:
            error_msg = f"File not found: {file_path}"
            logger.error(f"? {error_msg}")
            raise SFTPOperationError(error_msg)
        
        except Exception as e:
            error_msg = f"Failed to download {file_path}: {str(e)}"
            logger.error(f"? {error_msg}")
            raise SFTPOperationError(error_msg)
    
    def move_file(self, source_path: str, destination_folder: str) -> Tuple[bool, str]:

        self.ensure_connected()
        
        try:
            original_filename = Path(source_path).name
            destination_path = f"{destination_folder}/{original_filename}"
            final_filename = original_filename
            
            logger.debug(f"?? Moving: {source_path}")
            logger.debug(f"?? To: {destination_path}")
            
            # Ensure destination folder exists
            try:
                self.sftp_client.stat(destination_folder)
            except FileNotFoundError:
                logger.info(f"?? Creating destination folder: {destination_folder}")
                self.sftp_client.mkdir(destination_folder)
            
            # ?? CHECK IF FILE EXISTS IN DESTINATION
            file_exists = False
            try:
                self.sftp_client.stat(destination_path)
                file_exists = True
                logger.warning(f"?? File already exists in destination: {original_filename}")
            except FileNotFoundError:
                # File doesn't exist, proceed with original name
                pass
            
            # ?? GENERATE UNIQUE FILENAME IF DUPLICATE EXISTS
            if file_exists:
                unique_filename = generate_unique_filename(original_filename)
                destination_path = f"{destination_folder}/{unique_filename}"
                final_filename = unique_filename
                
                logger.info(f"?? Renaming to avoid conflict: {original_filename} ? {unique_filename}")
                logger.debug(f"?? New destination: {destination_path}")
            
            # Move file (rename operation in SFTP)
            self.sftp_client.rename(source_path, destination_path)
            
            if file_exists:
                logger.info(f"? Moved with UUID rename: {final_filename} ? {destination_folder}")
            else:
                logger.info(f"? Moved: {final_filename} ? {destination_folder}")
            
            return True, final_filename
        
        except FileNotFoundError as e:
            error_msg = f"File not found during move: {source_path}"
            logger.error(f"? {error_msg}")
            raise SFTPOperationError(error_msg)
        
        except Exception as e:
            error_msg = f"Failed to move {source_path}: {str(e)}"
            logger.error(f"? {error_msg}")
            raise SFTPOperationError(error_msg)
    
    def move_to_processed(self, file_path: str) -> Tuple[bool, str]:
        return self.move_file(file_path, self.config.moved_folder)
    
    def move_to_failed(self, file_path: str) -> Tuple[bool, str]:
        return self.move_file(file_path, self.config.failed_folder)
    
    def file_exists(self, file_path: str) -> bool:

        self.ensure_connected()
        
        try:
            self.sftp_client.stat(file_path)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.warning(f"?? Error checking file existence: {e}")
            return False
    
    def get_file_size(self, file_path: str) -> Optional[int]:

        self.ensure_connected()
        
        try:
            stat = self.sftp_client.stat(file_path)
            return stat.st_size
        except FileNotFoundError:
            logger.warning(f"?? File not found: {file_path}")
            return None
        except Exception as e:
            logger.warning(f"?? Error getting file size: {e}")
            return None


class SFTPManager:

    def __init__(self, sftp_config: SFTPConfig):
        
        self.config = sftp_config
        self.client: Optional[SFTPClient] = None
        
        logger.info("?? SFTPManager initialized")
    
    def __enter__(self):

        self.client = SFTPClient(self.config)
        self.client.connect()  # May raise SFTPConnectionError
        return self.client
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.disconnect()
    
    def get_new_pdfs(self) -> List[SFTPFile]:

        with self as client:
            all_files = client.scan_all_monitored_folders()
            pdf_files = client.filter_pdf_files(all_files)
            
            return pdf_files
    
    def download_and_move(self, sftp_file: SFTPFile) -> Tuple[bytes, bool, str]:

        with self as client:
            # Download file
            file_bytes = client.download_file(sftp_file.file_path)
            
            # Move to processed folder (handles UUID renaming)
            move_success, final_filename = client.move_to_processed(sftp_file.file_path)
            
            return file_bytes, move_success, final_filename