#!/usr/bin/env python3

import logging
import secrets
import hashlib
import hmac
import yaml
from fastapi import HTTPException, Depends
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from src.api.models.api_server_models import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== ENCRYPTION UTILITIES ====================

def load_encryption_key():
    """Load encryption key from config.yaml"""
    try:
        with open('config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            encryption_key = config.get('encryption', {}).get('api_key_encryption_key')
            
            if not encryption_key:
                raise ValueError("api_key_encryption_key not found in config.yaml")
            
            # Ensure key is bytes
            if isinstance(encryption_key, str):
                encryption_key = encryption_key.encode()
            
            return encryption_key
    except FileNotFoundError:
        logger.error("config.yaml file not found")
        raise HTTPException(status_code=500, detail="Configuration file not found")
    except Exception as e:
        logger.error(f"Error loading encryption key: {e}")
        raise HTTPException(status_code=500, detail="Failed to load encryption configuration")


def generate_api_key():

    # Random separator
    separators = ['+', '=', '_', '-', '*', '&']
    separator = secrets.choice(separators)

    # Generate timestamp (e.g. "110326022447" for 11 Mar 2026 02:24:47)
    timestamp = datetime.utcnow().strftime("%d%m%y%H%M%S")

    # 8 random hex characters
    random_part = secrets.token_hex(4)  # 4 bytes = 8 hex chars
    
    # Construct API key: gspl-<timestamp><separator><random>
    api_key = f"gspl-{timestamp}{separator}{random_part}"
    
    return api_key


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()

def encrypt_api_key(api_key: str, encryption_key: bytes) -> str:

    try:
        fernet = Fernet(encryption_key)
        encrypted = fernet.encrypt(api_key.encode())
        return encrypted.decode()
    except Exception as e:
        logger.error(f"Error encrypting API key: {e}")
        raise HTTPException(status_code=500, detail="Failed to encrypt API key")


def decrypt_api_key(encrypted_key: str, encryption_key: bytes) -> str:

    try:
        fernet = Fernet(encryption_key)
        decrypted = fernet.decrypt(encrypted_key.encode())
        return decrypted.decode()
    except Exception as e:
        logger.error(f"Error decrypting API key: {e}")
        raise HTTPException(status_code=500, detail="Failed to decrypt API key")


# ==================== PYDANTIC MODELS ====================

class ApiKeyResponse(BaseModel):
    id: int
    user_id: int
    api_key: Optional[str] = Field(None, description="The actual API key")
    key_prefix: str = Field(..., description="First 8 characters of the key for identification")
    status: str
    description: Optional[str]
    created_at: str
    updated_at: str
    expires_at: Optional[str]


class ApiKeyListResponse(BaseModel):
    id: int
    user_id: int
    key_prefix: str
    status: str
    description: Optional[str]
    created_at: str
    updated_at: str
    expires_at: Optional[str]


# ==================== USER API KEYS ENDPOINTS ====================

def create_user_apikey_routes(app, get_current_user):
    """Create user-based API key routes (one API key per user)"""

    @app.post("/user-apikeys", tags=["User API Keys"], response_model=ApiKeyResponse)
    async def create_user_apikey(
        current_user = Depends(get_current_user)
    ):

        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if API key already exists for this user
            cursor.execute("""
                SELECT id, status FROM api_keys
                WHERE user_id = %s
            """, (user_id,))
            
            existing = cursor.fetchone()
            if existing:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"API key already exists for user {user_id}. Use DELETE to remove it first before creating a new one."
                )
            
            # Generate new API key
            api_key = generate_api_key()
            key_prefix = api_key[:12]  # Store first 12 chars for identification (gspl-02m26y1)
            
            # Hash the API key (one-way, for authentication)
            key_hash = hash_api_key(api_key)
            
            # Load encryption key and encrypt the API key (reversible, for recovery)
            encryption_key = load_encryption_key()
            encrypted_key = encrypt_api_key(api_key, encryption_key)
            
            # API keys never expire by default (expires_at = NULL)
            expires_at = None
            
            # Insert new API key
            cursor.execute("""
                INSERT INTO api_keys 
                (user_id, key_hash, encrypted_key, status, expires_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, user_id, status, created_at, updated_at, expires_at
            """, (
                user_id,
                key_hash,
                encrypted_key,
                'active',
                expires_at
            ))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"? Created API key for user_id: {user_id}")
            logger.info(f"   ?? Key Prefix: {key_prefix}")
            logger.info(f"   ? Expires: Never")
            logger.info(f"   ? Status: active")
            
            # Return the API key
            return ApiKeyResponse(
                id=result['id'],
                user_id=result['user_id'],
                api_key=api_key,
                key_prefix=key_prefix,
                status=result['status'],
                description=None,
                created_at=result['created_at'].isoformat(),
                updated_at=result['updated_at'].isoformat(),
                expires_at=result['expires_at'].isoformat() if result['expires_at'] else None
            )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating user API key: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to create API key: {str(e)}")

    @app.get("/user-apikeys", tags=["User API Keys"])
    async def get_user_apikey(current_user = Depends(get_current_user)):

        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT id, user_id, encrypted_key, status, created_at, updated_at, expires_at
                FROM api_keys
                WHERE user_id = %s
            """, (user_id,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result:
                return {
                    "success": True,
                    "api_key": None,
                    "message": "No API key found for this user"
                }
            
            # Decrypt the API key
            encryption_key = load_encryption_key()
            decrypted_api_key = decrypt_api_key(result['encrypted_key'], encryption_key)
            key_prefix = decrypted_api_key[:12]
            
            return {
                "success": True,
                "api_key": {
                    "id": result['id'],
                    "user_id": result['user_id'],
                    "api_key": decrypted_api_key,
                    "key_prefix": key_prefix,
                    "status": result['status'],
                    "created_at": result['created_at'].isoformat(),
                    "updated_at": result['updated_at'].isoformat(),
                    "expires_at": result['expires_at'].isoformat() if result['expires_at'] else None
                }
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting user API key: {e}", exc_info=True)
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to get API key: {str(e)}")

    @app.delete("/user-apikeys", tags=["User API Keys"])
    async def delete_user_apikey(current_user = Depends(get_current_user)):

        conn = None
        try:
            user_id = current_user.get('user_id')
            if not user_id:
                raise HTTPException(status_code=401, detail="User ID not found in token")
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Check if API key exists for user
            cursor.execute("""
                SELECT id FROM api_keys
                WHERE user_id = %s
            """, (user_id,))
            
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404, 
                    detail="API key not found for this user"
                )
            
            # Delete API key
            cursor.execute("""
                DELETE FROM api_keys
                WHERE user_id = %s
            """, (user_id,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"??? Deleted API key for user_id: {user_id}")
            
            return {
                "success": True,
                "message": "API key deleted successfully. You can now create a new one."
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting user API key: {e}", exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            raise HTTPException(status_code=500, detail=f"Failed to delete API key: {str(e)}")


# ==================== HELPER FUNCTION FOR API KEY VALIDATION ====================

def validate_api_key_from_header(api_key: str) -> Optional[dict]:

    conn = None
    try:
        # Hash the incoming API key
        key_hash = hash_api_key(api_key)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Look up the hashed key
        cursor.execute("""
            SELECT id, user_id, status, expires_at
            FROM api_keys
            WHERE key_hash = %s
        """, (key_hash,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            logger.warning("API key not found in database")
            return None
        
        # Check if key is active
        if result['status'] != 'active':
            logger.warning(f"API key is not active. Status: {result['status']}")
            return None
        
        # Check if key is expired
        if result['expires_at']:
            if datetime.utcnow() > result['expires_at']:
                logger.warning("API key has expired")
                return None
        
        logger.info(f"? Valid API key for user_id: {result['user_id']}")
        
        return {
            'user_id': result['user_id'],
            'api_key_id': result['id']
        }
    
    except Exception as e:
        logger.error(f"Error validating API key: {e}", exc_info=True)
        if conn:
            conn.close()
        return None