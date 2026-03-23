# -*- coding: utf-8 -*-

"""
?? Authentication Management for SFTP ? OCR Document Pipeline
Responsibilities:
- Call /pipeline/auth/signin API
- Parse and store JWT tokens
- Manage token lifecycle (refresh every 20 hours)
- Expose get_valid_token() for OCR requests
"""

import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from threading import RLock

from src.services.sftp_fetch.sftp_fetch_config import AuthConfig
from src.services.sftp_fetch.sftp_fetch_models import TokenState, AuthCredentials

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Raised when authentication fails"""
    pass

class TokenManager:

    def __init__(self, auth_config: AuthConfig):
        self.auth_config = auth_config
        self.token_state: Optional[TokenState] = None
        self._lock = RLock()
        self._signin_in_progress = False
        logger.info("?? TokenManager initialized")
        logger.info(f"?? Auth endpoint: {auth_config.signin_url}")
        logger.info(f"?? Email: {auth_config.username}")
        logger.info(f"? Token refresh interval: {auth_config.token_refresh_interval_hours} hours")
    
    def _perform_signin_request(self) -> Dict[str, Any]:
        
        credentials = {
            "email": self.auth_config.username,
            "password": self.auth_config.password
        }
        
        try:
            logger.info(f"?? Calling signin API: {self.auth_config.signin_url}")
            logger.info(f"?? Email: {credentials['email']}")
            
            response = requests.post(
                self.auth_config.signin_url,
                json=credentials,
                timeout=30,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            )
            
            logger.info(f"?? Response Status: {response.status_code}")
            
            if response.status_code != 200:
                error_msg = f"Authentication failed with status {response.status_code}"
                try:
                    error_data = response.json()
                    error_detail = error_data.get('error') or error_data.get('message') or error_data.get('detail')
                    if error_detail:
                        error_msg = f"{error_msg}: {error_detail}"
                except:
                    error_msg = f"{error_msg}: {response.text[:200]}"
                
                logger.error(f"? {error_msg}")
                raise AuthenticationError(error_msg)
            
            try:
                response_data = response.json()
            except Exception as e:
                logger.error(f"? Failed to parse JSON response: {e}")
                raise AuthenticationError(f"Invalid JSON response: {str(e)}")
            
            access_token = response_data.get('access_token')
            if not access_token:
                logger.error("? No access_token in response")
                logger.debug(f"Response data: {response_data}")
                raise AuthenticationError("No access_token in response")
            
            return response_data
        
        except requests.exceptions.Timeout:
            error_msg = "Authentication request timed out after 30 seconds"
            logger.error(f"? {error_msg}")
            raise AuthenticationError(error_msg)
        
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Failed to connect to authentication server: {str(e)}"
            logger.error(f"? {error_msg}")
            raise AuthenticationError(error_msg)
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Authentication request failed: {str(e)}"
            logger.error(f"? {error_msg}")
            raise AuthenticationError(error_msg)
        
        except AuthenticationError:
            raise
        
        except Exception as e:
            error_msg = f"Unexpected error during authentication: {str(e)}"
            logger.error(f"? {error_msg}", exc_info=True)
            raise AuthenticationError(error_msg)
    
    def signin(self) -> TokenState:

        logger.info("=" * 80)
        logger.info("?? SIGNING IN TO OBTAIN JWT TOKEN")
        logger.info("=" * 80)
        
        with self._lock:
            if self._signin_in_progress:
                logger.info("? Sign-in already in progress, waiting for completion...")
                while self._signin_in_progress:
                    pass
                
                if self.token_state is not None and self.token_state.is_valid():
                    logger.info("? Token obtained by concurrent thread, reusing")
                    return self.token_state
            
            self._signin_in_progress = True
            current_refresh_count = 0 if self.token_state is None else self.token_state.refresh_count
        
        try:
            response_data = self._perform_signin_request()
            access_token = response_data['access_token']
            
            expires_at = datetime.now() + timedelta(hours=24)
            
            with self._lock:
                self.token_state = TokenState(
                    access_token=access_token,
                    expires_at=expires_at,
                    refresh_count=current_refresh_count + 1,
                    last_refreshed=datetime.now()
                )
                
                new_token_state = self.token_state
            
            logger.info("? Authentication successful")
            logger.info(f"?? Token obtained: {access_token[:20]}...{access_token[-10:]}")
            logger.info(f"? Token expires at: {expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"?? Token valid for: 24 hours")
            logger.info(f"?? Refresh count: {new_token_state.refresh_count}")
            logger.info("=" * 80)
            
            return new_token_state
        
        finally:
            with self._lock:
                self._signin_in_progress = False
    
    def get_valid_token(self) -> str:

        with self._lock:
            # Case 1: No token exists yet
            if self.token_state is None:
                logger.info("?? No token exists, performing initial signin")
                # Release lock before calling signin (which will re-acquire it)
                needs_signin = True
            # Case 2: Token is expired
            elif self.token_state.is_expired():
                logger.warning("?? Token has expired, refreshing...")
                needs_signin = True
            else:
                # Case 3: Token exists and is valid
                remaining_hours = self.token_state.time_until_expiry() / 3600
                logger.debug(f"? Using existing valid token (expires in {remaining_hours:.2f} hours)")
                return self.token_state.access_token
        
        # Perform sign-in outside the lock to avoid deadlock
        if needs_signin:
            token_state = self.signin()
            return token_state.access_token
    
    def should_refresh_token(self) -> bool:

        with self._lock:
            if self.token_state is None:
                return True
            if self.token_state.is_expired():
                return True
            buffer_hours = 24 - self.auth_config.token_refresh_interval_hours
            return self.token_state.should_refresh(buffer_hours=buffer_hours)
    
    def refresh_token_if_needed(self) -> bool:

        should_refresh = self.should_refresh_token()
        
        if should_refresh:
            logger.info("?? Token refresh needed")
            self.signin()
            return True
        else:
            with self._lock:
                if self.token_state:
                    remaining_hours = self.token_state.time_until_expiry() / 3600
                    logger.debug(f"?? Token refresh not needed (expires in {remaining_hours:.2f} hours)")
            return False
    
    def force_refresh(self) -> TokenState:
        logger.info("?? Forcing token refresh")
        return self.signin()
    
    def get_token_info(self) -> Optional[Dict[str, Any]]:
        
        with self._lock:
            if self.token_state is None:
                return None
            return {
                "is_valid": self.token_state.is_valid(),
                "expires_at": self.token_state.expires_at.isoformat(),
                "time_until_expiry_hours": round(self.token_state.time_until_expiry() / 3600, 2),
                "refresh_count": self.token_state.refresh_count,
                "last_refreshed": self.token_state.last_refreshed.isoformat() if self.token_state.last_refreshed else None,
                "should_refresh": self.should_refresh_token()
            }
    
    def clear_token(self):
        with self._lock:
            logger.info("??? Clearing stored token")
            self.token_state = None


class AuthenticationManager:

    def __init__(self, auth_config: AuthConfig):

        self.token_manager = TokenManager(auth_config)
        logger.info("?? AuthenticationManager initialized")
    
    def initialize(self) -> bool:

        try:
            logger.info("?? Initializing authentication...")
            self.token_manager.signin()
            logger.info("? Authentication initialized successfully")
            return True
        except AuthenticationError as e:
            logger.error(f"? Failed to initialize authentication: {e}")
            return False
    
    def get_auth_header(self) -> str:

        token = self.token_manager.get_valid_token()
        return f"Bearer {token}"
    
    def get_token(self) -> str:

        return self.token_manager.get_valid_token()
    
    def check_and_refresh(self) -> bool:

        try:
            return self.token_manager.refresh_token_if_needed()
        except AuthenticationError as e:
            logger.error(f"? Token refresh failed: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:

        token_info = self.token_manager.get_token_info()
        
        if token_info is None:
            return {
                "authenticated": False,
                "token_exists": False
            }
        
        return {
            "authenticated": True,
            "token_exists": True,
            "token_valid": token_info["is_valid"],
            "expires_at": token_info["expires_at"],
            "time_until_expiry_hours": token_info["time_until_expiry_hours"],
            "refresh_count": token_info["refresh_count"],
            "last_refreshed": token_info["last_refreshed"],
            "should_refresh": token_info["should_refresh"]
        }


def get_auth_token(auth_config: AuthConfig) -> str:

    manager = TokenManager(auth_config)
    return manager.get_valid_token()