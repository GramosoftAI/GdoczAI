# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import logging
import requests
import json
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class WebhookConfig:
    """Webhook configuration for a user"""
    webhook_url: str
    webhook_token: str
    webhook_agent_name: str
    enabled: bool = True

class WebhookHandler:

    def __init__(self, config):

        self.config = config
        self.pg_config = config.pg_config
        self.timeout = 30  # Default timeout in seconds
        self.max_retries = 3
        
        logger.info("?? Webhook Handler initialized")
    
    def fetch_webhook_config(self, user_id: str) -> Optional[WebhookConfig]:

        if not user_id:
            logger.debug("?? No user_id provided, skipping webhook config fetch")
            return None
        
        try:
            import psycopg2
            
            conn = psycopg2.connect(**self.pg_config)
            cursor = conn.cursor()
            
            # Query user_webhooks table
            query = """
                SELECT webhook_url, webhook_token, webhook_agent_name
                FROM user_webhooks
                WHERE user_id = %s
                LIMIT 1
            """
            
            cursor.execute(query, (user_id,))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result:
                webhook_url, webhook_token, webhook_agent_name = result
                
                # Validate required fields
                if webhook_url and webhook_token and webhook_agent_name:
                    logger.info(f"? Webhook config found for user: {user_id}")
                    logger.debug(f"   ?? URL: {webhook_url}")
                    logger.debug(f"   ?? Token: {'*' * len(webhook_token)}")
                    logger.debug(f"   ?? Agent: {webhook_agent_name}")
                    
                    return WebhookConfig(
                        webhook_url=webhook_url,
                        webhook_token=webhook_token,
                        webhook_agent_name=webhook_agent_name,
                        enabled=True
                    )
                else:
                    logger.warning(f"?? Incomplete webhook config for user {user_id}")
                    return None
            else:
                logger.debug(f"?? No webhook config found for user: {user_id}")
                return None
        
        except Exception as e:
            logger.error(f"? Error fetching webhook config for user {user_id}: {e}")
            return None
    
    def send_webhook(
        self,
        webhook_config: WebhookConfig,
        json_payload: Dict[str, Any],
        request_id: Optional[str] = None,
        document_type: Optional[str] = None
    ) -> bool:

        if not webhook_config or not webhook_config.enabled:
            logger.debug("?? Webhook disabled or config missing")
            return False
        
        try:
            # Prepare headers
            headers = {
                'accept': 'application/json',
                'token': webhook_config.webhook_token,
                'user-agent': webhook_config.webhook_agent_name,
                'Content-Type': 'application/json',
            }
            
            # Add invoice-type header if document_type is provided
            if document_type:
                headers['invoice-type'] = document_type
            
            logger.info("=" * 80)
            logger.info("?? SENDING WEBHOOK REQUEST")
            logger.info("=" * 80)
            logger.info(f"?? URL: {webhook_config.webhook_url}")
            logger.info(f"?? Token: {webhook_config.webhook_token}")
            logger.info(f"?? User Agent: {webhook_config.webhook_agent_name}")
            if document_type:
                logger.info(f"?? Invoice Type: {document_type}")
            logger.info(f"?? Payload size: {len(json.dumps(json_payload))} bytes")
            if request_id:
                logger.info(f"?? Request ID: {request_id}")
            logger.info("=" * 80)
            
            # Send POST request with retry logic
            for attempt in range(1, self.max_retries + 1):
                try:
                    response = requests.post(
                        webhook_config.webhook_url,
                        headers=headers,
                        json=json_payload,
                        timeout=self.timeout
                    )
                    
                    # Log response details
                    logger.info(f"?? Webhook response received (Attempt {attempt}/{self.max_retries})")
                    logger.info(f"   ?? Status Code: {response.status_code}")
                    logger.info(f"   ?? Response Time: {response.elapsed.total_seconds():.2f}s")
                    
                    # Check if successful
                    if response.status_code == 200:
                        logger.info("? Webhook delivered successfully")
                        logger.debug(f"   Response: {response.text[:200]}")
                        return True
                    else:
                        logger.warning(f"?? Webhook returned non-200 status: {response.status_code}")
                        logger.debug(f"   Response: {response.text[:500]}")
                        
                        # Retry for 5xx errors
                        if 500 <= response.status_code < 600 and attempt < self.max_retries:
                            logger.info(f"?? Retrying webhook (attempt {attempt + 1}/{self.max_retries})...")
                            continue
                        
                        return False
                
                except requests.exceptions.Timeout:
                    logger.error(f"? Webhook request timeout (attempt {attempt}/{self.max_retries})")
                    if attempt < self.max_retries:
                        logger.info(f"?? Retrying webhook...")
                        continue
                    return False
                
                except requests.exceptions.ConnectionError as ce:
                    logger.error(f"?? Webhook connection error (attempt {attempt}/{self.max_retries}): {ce}")
                    if attempt < self.max_retries:
                        logger.info(f"?? Retrying webhook...")
                        continue
                    return False
                
                except requests.exceptions.RequestException as re:
                    logger.error(f"? Webhook request error (attempt {attempt}/{self.max_retries}): {re}")
                    if attempt < self.max_retries:
                        logger.info(f"?? Retrying webhook...")
                        continue
                    return False
            
            # All retries exhausted
            logger.error(f"? Webhook failed after {self.max_retries} attempts")
            return False
        
        except Exception as e:
            logger.error(f"? Unexpected error sending webhook: {e}", exc_info=True)
            return False
    
    def trigger_webhook(
        self,
        user_id: str,
        json_output: Dict[str, Any],
        request_id: Optional[str] = None,
        document_type: Optional[str] = None
    ) -> bool:

        if not user_id:
            logger.debug("?? No user_id provided, skipping webhook")
            return False
        
        logger.info("=" * 80)
        logger.info("?? WEBHOOK TRIGGER INITIATED")
        logger.info("=" * 80)
        logger.info(f"?? User ID: {user_id}")
        if document_type:
            logger.info(f"?? Document Type: {document_type}")
        if request_id:
            logger.info(f"?? Request ID: {request_id}")
        logger.info("=" * 80)
        
        # Fetch webhook configuration
        webhook_config = self.fetch_webhook_config(user_id)
        
        if not webhook_config:
            logger.info("?? No webhook configured for this user - skipping webhook delivery")
            logger.info("=" * 80)
            return False
        
        # Send webhook
        success = self.send_webhook(webhook_config, json_output, request_id, document_type)
        
        if success:
            logger.info("=" * 80)
            logger.info("? WEBHOOK DELIVERY SUCCESSFUL")
            logger.info("=" * 80)
        else:
            logger.warning("=" * 80)
            logger.warning("?? WEBHOOK DELIVERY FAILED")
            logger.warning("? OCR processing completed successfully despite webhook failure")
            logger.warning("=" * 80)
        
        return success

def trigger_webhook_if_needed(
    webhook_handler: WebhookHandler,
    user_id: Optional[str],
    json_output: Dict[str, Any],
    request_id: Optional[str] = None,
    document_type: Optional[str] = None
) -> bool:

    if not webhook_handler:
        logger.debug("?? No webhook handler available")
        return False
    
    try:
        return webhook_handler.trigger_webhook(
            user_id=user_id,
            json_output=json_output,
            request_id=request_id,
            document_type=document_type
        )
    except Exception as e:
        logger.error(f"? Webhook trigger error (non-blocking): {e}", exc_info=True)
        logger.warning("? OCR processing continues despite webhook error")
        return False

# ============================================================================
# EXAMPLE USAGE
# ============================================================================
if __name__ == "__main__":
    # Example configuration
    class MockConfig:
        pg_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'ocr_db',
            'user': 'ocr_user',
            'password': 'ocr_password'
        }
    
    # Initialize handler
    config = MockConfig()
    handler = WebhookHandler(config)
    
    # Example JSON output
    sample_json = {
        "invoice_number": "INV-2024-001",
        "date": "2024-12-16",
        "amount": 1500.00,
        "vendor": "Acme Corp"
    }
    
    # Trigger webhook
    success = handler.trigger_webhook(
        user_id="user_123",
        json_output=sample_json,
        request_id="req_abc123",
        document_type="Service Request"
    )
    
    if success:
        print("? Webhook delivered successfully")
    else:
        print("? Webhook delivery failed")