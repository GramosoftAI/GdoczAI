# -*- coding: utf-8 -*-

"""
Multi-User IMAP Connector Manager for Email Fetcher Pipeline

Responsibility:
- Poll database for active SMTP/IMAP connectors (every 1 second)
- Create per-connector EmailFetcherCore instances
- Manage per-connector PerConnectorEmailScheduler instances
- Track running connectors in-memory
- Start/stop connectors based on is_active flag
- Handle connector activation and deactivation
"""

import logging
import threading
import time
from typing import Dict, Optional, List
from datetime import datetime
import json

from src.services.sftp_fetch.sftp_fetch_config import get_slim_config
from src.services.smtp_fetch.smtp_fetcher_config import IMAPConnectorConfig
from src.services.smtp_fetch.smtp_fetcher_scheduler import PerConnectorEmailScheduler
from src.services.smtp_fetch.smtp_fetcher_database import init_db_connection, query_active_smtp_connectors

logger = logging.getLogger(__name__)

class EmailConnectorDBError(Exception):
    """Raised when database operations fail. Mirrors SFTPConnectorDBError."""
    pass

class EmailConnectorManager:

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.slim_config = get_slim_config(config_path)

        import yaml
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                full_config = yaml.safe_load(f)
                pg_config = full_config.get('postgres', {})
        except Exception as e:
            logger.error(f"Failed to load postgres config: {e}")
            raise

        try:
            init_db_connection(pg_config)
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

        self.running_connectors: Dict[int, 'RunningEmailConnector'] = {}
        self._lock = threading.Lock()

        self.is_running = False
        self._stop_event = threading.Event()
        self._poll_thread: Optional[threading.Thread] = None

        logger.info("=" * 80)
        logger.info("EMAIL CONNECTOR MANAGER INITIALIZED")
        logger.info("=" * 80)
        logger.info(f"Configuration: {config_path}")
        logger.info(f"Auth Endpoint: {self.slim_config.auth.signin_url}")
        logger.info(f"OCR Endpoint: {self.slim_config.ocr.endpoint_url}")
        logger.info("=" * 80)

    def _get_active_connectors_from_db(self) -> List[dict]:
        try:
            connectors = query_active_smtp_connectors()
            return connectors
        except Exception as e:
            logger.error(f"Database error while querying connectors: {e}", exc_info=True)
            raise EmailConnectorDBError(f"Failed to query connectors: {e}")

    def _create_imap_config_from_row(self, connector_row: dict) -> IMAPConnectorConfig:
        approved_senders_str = connector_row.get('approved_senders', '')
        if approved_senders_str:
            approved_senders = [s.strip() for s in approved_senders_str.split(',') if s.strip()]
        else:
            approved_senders = []

        email_method = connector_row.get('email_method', 'gmail')
        if not email_method:
            email_method = 'gmail'
        email_method = email_method.lower().strip()
        
        if email_method == 'hostinger':
            imap_server = 'imap.hostinger.com'
        elif email_method == 'gmail':
            imap_server = 'imap.gmail.com'
        else:
            logger.warning(f"Unknown email_method {email_method}, defaulting to imap.gmail.com")
            imap_server = 'imap.gmail.com'

        base_dir = f"downloads/email_pdfs/user_{connector_row['user_id']}"
        
        imap_config = IMAPConnectorConfig(
            email_id=connector_row['email_id'],
            app_password=connector_row['app_password'],
            imap_server=imap_server,
            imap_port=993,
            approved_senders=approved_senders,
            email_method=email_method,
            download_dir=f"{base_dir}/inbox",
            processed_dir=f"{base_dir}/processed",
            failed_dir=f"{base_dir}/failed"
        )
        return imap_config

    def _start_connector(self, connector_row: dict):
        user_id = connector_row['user_id']
        connector_id = connector_row['id']

        try:
            logger.info("=" * 80)
            logger.info(f"STARTING NEW CONNECTOR")
            logger.info(f"Connector ID: {connector_id}")
            logger.info(f"User ID: {user_id}")
            email_masked = "***" + connector_row['email_id'][connector_row['email_id'].find('@'):] if '@' in connector_row['email_id'] else connector_row['email_id']
            logger.info(f"Email: {email_masked}")
            logger.info(f"Scan Interval: {connector_row['interval_minute']} minutes")
            logger.info("=" * 80)

            imap_config = self._create_imap_config_from_row(connector_row)
            
            scheduler = PerConnectorEmailScheduler(
                connector_id=connector_id,
                user_id=user_id,
                imap_config=imap_config,
                slim_config=self.slim_config,
                scan_interval_minutes=connector_row['interval_minute']
            )
            
            scheduler.start()

            running_connector = RunningEmailConnector(
                connector_id=connector_id,
                user_id=user_id,
                email_account=connector_row['email_id'],
                imap_server=imap_config.imap_server,
                scheduler=scheduler,
                started_at=datetime.now(),
                scan_interval_minutes=connector_row['interval_minute'],
                connector_row=connector_row
            )

            with self._lock:
                self.running_connectors[user_id] = running_connector

            logger.info(f"Connector {connector_id} for user {user_id} STARTED")
            
        except Exception as e:
            logger.error(f"Failed to start connector {connector_id}: {e}", exc_info=True)

    def _stop_connector(self, user_id: int, connector_id: int):
        try:
            logger.info("=" * 80)
            logger.info(f"STOPPING CONNECTOR")
            logger.info(f"Connector ID: {connector_id}")
            logger.info(f"User ID: {user_id}")
            logger.info("=" * 80)

            with self._lock:
                if user_id in self.running_connectors:
                    running_connector = self.running_connectors[user_id]
                    running_connector.scheduler.stop()
                    del self.running_connectors[user_id]
                    logger.info(f"Connector {connector_id} for user {user_id} STOPPED")
                else:
                    logger.warning(f"Connector for user {user_id} was not running")

        except Exception as e:
            logger.error(f"Error stopping connector {connector_id}: {e}", exc_info=True)

    def _poll_database(self):
        logger.info("Database polling thread started")
        last_active_user_ids = set()
        poll_count = 0

        while not self._stop_event.is_set():
            try:
                poll_count += 1
                try:
                    active_connectors = self._get_active_connectors_from_db()
                except EmailConnectorDBError as e:
                    logger.warning(f"Database poll failed: {e} - retrying...")
                    time.sleep(1)
                    continue

                current_active_user_ids = {c['user_id'] for c in active_connectors}
                
                new_user_ids = current_active_user_ids - last_active_user_ids
                for connector_row in active_connectors:
                    if connector_row['user_id'] in new_user_ids:
                        self._start_connector(connector_row)

                removed_user_ids = last_active_user_ids - current_active_user_ids
                for user_id in removed_user_ids:
                    with self._lock:
                        if user_id in self.running_connectors:
                            connector_id = self.running_connectors[user_id].connector_id
                        else:
                            connector_id = None
                    if connector_id:
                        self._stop_connector(user_id, connector_id)

                for connector_row in active_connectors:
                    user_id = connector_row['user_id']
                    if user_id in new_user_ids:
                        continue
                    
                    with self._lock:
                        if user_id in self.running_connectors:
                            running_connector = self.running_connectors[user_id]
                        else:
                            continue

                    changes = running_connector.detect_credential_changes(connector_row)
                    if not changes:
                        continue

                    logger.info("=" * 80)
                    logger.info(f"Configuration changes detected for User {user_id}")
                    for column, (old_val, new_val) in changes.items():
                        if column == 'app_password':
                            logger.info(f"   {column}: *** -> ***")
                        else:
                            logger.info(f"   {column}: {old_val} -> {new_val}")

                    if running_connector.is_critical_change(changes):
                        logger.warning("CRITICAL CHANGE DETECTED - Restarting connector")
                        connector_id = running_connector.connector_id
                        self._stop_connector(user_id, connector_id)
                        self._start_connector(connector_row)
                    else:
                        if 'interval_minute' in changes:
                            old_int, new_int = changes['interval_minute']
                            success = running_connector.scheduler.update_scan_interval(new_int)
                            if success:
                                with self._lock:
                                    running_connector.scan_interval_minutes = new_int
                        if 'approved_senders' in changes:
                            with self._lock:
                                old_as, new_as = changes['approved_senders']
                                if new_as:
                                    approved_list = [s.strip() for s in new_as.split(',') if s.strip()]
                                else:
                                    approved_list = []
                                running_connector.scheduler.core.imap_config.approved_senders = approved_list

                        with self._lock:
                            running_connector.connector_row = connector_row
                            
                if poll_count % 30 == 0:
                    logger.info(f"Active Email connectors: {len(self.running_connectors)}")

                last_active_user_ids = current_active_user_ids
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error in database polling thread: {e}", exc_info=True)
                time.sleep(1)

        logger.info("Database polling thread stopped")

    def start(self):
        if self.is_running:
            return
        
        self.is_running = True
        self._stop_event.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_database,
            daemon=True,
            name="EmailConnectorManagerPollThread"
        )
        self._poll_thread.start()

    def stop(self):
        if not self.is_running:
            return
            
        self._stop_event.set()
        if self._poll_thread:
            self._poll_thread.join(timeout=5)
            
        user_ids = list(self.running_connectors.keys())
        for uid in user_ids:
            with self._lock:
                if uid in self.running_connectors:
                    cid = self.running_connectors[uid].connector_id
                else:
                    continue
            self._stop_connector(uid, cid)
            
        self.is_running = False

    def run_forever(self):
        import signal
        def signal_handler(signum, frame):
            self.stop()
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        self.start()
        
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def get_status(self) -> dict:
        with self._lock:
            connectors_info = [
                {
                    'connector_id': c.connector_id,
                    'user_id': c.user_id,
                    'email_account': c.email_account,
                    'imap_server': c.imap_server,
                    'started_at': c.started_at.isoformat(),
                    'uptime_seconds': (datetime.now() - c.started_at).total_seconds()
                }
                for c in self.running_connectors.values()
            ]
        return {
            'is_running': self.is_running,
            'total_active_connectors': len(self.running_connectors),
            'connectors': connectors_info
        }

class RunningEmailConnector:
    def __init__(self, connector_id: int, user_id: int, email_account: str,
                 imap_server: str, scheduler: 'PerConnectorEmailScheduler',
                 started_at: datetime, scan_interval_minutes: int,
                 connector_row: dict):
        self.connector_id = connector_id
        self.user_id = user_id
        self.email_account = email_account
        self.imap_server = imap_server
        self.scheduler = scheduler
        self.started_at = started_at
        self.scan_interval_minutes = scan_interval_minutes
        self.connector_row = connector_row or {}

    def detect_credential_changes(self, new_connector_row: dict) -> dict:
        changes = {}
        monitored_columns = [
            'email_id', 'app_password', 'approved_senders',
            'email_method', 'interval_minute'
        ]
        for col in monitored_columns:
            old_val = self.connector_row.get(col)
            new_val = new_connector_row.get(col)
            if old_val != new_val:
                changes[col] = (old_val, new_val)
        return changes

    def is_critical_change(self, changes: dict) -> bool:
        critical_fields = {'email_id', 'app_password', 'email_method'}
        return bool(changes.keys() & critical_fields)
