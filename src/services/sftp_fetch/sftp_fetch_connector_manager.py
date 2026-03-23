# -*- coding: utf-8 -*-

"""
Database Table: sftp_connector
Columns:
  - id: Unique identifier
  - user_id: User identifier
  - host_name: SFTP host
  - port: SFTP port
  - username: SFTP username
  - password: SFTP password
  - private_key_path: Optional SSH key path
  - monitor_folders: SFTP folders to monitor (JSON list)
  - moved_folder: Destination for processed files
  - failed_folder: Destination for failed files
  - interval_minute: Pipeline scan interval for this connector
  - is_active: Boolean flag to enable/disable
"""

import logging
import threading
import time
from typing import Dict, Optional, List
from datetime import datetime

from src.services.sftp_fetch.sftp_fetch_config import get_slim_config, SFTPConfig
from src.services.sftp_fetch.sftp_fetch_scheduler import PerConnectorScheduler
from src.services.sftp_fetch.sftp_fetch_database import init_db_connection, query_active_sftp_connectors

logger = logging.getLogger(__name__)


class SFTPConnectorDBError(Exception):
    pass

class ConnectorManager:
    
    def __init__(self, config_path: str = "config/config.yaml"):

        self.config_path = config_path
        self.slim_config = get_slim_config(config_path)
        
        import yaml
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                full_config = yaml.safe_load(f)
                pg_config = full_config.get('postgres', {})
        except Exception as e:
            logger.error(f"? Failed to load postgres config: {e}")
            raise
        
        try:
            init_db_connection(pg_config)
            logger.info("? Database connection pool initialized")
        except Exception as e:
            logger.error(f"? Database initialization failed: {e}")
            raise
        
        self.running_connectors: Dict[int, 'RunningConnector'] = {}
        self._lock = threading.Lock()
        
        self.is_running = False
        self._stop_event = threading.Event()
        
        self._poll_thread: Optional[threading.Thread] = None
        
        logger.info("=" * 80)
        logger.info("?? CONNECTOR MANAGER INITIALIZED")
        logger.info("=" * 80)
        logger.info(f"?? Configuration: {config_path}")
        logger.info(f"?? Auth Endpoint: {self.slim_config.auth.signin_url}")
        logger.info(f"?? OCR Endpoint: {self.slim_config.ocr.endpoint_url}")
        logger.info(f"?? Token Refresh Interval: {self.slim_config.auth.token_refresh_interval_hours} hours")
        logger.info(f"?? Token Refresh Check Interval: {self.slim_config.scheduler.token_refresh_check_interval_minutes} minutes")
        logger.info("=" * 80)
        logger.info("?? Connector Manager will poll database every 1 second")
        logger.info("?? Each active connector runs independently with its own scheduler")
        logger.info("=" * 80)
    
    def _get_active_connectors_from_db(self) -> List[dict]:

        try:
            logger.debug("? Querying database for active SFTP connectors...")
            connectors = query_active_sftp_connectors()
            logger.debug(f"? Found {len(connectors)} active connectors in database")
            return connectors
        
        except Exception as e:
            logger.error(f"? Database error while querying connectors: {e}", exc_info=True)
            raise SFTPConnectorDBError(f"Failed to query connectors: {e}")
    
    def _create_sftp_config_from_row(self, connector_row: dict) -> SFTPConfig:

        import json
        
        # Parse monitored folders (stored as comma-separated text in database)
        monitor_folders_str = connector_row.get('monitor_folders')
        monitored_folders = []
        
        if monitor_folders_str:
            try:
                if isinstance(monitor_folders_str, str):
                    try:
                        monitored_folders = json.loads(monitor_folders_str)
                    except (json.JSONDecodeError, ValueError):
                        monitored_folders = [
                            folder.strip() 
                            for folder in monitor_folders_str.split(',') 
                            if folder.strip()
                        ]
                else:
                    monitored_folders = monitor_folders_str if monitor_folders_str else []
            except Exception as e:
                logger.warning(f"? Failed to parse monitor_folders for connector {connector_row.get('id')}: {e}")
                monitored_folders = []
        
        if not monitored_folders:
            logger.info(f"? No monitored folders configured for connector {connector_row.get('id')}, using default ['/']")
            monitored_folders = ['/']
        
        sftp_config = SFTPConfig(
            host=connector_row['host_name'],
            port=connector_row['port'],
            username=connector_row['username'],
            password=connector_row['password'],
            monitored_folders=monitored_folders,
            moved_folder=connector_row['moved_folder'],
            failed_folder=connector_row['failed_folder'],
            private_key_path=connector_row.get('private_key_path'),
            key_password=None
        )
        
        return sftp_config
    
    def _start_connector(self, connector_row: dict):

        user_id = connector_row['user_id']
        connector_id = connector_row['id']
        
        try:
            logger.info("=" * 80)
            logger.info(f"?? STARTING NEW CONNECTOR")
            logger.info(f"?? Connector ID: {connector_id}")
            logger.info(f"?? User ID: {user_id}")
            logger.info(f"?? SFTP Host: {connector_row['host_name']}:{connector_row['port']}")
            logger.info(f"?? Scan Interval: {connector_row['interval_minute']} minutes")
            logger.info("=" * 80)
            
            sftp_config = self._create_sftp_config_from_row(connector_row)
            
            scheduler = PerConnectorScheduler(
                connector_id=connector_id,
                user_id=user_id,
                sftp_config=sftp_config,
                slim_config=self.slim_config,
                scan_interval_minutes=connector_row['interval_minute']
            )
            
            scheduler.start()
            
            # Store in running connectors registry
            running_connector = RunningConnector(
                connector_id=connector_id,
                user_id=user_id,
                sftp_host=connector_row['host_name'],
                scheduler=scheduler,
                started_at=datetime.now(),
                scan_interval_minutes=connector_row['interval_minute'],
                connector_row=connector_row
            )
            
            with self._lock:
                self.running_connectors[user_id] = running_connector
            
            logger.info(f"? Connector {connector_id} for user {user_id} STARTED")
            logger.info(f"?? Total running connectors: {len(self.running_connectors)}")
        
        except Exception as e:
            logger.error(f"?? Failed to start connector {connector_id}: {e}", exc_info=True)
    
    def _stop_connector(self, user_id: int, connector_id: int):

        try:
            logger.info("=" * 80)
            logger.info(f"? STOPPING CONNECTOR")
            logger.info(f"?? Connector ID: {connector_id}")
            logger.info(f"?? User ID: {user_id}")
            logger.info("=" * 80)
            
            with self._lock:
                if user_id in self.running_connectors:
                    running_connector = self.running_connectors[user_id]
                    
                    # Stop the scheduler
                    running_connector.scheduler.stop()
                    
                    # Remove from registry
                    del self.running_connectors[user_id]
                    
                    logger.info(f"? Connector {connector_id} for user {user_id} STOPPED")
                    logger.info(f"?? Remaining running connectors: {len(self.running_connectors)}")
                else:
                    logger.warning(f"?? Connector for user {user_id} was not running")
        
        except Exception as e:
            logger.error(f"?? Error stopping connector {connector_id}: {e}", exc_info=True)
    
    def _poll_database(self):

        logger.info("?? Database polling thread started")
        
        last_active_user_ids = set()
        poll_count = 0
        
        while not self._stop_event.is_set():
            try:
                poll_count += 1
                
                # Query database for active connectors
                try:
                    active_connectors = self._get_active_connectors_from_db()
                except SFTPConnectorDBError as e:
                    logger.warning(f"?? Database poll failed: {e} - will retry in 1 second")
                    time.sleep(1)
                    continue
                
                current_active_user_ids = {c['user_id'] for c in active_connectors}
                
                # Detect new connectors (activated)
                new_user_ids = current_active_user_ids - last_active_user_ids
                for connector_row in active_connectors:
                    if connector_row['user_id'] in new_user_ids:
                        self._start_connector(connector_row)
                
                # Detect removed connectors (deactivated)
                removed_user_ids = last_active_user_ids - current_active_user_ids
                for user_id in removed_user_ids:
                    # Find the connector_id from our running registry
                    with self._lock:
                        if user_id in self.running_connectors:
                            connector_id = self.running_connectors[user_id].connector_id
                        else:
                            connector_id = None
                    
                    if connector_id:
                        self._stop_connector(user_id, connector_id)
                
                # ?? NEW: Detect credential changes for existing running connectors
                for connector_row in active_connectors:
                    user_id = connector_row['user_id']
                    
                    # Skip if this is a newly started connector
                    if user_id in new_user_ids:
                        continue
                    
                    # Check if this connector is currently running
                    with self._lock:
                        if user_id in self.running_connectors:
                            running_connector = self.running_connectors[user_id]
                        else:
                            continue
                    
                    # Detect ALL credential changes
                    changes = running_connector.detect_credential_changes(connector_row)
                    
                    if not changes:
                        continue  # No changes detected
                    
                    # Log detected changes
                    logger.info("=" * 80)
                    logger.info(f"?? [DYNAMIC UPDATE] Configuration changes detected for User {user_id}")
                    logger.info(f"?? Connector ID: {running_connector.connector_id}")
                    logger.info("=" * 80)
                    
                    for column, (old_value, new_value) in changes.items():
                        # Mask sensitive values in logs
                        if column in ['password']:
                            logger.info(f"   {column}: *** ? ***")
                        else:
                            logger.info(f"   {column}: {old_value} ? {new_value}")
                    
                    # Check if critical credentials changed (requires restart)
                    if running_connector.is_critical_change(changes):
                        logger.warning("?? CRITICAL CHANGE DETECTED - Restarting connector")
                        logger.info("=" * 80)
                        
                        # Stop the old connector
                        connector_id = running_connector.connector_id
                        self._stop_connector(user_id, connector_id)
                        
                        # Start new connector with updated credentials
                        logger.info(f"?? Starting connector with updated configuration...")
                        self._start_connector(connector_row)
                        
                        logger.info(f"? Connector restarted with new credentials")
                        logger.info("=" * 80)
                    else:
                        # Non-critical changes (folders, interval) - update dynamically
                        logger.info("?? Non-critical changes - updating running configuration")
                        
                        # Update interval if changed
                        if 'interval_minute' in changes:
                            old_interval, new_interval = changes['interval_minute']
                            logger.info(f"   Updating scan interval: {old_interval} ? {new_interval} minutes")
                            success = running_connector.scheduler.update_scan_interval(new_interval)
                            if success:
                                with self._lock:
                                    running_connector.scan_interval_minutes = new_interval
                        
                        # Update folder paths in SFTP config if changed
                        folder_changes = {
                            k: v for k, v in changes.items() 
                            if k in ['monitor_folders', 'moved_folder', 'failed_folder']
                        }
                        
                        if folder_changes:
                            logger.info(f"   Updating folder configuration...")
                            # Update the core's SFTP config
                            if 'monitor_folders' in folder_changes:
                                old_f, new_f = folder_changes['monitor_folders']
                                logger.info(f"     monitor_folders: {old_f} ? {new_f}")
                                # Parse new folders
                                import json
                                try:
                                    if isinstance(new_f, str):
                                        try:
                                            new_folders = json.loads(new_f)
                                        except (json.JSONDecodeError, ValueError):
                                            new_folders = [f.strip() for f in new_f.split(',') if f.strip()]
                                    else:
                                        new_folders = new_f if new_f else []
                                except:
                                    new_folders = []
                                
                                if new_folders:
                                    running_connector.scheduler.core.sftp_config.monitored_folders = new_folders
                            
                            if 'moved_folder' in folder_changes:
                                old_f, new_f = folder_changes['moved_folder']
                                logger.info(f"     moved_folder: {old_f} ? {new_f}")
                                running_connector.scheduler.core.sftp_config.moved_folder = new_f
                            
                            if 'failed_folder' in folder_changes:
                                old_f, new_f = folder_changes['failed_folder']
                                logger.info(f"     failed_folder: {old_f} ? {new_f}")
                                running_connector.scheduler.core.sftp_config.failed_folder = new_f
                        
                        # Update stored connector row with latest values
                        with self._lock:
                            running_connector.connector_row = connector_row
                        
                        logger.info("? Dynamic configuration updated successfully")
                        logger.info("=" * 80)
                
                # Log status every 30 polls (30 seconds)
                if poll_count % 30 == 0:
                    logger.info(f"?? [Poll #{poll_count}] Active connectors: {len(self.running_connectors)}")
                    if self.running_connectors:
                        for user_id, connector in self.running_connectors.items():
                            logger.info(f"   User {user_id}: {connector.sftp_host} (running since {connector.started_at})")
                
                # Update for next iteration
                last_active_user_ids = current_active_user_ids
                
                # Sleep before next poll
                time.sleep(1)
            
            except Exception as e:
                logger.error(f"?? Error in database polling thread: {e}", exc_info=True)
                time.sleep(1)
        
        logger.info("?? Database polling thread stopped")
    
    def start(self):

        if self.is_running:
            logger.warning("?? Connector manager is already running")
            return
        
        logger.info("=" * 80)
        logger.info("?? STARTING CONNECTOR MANAGER")
        logger.info("=" * 80)
        
        self.is_running = True
        self._stop_event.clear()
        
        # Start database polling thread (daemon thread)
        self._poll_thread = threading.Thread(
            target=self._poll_database,
            daemon=True,
            name="ConnectorManagerPollThread"
        )
        self._poll_thread.start()
        
        logger.info("? Connector manager started with database polling thread")
        logger.info("=" * 80)
    
    def stop(self):

        if not self.is_running:
            logger.warning("?? Connector manager is not running")
            return
        
        logger.info("=" * 80)
        logger.info("? STOPPING CONNECTOR MANAGER")
        logger.info("=" * 80)
        
        # Signal polling thread to stop
        self._stop_event.set()
        
        # Wait for polling thread to finish
        if self._poll_thread:
            self._poll_thread.join(timeout=5)
            if self._poll_thread.is_alive():
                logger.warning("?? Polling thread did not stop within timeout")
        
        # Stop all running connectors
        user_ids_to_stop = list(self.running_connectors.keys())
        for user_id in user_ids_to_stop:
            with self._lock:
                if user_id in self.running_connectors:
                    connector_id = self.running_connectors[user_id].connector_id
                else:
                    continue
            
            self._stop_connector(user_id, connector_id)
        
        self.is_running = False
        logger.info("? Connector manager stopped")
        logger.info("=" * 80)
    
    def run_forever(self):

        import signal
        
        def signal_handler(signum, frame):
            logger.info("?? Shutdown signal received")
            self.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start manager
        self.start()
        
        logger.info("=" * 80)
        logger.info("?? CONNECTOR MANAGER RUNNING")
        logger.info("=" * 80)
        logger.info("?? Database polling every 1 second")
        logger.info("?? Each active connector runs independently")
        logger.info("?? Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        # Block until interrupted
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("?? Keyboard interrupt received")
            self.stop()
    
    def get_status(self) -> dict:

        with self._lock:
            connectors_info = [
                {
                    'connector_id': c.connector_id,
                    'user_id': c.user_id,
                    'sftp_host': c.sftp_host,
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


class RunningConnector:

    def __init__(self, connector_id: int, user_id: int, sftp_host: str, 
                 scheduler: 'PerConnectorScheduler', started_at: datetime,
                 scan_interval_minutes: int = 5, connector_row: dict = None):

        self.connector_id = connector_id
        self.user_id = user_id
        self.sftp_host = sftp_host
        self.scheduler = scheduler
        self.started_at = started_at
        self.scan_interval_minutes = scan_interval_minutes
        self.connector_row = connector_row or {}
    
    def detect_credential_changes(self, new_connector_row: dict) -> dict:

        changes = {}
        
        monitored_columns = [
            'host_name', 'port', 'username', 'password', 'private_key_path',
            'monitor_folders', 'moved_folder', 'failed_folder', 'interval_minute'
        ]
        
        for column in monitored_columns:
            old_value = self.connector_row.get(column)
            new_value = new_connector_row.get(column)
            
            if old_value != new_value:
                changes[column] = (old_value, new_value)
        
        return changes
    
    def is_critical_change(self, changes: dict) -> bool:

        critical_fields = {'host_name', 'port', 'username', 'password', 'private_key_path'}
        return bool(changes.keys() & critical_fields)
