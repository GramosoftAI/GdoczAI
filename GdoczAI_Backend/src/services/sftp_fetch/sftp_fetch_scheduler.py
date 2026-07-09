# -*- coding: utf-8 -*-
"""
? Scheduler for SFTP ? OCR Document Pipeline
Responsibilities:
- Schedule folder scans every 5 minutes
- Schedule token refresh checks every hour
- Run background tasks using APScheduler
"""

import logging
import signal
import sys
from datetime import datetime
from typing import Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from src.services.sftp_fetch.sftp_fetch_config import PipelineConfig, SlimPipelineConfig, SFTPConfig
from src.services.sftp_fetch.sftp_fetch_core import PipelineCore

logger = logging.getLogger(__name__)

class PipelineScheduler:

    def __init__(self, config: PipelineConfig):

        self.config = config
        self.core = PipelineCore(config)
        self.scheduler: Optional[BackgroundScheduler] = None
        self.is_running = False
        
        logger.info("? PipelineScheduler initialized")
        logger.info(f"?? Folder scan interval: {config.scheduler.folder_scan_interval_minutes} minutes")
        logger.info(f"?? Token refresh check interval: {config.scheduler.token_refresh_check_interval_minutes} minutes")
    
    def _folder_scan_job(self):

        try:
            logger.info("=" * 80)
            logger.info(f"? SCHEDULED FOLDER SCAN TRIGGERED")
            logger.info(f"?? Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80)            
            success = self.core.run_once()
            
            if success:
                logger.info("? Folder scan cycle completed successfully")
            else:
                logger.error("? Folder scan cycle failed")
        
        except Exception as e:
            logger.error(f"? Error in folder scan job: {e}", exc_info=True)
    
    def _token_refresh_job(self):

        try:
            logger.info("=" * 80)
            logger.info(f"?? SCHEDULED TOKEN REFRESH CHECK")
            logger.info(f"?? Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80)            
            was_refreshed = self.core.auth_manager.check_and_refresh()
            
            if was_refreshed:
                logger.info("? Token was refreshed")
                self.core.stats.record_token_refresh()
            else:
                logger.info("?? Token refresh not needed yet")
            
            auth_status = self.core.get_auth_status()
            if auth_status.get('token_valid'):
                hours_remaining = auth_status.get('time_until_expiry_hours', 0)
                logger.info(f"?? Token status: Valid (expires in {hours_remaining:.2f} hours)")
            else:
                logger.warning("?? Token status: Invalid or missing")
        
        except Exception as e:
            logger.error(f"? Error in token refresh job: {e}", exc_info=True)
    
    def _job_listener(self, event):

        if event.exception:
            logger.error(f"? Job {event.job_id} failed with exception: {event.exception}")
        else:
            logger.debug(f"? Job {event.job_id} executed successfully")
    
    def start(self):

        if self.is_running:
            logger.warning("?? Scheduler is already running")
            return
        
        logger.info("=" * 80)
        logger.info("?? STARTING PIPELINE SCHEDULER")
        logger.info("=" * 80)
        
        try:
            if not self.core.initialize():
                logger.error("? Failed to initialize pipeline")
                return
            
            self.scheduler = BackgroundScheduler(
                timezone='UTC',
                job_defaults={
                    'coalesce': True,  # Combine missed jobs into one
                    'max_instances': 1  # Only one instance of each job at a time
                }
            )
            
            self.scheduler.add_listener(
                self._job_listener,
                EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
            )
            
            self.scheduler.add_job(
                func=self._folder_scan_job,
                trigger=IntervalTrigger(
                    minutes=self.config.scheduler.folder_scan_interval_minutes
                ),
                id='folder_scan_job',
                name='Folder Scan and PDF Processing',
                replace_existing=True
            )
            
            logger.info(f"?? Scheduled: Folder scan every {self.config.scheduler.folder_scan_interval_minutes} minutes")
            
            self.scheduler.add_job(
                func=self._token_refresh_job,
                trigger=IntervalTrigger(
                    minutes=self.config.scheduler.token_refresh_check_interval_minutes
                ),
                id='token_refresh_job',
                name='Token Refresh Check',
                replace_existing=True
            )
            
            logger.info(f"?? Scheduled: Token refresh check every {self.config.scheduler.token_refresh_check_interval_minutes} minutes")
            
            # Start scheduler
            self.scheduler.start()
            self.is_running = True
            
            logger.info("=" * 80)
            logger.info("? SCHEDULER STARTED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info("?? Active Jobs:")
            for job in self.scheduler.get_jobs():
                logger.info(f"   ?? {job.name} (ID: {job.id})")
                logger.info(f"      Next run: {job.next_run_time}")
            logger.info("=" * 80)
            
            # Run initial folder scan immediately
            logger.info("?? Running initial folder scan...")
            self._folder_scan_job()
        
        except Exception as e:
            logger.error(f"? Failed to start scheduler: {e}", exc_info=True)
            self.is_running = False
    
    def stop(self):

        if not self.is_running:
            logger.warning("?? Scheduler is not running")
            return
        
        logger.info("=" * 80)
        logger.info("?? STOPPING PIPELINE SCHEDULER")
        logger.info("=" * 80)
        
        try:
            if self.scheduler:
                logger.info("? Waiting for running jobs to complete...")
                self.scheduler.shutdown(wait=True)
                logger.info("? All jobs completed")
            
            self.is_running = False
            
            # Log final statistics
            stats = self.core.get_statistics()
            logger.info("=" * 80)
            logger.info("?? FINAL PIPELINE STATISTICS")
            logger.info("=" * 80)
            logger.info(f"?? Total scans: {stats['total_scans']}")
            logger.info(f"?? PDFs detected: {stats['total_pdfs_detected']}")
            logger.info(f"? PDFs processed: {stats['total_pdfs_processed']}")
            logger.info(f"? PDFs failed: {stats['total_pdfs_failed']}")
            logger.info(f"?? PDFs moved: {stats['total_pdfs_moved']}")
            logger.info(f"?? Success rate: {stats['success_rate_percent']:.1f}%")
            logger.info(f"?? Token refreshes: {stats['total_token_refreshes']}")
            logger.info(f"?? Uptime: {stats['uptime_hours']:.2f} hours")
            logger.info("=" * 80)
            logger.info("? SCHEDULER STOPPED SUCCESSFULLY")
            logger.info("=" * 80)
        
        except Exception as e:
            logger.error(f"? Error stopping scheduler: {e}", exc_info=True)
    
    def run_now(self):

        if not self.is_running:
            logger.warning("?? Scheduler is not running")
            return
        
        logger.info("?? Manual folder scan triggered")
        self._folder_scan_job()
    
    def get_status(self) -> dict:

        status = {
            'is_running': self.is_running,
            'scheduler_active': self.scheduler is not None and self.scheduler.running,
            'pipeline_stats': self.core.get_statistics(),
            'auth_status': self.core.get_auth_status()
        }
        
        if self.scheduler and self.is_running:
            jobs_info = []
            for job in self.scheduler.get_jobs():
                jobs_info.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run': job.next_run_time.isoformat() if job.next_run_time else None
                })
            status['scheduled_jobs'] = jobs_info
        
        return status
    
    def run_forever(self):

        def signal_handler(signum, frame):
            logger.info(f"?? Received signal {signum}, initiating graceful shutdown...")
            self.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start scheduler
        self.start()
        
        if not self.is_running:
            logger.error("? Failed to start scheduler")
            return
        
        logger.info("=" * 80)
        logger.info("?? PIPELINE RUNNING IN BACKGROUND")
        logger.info("=" * 80)
        logger.info("?? Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        try:
            # Keep main thread alive
            while self.is_running:
                # Check scheduler health every minute
                signal.pause() if hasattr(signal, 'pause') else __import__('time').sleep(60)
        
        except KeyboardInterrupt:
            logger.info("?? Keyboard interrupt received")
            self.stop()


# ============================================================================
# NEW: Per-Connector Scheduler for Multi-User Approach
# ============================================================================

class PerConnectorScheduler:

    def __init__(self, connector_id: int, user_id: int, sftp_config: SFTPConfig, 
                 slim_config: SlimPipelineConfig, scan_interval_minutes: int = 5):

        self.connector_id = connector_id
        self.user_id = user_id
        self.sftp_config = sftp_config
        self.slim_config = slim_config
        self.scan_interval_minutes = scan_interval_minutes
        
        # Create pipeline core for this connector
        self.core = PipelineCore(
            slim_config=slim_config,
            sftp_config=sftp_config,
            email_config=None,  # Email disabled per-connector for now
            connector_id=connector_id,
            user_id=user_id
        )
        
        self.scheduler: Optional[BackgroundScheduler] = None
        self.is_running = False
        
        logger.info("=" * 80)
        logger.info(f"?? [USER {user_id}] PerConnectorScheduler initialized")
        logger.info(f"?? Connector ID: {connector_id}")
        logger.info(f"?? SFTP: {sftp_config.host}:{sftp_config.port}")
        logger.info(f"?? Folders: {sftp_config.monitored_folders}")
        logger.info(f"?? Scan Interval: {scan_interval_minutes} minutes")
        logger.info("=" * 80)
    
    def _folder_scan_job(self):

        try:
            logger.info("-" * 80)
            logger.info(f"[USER {self.user_id}] Folder scan triggered")
            logger.info(f"[USER {self.user_id}] Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("-" * 80)
            
            # Run one pipeline cycle for this connector
            success = self.core.run_once()
            
            if success:
                logger.info(f"[USER {self.user_id}] Folder scan cycle completed")
            else:
                logger.error(f"[USER {self.user_id}] Folder scan cycle failed")
        
        except Exception as e:
            logger.error(f"[USER {self.user_id}] Error in folder scan job: {e}", exc_info=True)
    
    def _token_refresh_job(self):

        try:
            logger.info("-" * 80)
            logger.info(f"[USER {self.user_id}] Token refresh check triggered")
            logger.info(f"[USER {self.user_id}] Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("-" * 80)
            
            # Check and refresh token if needed
            was_refreshed = self.core.auth_manager.check_and_refresh()
            
            if was_refreshed:
                logger.info(f"[USER {self.user_id}] Token was refreshed")
                self.core.stats.record_token_refresh()
            else:
                logger.info(f"[USER {self.user_id}] Token refresh not needed")
            
            # Log token status
            auth_status = self.core.get_auth_status()
            if auth_status.get('token_valid'):
                hours_remaining = auth_status.get('time_until_expiry_hours', 0)
                logger.info(f"[USER {self.user_id}] Token: Valid (expires in {hours_remaining:.2f} hours)")
            else:
                logger.warning(f"[USER {self.user_id}] Token: Invalid")
        
        except Exception as e:
            logger.error(f"[USER {self.user_id}] Error in token refresh job: {e}", exc_info=True)
    
    def _job_listener(self, event):

        if event.exception:
            logger.error(f"[USER {self.user_id}] Job {event.job_id} failed: {event.exception}")
        else:
            logger.debug(f"[USER {self.user_id}] Job {event.job_id} executed")
    
    def update_scan_interval(self, new_minutes: int) -> bool:

        if not self.scheduler or not self.is_running:
            logger.warning(f"[USER {self.user_id}] Cannot update interval - scheduler not running")
            return False
        
        try:
            job_id = f'folder_scan_job_user_{self.user_id}'
            job = self.scheduler.get_job(job_id)
            
            if not job:
                logger.error(f"[USER {self.user_id}] Folder scan job not found")
                return False
            
            # Reschedule job with new interval
            self.scheduler.reschedule_job(
                job_id=job_id,
                trigger=IntervalTrigger(minutes=new_minutes)
            )
            
            # Update stored interval
            self.scan_interval_minutes = new_minutes
            
            logger.info(f"[USER {self.user_id}] Folder scan interval updated: {new_minutes} minutes")
            return True
        
        except Exception as e:
            logger.error(f"[USER {self.user_id}] Failed to update scan interval: {e}", exc_info=True)
            return False
    
    def start(self):

        if self.is_running:
            logger.warning(f"[USER {self.user_id}] Connector scheduler already running")
            return
        
        logger.info("=" * 80)
        logger.info(f"[USER {self.user_id}] STARTING CONNECTOR SCHEDULER")
        logger.info("=" * 80)
        
        try:
            # Initialize pipeline core (obtain initial token)
            if not self.core.initialize():
                logger.error(f"[USER {self.user_id}] Failed to initialize pipeline")
                return
            
            # Create scheduler
            self.scheduler = BackgroundScheduler(
                timezone='UTC',
                job_defaults={
                    'coalesce': True,
                    'max_instances': 1
                }
            )
            self.scheduler.add_listener(
                self._job_listener,
                EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
            )

            self.scheduler.add_job(
                func=self._folder_scan_job,
                trigger=IntervalTrigger(minutes=self.scan_interval_minutes),
                id=f'folder_scan_job_user_{self.user_id}',
                name=f'[USER {self.user_id}] Folder Scan',
                replace_existing=True
            )
            
            logger.info(f"[USER {self.user_id}] Scheduled: Folder scan every {self.scan_interval_minutes} minutes")
            
            # Schedule token refresh job (global interval)
            self.scheduler.add_job(
                func=self._token_refresh_job,
                trigger=IntervalTrigger(
                    minutes=self.slim_config.scheduler.token_refresh_check_interval_minutes
                ),
                id=f'token_refresh_job_user_{self.user_id}',
                name=f'[USER {self.user_id}] Token Refresh Check',
                replace_existing=True
            )
            
            logger.info(f"[USER {self.user_id}] Scheduled: Token refresh every {self.slim_config.scheduler.token_refresh_check_interval_minutes} minutes")
            
            # Start scheduler
            self.scheduler.start()
            self.is_running = True
            
            logger.info(f"[USER {self.user_id}] Connector scheduler STARTED")
            logger.info("=" * 80)
            
            # Run initial folder scan immediately
            logger.info(f"[USER {self.user_id}] Running initial folder scan...")
            self._folder_scan_job()
        
        except Exception as e:
            logger.error(f"[USER {self.user_id}] Failed to start scheduler: {e}", exc_info=True)
            self.is_running = False
    
    def stop(self):

        if not self.is_running:
            logger.debug(f"[USER {self.user_id}] Connector scheduler not running")
            return
        
        logger.info("=" * 80)
        logger.info(f"[USER {self.user_id}] STOPPING CONNECTOR SCHEDULER")
        logger.info("=" * 80)
        
        try:
            if self.scheduler:
                logger.info(f"[USER {self.user_id}] Waiting for running jobs to complete...")
                self.scheduler.shutdown(wait=True)
                logger.info(f"[USER {self.user_id}] All jobs completed")
            
            self.is_running = False
            
            # Log final statistics
            stats = self.core.get_statistics()
            logger.info("=" * 80)
            logger.info(f"[USER {self.user_id}] FINAL STATISTICS")
            logger.info("=" * 80)
            logger.info(f"[USER {self.user_id}] PDFs processed: {stats['total_pdfs_processed']}")
            logger.info(f"[USER {self.user_id}] PDFs failed: {stats['total_pdfs_failed']}")
            logger.info(f"[USER {self.user_id}] Success rate: {stats['success_rate_percent']:.1f}%")
            logger.info("=" * 80)
            logger.info(f"[USER {self.user_id}] Connector scheduler STOPPED")
            logger.info("=" * 80)
        
        except Exception as e:
            logger.error(f"[USER {self.user_id}] Error stopping scheduler: {e}", exc_info=True)
    
    def get_status(self) -> dict:

        return {
            'connector_id': self.connector_id,
            'user_id': self.user_id,
            'is_running': self.is_running,
            'sftp_host': self.sftp_config.host,
            'scan_interval_minutes': self.scan_interval_minutes,
            'pipeline_stats': self.core.get_statistics() if self.is_running else {}
        }


def main():
    """
    Main entry point for running the scheduler as a standalone service
    """
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger.info("=" * 80)
    logger.info("?? DOCUMENT PIPELINE SCHEDULER")
    logger.info("=" * 80)
    
    try:
        # Load configuration
        from src.services.sftp_fetch.sftp_fetch_config import get_config
        config = get_config()
        
        # Create and run scheduler
        scheduler = PipelineScheduler(config)
        scheduler.run_forever()
    
    except FileNotFoundError as e:
        logger.error(f"? Configuration file not found: {e}")
        sys.exit(1)
    
    except ValueError as e:
        logger.error(f"? Invalid configuration: {e}")
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"? Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()