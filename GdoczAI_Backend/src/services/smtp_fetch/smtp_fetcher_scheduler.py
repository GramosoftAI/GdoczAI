# -*- coding: utf-8 -*-

"""
Scheduler for Gmail to OCR smtp fetcher.
Manages background jobs for inbox scanning and token refresh using APScheduler.
Handles graceful shutdown and orphaned file cleanup.
"""

import logging
import signal
import sys
import time
from datetime import datetime
from typing import Optional

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.services.smtp_fetch.smtp_fetcher_config import IMAPConnectorConfig
from src.services.sftp_fetch.sftp_fetch_config import SlimPipelineConfig, get_slim_config
from src.services.smtp_fetch.smtp_fetcher_core import EmailFetcherCore
from src.services.smtp_fetch.smtp_fetcher_utils import (
    cleanup_orphaned_downloads,
    format_duration,
)

logger = logging.getLogger(__name__)


class EmailFetcherScheduler:

    def __init__(self, imap_config: IMAPConnectorConfig, slim_config: SlimPipelineConfig):

        self.imap_config = imap_config
        self.slim_config = slim_config
        self.core = EmailFetcherCore(imap_config, slim_config)
        self.scheduler: Optional[BackgroundScheduler] = None
        self.is_running: bool = False
        self._start_time: Optional[datetime] = None

        logger.info("EmailFetcherScheduler initialised")
        logger.info(
            f"Inbox scan interval    : "
            f"{5} minute(s)"
        )
        logger.info(
            f"Token refresh interval : "
            f"{slim_config.scheduler.token_refresh_check_interval_minutes} minute(s)"
        )
        logger.info(f"Download dir           : {imap_config.download_dir}")
        logger.info(f"Processed dir          : {imap_config.processed_dir}")
        logger.info(f"Failed dir             : {imap_config.failed_dir}")

    def _inbox_scan_job(self):

        try:
            logger.info("=" * 80)
            logger.info("SCHEDULED INBOX SCAN TRIGGERED")
            logger.info(
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            logger.info("=" * 80)

            success = self.core.run_once()

            if success:
                logger.info("Inbox scan cycle completed successfully")
            else:
                logger.error("Inbox scan cycle failed")

        except Exception as e:
            logger.error(f" Error in inbox scan job: {e}", exc_info=True)

    def _token_refresh_job(self):

        try:
            logger.info("=" * 80)
            logger.info("SCHEDULED TOKEN REFRESH CHECK")
            logger.info(
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            logger.info("=" * 80)

            was_refreshed = self.core.auth_manager.check_and_refresh()

            if was_refreshed:
                logger.info("JWT token was refreshed successfully")
                self.core.stats.record_token_refresh()
            else:
                logger.info("Token refresh not needed yet")

            # Log current token status
            auth_status = self.core.get_auth_status()
            if auth_status.get("token_valid"):
                hours_remaining = auth_status.get("time_until_expiry_hours", 0)
                logger.info(
                    f"Token status: Valid "
                    f"(expires in {hours_remaining:.2f} hours)"
                )
            else:
                logger.warning("Token status: Invalid or missing")

        except Exception as e:
            logger.error(f" Error in token refresh job: {e}", exc_info=True)
    # ----------------------------------------------------------------
    # APScheduler event listener  (identical to SFTP scheduler)
    # ----------------------------------------------------------------
    def _job_listener(self, event):

        if event.exception:
            logger.error(
                f"Job '{event.job_id}' raised an exception: "
                f"{event.exception}"
            )
        else:
            logger.debug(f"Job '{event.job_id}' executed successfully")

    def _run_startup_orphan_cleanup(self):

        try:
            logger.info("=" * 80)
            logger.info("STARTUP ORPHAN CLEANUP")
            logger.info("=" * 80)
            logger.info(
                f"   Scanning  : {self.imap_config.download_dir}"
            )
            logger.info(
                f"   Moving to : {self.imap_config.failed_dir}"
            )
            logger.info("   Threshold : 24 hours")

            moved, errors = cleanup_orphaned_downloads(
                download_dir=self.imap_config.download_dir,
                failed_dir=self.imap_config.failed_dir,
                older_than_hours=24.0,
            )

            if moved == 0 and errors == 0:
                logger.info("No orphaned downloads found - download_dir is clean")
            else:
                logger.info(
                    f"Orphan cleanup: {moved} file(s) moved, "
                    f"{errors} error(s)"
                )
                if errors:
                    logger.warning(
                        f"{errors} orphan(s) could not be moved - "
                        f"check download_dir permissions"
                    )

            logger.info("=" * 80)

        except Exception as e:
            logger.error(
                f" Orphan cleanup failed (non-fatal): {e}", exc_info=True
            )

    def start(self):

        if self.is_running:
            logger.warning("Scheduler is already running")
            return

        logger.info("=" * 80)
        logger.info("STARTING EMAIL FETCHER SCHEDULER")
        logger.info("=" * 80)

        try:
            # Step 1: Initialise core (get initial JWT token)
            logger.info("Initialising email fetcher core...")
            if not self.core.initialize():
                logger.error("Failed to initialise email fetcher core")
                logger.error(
                    "   Check that the OCR authentication endpoint is reachable "
                    "and credentials are correct."
                )
                return

            # -- Step 2: Orphan cleanup (email-specific startup action) --
            self._run_startup_orphan_cleanup()

            # Step 3: Build APScheduler
            self.scheduler = BackgroundScheduler(
                timezone="UTC",
                job_defaults={
                    "coalesce": True,    # merge missed ticks into one run
                    "max_instances": 1,  # never run two instances of the same job
                },
            )

            # Register event listener
            self.scheduler.add_listener(
                self._job_listener,
                EVENT_JOB_EXECUTED | EVENT_JOB_ERROR,
            )

            # Step 4: Register inbox scan job
            scan_interval = 5
            self.scheduler.add_job(
                func=self._inbox_scan_job,
                trigger=IntervalTrigger(minutes=scan_interval),
                id="inbox_scan_job",
                name="Gmail Inbox Scan and PDF Processing",
                replace_existing=True,
            )
            logger.info(
                f"Scheduled: Inbox scan every {scan_interval} minute(s)"
            )

            # Step 5: Register token refresh job
            refresh_interval = (
                self.slim_config.scheduler.token_refresh_check_interval_minutes
            )
            self.scheduler.add_job(
                func=self._token_refresh_job,
                trigger=IntervalTrigger(minutes=refresh_interval),
                id="token_refresh_job",
                name="JWT Token Refresh Check",
                replace_existing=True,
            )
            logger.info(
                f"Scheduled: Token refresh check every {refresh_interval} minute(s)"
            )

            # Step 6: Start APScheduler
            self.scheduler.start()
            self.is_running = True
            self._start_time = datetime.now()

            logger.info("=" * 80)
            logger.info("EMAIL FETCHER SCHEDULER STARTED")
            logger.info("=" * 80)
            logger.info("Active Jobs:")
            for job in self.scheduler.get_jobs():
                logger.info(f"   {job.name}  (id: {job.id})")
                logger.info(f"      Next run: {job.next_run_time}")
            logger.info("=" * 80)

            # Step 7: Run first inbox scan immediately
            logger.info("Running initial inbox scan...")
            self._inbox_scan_job()

        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}", exc_info=True)
            self.is_running = False

    def stop(self):

        if not self.is_running:
            logger.warning("  Scheduler is not running")
            return

        logger.info("=" * 80)
        logger.info(" STOPPING EMAIL FETCHER SCHEDULER")
        logger.info("=" * 80)

        try:
            if self.scheduler:
                logger.info(" Waiting for running jobs to complete...")
                self.scheduler.shutdown(wait=True)
                logger.info(" All jobs completed")

            self.is_running = False

            # -- Log final statistics --
            stats = self.core.get_statistics()

            logger.info("=" * 80)
            logger.info(" FINAL EMAIL FETCHER STATISTICS")
            logger.info("=" * 80)

            # Inbox / email counts
            logger.info(
                f" Inbox checks          : "
                f"{stats.get('total_inbox_checks', 0)}"
            )
            logger.info(
                f"  Emails checked        : "
                f"{stats.get('total_emails_checked', 0)}"
            )

            # Attachment counts
            logger.info(
                f" Attachments found     : "
                f"{stats.get('total_attachments_found', 0)}"
            )
            logger.info(
                f" Attachments downloaded: "
                f"{stats.get('total_attachments_downloaded', 0)}"
            )
            logger.info(
                f" Attachments processed : "
                f"{stats.get('total_attachments_processed', 0)}"
            )
            logger.info(
                f" Attachments failed    : "
                f"{stats.get('total_attachments_failed', 0)}"
            )
            logger.info(
                f" Moved to processed   : "
                f"{stats.get('total_attachments_moved', 0)}"
            )
            logger.info(
                f" Moved to failed      : "
                f"{stats.get('total_attachments_moved_to_failed', 0)}"
            )

            # Dedup / rejection counts
            logger.info(
                f"  Duplicates skipped    : "
                f"{stats.get('total_attachments_duplicate', 0)}"
            )
            logger.info(
                f" Sender rejections     : "
                f"{stats.get('total_sender_rejections', 0)}"
            )

            # Success rate
            success_rate = stats.get("success_rate_percent", 0.0)
            logger.info(
                f" Success rate          : {success_rate:.1f}%"
            )

            # Auth / error counts
            logger.info(
                f" Token refreshes       : "
                f"{stats.get('total_token_refreshes', 0)}"
            )
            logger.info(
                f" IMAP errors           : "
                f"{stats.get('total_imap_errors', 0)}"
            )

            # Uptime
            if self._start_time:
                uptime_seconds = (
                    datetime.now() - self._start_time
                ).total_seconds()
                logger.info(
                    f"  Uptime                : "
                    f"{format_duration(uptime_seconds)}"
                )

            logger.info("=" * 80)
            logger.info(" EMAIL FETCHER SCHEDULER STOPPED SUCCESSFULLY")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f" Error stopping scheduler: {e}", exc_info=True)

    def run_now(self):

        if not self.is_running:
            logger.warning("Scheduler is not running - cannot trigger scan")
            return

        logger.info("Manual inbox scan triggered")
        self._inbox_scan_job()

    def get_status(self) -> dict:

        status = {
            "is_running": self.is_running,
            "scheduler_active": (
                self.scheduler is not None and self.scheduler.running
            ),
            "is_currently_processing": (
                EmailFetcherCore.is_currently_processing()
            ),
            "pipeline_stats": self.core.get_statistics(),
            "auth_status": self.core.get_auth_status(),
            "alert_status": self.core.get_alert_status(),
        }

        if self._start_time:
            status["uptime_seconds"] = (
                datetime.now() - self._start_time
            ).total_seconds()

        if self.scheduler and self.is_running:
            jobs_info = []
            for job in self.scheduler.get_jobs():
                jobs_info.append(
                    {
                        "id": job.id,
                        "name": job.name,
                        "next_run": (
                            job.next_run_time.isoformat()
                            if job.next_run_time
                            else None
                        ),
                    }
                )
            status["scheduled_jobs"] = jobs_info

        return status

    def run_forever(self):

        def _handle_signal(signum, frame):
            sig_name = (
                "SIGINT" if signum == signal.SIGINT else "SIGTERM"
            )
            logger.info(
                f" Received {sig_name}  initiating graceful shutdown..."
            )
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        self.start()

        if not self.is_running:
            logger.error(
                " Scheduler failed to start  check configuration and "
                "authentication credentials"
            )
            return

        logger.info("=" * 80)
        logger.info(" EMAIL FETCHER RUNNING IN BACKGROUND")
        logger.info("=" * 80)
        logger.info(
            f" Scanning inbox every "
            f"{5} minute(s)"
        )
        logger.info(
            f" Refreshing token every "
            f"{self.slim_config.scheduler.token_refresh_check_interval_minutes} minute(s)"
        )
        logger.info("  Press Ctrl+C to stop")
        logger.info("=" * 80)

        try:
            while self.is_running:
                if hasattr(signal, "pause"):
                    signal.pause()   # POSIX: blocks until any signal received
                else:
                    time.sleep(60)   # Windows fallback

        except KeyboardInterrupt:
            logger.info("  KeyboardInterrupt received")
            self.stop()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=" * 80)
    logger.info(" EMAIL FETCHER SCHEDULER  STANDALONE MODE")
    logger.info("=" * 80)

    try:
        slim_config = get_slim_config()
        imap_config = IMAPConnectorConfig(email_id="dummy", app_password="dummy", imap_server="dummy", approved_senders=["dummy"])
        scheduler = EmailFetcherScheduler(imap_config, slim_config)
        scheduler.run_forever()

    except FileNotFoundError as e:
        logger.error(f" Configuration file not found: {e}")
        sys.exit(1)

    except ValueError as e:
        logger.error(f" Invalid configuration: {e}")
        sys.exit(1)

    except Exception as e:
        logger.error(f" Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

# ============================================================================
# NEW: Per-Connector Scheduler for Multi-User Approach
# ============================================================================

from src.services.smtp_fetch.smtp_fetcher_config import IMAPConnectorConfig
from src.services.sftp_fetch.sftp_fetch_config import SlimPipelineConfig

class PerConnectorEmailScheduler:

    def __init__(
        self,
        connector_id: int,
        user_id: int,
        imap_config: IMAPConnectorConfig,
        slim_config: SlimPipelineConfig,
        scan_interval_minutes: int = 5,
    ):
        self.connector_id = connector_id
        self.user_id = user_id
        self.imap_config = imap_config
        self.slim_config = slim_config
        self.scan_interval_minutes = scan_interval_minutes
        self.core = EmailFetcherCore(imap_config, slim_config)
        self.scheduler = None
        self.is_running = False

    def _inbox_scan_job(self):
        try:
            logger.info("=" * 80)
            logger.info(f"[USER {self.user_id}] SCHEDULED INBOX SCAN TRIGGERED")
            logger.info(f"[USER {self.user_id}] Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80)
            success = self.core.run_once()
            if success:
                logger.info(f"[USER {self.user_id}] Inbox scan cycle completed successfully")
            else:
                logger.error(f"[USER {self.user_id}] Inbox scan cycle failed")
        except Exception as e:
            logger.error(f"[USER {self.user_id}] Error in inbox scan job: {e}", exc_info=True)

    def _token_refresh_job(self):
        try:
            logger.info("=" * 80)
            logger.info(f"[USER {self.user_id}] SCHEDULED TOKEN REFRESH CHECK")
            logger.info(f"[USER {self.user_id}] Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80)
            was_refreshed = self.core.auth_manager.check_and_refresh()
            if was_refreshed:
                logger.info(f"[USER {self.user_id}] Token was refreshed successfully")
                self.core.stats.record_token_refresh()
            else:
                logger.info(f"[USER {self.user_id}] Token refresh not needed yet")
            auth_status = self.core.get_auth_status()
            if auth_status.get("token_valid"):
                hours_remaining = auth_status.get("time_until_expiry_hours", 0)
                logger.info(f"[USER {self.user_id}] Token status: Valid (expires in {hours_remaining:.2f} hours)")
            else:
                logger.warning(f"[USER {self.user_id}] Token status: Invalid or missing")
        except Exception as e:
            logger.error(f"[USER {self.user_id}] Error in token refresh job: {e}", exc_info=True)

    def update_scan_interval(self, new_minutes: int) -> bool:
        if not self.scheduler or not self.is_running:
            return False
        try:
            job_id = f'inbox_scan_job_user_{self.user_id}'
            self.scheduler.reschedule_job(job_id=job_id, trigger=IntervalTrigger(minutes=new_minutes))
            self.scan_interval_minutes = new_minutes
            logger.info(f"[USER {self.user_id}] Scan interval updated to {new_minutes} minutes")
            return True
        except Exception as e:
            logger.error(f"[USER {self.user_id}] Failed to update scan interval: {e}", exc_info=True)
            return False

    def start(self):
        if self.is_running:
            return
        logger.info(f"[USER {self.user_id}] Starting connector scheduler")
        if not self.core.initialize():
            logger.error(f"[USER {self.user_id}] Failed to initialize core")
            return
        
        self.scheduler = BackgroundScheduler(
            timezone="UTC",
            job_defaults={"coalesce": True, "max_instances": 1}
        )
        self.scheduler.add_job(
            func=self._inbox_scan_job,
            trigger=IntervalTrigger(minutes=self.scan_interval_minutes),
            id=f"inbox_scan_job_user_{self.user_id}",
            name=f"[USER {self.user_id}] Inbox Scan",
            replace_existing=True
        )
        self.scheduler.add_job(
            func=self._token_refresh_job,
            trigger=IntervalTrigger(minutes=self.slim_config.scheduler.token_refresh_check_interval_minutes),
            id=f"token_refresh_job_user_{self.user_id}",
            name=f"[USER {self.user_id}] Token Refresh",
            replace_existing=True
        )
        self.scheduler.start()
        self.is_running = True
        logger.info(f"[USER {self.user_id}] Scheduler started, running initial scan...")
        self._inbox_scan_job()

    def stop(self):
        if not self.is_running:
            return
        logger.info(f"[USER {self.user_id}] Stopping connector scheduler")
        if self.scheduler:
            self.scheduler.shutdown(wait=True)
        self.is_running = False
        logger.info(f"[USER {self.user_id}] Scheduler stopped")

    def get_status(self) -> dict:
        return {
            "connector_id": self.connector_id,
            "user_id": self.user_id,
            "is_running": self.is_running,
            "scan_interval_minutes": self.scan_interval_minutes,
            "pipeline_stats": self.core.get_statistics() if self.is_running else {}
        }
