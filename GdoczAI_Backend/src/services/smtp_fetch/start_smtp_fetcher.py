#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys
import logging
from pathlib import Path

from src.services.smtp_fetch.smtp_fetcher_connector_manager import EmailConnectorManager
from src.services.sftp_fetch.sftp_fetch_config import get_slim_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Start multi-connector email fetcher manager"""
    parser = argparse.ArgumentParser(
        description=' Multi-Connector Email Fetcher Pipeline (DATABASE-DRIVEN)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Start with default config.yaml
  %(prog)s --config custom.yaml     # Start with custom config
  %(prog)s --port 3536               # Specify port (for PM2 compatibility)

?? KEY CHANGES:
  - IMAP credentials loaded from DATABASE (smtp_connector table), not config.yaml
  - Multiple connectors run INDEPENDENTLY (one per user)
  - Each connector has its own inbox scheduler
  - Database polling every 1 second detects activation/deactivation
  - email_method column: "gmail" -> imap.gmail.com | "hostinger" -> imap.hostinger.com
        """
    )

    parser.add_argument('--config', default='config/config.yaml',
                        help='Config file path (SLIM: auth/ocr/postgres only)')
    parser.add_argument('--port', type=int, default=3536,
                        help='Port number (for PM2 compatibility)')
    parser.add_argument('--host', default='127.0.0.1',
                        help='Host address (for PM2 compatibility)')

    args = parser.parse_args()

    if not Path(args.config).exists():
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)

    Path("logs").mkdir(exist_ok=True)

    logger.info("=" * 80)
    logger.info("EMAIL FETCHER PIPELINE - MULTI-CONNECTOR APPROACH")
    logger.info("=" * 80)
    logger.info("[NEW] IMAP Credentials : Database-driven (smtp_connector table)")
    logger.info("[NEW] Multiple Users   : Supported")
    logger.info("[NEW] Dynamic Activation: Supported")
    logger.info(f"Slim Config (auth/ocr only): {args.config}")
    logger.info("=" * 80)
    logger.info("Pipeline Features Enabled:")
    logger.info("    IMAP inbox monitoring (per-connector)")
    logger.info("    PDF attachment extraction (per-connector)")
    logger.info("    SHA-256 deduplication (per-connector)")
    logger.info("    OCR integration (per-connector)")
    logger.info("    Email notifications (optional)")
    logger.info("    Scheduled inbox scanning (per-connector interval)")
    logger.info("=" * 80)
    logger.info("Database Features:")
    logger.info("    smtp_connector table (IMAP credentials + config)")
    logger.info("    Per-connector scan intervals")
    logger.info("    Dynamic connector activation/deactivation")
    logger.info("    email_method: gmail->imap.gmail.com | hostinger->imap.hostinger.com")
    logger.info("    Database polling every 1 second")
    logger.info("=" * 80)

    try:
        logger.info(" Loading slim configuration...")
        slim_config = get_slim_config(args.config)
        logger.info(" Slim configuration loaded successfully")

        logger.info(" Initializing Email Connector Manager...")
        connector_manager = EmailConnectorManager(config_path=args.config)
        logger.info("=" * 80)
        logger.info(" MULTI-CONNECTOR EMAIL FETCHER STARTING")
        logger.info("=" * 80)
        logger.info(" Approach: Database-driven IMAP connectors")
        logger.info(" Each active connector runs a dedicated inbox scheduler")
        logger.info(" Polling database every 1 second for changes")
        logger.info("  Press Ctrl+C to stop")
        logger.info("=" * 80)
        connector_manager.run_forever()

    except FileNotFoundError as e:
        logger.error(f" Configuration file not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f" Invalid configuration: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f" Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("=" * 80)
        logger.info(" Email fetcher shutdown complete")
        logger.info("=" * 80)

if __name__ == "__main__":
    main()