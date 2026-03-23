#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse
import sys
import logging
from pathlib import Path

# ?? NEW: Import ConnectorManager instead of PipelineScheduler
from src.services.sftp_fetch.sftp_fetch_connector_manager import ConnectorManager
from src.services.sftp_fetch.sftp_fetch_config import get_slim_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Start multi-connector pipeline manager"""
    parser = argparse.ArgumentParser(
        description='?? Multi-Connector Document Processing Pipeline (NEW APPROACH)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Start with default config.yaml
  %(prog)s --config custom.yaml     # Start with custom config
  %(prog)s --port 3535               # Specify port (for PM2 compatibility)

?? KEY CHANGES:
  - SFTP credentials loaded from DATABASE, not config.yaml
  - Multiple connectors run INDEPENDENTLY
  - Each connector has its own pipeline scheduler
  - Database polling every 1 second detects activation/deactivation
  - Old single-user approach COMPLETELY REPLACED
        """
    )
    
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path (SLIM: auth/ocr only)')
    parser.add_argument('--port', type=int, default=8000, help='Port number (for reference/PM2 compatibility)')
    parser.add_argument('--host', default='127.0.0.1', help='Host address (for reference/PM2 compatibility)')
    
    args = parser.parse_args()
    
    # Verify slim config file exists
    if not Path(args.config).exists():
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    
    # Create required directories
    Path("logs").mkdir(exist_ok=True)
    
    logger.info("=" * 80)
    logger.info("?? DOCUMENT PROCESSING PIPELINE - MULTI-CONNECTOR APPROACH")
    logger.info("=" * 80)
    logger.info("?? [NEW] SFTP Credentials: Database-driven")
    logger.info("?? [NEW] Multiple Users: Supported",)
    logger.info("?? [NEW] Dynamic Activation: Supported")
    logger.info(f"?? Slim Config (auth/ocr only): {args.config}")
    logger.info("=" * 80)
    logger.info("? Pipeline Features Enabled:")
    logger.info("   ? SFTP folder monitoring (per-connector)")
    logger.info("   ? Automatic PDF processing (per-connector)")
    logger.info("   ? OCR integration (per-connector)")
    logger.info("   ? Email notifications (optional)")
    logger.info("   ? Scheduled batch processing (per-connector)")
    logger.info("=" * 80)
    logger.info("? Database Features:")
    logger.info("   ? sftp_connector table (user credentials + folders)")
    logger.info("   ? Per-connector scan intervals")
    logger.info("   ? Dynamic connector activation/deactivation")
    logger.info("   ? Database polling every 1 second")
    logger.info("=" * 80)
    logger.info("?? API Features (NOT Included):")
    logger.info("   ? REST API endpoints (use separate API server)")
    logger.info("   ? Web-based file uploads (use separate API)")
    logger.info("   ? API authentication (separate concern)")
    logger.info("=" * 80)
    
    try:
        # Load slim configuration
        logger.info("?? Loading slim configuration...")
        slim_config = get_slim_config(args.config)
        logger.info("? Slim configuration loaded successfully")
        
        # ?? Create and run connector manager
        logger.info("?? Initializing Connector Manager...")
        connector_manager = ConnectorManager(config_path=args.config)
        
        logger.info("=" * 80)
        logger.info("?? MULTI-CONNECTOR PIPELINE STARTING")
        logger.info("=" * 80)
        logger.info("?? Approach: Database-driven SFTP connectors")
        logger.info("?? Each active connector runs a dedicated pipeline scheduler")
        logger.info("?? Polling database every 1 second for changes")
        logger.info("?? Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        # Run forever (blocks until interrupted)
        connector_manager.run_forever()
        
    except FileNotFoundError as e:
        logger.error(f"? Configuration file not found: {e}")
        sys.exit(1)
    
    except ValueError as e:
        logger.error(f"? Invalid configuration: {e}")
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"? Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("=" * 80)
        logger.info("?? Pipeline shutdown complete")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
