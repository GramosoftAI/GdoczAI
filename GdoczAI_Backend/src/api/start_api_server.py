#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Start API server only"""
    parser = argparse.ArgumentParser(
        description='Document Processing API Server (Standalone)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Start on default port 4535
  %(prog)s --port 8000              # Start on custom port
  %(prog)s --host 0.0.0.0 --port 4535  # Bind to all interfaces
        """
    )
    
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=4535, help='Port to bind to')
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload for development')
    parser.add_argument('--log-level', default='info', help='Log level (debug, info, warning, error)')
    
    args = parser.parse_args()
    
    # Verify config file exists
    if not Path(args.config).exists():
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    
    # Create required directories
    Path("logs").mkdir(exist_ok=True)
    Path("temp_uploads").mkdir(exist_ok=True)
    Path("temp_jobs").mkdir(exist_ok=True)
    
    logger.info("=" * 80)
    logger.info("?? DOCUMENT PROCESSING API SERVER (STANDALONE)")
    logger.info("=" * 80)
    logger.info(f"?? Starting server on {args.host}:{args.port}")
    logger.info(f"?? Configuration: {args.config}")
    logger.info(f"?? Log level: {args.log_level.upper()}")
    logger.info("=" * 80)
    logger.info("? API Features Enabled:")
    logger.info("   ? File upload/download")
    logger.info("   ? User authentication")
    logger.info("   ? Document types & schemas")
    logger.info("   ? Webhook management")
    logger.info("   ? Alert configuration")
    logger.info("=" * 80)
    logger.info("??  Pipeline Features Disabled:")
    logger.info("   ? SFTP monitoring")
    logger.info("   ? Automatic background processing")
    logger.info("   ? Pipeline scheduler")
    logger.info("=" * 80)
    logger.info(f"?? API Documentation: http://{args.host}:{args.port}/docs")
    logger.info("=" * 80)
    
    # Import and run uvicorn
    import uvicorn
    
    uvicorn.run(
        "src.api.api_server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level
    )


if __name__ == "__main__":
    main()
