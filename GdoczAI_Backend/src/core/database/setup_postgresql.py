#!/usr/bin/env python3
"""
PostgreSQL Database Setup Runner

Initializes database using schema, triggers, and indexes from SQL files.
"""
import os
import sys
import logging
import yaml
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

CONFIG_PATH = "config/config.yaml"
SCHEMA_DIR = "src/core/schema"
TRIGGERS_DIR = "src/core/triggers"
INDEXES_DIR = "src/core/indexes"

REQUIRED_TABLES = [
    "users", "signup_otps", "api_keys", "document_types", "document_schemas",
    "processed_files", "user_webhooks", "alert_mail",
    "document_logics", "sftp_connector", "smtp_connector"
]

def load_config(config_path=CONFIG_PATH):
    logger.info("Loading configuration: %s", config_path)
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

def connect_database(pg_config, db_only=False):
    """Connect to PostgreSQL. If db_only=True, connect without specifying database."""
    params = pg_config.copy()
    if db_only:
        params['database'] = 'postgres'
    try:
        conn = psycopg2.connect(**params)
        logger.info("[INFO] Connecting to database")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)

def create_database(pg_config):
    db_name = pg_config['database']
    conn = connect_database(pg_config, db_only=True)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cur.fetchone()
        if not exists:
            logger.info("[INFO] Creating database: %s", db_name)
            cur.execute(f"CREATE DATABASE {db_name}")
        else:
            logger.info("[INFO] Database already exists: %s", db_name)
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

def execute_sql_directory(conn, directory):
    logger.info(f"[INFO] Running SQL scripts in: {directory}")
    sql_files = sorted(Path(directory).glob("*.sql"))
    for sql_file in sql_files:
        logger.info(f"[INFO] Executing: {sql_file.name}")
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql = f.read()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        except Exception as e:
            logger.error(f"Failed executing {sql_file.name}: {e}")
            sys.exit(1)

def run_migrations(conn):
    logger.info("[INFO] Running database migrations...")
    try:
        with conn.cursor() as cur:
            # Check if processed_files table exists
            cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'processed_files')")
            table_exists = cur.fetchone()[0]
            if table_exists:
                # Get current columns
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'processed_files'")
                columns = {row[0] for row in cur.fetchall()}
                
                # Columns to drop if they exist
                cols_to_drop = [
                    'mineru_markdown_content',
                    'olmocr_markdown_content',
                    'olmocr_used',
                    'mineru_markdown_path',
                    'olm_markdown_path',
                    'olmocr_markdown_path'
                ]
                for col in cols_to_drop:
                    if col in columns:
                        logger.info(f"[MIGRATION] Dropping deprecated column: {col}")
                        cur.execute(f"ALTER TABLE processed_files DROP COLUMN IF EXISTS {col}")
                        
                # Columns to add if missing
                if 'model' not in columns:
                    logger.info("[MIGRATION] Adding column: model")
                    cur.execute("ALTER TABLE processed_files ADD COLUMN model VARCHAR(100)")
                
                # Drop old index if exists
                logger.info("[MIGRATION] Dropping old index idx_olmocr_used if exists")
                cur.execute("DROP INDEX IF EXISTS idx_olmocr_used")
                
                conn.commit()
                logger.info("[SUCCESS] Database migration completed")
    except Exception as e:
        logger.error(f"[ERROR] Migration failed: {e}")
        conn.rollback()

def verify_tables(conn):
    logger.info("[INFO] Verifying tables exist")
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = {row[0] for row in cur.fetchall()}
        missing = [t for t in REQUIRED_TABLES if t not in tables]
        if missing:
            logger.error(f"Missing tables: {missing}")
            sys.exit(1)
        logger.info("[SUCCESS] Database initialized")

def main():
    config = load_config()
    pg_config = config.get('postgres', {})
    if not pg_config:
        logger.error("Missing postgres config in config.yaml")
        sys.exit(1)
    create_database(pg_config)
    conn = connect_database(pg_config)
    
    # Run dynamic schema migrations on existing tables before applying schema SQL directories
    run_migrations(conn)
    
    execute_sql_directory(conn, SCHEMA_DIR)
    execute_sql_directory(conn, TRIGGERS_DIR)
    execute_sql_directory(conn, INDEXES_DIR)
    verify_tables(conn)
    conn.close()

if __name__ == "__main__":
    main()
