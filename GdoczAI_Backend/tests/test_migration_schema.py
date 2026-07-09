"""
Test Suite: Migration & Schema
Covers: PostgreSQL schema, table creation, and migration logic
"""
import pytest
import psycopg2
from src.api.models.api_server_models import ConfigManager

@pytest.fixture(scope="module")
def pg_config():
    config = ConfigManager('config/config.yaml')
    return config.get('postgres', {})

def test_db_connection(pg_config):
    conn = psycopg2.connect(
        host=pg_config.get('host'),
        port=pg_config.get('port'),
        database=pg_config.get('database'),
        user=pg_config.get('user'),
        password=pg_config.get('password')
    )
    assert conn is not None
    conn.close()

def test_processed_files_table_exists(pg_config):
    conn = psycopg2.connect(
        host=pg_config.get('host'),
        port=pg_config.get('port'),
        database=pg_config.get('database'),
        user=pg_config.get('user'),
        password=pg_config.get('password')
    )
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'processed_files'")
    assert cur.fetchone() is not None
    cur.close()
    conn.close()

def test_document_types_table_exists(pg_config):
    conn = psycopg2.connect(
        host=pg_config.get('host'),
        port=pg_config.get('port'),
        database=pg_config.get('database'),
        user=pg_config.get('user'),
        password=pg_config.get('password')
    )
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'document_types'")
    assert cur.fetchone() is not None
    cur.close()
    conn.close()

def test_document_schema_table_exists(pg_config):
    conn = psycopg2.connect(
        host=pg_config.get('host'),
        port=pg_config.get('port'),
        database=pg_config.get('database'),
        user=pg_config.get('user'),
        password=pg_config.get('password')
    )
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'document_schema'")
    assert cur.fetchone() is not None
    cur.close()
    conn.close()
