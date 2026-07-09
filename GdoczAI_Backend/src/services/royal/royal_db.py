# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
Royal Tech Database Setup Script

Creates the royal_processing table in the database specified in royal_config.yaml
"""

import yaml
import psycopg2
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# ============================================================================
# LOAD CONFIGURATION
# ============================================================================
def load_config():
    """Load configuration from royal_config.yaml in the royal folder"""
    try:
        # Load from royal_config.yaml in the same directory as this script
        config_path = Path(__file__).parent / 'royal_config.yaml'
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"✓ Configuration loaded from {config_path}")
            return config
    except FileNotFoundError:
        logger.error(f"✗ royal_config.yaml not found in {Path(__file__).parent}")
        return None
    except Exception as e:
        logger.error(f"✗ Failed to load config: {e}")
        return None

# ============================================================================
# DATABASE CONNECTION
# ============================================================================
def get_db_connection(pg_config):
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=pg_config.get('database', 'document_pipeline'),
            user=pg_config.get('user'),
            password=pg_config.get('password')
        )
        logger.info(f"✓ Connected to database: {pg_config.get('database')}")
        return conn
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return None

# ============================================================================
# CREATE ROYAL_PROCESSING TABLE
# ============================================================================
def create_royal_processing_table(conn):
    """
    Create royal_processing table with specified columns only
    
    Columns:
    - file_id (auto-increment primary key)
    - file_name
    - page_count
    - processed_on
    - processing_duration
    - json_output
    - markdown_output
    - token_usage
    - error_details
    - processing_status
    - created_on
    - updated_on
    - request_id
    - file_path
    - missed_keys
    """
    try:
        cursor = conn.cursor()
        
        # Drop table if exists (optional - comment out if you don't want to drop)
        logger.info("→ Checking if royal_processing table exists...")
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'royal_processing'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            logger.warning("⚠ Table royal_processing already exists")
            response = input("Do you want to drop and recreate it? (yes/no): ")
            if response.lower() in ['yes', 'y']:
                cursor.execute("DROP TABLE royal_processing;")
                logger.info("✓ Dropped existing royal_processing table")
            else:
                logger.info("→ Skipping table creation")
                cursor.close()
                return True
        
        # Create table
        logger.info("→ Creating royal_processing table...")
        
        create_table_sql = """
        CREATE TABLE royal_processing (
            file_id SERIAL PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL,
            page_count INTEGER,
            processed_on TIMESTAMP,
            processing_duration FLOAT,
            json_output JSONB,
            markdown_output TEXT,
            token_usage INTEGER,
            error_details TEXT,
            processing_status VARCHAR(50),
            created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            request_id VARCHAR(100) UNIQUE,
            file_path VARCHAR(500),
            missed_keys JSONB
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        
        logger.info("=" * 80)
        logger.info("✓ ROYAL_PROCESSING TABLE CREATED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info("Table: royal_processing")
        logger.info("Columns:")
        logger.info("  1. file_id (SERIAL PRIMARY KEY)")
        logger.info("  2. file_name (VARCHAR(255) NOT NULL)")
        logger.info("  3. page_count (INTEGER)")
        logger.info("  4. processed_on (TIMESTAMP)")
        logger.info("  5. processing_duration (FLOAT)")
        logger.info("  6. json_output (JSONB)")
        logger.info("  7. markdown_output (TEXT)")
        logger.info("  8. token_usage (INTEGER)")
        logger.info("  9. error_details (TEXT)")
        logger.info(" 10. processing_status (VARCHAR(50))")
        logger.info(" 11. created_on (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        logger.info(" 12. updated_on (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        logger.info(" 13. request_id (VARCHAR(100) UNIQUE)")
        logger.info(" 14. file_path (VARCHAR(500))")
        logger.info(" 15. missed_keys (JSONB)")
        logger.info("=" * 80)
        
        # Create indexes for better performance
        logger.info("→ Creating indexes...")
        
        cursor.execute("""
            CREATE INDEX idx_royal_request_id ON royal_processing(request_id);
        """)
        
        cursor.execute("""
            CREATE INDEX idx_royal_file_name ON royal_processing(file_name);
        """)
        
        cursor.execute("""
            CREATE INDEX idx_royal_processing_status ON royal_processing(processing_status);
        """)
        
        cursor.execute("""
            CREATE INDEX idx_royal_processed_on ON royal_processing(processed_on);
        """)
        
        conn.commit()
        
        logger.info("✓ Created indexes:")
        logger.info("  - idx_royal_request_id")
        logger.info("  - idx_royal_file_name")
        logger.info("  - idx_royal_processing_status")
        logger.info("  - idx_royal_processed_on")
        
        cursor.close()
        
        logger.info("=" * 80)
        logger.info("✓✓✓ DATABASE SETUP COMPLETE ✓✓✓")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to create table: {e}")
        conn.rollback()
        return False

# ============================================================================
# VERIFY TABLE CREATION
# ============================================================================
def verify_table(conn):
    """Verify that the table was created successfully"""
    try:
        cursor = conn.cursor()
        
        logger.info("→ Verifying table structure...")
        
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'royal_processing'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        
        logger.info("=" * 80)
        logger.info("✓ TABLE STRUCTURE VERIFICATION")
        logger.info("=" * 80)
        
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            max_length = col[2] if col[2] else ''
            nullable = col[3]
            
            type_str = f"{data_type}"
            if max_length:
                type_str += f"({max_length})"
            
            null_str = "NULL" if nullable == 'YES' else "NOT NULL"
            
            logger.info(f"  {col_name:25} {type_str:20} {null_str}")
        
        logger.info("=" * 80)
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Verification failed: {e}")
        return False

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("ROYAL TECH DATABASE SETUP")
    logger.info("=" * 80)
    
    # Load configuration
    config = load_config()
    if not config:
        logger.error("✗ Cannot proceed without configuration")
        return
    
    pg_config = config.get('postgres', {})
    
    if not pg_config.get('host') or not pg_config.get('user'):
        logger.error("✗ PostgreSQL configuration incomplete in config.yaml")
        logger.error("  Required: host, user, password, database")
        return
    
    logger.info(f"→ Database: {pg_config.get('database')}")
    logger.info(f"→ Host: {pg_config.get('host')}")
    logger.info(f"→ Port: {pg_config.get('port', 5432)}")
    logger.info(f"→ User: {pg_config.get('user')}")
    
    # Connect to database
    conn = get_db_connection(pg_config)
    if not conn:
        logger.error("✗ Cannot proceed without database connection")
        return
    
    try:
        # Create table
        success = create_royal_processing_table(conn)
        
        if success:
            # Verify table
            verify_table(conn)
            
            logger.info("=" * 80)
            logger.info("✓ Setup completed successfully!")
            logger.info("=" * 80)
            logger.info("You can now run the Royal Tech OCR API:")
            logger.info("  python royal_tech_app.py")
            logger.info("=" * 80)
        else:
            logger.error("✗ Setup failed")
    
    finally:
        conn.close()
        logger.info("→ Database connection closed")

if __name__ == "__main__":
    main()
