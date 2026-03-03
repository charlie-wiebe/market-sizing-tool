#!/usr/bin/env python3
"""
Migration to create sync_metadata table for tracking last sync timestamps.
Run: python migrations/add_sync_metadata_table.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from config import get_database_url
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Create sync metadata table."""
    database_url = get_database_url()
    engine = create_engine(database_url)
    
    try:
        with engine.connect() as conn:
            logger.info("Creating sync_metadata table...")
            
            # Create the sync metadata table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sync_metadata (
                    id SERIAL PRIMARY KEY,
                    sync_type VARCHAR(50) UNIQUE NOT NULL,
                    last_sync_timestamp TIMESTAMP,
                    last_sync_status VARCHAR(50),
                    records_added INTEGER DEFAULT 0,
                    records_removed INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            conn.commit()
            logger.info("sync_metadata table created successfully!")
            
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise

if __name__ == "__main__":
    main()
