#!/usr/bin/env python3
"""
Migration to create HubSpot cache table for fast lookups.
Run: python migrations/add_hubspot_cache_table.py
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
    """Create HubSpot cache table."""
    database_url = get_database_url()
    engine = create_engine(database_url)
    
    try:
        with engine.connect() as conn:
            logger.info("Creating HubSpot cache table...")
            
            # Create the cache table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS hubspot_company_cache (
                    id SERIAL PRIMARY KEY,
                    hubspot_object_id VARCHAR(100) UNIQUE NOT NULL,
                    domain VARCHAR(255),
                    linkedin_handle VARCHAR(255),
                    vertical VARCHAR(255),
                    company_name VARCHAR(500),
                    hubspot_created_date TIMESTAMP,
                    last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # Create indexes for fast lookups
            logger.info("Creating indexes...")
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hubspot_cache_domain ON hubspot_company_cache(domain);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hubspot_cache_linkedin ON hubspot_company_cache(linkedin_handle);"))
            
            conn.commit()
            logger.info("HubSpot cache table created successfully!")
            
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise

if __name__ == "__main__":
    main()
