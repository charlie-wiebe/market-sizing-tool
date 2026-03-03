#!/usr/bin/env python3
"""
Migration to add hs_additional_domains column to hubspot_company_cache table.
This column will store semicolon-separated list of additional domains for a company.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from config import get_database_url
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_migration():
    """Add hs_additional_domains column to hubspot_company_cache table."""
    database_url = get_database_url()
    engine = create_engine(database_url)
    
    try:
        with engine.connect() as conn:
            logger.info("Adding hs_additional_domains column to hubspot_company_cache table...")
            
            # Add the column
            conn.execute(text("""
                ALTER TABLE hubspot_company_cache 
                ADD COLUMN IF NOT EXISTS hs_additional_domains TEXT
            """))
            conn.commit()
            
            logger.info("Migration completed successfully!")
            
            # Verify the column was added
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'hubspot_company_cache' 
                AND column_name = 'hs_additional_domains'
            """))
            
            if result.fetchone():
                logger.info("✓ Confirmed: hs_additional_domains column exists")
            else:
                logger.error("✗ Column was not created successfully")
                return False
                
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
