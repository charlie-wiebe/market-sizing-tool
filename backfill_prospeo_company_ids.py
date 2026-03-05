#!/usr/bin/env python3
"""
Backfill NULL prospeo_company_id values in person_counts table.

The retry script was missing this field, causing data integrity issues.
This script fixes existing records by copying prospeo_company_id from
the associated companies table.
"""

import sys
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Backfill NULL prospeo_company_id values in person_counts"""
    
    # Import Flask app for database configuration
    from app import app
    from config import get_database_url
    
    with app.app_context():
        # Connect to database
        database_url = get_database_url()
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # First, check how many records need backfilling
            logger.info("🔍 Checking records that need backfilling...")
            check_query = text("""
                SELECT 
                    COUNT(*) as records_to_backfill,
                    COUNT(CASE WHEN c.prospeo_company_id IS NOT NULL THEN 1 END) as companies_have_prospeo_id
                FROM person_counts pc
                JOIN companies c ON pc.company_id = c.id
                WHERE pc.prospeo_company_id IS NULL 
                    AND pc.is_active = true
            """)
            
            result = session.execute(check_query).fetchone()
            records_to_backfill = result.records_to_backfill
            companies_have_prospeo_id = result.companies_have_prospeo_id
            
            logger.info(f"📊 Found {records_to_backfill} person_counts with NULL prospeo_company_id")
            logger.info(f"📊 {companies_have_prospeo_id} of these have companies with valid prospeo_company_id")
            
            if records_to_backfill == 0:
                logger.info("✅ No records need backfilling!")
                return
            
            # Execute the backfill UPDATE query
            logger.info("🔧 Running backfill UPDATE query...")
            backfill_query = text("""
                UPDATE person_counts 
                SET prospeo_company_id = c.prospeo_company_id
                FROM companies c
                WHERE person_counts.company_id = c.id
                    AND person_counts.prospeo_company_id IS NULL
                    AND person_counts.is_active = true
                    AND c.prospeo_company_id IS NOT NULL
            """)
            
            result = session.execute(backfill_query)
            records_updated = result.rowcount
            session.commit()
            
            logger.info(f"✅ Backfill completed: {records_updated} records updated")
            
            # Verify the fix
            logger.info("🔍 Verifying backfill results...")
            verify_query = text("""
                SELECT COUNT(*) as remaining_null_records
                FROM person_counts 
                WHERE prospeo_company_id IS NULL 
                    AND is_active = true
            """)
            
            result = session.execute(verify_query).fetchone()
            remaining_null = result.remaining_null_records
            
            if remaining_null == 0:
                logger.info("🎉 SUCCESS: All person_counts now have valid prospeo_company_id values!")
            else:
                logger.warning(f"⚠️  Still have {remaining_null} records with NULL prospeo_company_id")
                
                # Show some examples of remaining NULL records
                sample_query = text("""
                    SELECT pc.id, pc.company_id, c.name, c.prospeo_company_id as company_prospeo_id
                    FROM person_counts pc
                    JOIN companies c ON pc.company_id = c.id
                    WHERE pc.prospeo_company_id IS NULL 
                        AND pc.is_active = true
                    LIMIT 5
                """)
                samples = session.execute(sample_query).fetchall()
                logger.info("Sample remaining NULL records:")
                for sample in samples:
                    logger.info(f"  PersonCount {sample.id} -> Company {sample.company_id} ({sample.name}) - Company prospeo_id: {sample.company_prospeo_id}")
            
        except Exception as e:
            logger.error(f"❌ Backfill failed: {str(e)}")
            session.rollback()
            raise
        finally:
            session.close()
            logger.info("🏁 Backfill script completed")

if __name__ == "__main__":
    main()
