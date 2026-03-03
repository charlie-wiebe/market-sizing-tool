#!/usr/bin/env python3
"""
Ultra-fast HubSpot enrichment using cache table for Jobs 23 and 25.
Run this script in the Render shell: python cleanup_hubspot_cached.py
"""

import os
import sys
import logging
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add project directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.database import Company, HubSpotEnrichment, HubSpotCache
from services.hubspot_client_cached import HubSpotClientCached
from config import get_database_url

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Re-run HubSpot enrichment for Jobs 23 and 25 using cache."""
    target_job_ids = [23, 25]
    
    # Get database URL from environment
    database_url = get_database_url()
    logger.info(f"Connecting to database...")
    
    # Create database engine and session
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Check cache status
        cache_count = session.query(HubSpotCache).count()
        if cache_count == 0:
            logger.error("HubSpot cache is empty! Run import_hubspot_csv.py first.")
            return
        logger.info(f"HubSpot cache contains {cache_count} companies")
        
        # Domain normalization still needed for companies missing domain
        logger.info("Checking for companies missing domain field...")
        from services.domain_utils import registrable_root_domain
        
        for job_id in target_job_ids:
            companies_missing_domain = session.query(Company).filter_by(job_id=job_id)\
                .filter(Company.domain.is_(None))\
                .filter(Company.website.isnot(None))\
                .all()
            
            if companies_missing_domain:
                logger.info(f"Job {job_id}: Found {len(companies_missing_domain)} companies missing domain, normalizing...")
                for company in companies_missing_domain:
                    try:
                        normalized_domain = registrable_root_domain(company.website)
                        if normalized_domain:
                            company.domain = normalized_domain
                    except Exception as e:
                        logger.warning(f"Failed to normalize domain for company {company.id}: {e}")
                
                session.commit()
                logger.info(f"Job {job_id}: Domain normalization complete.")
        
        # Initialize cached HubSpot client with session
        logger.info("Initializing cached HubSpot client...")
        hubspot_client = HubSpotClientCached(session=session)
        
        # Process each job
        for job_id in target_job_ids:
            start_time = datetime.utcnow()
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing Job {job_id}")
            logger.info(f"{'='*60}")
            
            # Get total count first for progress tracking
            total_count = session.query(Company).filter_by(job_id=job_id).count()
            logger.info(f"Found {total_count} companies for Job {job_id}")
            
            if total_count == 0:
                logger.info(f"No companies found for Job {job_id}, skipping...")
                continue
            
            # Process companies in chunks to prevent memory exhaustion
            chunk_size = 100
            total_enriched = 0
            total_failed = 0
            
            for offset in range(0, total_count, chunk_size):
                # Load chunk of companies
                companies = session.query(Company).filter_by(job_id=job_id)\
                    .offset(offset).limit(chunk_size).all()
                
                chunk_num = (offset // chunk_size) + 1
                total_chunks = (total_count + chunk_size - 1) // chunk_size
                logger.info(f"Processing chunk {chunk_num}/{total_chunks} " + 
                          f"(companies {offset+1}-{min(offset+chunk_size, total_count)} of {total_count})")
                
                # Build batch data for this chunk
                batch_data = []
                for company in companies:
                    batch_data.append({
                        'id': company.id,
                        'linkedin_url': company.linkedin_url,
                        'domain': company.domain
                    })
                
                # Get HubSpot enrichments from cache for this chunk
                enrichments = hubspot_client.batch_enrich_companies(batch_data)
                
                # Process enrichments for this chunk
                for company_id, enrichment_data in enrichments.items():
                    if enrichment_data:
                        try:
                            # Mark existing enrichments as inactive
                            session.query(HubSpotEnrichment).filter(
                                HubSpotEnrichment.company_id == company_id,
                                HubSpotEnrichment.is_active == True
                            ).update({"is_active": False})
                            
                            # Create new active enrichment record
                            new_enrichment = HubSpotEnrichment(
                                company_id=company_id,
                                job_id=job_id,
                                hubspot_object_id=enrichment_data['hubspot_object_id'],
                                vertical=enrichment_data['vertical'],
                                lookup_method=enrichment_data['lookup_method'],
                                hubspot_created_date=enrichment_data['hubspot_created_date'],
                                is_active=True
                            )
                            session.add(new_enrichment)
                            total_enriched += 1
                            
                        except Exception as e:
                            logger.error(f"Failed to save enrichment for company {company_id}: {e}")
                            total_failed += 1
                    else:
                        # No enrichment data found
                        total_failed += 1
                
                # Commit after each chunk to free memory
                session.commit()
                logger.info(f"Chunk {chunk_num}/{total_chunks} completed: {len(enrichments)} enrichments processed")
            
            # Calculate timing
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info(f"\nJob {job_id} completed in {elapsed:.1f} seconds:")
            logger.info(f"  Total companies: {total_count}")
            logger.info(f"  Successfully enriched: {total_enriched}")
            logger.info(f"  Failed: {total_failed}")
            logger.info(f"  No HubSpot match: {total_count - total_enriched - total_failed}")
            logger.info(f"  Processing speed: {int(total_count / elapsed)} companies/second")
        
        logger.info("\n🚀 Cache-based enrichment completed successfully!")
        
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        session.rollback()
        raise
    finally:
        session.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
