#!/usr/bin/env python3
"""
One-time cleanup script to re-run HubSpot enrichment for Jobs 23 and 25.
Run this script in the Render shell: python cleanup_hubspot_enrichment.py
"""

import os
import sys
import logging
from datetime import datetime
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

# Add project directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.database import Company, HubSpotEnrichment
from services.hubspot_client import HubSpotClient
from config import get_database_url

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Re-run HubSpot enrichment for Jobs 23 and 25."""
    target_job_ids = [23, 25]
    
    # Get database URL from environment
    database_url = get_database_url()
    logger.info(f"Connecting to database...")
    
    # Create database engine and session
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # First, normalize domains for companies missing domain but having website
        logger.info("Checking for companies missing domain field across target jobs...")
        from services.domain_utils import registrable_root_domain
        
        for job_id in target_job_ids:
            companies_missing_domain = session.query(Company).filter_by(job_id=job_id)\
                .filter(Company.domain.is_(None))\
                .filter(Company.website.isnot(None))\
                .all()
            
            if companies_missing_domain:
                logger.info(f"Job {job_id}: Found {len(companies_missing_domain)} companies missing domain, normalizing from website field...")
                for company in companies_missing_domain:
                    try:
                        normalized_domain = registrable_root_domain(company.website)
                        if normalized_domain:
                            company.domain = normalized_domain
                            logger.debug(f"Set domain for {company.name}: {normalized_domain} (from {company.website})")
                    except Exception as e:
                        logger.warning(f"Failed to normalize domain for company {company.id} ({company.name}): {e}")
                
                session.commit()
                logger.info(f"Job {job_id}: Domain normalization complete.")
        
        # Initialize HubSpot client
        logger.info("Initializing HubSpot client...")
        hubspot_client = HubSpotClient()
        
        if not hubspot_client.enabled:
            logger.error("HubSpot API key not configured. Exiting.")
            return
        
        # Process each job
        for job_id in target_job_ids:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing Job {job_id}")
            logger.info(f"{'='*60}")
            
            # Get total company count for this job
            total_count = session.query(Company).filter_by(job_id=job_id).count()
            logger.info(f"Found {total_count} companies for Job {job_id}")
            
            if total_count == 0:
                logger.info(f"No companies found for Job {job_id}, skipping...")
                continue
            
            # Process companies in chunks to avoid memory issues
            chunk_size = 500
            total_enriched = 0
            total_failed = 0
            
            for offset in range(0, total_count, chunk_size):
                logger.info(f"\nProcessing companies {offset+1}-{min(offset+chunk_size, total_count)} of {total_count}")
                
                # Load chunk of companies
                companies = session.query(Company).filter_by(job_id=job_id)\
                    .limit(chunk_size)\
                    .offset(offset)\
                    .all()
                
                # Process in batches of 50 for HubSpot API efficiency
                batch_size = 50
                for i in range(0, len(companies), batch_size):
                    batch = companies[i:i + batch_size]
                    
                    # Prepare batch data
                    batch_data = []
                    for company in batch:
                        batch_data.append({
                            'id': company.id,
                            'linkedin_url': company.linkedin_url,
                            'domain': company.domain
                        })
                    
                    logger.info(f"Enriching batch of {len(batch_data)} companies...")
                    
                    # Get HubSpot enrichments
                    enrichments = hubspot_client.batch_enrich_companies(batch_data)
                    
                    # Save enrichment results
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
                            # No match found in HubSpot
                            logger.debug(f"No HubSpot match for company {company_id}")
                    
                    # Commit batch results
                    session.commit()
                    logger.info(f"Batch processed. Total enriched so far: {total_enriched}")
            
            logger.info(f"\nJob {job_id} completed:")
            logger.info(f"  Total companies: {total_count}")
            logger.info(f"  Successfully enriched: {total_enriched}")
            logger.info(f"  Failed: {total_failed}")
            logger.info(f"  No HubSpot match: {total_count - total_enriched - total_failed}")
        
        logger.info("\nCleanup script completed successfully!")
        
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
