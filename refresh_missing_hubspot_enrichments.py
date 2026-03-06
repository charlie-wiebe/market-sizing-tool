#!/usr/bin/env python3
"""
Refresh HubSpot enrichments ONLY for companies with NO existing enrichments.
This will test the waterfall matching fix on companies that should match cache but don't have enrichments yet.

Usage:
    python3 refresh_missing_hubspot_enrichments.py                    # Process all companies with no enrichments
    python3 refresh_missing_hubspot_enrichments.py --limit 50         # Process first 50 companies
    python3 refresh_missing_hubspot_enrichments.py --dry-run          # Test without making changes
    python3 refresh_missing_hubspot_enrichments.py --job-id 123       # Use specific job ID for new enrichments
"""
import os
import sys
import logging
import argparse
from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

# Add project directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.database import Company, HubSpotEnrichment, HubSpotCache, Job
from services.hubspot_client_cached import HubSpotClientCached
from config import get_database_url

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_companies_without_enrichments_count(session):
    """Get count of companies that have NO HubSpot enrichments at all."""
    return session.query(Company)\
        .outerjoin(HubSpotEnrichment, 
                   (Company.id == HubSpotEnrichment.company_id) & 
                   (HubSpotEnrichment.is_active == True))\
        .filter(HubSpotEnrichment.id == None)\
        .count()

def get_companies_without_enrichments_chunk(session, offset, limit):
    """Get a chunk of companies that have NO HubSpot enrichments."""
    return session.query(Company)\
        .outerjoin(HubSpotEnrichment, 
                   (Company.id == HubSpotEnrichment.company_id) & 
                   (HubSpotEnrichment.is_active == True))\
        .filter(HubSpotEnrichment.id == None)\
        .order_by(Company.id)\
        .offset(offset).limit(limit).all()

def main():
    """Run HubSpot enrichment ONLY for companies with no existing enrichments."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Refresh HubSpot enrichments for companies with NO existing enrichments'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of companies to process (default: all)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test run without creating enrichments'
    )
    parser.add_argument(
        '--job-id',
        type=int,
        help='Specific job ID to use for new enrichments (default: latest job)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=25,
        help='Number of companies to process per batch (default: 25)'
    )
    
    args = parser.parse_args()
    
    # Connect to database
    database_url = get_database_url()
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Get target job ID
        if args.job_id:
            target_job = session.query(Job).filter_by(id=args.job_id).first()
            if not target_job:
                logger.error(f"Job ID {args.job_id} not found!")
                return
        else:
            # Use latest job
            target_job = session.query(Job).order_by(Job.id.desc()).first()
            if not target_job:
                logger.error("No jobs found in database!")
                return
        
        logger.info(f"Using job ID: {target_job.id}")
        
        # Get count of companies without enrichments
        logger.info("Counting companies with NO HubSpot enrichments...")
        total_companies = get_companies_without_enrichments_count(session)
        
        if total_companies == 0:
            logger.info("No companies found without HubSpot enrichments!")
            return
        
        # Apply limit if specified
        companies_to_process = min(total_companies, args.limit) if args.limit else total_companies
        
        logger.info(f"Found {total_companies} companies with NO HubSpot enrichments")
        logger.info(f"Processing {companies_to_process} companies")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No enrichments will be created")
        
        # Initialize HubSpot cached client
        hubspot_client = HubSpotClientCached(session)
        
        # Process in batches using chunked queries
        total_processed = 0
        total_matches = 0
        total_created = 0
        offset = 0
        
        while offset < companies_to_process:
            # Calculate batch size for this iteration
            remaining = companies_to_process - offset
            current_batch_size = min(args.batch_size, remaining)
            
            # Load batch from database
            batch = get_companies_without_enrichments_chunk(session, offset, current_batch_size)
            
            if not batch:
                break
                
            batch_num = (offset // args.batch_size) + 1
            total_batches = (companies_to_process + args.batch_size - 1) // args.batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} companies)...")
            
            # Build batch data for enrichment
            batch_data = []
            for company in batch:
                batch_data.append({
                    'id': company.id,
                    'linkedin_url': company.linkedin_url,
                    'domain': company.domain,
                    'website': company.website,
                    'other_websites': company.other_websites
                })
            
            # Run batch enrichment
            enrichment_results = hubspot_client.batch_enrich_companies(batch_data)
            
            # Process results
            batch_matches = 0
            batch_created = 0
            
            for company_id, enrichment_data in enrichment_results.items():
                total_processed += 1
                
                if enrichment_data:
                    batch_matches += 1
                    total_matches += 1
                    
                    company = next(c for c in batch if c.id == company_id)
                    
                    logger.info(f"✅ MATCH: {company.name} (ID: {company_id}) → hubspot_object_id: {enrichment_data['hubspot_object_id']}")
                    logger.info(f"   Domain match: {company.domain} | Website: {company.website}")
                    
                    if not args.dry_run:
                        try:
                            # Create new enrichment record
                            new_enrichment = HubSpotEnrichment(
                                company_id=company_id,
                                job_id=target_job.id,
                                hubspot_object_id=enrichment_data['hubspot_object_id'],
                                properties=enrichment_data,
                                is_active=True
                            )
                            session.add(new_enrichment)
                            session.commit()
                            batch_created += 1
                            total_created += 1
                            
                            logger.info(f"   💾 Created enrichment record")
                            
                        except Exception as e:
                            session.rollback()
                            logger.error(f"   ❌ Failed to create enrichment: {e}")
                    
                else:
                    company = next(c for c in batch if c.id == company_id)
                    logger.debug(f"No match: {company.name} (ID: {company_id})")
            
            logger.info(f"Batch {batch_num} results: {batch_matches} matches, {batch_created} created")
            
            # Move to next batch
            offset += current_batch_size
        
        # Final summary
        logger.info("="*60)
        logger.info("FINAL SUMMARY:")
        logger.info(f"Companies processed: {total_processed}")
        logger.info(f"Cache matches found: {total_matches}")
        logger.info(f"Enrichments created: {total_created}")
        logger.info(f"Match rate: {(total_matches/total_processed*100):.1f}%")
        
        if args.dry_run:
            logger.info("DRY RUN - No changes made to database")
        else:
            logger.info(f"✅ SUCCESS: {total_created} new HubSpot enrichments created!")
        
        # Show some example matches for verification
        if total_matches > 0:
            logger.info("="*60)
            logger.info("Test your fix by checking these companies now have enrichments:")
            
            # Get a few example company IDs that got matches
            example_companies = []
            for company_id, enrichment_data in list(enrichment_results.items())[:3]:
                if enrichment_data:
                    company = session.query(Company).filter_by(id=company_id).first()
                    example_companies.append(f"Company {company_id}: {company.name}")
            
            for example in example_companies:
                logger.info(f"  - {example}")
    
    finally:
        session.close()

if __name__ == "__main__":
    main()
