#!/usr/bin/env python3
"""
Memory-efficient HubSpot enrichment refresh with job filtering.
Fixed to use chunked processing pattern from commit 71a474f to prevent memory exhaustion.

Usage:
    python3 refresh_all_hubspot_enrichment_fixed.py                    # Process all jobs
    python3 refresh_all_hubspot_enrichment_fixed.py --job-ids 1,2,3    # Process specific jobs
    python3 refresh_all_hubspot_enrichment_fixed.py --since-job 10     # Process jobs with ID >= 10
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

def main():
    """Run comprehensive HubSpot enrichment for filtered companies."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Refresh HubSpot enrichments with job filtering options'
    )
    parser.add_argument(
        '--job-ids',
        type=str,
        help='Comma-separated list of specific job IDs to process (e.g., 1,2,3)'
    )
    parser.add_argument(
        '--since-job',
        type=int,
        help='Process jobs with ID greater than or equal to this number'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.job_ids and args.since_job:
        logger.error("Cannot specify both --job-ids and --since-job")
        sys.exit(1)
    
    start_time = datetime.utcnow()
    
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
        
        # Initialize cached HubSpot client
        logger.info("Initializing cached HubSpot client...")
        hubspot_client = HubSpotClientCached(session=session)
        
        # Get jobs based on filtering arguments - use chunked processing
        base_jobs_query = session.query(Job).join(Company).group_by(Job.id).order_by(Job.id)
        
        if args.job_ids:
            # Parse specific job IDs
            try:
                specific_job_ids = [int(x.strip()) for x in args.job_ids.split(',')]
                base_jobs_query = base_jobs_query.filter(Job.id.in_(specific_job_ids))
                logger.info(f"Filtering to specific job IDs: {specific_job_ids}")
            except ValueError as e:
                logger.error(f"Invalid job IDs format: {args.job_ids}")
                sys.exit(1)
        elif args.since_job:
            # Filter jobs since specific ID
            base_jobs_query = base_jobs_query.filter(Job.id >= args.since_job)
            logger.info(f"Filtering to jobs with ID >= {args.since_job}")
        else:
            logger.info("Processing all jobs with companies")
            
        # COUNT first - don't load all jobs into memory (fix from commit 71a474f pattern)
        total_job_count = base_jobs_query.count()
        
        if total_job_count == 0:
            logger.error("No jobs found matching the specified criteria")
            return
            
        logger.info(f"Found {total_job_count} jobs with companies to process")
        
        # Track overall statistics
        total_companies_processed = 0
        total_enrichments_added = 0
        total_already_enriched = 0
        job_stats = {}
        
        # Process jobs in chunks to avoid memory exhaustion (pattern from commit 71a474f)
        job_chunk_size = 5  # Process 5 jobs at a time
        job_offset = 0
        
        while job_offset < total_job_count:
            # Load chunk of jobs
            jobs_chunk = base_jobs_query.offset(job_offset).limit(job_chunk_size).all()
            
            if not jobs_chunk:
                break
                
            logger.info(f"\n{'='*50} JOB CHUNK {job_offset//job_chunk_size + 1} {'='*50}")
            logger.info(f"Processing {len(jobs_chunk)} jobs (offset {job_offset})")
            
            # Process each job in this chunk
            for job in jobs_chunk:
                job_start = datetime.utcnow()
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing Job {job.id}: {job.name}")
                logger.info(f"{'='*60}")
                
                # Get total count for this job
                total_count = session.query(Company).filter_by(job_id=job.id).count()
                logger.info(f"Total companies in job: {total_count}")
                
                if total_count == 0:
                    continue
                
                # Track job-specific stats
                job_enrichments = 0
                job_already_enriched = 0
                job_no_match = 0
                
                # Process companies in chunks (prevent memory exhaustion)
                chunk_size = 100
                for offset in range(0, total_count, chunk_size):
                    # Load chunk of companies
                    companies = session.query(Company).filter_by(job_id=job.id)\
                        .offset(offset).limit(chunk_size).all()
                    
                    chunk_num = (offset // chunk_size) + 1
                    total_chunks = (total_count + chunk_size - 1) // chunk_size
                    
                    # Build batch data for enrichment check
                    batch_data = []
                    companies_needing_enrichment = []
                    
                    for company in companies:
                        # Check if company already has active enrichment
                        existing = session.query(HubSpotEnrichment).filter(
                            HubSpotEnrichment.company_id == company.id,
                            HubSpotEnrichment.job_id == job.id,
                            HubSpotEnrichment.is_active == True
                        ).first()
                        
                        if existing:
                            job_already_enriched += 1
                        else:
                            companies_needing_enrichment.append(company)
                            batch_data.append({
                                'id': company.id,
                                'linkedin_url': company.linkedin_url,
                                'domain': company.domain,
                                'website': company.website,
                                'other_websites': company.other_websites
                            })
                    
                    if batch_data:
                        logger.info(f"Chunk {chunk_num}/{total_chunks}: Enriching {len(batch_data)} companies...")
                        
                        # Get enrichments from cache
                        enrichments = hubspot_client.batch_enrich_companies(batch_data)
                        
                        # Save enrichments
                        for company_id, enrichment_data in enrichments.items():
                            if enrichment_data:
                                try:
                                    # Create new enrichment record
                                    new_enrichment = HubSpotEnrichment(
                                        company_id=company_id,
                                        job_id=job.id,
                                        hubspot_object_id=enrichment_data['hubspot_object_id'],
                                        vertical=enrichment_data['vertical'],
                                        lookup_method=enrichment_data['lookup_method'],
                                        hubspot_created_date=enrichment_data['hubspot_created_date'],
                                        is_active=True
                                    )
                                    session.add(new_enrichment)
                                    job_enrichments += 1
                                except Exception as e:
                                    logger.error(f"Failed to save enrichment for company {company_id}: {e}")
                            else:
                                job_no_match += 1
                        
                        # Commit after each chunk to free memory (pattern from commit 71a474f)
                        session.commit()
                    else:
                        logger.info(f"Chunk {chunk_num}/{total_chunks}: All companies already enriched")
                
                # Calculate job timing
                job_elapsed = (datetime.utcnow() - job_start).total_seconds()
                
                # Store job stats
                job_stats[job.id] = {
                    'name': job.name,
                    'total': total_count,
                    'enriched': job_enrichments,
                    'already_enriched': job_already_enriched,
                    'no_match': job_no_match,
                    'time': job_elapsed
                }
                
                # Update totals
                total_companies_processed += total_count
                total_enrichments_added += job_enrichments
                total_already_enriched += job_already_enriched
                
                logger.info(f"\nJob {job.id} completed in {job_elapsed:.1f} seconds:")
                logger.info(f"  New enrichments: {job_enrichments}")
                logger.info(f"  Already enriched: {job_already_enriched}")
                logger.info(f"  No match in cache: {job_no_match}")
            
            # Move to next job chunk
            job_offset += job_chunk_size
        
        # Final summary
        total_elapsed = (datetime.utcnow() - start_time).total_seconds()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"COMPREHENSIVE ENRICHMENT COMPLETE")
        logger.info(f"{'='*60}")
        logger.info(f"\nOverall Statistics:")
        logger.info(f"  Total jobs processed: {len(job_stats)}")
        logger.info(f"  Total companies processed: {total_companies_processed}")
        logger.info(f"  New enrichments added: {total_enrichments_added}")
        logger.info(f"  Already enriched: {total_already_enriched}")
        logger.info(f"  Total time: {total_elapsed:.1f} seconds")
        
        if total_enrichments_added > 0:
            logger.info(f"\nJobs with new enrichments:")
            for job_id, stats in job_stats.items():
                if stats['enriched'] > 0:
                    logger.info(f"  Job {job_id} ({stats['name']}): {stats['enriched']} new enrichments")
        
        logger.info("\n✅ All companies have been processed for HubSpot enrichment!")
        
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        session.close()

if __name__ == '__main__':
    main()
