#!/usr/bin/env python3
"""
Standalone script to retry person count searches for records that returned errors or 0 results.
Uses the evidence-based domain priority waterfall: website → domain → other_websites

Usage: python retry_failed_person_counts.py [--job-id JOB_ID] [--max-retries MAX] [--dry-run]
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

# Add project directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.database import Company, PersonCount, db
from services.prospeo_client import ProspeoClient
from services.domain_utils import get_search_domains_priority_order
from config import get_database_url

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('retry_failed_person_counts.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def get_failed_person_counts(session, job_id=None, max_age_days=30):
    """
    Query database for person_counts that need retry:
    - ONLY 'SDR Count' query names
    - status != 'ok' OR total_count = 0
    - is_active = true
    """
    max_age = datetime.utcnow() - timedelta(days=max_age_days)
    
    query = session.query(PersonCount).filter(
        PersonCount.is_active == True,
        PersonCount.created_at >= max_age,
        PersonCount.query_name == 'SDR Count',
        or_(
            PersonCount.status != 'ok',
            PersonCount.total_count == 0,
            PersonCount.total_count.is_(None)
        )
    )
    
    if job_id:
        query = query.filter(PersonCount.job_id == job_id)
    
    return query.all()

def prepare_person_search_filters(query_name):
    """
    Prepare person search filters for SDR Count queries.
    Uses the exact filters specified by the user.
    """
    # SDR Count filters as specified
    if query_name == "SDR Count":
        return {
            "person_department": {
                "include": ["Sales Development"],
                "exclude": []
            },
            "person_seniority": {
                "include": ["Entry", "Senior"],
                "exclude": []
            },
            "person_location_search": {
                "include": ["United States", "Canada", "United Kingdom", "Australia"],
                "exclude": ["India"]
            }
        }
    else:
        # This should not happen since we only process SDR Count records
        raise ValueError(f"Unsupported query_name: {query_name}")

def execute_person_search_with_domain(client, filters, domain_root, company_name, query_name):
    """Execute person search with given domain"""
    search_filters = dict(filters)
    if "company" not in search_filters:
        search_filters["company"] = {}
    if "websites" not in search_filters["company"]:
        search_filters["company"]["websites"] = {"include": [], "exclude": []}
    
    search_filters["company"]["websites"]["include"] = [domain_root]
    
    logger.debug(f"Executing person search for {company_name} - {query_name} with domain: {domain_root}")
    response = client.search_people(search_filters, page=1)
    
    result = {"total_count": 0, "status": "ok", "error_code": None}
    
    if client.is_error(response):
        result["status"] = "error"
        result["error_code"] = client.get_error_code(response)
        logger.warning(f"Person search failed for {company_name} - {query_name} (domain: {domain_root}): {result['error_code']}")
    else:
        pagination = client.get_pagination(response)
        result["total_count"] = pagination.get("total_count", 0)
        logger.debug(f"Person search for {company_name} - {query_name} (domain: {domain_root}): {result['total_count']}")
    
    return result

def retry_person_count(session, client, person_count_record, dry_run=False):
    """
    Retry person count search for a single record using evidence-based priority waterfall
    """
    # Get the associated company
    company = session.query(Company).filter_by(id=person_count_record.company_id).first()
    if not company:
        logger.error(f"Company not found for person_count ID {person_count_record.id}")
        return False
    
    logger.info(f"Retrying person count for {company.name} - {person_count_record.query_name}")
    logger.info(f"Previous result: count={person_count_record.total_count}, status={person_count_record.status}, error={person_count_record.error_code}")
    
    # Get domains to try in evidence-based priority order
    domains_to_try = get_search_domains_priority_order(company)
    
    if not domains_to_try:
        logger.warning(f"No domains available for {company.name}")
        return False
    
    # Prepare search filters
    filters = prepare_person_search_filters(person_count_record.query_name)
    
    # Try domains in evidence-based priority order (website → domain → other_websites)
    result = None
    successful_domain = None
    
    for i, domain_root in enumerate(domains_to_try):
        if not domain_root:
            continue
            
        domain_source = "website" if i == 0 else "domain" if i == 1 else "other_websites"
        logger.info(f"Trying {domain_source} for {company.name}: {domain_root}")
        
        result = execute_person_search_with_domain(
            client, filters, domain_root, company.name, person_count_record.query_name
        )
        
        # If we got results, we're done
        if result and result.get("total_count", 0) > 0:
            successful_domain = domain_root
            logger.info(f"✅ Success with {domain_source} for {company.name}: {result['total_count']} people (domain: {domain_root})")
            break
        else:
            logger.info(f"❌ No results with {domain_source} for {company.name} (domain: {domain_root})")
    
    if not result:
        logger.warning(f"All domain attempts failed for {company.name}")
        return False
    
    # Update database with new results
    if not dry_run:
        # Set previous record to inactive
        person_count_record.is_active = False
        
        # Create new active record with retry results
        new_person_count = PersonCount(
            company_id=company.id,
            job_id=person_count_record.job_id,
            query_name=person_count_record.query_name,
            total_count=result.get("total_count", 0),
            status=result.get("status", "ok"),
            error_code=result.get("error_code"),
            domain_searched=successful_domain,  # Record successful domain
            is_active=True
        )
            
        session.add(new_person_count)
        session.commit()
        
        logger.info(f"✅ Updated database for {company.name} - {person_count_record.query_name}: {result['total_count']} people")
    else:
        logger.info(f"[DRY RUN] Would update {company.name} - {person_count_record.query_name}: {result['total_count']} people")
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Retry failed person count searches")
    parser.add_argument("--job-id", type=int, help="Only retry records for specific job ID")
    parser.add_argument("--max-retries", type=int, default=100, help="Maximum number of records to retry (default: 100)")
    parser.add_argument("--max-age-days", type=int, default=30, help="Only retry records newer than this many days (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be retried without making changes")
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting person count retry script")
    logger.info(f"Job ID filter: {args.job_id or 'All jobs'}")
    logger.info(f"Max retries: {args.max_retries}")
    logger.info(f"Max age: {args.max_age_days} days")
    logger.info(f"Dry run: {args.dry_run}")
    
    # Connect to database
    database_url = get_database_url()
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Initialize Prospeo client
    client = ProspeoClient()
    
    try:
        # Find failed person count records
        failed_records = get_failed_person_counts(session, args.job_id, args.max_age_days)
        logger.info(f"📊 Found {len(failed_records)} failed person count records")
        
        if len(failed_records) == 0:
            logger.info("✅ No failed records to retry!")
            return
        
        # Limit to max_retries
        records_to_process = failed_records[:args.max_retries]
        if len(records_to_process) < len(failed_records):
            logger.info(f"📝 Processing first {len(records_to_process)} records (limited by --max-retries)")
        
        success_count = 0
        failed_count = 0
        
        for i, record in enumerate(records_to_process, 1):
            logger.info(f"\n--- Processing record {i}/{len(records_to_process)} ---")
            
            try:
                if retry_person_count(session, client, record, args.dry_run):
                    success_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing record {record.id}: {str(e)}")
                failed_count += 1
                
            # Rate limiting - respect Prospeo limits
            if i < len(records_to_process):  # Don't sleep after last record
                time.sleep(0.1)  # Small delay between requests
        
        # Summary
        logger.info(f"\n🎯 Retry Summary:")
        logger.info(f"   Total records processed: {len(records_to_process)}")
        logger.info(f"   Successful retries: {success_count}")
        logger.info(f"   Failed retries: {failed_count}")
        
        if args.dry_run:
            logger.info("ℹ️  This was a dry run - no database changes were made")
        
        # Show Prospeo API usage stats
        stats = client.get_tracking_stats()
        logger.info(f"💰 API Usage: {stats['total_requests']} requests, {stats['total_rate_limit_delay']:.1f}s rate limit delay")
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Script interrupted by user")
    except Exception as e:
        logger.error(f"❌ Script failed: {str(e)}")
        raise
    finally:
        session.close()
        logger.info("🏁 Script completed")

if __name__ == "__main__":
    main()
