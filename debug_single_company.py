#!/usr/bin/env python3
"""
Debug a single company's person count by Prospeo company ID.
Provides detailed debugging output for domain extraction and person search.

Usage:
    python3 debug_single_company.py <prospeo_company_id>
    python3 debug_single_company.py --help
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

from models.database import Company, PersonCount, db
from services.prospeo_client import ProspeoClient
from services.domain_utils import get_search_domains_priority_order
from config import get_database_url

def setup_logging():
    """Setup detailed logging for debugging"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def prepare_person_search_filters():
    """
    Prepare person search filters for debugging.
    Uses standard SDR Count filters.
    """
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

def debug_domain_extraction(company, logger):
    """Debug domain extraction step by step"""
    logger.info(f"=== DOMAIN EXTRACTION DEBUG ===")
    logger.info(f"Company: {company.name}")
    logger.info(f"  ID: {company.id}")
    logger.info(f"  Prospeo Company ID: {company.prospeo_company_id}")
    logger.info(f"  Domain: {company.domain}")
    logger.info(f"  Website: {company.website}")
    logger.info(f"  Other websites: {company.other_websites}")
    logger.info(f"  Other websites type: {type(company.other_websites)}")
    
    # Test direct field access
    logger.info(f"Field access test:")
    logger.info(f"  getattr(company, 'domain', None) = {getattr(company, 'domain', None)}")
    logger.info(f"  getattr(company, 'website', None) = {getattr(company, 'website', None)}")
    logger.info(f"  getattr(company, 'other_websites', None) = {getattr(company, 'other_websites', None)}")
    
    # Test domain extraction function
    domains_to_try = get_search_domains_priority_order(company)
    logger.info(f"Domain extraction result: {domains_to_try}")
    logger.info(f"Number of domains found: {len(domains_to_try)}")
    
    if not domains_to_try:
        logger.error("❌ NO DOMAINS FOUND - This is the bug!")
        return None
    
    logger.info("✅ Domain extraction successful")
    return domains_to_try

def debug_person_search(domains_to_try, filters, company, logger):
    """Debug person search for each domain"""
    logger.info(f"=== PERSON SEARCH DEBUG ===")
    client = ProspeoClient()
    
    for i, domain_root in enumerate(domains_to_try):
        if not domain_root:
            continue
            
        domain_source = "website" if i == 0 else "domain" if i == 1 else "other_websites"
        logger.info(f"Trying domain {i+1}/{len(domains_to_try)}: {domain_root} (from {domain_source})")
        
        # Prepare search filters (match production code structure)
        search_filters = dict(filters)
        if "company" not in search_filters:
            search_filters["company"] = {}
        if "websites" not in search_filters["company"]:
            search_filters["company"]["websites"] = {"include": [], "exclude": []}
        
        search_filters["company"]["websites"]["include"] = [domain_root]
        
        logger.info(f"  Search filters: {search_filters}")
        
        # Make API call (match production code format)
        try:
            response = client.search_people(search_filters, page=1)
            
            if client.is_error(response):
                error_code = client.get_error_code(response)
                logger.warning(f"  ❌ API Error: {error_code}")
                logger.debug(f"  Full response: {response}")
                continue
            
            # Extract results
            pagination = client.get_pagination(response)
            total_count = pagination.get("total_count", 0)
            
            logger.info(f"  ✅ Success: {total_count} people found")
            logger.debug(f"  Pagination: {pagination}")
            
            if total_count > 0:
                logger.info(f"🎉 SUCCESS! Found {total_count} people using {domain_source}: {domain_root}")
                return {
                    "total_count": total_count,
                    "status": "ok",
                    "successful_domain": domain_root,
                    "domain_source": domain_source
                }
            else:
                logger.info(f"  No people found for {domain_root}")
                
        except Exception as e:
            logger.error(f"  ❌ Exception during API call: {e}")
            continue
    
    logger.warning("❌ No successful person search across all domains")
    return {
        "total_count": 0,
        "status": "error",
        "error_code": "NO_RESULTS_ALL_DOMAINS"
    }

def main():
    parser = argparse.ArgumentParser(description='Debug person count for a specific company by Prospeo ID')
    parser.add_argument('prospeo_company_id', help='Prospeo company ID to debug')
    parser.add_argument('--verbose', '-v', action='store_true', help='Extra verbose output')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info(f"=== SINGLE COMPANY DEBUG SCRIPT ===")
    logger.info(f"Target Prospeo Company ID: {args.prospeo_company_id}")
    
    # Connect to database
    database_url = get_database_url()
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Find the company by prospeo_company_id
        logger.info(f"Searching for company with prospeo_company_id: {args.prospeo_company_id}")
        
        company = session.query(Company).filter_by(
            prospeo_company_id=args.prospeo_company_id
        ).first()
        
        if not company:
            logger.error(f"❌ Company not found with prospeo_company_id: {args.prospeo_company_id}")
            logger.info("Searching for similar prospeo_company_ids...")
            
            # Search for similar IDs
            similar = session.query(Company).filter(
                Company.prospeo_company_id.like(f'%{args.prospeo_company_id}%')
            ).limit(5).all()
            
            if similar:
                logger.info("Similar prospeo_company_ids found:")
                for comp in similar:
                    logger.info(f"  - {comp.prospeo_company_id} | {comp.name}")
            
            return 1
        
        logger.info(f"✅ Company found: {company.name}")
        
        # Debug domain extraction
        domains_to_try = debug_domain_extraction(company, logger)
        
        if not domains_to_try:
            logger.error("❌ FAILED: No domains available for person search")
            return 1
        
        # Prepare filters
        filters = prepare_person_search_filters()
        logger.info(f"Using person search filters: {filters}")
        
        # Debug person search
        result = debug_person_search(domains_to_try, filters, company, logger)
        
        logger.info(f"=== FINAL RESULT ===")
        logger.info(f"Company: {company.name}")
        logger.info(f"Total count: {result.get('total_count', 0)}")
        logger.info(f"Status: {result.get('status', 'unknown')}")
        if result.get('successful_domain'):
            logger.info(f"Successful domain: {result['successful_domain']} (from {result.get('domain_source', 'unknown')})")
        if result.get('error_code'):
            logger.info(f"Error code: {result['error_code']}")
        
        # Show existing person count records for this company
        logger.info(f"=== EXISTING PERSON COUNT RECORDS ===")
        existing_counts = session.query(PersonCount).filter_by(
            company_id=company.id
        ).order_by(PersonCount.created_at.desc()).limit(5).all()
        
        if existing_counts:
            logger.info(f"Found {len(existing_counts)} existing records (showing last 5):")
            for count in existing_counts:
                logger.info(f"  - {count.query_name}: count={count.total_count}, "
                           f"status={count.status}, active={count.is_active}, "
                           f"created={count.created_at}")
        else:
            logger.info("No existing person count records found")
        
        logger.info("=== DEBUG COMPLETE ===")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
        
    finally:
        session.close()

if __name__ == "__main__":
    sys.exit(main())
