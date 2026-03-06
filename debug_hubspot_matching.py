#!/usr/bin/env python3
"""
Debug script to test HubSpot waterfall matching logic
"""

import os
import sys

# Add project directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import app
from models.database import db
from services.hubspot_client_cached import HubSpotClientCached
from services.linkedin_utils import normalize_domain, extract_linkedin_handle
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from config import get_database_url

def test_normalize_domain():
    """Test domain normalization"""
    print("=== TESTING DOMAIN NORMALIZATION ===")
    
    test_cases = [
        "https://ttec.com",
        "http://ttec.com",
        "ttec.com",
        "www.ttec.com",
        "https://www.ttec.com/path"
    ]
    
    for test_domain in test_cases:
        normalized = normalize_domain(test_domain)
        print(f"  {test_domain:<30} → {normalized}")

def test_search_company_by_domain():
    """Test the search_company_by_domain method directly"""
    print("\n=== TESTING search_company_by_domain METHOD ===")
    
    with app.app_context():
        database_url = get_database_url()
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # First check if cache has data
        from models.database import HubSpotCache
        cache_count = session.query(HubSpotCache).count()
        print(f"HubSpot cache records: {cache_count}")
        
        if cache_count == 0:
            print("ERROR: HubSpot cache is empty!")
            session.close()
            return
            
        # Check for specific record we expect to find
        ttec_record = session.query(HubSpotCache).filter_by(domain='ttec.com').first()
        if ttec_record:
            print(f"Found TTEC record in cache:")
            print(f"  hubspot_object_id: {ttec_record.hubspot_object_id}")
            print(f"  domain: {ttec_record.domain}")
            print(f"  hs_additional_domains: {ttec_record.hs_additional_domains}")
            print(f"  company_name: {ttec_record.company_name}")
        else:
            print("TTEC record not found in cache with domain='ttec.com'")
            
        client = HubSpotClientCached(session)
        
        # Test with ttec.com (should find hubspot_object_id='43861826587')
        print("\nTesting search_company_by_domain('ttec.com'):")
        results = client.search_company_by_domain('ttec.com')
        print(f"  Results count: {len(results)}")
        for result in results:
            print(f"  - hubspot_object_id: {result.get('id')}")
            props = result.get('properties', {})
            print(f"    domain: {props.get('domain')}")
            print(f"    hs_additional_domains: {props.get('hs_additional_domains')}")
        
        # Test with domain that should be in additional domains
        print("\nTesting search_company_by_domain('ttecjobs.com'):")
        results = client.search_company_by_domain('ttecjobs.com')
        print(f"  Results count: {len(results)}")
        for result in results:
            print(f"  - hubspot_object_id: {result.get('id')}")
            props = result.get('properties', {})
            print(f"    domain: {props.get('domain')}")
            print(f"    hs_additional_domains: {props.get('hs_additional_domains')}")
            
        session.close()

def test_company_15000_waterfall():
    """Test full waterfall logic for company 15000"""
    print("\n=== TESTING COMPANY 15000 WATERFALL ===")
    
    with app.app_context():
        database_url = get_database_url()
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Get company 15000 data directly via raw SQL to avoid model issues
        from sqlalchemy import text
        result = session.execute(text("SELECT name, domain, website, other_websites, linkedin_url FROM companies WHERE id = 15000")).fetchone()
        if not result:
            print("Company 15000 not found!")
            session.close()
            return
            
        name, domain, website, other_websites, linkedin_url = result
        print(f"Company data:")
        print(f"  name: {name}")
        print(f"  domain: {domain}")
        print(f"  website: {website}")
        print(f"  other_websites: {other_websites}")
        print(f"  linkedin_url: {linkedin_url}")
        
        # Build domain candidates
        domains_to_try = []
        if website:
            domains_to_try.append(normalize_domain(website))
        if domain:
            domains_to_try.append(normalize_domain(domain))
        if other_websites:
            try:
                import json
                other_sites = json.loads(other_websites) if isinstance(other_websites, str) else other_websites
                if isinstance(other_sites, list):
                    for site in other_sites:
                        if site:
                            domains_to_try.append(normalize_domain(site))
            except (json.JSONDecodeError, TypeError):
                pass
        
        print(f"\nDomain candidates to try: {domains_to_try}")
        
        # Test each domain
        client = HubSpotClientCached(session)
        for i, domain in enumerate(domains_to_try):
            if domain:
                print(f"\nTrying domain {i+1}: {domain}")
                results = client.search_company_by_domain(domain)
                print(f"  Results: {len(results)} matches")
                for result in results:
                    print(f"    - hubspot_object_id: {result.get('id')}")
        
        # Test LinkedIn handle
        linkedin_handle = extract_linkedin_handle(linkedin_url)
        print(f"\nLinkedIn handle: {linkedin_handle}")
        if linkedin_handle:
            linkedin_results = client.search_company_by_linkedin_handle(linkedin_handle)
            print(f"LinkedIn results: {len(linkedin_results)} matches")
            for result in linkedin_results:
                print(f"  - hubspot_object_id: {result.get('id')}")
        
        session.close()

if __name__ == "__main__":
    test_normalize_domain()
    test_search_company_by_domain()  
    test_company_15000_waterfall()
