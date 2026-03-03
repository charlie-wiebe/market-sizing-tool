#!/usr/bin/env python3
"""
Diagnostic script to understand why HubSpot cache matching is low.
Shows sample data from both sides to identify mismatches.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.database import Company, HubSpotCache
from services.linkedin_utils import extract_linkedin_handle
from services.domain_utils import registrable_root_domain
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import get_database_url

def main():
    # Create database session
    engine = create_engine(get_database_url())
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print("=== HubSpot Cache Matching Diagnostic ===\n")
    
    # Get sample companies from Job 23
    job_id = 23
    sample_companies = session.query(Company).filter_by(job_id=job_id).limit(10).all()
    
    print(f"Analyzing first 10 companies from Job {job_id}:\n")
    
    matches = 0
    for i, company in enumerate(sample_companies, 1):
        print(f"\n{i}. Company: {company.name}")
        print(f"   ID: {company.id}")
        print(f"   LinkedIn URL: {company.linkedin_url}")
        print(f"   Domain: {company.domain}")
        
        # Extract handle and normalize domain
        linkedin_handle = extract_linkedin_handle(company.linkedin_url)
        normalized_domain = registrable_root_domain(company.domain)
        
        print(f"   → Extracted LinkedIn handle: {linkedin_handle}")
        print(f"   → Normalized domain: {normalized_domain}")
        
        # Search cache by LinkedIn
        linkedin_matches = session.query(HubSpotCache).filter_by(linkedin_handle=linkedin_handle).all()
        print(f"   LinkedIn matches in cache: {len(linkedin_matches)}")
        if linkedin_matches:
            for match in linkedin_matches[:2]:  # Show first 2
                print(f"     - {match.company_name} (handle: {match.linkedin_handle})")
        
        # Search cache by domain
        domain_matches = session.query(HubSpotCache).filter_by(domain=normalized_domain).all()
        print(f"   Domain matches in cache: {len(domain_matches)}")
        if domain_matches:
            for match in domain_matches[:2]:  # Show first 2
                print(f"     - {match.company_name} (domain: {match.domain})")
        
        if linkedin_matches or domain_matches:
            matches += 1
            print(f"   ✓ MATCH FOUND")
        else:
            print(f"   ✗ NO MATCH")
    
    print(f"\n\nSummary: {matches}/10 companies had matches ({matches*10}%)")
    
    # Show some sample HubSpot cache entries
    print("\n\n=== Sample HubSpot Cache Entries ===")
    cache_samples = session.query(HubSpotCache).filter(
        HubSpotCache.linkedin_handle.isnot(None),
        HubSpotCache.domain.isnot(None)
    ).limit(10).all()
    
    for cache in cache_samples:
        print(f"\nCompany: {cache.company_name}")
        print(f"  LinkedIn handle: {cache.linkedin_handle}")
        print(f"  Domain: {cache.domain}")
    
    session.close()

if __name__ == "__main__":
    main()
