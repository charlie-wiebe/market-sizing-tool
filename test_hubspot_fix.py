#!/usr/bin/env python3
"""
Test HubSpot waterfall matching fix
"""

import os
import sys

# Add project directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import app
from models.database import HubSpotCache
from services.hubspot_client_cached import HubSpotClientCached
from services.linkedin_utils import normalize_domain

def test_ttec_matching():
    """Test that ttec.com matches the expected cache record"""
    with app.app_context():
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import create_engine
        from config import get_database_url
        
        database_url = get_database_url()
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        print("=== TESTING TTEC DOMAIN MATCHING ===")
        
        # First verify the expected cache record exists
        print("\n1. Checking for expected TTEC cache record...")
        ttec_record = session.query(HubSpotCache).filter_by(hubspot_object_id='43861826587').first()
        if ttec_record:
            print(f"✅ Found cache record:")
            print(f"   hubspot_object_id: {ttec_record.hubspot_object_id}")
            print(f"   domain: {ttec_record.domain}")
            print(f"   hs_additional_domains: '{ttec_record.hs_additional_domains}'")
            print(f"   company_name: {ttec_record.company_name}")
        else:
            print("❌ Cache record with hubspot_object_id='43861826587' not found!")
            session.close()
            return
            
        # Test the fixed search method
        print("\n2. Testing search_company_by_domain('ttec.com')...")
        client = HubSpotClientCached(session)
        results = client.search_company_by_domain('ttec.com')
        
        print(f"   Results count: {len(results)}")
        for result in results:
            print(f"   ✅ Match found: hubspot_object_id = {result.get('id')}")
            props = result.get('properties', {})
            print(f"      domain: {props.get('domain')}")
            print(f"      hs_additional_domains: '{props.get('hs_additional_domains')}'")
            
        if len(results) == 0:
            print("   ❌ No matches found - fix didn't work!")
        elif any(r.get('id') == '43861826587' for r in results):
            print("   ✅ SUCCESS: Found expected hubspot_object_id='43861826587'")
        else:
            print("   ⚠️  Found matches but not the expected one")
            
        # Test additional domain matching if ttecjobs.com is in hs_additional_domains
        if ttec_record.hs_additional_domains:
            print(f"\n3. Testing additional domains: '{ttec_record.hs_additional_domains}'")
            if 'ttecjobs.com' in ttec_record.hs_additional_domains:
                print("   Testing search_company_by_domain('ttecjobs.com')...")
                results2 = client.search_company_by_domain('ttecjobs.com')
                print(f"   Results count: {len(results2)}")
                if len(results2) > 0:
                    print("   ✅ Additional domain matching works!")
                else:
                    print("   ❌ Additional domain matching failed")
        
        session.close()

def test_company_15000_full_waterfall():
    """Test the complete waterfall for company 15000"""
    with app.app_context():
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import create_engine, text
        from config import get_database_url
        import json
        
        database_url = get_database_url()
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        print("\n=== TESTING COMPANY 15000 COMPLETE WATERFALL ===")
        
        # Get company data
        result = session.execute(text("SELECT name, domain, website, other_websites, linkedin_url FROM companies WHERE id = 15000")).fetchone()
        if not result:
            print("❌ Company 15000 not found!")
            session.close()
            return
            
        name, domain, website, other_websites, linkedin_url = result
        print(f"\nCompany 15000 data:")
        print(f"  name: {name}")
        print(f"  domain: {domain}")
        print(f"  website: {website}")
        print(f"  other_websites: {other_websites}")
        
        # Build domain candidates exactly like the cached client does
        domains_to_try = []
        if website:
            normalized = normalize_domain(website)
            if normalized:
                domains_to_try.append(normalized)
                print(f"  website candidate: {normalized}")
        if domain:
            normalized = normalize_domain(domain)
            if normalized:
                domains_to_try.append(normalized)
                print(f"  domain candidate: {normalized}")
        if other_websites:
            try:
                other_sites = json.loads(other_websites) if isinstance(other_websites, str) else other_websites
                if isinstance(other_sites, list):
                    for site in other_sites:
                        if site:
                            normalized = normalize_domain(site)
                            if normalized:
                                domains_to_try.append(normalized)
                                print(f"  other_websites candidate: {normalized}")
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Test each domain with fixed search method
        client = HubSpotClientCached(session)
        print(f"\nTesting {len(domains_to_try)} domain candidates:")
        
        total_matches = 0
        for i, test_domain in enumerate(domains_to_try, 1):
            print(f"\n{i}. Testing domain: '{test_domain}'")
            results = client.search_company_by_domain(test_domain)
            print(f"   Results: {len(results)} matches")
            total_matches += len(results)
            
            for result in results:
                hubspot_id = result.get('id')
                props = result.get('properties', {})
                print(f"   ✅ Match: hubspot_object_id={hubspot_id}")
                print(f"      cache domain: {props.get('domain')}")
                if props.get('hs_additional_domains'):
                    print(f"      additional domains: '{props.get('hs_additional_domains')}'")
                    
                # Check if this is the expected match
                if hubspot_id == '43861826587':
                    print(f"   🎯 SUCCESS: Found expected match for company 15000!")
        
        if total_matches == 0:
            print("\n❌ NO MATCHES FOUND - waterfall still not working!")
        else:
            print(f"\n✅ Found {total_matches} total matches across all domains")
        
        session.close()

if __name__ == "__main__":
    test_ttec_matching()
    test_company_15000_full_waterfall()
