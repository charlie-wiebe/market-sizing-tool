#!/usr/bin/env python3
"""
Direct SQL queries to analyze HubSpot cache matching patterns.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from config import get_database_url

def main():
    # Create database connection
    engine = create_engine(get_database_url())
    
    print("=== Direct SQL Analysis of HubSpot Cache Matching ===\n")
    
    with engine.connect() as conn:
        # 1. Check data quality in companies table for Job 23
        print("1. Data quality check for Job 23 companies:")
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_companies,
                COUNT(linkedin_url) as has_linkedin,
                COUNT(domain) as has_domain,
                COUNT(CASE WHEN linkedin_url IS NOT NULL AND domain IS NOT NULL THEN 1 END) as has_both
            FROM companies 
            WHERE job_id = 23
        """))
        row = result.fetchone()
        print(f"   Total companies: {row[0]}")
        print(f"   Has LinkedIn URL: {row[1]} ({row[1]/row[0]*100:.1f}%)")
        print(f"   Has domain: {row[2]} ({row[2]/row[0]*100:.1f}%)")
        print(f"   Has both: {row[3]} ({row[3]/row[0]*100:.1f}%)")
        
        # 2. Check LinkedIn handle format patterns
        print("\n2. LinkedIn URL patterns in Job 23:")
        result = conn.execute(text("""
            SELECT 
                SUBSTRING(linkedin_url FROM 'linkedin.com/(.+?)$') as pattern,
                COUNT(*) as count
            FROM companies 
            WHERE job_id = 23 AND linkedin_url IS NOT NULL
            GROUP BY pattern
            ORDER BY count DESC
            LIMIT 10
        """))
        for row in result:
            print(f"   {row[0]}: {row[1]} companies")
        
        # 3. Check domain patterns
        print("\n3. Domain patterns in Job 23 (top 10 TLDs):")
        result = conn.execute(text("""
            SELECT 
                SUBSTRING(domain FROM '\\.([^.]+)$') as tld,
                COUNT(*) as count
            FROM companies 
            WHERE job_id = 23 AND domain IS NOT NULL
            GROUP BY tld
            ORDER BY count DESC
            LIMIT 10
        """))
        for row in result:
            print(f"   .{row[0]}: {row[1]} domains")
        
        # 4. Check HubSpot cache data quality
        print("\n4. HubSpot cache data quality:")
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_cached,
                COUNT(linkedin_handle) as has_linkedin,
                COUNT(domain) as has_domain,
                COUNT(CASE WHEN linkedin_handle IS NOT NULL AND domain IS NOT NULL THEN 1 END) as has_both
            FROM hubspot_company_cache
        """))
        row = result.fetchone()
        print(f"   Total cached: {row[0]}")
        print(f"   Has LinkedIn handle: {row[1]} ({row[1]/row[0]*100:.1f}%)")
        print(f"   Has domain: {row[2]} ({row[2]/row[0]*100:.1f}%)")
        print(f"   Has both: {row[3]} ({row[3]/row[0]*100:.1f}%)")
        
        # 5. Direct matching test
        print("\n5. Direct matching test (sample 5 companies):")
        result = conn.execute(text("""
            SELECT 
                c.id,
                c.name,
                c.linkedin_url,
                c.domain,
                hc_linkedin.company_name as linkedin_match_name,
                hc_domain.company_name as domain_match_name
            FROM companies c
            LEFT JOIN hubspot_company_cache hc_linkedin 
                ON LOWER(REGEXP_REPLACE(c.linkedin_url, '^.*linkedin.com/company/([^/]+).*$', 'company/\\1')) = hc_linkedin.linkedin_handle
            LEFT JOIN hubspot_company_cache hc_domain
                ON c.domain = hc_domain.domain
            WHERE c.job_id = 23
            LIMIT 5
        """))
        for row in result:
            print(f"\n   Company: {row[1]} (ID: {row[0]})")
            print(f"   LinkedIn URL: {row[2]}")
            print(f"   Domain: {row[3]}")
            print(f"   LinkedIn match: {row[4] or 'NO MATCH'}")
            print(f"   Domain match: {row[5] or 'NO MATCH'}")
        
        # 6. Check for case sensitivity issues
        print("\n\n6. Case sensitivity check:")
        result = conn.execute(text("""
            SELECT COUNT(*) FROM companies 
            WHERE job_id = 23 
            AND domain IS NOT NULL 
            AND domain != LOWER(domain)
        """))
        print(f"   Companies with uppercase in domain: {result.scalar()}")
        
        result = conn.execute(text("""
            SELECT COUNT(*) FROM hubspot_company_cache 
            WHERE domain IS NOT NULL 
            AND domain != LOWER(domain)
        """))
        print(f"   HubSpot cache entries with uppercase in domain: {result.scalar()}")

if __name__ == "__main__":
    main()
