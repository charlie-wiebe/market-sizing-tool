#!/usr/bin/env python3
"""
Check the ACTUAL quality of HubSpot cache data.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from config import get_database_url

def main():
    # Create database connection
    engine = create_engine(get_database_url())
    
    print("=== REAL HubSpot Cache Data Quality Check ===\n")
    
    with engine.connect() as conn:
        # Check for NULL vs empty string
        print("1. Checking for NULL and empty values in HubSpot cache:")
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN linkedin_handle IS NULL THEN 1 END) as linkedin_null,
                COUNT(CASE WHEN linkedin_handle = '' THEN 1 END) as linkedin_empty,
                COUNT(CASE WHEN linkedin_handle IS NOT NULL AND linkedin_handle != '' THEN 1 END) as linkedin_valid,
                COUNT(CASE WHEN domain IS NULL THEN 1 END) as domain_null,
                COUNT(CASE WHEN domain = '' THEN 1 END) as domain_empty,
                COUNT(CASE WHEN domain IS NOT NULL AND domain != '' THEN 1 END) as domain_valid
            FROM hubspot_company_cache
        """))
        row = result.fetchone()
        print(f"   Total entries: {row[0]}")
        print(f"   LinkedIn handle NULL: {row[1]} ({row[1]/row[0]*100:.1f}%)")
        print(f"   LinkedIn handle empty: {row[2]} ({row[2]/row[0]*100:.1f}%)")
        print(f"   LinkedIn handle valid: {row[3]} ({row[3]/row[0]*100:.1f}%)")
        print(f"   Domain NULL: {row[4]} ({row[4]/row[0]*100:.1f}%)")
        print(f"   Domain empty: {row[5]} ({row[5]/row[0]*100:.1f}%)")
        print(f"   Domain valid: {row[6]} ({row[6]/row[0]*100:.1f}%)")
        
        # Show sample entries with missing data
        print("\n2. Sample entries with missing LinkedIn handles:")
        result = conn.execute(text("""
            SELECT 
                hubspot_object_id,
                company_name,
                domain,
                linkedin_handle
            FROM hubspot_company_cache
            WHERE linkedin_handle IS NULL OR linkedin_handle = ''
            LIMIT 10
        """))
        for row in result:
            print(f"   ID: {row[0]}, Name: {row[1]}, Domain: {row[2]}, LinkedIn: '{row[3]}'")
        
        # Check LinkedIn handle format
        print("\n3. LinkedIn handle formats in cache (sample):")
        result = conn.execute(text("""
            SELECT 
                linkedin_handle,
                COUNT(*) as count
            FROM hubspot_company_cache
            WHERE linkedin_handle IS NOT NULL AND linkedin_handle != ''
            GROUP BY linkedin_handle
            ORDER BY count DESC
            LIMIT 5
        """))
        for row in result:
            print(f"   '{row[0]}': {row[1]} entries")
        
        # Check if handles have the expected format
        print("\n4. LinkedIn handle format check:")
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_with_handle,
                COUNT(CASE WHEN linkedin_handle LIKE 'company/%' THEN 1 END) as correct_format,
                COUNT(CASE WHEN linkedin_handle NOT LIKE 'company/%' THEN 1 END) as wrong_format
            FROM hubspot_company_cache
            WHERE linkedin_handle IS NOT NULL AND linkedin_handle != ''
        """))
        row = result.fetchone()
        if row[0] > 0:
            print(f"   With handle: {row[0]}")
            print(f"   Correct format (company/...): {row[1]} ({row[1]/row[0]*100:.1f}%)")
            print(f"   Wrong format: {row[2]} ({row[2]/row[0]*100:.1f}%)")

if __name__ == "__main__":
    main()
