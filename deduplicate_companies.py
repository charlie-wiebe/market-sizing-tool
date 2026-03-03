#!/usr/bin/env python3
"""
One-time script to remove duplicate company records based on prospeo_company_id

This script will:
1. Identify duplicate companies with the same prospeo_company_id
2. Keep the oldest record (smallest id) for each prospeo_company_id
3. Delete all other duplicates
4. Show statistics before and after
"""

import sys
import os
from sqlalchemy import create_engine, text

# Add the parent directory to sys.path so we can import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def run_deduplication():
    """Remove duplicate company records based on prospeo_company_id."""
    print("This script will connect to the Render production database.")
    print("Database: dpg-d6aeo7oboq4c73dclbpg-a.oregon-postgres.render.com")
    print("User: market_sizing_db_user")
    print("Database: market_sizing_db")
    
    # Ask for database password
    import getpass
    password = getpass.getpass("Enter production database password: ")
    
    database_url = f"postgresql+psycopg://market_sizing_db_user:{password}@dpg-d6aeo7oboq4c73dclbpg-a.oregon-postgres.render.com/market_sizing_db?sslmode=require"
    
    print(f"Connecting to production database...")
    
    try:
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            print("Starting company deduplication...")
            
            # Get current statistics
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_companies,
                    COUNT(DISTINCT prospeo_company_id) as unique_prospeo_ids,
                    COUNT(*) - COUNT(DISTINCT prospeo_company_id) as duplicates_to_remove
                FROM companies 
                WHERE prospeo_company_id IS NOT NULL
            """))
            stats = result.fetchone()
            
            print(f"Before deduplication:")
            print(f"  Total companies with prospeo_company_id: {stats[0]}")
            print(f"  Unique prospeo_company_ids: {stats[1]}")
            print(f"  Duplicates to remove: {stats[2]}")
            
            if stats[2] == 0:
                print("No duplicates found. Exiting.")
                return True
                
            # Show some example duplicates
            result = conn.execute(text("""
                SELECT prospeo_company_id, COUNT(*) as duplicate_count 
                FROM companies 
                WHERE prospeo_company_id IS NOT NULL 
                GROUP BY prospeo_company_id 
                HAVING COUNT(*) > 1 
                ORDER BY duplicate_count DESC 
                LIMIT 5
            """))
            duplicates = result.fetchall()
            
            print(f"Example duplicates:")
            for dup in duplicates:
                print(f"  {dup[0]}: {dup[1]} copies")
            
            # Confirm before deletion
            confirmation = input(f"\nAre you sure you want to delete {stats[2]} duplicate records? (yes/no): ")
            if confirmation.lower() != 'yes':
                print("Aborted.")
                return False
            
            # Delete duplicates, keeping the oldest (smallest id) for each prospeo_company_id
            print("Deleting duplicates...")
            result = conn.execute(text("""
                DELETE FROM companies 
                WHERE id NOT IN (
                    SELECT MIN(id) 
                    FROM companies 
                    WHERE prospeo_company_id IS NOT NULL 
                    GROUP BY prospeo_company_id
                )
                AND prospeo_company_id IS NOT NULL
            """))
            
            deleted_count = result.rowcount
            print(f"✓ Deleted {deleted_count} duplicate records")
            
            # Commit the transaction
            conn.commit()
            
            # Verify final statistics
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_companies,
                    COUNT(DISTINCT prospeo_company_id) as unique_prospeo_ids
                FROM companies 
                WHERE prospeo_company_id IS NOT NULL
            """))
            final_stats = result.fetchone()
            
            print(f"\nAfter deduplication:")
            print(f"  Total companies with prospeo_company_id: {final_stats[0]}")
            print(f"  Unique prospeo_company_ids: {final_stats[1]}")
            print(f"  Duplicates remaining: {final_stats[0] - final_stats[1]}")
            
            print(f"\n✓ Deduplication completed successfully!")
            print(f"  Records deleted: {deleted_count}")
            print(f"  Records remaining: {final_stats[0]}")
        
        return True
        
    except Exception as e:
        print(f"Error during deduplication: {e}")
        return False

if __name__ == "__main__":
    print("Company Deduplication Script")
    print("===========================")
    success = run_deduplication()
    exit(0 if success else 1)
