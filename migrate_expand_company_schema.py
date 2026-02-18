#!/usr/bin/env python3
"""
Database migration to expand Company schema with all Prospeo API fields
and add prospeo_company_id to person_counts table.
"""

import sys
from app import app, db
from sqlalchemy import text

def expand_company_schema():
    """Add all missing fields from Prospeo Company API to companies table."""
    
    with app.app_context():
        try:
            print("Expanding Company schema with Prospeo API fields...")
            
            # Phase 1: Add new columns to companies table
            migrations = [
                # Extended company information
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS description TEXT",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS description_seo TEXT", 
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS description_ai TEXT",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_type VARCHAR(50)",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS employee_range VARCHAR(50)",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS other_websites JSON",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS keywords JSON",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS logo_url VARCHAR(500)",
                
                # Extended location
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS location_country_code VARCHAR(10)",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS location_raw_address TEXT",
                
                # Contact information
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS email_tech JSON",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS phone_hq JSON",
                
                # Social media URLs
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS twitter_url VARCHAR(500)",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS facebook_url VARCHAR(500)",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS crunchbase_url VARCHAR(500)",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS instagram_url VARCHAR(500)",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS youtube_url VARCHAR(500)",
                
                # Revenue details
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS revenue_min BIGINT",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS revenue_max BIGINT",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS revenue_printed VARCHAR(50)",
                
                # Attributes (boolean flags)
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS has_demo BOOLEAN",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS has_free_trial BOOLEAN",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS has_downloadable BOOLEAN",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS has_mobile_apps BOOLEAN",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS has_online_reviews BOOLEAN",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS has_pricing BOOLEAN",
                
                # Funding information
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS funding JSON",
                
                # Technology stack
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS technology JSON",
                
                # Job postings
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS job_postings JSON",
                
                # Classification codes
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS sic_codes JSON",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS naics_codes JSON",
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS linkedin_id VARCHAR(100)"
            ]
            
            # Execute all company table migrations
            for migration in migrations:
                try:
                    with db.engine.connect() as conn:
                        conn.execute(text(migration))
                        conn.commit()
                    print(f"✓ {migration.split('ADD COLUMN IF NOT EXISTS')[1].split()[0]}")
                except Exception as e:
                    print(f"✗ Failed: {migration} - {e}")
            
            # Phase 2: Add prospeo_company_id to person_counts
            person_count_migrations = [
                "ALTER TABLE person_counts ADD COLUMN IF NOT EXISTS prospeo_company_id VARCHAR(100)",
                "CREATE INDEX IF NOT EXISTS ix_person_counts_prospeo_company_id ON person_counts (prospeo_company_id)"
            ]
            
            for migration in person_count_migrations:
                try:
                    with db.engine.connect() as conn:
                        conn.execute(text(migration))
                        conn.commit()
                    if "ADD COLUMN" in migration:
                        print("✓ prospeo_company_id column added to person_counts")
                    else:
                        print("✓ Index created on person_counts.prospeo_company_id")
                except Exception as e:
                    print(f"✗ Failed: {migration} - {e}")
            
            print("\n✓ Schema expansion completed successfully!")
            return True
            
        except Exception as e:
            print(f"✗ Migration failed: {e}")
            return False

def verify_schema():
    """Verify that all new columns were added successfully."""
    
    with app.app_context():
        try:
            # Check companies table columns
            with db.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'companies' 
                    ORDER BY column_name
                """))
                company_columns = [row[0] for row in result.fetchall()]
            
            # Check person_counts table columns
            with db.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'person_counts' 
                    ORDER BY column_name
                """))
                person_count_columns = [row[0] for row in result.fetchall()]
            
            print(f"\nCompanies table now has {len(company_columns)} columns:")
            for col in company_columns:
                print(f"  - {col}")
            
            print(f"\nPerson_counts table now has {len(person_count_columns)} columns:")
            for col in person_count_columns:
                print(f"  - {col}")
            
            # Verify key new columns exist
            expected_new_fields = [
                'description', 'company_type', 'email_tech', 'phone_hq', 
                'twitter_url', 'revenue_min', 'has_demo', 'funding', 
                'technology', 'job_postings', 'sic_codes'
            ]
            
            missing_fields = [field for field in expected_new_fields if field not in company_columns]
            
            if missing_fields:
                print(f"\n✗ Missing fields: {missing_fields}")
                return False
            
            if 'prospeo_company_id' not in person_count_columns:
                print("\n✗ prospeo_company_id missing from person_counts")
                return False
            
            print("\n✓ All expected fields verified successfully!")
            return True
            
        except Exception as e:
            print(f"✗ Schema verification failed: {e}")
            return False

def main():
    print("Database Migration: Expand Company Schema")
    print("=" * 50)
    
    # Show current database URL (without credentials)
    db_url = app.config['SQLALCHEMY_DATABASE_URI']
    safe_url = db_url.split('@')[-1] if '@' in db_url else db_url
    print(f"Database: {safe_url}")
    print()
    
    # Run migration
    success = expand_company_schema()
    
    if success:
        # Verify the changes
        verify_success = verify_schema()
        
        if verify_success:
            print("\n✓ Migration completed and verified successfully!")
            print("Companies table now supports all Prospeo API fields.")
            print("Person_counts table now includes prospeo_company_id.")
        else:
            print("\n⚠ Migration completed but verification failed.")
            sys.exit(1)
    else:
        print("\n✗ Migration failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
