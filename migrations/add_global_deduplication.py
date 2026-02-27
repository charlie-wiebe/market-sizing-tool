#!/usr/bin/env python3
"""
Database migration for global deduplication system.

This migration adds:
1. CompanyJobReference table for many-to-many job-company relationships
2. Global uniqueness constraint on prospeo_company_id
3. Deduplication metadata tracking columns
4. Indexes for performance optimization
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask
from config import Config
from models.database import db
from sqlalchemy import text

def run_migration():
    app = Flask(__name__)
    app.config.from_object(Config)
    db.init_app(app)
    
    with app.app_context():
        print("Starting global deduplication migration...")
        
        with db.engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            try:
                # 1. Create CompanyJobReference table
                print("Creating company_job_references table...")
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS company_job_references (
                        id SERIAL PRIMARY KEY,
                        company_id INTEGER NOT NULL,
                        job_id INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
                        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
                        UNIQUE(company_id, job_id)
                    )
                """))
                
                # 2. Add deduplication tracking columns to jobs table
                print("Adding deduplication tracking columns to jobs...")
                conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS companies_skipped INTEGER DEFAULT 0"))
                conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS person_counts_skipped INTEGER DEFAULT 0"))
                conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS hubspot_skipped INTEGER DEFAULT 0"))
                conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS skip_existing_companies BOOLEAN DEFAULT TRUE"))
                conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS skip_existing_person_counts BOOLEAN DEFAULT TRUE"))
                conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS skip_existing_hubspot BOOLEAN DEFAULT TRUE"))
                conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_data_age_days INTEGER DEFAULT 30"))
                
                # 3. Handle existing companies with duplicate prospeo_company_ids
                print("Handling existing duplicate prospeo_company_ids...")
                
                # Find duplicates and keep the most recent one
                duplicate_result = conn.execute(text("""
                    SELECT prospeo_company_id, COUNT(*) as count, 
                           array_agg(id ORDER BY created_at DESC) as company_ids
                    FROM companies 
                    WHERE prospeo_company_id IS NOT NULL 
                    GROUP BY prospeo_company_id 
                    HAVING COUNT(*) > 1
                """))
                
                duplicates = duplicate_result.fetchall()
                print(f"Found {len(duplicates)} sets of duplicate companies")
                
                for row in duplicates:
                    prospeo_id, count, company_ids = row
                    # Keep the first (most recent) ID, mark others for deletion
                    ids_to_delete = company_ids[1:]  # All but the first (most recent)
                    
                    if ids_to_delete:
                        print(f"Marking {len(ids_to_delete)} duplicate companies for prospeo_id {prospeo_id}")
                        
                        # Update person_counts and hubspot_enrichments to reference the kept company
                        kept_company_id = company_ids[0]
                        
                        for old_id in ids_to_delete:
                            # Move person_counts to the kept company
                            conn.execute(text("""
                                UPDATE person_counts 
                                SET company_id = :kept_id 
                                WHERE company_id = :old_id
                            """), {"kept_id": kept_company_id, "old_id": old_id})
                            
                            # Move hubspot_enrichments to the kept company (avoid duplicates)
                            conn.execute(text("""
                                UPDATE hubspot_enrichments 
                                SET company_id = :kept_id 
                                WHERE company_id = :old_id
                                AND NOT EXISTS (
                                    SELECT 1 FROM hubspot_enrichments h2 
                                    WHERE h2.company_id = :kept_id 
                                    AND h2.job_id = hubspot_enrichments.job_id
                                )
                            """), {"kept_id": kept_company_id, "old_id": old_id})
                            
                            # Delete duplicate hubspot enrichments
                            conn.execute(text("""
                                DELETE FROM hubspot_enrichments 
                                WHERE company_id = :old_id
                            """), {"old_id": old_id})
                            
                            # Delete the duplicate company
                            conn.execute(text("""
                                DELETE FROM companies WHERE id = :old_id
                            """), {"old_id": old_id})
                
                # 4. Add global uniqueness constraint on prospeo_company_id
                print("Adding global uniqueness constraint on prospeo_company_id...")
                try:
                    conn.execute(text("""
                        ALTER TABLE companies 
                        ADD CONSTRAINT unique_prospeo_company_id 
                        UNIQUE (prospeo_company_id)
                    """))
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print("Uniqueness constraint already exists, skipping...")
                    else:
                        raise
                
                # 5. Add performance indexes
                print("Adding performance indexes...")
                
                # Index on company domain for faster lookups
                try:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies(domain)"))
                except:
                    pass
                    
                # Index on company website
                try:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_companies_website ON companies(website)"))
                except:
                    pass
                
                # Index on person_counts for deduplication checks
                try:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_person_counts_company_query ON person_counts(company_id, query_name)"))
                except:
                    pass
                
                # Index on hubspot_enrichments for deduplication checks  
                try:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hubspot_enrichments_company_job ON hubspot_enrichments(company_id, job_id)"))
                except:
                    pass
                
                # Index on company_job_references
                try:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_company_job_ref_company ON company_job_references(company_id)"))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_company_job_ref_job ON company_job_references(job_id)"))
                except:
                    pass
                
                # 6. Populate company_job_references for existing data
                print("Populating company_job_references for existing companies...")
                conn.execute(text("""
                    INSERT INTO company_job_references (company_id, job_id, created_at)
                    SELECT id, job_id, created_at 
                    FROM companies
                    ON CONFLICT (company_id, job_id) DO NOTHING
                """))
                
                # Commit transaction
                trans.commit()
                print("Migration completed successfully!")
                
            except Exception as e:
                trans.rollback()
                print(f"Migration failed: {e}")
                raise

if __name__ == "__main__":
    run_migration()
