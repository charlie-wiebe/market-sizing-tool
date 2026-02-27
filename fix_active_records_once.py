#!/usr/bin/env python3
"""
One-time migration to fix corrupted is_active data.

The initial migration incorrectly marked ALL records as is_active=TRUE,
breaking the active record deduplication system. This script fixes that
by properly deduplicating existing data.

Run once: python fix_active_records_once.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.database import db, PersonCount, HubSpotEnrichment
from config import Config
from sqlalchemy import create_engine, text
from datetime import datetime

def fix_active_records():
    """Fix corrupted is_active data with proper deduplication."""
    
    # Connect to database
    if Config.SQLALCHEMY_DATABASE_URI.startswith('sqlite'):
        engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
    else:
        # Production PostgreSQL
        engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
    
    with engine.connect() as conn:
        trans = conn.begin()
        
        try:
            print("Starting is_active data fix...")
            
            # Check current state
            pc_total = conn.execute(text("SELECT COUNT(*) FROM person_counts")).fetchone()[0]
            pc_active = conn.execute(text("SELECT COUNT(*) FROM person_counts WHERE is_active = TRUE")).fetchone()[0]
            
            he_total = conn.execute(text("SELECT COUNT(*) FROM hubspot_enrichments")).fetchone()[0] 
            he_active = conn.execute(text("SELECT COUNT(*) FROM hubspot_enrichments WHERE is_active = TRUE")).fetchone()[0]
            
            print(f"BEFORE: PersonCount - {pc_active}/{pc_total} active")
            print(f"BEFORE: HubSpotEnrichment - {he_active}/{he_total} active")
            
            if pc_active == pc_total and he_active == he_total and pc_total > 0:
                print("⚠️  Data appears corrupted - ALL records are marked active!")
            
            # Step 1: Set ALL records to inactive first
            print("\nStep 1: Setting all records to inactive...")
            
            conn.execute(text("UPDATE person_counts SET is_active = FALSE"))
            updated_pc = conn.rowcount if hasattr(conn, 'rowcount') else pc_total
            print(f"Set {updated_pc} PersonCount records to inactive")
            
            conn.execute(text("UPDATE hubspot_enrichments SET is_active = FALSE"))  
            updated_he = conn.rowcount if hasattr(conn, 'rowcount') else he_total
            print(f"Set {updated_he} HubSpotEnrichment records to inactive")
            
            # Step 2: PersonCount deduplication - latest per (prospeo_company_id, query_name)
            print("\nStep 2: PersonCount deduplication...")
            
            if Config.SQLALCHEMY_DATABASE_URI.startswith('sqlite'):
                # SQLite version with different syntax
                conn.execute(text("""
                    UPDATE person_counts 
                    SET is_active = TRUE 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, 
                                   ROW_NUMBER() OVER (PARTITION BY prospeo_company_id, query_name ORDER BY created_at DESC) as rn
                            FROM person_counts
                            WHERE prospeo_company_id IS NOT NULL
                        ) 
                        WHERE rn = 1
                    )
                """))
                # Also handle records with NULL prospeo_company_id separately  
                conn.execute(text("""
                    UPDATE person_counts 
                    SET is_active = TRUE 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, 
                                   ROW_NUMBER() OVER (PARTITION BY company_id, query_name ORDER BY created_at DESC) as rn
                            FROM person_counts
                            WHERE prospeo_company_id IS NULL
                        ) 
                        WHERE rn = 1
                    )
                """))
            else:
                # PostgreSQL version
                conn.execute(text("""
                    UPDATE person_counts 
                    SET is_active = TRUE 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, 
                                   ROW_NUMBER() OVER (PARTITION BY prospeo_company_id, query_name ORDER BY created_at DESC) as rn
                            FROM person_counts
                            WHERE prospeo_company_id IS NOT NULL
                        ) ranked
                        WHERE rn = 1
                    )
                """))
                # Also handle records with NULL prospeo_company_id separately
                conn.execute(text("""
                    UPDATE person_counts 
                    SET is_active = TRUE 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, 
                                   ROW_NUMBER() OVER (PARTITION BY company_id, query_name ORDER BY created_at DESC) as rn
                            FROM person_counts
                            WHERE prospeo_company_id IS NULL
                        ) ranked
                        WHERE rn = 1
                    )
                """))
            
            # Step 3: HubSpotEnrichment deduplication - latest per hubspot_object_id
            print("Step 3: HubSpotEnrichment deduplication...")
            
            if Config.SQLALCHEMY_DATABASE_URI.startswith('sqlite'):
                # SQLite version
                conn.execute(text("""
                    UPDATE hubspot_enrichments 
                    SET is_active = TRUE 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, 
                                   ROW_NUMBER() OVER (PARTITION BY hubspot_object_id ORDER BY created_at DESC) as rn
                            FROM hubspot_enrichments
                            WHERE hubspot_object_id IS NOT NULL
                        )
                        WHERE rn = 1
                    )
                """))
                # Handle records with NULL hubspot_object_id separately
                conn.execute(text("""
                    UPDATE hubspot_enrichments 
                    SET is_active = TRUE 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, 
                                   ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at DESC) as rn
                            FROM hubspot_enrichments
                            WHERE hubspot_object_id IS NULL
                        )
                        WHERE rn = 1
                    )
                """))
            else:
                # PostgreSQL version  
                conn.execute(text("""
                    UPDATE hubspot_enrichments 
                    SET is_active = TRUE 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, 
                                   ROW_NUMBER() OVER (PARTITION BY hubspot_object_id ORDER BY created_at DESC) as rn
                            FROM hubspot_enrichments
                            WHERE hubspot_object_id IS NOT NULL
                        ) ranked
                        WHERE rn = 1
                    )
                """))
                # Handle records with NULL hubspot_object_id separately
                conn.execute(text("""
                    UPDATE hubspot_enrichments 
                    SET is_active = TRUE 
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, 
                                   ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at DESC) as rn
                            FROM hubspot_enrichments
                            WHERE hubspot_object_id IS NULL
                        ) ranked
                        WHERE rn = 1
                    )
                """))
            
            # Step 4: Verify results
            print("\nStep 4: Verifying results...")
            
            pc_active_after = conn.execute(text("SELECT COUNT(*) FROM person_counts WHERE is_active = TRUE")).fetchone()[0]
            pc_inactive_after = conn.execute(text("SELECT COUNT(*) FROM person_counts WHERE is_active = FALSE")).fetchone()[0]
            
            he_active_after = conn.execute(text("SELECT COUNT(*) FROM hubspot_enrichments WHERE is_active = TRUE")).fetchone()[0] 
            he_inactive_after = conn.execute(text("SELECT COUNT(*) FROM hubspot_enrichments WHERE is_active = FALSE")).fetchone()[0]
            
            print(f"AFTER: PersonCount - {pc_active_after} active, {pc_inactive_after} inactive")
            print(f"AFTER: HubSpotEnrichment - {he_active_after} active, {he_inactive_after} inactive")
            
            # Check for reasonable deduplication
            if pc_total > 0:
                pc_dedup_ratio = (pc_total - pc_active_after) / pc_total * 100
                print(f"PersonCount deduplication: {pc_dedup_ratio:.1f}% records marked inactive")
            
            if he_total > 0:
                he_dedup_ratio = (he_total - he_active_after) / he_total * 100  
                print(f"HubSpotEnrichment deduplication: {he_dedup_ratio:.1f}% records marked inactive")
            
            # Commit the transaction
            trans.commit()
            
            print(f"\n✅ Migration completed successfully!")
            print(f"   PersonCount: {pc_active_after}/{pc_total} records now active")
            print(f"   HubSpotEnrichment: {he_active_after}/{he_total} records now active")
            
            # Mark migration as completed
            try:
                conn.execute(text("CREATE TABLE IF NOT EXISTS migration_log (name VARCHAR(255) PRIMARY KEY, completed_at TIMESTAMP)"))
                conn.execute(text("INSERT OR IGNORE INTO migration_log (name, completed_at) VALUES ('fix_active_records', ?)"), (datetime.utcnow(),))
                conn.commit()
                print("Migration logged successfully")
            except Exception as log_error:
                print(f"Note: Could not log migration completion: {log_error}")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Migration failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

if __name__ == "__main__":
    print("=== Fix Active Records Migration ===")
    print("This will fix corrupted is_active data by properly deduplicating records.")
    
    response = input("Continue? (y/N): ").lower().strip()
    if response != 'y':
        print("Aborted")
        sys.exit(0)
    
    fix_active_records()
