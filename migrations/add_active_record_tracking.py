#!/usr/bin/env python3
"""
Migration: Add is_active columns to PersonCount and HubSpotEnrichment tables

This migration:
1. Adds is_active column to person_counts table
2. Adds is_active column to hubspot_enrichments table  
3. Sets all existing records to is_active = True
4. Creates database indexes for performance

Supports both SQLite and PostgreSQL databases.
"""

import sys
import os
from sqlalchemy import create_engine, text
from urllib.parse import urlparse

# Add the parent directory to sys.path so we can import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def get_database_type(database_url):
    """Determine database type from URL."""
    parsed = urlparse(database_url)
    return parsed.scheme.split('+')[0]  # Handle postgresql+psycopg format

def run_migration():
    """Run the database migration."""
    database_url = Config.SQLALCHEMY_DATABASE_URI
    db_type = get_database_type(database_url)
    
    print(f"Running migration on {db_type} database: {database_url}")
    
    try:
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            print("Starting migration: Add active record tracking...")
            
            # Check if columns already exist
            if db_type == 'sqlite':
                # SQLite-specific column checks
                result = conn.execute(text("PRAGMA table_info(person_counts)"))
                person_counts_columns = [row[1] for row in result]
                
                result = conn.execute(text("PRAGMA table_info(hubspot_enrichments)"))
                hubspot_columns = [row[1] for row in result]
            else:
                # PostgreSQL-specific column checks
                result = conn.execute(text("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'person_counts' AND table_schema = 'public'
                """))
                person_counts_columns = [row[0] for row in result]
                
                result = conn.execute(text("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'hubspot_enrichments' AND table_schema = 'public'
                """))
                hubspot_columns = [row[0] for row in result]
            
            # Add is_active column to person_counts if it doesn't exist
            if 'is_active' not in person_counts_columns:
                print("Adding is_active column to person_counts table...")
                conn.execute(text("ALTER TABLE person_counts ADD COLUMN is_active BOOLEAN DEFAULT TRUE"))
                
                # Set all existing records to active
                conn.execute(text("UPDATE person_counts SET is_active = TRUE WHERE is_active IS NULL"))
                
                # Create index for performance
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_person_counts_is_active ON person_counts(is_active)"))
                print("✓ Added is_active column and index to person_counts")
            else:
                print("✓ is_active column already exists in person_counts")
            
            # Add is_active column to hubspot_enrichments if it doesn't exist
            if 'is_active' not in hubspot_columns:
                print("Adding is_active column to hubspot_enrichments table...")
                conn.execute(text("ALTER TABLE hubspot_enrichments ADD COLUMN is_active BOOLEAN DEFAULT TRUE"))
                
                # Set all existing records to active
                conn.execute(text("UPDATE hubspot_enrichments SET is_active = TRUE WHERE is_active IS NULL"))
                
                # Create index for performance
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hubspot_enrichments_is_active ON hubspot_enrichments(is_active)"))
                print("✓ Added is_active column and index to hubspot_enrichments")
            else:
                print("✓ is_active column already exists in hubspot_enrichments")
            
            # Commit all changes
            conn.commit()
            
            # Verify the changes
            result = conn.execute(text("SELECT COUNT(*) FROM person_counts WHERE is_active = TRUE"))
            active_person_counts = result.fetchone()[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM hubspot_enrichments WHERE is_active = TRUE"))
            active_hubspot_enrichments = result.fetchone()[0]
            
            print(f"✓ Migration completed successfully!")
            print(f"  - Active person_counts: {active_person_counts}")
            print(f"  - Active hubspot_enrichments: {active_hubspot_enrichments}")
        
        return True
        
    except Exception as e:
        print(f"Error during migration: {e}")
        return False

def rollback_migration():
    """Rollback the migration by removing the is_active columns."""
    db_path = Config.SQLALCHEMY_DATABASE_URI.replace('sqlite:///', '')
    print(f"Rolling back migration on database: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("Rolling back: Remove active record tracking...")
        
        # Note: SQLite doesn't support DROP COLUMN directly
        # For rollback, we'd need to recreate tables without the columns
        # This is a destructive operation, so we'll just print a warning
        print("WARNING: SQLite doesn't support DROP COLUMN.")
        print("To rollback, you would need to:")
        print("1. Export data from both tables")
        print("2. Drop and recreate tables without is_active columns")
        print("3. Re-import data")
        print("This migration is designed to be safe and non-destructive.")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error during rollback: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--rollback":
        success = rollback_migration()
    else:
        success = run_migration()
    
    sys.exit(0 if success else 1)
