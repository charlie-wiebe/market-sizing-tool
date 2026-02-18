#!/usr/bin/env python3
"""
Database migration to add missing query_fingerprint column to jobs table.
This fixes the schema mismatch causing job creation failures in production.
"""

import sys
from app import app, db
from models.database import Job

def add_query_fingerprint_column():
    """Add the missing query_fingerprint column to the jobs table."""
    
    with app.app_context():
        try:
            # Check if column already exists
            inspector = db.inspect(db.engine)
            columns = [col['name'] for col in inspector.get_columns('jobs')]
            
            if 'query_fingerprint' in columns:
                print("✓ query_fingerprint column already exists")
                return True
            
            print("Adding query_fingerprint column to jobs table...")
            
            # Add the column with SQL
            db.engine.execute("""
                ALTER TABLE jobs 
                ADD COLUMN query_fingerprint VARCHAR(32);
            """)
            
            # Add the index
            db.engine.execute("""
                CREATE INDEX ix_jobs_query_fingerprint 
                ON jobs (query_fingerprint);
            """)
            
            print("✓ Successfully added query_fingerprint column and index")
            
            # Verify the column was added
            columns_after = [col['name'] for col in inspector.get_columns('jobs')]
            if 'query_fingerprint' in columns_after:
                print("✓ Column verified in database schema")
                return True
            else:
                print("✗ Column not found after migration")
                return False
                
        except Exception as e:
            print(f"✗ Migration failed: {e}")
            return False

def main():
    print("Database Migration: Adding query_fingerprint column")
    print("=" * 50)
    
    # Show current database URL (without credentials)
    db_url = app.config['SQLALCHEMY_DATABASE_URI']
    safe_url = db_url.split('@')[-1] if '@' in db_url else db_url
    print(f"Database: {safe_url}")
    print()
    
    success = add_query_fingerprint_column()
    
    if success:
        print("\n✓ Migration completed successfully!")
        print("Jobs should now be able to save to the database.")
    else:
        print("\n✗ Migration failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
