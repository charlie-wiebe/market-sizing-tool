#!/usr/bin/env python3
"""
Test script to check sync status and verify incremental sync works.
Run this in Render shell to test the sync mechanism.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.database import SyncMetadata, HubSpotCache
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import get_database_url
from datetime import datetime

def main():
    # Create database session
    engine = create_engine(get_database_url())
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print("=== HubSpot Cache Sync Status ===\n")
    
    # Check sync metadata
    sync = session.query(SyncMetadata).filter_by(sync_type='hubspot_cache').first()
    if sync:
        print(f"Last sync timestamp: {sync.last_sync_timestamp}")
        print(f"Last sync status: {sync.last_sync_status}")
        print(f"Records added: {sync.records_added}")
        print(f"Records removed: {sync.records_removed}")
        if sync.error_message:
            print(f"Error message: {sync.error_message}")
        print(f"Created at: {sync.created_at}")
        print(f"Updated at: {sync.updated_at}")
    else:
        print("No sync record found - this will be the first sync")
    
    # Check cache status
    print(f"\n=== Cache Status ===")
    cache_count = session.query(HubSpotCache).count()
    print(f"Total companies in cache: {cache_count}")
    
    if cache_count > 0:
        # Get sample of recent entries
        recent = session.query(HubSpotCache).order_by(HubSpotCache.last_synced.desc()).limit(5).all()
        print(f"\nMost recently synced companies:")
        for company in recent:
            print(f"  - {company.company_name} (ID: {company.hubspot_object_id}, synced: {company.last_synced})")
        
        # Get oldest entries
        oldest = session.query(HubSpotCache).order_by(HubSpotCache.last_synced.asc()).limit(5).all()
        print(f"\nOldest synced companies:")
        for company in oldest:
            print(f"  - {company.company_name} (ID: {company.hubspot_object_id}, synced: {company.last_synced})")
    
    session.close()

if __name__ == "__main__":
    main()
