#!/usr/bin/env python3
"""
Sync HubSpot cache with latest data from HubSpot API.
Tracks new companies by creation date and removes archived companies.
Usage: python sync_hubspot_cache.py
"""

import sys
import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_database_url, Config
from models.database import HubSpotCache, SyncMetadata, db

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HubSpotCacheSync:
    """Sync HubSpot data to local cache efficiently."""
    
    def __init__(self):
        self.api_key = Config.HUBSPOT_API_KEY
        if not self.api_key:
            raise ValueError("HUBSPOT_API_KEY not configured")
        
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.base_url = "https://api.hubapi.com"
        
        # Database setup
        engine = create_engine(get_database_url())
        Session = sessionmaker(bind=engine)
        self.session = Session()
    
    def get_last_sync_timestamp(self):
        """Get the last successful sync timestamp from metadata."""
        sync_record = self.session.query(SyncMetadata).filter_by(
            sync_type='hubspot_cache',
            last_sync_status='success'
        ).first()
        
        if sync_record:
            return sync_record.last_sync_timestamp
        return None
    
    def update_sync_metadata(self, status, records_added=0, records_removed=0, error_message=None):
        """Update sync metadata with results."""
        sync_record = self.session.query(SyncMetadata).filter_by(
            sync_type='hubspot_cache'
        ).first()
        
        if not sync_record:
            sync_record = SyncMetadata(sync_type='hubspot_cache')
            self.session.add(sync_record)
        
        sync_record.last_sync_timestamp = datetime.utcnow()
        sync_record.last_sync_status = status
        sync_record.records_added = records_added
        sync_record.records_removed = records_removed
        sync_record.error_message = error_message
        sync_record.updated_at = datetime.utcnow()
        
        self.session.commit()
    
    def get_archived_companies(self, last_sync_timestamp):
        """Get companies archived since last sync."""
        url = f"{self.base_url}/crm/v3/objects/companies"
        archived_companies = []
        after = None
        
        while True:
            params = {
                "limit": 100,
                "properties": "hs_object_id,archivedAt",
                "archived": "true"  # Only get archived companies
            }
            if after:
                params["after"] = after
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Filter by archivedAt timestamp
            for company in data.get("results", []):
                if last_sync_timestamp:
                    archived_at = company.get("properties", {}).get("archivedAt")
                    if archived_at:
                        # Parse ISO format timestamp
                        archived_ts = datetime.fromisoformat(archived_at.replace('Z', '+00:00'))
                        if archived_ts > last_sync_timestamp:
                            archived_companies.append(company["id"])
                else:
                    # First sync - get all archived
                    archived_companies.append(company["id"])
            
            # Handle pagination
            if data.get("paging", {}).get("next", {}).get("after"):
                after = data["paging"]["next"]["after"]
            else:
                break
        
        return archived_companies
    
    def get_companies_created_after(self, timestamp):
        """Get new companies created since timestamp using search API."""
        url = f"{self.base_url}/crm/v3/objects/companies/search"
        companies = []
        after = 0
        
        while True:
            body = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "createdate",
                        "operator": "GT",
                        "value": int(timestamp.timestamp() * 1000)
                    }]
                }],
                "properties": [
                    "domain",
                    "hs_linkedin_handle", 
                    "vertical",
                    "name",
                    "createdate",
                    "hs_object_id"
                ],
                "limit": 100,
                "after": after
            }
            
            response = requests.post(url, headers=self.headers, json=body)
            response.raise_for_status()
            data = response.json()
            
            companies.extend(data.get("results", []))
            
            # Handle pagination
            if data.get("paging", {}).get("next", {}).get("after"):
                after = data["paging"]["next"]["after"]
            else:
                break
        
        return companies
    
    def get_all_active_companies(self):
        """Get all active companies for initial sync."""
        url = f"{self.base_url}/crm/v3/objects/companies"
        companies = []
        after = None
        
        while True:
            params = {
                "limit": 100,
                "properties": "domain,hs_linkedin_handle,vertical,name,createdate,hs_object_id",
                "archived": "false"  # Only active companies
            }
            if after:
                params["after"] = after
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            companies.extend(data.get("results", []))
            
            # Handle pagination
            if data.get("paging", {}).get("next", {}).get("after"):
                after = data["paging"]["next"]["after"]
            else:
                break
        
        return companies
    
    def remove_from_cache(self, company_ids):
        """Remove companies from cache by HubSpot object ID."""
        if not company_ids:
            return 0
        
        count = 0
        for company_id in company_ids:
            result = self.session.query(HubSpotCache).filter_by(
                hubspot_object_id=str(company_id)
            ).delete()
            if result:
                count += 1
        
        self.session.commit()
        return count
    
    def add_to_cache(self, companies):
        """Add or update companies in cache."""
        if not companies:
            return 0
        
        count = 0
        for company in companies:
            properties = company.get("properties", {})
            
            # Parse create date
            created_date = None
            if properties.get("createdate"):
                try:
                    timestamp_ms = int(properties["createdate"])
                    created_date = datetime.fromtimestamp(timestamp_ms / 1000)
                except:
                    pass
            
            # Check if exists
            existing = self.session.query(HubSpotCache).filter_by(
                hubspot_object_id=str(company["id"])
            ).first()
            
            if existing:
                # Update existing
                existing.domain = properties.get("domain")
                existing.linkedin_handle = properties.get("hs_linkedin_handle")
                existing.vertical = properties.get("vertical")
                existing.company_name = properties.get("name")
                existing.hubspot_created_date = created_date
                existing.last_synced = datetime.utcnow()
            else:
                # Create new
                new_cache_entry = HubSpotCache(
                    hubspot_object_id=str(company["id"]),
                    domain=properties.get("domain"),
                    linkedin_handle=properties.get("hs_linkedin_handle"),
                    vertical=properties.get("vertical"),
                    company_name=properties.get("name"),
                    hubspot_created_date=created_date,
                    last_synced=datetime.utcnow()
                )
                self.session.add(new_cache_entry)
            
            count += 1
        
        self.session.commit()
        return count
    
    def sync(self):
        """Main sync process."""
        last_sync = self.get_last_sync_timestamp()
        sync_start = datetime.utcnow()
        
        try:
            logger.info(f"Starting HubSpot cache sync...")
            if last_sync:
                logger.info(f"Last successful sync: {last_sync}")
            else:
                logger.info("This is the first sync - getting all active companies")
            
            records_removed = 0
            records_added = 0
            
            # Step 1: Handle archived companies
            if last_sync:
                logger.info("Checking for archived companies...")
                archived_ids = self.get_archived_companies(last_sync)
                if archived_ids:
                    logger.info(f"Found {len(archived_ids)} companies archived since last sync")
                    records_removed = self.remove_from_cache(archived_ids)
                    logger.info(f"Removed {records_removed} companies from cache")
                else:
                    logger.info("No archived companies found")
            
            # Step 2: Get new companies
            if last_sync:
                logger.info("Getting new companies created since last sync...")
                new_companies = self.get_companies_created_after(last_sync)
            else:
                logger.info("Getting all active companies for initial sync...")
                new_companies = self.get_all_active_companies()
            
            if new_companies:
                logger.info(f"Found {len(new_companies)} companies to add/update")
                records_added = self.add_to_cache(new_companies)
                logger.info(f"Added/updated {records_added} companies in cache")
            else:
                logger.info("No new companies found")
            
            # Update sync metadata
            self.update_sync_metadata(
                status='success',
                records_added=records_added,
                records_removed=records_removed
            )
            
            # Show final stats
            total_in_cache = self.session.query(HubSpotCache).count()
            elapsed = (datetime.utcnow() - sync_start).total_seconds()
            
            logger.info("✅ Sync completed successfully!")
            logger.info(f"  Time taken: {elapsed:.1f} seconds")
            logger.info(f"  Records added/updated: {records_added}")
            logger.info(f"  Records removed: {records_removed}")
            logger.info(f"  Total companies in cache: {total_in_cache}")
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            import traceback
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            
            # Update sync metadata with error
            self.update_sync_metadata(
                status='failed',
                error_message=str(e)
            )
            
            raise
        finally:
            self.session.close()

def main():
    """Run the sync."""
    try:
        sync = HubSpotCacheSync()
        sync.sync()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
