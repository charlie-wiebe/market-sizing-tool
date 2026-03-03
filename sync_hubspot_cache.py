#!/usr/bin/env python3
"""
Sync HubSpot cache with latest data from HubSpot API.
Can be run daily/weekly via cron to keep cache fresh.
Usage: python sync_hubspot_cache.py [--full]
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_database_url, Config
from models.database import HubSpotCache, db
from services.hubspot_client import HubSpotClient
import requests

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HubSpotCacheSync:
    """Sync HubSpot data to local cache."""
    
    def __init__(self):
        self.api_key = Config.HUBSPOT_API_KEY
        if not self.api_key:
            raise ValueError("HUBSPOT_API_KEY not configured")
        
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.base_url = "https://api.hubapi.com"
    
    def get_all_companies(self, modified_after=None):
        """Get all companies from HubSpot, optionally filtered by modification date."""
        companies = []
        after = None
        
        while True:
            url = f"{self.base_url}/crm/v3/objects/companies"
            params = {
                "limit": 100,
                "properties": "domain,hs_linkedin_handle,vertical,name,createdate",
                "associations": "none"
            }
            
            if after:
                params["after"] = after
            
            # Add modified date filter for incremental sync
            if modified_after:
                params["filterGroups"] = [{
                    "filters": [{
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "GTE",
                        "value": int(modified_after.timestamp() * 1000)
                    }]
                }]
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            companies.extend(data.get("results", []))
            
            # Check for pagination
            paging = data.get("paging", {})
            if paging.get("next", {}).get("after"):
                after = paging["next"]["after"]
            else:
                break
        
        return companies
    
    def sync_to_cache(self, companies):
        """Sync companies to cache table."""
        engine = create_engine(get_database_url())
        
        updated_count = 0
        new_count = 0
        
        with engine.begin() as conn:
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
                result = conn.execute(
                    text("SELECT id FROM hubspot_company_cache WHERE hubspot_object_id = :obj_id"),
                    {"obj_id": company["id"]}
                ).first()
                
                if result:
                    # Update existing
                    conn.execute(text("""
                        UPDATE hubspot_company_cache 
                        SET domain = :domain,
                            linkedin_handle = :linkedin_handle,
                            vertical = :vertical,
                            company_name = :company_name,
                            hubspot_created_date = :created_date,
                            last_synced = CURRENT_TIMESTAMP
                        WHERE hubspot_object_id = :obj_id
                    """), {
                        "domain": properties.get("domain"),
                        "linkedin_handle": properties.get("hs_linkedin_handle"),
                        "vertical": properties.get("vertical"),
                        "company_name": properties.get("name"),
                        "created_date": created_date,
                        "obj_id": company["id"]
                    })
                    updated_count += 1
                else:
                    # Insert new
                    conn.execute(text("""
                        INSERT INTO hubspot_company_cache 
                        (hubspot_object_id, domain, linkedin_handle, vertical, 
                         company_name, hubspot_created_date, last_synced)
                        VALUES (:obj_id, :domain, :linkedin_handle, :vertical,
                                :company_name, :created_date, CURRENT_TIMESTAMP)
                    """), {
                        "obj_id": company["id"],
                        "domain": properties.get("domain"),
                        "linkedin_handle": properties.get("hs_linkedin_handle"),
                        "vertical": properties.get("vertical"),
                        "company_name": properties.get("name"),
                        "created_date": created_date
                    })
                    new_count += 1
        
        return new_count, updated_count

def main():
    """Run cache sync."""
    full_sync = "--full" in sys.argv
    
    try:
        sync = HubSpotCacheSync()
        
        if full_sync:
            logger.info("Running FULL sync of all HubSpot companies...")
            modified_after = None
        else:
            # Incremental sync - get companies modified in last 7 days
            modified_after = datetime.utcnow() - timedelta(days=7)
            logger.info(f"Running incremental sync (changes since {modified_after})...")
        
        # Get companies from HubSpot
        logger.info("Fetching companies from HubSpot API...")
        companies = sync.get_all_companies(modified_after)
        logger.info(f"Found {len(companies)} companies to sync")
        
        if not companies:
            logger.info("No companies to sync")
            return
        
        # Sync to cache
        logger.info("Syncing to cache table...")
        new_count, updated_count = sync.sync_to_cache(companies)
        
        logger.info(f"✅ Sync complete!")
        logger.info(f"  New companies: {new_count}")
        logger.info(f"  Updated companies: {updated_count}")
        
        # Show cache stats
        engine = create_engine(get_database_url())
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM hubspot_company_cache"))
            total = result.scalar()
            logger.info(f"  Total companies in cache: {total}")
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
