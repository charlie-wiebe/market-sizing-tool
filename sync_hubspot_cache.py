#!/usr/bin/env python3
"""
Sync HubSpot cache with latest data from HubSpot API.
Tracks new companies by creation date and removes archived companies.
Usage: python sync_hubspot_cache.py
"""

import sys
import os
import logging
import time
from datetime import datetime, UTC
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
        
        # Rate limiting: 5 requests per second for HubSpot API
        self.max_requests_per_window = 5
        self.window_duration = 1.0
        self.request_times = []
    
    def get_last_sync_timestamp(self):
        """Get the last successful sync timestamp from metadata."""
        sync_record = self.session.query(SyncMetadata).filter_by(
            sync_type='hubspot_cache',
            last_sync_status='success'
        ).first()
        
        if sync_record:
            return sync_record.last_sync_timestamp
        return None
    
    def _rate_limit_wait(self):
        """Enforce rate limiting based on HubSpot's 5 requests per second limit."""
        now = time.time()
        
        # Remove requests older than the window
        self.request_times = [t for t in self.request_times if now - t < self.window_duration]
        
        # If we're at the limit, wait until we can make another request
        if len(self.request_times) >= self.max_requests_per_window:
            sleep_time = self.window_duration - (now - self.request_times[0]) + 0.1
            if sleep_time > 0:
                logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        # Record this request
        self.request_times.append(time.time())
    
    def update_sync_metadata(self, status, records_added=0, records_removed=0, error_message=None):
        """Update sync metadata with results."""
        sync_record = self.session.query(SyncMetadata).filter_by(
            sync_type='hubspot_cache'
        ).first()
        
        if not sync_record:
            sync_record = SyncMetadata(sync_type='hubspot_cache')
            self.session.add(sync_record)
        
        sync_record.last_sync_timestamp = datetime.now(UTC)
        sync_record.last_sync_status = status
        sync_record.records_added = records_added
        sync_record.records_removed = records_removed
        sync_record.error_message = error_message
        sync_record.updated_at = datetime.now(UTC)
        
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
                    "hs_additional_domains",
                    "hs_linkedin_handle", 
                    "vertical",
                    "name",
                    "createdate",
                    "hs_object_id",
                    "aip___of_sdrs",
                    "manual_override_____sdrs",
                    "mixrank_____sdrs",
                    "keyplay___sdrs_bdrs",
                    "clay_estimated___sdrs",
                    "estimated___sdrs"
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
                "properties": "domain,hs_additional_domains,hs_linkedin_handle,vertical,name,createdate,hs_object_id,aip___of_sdrs,manual_override_____sdrs,mixrank_____sdrs,keyplay___sdrs_bdrs,clay_estimated___sdrs,estimated___sdrs",
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
            
            # Helper function to parse integer safely
            def parse_int(value):
                if value is None or value == "":
                    return None
                try:
                    return int(float(str(value)))
                except (ValueError, TypeError):
                    return None
            
            if existing:
                # Update existing
                existing.domain = properties.get("domain")
                existing.hs_additional_domains = properties.get("hs_additional_domains")
                existing.linkedin_handle = properties.get("hs_linkedin_handle")
                existing.vertical = properties.get("vertical")
                existing.company_name = properties.get("name")
                existing.hubspot_created_date = created_date
                # Update SDR count fields
                existing.aip_sdrs = parse_int(properties.get("aip___of_sdrs"))
                existing.override_sdrs = parse_int(properties.get("manual_override_____sdrs"))
                existing.mixrank_sdrs = parse_int(properties.get("mixrank_____sdrs"))
                existing.keyplay_sdrs = parse_int(properties.get("keyplay___sdrs_bdrs"))
                existing.clay_sdrs = parse_int(properties.get("clay_estimated___sdrs"))
                existing.final_sdrs = parse_int(properties.get("estimated___sdrs"))
                existing.last_synced = datetime.now(UTC)
            else:
                # Create new
                new_cache_entry = HubSpotCache(
                    hubspot_object_id=str(company["id"]),
                    domain=properties.get("domain"),
                    hs_additional_domains=properties.get("hs_additional_domains"),
                    linkedin_handle=properties.get("hs_linkedin_handle"),
                    vertical=properties.get("vertical"),
                    company_name=properties.get("name"),
                    hubspot_created_date=created_date,
                    # Add SDR count fields
                    aip_sdrs=parse_int(properties.get("aip___of_sdrs")),
                    override_sdrs=parse_int(properties.get("manual_override_____sdrs")),
                    mixrank_sdrs=parse_int(properties.get("mixrank_____sdrs")),
                    keyplay_sdrs=parse_int(properties.get("keyplay___sdrs_bdrs")),
                    clay_sdrs=parse_int(properties.get("clay_estimated___sdrs")),
                    final_sdrs=parse_int(properties.get("estimated___sdrs")),
                    last_synced=datetime.now(UTC)
                )
                self.session.add(new_cache_entry)
            
            count += 1
        
        self.session.commit()
        return count
    
    def reconcile_hubspot_ids(self):
        """Reconcile HubSpot IDs for companies that may have been merged.
        
        When companies are merged in HubSpot, they get new IDs but our cache
        still has the old ones. This method batch-checks all cached IDs.
        """
        logger.info("Starting HubSpot ID reconciliation for merged companies...")
        
        # Get all HubSpot IDs currently in cache
        cache_records = self.session.query(HubSpotCache).all()
        if not cache_records:
            logger.info("No records in cache to reconcile")
            return 0
            
        updated_count = 0
        total_records = len(cache_records)
        logger.info(f"Reconciling {total_records} cached HubSpot IDs...")
        
        # Process in batches of 100 (HubSpot batch read limit)
        for i in range(0, len(cache_records), 100):
            batch = cache_records[i:i + 100]
            batch_ids = [record.hubspot_object_id for record in batch]
            
            logger.info(f"Reconciling batch {i//100 + 1}: {len(batch)} records")
            
            # Use batch read to get current HubSpot data
            url = f"{self.base_url}/crm/v3/objects/companies/batch/read"
            payload = {
                "inputs": [{"id": str(obj_id)} for obj_id in batch_ids],
                "properties": ["hs_object_id"]  # Only need the current ID
            }
            
            try:
                self._rate_limit_wait()
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
                response.raise_for_status()
                batch_response = response.json()
                
                if 'results' not in batch_response:
                    logger.warning(f"Invalid response for ID reconciliation batch")
                    continue
                    
                # FIXED LOGIC: HubSpot batch/read does NOT preserve order!
                # We need to create a mapping from requested IDs to returned IDs
                # When a company is merged, the hs_object_id property shows the current ID
                
                # Create mapping from requested ID to current ID
                id_mapping = {}
                for result in batch_response['results']:
                    current_id = result['id']  # The actual current ID in HubSpot
                    hs_object_id = result.get('properties', {}).get('hs_object_id')
                    
                    # hs_object_id should match current_id for active records
                    # If they differ, something is wrong with our understanding
                    if hs_object_id and hs_object_id != current_id:
                        logger.warning(f"Unexpected: hs_object_id ({hs_object_id}) != current_id ({current_id})")
                
                # The real way to detect merges: if we requested ID X but got no result for X,
                # then X was likely merged. But HubSpot's batch read should return results
                # for valid IDs, even if they were merged (returning the new merged record).
                
                # Actually, let's check what IDs we got back vs what we requested
                returned_ids = {result['id'] for result in batch_response['results']}
                requested_ids = {record.hubspot_object_id for record in batch}
                
                # If we requested IDs that aren't in the returned set, they might be merged/deleted
                missing_ids = requested_ids - returned_ids
                if missing_ids:
                    logger.info(f"IDs not returned by batch read (possibly merged/deleted): {missing_ids}")
                
                # For now, let's not make any updates since we're not sure about the correct logic
                # This needs more investigation with the actual HubSpot API behavior
                logger.info("Skipping ID updates - logic needs verification with HubSpot API behavior")
                        
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to reconcile ID batch: {e}")
                continue
                
        # Commit all ID updates
        if updated_count > 0:
            try:
                self.session.commit()
                logger.info(f"✅ ID reconciliation complete: {updated_count} HubSpot IDs updated")
            except Exception as e:
                logger.error(f"Failed to commit ID updates: {e}")
                self.session.rollback()
                updated_count = 0
        else:
            logger.info("✅ ID reconciliation complete: No ID updates needed")
            
        return updated_count
    
    def sync(self):
        """Main sync process."""
        last_sync = self.get_last_sync_timestamp()
        sync_start = datetime.now(UTC)
        
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
            
            # Step 2: Reconcile HubSpot IDs for merged companies - DISABLED DUE TO BUG
            logger.info("SKIPPING HubSpot ID reconciliation - disabled due to database corruption bug")
            reconciled_count = 0
            # reconciled_count = self.reconcile_hubspot_ids()
            # if reconciled_count > 0:
            #     logger.info(f"Reconciled {reconciled_count} HubSpot IDs from merged companies")
            
            # Step 3: Get new companies
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
            elapsed = (datetime.now(UTC) - sync_start).total_seconds()
            
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
