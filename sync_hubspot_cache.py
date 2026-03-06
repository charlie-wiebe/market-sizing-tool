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
        """
        IRONCLAD reconciliation of HubSpot IDs for merged companies.
        
        Uses individual API calls for guaranteed correct merge detection:
        - Request old_id → returns record with current_id 
        - If old_id != current_id → merge detected
        - Updates BOTH cache AND enrichment records atomically
        """
        from models.database import HubSpotEnrichment
        
        logger.info("Starting HubSpot ID reconciliation for merged companies...")
        
        # Get all unique HubSpot IDs from cache
        cache_records = self.session.query(HubSpotCache).all()
        if not cache_records:
            logger.info("No records in cache to reconcile")
            return 0
        
        total_records = len(cache_records)
        updated_cache_count = 0
        updated_enrichment_count = 0
        
        logger.info(f"Reconciling {total_records} cached HubSpot IDs using individual API calls...")
        
        # Process each record individually for GUARANTEED correct mapping
        for i, cache_record in enumerate(cache_records, 1):
            old_id = cache_record.hubspot_object_id
            
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{total_records} records processed")
            
            try:
                # Make individual API call to get current record
                self._rate_limit_wait()
                url = f"{self.base_url}/crm/v3/objects/companies/{old_id}"
                params = {"properties": "hs_object_id,name,domain"}
                
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                
                if response.status_code == 404:
                    logger.warning(f"ID {old_id} not found - company may be deleted")
                    continue
                    
                response.raise_for_status()
                company_data = response.json()
                
                # CRITICAL: Compare requested ID vs returned ID
                current_id = company_data.get('id')
                if not current_id:
                    logger.warning(f"No ID returned for {old_id}")
                    continue
                
                # MERGE DETECTION: requested_id != returned_id means merge occurred
                if old_id != current_id:
                    company_name = company_data.get('properties', {}).get('name', 'Unknown')
                    domain = company_data.get('properties', {}).get('domain', 'Unknown')
                    
                    logger.info(f"🔄 MERGE DETECTED: {old_id} → {current_id} ({company_name}, {domain})")
                    
                    # ATOMIC UPDATE: Both cache and enrichment records in single transaction
                    try:
                        # Update cache record
                        cache_record.hubspot_object_id = current_id
                        updated_cache_count += 1
                        
                        # Update ALL enrichment records with the old ID  
                        enrichment_records = self.session.query(HubSpotEnrichment).filter_by(
                            hubspot_object_id=old_id
                        ).all()
                        
                        for enrichment in enrichment_records:
                            logger.info(f"   📝 Updating enrichment company_id={enrichment.company_id}: {old_id} → {current_id}")
                            enrichment.hubspot_object_id = current_id
                            updated_enrichment_count += 1
                        
                        # Commit this specific update
                        self.session.commit()
                        logger.info(f"   ✅ Updated cache + {len(enrichment_records)} enrichment records")
                        
                    except Exception as e:
                        logger.error(f"   ❌ Failed to update records for {old_id} → {current_id}: {e}")
                        self.session.rollback()
                        
                else:
                    # No merge - ID is still current
                    logger.debug(f"✓ ID {old_id} is current")
                    
            except requests.exceptions.RequestException as e:
                if "404" not in str(e):
                    logger.error(f"Failed to check ID {old_id}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing ID {old_id}: {e}")
                continue
        
        # Final summary
        logger.info(f"✅ ID reconciliation complete:")
        logger.info(f"   Cache records updated: {updated_cache_count}")
        logger.info(f"   Enrichment records updated: {updated_enrichment_count}")
        logger.info(f"   Total records processed: {total_records}")
        
        return updated_cache_count + updated_enrichment_count
    
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
            
            # Step 2: Reconcile HubSpot IDs for merged companies - FIXED WITH IRONCLAD LOGIC
            logger.info("Reconciling HubSpot IDs for merged companies...")
            reconciled_count = self.reconcile_hubspot_ids()
            if reconciled_count > 0:
                logger.info(f"Reconciled {reconciled_count} total records (cache + enrichments) from merged companies")
            else:
                logger.info("No merged companies detected - all IDs are current")
            
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
