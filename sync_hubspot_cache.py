#!/usr/bin/env python3
"""
Sync HubSpot cache with latest data from HubSpot API.
Tracks new companies by creation date and removes archived companies.
Usage: python sync_hubspot_cache.py
"""

import sys
import os
import logging
from datetime import datetime, UTC
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_database_url, Config
from models.database import HubSpotCache, SyncMetadata, Company, HubSpotEnrichment, db
from services.hubspot_client_cached import HubSpotClientCached

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
        
        # Rate limiting setup
        import time
        self.request_times = []
        self.max_requests = 5  # 5 requests per second
        self.window_duration = 1.0  # 1 second window
        
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
        
        sync_record.last_sync_timestamp = datetime.now(UTC)
        sync_record.last_sync_status = status
        sync_record.records_added = records_added
        sync_record.records_removed = records_removed
        sync_record.error_message = error_message
        sync_record.updated_at = datetime.now(UTC)
        
        self.session.commit()
    
    def _rate_limit_wait(self):
        """Enforce rate limiting based on HubSpot's 5 requests per second limit."""
        import time
        now = time.time()
        
        # Remove requests older than the window
        self.request_times = [t for t in self.request_times if now - t < self.window_duration]
        
        # If we're at the limit, wait until we can make another request
        if len(self.request_times) >= self.max_requests:
            sleep_time = self.window_duration - (now - self.request_times[0])
            if sleep_time > 0:
                logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        # Record this request
        self.request_times.append(time.time())
    
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
                    
                # Create lookup map of old ID -> new ID
                id_updates = {}
                for result in batch_response['results']:
                    current_id = result['id']
                    stored_id = result.get('properties', {}).get('hs_object_id')
                    if stored_id and stored_id != current_id:
                        id_updates[current_id] = stored_id
                        
                # Update cache records that have ID mismatches
                for record in batch:
                    new_id = id_updates.get(record.hubspot_object_id)
                    if new_id:
                        old_id = record.hubspot_object_id  # CRITICAL: Store old_id BEFORE updating
                        logger.info(f"🔄 MERGE DETECTED: {old_id} → {new_id}")
                        
                        # Get HubSpot company data for the surviving company
                        company_data = next((item for item in batch_response['results'] if item['id'] == new_id), None)
                        hubspot_company_name = company_data.get('properties', {}).get('name', '') if company_data else ''
                        hubspot_company_domain = company_data.get('properties', {}).get('domain', '').lower() if company_data else ''
                        
                        # Update cache record
                        record.hubspot_object_id = new_id
                        updated_count += 1
                        
                        # Handle ALL enrichment records with the OLD ID
                        from models.database import HubSpotEnrichment, Company
                        enrichment_records = self.session.query(HubSpotEnrichment).filter_by(
                            hubspot_object_id=old_id  # Use OLD ID before we changed it
                        ).all()
                        
                        cache_updates = 0
                        inactive_marks = 0
                        for enrichment in enrichment_records:
                            try:
                                enrichment_company = self.session.query(Company).filter_by(id=enrichment.company_id).first()
                                enrichment_domain = enrichment_company.domain.lower() if enrichment_company and enrichment_company.domain else ''
                                
                                # Check if enrichment company domain matches HubSpot surviving company domain
                                if enrichment_domain and hubspot_company_domain and enrichment_domain == hubspot_company_domain:
                                    # Same domain = company survived, update HubSpot ID
                                    logger.info(f"   ✅ Survivor company: updating {enrichment_company.name} ({enrichment_domain}) HubSpot ID: {old_id} → {new_id}")
                                    enrichment.hubspot_object_id = new_id
                                    cache_updates += 1
                                else:
                                    # Different domain = company was merged away, mark inactive
                                    logger.info(f"   ❌ Merged-away company: marking inactive {enrichment_company.name if enrichment_company else 'Unknown'} ({enrichment_domain}) - merged into {hubspot_company_name} ({hubspot_company_domain})")
                                    enrichment.is_active = False
                                    # Keep old HubSpot ID for audit trail
                                    inactive_marks += 1
                                    
                            except Exception as domain_check_error:
                                logger.warning(f"   ⚠️  Could not process enrichment {enrichment.id}: {domain_check_error}")
                                # Default to marking inactive on errors to be safe
                                enrichment.is_active = False
                                inactive_marks += 1
                                
                        logger.info(f"   📊 Updated cache + {cache_updates} enrichment records, marked {inactive_marks} as inactive")
                        
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
    
    def enrich_unenriched_companies(self):
        """Check all companies without active HubSpot enrichments against the cache.
        
        Called automatically after cache sync so that newly-added cache entries
        can match previously un-enrichable companies.
        
        Uses chunked queries to avoid memory exhaustion on small Render instances.
        """
        logger.info("Checking for un-enriched companies that now match cache...")
        
        # COUNT first — never load all into memory
        unenriched_count = self.session.query(Company)\
            .outerjoin(HubSpotEnrichment,
                       (Company.id == HubSpotEnrichment.company_id) &
                       (HubSpotEnrichment.is_active == True))\
            .filter(HubSpotEnrichment.id == None)\
            .count()
        
        if unenriched_count == 0:
            logger.info("All companies already have active HubSpot enrichments")
            return 0
        
        logger.info(f"Found {unenriched_count} companies without active enrichments")
        
        hubspot_client = HubSpotClientCached(session=self.session)
        
        total_created = 0
        batch_size = 100
        batch_num = 0
        
        # Process in chunks — re-query each batch to keep memory flat
        while True:
            batch = self.session.query(Company)\
                .outerjoin(HubSpotEnrichment,
                           (Company.id == HubSpotEnrichment.company_id) &
                           (HubSpotEnrichment.is_active == True))\
                .filter(HubSpotEnrichment.id == None)\
                .order_by(Company.id)\
                .limit(batch_size)\
                .all()
            
            if not batch:
                break
            
            batch_num += 1
            
            batch_data = []
            for company in batch:
                batch_data.append({
                    'id': company.id,
                    'linkedin_url': company.linkedin_url,
                    'domain': company.domain,
                    'website': company.website,
                    'other_websites': company.other_websites
                })
            
            enrichments = hubspot_client.batch_enrich_companies(batch_data)
            
            batch_created = 0
            for company_id, enrichment_data in enrichments.items():
                if enrichment_data:
                    company = next(c for c in batch if c.id == company_id)
                    new_enrichment = HubSpotEnrichment(
                        company_id=company_id,
                        job_id=company.job_id,
                        hubspot_object_id=enrichment_data['hubspot_object_id'],
                        vertical=enrichment_data.get('vertical'),
                        lookup_method=enrichment_data.get('lookup_method'),
                        hubspot_created_date=enrichment_data.get('hubspot_created_date'),
                        is_active=True
                    )
                    self.session.add(new_enrichment)
                    batch_created += 1
            
            total_created += batch_created
            
            # Commit and free memory after each batch
            self.session.commit()
            self.session.expire_all()
            
            logger.info(f"  Batch {batch_num}: {batch_created} enrichments from {len(batch)} companies ({total_created} total)")
        
        logger.info(f"✅ Auto-enrichment complete: {total_created} new enrichments created from {unenriched_count} un-enriched companies")
        return total_created

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
            
            # Step 2: Reconcile HubSpot IDs for merged companies
            logger.info("Reconciling HubSpot IDs for merged companies...")
            reconciled_count = self.reconcile_hubspot_ids()
            if reconciled_count > 0:
                logger.info(f"Reconciled {reconciled_count} HubSpot IDs from merged companies")
            
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
            
            # Step 4: Auto-enrich companies that now match cache
            try:
                new_enrichments = self.enrich_unenriched_companies()
                logger.info(f"Auto-enrichment: {new_enrichments} new enrichments created")
            except Exception as e:
                logger.warning(f"Auto-enrichment failed (non-fatal): {e}")
            
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
