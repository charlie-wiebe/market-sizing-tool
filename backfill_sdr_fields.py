#!/usr/bin/env python3
"""
Backfill SDR count fields for existing companies in HubSpot cache.
Only fetches and updates the 6 SDR fields, leaves other data untouched.
Usage: python backfill_sdr_fields.py [--batch-size 100] [--start-from-id 12345]
"""

import sys
import os
import logging
import argparse
import time
from datetime import datetime, UTC
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_database_url, Config
from models.database import HubSpotCache

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SDRBackfiller:
    """Backfill SDR count fields for existing HubSpot cache records."""
    
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
        
        # Rate limiting: 5 requests per second for search endpoints
        self.max_requests_per_window = 5
        self.window_duration = 1.0
        self.request_times = []
    
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
    
    def get_companies_to_backfill(self, start_from_id=None, batch_size=100):
        """Get companies from cache that need SDR field backfill."""
        query = self.session.query(HubSpotCache).filter(
            HubSpotCache.hubspot_object_id.isnot(None)
        )
        
        if start_from_id:
            query = query.filter(HubSpotCache.id >= start_from_id)
        
        # Order by ID for consistent pagination
        query = query.order_by(HubSpotCache.id)
        
        if batch_size:
            query = query.limit(batch_size)
        
        return query.all()
    
    def fetch_sdr_fields_batch(self, hubspot_object_ids):
        """Fetch SDR fields for multiple companies using HubSpot batch read endpoint.
        
        HubSpot batch read can handle up to 100 companies per request.
        """
        self._rate_limit_wait()
        
        url = f"{self.base_url}/crm/v3/objects/companies/batch/read"
        payload = {
            "inputs": [{"id": str(obj_id)} for obj_id in hubspot_object_ids],
            "properties": [
                "aip___of_sdrs",
                "manual_override_____sdrs", 
                "mixrank_____sdrs",
                "keyplay___sdrs_bdrs",
                "clay_estimated___sdrs",
                "estimated___sdrs"
            ]
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Batch API request failed for {len(hubspot_object_ids)} companies: {e}")
            return None
    
    def parse_int(self, value):
        """Parse integer safely, handling None/empty values."""
        if value is None or value == "":
            return None
        try:
            return int(float(str(value)))
        except (ValueError, TypeError):
            return None
    
    def update_sdr_fields(self, cache_record, hubspot_data):
        """Update only SDR fields for a cache record."""
        if not hubspot_data or 'properties' not in hubspot_data:
            return False
        
        properties = hubspot_data['properties']
        
        # Update only SDR fields
        cache_record.aip_sdrs = self.parse_int(properties.get("aip___of_sdrs"))
        cache_record.override_sdrs = self.parse_int(properties.get("manual_override_____sdrs"))
        cache_record.mixrank_sdrs = self.parse_int(properties.get("mixrank_____sdrs"))
        cache_record.keyplay_sdrs = self.parse_int(properties.get("keyplay___sdrs_bdrs"))
        cache_record.clay_sdrs = self.parse_int(properties.get("clay_estimated___sdrs"))
        cache_record.final_sdrs = self.parse_int(properties.get("estimated___sdrs"))
        cache_record.last_synced = datetime.now(UTC)
        
        return True
    
    def backfill_batch(self, start_from_id=None, batch_size=100):
        """Backfill SDR fields for a batch of companies."""
        companies = self.get_companies_to_backfill(start_from_id, batch_size)
        
        if not companies:
            logger.info("No more companies to backfill")
            return None
        
        logger.info(f"Processing batch of {len(companies)} companies (starting from ID {companies[0].id})")
        
        updated_count = 0
        error_count = 0
        
        # Batch fetch from HubSpot - up to 100 companies per API call
        hubspot_ids = [company.hubspot_object_id for company in companies]
        
        try:
            logger.info(f"Making batch API call for {len(hubspot_ids)} companies")
            batch_response = self.fetch_sdr_fields_batch(hubspot_ids)
            
            if not batch_response or 'results' not in batch_response:
                logger.error("Batch API call failed or returned invalid response")
                error_count = len(companies)
            else:
                # Create lookup map of HubSpot ID to company data
                hubspot_data_map = {}
                for result in batch_response['results']:
                    hubspot_data_map[result['id']] = result
                
                # Update each company with batch results
                for company in companies:
                    try:
                        logger.info(f"Processing company ID {company.id} (HubSpot ID: {company.hubspot_object_id})")
                        
                        hubspot_data = hubspot_data_map.get(company.hubspot_object_id)
                        if hubspot_data:
                            if self.update_sdr_fields(company, hubspot_data):
                                updated_count += 1
                            else:
                                error_count += 1
                                logger.warning(f"Failed to update company ID {company.id}")
                        else:
                            error_count += 1
                            logger.warning(f"No data returned for company ID {company.id} from batch response")
                        
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error processing company ID {company.id}: {e}")
                        
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            error_count = len(companies)
        
        # Commit batch updates
        try:
            self.session.commit()
            logger.info(f"Batch complete: {updated_count} updated, {error_count} errors")
        except Exception as e:
            logger.error(f"Failed to commit batch: {e}")
            self.session.rollback()
        
        # Return the last processed ID for continuation
        return companies[-1].id if companies else None
    
    def run_full_backfill(self, start_from_id=None, batch_size=100):
        """Run complete backfill process."""
        logger.info("Starting SDR fields backfill...")
        
        total_updated = 0
        total_errors = 0
        current_id = start_from_id
        
        while True:
            last_id = self.backfill_batch(current_id, batch_size)
            
            if last_id is None:
                break
            
            current_id = last_id + 1
            
            # Progress update
            batch_stats = self.session.execute(
                text("SELECT COUNT(*) as total, MAX(id) as max_id FROM hubspot_company_cache WHERE last_synced IS NOT NULL")
            ).fetchone()
            
            if batch_stats:
                logger.info(f"Progress: Last processed ID {last_id}, Total records with data: {batch_stats[0]}")
        
        logger.info("SDR fields backfill complete!")

def main():
    parser = argparse.ArgumentParser(description='Backfill SDR count fields for HubSpot cache')
    parser.add_argument('--batch-size', type=int, default=100, 
                       help='Number of companies to process per batch (default: 100)')
    parser.add_argument('--start-from-id', type=int, default=None,
                       help='Start backfill from specific cache record ID')
    
    args = parser.parse_args()
    
    try:
        backfiller = SDRBackfiller()
        backfiller.run_full_backfill(args.start_from_id, args.batch_size)
    except KeyboardInterrupt:
        logger.info("Backfill interrupted by user")
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
