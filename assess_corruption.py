#!/usr/bin/env python3
"""
Script to assess the extent of database corruption caused by the faulty HubSpot ID reconciliation.
This script will help identify which records were incorrectly updated.
"""

import os
import sys
import requests
from datetime import datetime, timezone as tz
import time
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mixrank_db import get_session
from models.hubspot_cache import HubSpotCache
from config import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CorruptionAssessment:
    def __init__(self):
        self.session = get_session()
        self.api_key = Config.HUBSPOT_API_KEY
        self.base_url = "https://api.hubapi.com"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Rate limiting
        self.max_requests_per_window = 5
        self.window_duration = 1.0  # 1 second
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
    
    def check_hubspot_id_validity(self, hubspot_ids, batch_size=100):
        """
        Check if HubSpot IDs in our cache are still valid in HubSpot.
        Returns dict mapping ID -> status ('valid', 'invalid', 'error')
        """
        id_status = {}
        
        for i in range(0, len(hubspot_ids), batch_size):
            batch = hubspot_ids[i:i + batch_size]
            logger.info(f"Checking batch {i//batch_size + 1}: {len(batch)} IDs")
            
            url = f"{self.base_url}/crm/v3/objects/companies/batch/read"
            payload = {
                "inputs": [{"id": str(obj_id)} for obj_id in batch],
                "properties": ["hs_object_id", "name", "domain"]
            }
            
            try:
                self._rate_limit_wait()
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
                response.raise_for_status()
                batch_response = response.json()
                
                # Track which IDs were returned
                returned_ids = {result['id'] for result in batch_response.get('results', [])}
                
                # Mark returned IDs as valid
                for result in batch_response.get('results', []):
                    id_status[result['id']] = 'valid'
                
                # Mark missing IDs as invalid (possibly merged or deleted)
                for requested_id in batch:
                    if requested_id not in returned_ids:
                        id_status[requested_id] = 'invalid'
                        
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to check batch: {e}")
                # Mark all IDs in this batch as error
                for obj_id in batch:
                    id_status[obj_id] = 'error'
                    
        return id_status
    
    def assess_corruption(self):
        """
        Main assessment function to identify potentially corrupted records.
        """
        logger.info("Starting corruption assessment...")
        
        # Get all records from cache
        all_records = self.session.query(HubSpotCache).all()
        logger.info(f"Found {len(all_records)} records in HubSpot cache")
        
        # Extract all HubSpot IDs
        hubspot_ids = [record.hubspot_object_id for record in all_records]
        
        # Check validity of all IDs
        logger.info("Checking HubSpot ID validity...")
        id_status = self.check_hubspot_id_validity(hubspot_ids)
        
        # Analyze results
        valid_count = sum(1 for status in id_status.values() if status == 'valid')
        invalid_count = sum(1 for status in id_status.values() if status == 'invalid')
        error_count = sum(1 for status in id_status.values() if status == 'error')
        
        logger.info(f"Assessment Results:")
        logger.info(f"  Valid IDs: {valid_count}")
        logger.info(f"  Invalid IDs: {invalid_count}")
        logger.info(f"  Error checking: {error_count}")
        
        # Identify potentially corrupted records
        corrupted_records = []
        for record in all_records:
            if id_status.get(record.hubspot_object_id) == 'invalid':
                corrupted_records.append(record)
        
        if corrupted_records:
            logger.warning(f"Found {len(corrupted_records)} potentially corrupted records:")
            for record in corrupted_records[:10]:  # Show first 10
                logger.warning(f"  ID: {record.hubspot_object_id}, Domain: {record.domain}, Company: {record.company_name}")
            
            if len(corrupted_records) > 10:
                logger.warning(f"  ... and {len(corrupted_records) - 10} more")
        
        return {
            'total_records': len(all_records),
            'valid_ids': valid_count,
            'invalid_ids': invalid_count,
            'error_ids': error_count,
            'corrupted_records': corrupted_records,
            'id_status': id_status
        }

def main():
    """Run the corruption assessment."""
    try:
        assessor = CorruptionAssessment()
        results = assessor.assess_corruption()
        
        # Save results for potential rollback
        timestamp = datetime.now(tz.utc).strftime("%Y%m%d_%H%M%S")
        results_file = f"corruption_assessment_{timestamp}.txt"
        
        with open(results_file, 'w') as f:
            f.write(f"HubSpot Cache Corruption Assessment - {datetime.now(tz.utc)}\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Total records in cache: {results['total_records']}\n")
            f.write(f"Valid HubSpot IDs: {results['valid_ids']}\n")
            f.write(f"Invalid HubSpot IDs: {results['invalid_ids']}\n")
            f.write(f"Errors checking IDs: {results['error_ids']}\n\n")
            
            if results['corrupted_records']:
                f.write("Potentially Corrupted Records:\n")
                f.write("-" * 40 + "\n")
                for record in results['corrupted_records']:
                    f.write(f"ID: {record.hubspot_object_id}\n")
                    f.write(f"Domain: {record.domain}\n")
                    f.write(f"Company: {record.company_name}\n")
                    f.write(f"Last Synced: {record.last_synced}\n")
                    f.write("\n")
        
        logger.info(f"Assessment complete. Results saved to: {results_file}")
        
        return results
        
    except Exception as e:
        logger.error(f"Assessment failed: {e}")
        return None

if __name__ == "__main__":
    main()
