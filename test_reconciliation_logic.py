#!/usr/bin/env python3
"""
CRITICAL TEST: Verify the IRONCLAD reconciliation logic before deployment.

This script tests the reconciliation logic with known scenarios:
1. Normal companies (no merge) - should NOT be updated
2. Known merged companies - should be updated correctly
3. Edge cases (404s, API errors, etc.)

MUST PASS ALL TESTS before re-enabling reconciliation in production.
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
from models.database import HubSpotCache, HubSpotEnrichment
from config import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ReconciliationTester:
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
    
    def test_individual_id_lookup(self, test_id):
        """
        Test the core logic: individual API call to detect merges.
        Returns (requested_id, returned_id, is_merge, company_info)
        """
        logger.info(f"Testing ID lookup: {test_id}")
        
        try:
            self._rate_limit_wait()
            url = f"{self.base_url}/crm/v3/objects/companies/{test_id}"
            params = {"properties": "hs_object_id,name,domain"}
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            
            if response.status_code == 404:
                return test_id, None, False, "Company not found (404)"
                
            response.raise_for_status()
            company_data = response.json()
            
            # CRITICAL LOGIC: Compare requested vs returned ID
            returned_id = company_data.get('id')
            is_merge = (test_id != returned_id)
            
            company_info = {
                'name': company_data.get('properties', {}).get('name', 'Unknown'),
                'domain': company_data.get('properties', {}).get('domain', 'Unknown'),
                'hs_object_id': company_data.get('properties', {}).get('hs_object_id')
            }
            
            logger.info(f"   Requested: {test_id}")
            logger.info(f"   Returned: {returned_id}")
            logger.info(f"   Merge detected: {is_merge}")
            logger.info(f"   Company: {company_info['name']} ({company_info['domain']})")
            
            return test_id, returned_id, is_merge, company_info
            
        except Exception as e:
            logger.error(f"   Error testing {test_id}: {e}")
            return test_id, None, False, f"Error: {e}"
    
    def test_known_scenarios(self):
        """
        Test with known scenarios to validate logic.
        """
        logger.info("=" * 60)
        logger.info("TESTING KNOWN SCENARIOS")
        logger.info("=" * 60)
        
        # Test cases - add known IDs from your database
        test_cases = []
        
        # Get some real IDs from cache for testing
        cache_samples = self.session.query(HubSpotCache).limit(5).all()
        
        for cache_record in cache_samples:
            test_id = cache_record.hubspot_object_id
            company_name = cache_record.company_name or "Unknown"
            domain = cache_record.domain or "Unknown"
            
            logger.info(f"\nTesting cache record: {company_name} ({domain})")
            requested_id, returned_id, is_merge, info = self.test_individual_id_lookup(test_id)
            
            if is_merge:
                logger.warning(f"🔄 MERGE DETECTED: {requested_id} → {returned_id}")
                logger.warning(f"   This record would be updated!")
            else:
                logger.info(f"✓ No merge: ID {test_id} is current")
        
        # Test the specific Salesforce example if it exists
        salesforce_old_id = "51934478246"
        salesforce_expected_new_id = "52511204275"
        
        logger.info(f"\n" + "=" * 40)
        logger.info("TESTING SALESFORCE EXAMPLE")
        logger.info("=" * 40)
        
        requested_id, returned_id, is_merge, info = self.test_individual_id_lookup(salesforce_old_id)
        
        if is_merge and returned_id == salesforce_expected_new_id:
            logger.info(f"✅ SALESFORCE TEST PASSED")
            logger.info(f"   {salesforce_old_id} → {returned_id} (expected {salesforce_expected_new_id})")
        elif not is_merge:
            logger.warning(f"⚠️  SALESFORCE TEST: No merge detected")
            logger.warning(f"   This might mean the old ID is no longer valid")
        else:
            logger.error(f"❌ SALESFORCE TEST FAILED")
            logger.error(f"   Expected: {salesforce_old_id} → {salesforce_expected_new_id}")
            logger.error(f"   Got: {salesforce_old_id} → {returned_id}")
    
    def test_edge_cases(self):
        """
        Test edge cases that could break the logic.
        """
        logger.info("\n" + "=" * 60)
        logger.info("TESTING EDGE CASES")
        logger.info("=" * 60)
        
        # Test invalid ID
        logger.info("\nTesting invalid ID...")
        requested_id, returned_id, is_merge, info = self.test_individual_id_lookup("99999999999")
        
        if returned_id is None:
            logger.info("✅ Invalid ID test passed - correctly handled 404")
        else:
            logger.error("❌ Invalid ID test failed - should have returned None")
    
    def simulate_reconciliation_dry_run(self):
        """
        Simulate what the reconciliation would do without making changes.
        """
        logger.info("\n" + "=" * 60)
        logger.info("SIMULATION: RECONCILIATION DRY RUN")
        logger.info("=" * 60)
        
        # Get first 10 cache records for simulation
        cache_records = self.session.query(HubSpotCache).limit(10).all()
        
        simulated_updates = 0
        simulated_enrichment_updates = 0
        
        for cache_record in cache_records:
            old_id = cache_record.hubspot_object_id
            
            # Test the lookup
            requested_id, returned_id, is_merge, info = self.test_individual_id_lookup(old_id)
            
            if is_merge:
                logger.info(f"🔄 WOULD UPDATE: {old_id} → {returned_id}")
                simulated_updates += 1
                
                # Check how many enrichments would be updated
                enrichment_count = self.session.query(HubSpotEnrichment).filter_by(
                    hubspot_object_id=old_id
                ).count()
                
                if enrichment_count > 0:
                    logger.info(f"   Would also update {enrichment_count} enrichment records")
                    simulated_enrichment_updates += enrichment_count
            else:
                logger.debug(f"✓ No change needed for {old_id}")
        
        logger.info(f"\n📊 SIMULATION SUMMARY:")
        logger.info(f"   Cache records that would be updated: {simulated_updates}")
        logger.info(f"   Enrichment records that would be updated: {simulated_enrichment_updates}")
        logger.info(f"   Total updates: {simulated_updates + simulated_enrichment_updates}")

def main():
    """Run comprehensive reconciliation logic tests."""
    logger.info("🧪 STARTING RECONCILIATION LOGIC TESTS")
    logger.info("This will test the logic WITHOUT making any database changes")
    
    try:
        tester = ReconciliationTester()
        
        # Run all tests
        tester.test_known_scenarios()
        tester.test_edge_cases() 
        tester.simulate_reconciliation_dry_run()
        
        logger.info("\n" + "🎯" * 20)
        logger.info("TESTING COMPLETE")
        logger.info("Review the results above to verify logic is correct.")
        logger.info("If all tests pass, the reconciliation logic is ready for deployment.")
        logger.info("🎯" * 20)
        
    except Exception as e:
        logger.error(f"Testing failed: {e}")
        return False

if __name__ == "__main__":
    main()
