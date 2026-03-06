#!/usr/bin/env python3
"""
Script to rollback the HubSpot ID corruption caused by the faulty reconciliation logic.

This script will:
1. Identify records that were corrupted by the faulty reconciliation
2. Attempt to find the correct HubSpot IDs for corrupted records
3. Restore the correct IDs where possible
4. Generate a report of what was fixed vs. what couldn't be fixed
"""

import os
import sys
import requests
from datetime import datetime, timezone as tz
import time
import logging
import json
from collections import defaultdict

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

class HubSpotCorruptionRollback:
    def __init__(self, dry_run=True):
        self.session = get_session()
        self.api_key = Config.HUBSPOT_API_KEY
        self.base_url = "https://api.hubapi.com"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.dry_run = dry_run
        
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
    
    def find_correct_hubspot_id_by_domain(self, domain, company_name=None):
        """
        Attempt to find the correct HubSpot ID for a company by searching by domain.
        """
        if not domain:
            return None
            
        # Search for company by domain
        url = f"{self.base_url}/crm/v3/objects/companies/search"
        
        # Build search criteria
        filter_groups = [{
            "filters": [{
                "propertyName": "domain",
                "operator": "EQ",
                "value": domain
            }]
        }]
        
        # If we have company name, add it as additional criteria
        if company_name:
            filter_groups.append({
                "filters": [{
                    "propertyName": "name",
                    "operator": "CONTAINS_TOKEN",
                    "value": company_name
                }]
            })
        
        payload = {
            "filterGroups": filter_groups,
            "properties": ["hs_object_id", "name", "domain"],
            "limit": 10  # Get up to 10 matches
        }
        
        try:
            self._rate_limit_wait()
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            search_results = response.json()
            
            results = search_results.get('results', [])
            if results:
                # If multiple results, try to find best match
                if len(results) == 1:
                    return results[0]['id']
                else:
                    # Multiple results - try to find best match by name
                    if company_name:
                        for result in results:
                            result_name = result.get('properties', {}).get('name', '').lower()
                            if company_name.lower() in result_name or result_name in company_name.lower():
                                return result['id']
                    
                    # If no good name match, return the first result
                    logger.warning(f"Multiple matches for domain {domain}, using first result")
                    return results[0]['id']
            
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to search for domain {domain}: {e}")
            return None
    
    def identify_corrupted_records(self):
        """
        Identify records that were likely corrupted by the faulty reconciliation.
        
        This is tricky since we don't have a backup of the original IDs.
        We'll use heuristics:
        1. Records with invalid HubSpot IDs
        2. Records where we can find a different valid ID for the same domain
        """
        logger.info("Identifying corrupted records...")
        
        all_records = self.session.query(HubSpotCache).all()
        logger.info(f"Checking {len(all_records)} cache records")
        
        # Group records by domain to identify potential duplicates/conflicts
        domain_groups = defaultdict(list)
        for record in all_records:
            if record.domain:
                domain_groups[record.domain].append(record)
        
        corrupted_records = []
        
        # Check each domain group
        for domain, records in domain_groups.items():
            if len(records) > 1:
                logger.info(f"Domain {domain} has {len(records)} records - checking for conflicts")
                
                # Check if all records have the same HubSpot ID
                hubspot_ids = {record.hubspot_object_id for record in records}
                if len(hubspot_ids) > 1:
                    logger.warning(f"Domain {domain} has conflicting HubSpot IDs: {hubspot_ids}")
                    
                    # Try to find the correct ID for this domain
                    correct_id = self.find_correct_hubspot_id_by_domain(domain)
                    if correct_id:
                        # Mark records with incorrect IDs as corrupted
                        for record in records:
                            if record.hubspot_object_id != correct_id:
                                corrupted_records.append({
                                    'record': record,
                                    'correct_id': correct_id,
                                    'reason': 'domain_conflict'
                                })
                                logger.info(f"Marked as corrupted: {record.hubspot_object_id} -> {correct_id} (domain: {domain})")
        
        return corrupted_records
    
    def perform_rollback(self, corrupted_records):
        """
        Perform the actual rollback of corrupted records.
        """
        if not corrupted_records:
            logger.info("No corrupted records to rollback")
            return
        
        logger.info(f"{'DRY RUN: ' if self.dry_run else ''}Rolling back {len(corrupted_records)} corrupted records")
        
        fixed_count = 0
        failed_count = 0
        
        for corruption in corrupted_records:
            record = corruption['record']
            correct_id = corruption['correct_id']
            reason = corruption['reason']
            
            try:
                old_id = record.hubspot_object_id
                
                if self.dry_run:
                    logger.info(f"DRY RUN: Would update {old_id} -> {correct_id} (domain: {record.domain}, reason: {reason})")
                else:
                    record.hubspot_object_id = correct_id
                    logger.info(f"Updated {old_id} -> {correct_id} (domain: {record.domain}, reason: {reason})")
                
                fixed_count += 1
                
            except Exception as e:
                logger.error(f"Failed to update record {record.hubspot_object_id}: {e}")
                failed_count += 1
        
        if not self.dry_run and fixed_count > 0:
            try:
                self.session.commit()
                logger.info(f"✅ Rollback complete: {fixed_count} records fixed")
            except Exception as e:
                logger.error(f"Failed to commit rollback changes: {e}")
                self.session.rollback()
                failed_count += fixed_count
                fixed_count = 0
        
        return {
            'fixed_count': fixed_count,
            'failed_count': failed_count
        }
    
    def generate_rollback_report(self, corrupted_records, results):
        """
        Generate a detailed rollback report.
        """
        timestamp = datetime.now(tz.utc).strftime("%Y%m%d_%H%M%S")
        report_file = f"rollback_report_{timestamp}.txt"
        
        with open(report_file, 'w') as f:
            f.write(f"HubSpot Corruption Rollback Report - {datetime.now(tz.utc)}\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE ROLLBACK'}\n")
            f.write(f"Total corrupted records found: {len(corrupted_records)}\n")
            f.write(f"Records fixed: {results['fixed_count']}\n")
            f.write(f"Records failed: {results['failed_count']}\n\n")
            
            if corrupted_records:
                f.write("Corrupted Records Details:\n")
                f.write("-" * 40 + "\n")
                for corruption in corrupted_records:
                    record = corruption['record']
                    f.write(f"Domain: {record.domain}\n")
                    f.write(f"Company: {record.company_name}\n")
                    f.write(f"Old ID: {record.hubspot_object_id}\n")
                    f.write(f"Correct ID: {corruption['correct_id']}\n")
                    f.write(f"Reason: {corruption['reason']}\n")
                    f.write(f"Last Synced: {record.last_synced}\n")
                    f.write("\n")
        
        logger.info(f"Rollback report saved to: {report_file}")
        return report_file

def main():
    """Run the rollback process."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Rollback HubSpot ID corruption')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Run in dry-run mode (default: True)')
    parser.add_argument('--execute', action='store_true',
                        help='Execute the rollback (overrides dry-run)')
    
    args = parser.parse_args()
    
    # If --execute is specified, turn off dry-run
    dry_run = args.dry_run and not args.execute
    
    if not dry_run:
        confirm = input("This will modify the database. Are you sure? (type 'yes' to confirm): ")
        if confirm.lower() != 'yes':
            logger.info("Rollback cancelled by user")
            return
    
    try:
        rollback = HubSpotCorruptionRollback(dry_run=dry_run)
        
        # Identify corrupted records
        corrupted_records = rollback.identify_corrupted_records()
        
        if not corrupted_records:
            logger.info("No corrupted records found - nothing to rollback")
            return
        
        # Perform rollback
        results = rollback.perform_rollback(corrupted_records)
        
        # Generate report
        report_file = rollback.generate_rollback_report(corrupted_records, results)
        
        logger.info("Rollback process complete")
        logger.info(f"Report saved to: {report_file}")
        
        if dry_run:
            logger.info("This was a DRY RUN - no changes were made")
            logger.info("To execute the rollback, run with --execute flag")
        
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        return None

if __name__ == "__main__":
    main()
