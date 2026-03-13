#!/usr/bin/env python3
"""
One-time backfill: Fix NULL hubspot_created_date in hubspot_company_cache.
Uses HubSpot batch read API to fetch createdAt for affected records.

Usage: python3 backfill_cache_created_dates.py
       python3 backfill_cache_created_dates.py --dry-run
"""
import sys
import os
import logging
import argparse
import requests
from datetime import datetime, UTC
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_database_url, Config
from models.database import HubSpotCache

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Backfill NULL hubspot_created_date in cache')
    parser.add_argument('--dry-run', action='store_true', help='Show counts without updating')
    args = parser.parse_args()

    engine = create_engine(get_database_url())
    Session = sessionmaker(bind=engine)
    session = Session()

    api_key = Config.HUBSPOT_API_KEY
    if not api_key:
        logger.error("HUBSPOT_API_KEY not configured")
        return

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        # Get all cache records with NULL hubspot_created_date
        null_records = session.query(HubSpotCache).filter(
            HubSpotCache.hubspot_created_date == None
        ).all()

        logger.info(f"Found {len(null_records)} records with NULL hubspot_created_date")

        if args.dry_run:
            logger.info("DRY RUN — no changes will be made")
            return

        if not null_records:
            logger.info("Nothing to backfill")
            return

        # Process in batches of 100 (HubSpot batch read limit)
        batch_size = 100
        total_updated = 0

        for i in range(0, len(null_records), batch_size):
            batch = null_records[i:i + batch_size]
            batch_ids = [r.hubspot_object_id for r in batch]

            # Use batch read API
            url = "https://api.hubapi.com/crm/v3/objects/companies/batch/read"
            body = {
                "properties": ["createdate"],
                "inputs": [{"id": hs_id} for hs_id in batch_ids]
            }

            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            data = response.json()

            # Build lookup: hubspot_id -> createdAt
            id_to_date = {}
            for result in data.get("results", []):
                hs_id = result["id"]
                # Use top-level createdAt (ISO string)
                created_at_str = result.get("createdAt")
                if created_at_str:
                    try:
                        created_date = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).replace(tzinfo=None)
                        id_to_date[hs_id] = created_date
                    except (ValueError, TypeError):
                        pass

            # Update records
            batch_updated = 0
            for record in batch:
                if record.hubspot_object_id in id_to_date:
                    record.hubspot_created_date = id_to_date[record.hubspot_object_id]
                    batch_updated += 1

            session.commit()
            session.expire_all()
            total_updated += batch_updated

            batch_num = i // batch_size + 1
            total_batches = (len(null_records) + batch_size - 1) // batch_size
            logger.info(f"  Batch {batch_num}/{total_batches}: updated {batch_updated}/{len(batch)} records ({total_updated} total)")

        logger.info(f"✅ Backfill complete: {total_updated} records updated out of {len(null_records)} NULL records")

    finally:
        session.close()


if __name__ == "__main__":
    main()
