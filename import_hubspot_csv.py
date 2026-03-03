#!/usr/bin/env python3
"""
Import HubSpot company data from CSV into cache table.
Usage: python import_hubspot_csv.py <csv_file_path>
"""

import sys
import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_database_url
from services.linkedin_utils import extract_linkedin_handle

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_hubspot_date(date_str):
    """Parse HubSpot date strings to datetime."""
    if pd.isna(date_str) or not date_str:
        return None
    try:
        # Try different date formats HubSpot might use
        for fmt in ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%Y-%m-%d']:
            try:
                return datetime.strptime(str(date_str).split('.')[0], fmt)
            except ValueError:
                continue
        return None
    except:
        return None

def clean_domain(domain):
    """Clean and normalize domain."""
    if pd.isna(domain) or not domain:
        return None
    domain = str(domain).strip().lower()
    # Remove common prefixes
    domain = domain.replace('http://', '').replace('https://', '').replace('www.', '')
    # Remove trailing slashes and paths
    domain = domain.split('/')[0]
    return domain if domain else None

def import_hubspot_csv(csv_path):
    """Import HubSpot CSV data into cache table."""
    logger.info(f"Reading CSV file: {csv_path}")
    
    try:
        # Read CSV - be flexible with column names
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {len(df)} rows from CSV")
        
        # Log available columns
        logger.info(f"Available columns: {list(df.columns)}")
        
        # Map columns (case-insensitive)
        column_mapping = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'record id' in col_lower or 'object id' in col_lower:
                column_mapping['hubspot_object_id'] = col
            elif 'domain' in col_lower and 'company' in col_lower:
                column_mapping['domain'] = col
            elif 'linkedin' in col_lower and 'page' in col_lower:
                column_mapping['linkedin_url'] = col
            elif col_lower == 'vertical':
                column_mapping['vertical'] = col
            elif 'name' in col_lower and 'company' not in col_lower:
                column_mapping['company_name'] = col
            elif 'create date' in col_lower or 'created' in col_lower:
                column_mapping['hubspot_created_date'] = col
        
        logger.info(f"Column mapping: {column_mapping}")
        
        # Create clean dataframe with required columns
        clean_df = pd.DataFrame()
        
        # Map hubspot_object_id
        if 'hubspot_object_id' in column_mapping:
            clean_df['hubspot_object_id'] = df[column_mapping['hubspot_object_id']].astype(str)
        else:
            logger.error("No Record ID column found!")
            return
        
        # Map other fields with defaults
        clean_df['domain'] = df[column_mapping.get('domain', '')].apply(clean_domain) if 'domain' in column_mapping else None
        clean_df['company_name'] = df[column_mapping.get('company_name', '')] if 'company_name' in column_mapping else None
        clean_df['vertical'] = df[column_mapping.get('vertical', '')] if 'vertical' in column_mapping else None
        
        # Extract LinkedIn handle from URL
        if 'linkedin_url' in column_mapping:
            clean_df['linkedin_handle'] = df[column_mapping['linkedin_url']].apply(
                lambda x: extract_linkedin_handle(x) if pd.notna(x) else None
            )
        else:
            clean_df['linkedin_handle'] = None
        
        # Parse dates
        if 'hubspot_created_date' in column_mapping:
            clean_df['hubspot_created_date'] = df[column_mapping['hubspot_created_date']].apply(parse_hubspot_date)
        else:
            clean_df['hubspot_created_date'] = None
        
        # Remove rows without hubspot_object_id
        clean_df = clean_df[clean_df['hubspot_object_id'].notna()]
        clean_df = clean_df[clean_df['hubspot_object_id'] != '']
        
        logger.info(f"Cleaned data: {len(clean_df)} valid rows")
        
        # Add timestamp
        clean_df['last_synced'] = datetime.utcnow()
        
        # Connect to database
        engine = create_engine(get_database_url())
        
        # Import in chunks to avoid memory issues
        chunk_size = 1000
        total_imported = 0
        
        for i in range(0, len(clean_df), chunk_size):
            chunk = clean_df.iloc[i:i+chunk_size]
            try:
                # Use 'replace' to update existing records
                chunk.to_sql('hubspot_company_cache', engine, if_exists='append', index=False, method='multi')
                total_imported += len(chunk)
                logger.info(f"Imported {total_imported}/{len(clean_df)} records...")
            except Exception as e:
                # Try one by one if bulk fails (duplicate key handling)
                logger.warning(f"Bulk insert failed, trying individually: {e}")
                for _, row in chunk.iterrows():
                    try:
                        row.to_frame().T.to_sql('hubspot_company_cache', engine, if_exists='append', index=False)
                        total_imported += 1
                    except Exception as row_error:
                        if 'duplicate key' in str(row_error).lower():
                            logger.debug(f"Skipping duplicate: {row['hubspot_object_id']}")
                        else:
                            logger.error(f"Failed to import row {row['hubspot_object_id']}: {row_error}")
        
        logger.info(f"✅ Import complete! Imported {total_imported} companies to cache.")
        
        # Show sample of imported data
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM hubspot_company_cache")
            count = result.scalar()
            logger.info(f"Total records in cache: {count}")
        
    except Exception as e:
        logger.error(f"Import failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_hubspot_csv.py <csv_file_path>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)
    
    import_hubspot_csv(csv_path)
