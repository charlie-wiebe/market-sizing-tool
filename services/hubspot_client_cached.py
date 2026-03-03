"""
HubSpot client with local cache support for ultra-fast lookups.
"""
import logging
from typing import List, Dict, Optional
from datetime import datetime
from services.linkedin_utils import extract_linkedin_handle, normalize_domain
from models.database import HubSpotCache, db

logger = logging.getLogger(__name__)

class HubSpotClientCached:
    """HubSpot client that uses local cache first, with API fallback."""
    
    def __init__(self):
        self.base_url = None  # Not needed for cache-only mode
        self.api_key = None  # Not needed for cache-only mode
        self.enabled = True  # Always enabled in cache mode
        self.headers = {}
        self.timeout = 30
        
        # Check if cache has data
        cache_count = HubSpotCache.query.count()
        if cache_count == 0:
            logger.warning("HubSpot cache is empty. Run import_hubspot_csv.py first!")
        else:
            logger.info(f"HubSpot cache initialized with {cache_count} companies")
    
    def search_company_by_linkedin_handle(self, handle: str) -> List[Dict]:
        """Search cache for a LinkedIn handle. Returns list of matching records."""
        if not handle:
            return []
        
        matches = HubSpotCache.query.filter_by(linkedin_handle=handle).all()
        results = []
        for match in matches:
            results.append({
                'id': match.hubspot_object_id,
                'properties': {
                    'hs_object_id': match.hubspot_object_id,
                    'domain': match.domain,
                    'hs_linkedin_handle': match.linkedin_handle,
                    'vertical': match.vertical,
                    'createdate': int(match.hubspot_created_date.timestamp() * 1000) if match.hubspot_created_date else None
                }
            })
        return results
    
    def search_company_by_domain(self, domain: str) -> List[Dict]:
        """Search cache for a domain. Returns list of matching records."""
        if not domain:
            return []
        
        # Normalize the domain
        domain = normalize_domain(domain)
        if not domain:
            return []
        
        matches = HubSpotCache.query.filter_by(domain=domain).all()
        results = []
        for match in matches:
            results.append({
                'id': match.hubspot_object_id,
                'properties': {
                    'hs_object_id': match.hubspot_object_id,
                    'domain': match.domain,
                    'hs_linkedin_handle': match.linkedin_handle,
                    'vertical': match.vertical,
                    'createdate': int(match.hubspot_created_date.timestamp() * 1000) if match.hubspot_created_date else None
                }
            })
        return results
    
    def resolve_duplicates(self, linkedin_results: List[Dict], domain_results: List[Dict], 
                          linkedin_handle: str, domain: str) -> Optional[Dict]:
        """
        Resolve duplicate records using the same logic as original HubSpot client.
        """
        # If no results from either search, return None
        if not linkedin_results and not domain_results:
            return None
        
        # If only LinkedIn results, return first one
        if linkedin_results and not domain_results:
            best_match = linkedin_results[0]
            best_match['_lookup_method'] = 'linkedin_handle'
            return best_match
        
        # If only domain results, return first one
        if domain_results and not linkedin_results:
            best_match = domain_results[0]
            best_match['_lookup_method'] = 'domain'
            return best_match
        
        # Both searches returned results - find best match
        # First, look for records that match BOTH parameters
        linkedin_ids = {r.get('id') for r in linkedin_results}
        domain_ids = {r.get('id') for r in domain_results}
        both_match_ids = linkedin_ids.intersection(domain_ids)
        
        if both_match_ids:
            # Get all records that match both
            both_match_records = [r for r in linkedin_results if r.get('id') in both_match_ids]
            
            # If multiple match both, choose the oldest (earliest created date)
            if len(both_match_records) > 1:
                both_match_records.sort(key=lambda x: x.get('properties', {}).get('createdate', 0))
            
            best_match = both_match_records[0]
            best_match['_lookup_method'] = 'both_match'
            return best_match
        
        # No records match both - prefer LinkedIn match
        if linkedin_results:
            best_match = linkedin_results[0]
            best_match['_lookup_method'] = 'linkedin_handle'
            return best_match
        
        # Fall back to domain match
        if domain_results:
            best_match = domain_results[0]
            best_match['_lookup_method'] = 'domain'
            return best_match
        
        # No results found
        return None
    
    def batch_enrich_companies(self, companies: List[Dict]) -> Dict[int, Optional[Dict]]:
        """
        Enrich companies with HubSpot data from cache.
        Process all companies at once since we're just doing DB lookups.
        """
        if not self.enabled:
            logger.info("HubSpot enrichment skipped - not enabled")
            return {company['id']: None for company in companies}
        
        enrichments = {}
        found_count = 0
        
        for company in companies:
            company_id = company['id']
            linkedin_handle = extract_linkedin_handle(company.get('linkedin_url'))
            domain = normalize_domain(company.get('domain'))
            
            # Search cache by LinkedIn handle
            linkedin_results = self.search_company_by_linkedin_handle(linkedin_handle)
            
            # Search cache by domain
            domain_results = self.search_company_by_domain(domain)
            
            # Resolve best match
            best_match = self.resolve_duplicates(linkedin_results, domain_results,
                                               linkedin_handle, domain)
            
            if best_match:
                properties = best_match.get('properties', {})
                
                hubspot_created_date = None
                if properties.get('createdate'):
                    try:
                        timestamp_ms = int(properties['createdate'])
                        hubspot_created_date = datetime.fromtimestamp(timestamp_ms / 1000)
                    except (ValueError, TypeError):
                        pass
                
                enrichments[company_id] = {
                    'hubspot_object_id': best_match.get('id'),
                    'vertical': properties.get('vertical'),
                    'lookup_method': best_match.get('_lookup_method'),
                    'hubspot_created_date': hubspot_created_date
                }
                found_count += 1
            else:
                enrichments[company_id] = None
        
        logger.info(f"Enriched {found_count} out of {len(companies)} companies from cache")
        return enrichments
