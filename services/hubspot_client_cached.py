"""
HubSpot client with local cache support for ultra-fast lookups.
"""
import logging
from datetime import datetime
from typing import List, Dict, Optional
from models.database import HubSpotCache
from services.linkedin_utils import extract_linkedin_handle, normalize_domain
from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HubSpotClientCached:
    """HubSpot client that uses local cache first, with API fallback."""
    
    def __init__(self, session=None):
        """Initialize the cached client."""
        self.base_url = None  # Not needed for cache-only mode
        self.api_key = None  # Not needed for cache-only mode
        self.enabled = True  # Always enabled in cache mode
        self.headers = {}
        self.timeout = 30
        self.session = session
        
        # Check if cache has data if session provided
        if self.session:
            cache_count = self.session.query(HubSpotCache).count()
            if cache_count == 0:
                logger.warning("HubSpot cache is empty. Run import_hubspot_csv.py first!")
            else:
                logger.info(f"HubSpot cache initialized with {cache_count} companies")
    
    def search_company_by_linkedin_handle(self, handle: str) -> List[Dict]:
        """Search cache for a LinkedIn handle. Returns list of matching records."""
        if not handle or not self.session:
            return []
        
        matches = self.session.query(HubSpotCache).filter_by(linkedin_handle=handle).all()
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
        if not domain or not self.session:
            return []
        
        # Normalize the domain
        domain = normalize_domain(domain)
        if not domain:
            return []
        
        # Search for domain in both primary domain and additional domains
        from sqlalchemy import or_
        matches = self.session.query(HubSpotCache).filter(
            or_(
                HubSpotCache.domain == domain,
                # Check if domain appears in semicolon-separated list
                HubSpotCache.hs_additional_domains.like(f'%;{domain};%'),  # Middle
                HubSpotCache.hs_additional_domains.like(f'{domain};%'),     # Start
                HubSpotCache.hs_additional_domains.like(f'%;{domain}'),     # End
                HubSpotCache.hs_additional_domains == domain                # Only value
            )
        ).all()
        
        results = []
        for match in matches:
            results.append({
                'id': match.hubspot_object_id,
                'properties': {
                    'hs_object_id': match.hubspot_object_id,
                    'domain': match.domain,
                    'hs_additional_domains': match.hs_additional_domains,
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
            
            # Search cache by LinkedIn handle
            linkedin_results = self.search_company_by_linkedin_handle(linkedin_handle)
            
            # Search cache by domain waterfall (website -> domain -> other_websites)
            domain_results = []
            domains_to_try = []
            
            # Build domain waterfall priority order
            if company.get('website'):
                domains_to_try.append(normalize_domain(company.get('website')))
            if company.get('domain'):
                domains_to_try.append(normalize_domain(company.get('domain')))
            if company.get('other_websites'):
                try:
                    import json
                    other_sites = json.loads(company.get('other_websites')) if isinstance(company.get('other_websites'), str) else company.get('other_websites')
                    if isinstance(other_sites, list):
                        for site in other_sites:
                            if site:
                                domains_to_try.append(normalize_domain(site))
                except (json.JSONDecodeError, TypeError):
                    pass
            
            # Try each domain in waterfall order until we find matches
            for domain in domains_to_try:
                if domain:
                    results = self.search_company_by_domain(domain)
                    if results:
                        domain_results = results
                        break  # Use first domain that has results
            
            # Resolve best match
            best_match = self.resolve_duplicates(linkedin_results, domain_results,
                                               linkedin_handle, domains_to_try[0] if domains_to_try else None)
            
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
