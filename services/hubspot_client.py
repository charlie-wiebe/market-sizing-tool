import time
import requests
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from config import Config

logger = logging.getLogger(__name__)

class HubSpotClient:
    def __init__(self):
        self.base_url = Config.HUBSPOT_BASE_URL
        self.api_key = Config.HUBSPOT_API_KEY
        self.enabled = bool(self.api_key)  # Only enable if API key is present
        
        if self.enabled:
            self.headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
        else:
            self.headers = {}
            logger.warning("HubSpot API key not configured - HubSpot enrichment will be skipped")
        
        # Rate limiting: 100 requests per 10 seconds
        self.max_requests_per_window = Config.HUBSPOT_MAX_PER_10_SECONDS
        self.window_duration = 10.0  # seconds
        self.request_times = []
        self.timeout = 30

    def _rate_limit_wait(self):
        """Enforce rate limiting based on HubSpot's 100 requests per 10 seconds limit."""
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

    def _make_request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """Make a rate-limited request to HubSpot API."""
        self._rate_limit_wait()
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method.upper() == 'POST':
                response = requests.post(url, headers=self.headers, json=data, timeout=self.timeout)
            else:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot API request failed: {e}")
            return {"error": str(e)}

    def search_company_by_linkedin_handle(self, handle: str) -> List[Dict]:
        """Search HubSpot for a single LinkedIn handle. Returns list of matching records."""
        if not handle:
            return []
        
        search_request = {
            "filterGroups": [{
                "filters": [{"propertyName": "hs_linkedin_handle", "operator": "EQ", "value": handle}]
            }],
            "properties": ["hs_object_id", "domain", "hs_linkedin_handle", "vertical", "createdate"],
            "limit": 100
        }
        
        result = self._make_request("POST", "/crm/v3/objects/companies/search", search_request)
        return result.get("results", [])

    def search_company_by_domain(self, domain: str) -> List[Dict]:
        """Search HubSpot for a single domain. Returns list of matching records."""
        if not domain:
            return []
        
        search_request = {
            "filterGroups": [{
                "filters": [{"propertyName": "domain", "operator": "EQ", "value": domain}]
            }],
            "properties": ["hs_object_id", "domain", "hs_linkedin_handle", "vertical", "createdate"],
            "limit": 100
        }
        
        result = self._make_request("POST", "/crm/v3/objects/companies/search", search_request)
        return result.get("results", [])

    def resolve_duplicates(self, linkedin_results: List[Dict], domain_results: List[Dict], 
                          linkedin_handle: str, domain: str) -> Optional[Dict]:
        """
        Resolve duplicate HubSpot records using the specified logic:
        1. If results from both searches, prefer record matching both parameters
        2. If multiple records match both, choose older created date
        3. If no results from either search, return None
        
        Args:
            linkedin_results: Results from LinkedIn handle search
            domain_results: Results from domain search
            linkedin_handle: The LinkedIn handle we searched for
            domain: The domain we searched for
            
        Returns:
            Best matching HubSpot record or None
        """
        all_results = []
        
        # Tag results with their source
        for result in linkedin_results:
            result['_source'] = 'linkedin'
        for result in domain_results:
            result['_source'] = 'domain'
        
        # Find records that match both parameters
        both_matches = []
        for result in linkedin_results + domain_results:
            properties = result.get('properties', {})
            result_linkedin = properties.get('hs_linkedin_handle', '')
            result_domain = properties.get('domain', '')
            
            if (result_linkedin == linkedin_handle and result_domain == domain):
                both_matches.append(result)
        
        # If we have records matching both parameters, use those
        if both_matches:
            # If multiple matches, choose the one with older created date
            if len(both_matches) > 1:
                both_matches.sort(key=lambda x: x.get('properties', {}).get('createdate', '9999-12-31'))
            
            best_match = both_matches[0]
            best_match['_lookup_method'] = 'both_match'
            return best_match
        
        # Otherwise, use any single result we found
        if linkedin_results:
            best_match = linkedin_results[0]
            best_match['_lookup_method'] = 'linkedin_handle'
            return best_match
        
        if domain_results:
            best_match = domain_results[0]
            best_match['_lookup_method'] = 'domain'
            return best_match
        
        # No results found
        return None

    def batch_enrich_companies(self, companies: List[Dict]) -> Dict[int, Optional[Dict]]:
        """
        Enrich companies with HubSpot data, searching one at a time.
        For each company: search by LinkedIn handle first, then by domain.
        """
        if not self.enabled:
            logger.info("HubSpot enrichment skipped - API key not configured")
            return {company['id']: None for company in companies}
        
        from services.linkedin_utils import extract_linkedin_handle, normalize_domain
        
        enrichments = {}
        found_count = 0
        
        for company in companies:
            company_id = company['id']
            linkedin_handle = extract_linkedin_handle(company.get('linkedin_url'))
            domain = normalize_domain(company.get('domain'))
            
            # Search by LinkedIn handle
            linkedin_results = self.search_company_by_linkedin_handle(linkedin_handle)
            
            # Search by domain
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
        
        logger.info(f"Enriched {found_count} out of {len(companies)} companies")
        return enrichments
