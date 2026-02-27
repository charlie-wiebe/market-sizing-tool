import time
import requests
import logging
import json
from config import Config

class ProspeoClient:
    def __init__(self):
        self.base_url = Config.PROSPEO_BASE_URL
        self.api_key = Config.PROSPEO_API_KEY
        self.headers = {
            "X-KEY": self.api_key,
            "Content-Type": "application/json"
        }
        self.min_interval = max(
            1.0 / Config.PROSPEO_MAX_PER_SECOND,
            60.0 / Config.PROSPEO_MAX_PER_MINUTE
        )
        self._last_request_ts = 0.0
        self.timeout = 30
        self.logger = logging.getLogger(f"{__name__}.ProspeoClient")
        
        # Tracking metrics
        self._request_count = 0
        self._total_rate_limit_delay = 0.0
        self._total_companies_collected = 0
        self._total_people_collected = 0

    def _rate_limit_wait(self):
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < self.min_interval:
            delay_time = self.min_interval - elapsed
            self.logger.debug(f"Rate limit wait: {delay_time:.3f}s")
            self._total_rate_limit_delay += delay_time
            time.sleep(delay_time)
        self._last_request_ts = time.time()

    def _safe_json(self, response):
        try:
            return response.json()
        except Exception:
            return {
                "error": True,
                "error_code": "NON_JSON_RESPONSE",
                "raw": response.text[:2000]
            }

    def _post(self, path, payload):
        self._rate_limit_wait()
        self._request_count += 1
        
        url = f"{self.base_url}{path}"
        start_time = time.time()
        
        self.logger.info(f"Prospeo API Request #{self._request_count}: {path}")
        self.logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(
            url,
            headers=self.headers,
            json=payload,
            timeout=self.timeout
        )
        
        request_duration = time.time() - start_time
        data = self._safe_json(response)
        data["_http_status"] = response.status_code
        data["_request_duration"] = request_duration
        
        # Log response summary
        self.logger.info(f"Prospeo API Response #{self._request_count}: HTTP {response.status_code}, Duration: {request_duration:.3f}s")
        
        if "pagination" in data:
            pagination = data["pagination"]
            self.logger.info(f"Pagination: page {pagination.get('current_page', 'N/A')}/{pagination.get('total_page', 'N/A')}, "
                           f"total_count: {pagination.get('total_count', 'N/A')}, "
                           f"per_page: {pagination.get('per_page', 'N/A')}")
        
        if "error" in data and data["error"]:
            self.logger.warning(f"API Error: {data.get('error_code', 'Unknown')} - {data.get('filter_error', data.get('message', 'No details'))}")
        
        return data

    def search_companies(self, filters, page=1):
        payload = {
            "page": page,
            "filters": filters
        }
        return self._post("/search-company", payload)

    def search_people(self, filters, page=1):
        payload = {
            "page": page,
            "filters": filters
        }
        return self._post("/search-person", payload)

    def extract_companies(self, response):
        rows = response.get("results") or []
        companies = []
        for row in rows:
            if isinstance(row, dict) and isinstance(row.get("company"), dict):
                companies.append(row["company"])
            elif isinstance(row, dict):
                companies.append(row)
        
        self._total_companies_collected += len(companies)
        self.logger.debug(f"Extracted {len(companies)} companies from response. Total collected: {self._total_companies_collected}")
        return companies

    def extract_people(self, response):
        rows = response.get("results") or []
        people = []
        for row in rows:
            if isinstance(row, dict) and isinstance(row.get("person"), dict):
                people.append(row["person"])
            elif isinstance(row, dict):
                people.append(row)
        
        self._total_people_collected += len(people)
        self.logger.debug(f"Extracted {len(people)} people from response. Total collected: {self._total_people_collected}")
        return people
        
    def get_pagination(self, response):
        return response.get("pagination", {})
    
    def is_error(self, response):
        return response.get("error", False) or response.get("_http_status", 200) >= 400
    
    def get_error_code(self, response):
        return response.get("error_code", "UNKNOWN_ERROR")
    
    def get_tracking_stats(self):
        """Return tracking statistics for logging and debugging."""
        return {
            "total_requests": self._request_count,
            "total_rate_limit_delay": self._total_rate_limit_delay,
            "total_companies_collected": self._total_companies_collected,
            "total_people_collected": self._total_people_collected,
            "avg_request_delay": self._total_rate_limit_delay / max(1, self._request_count)
        }
    
    def reset_tracking_stats(self):
        """Reset tracking statistics (useful for per-job tracking)."""
        self._request_count = 0
        self._total_rate_limit_delay = 0.0
        self._total_companies_collected = 0
        self._total_people_collected = 0
    
    def build_exclusion_filters(self, existing_companies, base_filters):
        """Build exclusion filters to prevent collecting duplicate companies."""
        if not existing_companies:
            return base_filters
            
        # Extract company names and websites for exclusion
        exclusion_names = []
        exclusion_websites = []
        
        for company in existing_companies:
            if company.get('name'):
                exclusion_names.append(company['name'])
            if company.get('website'):
                exclusion_websites.append(company['website'])
            if company.get('domain'):
                exclusion_websites.append(company['domain'])
        
        # Remove duplicates and limit to 500 (Prospeo API limit)
        exclusion_names = list(set(exclusion_names))[:500]
        exclusion_websites = list(set(exclusion_websites))[:500]
        
        # Create modified filters with exclusions
        modified_filters = dict(base_filters)
        
        if exclusion_names:
            if "company" not in modified_filters:
                modified_filters["company"] = {}
            if "names" not in modified_filters["company"]:
                modified_filters["company"]["names"] = {}
            if "exclude" not in modified_filters["company"]["names"]:
                modified_filters["company"]["names"]["exclude"] = []
            
            modified_filters["company"]["names"]["exclude"].extend(exclusion_names)
            self.logger.info(f"Added {len(exclusion_names)} company names to exclusion filter")
        
        if exclusion_websites:
            if "company" not in modified_filters:
                modified_filters["company"] = {}
            if "websites" not in modified_filters["company"]:
                modified_filters["company"]["websites"] = {}
            if "exclude" not in modified_filters["company"]["websites"]:
                modified_filters["company"]["websites"]["exclude"] = []
            
            modified_filters["company"]["websites"]["exclude"].extend(exclusion_websites)
            self.logger.info(f"Added {len(exclusion_websites)} company websites to exclusion filter")
        
        return modified_filters
    
    def search_suggestions(self, location=None, job_title=None):
        payload = {}
        if location is not None:
            payload["location_search"] = location
        elif job_title is not None:
            payload["job_title_search"] = job_title
        else:
            return {"error": True, "error_code": "MISSING_PARAM"}
        return self._post("/search-suggestions", payload)
