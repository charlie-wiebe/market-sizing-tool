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
        
        # Location format caching for Search Suggestions API
        self._location_format_cache = {}
        self._current_per_second = None

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
            data = response.json()
            # Ensure we always return a dictionary object
            if not isinstance(data, dict):
                return {
                    "error": True,
                    "error_code": "INVALID_JSON_FORMAT",
                    "raw": str(data)[:2000]
                }
            return data
        except Exception:
            # Safely handle response.text that might be None or non-string
            raw_text = ""
            try:
                if hasattr(response, 'text') and response.text is not None:
                    raw_text = str(response.text)[:2000]
                elif hasattr(response, 'content'):
                    raw_text = str(response.content)[:2000]
            except Exception:
                raw_text = "Unable to extract response text"
            
            return {
                "error": True,
                "error_code": "NON_JSON_RESPONSE",
                "raw": raw_text
            }

    def _post(self, path, payload, retry_count=0):
        """Enhanced _post with 429 handling and dynamic rate limiting"""
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
        
        # Handle 429 rate limit exceeded
        if response.status_code == 429:
            if retry_count < 3:  # Max 3 retries
                try:
                    retry_after = int(response.headers.get('retry-after', 60))
                    # Validate retry_after is reasonable (1-300 seconds)
                    retry_after = max(1, min(retry_after, 300))
                except (ValueError, TypeError):
                    retry_after = 60  # Safe fallback
                
                self.logger.warning(f"Rate limit exceeded, waiting {retry_after}s (retry {retry_count + 1})")
                time.sleep(retry_after)
                return self._post(path, payload, retry_count + 1)
            else:
                self.logger.error("Max retries reached for rate limit")
                data = self._safe_json(response)
                data["_http_status"] = response.status_code
                data["error"] = True
                data["error_code"] = "RATE_LIMIT_EXCEEDED"
                return data
        
        # Extract and update rate limits from response headers
        self._update_rate_limits_from_headers(response.headers)
        
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
    
    
    def search_suggestions(self, location=None, job_title=None):
        payload = {}
        if location is not None:
            payload["location_search"] = location
        elif job_title is not None:
            payload["job_title_search"] = job_title
        else:
            return {"error": True, "error_code": "MISSING_PARAM"}
        return self._post("/search-suggestions", payload)
    
    def resolve_location_format(self, location_name):
        """Use Search Suggestions API to get proper location format with caching"""
        if not location_name:
            return location_name
            
        # Check cache first
        if location_name in self._location_format_cache:
            return self._location_format_cache[location_name]
        
        self.logger.debug(f"Resolving location format for: {location_name}")
        response = self.search_suggestions(location=location_name)
        resolved = location_name  # Default fallback
        
        if not self.is_error(response):
            suggestions = response.get("location_suggestions", [])
            # Return first COUNTRY match with proper format
            for suggestion in suggestions:
                if suggestion.get("type") == "COUNTRY":
                    resolved = suggestion.get("name")
                    self.logger.debug(f"Resolved '{location_name}' -> '{resolved}'")
                    break
        else:
            self.logger.warning(f"Failed to resolve location format for '{location_name}': {self.get_error_code(response)}")
        
        # Cache the result
        self._location_format_cache[location_name] = resolved
        return resolved
    
    def _update_rate_limits_from_headers(self, headers):
        """Update rate limits based on actual API response headers"""
        if 'x-second-rate-limit' in headers:
            try:
                actual_per_second = int(headers['x-second-rate-limit'])
                # Validate rate limit is reasonable (1-1000 requests/second)
                if 1 <= actual_per_second <= 1000:
                    if actual_per_second != self._current_per_second:
                        self._current_per_second = actual_per_second
                        self.min_interval = 1.0 / actual_per_second
                        self.logger.info(f"Updated rate limit: {actual_per_second}/second")
                else:
                    self.logger.warning(f"Ignoring invalid rate limit from headers: {actual_per_second}")
            except (ValueError, TypeError, ZeroDivisionError):
                self.logger.warning(f"Failed to parse rate limit header: {headers.get('x-second-rate-limit')}")
        
        # Log current rate limit status for debugging
        if 'x-minute-request-left' in headers:
            minute_left = headers['x-minute-request-left']
            self.logger.debug(f"Rate limit status - minute requests left: {minute_left}")
