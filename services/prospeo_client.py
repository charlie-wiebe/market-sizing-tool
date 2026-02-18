import time
import requests
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

    def _rate_limit_wait(self):
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
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
        url = f"{self.base_url}{path}"
        response = requests.post(
            url,
            headers=self.headers,
            json=payload,
            timeout=self.timeout
        )
        data = self._safe_json(response)
        data["_http_status"] = response.status_code
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
        return companies

    def extract_people(self, response):
        rows = response.get("results") or []
        people = []
        for row in rows:
            if isinstance(row, dict) and isinstance(row.get("person"), dict):
                people.append(row["person"])
            elif isinstance(row, dict):
                people.append(row)
        return people

    def get_pagination(self, response):
        pagination = response.get("pagination") or {}
        return {
            "current_page": pagination.get("current_page", 1),
            "per_page": pagination.get("per_page", 25),
            "total_page": pagination.get("total_page", 0),
            "total_count": pagination.get("total_count", 0)
        }

    def search_suggestions(self, location=None, job_title=None):
        payload = {}
        if location is not None:
            payload["location_search"] = location
        elif job_title is not None:
            payload["job_title_search"] = job_title
        else:
            return {"error": True, "error_code": "MISSING_PARAM"}
        return self._post("/search-suggestions", payload)

    def is_error(self, response):
        return (
            response.get("_http_status", 200) >= 400 or
            response.get("error") is True
        )

    def get_error_code(self, response):
        return response.get("error_code", "UNKNOWN_ERROR")
