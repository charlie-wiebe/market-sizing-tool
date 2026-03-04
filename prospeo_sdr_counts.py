#!/usr/bin/env python3
import os
import sys
import time
import json
from urllib.parse import urlparse
import requests

BASE_URL = "https://api.prospeo.io"
TIMEOUT_S = 30

API_KEY = os.getenv("PROSPEO_API_KEY") or "pk_16327cda95b2b61b874c245d47f0d8a1c9bc452661098baf4d481b1198d401c4"

HEADERS = {
    "X-KEY": API_KEY,
    "Content-Type": "application/json",
}

# Rate limits
MAX_PER_SECOND = 30
MAX_PER_MINUTE = 1800
MIN_INTERVAL = max(1.0 / MAX_PER_SECOND, 60.0 / MAX_PER_MINUTE)
_last_request_ts = 0.0


def rate_limit_wait():
    global _last_request_ts
    now = time.time()
    elapsed = now - _last_request_ts
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_request_ts = time.time()


def safe_json(r):
    try:
        return r.json()
    except Exception:
        return {"error": True, "error_code": "NON_JSON_RESPONSE", "raw": r.text[:2000]}


def post_json(path, payload):
    rate_limit_wait()
    url = f"{BASE_URL}{path}"
    r = requests.post(url, headers=HEADERS, json=payload, timeout=TIMEOUT_S)
    data = safe_json(r)
    data["_http_status"] = r.status_code
    return data


# --- Domain Handling ---

MULTI_PART_SUFFIXES = {
    "co.uk", "org.uk", "ac.uk",
    "com.au", "net.au", "org.au",
    "co.nz", "org.nz",
    "co.jp", "ne.jp", "or.jp",
    "co.in", "firm.in", "net.in", "org.in",
}


def hostname_from_anything(s):
    s = (s or "").strip()
    if not s:
        return ""
    if "://" in s:
        host = urlparse(s).netloc.lower()
    else:
        host = s.lower()
    host = host.split("/")[0].rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def registrable_root_domain(s):
    host = hostname_from_anything(s)
    if not host:
        return ""

    parts = [p for p in host.split(".") if p]
    if len(parts) <= 2:
        return host

    last2 = ".".join(parts[-2:])
    last3 = ".".join(parts[-3:])

    if last2 in MULTI_PART_SUFFIXES:
        return last3

    return last2


# --- API Logic ---

def search_companies():
    payload = {
        "page": 1,
        "filters": {
            "company_attributes": {
                "b2b": True
            },
            "company_headcount_by_department": [
                {
                    "department": "Sales Development",
                    "min": 3,
                    "max": 100000
                }
            ]
        }
    }
    return post_json("/search-company", payload)


def extract_companies(resp):
    rows = resp.get("results") or []
    companies = []
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("company"), dict):
            companies.append(row["company"])
        elif isinstance(row, dict):
            companies.append(row)
    return companies


def get_search_domains_priority_order(company):
    """
    Get domains to try for person search in evidence-based priority order.
    Same logic as services/domain_utils.py for consistency.
    """
    domains_to_try = []
    
    # Priority 1: website domain (empirically most accurate - 100% success rate)
    website = company.get("website")
    if website:
        website_domain = registrable_root_domain(website)
        if website_domain:
            domains_to_try.append(website_domain)
    
    # Priority 2: domain field (parent/canonical - broader fallback)  
    domain = company.get("domain")
    if domain:
        if domain not in domains_to_try:
            domains_to_try.append(domain)
    
    # Priority 3: other_websites (often redirects/invalid - last resort)
    other_websites = company.get("other_websites")
    if other_websites and isinstance(other_websites, (list, tuple)):
        for site in other_websites:
            if site:
                root_domain = registrable_root_domain(site)
                if root_domain and root_domain not in domains_to_try:
                    domains_to_try.append(root_domain)
                    
    return domains_to_try

def search_people_for_company(root_domain):
    payload = {
        "page": 1,
        "filters": {
            "person_department": {
                "include": ["Sales Development"],
                "exclude": []
            },
            "person_seniority": {
                "include": ["Entry", "Senior"],
                "exclude": []
            },
            "person_time_in_current_role": {
                "min": 1,
                "max": 60
            },
            "person_location_search": {
                "include": ["United States #US"],
                "exclude": []
            },
            "company": {
                "websites": {
                    "include": [root_domain],
                    "exclude": []
                }
            }
        }
    }

    resp = post_json("/search-person", payload)

    if resp.get("_http_status", 200) >= 400 or resp.get("error"):
        return "not found", resp.get("error_code", "error")

    pagination = resp.get("pagination") or {}
    total = pagination.get("total_count")

    if isinstance(total, int):
        return total, "ok"

    return len(resp.get("results") or []), "ok"


def main():
    company_resp = search_companies()

    if company_resp.get("_http_status", 200) >= 400 or company_resp.get("error"):
        print(json.dumps([{
            "company": "not found",
            "company_id": "not found",
            "website": "not found",
            "root_domain_used": "not found",
            "sales_dev_count_entry_or_senior": "not found",
            "status": "company search failed"
        }], indent=2))
        return

    companies = extract_companies(company_resp)[:25]

    output = []

    for c in companies:
        name = c.get("name") or "not found"
        company_id = c.get("company_id") or "not found"
        website = c.get("website") or "not found"

        # Get domains to try in evidence-based priority order
        domains_to_try = get_search_domains_priority_order(c)

        if not domains_to_try:
            output.append({
                "company": name,
                "company_id": company_id,
                "website": website,
                "root_domain_used": "not found",
                "sales_dev_count_entry_or_senior": "not found",
                "status": "no domain"
            })
            continue

        # Try domains in evidence-based priority order (website → domain → other_websites)
        count = "not found"
        status = "error"
        successful_domain = None

        for i, domain_root in enumerate(domains_to_try):
            domain_source = "website" if i == 0 else "domain" if i == 1 else "other_websites"
            print(f"Trying {domain_source} for {name}: {domain_root}", file=sys.stderr)
            
            count, status = search_people_for_company(domain_root)
            
            if status == "ok" and isinstance(count, int) and count > 0:
                successful_domain = domain_root
                print(f"Success with {domain_source} for {name}: {count}", file=sys.stderr)
                break
            else:
                print(f"No results with {domain_source} for {name}: {count}", file=sys.stderr)

        output.append({
            "company": name,
            "company_id": company_id,
            "website": website,
            "root_domain_used": successful_domain or domains_to_try[0] if domains_to_try else "not found",
            "sales_dev_count_entry_or_senior": count,
            "status": status
        })

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()