#!/usr/bin/env python3
"""
Test script to verify aggregate person search matches per-company counts.

CORRECT TEST: Use the SAME 10 company domains in both approaches:
1. Aggregate: Person search with company.websites filter for 10 domains → total_count
2. Per-company: Sum of person searches for each of those 10 companies
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("PROSPEO_API_KEY")
BASE_URL = "https://api.prospeo.io"
HEADERS = {
    "X-KEY": API_KEY,
    "Content-Type": "application/json"
}

def post_json(endpoint, payload):
    """Make API request with rate limiting."""
    time.sleep(0.05)  # Rate limit
    url = f"{BASE_URL}{endpoint}"
    response = requests.post(url, headers=HEADERS, json=payload)
    return response.json()

def extract_root_domain(domain):
    """Extract root domain from URL/domain."""
    if not domain:
        return None
    import tldextract
    ext = tldextract.extract(domain)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}"
    return None

# Define test filters
COMPANY_FILTERS = {
    "company_location_search": {"include": ["United States"], "exclude": []},
    "company_attributes": {"b2b": True},
    "company_headcount_range": ["201-500"],
    "company_headcount_by_department": [{
        "department": "Sales Development",
        "min": 2,
        "max": 100000
    }]
}

PERSON_FILTERS = {
    "person_department": {"include": ["Sales Development"], "exclude": []},
    "person_seniority": {"include": ["Entry", "Senior", "Intern"], "exclude": []}
}

print("=" * 60)
print("TEST: Aggregate vs Per-Company (Same 10 Companies)")
print("=" * 60)

# Step 1: Get first 10 companies and extract their domains
print("\n1. Getting first 10 companies...")
company_response = post_json("/search-company", {"filters": COMPANY_FILTERS, "page": 1})

if company_response.get("error"):
    print(f"ERROR: {company_response}")
    exit(1)

companies_raw = company_response.get("results", [])
companies = [c.get("company", c) for c in companies_raw]

# Extract root domains for first 10 companies
test_domains = []
test_companies = []
for company in companies[:10]:
    name = company.get("name", "Unknown")
    domain = company.get("domain") or company.get("website") or ""
    root = extract_root_domain(domain)
    if root:
        test_domains.append(root)
        test_companies.append({"name": name, "domain": root})
        print(f"   [{len(test_domains)}] {name} → {root}")

print(f"\n   Collected {len(test_domains)} valid domains")

# Step 2: AGGREGATE APPROACH - single search with all 10 domains
print("\n2. AGGREGATE APPROACH: Single person search with 10 company domains...")
aggregate_filters = dict(PERSON_FILTERS)
aggregate_filters["company"] = {"websites": {"include": test_domains, "exclude": []}}

print(f"   Filter: company.websites.include = {test_domains}")

aggregate_response = post_json("/search-person", {"filters": aggregate_filters, "page": 1})

if aggregate_response.get("error"):
    print(f"   ERROR: {aggregate_response}")
    aggregate_total = "ERROR"
else:
    aggregate_total = aggregate_response.get("pagination", {}).get("total_count", 0)
    print(f"   Aggregate total_count: {aggregate_total}")

# Step 3: PER-COMPANY APPROACH - search each company individually
print("\n3. PER-COMPANY APPROACH: Searching each company individually...")
per_company_total = 0
per_company_details = []

for i, company in enumerate(test_companies):
    name = company["name"]
    domain = company["domain"]
    
    filters = dict(PERSON_FILTERS)
    filters["company"] = {"websites": {"include": [domain], "exclude": []}}
    
    response = post_json("/search-person", {"filters": filters, "page": 1})
    
    if response.get("error"):
        count = 0
        error = response.get("error_code", "unknown")
        print(f"   [{i+1}] {name} ({domain}): ERROR - {error}")
    else:
        count = response.get("pagination", {}).get("total_count", 0)
        print(f"   [{i+1}] {name} ({domain}): {count} people")
    
    per_company_total += count
    per_company_details.append({"name": name, "domain": domain, "count": count})

# Step 4: Compare results
print("\n" + "=" * 60)
print("RESULTS COMPARISON (Same 10 Companies)")
print("=" * 60)
print(f"Aggregate approach (1 query with 10 domains):  {aggregate_total}")
print(f"Per-company sum (10 individual queries):       {per_company_total}")
print(f"Companies tested: {len(per_company_details)}")

if isinstance(aggregate_total, int):
    if aggregate_total == per_company_total:
        print("\n✅ PASS: Counts match exactly!")
        print("   The aggregate approach returns the same results as summing individual queries.")
    else:
        diff = abs(aggregate_total - per_company_total)
        print(f"\n⚠️  MISMATCH: Difference of {diff}")
        if aggregate_total > per_company_total:
            print("   Aggregate is higher - some people may work at multiple test companies")
        else:
            print("   Per-company is higher - unexpected, investigate")

print(f"\nCredits used:")
print(f"   Company search: 1")
print(f"   Aggregate person search: 1")
print(f"   Per-company searches: {len(per_company_details)}")
print(f"   TOTAL: {2 + len(per_company_details)}")
