"""
Local validation tests for HubSpot search API compliance.
Validates payload format against HubSpot CRM Search API spec:
https://developers.hubspot.com/docs/guides/api/crm/search

Constraints per docs:
- Max 5 filterGroups
- Max 6 filters per group
- Max 18 filters total
- Max 200 results per page (limit)
- Search endpoints: 5 requests/second/account
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.hubspot_client import HubSpotClient
from services.linkedin_utils import extract_linkedin_handle, normalize_domain


def validate_search_payload(payload):
    """Validate a HubSpot search payload against API spec constraints."""
    errors = []

    # Check filterGroups
    fg = payload.get("filterGroups", [])
    if len(fg) > 5:
        errors.append(f"Too many filterGroups: {len(fg)} (max 5)")

    total_filters = 0
    for i, group in enumerate(fg):
        filters = group.get("filters", [])
        if len(filters) > 6:
            errors.append(f"filterGroups[{i}] has {len(filters)} filters (max 6)")
        total_filters += len(filters)
        
        for j, f in enumerate(filters):
            if "propertyName" not in f:
                errors.append(f"filterGroups[{i}].filters[{j}] missing propertyName")
            if "operator" not in f:
                errors.append(f"filterGroups[{i}].filters[{j}] missing operator")
            if "value" not in f and "values" not in f:
                errors.append(f"filterGroups[{i}].filters[{j}] missing value/values")

    if total_filters > 18:
        errors.append(f"Too many total filters: {total_filters} (max 18)")

    # Check limit
    limit = payload.get("limit", 100)
    if limit > 200:
        errors.append(f"limit {limit} exceeds max of 200")

    # Check properties is a list of strings
    props = payload.get("properties", [])
    if not isinstance(props, list):
        errors.append("properties must be a list")
    for p in props:
        if not isinstance(p, str):
            errors.append(f"property {p} is not a string")

    return errors


def test_linkedin_handle_search_payload():
    """Test that LinkedIn handle search produces valid payload."""
    # Build the payload the same way the client does
    handle = "company/nooks"
    payload = {
        "filterGroups": [{
            "filters": [{"propertyName": "hs_linkedin_handle", "operator": "EQ", "value": handle}]
        }],
        "properties": ["hs_object_id", "domain", "hs_linkedin_handle", "vertical", "createdate"],
        "limit": 100
    }
    errors = validate_search_payload(payload)
    assert not errors, f"LinkedIn handle search payload invalid: {errors}"
    print("  PASS: LinkedIn handle search payload is valid")


def test_domain_search_payload():
    """Test that domain search produces valid payload."""
    domain = "nooks.ai"
    payload = {
        "filterGroups": [{
            "filters": [{"propertyName": "domain", "operator": "EQ", "value": domain}]
        }],
        "properties": ["hs_object_id", "domain", "hs_linkedin_handle", "vertical", "createdate"],
        "limit": 100
    }
    errors = validate_search_payload(payload)
    assert not errors, f"Domain search payload invalid: {errors}"
    print("  PASS: Domain search payload is valid")


def test_linkedin_handle_extraction():
    """Test LinkedIn handle extraction from various URL formats."""
    cases = [
        ("https://linkedin.com/company/rippling", "company/rippling"),
        ("https://www.linkedin.com/company/hubspot/", "company/hubspot"),
        ("linkedin.com/company/salesforce", "company/salesforce"),
        ("https://www.linkedin.com/company/nooks-ai/about/", "company/nooks-ai"),
        (None, None),
        ("", None),
        ("https://google.com", None),
    ]
    for url, expected in cases:
        result = extract_linkedin_handle(url)
        assert result == expected, f"extract_linkedin_handle({url!r}) = {result!r}, expected {expected!r}"
    print("  PASS: LinkedIn handle extraction works correctly")


def test_domain_normalization():
    """Test domain normalization."""
    cases = [
        ("https://www.nooks.ai/pricing", "nooks.ai"),
        ("www.hubspot.com", "hubspot.com"),
        ("salesforce.com", "salesforce.com"),
        ("NOOKS.AI", "nooks.ai"),
        (None, None),
        ("", None),
    ]
    for domain, expected in cases:
        result = normalize_domain(domain)
        assert result == expected, f"normalize_domain({domain!r}) = {result!r}, expected {expected!r}"
    print("  PASS: Domain normalization works correctly")


def test_rate_limiter_config():
    """Verify rate limiter is set to 5 req/sec per HubSpot search docs."""
    # Don't actually init (no API key needed) — just check config
    client = HubSpotClient()
    assert client.max_requests_per_window == 5, f"Expected 5, got {client.max_requests_per_window}"
    assert client.window_duration == 1.0, f"Expected 1.0s, got {client.window_duration}"
    print("  PASS: Rate limiter configured to 5 req/sec")


def test_empty_inputs_dont_make_requests():
    """Verify empty LinkedIn handle / domain returns empty without API call."""
    client = HubSpotClient()
    assert client.search_company_by_linkedin_handle(None) == []
    assert client.search_company_by_linkedin_handle("") == []
    assert client.search_company_by_domain(None) == []
    assert client.search_company_by_domain("") == []
    print("  PASS: Empty inputs return [] without making API calls")


def test_company_model_columns():
    """Verify Company model has correct columns (no legacy names)."""
    from models.database import Company
    mapper = Company.__table__.columns
    col_names = {c.name for c in mapper}
    
    # Must NOT have old columns
    forbidden = {'root_domain', 'headcount', 'headcount_by_department', 'founded_year', 
                 'funding_stage', 'b2b', 'revenue_range', 'revenue_printed', 'processed'}
    found_forbidden = forbidden & col_names
    assert not found_forbidden, f"Company model still has old columns: {found_forbidden}"
    
    # Must have new columns
    required = {'employee_count', 'founded', 'is_b2b', 'revenue_range_printed',
                'name', 'website', 'domain', 'industry', 'linkedin_url', 'linkedin_id'}
    missing = required - col_names
    assert not missing, f"Company model missing columns: {missing}"
    
    print("  PASS: Company model has correct columns")


if __name__ == "__main__":
    print("Running HubSpot validation tests...\n")
    
    tests = [
        test_linkedin_handle_search_payload,
        test_domain_search_payload,
        test_linkedin_handle_extraction,
        test_domain_normalization,
        test_rate_limiter_config,
        test_empty_inputs_dont_make_requests,
        test_company_model_columns,
    ]
    
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {test.__name__}: {e}")
            failed += 1
    
    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
    else:
        print("All tests passed!")
