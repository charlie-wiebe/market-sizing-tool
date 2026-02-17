# AGENTS.md

## Prospeo API Integration -- Full Technical Reference

This document summarizes:

-   How the Prospeo API works
-   Credit model and rate limits
-   Domain handling requirements
-   Full workflow logic implemented
-   Reference to the working Python implementation
-   Architectural guidance for production app build (Windsurf)

------------------------------------------------------------------------

# 1. Authentication

All requests require:

Headers: - X-KEY: `<your_api_key>`{=html} - Content-Type:
application/json

Base URL: https://api.prospeo.io

------------------------------------------------------------------------

# 2. Credit Model

Observed behavior from live usage:

-   1 company search (25 results per page) = 1 credit
-   1 person search (25 results per page) = 1 credit
-   Pagination costs additional credits per page
-   `pagination.total_count` returns the full match count
-   Failed requests still consume credits

Example operation: 1 company search + 25 person searches = 26 credits

Important: You DO NOT need to paginate to retrieve full counts if
`pagination.total_count` is returned.

------------------------------------------------------------------------

# 3. Rate Limits (Account-Level)

-   30 requests / second
-   1800 requests / minute
-   500,000 requests / day

Effective enforced interval: max(1/30 sec, 60/1800 sec) = 0.0333 seconds
between requests

------------------------------------------------------------------------

# 4. Core Endpoints

## Search Company

POST /search-company

Filters used: - company_attributes.b2b = true -
company_headcount_by_department: department: "Sales Development" min: 3
max: 100000

Returns: - results - pagination.total_count

------------------------------------------------------------------------

## Search Person

POST /search-person

Filters used: - person_department.include = \["Sales Development"\] -
person_seniority.include = \["Entry", "Senior"\] -
person_time_in_current_role.min = 3 - person_location_search.include =
\["United States #US"\] - company.websites.include = \[root_domain\]

Important: Only ROOT DOMAINS are supported.

Subdomains cause: INVALID_FILTERS "Subdomains are not supported"

------------------------------------------------------------------------

## Enrich Person

POST /enrich-person

Example:

{ "data": { "first_name": "Robbie", "last_name": "Seeley",
"company_name": "Deloitte", "company_website": "deloitte.com" } }

------------------------------------------------------------------------

# 5. Domain Handling Requirements

Prospeo rejects subdomains.

Invalid: gds.ey.com

Valid: ey.com

Required normalization logic:

1.  Strip protocol
2.  Strip www
3.  Remove path
4.  Convert to registrable root domain
5.  Handle multi-part suffixes (co.uk, com.au, etc.)

This is mandatory before calling /search-person.

------------------------------------------------------------------------

# 6. Implemented Workflow (Production Logic)

Step 1 --- Search Companies - B2B only - SDR headcount \>= 3 - Page 1
(25 companies)

Step 2 --- For each company - Extract company_id - Normalize root
domain - Run /search-person - Read pagination.total_count - Do NOT
paginate

Step 3 --- Error Handling - Continue if any search fails - Return:
sales_dev_count_entry_or_senior: "not found" status: error code - Never
crash entire job

Step 4 --- Final Output Structured JSON list containing: - company -
company_id - website - root_domain_used -
sales_dev_count_entry_or_senior - status

------------------------------------------------------------------------

# 7. Working Python Implementation (Reference)

The working script includes:

-   Rate limit enforcement
-   Root-domain normalization
-   Subdomain retry handling
-   Continue-on-error logic
-   Efficient credit usage
-   Full count retrieval via pagination.total_count

Primary file: prospeo_sdr_counts.py

Core functions implemented:

-   search_companies()
-   extract_companies()
-   registrable_root_domain()
-   search_people_for_company()
-   rate_limit_wait()
-   post_json()

The script:

-   Executes 1 company search
-   Executes 1 person search per company
-   Returns aggregated counts
-   Respects account-level rate limits
-   Avoids pagination to minimize credits

This script is verified working end-to-end.

------------------------------------------------------------------------

# 8. Observed Edge Cases

-   Some companies return mismatched domains
-   Some domains return NO_RESULTS
-   Subdomains must be stripped before querying
-   Large enterprises return 1000+ counts in single call

------------------------------------------------------------------------

# 9. Recommended Windsurf App Architecture

/services/prospeoClient.ts - rate limiter - POST wrapper - unified error
handler

/services/domainUtils.ts - root domain normalization - suffix handling

/workflows/companySdrWorkflow.ts - search-company - iterate companies -
search-person - aggregate counts

/models/types.ts - CompanyResult - PersonCountResult - ApiError

------------------------------------------------------------------------

# 10. Operational Guarantees

Current implementation:

-   Minimizes credit usage
-   Does not over-paginate
-   Stays under rate limits
-   Continues execution despite partial failures
-   Produces deterministic structured output

This reflects live-tested behavior and working production logic.
