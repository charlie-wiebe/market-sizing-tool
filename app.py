import os
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, Response
import csv
import io

from config import Config
from models.database import db, Job, Company, PersonCount, HubSpotEnrichment, CompanyJobReference, generate_query_fingerprint
from sqlalchemy import text
from services.prospeo_client import ProspeoClient
from services.query_segmenter import QuerySegmenter
from services.domain_utils import registrable_root_domain
from jobs.market_sizing_job import start_job_async

app = Flask(__name__)
app.config.from_object(Config)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('market-sizing')
db.init_app(app)

# Basic database initialization with safe migrations
try:
    with app.app_context():
        db.create_all()
        
        # Safe migrations - add new SDR fields to HubSpot cache
        from sqlalchemy import text
        with db.engine.connect() as conn:
            try:
                # Add SDR count columns to HubSpot cache table (lowercase to match PostgreSQL)
                conn.execute(text("ALTER TABLE hubspot_company_cache ADD COLUMN IF NOT EXISTS aip_sdrs INTEGER"))
                conn.execute(text("ALTER TABLE hubspot_company_cache ADD COLUMN IF NOT EXISTS override_sdrs INTEGER"))
                conn.execute(text("ALTER TABLE hubspot_company_cache ADD COLUMN IF NOT EXISTS mixrank_sdrs INTEGER"))
                conn.execute(text("ALTER TABLE hubspot_company_cache ADD COLUMN IF NOT EXISTS keyplay_sdrs INTEGER"))
                conn.execute(text("ALTER TABLE hubspot_company_cache ADD COLUMN IF NOT EXISTS clay_sdrs INTEGER"))
                conn.execute(text("ALTER TABLE hubspot_company_cache ADD COLUMN IF NOT EXISTS final_sdrs INTEGER"))
                conn.commit()
            except Exception as e:
                print(f"Migration note: {e}")
except Exception as e:
    print(f"Database initialization error: {e}")

client = ProspeoClient()
segmenter = QuerySegmenter(client)

running_jobs = {}

def _normalize_company_filters(company_filters):
    """Apply location normalization to company search filters."""
    if not company_filters:
        return company_filters
        
    normalized_filters = dict(company_filters)
    
    # Handle company location search normalization
    if "company_location_search" in normalized_filters:
        location_search = normalized_filters["company_location_search"]
        
        # Normalize include locations
        if "include" in location_search and isinstance(location_search["include"], list):
            normalized_includes = []
            for location in location_search["include"]:
                resolved = client.resolve_location_format(location)
                normalized_includes.append(resolved)
            location_search["include"] = normalized_includes
        
        # Normalize exclude locations
        if "exclude" in location_search and isinstance(location_search["exclude"], list):
            normalized_excludes = []
            for location in location_search["exclude"]:
                resolved = client.resolve_location_format(location)
                normalized_excludes.append(resolved)
            location_search["exclude"] = normalized_excludes
    
    return normalized_filters


@app.route("/")
def index():
    jobs = Job.query.order_by(Job.created_at.desc()).limit(20).all()
    
    # Add HubSpot enrichment statistics for each job
    for job in jobs:
        if job.mode == 'detailed':
            # Count total companies and HubSpot enrichments for this job
            total_companies = Company.query.filter_by(job_id=job.id).count()
            hubspot_enrichments = HubSpotEnrichment.query.filter_by(job_id=job.id, is_active=True).count()
            
            job.hubspot_enriched_count = hubspot_enrichments
            job.hubspot_enrichment_percentage = round((hubspot_enrichments / total_companies * 100), 1) if total_companies > 0 else 0
        else:
            job.hubspot_enriched_count = 0
            job.hubspot_enrichment_percentage = 0
    
    return render_template("index.html", jobs=jobs)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/suggestions", methods=["POST"])
def suggestions():
    data = request.json or {}
    location = data.get("location")
    job_title = data.get("job_title")
    
    if not location and not job_title:
        return jsonify({"error": True, "message": "Provide location or job_title"}), 400
    
    result = client.search_suggestions(location=location, job_title=job_title)
    
    if client.is_error(result):
        return jsonify({"error": True, "message": "Suggestions lookup failed"}), 400
    
    return jsonify({
        "error": False,
        "location_suggestions": result.get("location_suggestions"),
        "job_title_suggestions": result.get("job_title_suggestions")
    })


@app.route("/api/preview", methods=["POST"])
def preview():
    data = request.json
    company_filters = data.get("company_filters", {})
    person_filters = data.get("person_filters", [])
    
    # Apply location normalization to company filters
    company_filters = _normalize_company_filters(company_filters)
    
    # Generate fingerprint to check for existing data
    fingerprint = generate_query_fingerprint(company_filters, person_filters)
    
    # Check for existing jobs with same query
    existing_jobs = Job.query.filter_by(query_fingerprint=fingerprint).filter(
        Job.status.in_(['completed', 'running'])
    ).order_by(Job.created_at.desc()).all()
    
    existing_company_count = 0
    existing_job_info = None
    if existing_jobs:
        latest_job = existing_jobs[0]
        existing_company_count = Company.query.filter_by(job_id=latest_job.id).count()
        existing_job_info = {
            'job_id': latest_job.id,
            'job_name': latest_job.name,
            'company_count': existing_company_count,
            'created_at': latest_job.created_at.isoformat() if latest_job.created_at else None
        }
    
    logger.info("=== PREVIEW: Company Search ===")
    logger.info(json.dumps({"endpoint": "/search-company", "payload": {"page": 1, "filters": company_filters}}, indent=2))
    
    response = client.search_companies(company_filters, page=1)
    
    if client.is_error(response):
        logger.warning("Company search failed: %s", client.get_error_code(response))
        return jsonify({
            "error": True,
            "error_code": client.get_error_code(response),
            "message": response.get("filter_error", "Company search failed")
        }), 400
    
    pagination = client.get_pagination(response)
    companies = client.extract_companies(response)
    
    sample_companies = []
    for c in companies[:25]:
        domain = c.get("domain") or c.get("website") or ""
        loc = c.get("location") if isinstance(c.get("location"), dict) else {}
        sample_companies.append({
            "name": c.get("name"),
            "domain": domain,
            "industry": c.get("industry"),
            "headcount": c.get("headcount"),
            "country": loc.get("country"),
            "city": loc.get("city"),
            "state": loc.get("state"),
            "revenue": c.get("revenue_range") or c.get("revenue"),
            "linkedin_url": c.get("linkedin_url"),
        })
    
    person_preview = None
    if person_filters and companies:
        first_company = companies[0]
        domain = first_company.get("domain") or first_company.get("website") or ""
        root = registrable_root_domain(domain)
        
        if root and person_filters:
            first_person_config = person_filters[0]
            p_filters = dict(first_person_config.get("filters", {}))
            
            if "company" not in p_filters:
                p_filters["company"] = {}
            if "websites" not in p_filters["company"]:
                p_filters["company"]["websites"] = {"include": [], "exclude": []}
            p_filters["company"]["websites"]["include"] = [root]
            
            p_response = client.search_people(p_filters, page=1)
            
            if not client.is_error(p_response):
                p_pagination = client.get_pagination(p_response)
                people = client.extract_people(p_response)
                
                sample_people = []
                for p in people[:5]:
                    sample_people.append({
                        "name": p.get("full_name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                        "title": p.get("job_title") or p.get("title"),
                        "seniority": p.get("seniority")
                    })
                
                person_preview = {
                    "query_name": first_person_config.get("name", "Person Query"),
                    "company_name": first_company.get("name"),
                    "total_count": p_pagination["total_count"],
                    "sample_people": sample_people
                }
    
    total_companies = pagination["total_count"]
    company_pages = (total_companies + 24) // 25
    person_queries = len(person_filters)
    
    # Calculate credits for both modes
    detailed_credits = company_pages + (total_companies * person_queries)
    quick_credits = 1 + person_queries  # 1 company search + N person searches
    
    # Check if over 25k limit
    exceeds_limit = total_companies > 25000
    
    # Aggregate person counts (Quick TAM mode)
    aggregate_person_counts = {}
    for person_config in person_filters:
        query_name = person_config.get("name", "Unnamed Query")
        p_filters = dict(person_config.get("filters", {}))
        
        # Merge company filters into person search (valid per Prospeo docs)
        p_filters.update(company_filters)
        
        # Prospeo rejects include+exclude simultaneously on person_department
        dept = p_filters.get("person_department")
        if isinstance(dept, dict) and dept.get("include") and dept.get("exclude"):
            p_filters["person_department"] = {"include": dept["include"]}
        
        logger.info("=== PREVIEW: Person Search [%s] ===", query_name)
        logger.info(json.dumps({"endpoint": "/search-person", "payload": {"page": 1, "filters": p_filters}}, indent=2))
        
        p_response = client.search_people(p_filters, page=1)
        
        # Log the raw response for debugging
        logger.info("=== PREVIEW: Person Search [%s] RESPONSE ===", query_name)
        logger.info(json.dumps({
            "http_status": p_response.get("_http_status"),
            "error": p_response.get("error"),
            "error_code": p_response.get("error_code"),
            "filter_error": p_response.get("filter_error"),
            "pagination": p_response.get("pagination"),
            "result_count": len(p_response.get("results") or [])
        }, indent=2))
        
        if not client.is_error(p_response):
            p_pagination = client.get_pagination(p_response)
            aggregate_person_counts[query_name] = p_pagination["total_count"]
            
            # Extract country breakdown from results
            people = client.extract_people(p_response)
            country_counts = {}
            for person in people:
                if isinstance(person, dict):
                    country = person.get("location", {}).get("country") if isinstance(person.get("location"), dict) else None
                    if country:
                        country_counts[country] = country_counts.get(country, 0) + 1
            aggregate_person_counts[f"{query_name}_sample_countries"] = country_counts
        else:
            aggregate_person_counts[query_name] = 0
    
    # Calculate potential credit savings from deduplication
    max_age = datetime.utcnow() - timedelta(days=30)  # Default to 30 days
    existing_companies_count = Company.query.filter(
        Company.created_at >= max_age,
        Company.prospeo_company_id.isnot(None)
    ).count()
    
    # Estimate potential savings (conservative estimate)
    potential_duplicate_ratio = min(0.7, existing_companies_count / max(1, total_companies))
    estimated_company_savings = int(total_companies * potential_duplicate_ratio)
    estimated_credit_savings = estimated_company_savings * (1 + person_queries)  # Company + person searches
    
    return jsonify({
        "error": False,
        "company_count": total_companies,
        "company_pages": company_pages,
        "sample_companies": sample_companies,
        "person_preview": person_preview,
        "estimated_credits": detailed_credits,
        "credit_breakdown": {
            "company_search": company_pages,
            "person_searches": total_companies * person_queries
        },
        "exceeds_limit": exceeds_limit,
        "existing_data": existing_job_info,
        "query_fingerprint": fingerprint,
        "aggregate_person_counts": aggregate_person_counts,
        "credits": {
            "quick_tam": quick_credits,
            "detailed": detailed_credits
        },
        "deduplication_estimate": {
            "existing_companies_in_db": existing_companies_count,
            "estimated_duplicates": estimated_company_savings,
            "estimated_credit_savings": estimated_credit_savings,
            "potential_savings_percentage": round(potential_duplicate_ratio * 100, 1)
        }
    })


@app.route("/api/jobs", methods=["POST"])
def create_job():
    data = request.json
    
    company_filters = data.get("company_filters", {})
    person_filters = data.get("person_filters", [])
    
    # Apply location normalization to company filters
    company_filters = _normalize_company_filters(company_filters)
    mode = data.get("mode", "quick_tam")  # Default to quick_tam
    fingerprint = generate_query_fingerprint(company_filters, person_filters)
    
    # Deduplication settings (with defaults)
    skip_existing_companies = data.get("skip_existing_companies", True)
    skip_existing_person_counts = data.get("skip_existing_person_counts", True)  
    skip_existing_hubspot = data.get("skip_existing_hubspot", True)
    max_data_age_days = data.get("max_data_age_days", 30)
    
    job = Job(
        name=data.get("name", f"Job {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"),
        status="pending",
        company_filters=company_filters,
        person_filters=person_filters,
        mode=mode,
        query_fingerprint=fingerprint,
        skip_existing_companies=skip_existing_companies,
        skip_existing_person_counts=skip_existing_person_counts,
        skip_existing_hubspot=skip_existing_hubspot,
        max_data_age_days=max_data_age_days
    )
    db.session.add(job)
    db.session.commit()
    
    # Quick TAM mode runs synchronously (very fast)
    if mode == "quick_tam":
        run_quick_tam_job(job)
        return jsonify(job.to_dict()), 201
    
    # Detailed mode runs async
    job_runner = start_job_async(job.id, app)
    running_jobs[job.id] = job_runner
    
    return jsonify(job.to_dict()), 201


def run_quick_tam_job(job):
    """Run Quick TAM job synchronously - just aggregate counts."""
    job.status = 'running'
    job.started_at = datetime.utcnow()
    db.session.commit()
    
    try:
        # Get company count
        logger.info("=== JOB %d: Company Search ===", job.id)
        logger.info(json.dumps({"endpoint": "/search-company", "payload": {"page": 1, "filters": job.company_filters}}, indent=2))
        
        response = client.search_companies(job.company_filters, page=1)
        
        if client.is_error(response):
            job.status = 'failed'
            job.error_message = f"Company search failed: {client.get_error_code(response)}"
            job.completed_at = datetime.utcnow()
            db.session.commit()
            return
        
        pagination = client.get_pagination(response)
        job.total_companies = pagination["total_count"]
        credits_used = 1
        
        # Get aggregate person counts
        aggregate_results = {}
        for person_config in (job.person_filters or []):
            query_name = person_config.get("name", "Unnamed Query")
            p_filters = dict(person_config.get("filters", {}))
            
            # Merge company filters into person search (valid per Prospeo docs)
            p_filters.update(job.company_filters)
            
            # Prospeo rejects include+exclude simultaneously on person_department
            dept = p_filters.get("person_department")
            if isinstance(dept, dict) and dept.get("include") and dept.get("exclude"):
                p_filters["person_department"] = {"include": dept["include"]}
            
            logger.info("=== JOB %d: Person Search [%s] ===", job.id, query_name)
            logger.info(json.dumps({"endpoint": "/search-person", "payload": {"page": 1, "filters": p_filters}}, indent=2))
            
            p_response = client.search_people(p_filters, page=1)
            credits_used += 1
            
            # Log response for debugging
            logger.info("=== JOB %d: Person Search [%s] RESPONSE ===", job.id, query_name)
            logger.info(json.dumps({
                "http_status": p_response.get("_http_status"),
                "error": p_response.get("error"),
                "error_code": p_response.get("error_code"),
                "filter_error": p_response.get("filter_error"),
                "pagination": p_response.get("pagination"),
                "result_count": len(p_response.get("results") or [])
            }, indent=2))
            
            if not client.is_error(p_response):
                p_pagination = client.get_pagination(p_response)
                aggregate_results[query_name] = p_pagination["total_count"]
            else:
                logger.warning("Person search failed for [%s]: %s", query_name, client.get_error_code(p_response))
                aggregate_results[query_name] = 0
        
        job.aggregate_results = aggregate_results
        job.actual_credits = credits_used
        job.status = 'completed'
        job.completed_at = datetime.utcnow()
        
    except Exception as e:
        job.status = 'failed'
        job.error_message = str(e)
        job.completed_at = datetime.utcnow()
    
    db.session.commit()


@app.route("/api/jobs/<int:job_id>")
def get_job(job_id):
    job = Job.query.get_or_404(job_id)
    return jsonify(job.to_dict())


@app.route("/api/jobs/<int:job_id>/stop", methods=["POST"])
def stop_job(job_id):
    job = Job.query.get_or_404(job_id)
    
    if job_id in running_jobs:
        running_jobs[job_id].stop()
        del running_jobs[job_id]
    
    job.status = "stopped"
    job.completed_at = datetime.utcnow()
    db.session.commit()
    
    return jsonify(job.to_dict())


@app.route("/api/jobs/<int:job_id>/results")
def get_job_results(job_id):
    job = Job.query.get_or_404(job_id)
    
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    
    companies = Company.query.filter_by(job_id=job_id).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    results = []
    for company in companies.items:
        company_dict = company.to_dict()
        results.append(company_dict)
    
    person_counts_agg = db.session.query(
        PersonCount.query_name,
        db.func.sum(PersonCount.total_count).label("total")
    ).filter_by(job_id=job_id, is_active=True).group_by(PersonCount.query_name).all()
    
    aggregates = {name: total for name, total in person_counts_agg}
    
    # Calculate deduplication statistics
    total_companies_found = Company.query.filter_by(job_id=job_id).count()
    total_company_references = CompanyJobReference.query.filter_by(job_id=job_id).count()
    
    return jsonify({
        "job": job.to_dict(),
        "companies": results,
        "pagination": {
            "page": companies.page,
            "per_page": per_page,
            "total": companies.total,
            "pages": companies.pages
        },
        "aggregates": {
            "total_companies": job.processed_companies,
            "person_counts": aggregates
        },
        "deduplication_stats": {
            "companies_processed": job.processed_companies or 0,
            "companies_skipped": job.companies_skipped or 0,
            "person_counts_skipped": job.person_counts_skipped or 0,
            "hubspot_skipped": job.hubspot_skipped or 0,
            "total_companies_in_job": total_companies_found,
            "total_company_references": total_company_references,
            "credit_savings_estimate": (job.companies_skipped or 0) + (job.person_counts_skipped or 0)
        }
    })


@app.route("/api/jobs/<int:job_id>/export")
def export_job(job_id):
    job = Job.query.get_or_404(job_id)
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    if job.mode == 'quick_tam':
        # Quick TAM: export aggregate results
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Job Name", job.name])
        writer.writerow(["Mode", "Quick TAM Estimate"])
        writer.writerow(["Total Companies", job.total_companies])
        writer.writerow([])
        
        writer.writerow(["Person Search", "Aggregate Count"])
        for query_name, count in (job.aggregate_results or {}).items():
            writer.writerow([query_name, count])
        
        writer.writerow([])
        writer.writerow(["Credits Used", job.actual_credits])
        writer.writerow(["Completed At", job.completed_at.isoformat() if job.completed_at else ""])
    else:
        # Detailed mode: export per-company data
        companies = Company.query.filter_by(job_id=job_id).all()
        
        person_query_names = set()
        for pf in (job.person_filters or []):
            person_query_names.add(pf.get("name", "Unnamed Query"))
        
        # Headers with actual Prospeo API fields only
        headers = [
            # Core fields
            "prospeo_company_id", "name", "website", "domain",
            
            # Basic company information  
            "description", "description_seo", "description_ai", "company_type", 
            "industry", "employee_count", "employee_range", "founded", "logo_url",
            
            # Location details
            "location_country", "location_city", "location_state", "location_country_code", 
            "location_raw_address",
            
            # Social media URLs
            "linkedin_url", "twitter_url", "facebook_url", "crunchbase_url", 
            "instagram_url", "youtube_url",
            
            # Revenue information
            "revenue_min", "revenue_max", "revenue_range_printed",
            
            # Attributes
            "is_b2b", "has_demo", "has_free_trial", "has_downloadable", 
            "has_mobile_apps", "has_online_reviews", "has_pricing",
            
            # Classification
            "linkedin_id",
            
            # HubSpot enrichment
            "hubspot_object_id", "hubspot_vertical", "hubspot_lookup_method"
        ]
        
        # Add person query columns
        headers.extend(sorted(person_query_names))
        writer.writerow(headers)
        
        for company in companies:
            person_counts = {pc.query_name: pc.total_count for pc in company.person_counts.filter_by(is_active=True)}
            
            # Helper function to serialize JSON fields for CSV
            def serialize_json(value):
                if value is None:
                    return ""
                if isinstance(value, (dict, list)):
                    return str(value).replace(',', ';')  # Replace commas to avoid CSV issues
                return str(value)
            
            row = [
                # Core fields
                company.prospeo_company_id or "",
                company.name or "",
                company.website or "",
                company.domain or "",
                
                # Basic company information
                (company.description or "")[:500] if company.description else "",  # Truncate long descriptions
                (company.description_seo or "")[:200] if company.description_seo else "",
                (company.description_ai or "")[:200] if company.description_ai else "",
                company.company_type or "",
                company.industry or "",
                company.employee_count or "",
                company.employee_range or "",
                company.founded or "",
                company.logo_url or "",
                
                # Location details
                company.location_country or "",
                company.location_city or "",
                company.location_state or "",
                company.location_country_code or "",
                company.location_raw_address or "",
                
                # Social media URLs
                company.linkedin_url or "",
                company.twitter_url or "",
                company.facebook_url or "",
                company.crunchbase_url or "",
                company.instagram_url or "",
                company.youtube_url or "",
                
                # Revenue information
                company.revenue_min or "",
                company.revenue_max or "",
                company.revenue_range_printed or "",
                
                # Attributes
                company.is_b2b if company.is_b2b is not None else "",
                company.has_demo if company.has_demo is not None else "",
                company.has_free_trial if company.has_free_trial is not None else "",
                company.has_downloadable if company.has_downloadable is not None else "",
                company.has_mobile_apps if company.has_mobile_apps is not None else "",
                company.has_online_reviews if company.has_online_reviews is not None else "",
                company.has_pricing if company.has_pricing is not None else "",
                
                # Classification
                company.linkedin_id or ""
            ]
            
            # HubSpot enrichment (cached to avoid multiple queries, filter for active only)
            hubspot_enrichment = company.hubspot_enrichments.filter_by(is_active=True).first()
            row.extend([
                hubspot_enrichment.hubspot_object_id if hubspot_enrichment else "",
                hubspot_enrichment.vertical if hubspot_enrichment else "",
                hubspot_enrichment.lookup_method if hubspot_enrichment else ""
            ])
            
            # Add person count columns
            for qn in sorted(person_query_names):
                row.append(person_counts.get(qn, 0))
            
            writer.writerow(row)
    
    output.seek(0)
    
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=job_{job_id}_results.csv"
        }
    )


@app.route("/results/<int:job_id>")
def results_page(job_id):
    job = Job.query.get_or_404(job_id)
    return render_template("results.html", job=job)

@app.route('/api/admin/backfill-prospeo-ids', methods=['POST'])
def backfill_prospeo_company_ids():
    """ADMIN ENDPOINT: Backfill NULL prospeo_company_id values in person_counts"""
    try:
        # Check records that need backfilling
        check_query = text("""
            SELECT 
                COUNT(*) as records_to_backfill,
                COUNT(CASE WHEN c.prospeo_company_id IS NOT NULL THEN 1 END) as companies_have_prospeo_id
            FROM person_counts pc
            JOIN companies c ON pc.company_id = c.id
            WHERE pc.prospeo_company_id IS NULL 
                AND pc.is_active = true
        """)
        
        with db.engine.connect() as conn:
            result = conn.execute(check_query).fetchone()
            records_to_backfill = result.records_to_backfill
            companies_have_prospeo_id = result.companies_have_prospeo_id
            
            if records_to_backfill == 0:
                return jsonify({
                    'success': True,
                    'message': 'No records need backfilling',
                    'records_updated': 0
                })
            
            # Execute backfill UPDATE
            backfill_query = text("""
                UPDATE person_counts 
                SET prospeo_company_id = c.prospeo_company_id
                FROM companies c
                WHERE person_counts.company_id = c.id
                    AND person_counts.prospeo_company_id IS NULL
                    AND person_counts.is_active = true
                    AND c.prospeo_company_id IS NOT NULL
            """)
            
            result = conn.execute(backfill_query)
            records_updated = result.rowcount
            conn.commit()
            
            # Verify the fix
            verify_query = text("""
                SELECT COUNT(*) as remaining_null_records
                FROM person_counts 
                WHERE prospeo_company_id IS NULL 
                    AND is_active = true
            """)
            
            result = conn.execute(verify_query).fetchone()
            remaining_null = result.remaining_null_records
            
            return jsonify({
                'success': True,
                'message': 'Backfill completed successfully',
                'records_found': records_to_backfill,
                'records_updated': records_updated,
                'remaining_null': remaining_null
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/admin/fix-hubspot-duplicates', methods=['POST'])
def fix_hubspot_duplicate_enrichments():
    """ADMIN ENDPOINT: Fix duplicate active HubSpot enrichments - keep most recent per company"""
    try:
        # Check companies with duplicate active enrichments
        check_query = text("""
            SELECT 
                COUNT(*) as companies_with_duplicates,
                SUM(active_count) - COUNT(*) as records_to_deactivate
            FROM (
                SELECT 
                    company_id,
                    COUNT(*) as active_count
                FROM hubspot_enrichments 
                WHERE is_active = true
                GROUP BY company_id
                HAVING COUNT(*) > 1
            ) duplicates
        """)
        
        with db.engine.connect() as conn:
            result = conn.execute(check_query).fetchone()
            companies_with_duplicates = result.companies_with_duplicates
            records_to_deactivate = result.records_to_deactivate
            
            if companies_with_duplicates == 0:
                return jsonify({
                    'success': True,
                    'message': 'No duplicate active HubSpot enrichments found',
                    'companies_fixed': 0,
                    'records_deactivated': 0
                })
            
            # Fix duplicates: keep most recent active enrichment per company, deactivate older ones
            fix_query = text("""
                UPDATE hubspot_enrichments 
                SET is_active = false 
                WHERE id IN (
                    SELECT id 
                    FROM (
                        SELECT 
                            id,
                            ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at DESC) as rn
                        FROM hubspot_enrichments 
                        WHERE is_active = true
                    ) ranked
                    WHERE rn > 1
                )
            """)
            
            result = conn.execute(fix_query)
            records_deactivated = result.rowcount
            conn.commit()
            
            # Verify the fix
            verify_query = text("""
                SELECT COUNT(*) as remaining_duplicates
                FROM (
                    SELECT company_id
                    FROM hubspot_enrichments 
                    WHERE is_active = true
                    GROUP BY company_id
                    HAVING COUNT(*) > 1
                ) remaining
            """)
            
            result = conn.execute(verify_query).fetchone()
            remaining_duplicates = result.remaining_duplicates
            
            return jsonify({
                'success': True,
                'message': 'HubSpot duplicate enrichments fixed successfully',
                'companies_with_duplicates': companies_with_duplicates,
                'records_deactivated': records_deactivated,
                'remaining_duplicates': remaining_duplicates
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/admin/refresh-hubspot-enrichments', methods=['POST'])
def refresh_hubspot_enrichments():
    """ADMIN ENDPOINT: Backfill missing HubSpot enrichments from cache"""
    try:
        from services.hubspot_client_cached import HubSpotClientCached
        from models.database import HubSpotCache, Company, HubSpotEnrichment, Job
        
        # Check cache status first
        cache_count = HubSpotCache.query.count()
        if cache_count == 0:
            return jsonify({
                'success': False,
                'error': 'HubSpot cache is empty! Cannot refresh enrichments.'
            }), 400
        
        # Initialize cached HubSpot client
        hubspot_client = HubSpotClientCached(session=db.session)
        
        # Get companies that need enrichment (have no active HubSpot enrichment)
        companies_needing_enrichment = db.session.query(Company).filter(
            ~Company.id.in_(
                db.session.query(HubSpotEnrichment.company_id).filter(
                    HubSpotEnrichment.is_active == True
                )
            )
        ).limit(5000).all()  # Process 5000 companies per batch for faster backfill
        
        if not companies_needing_enrichment:
            return jsonify({
                'success': True,
                'message': 'No companies need HubSpot enrichment - all are already enriched.',
                'cache_count': cache_count,
                'companies_checked': 0,
                'new_enrichments': 0
            })
        
        # Build batch data for enrichment with full domain waterfall
        batch_data = []
        for company in companies_needing_enrichment:
            batch_data.append({
                'id': company.id,
                'linkedin_url': company.linkedin_url,
                'domain': company.domain,
                'website': company.website,
                'other_websites': company.other_websites
            })
        
        # Get enrichments from cache
        enrichments = hubspot_client.batch_enrich_companies(batch_data)
        
        # Save enrichments
        new_enrichments = 0
        for company in companies_needing_enrichment:
            company_id = company.id
            enrichment_data = enrichments.get(company_id)
            
            if enrichment_data:
                try:
                    # Create new enrichment record
                    new_enrichment = HubSpotEnrichment(
                        company_id=company_id,
                        job_id=company.job_id,
                        hubspot_object_id=enrichment_data['hubspot_object_id'],
                        vertical=enrichment_data['vertical'],
                        lookup_method=enrichment_data['lookup_method'],
                        hubspot_created_date=enrichment_data['hubspot_created_date'],
                        is_active=True
                    )
                    db.session.add(new_enrichment)
                    new_enrichments += 1
                except Exception as e:
                    print(f"Failed to save enrichment for company {company_id}: {e}")
        
        # Commit all changes
        db.session.commit()
        
        # Get total counts for response
        total_companies = Company.query.count()
        total_enriched = HubSpotEnrichment.query.filter(HubSpotEnrichment.is_active == True).count()
        
        return jsonify({
            'success': True,
            'cache_count': cache_count,
            'companies_checked': len(companies_needing_enrichment),
            'new_enrichments': new_enrichments,
            'total_companies': total_companies,
            'total_enriched': total_enriched,
            'coverage_percent': round((total_enriched / total_companies * 100), 2) if total_companies > 0 else 0,
            'message': f'Added {new_enrichments} new HubSpot enrichments from cache'
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
