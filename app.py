import os
import json
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response
import csv
import io

from config import Config
from models.database import db, Job, Company, PersonCount, HubSpotEnrichment, generate_query_fingerprint
from services.prospeo_client import ProspeoClient
from services.query_segmenter import QuerySegmenter
from services.domain_utils import registrable_root_domain
from jobs.market_sizing_job import start_job_async

app = Flask(__name__)
app.config.from_object(Config)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('market-sizing')
db.init_app(app)

with app.app_context():
    db.create_all()
    
    # Run migrations
    from sqlalchemy import text
    with db.engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS query_fingerprint VARCHAR(32)"))
            conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS mode VARCHAR(20) DEFAULT 'quick_tam'"))
            conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS aggregate_results JSON"))
            for col in ['sample_titles', 'sample_names', 'query_filters']:
                conn.execute(text(f"ALTER TABLE person_counts DROP COLUMN IF EXISTS {col}"))
            
            # Idempotent column fixes (safe to run every deploy)
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS employee_count INTEGER"))
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS founded INTEGER"))
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS is_b2b BOOLEAN"))
            conn.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS revenue_range_printed VARCHAR(50)"))
            
            conn.commit()
        except Exception as e:
            print(f"Migration note: {e}")

client = ProspeoClient()
segmenter = QuerySegmenter(client)

running_jobs = {}


@app.route("/")
def index():
    jobs = Job.query.order_by(Job.created_at.desc()).limit(20).all()
    
    # Add HubSpot enrichment statistics for each job
    for job in jobs:
        if job.mode == 'detailed':
            # Count total companies and HubSpot enrichments for this job
            total_companies = Company.query.filter_by(job_id=job.id).count()
            hubspot_enrichments = HubSpotEnrichment.query.filter_by(job_id=job.id).count()
            
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
        }
    })


@app.route("/api/jobs", methods=["POST"])
def create_job():
    data = request.json
    
    company_filters = data.get("company_filters", {})
    person_filters = data.get("person_filters", [])
    mode = data.get("mode", "quick_tam")  # Default to quick_tam
    fingerprint = generate_query_fingerprint(company_filters, person_filters)
    
    job = Job(
        name=data.get("name", f"Job {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"),
        status="pending",
        company_filters=company_filters,
        person_filters=person_filters,
        mode=mode,
        query_fingerprint=fingerprint
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
    ).filter_by(job_id=job_id).group_by(PersonCount.query_name).all()
    
    aggregates = {name: total for name, total in person_counts_agg}
    
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
            person_counts = {pc.query_name: pc.total_count for pc in company.person_counts}
            
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
                company.linkedin_id or "",
                
                # HubSpot enrichment
                company.hubspot_enrichments.first().hubspot_object_id if company.hubspot_enrichments.first() else "",
                company.hubspot_enrichments.first().vertical if company.hubspot_enrichments.first() else "",
                company.hubspot_enrichments.first().lookup_method if company.hubspot_enrichments.first() else ""
            ]
            
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


if __name__ == "__main__":
    app.run(debug=True, port=5000)
