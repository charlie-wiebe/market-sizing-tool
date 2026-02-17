import os
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response
import csv
import io

from config import Config
from models.database import db, Job, Company, PersonCount, generate_query_fingerprint
from services.prospeo_client import ProspeoClient
from services.query_segmenter import QuerySegmenter
from services.domain_utils import registrable_root_domain
from jobs.market_sizing_job import start_job_async

app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)

with app.app_context():
    db.create_all()
    
    # Run migrations for new columns
    from sqlalchemy import text
    with db.engine.connect() as conn:
        # Add missing columns if they don't exist
        try:
            conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS query_fingerprint VARCHAR(32)"))
            conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS mode VARCHAR(20) DEFAULT 'quick_tam'"))
            conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS aggregate_results JSON"))
            conn.commit()
        except Exception as e:
            print(f"Migration note: {e}")

client = ProspeoClient()
segmenter = QuerySegmenter(client)

running_jobs = {}


@app.route("/")
def index():
    jobs = Job.query.order_by(Job.created_at.desc()).limit(20).all()
    return render_template("index.html", jobs=jobs)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


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
    
    response = client.search_companies(company_filters, page=1)
    
    if client.is_error(response):
        return jsonify({
            "error": True,
            "error_code": client.get_error_code(response),
            "message": response.get("filter_error", "Company search failed")
        }), 400
    
    pagination = client.get_pagination(response)
    companies = client.extract_companies(response)
    
    sample_companies = []
    for c in companies[:5]:
        domain = c.get("domain") or c.get("website") or ""
        sample_companies.append({
            "name": c.get("name"),
            "domain": domain,
            "industry": c.get("industry"),
            "headcount": c.get("headcount"),
            "location": c.get("location", {}).get("country") if isinstance(c.get("location"), dict) else None
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
        
        # Merge company filters into person search
        p_filters.update(company_filters)
        
        p_response = client.search_people(p_filters, page=1)
        
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
            
            # Merge company filters into person search
            p_filters.update(job.company_filters)
            
            p_response = client.search_people(p_filters, page=1)
            credits_used += 1
            
            if not client.is_error(p_response):
                p_pagination = client.get_pagination(p_response)
                aggregate_results[query_name] = p_pagination["total_count"]
            else:
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
    
    companies = Company.query.filter_by(job_id=job_id).all()
    
    person_query_names = set()
    for pf in (job.person_filters or []):
        person_query_names.add(pf.get("name", "Unnamed Query"))
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    headers = [
        "company_id", "name", "domain", "website", "industry", 
        "headcount", "country", "city", "state", 
        "founded_year", "funding_stage", "revenue_range", "b2b"
    ]
    headers.extend(sorted(person_query_names))
    writer.writerow(headers)
    
    for company in companies:
        person_counts = {pc.query_name: pc.total_count for pc in company.person_counts}
        
        row = [
            company.prospeo_company_id,
            company.name,
            company.domain,
            company.website,
            company.industry,
            company.headcount,
            company.location_country,
            company.location_city,
            company.location_state,
            company.founded_year,
            company.funding_stage,
            company.revenue_range,
            company.b2b
        ]
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
