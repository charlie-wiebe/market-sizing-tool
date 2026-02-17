import threading
from datetime import datetime
from models.database import db, Job, Company, PersonCount
from services.prospeo_client import ProspeoClient
from services.domain_utils import registrable_root_domain
from services.query_segmenter import QuerySegmenter

class MarketSizingJob:
    def __init__(self, job_id):
        self.job_id = job_id
        self.client = ProspeoClient()
        self.segmenter = QuerySegmenter(self.client)
        self._stop_requested = False

    def stop(self):
        self._stop_requested = True

    def run(self, app):
        with app.app_context():
            job = Job.query.get(self.job_id)
            if not job:
                return
            
            job.status = 'running'
            job.started_at = datetime.utcnow()
            db.session.commit()
            
            try:
                self._execute(job)
                job.status = 'completed'
                job.completed_at = datetime.utcnow()
            except Exception as e:
                job.status = 'failed'
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
            
            db.session.commit()

    def _execute(self, job):
        plan = self.segmenter.create_execution_plan(job.company_filters)
        
        if plan["error"]:
            job.error_message = f"Segmentation failed: {plan.get('error_code')}"
            return
        
        job.total_companies = plan["total_estimated"]
        job.estimated_credits = plan["credits_estimate"]
        db.session.commit()
        
        credits_used = 0
        companies_processed = 0
        
        for segment in plan["segments"]:
            if self._stop_requested:
                break
            
            segment_filters = segment["filters"]
            pages = segment["pages"]
            
            for page in range(1, pages + 1):
                if self._stop_requested:
                    break
                
                response = self.client.search_companies(segment_filters, page=page)
                credits_used += 1
                
                if self.client.is_error(response):
                    continue
                
                companies_data = self.client.extract_companies(response)
                
                for company_data in companies_data:
                    if self._stop_requested:
                        break
                    
                    company = self._save_company(job.id, company_data)
                    
                    if job.person_filters:
                        credits_used += self._process_person_counts(job, company)
                    
                    company.processed = True
                    companies_processed += 1
                    job.processed_companies = companies_processed
                    job.actual_credits = credits_used
                    
                    if companies_processed % 10 == 0:
                        db.session.commit()
                
                db.session.commit()

    def _save_company(self, job_id, data):
        domain = data.get("domain") or data.get("website") or ""
        root = registrable_root_domain(domain)
        
        company = Company(
            job_id=job_id,
            prospeo_company_id=data.get("company_id"),
            name=data.get("name"),
            domain=data.get("domain"),
            website=data.get("website"),
            root_domain=root,
            industry=data.get("industry"),
            headcount=data.get("headcount"),
            headcount_by_department=data.get("headcount_by_department"),
            location_country=data.get("location", {}).get("country") if isinstance(data.get("location"), dict) else None,
            location_city=data.get("location", {}).get("city") if isinstance(data.get("location"), dict) else None,
            location_state=data.get("location", {}).get("state") if isinstance(data.get("location"), dict) else None,
            founded_year=data.get("founded"),
            funding_stage=data.get("funding_stage"),
            revenue_range=data.get("revenue"),
            b2b=data.get("b2b"),
            linkedin_url=data.get("linkedin_url")
        )
        db.session.add(company)
        db.session.flush()
        return company

    def _process_person_counts(self, job, company):
        credits_used = 0
        
        if not company.root_domain:
            return credits_used
        
        for person_config in job.person_filters:
            query_name = person_config.get("name", "Unnamed Query")
            filters = dict(person_config.get("filters", {}))
            
            if "company" not in filters:
                filters["company"] = {}
            if "websites" not in filters["company"]:
                filters["company"]["websites"] = {"include": [], "exclude": []}
            
            filters["company"]["websites"]["include"] = [company.root_domain]
            
            response = self.client.search_people(filters, page=1)
            credits_used += 1
            
            pagination = self.client.get_pagination(response)
            total_count = pagination["total_count"]
            
            sample_titles = []
            sample_names = []
            status = "ok"
            error_code = None
            
            if self.client.is_error(response):
                status = "error"
                error_code = self.client.get_error_code(response)
                total_count = 0
            else:
                people = self.client.extract_people(response)
                for person in people[:25]:
                    if isinstance(person, dict):
                        title = person.get("job_title") or person.get("title")
                        name = person.get("full_name") or f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
                        if title:
                            sample_titles.append(title)
                        if name:
                            sample_names.append(name)
            
            person_count = PersonCount(
                company_id=company.id,
                job_id=job.id,
                query_name=query_name,
                query_filters=filters,
                total_count=total_count,
                sample_titles=sample_titles,
                sample_names=sample_names,
                status=status,
                error_code=error_code
            )
            db.session.add(person_count)
        
        return credits_used


def start_job_async(job_id, app):
    job_runner = MarketSizingJob(job_id)
    thread = threading.Thread(target=job_runner.run, args=(app,))
    thread.daemon = True
    thread.start()
    return job_runner
