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
        """Upsert company - update existing by prospeo_company_id or create new."""
        domain = data.get("domain") or data.get("website") or ""
        root = registrable_root_domain(domain)
        prospeo_id = data.get("company_id")
        
        # Check for existing company with same prospeo_company_id in this job
        existing = None
        if prospeo_id:
            existing = Company.query.filter_by(
                job_id=job_id, 
                prospeo_company_id=prospeo_id
            ).first()
        
        if existing:
            # Update existing record with latest data from all Prospeo fields
            self._update_company_fields(existing, data, root)
            existing.created_at = datetime.utcnow()  # Update timestamp
            db.session.flush()
            return existing
        
        # Create new company with all Prospeo fields
        company = Company(job_id=job_id, prospeo_company_id=prospeo_id)
        self._update_company_fields(company, data, root)
        db.session.add(company)
        db.session.flush()
        return company

    def _update_company_fields(self, company, data, root_domain):
        """Update company object with all fields from Prospeo API response."""
        # Core fields
        company.name = data.get("name") or company.name
        company.domain = data.get("domain") or company.domain
        company.website = data.get("website") or company.website
        company.root_domain = root_domain or company.root_domain
        
        # Extended company information
        company.description = data.get("description") or company.description
        company.description_seo = data.get("description_seo") or company.description_seo
        company.description_ai = data.get("description_ai") or company.description_ai
        company.company_type = data.get("type") or company.company_type
        company.employee_range = data.get("employee_range") or company.employee_range
        company.other_websites = data.get("other_websites") or company.other_websites
        company.keywords = data.get("keywords") or company.keywords
        company.logo_url = data.get("logo_url") or company.logo_url
        
        # Business details
        company.industry = data.get("industry") or company.industry
        company.headcount = data.get("employee_count") or data.get("headcount") or company.headcount
        company.headcount_by_department = data.get("headcount_by_department") or company.headcount_by_department
        company.founded_year = data.get("founded") or company.founded_year
        company.funding_stage = data.get("funding_stage") or company.funding_stage
        
        # Location details
        location = data.get("location", {}) if isinstance(data.get("location"), dict) else {}
        company.location_country = location.get("country") or company.location_country
        company.location_city = location.get("city") or company.location_city
        company.location_state = location.get("state") or company.location_state
        company.location_country_code = location.get("country_code") or company.location_country_code
        company.location_raw_address = location.get("raw_address") or company.location_raw_address
        
        # Contact information
        company.email_tech = data.get("email_tech") or company.email_tech
        company.phone_hq = data.get("phone_hq") or company.phone_hq
        
        # Social media URLs
        company.linkedin_url = data.get("linkedin_url") or company.linkedin_url
        company.twitter_url = data.get("twitter_url") or company.twitter_url
        company.facebook_url = data.get("facebook_url") or company.facebook_url
        company.crunchbase_url = data.get("crunchbase_url") or company.crunchbase_url
        company.instagram_url = data.get("instagram_url") or company.instagram_url
        company.youtube_url = data.get("youtube_url") or company.youtube_url
        
        # Revenue information
        company.revenue_range = data.get("revenue") or data.get("revenue_range_printed") or company.revenue_range
        revenue_range = data.get("revenue_range", {}) if isinstance(data.get("revenue_range"), dict) else {}
        company.revenue_min = revenue_range.get("min") or company.revenue_min
        company.revenue_max = revenue_range.get("max") or company.revenue_max
        company.revenue_printed = data.get("revenue_range_printed") or company.revenue_printed
        
        # Attribute flags
        attributes = data.get("attributes", {}) if isinstance(data.get("attributes"), dict) else {}
        company.b2b = attributes.get("is_b2b") if attributes.get("is_b2b") is not None else (data.get("b2b") if data.get("b2b") is not None else company.b2b)
        company.has_demo = attributes.get("has_demo") if attributes.get("has_demo") is not None else company.has_demo
        company.has_free_trial = attributes.get("has_free_trial") if attributes.get("has_free_trial") is not None else company.has_free_trial
        company.has_downloadable = attributes.get("has_downloadable") if attributes.get("has_downloadable") is not None else company.has_downloadable
        company.has_mobile_apps = attributes.get("has_mobile_apps") if attributes.get("has_mobile_apps") is not None else company.has_mobile_apps
        company.has_online_reviews = attributes.get("has_online_reviews") if attributes.get("has_online_reviews") is not None else company.has_online_reviews
        company.has_pricing = attributes.get("has_pricing") if attributes.get("has_pricing") is not None else company.has_pricing
        
        # Complex data structures
        company.funding = data.get("funding") or company.funding
        company.technology = data.get("technology") or company.technology
        company.job_postings = data.get("job_postings") or company.job_postings
        
        # Classification codes
        company.sic_codes = data.get("sic_codes") or company.sic_codes
        company.naics_codes = data.get("naics_codes") or company.naics_codes
        company.linkedin_id = data.get("linkedin_id") or company.linkedin_id

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
            
            status = "ok"
            error_code = None
            
            if self.client.is_error(response):
                status = "error"
                error_code = self.client.get_error_code(response)
                total_count = 0
            
            person_count = PersonCount(
                company_id=company.id,
                job_id=job.id,
                query_name=query_name,
                total_count=total_count,
                status=status,
                error_code=error_code,
                prospeo_company_id=company.prospeo_company_id
            )
            db.session.add(person_count)
        
        return credits_used


def start_job_async(job_id, app):
    job_runner = MarketSizingJob(job_id)
    thread = threading.Thread(target=job_runner.run, args=(app,))
    thread.daemon = True
    thread.start()
    return job_runner
