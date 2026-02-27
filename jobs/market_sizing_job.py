import threading
from datetime import datetime, timedelta
from models.database import db, Job, Company, PersonCount, HubSpotEnrichment, CompanyJobReference
from services.prospeo_client import ProspeoClient
from services.domain_utils import registrable_root_domain
from services.query_segmenter import QuerySegmenter
from services.hubspot_client import HubSpotClient
from sqlalchemy import and_, or_

class MarketSizingJob:
    def __init__(self, job_id):
        self.job_id = job_id
        self.client = ProspeoClient()
        self.hubspot_client = None  # Lazy load to prevent initialization errors from blocking job
        self.segmenter = QuerySegmenter(self.client)
        self._stop_requested = False

    def stop(self):
        self._stop_requested = True

    def run(self, app):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"JOB {self.job_id}: Thread started")
        
        try:
            with app.app_context():
                logger.info(f"JOB {self.job_id}: App context entered")
                job = Job.query.get(self.job_id)
                if not job:
                    logger.error(f"JOB {self.job_id}: Job not found in database")
                    return
                
                logger.info(f"JOB {self.job_id}: Setting status to running")
                job.status = 'running'
                job.started_at = datetime.utcnow()
                db.session.commit()
                
                try:
                    logger.info(f"JOB {self.job_id}: Starting execution")
                    self._execute(job)
                    logger.info(f"JOB {self.job_id}: Execution completed successfully")
                    job.status = 'completed'
                    job.completed_at = datetime.utcnow()
                except Exception as e:
                    logger.error(f"JOB {self.job_id}: Execution failed: {e}")
                    import traceback
                    logger.error(f"JOB {self.job_id}: Traceback: {traceback.format_exc()}")
                    job.status = 'failed'
                    job.error_message = str(e)
                    job.completed_at = datetime.utcnow()
                
                db.session.commit()
                logger.info(f"JOB {self.job_id}: Final status: {job.status}")
        except Exception as e:
            logger.error(f"JOB {self.job_id}: Thread crashed: {e}")
            import traceback
            logger.error(f"JOB {self.job_id}: Thread traceback: {traceback.format_exc()}")

    def _execute(self, job):
        import logging
        logger = logging.getLogger(__name__)
        
        # Reset client tracking stats for this job
        self.client.reset_tracking_stats()
        
        # Log job configuration
        logger.info(f"JOB {job.id}: Starting execution with deduplication settings:")
        logger.info(f"  Skip existing companies: {job.skip_existing_companies}")
        logger.info(f"  Skip existing person counts: {job.skip_existing_person_counts}")
        logger.info(f"  Skip existing HubSpot: {job.skip_existing_hubspot}")
        logger.info(f"  Max data age: {job.max_data_age_days} days")
        
        # Normalize company search filters
        normalized_company_filters = self._prepare_company_search_filters(job.company_filters)
        
        # Create execution plan with enhanced logging
        logger.info(f"JOB {job.id}: Creating segmentation plan...")
        plan = self.segmenter.create_execution_plan(normalized_company_filters)
        
        if plan["error"]:
            job.error_message = f"Segmentation failed: {plan.get('error_code')}"
            logger.error(f"JOB {job.id}: {job.error_message}")
            return
        
        # Log segmentation plan details
        logger.info(f"JOB {job.id}: Segmentation plan created:")
        logger.info(f"  Total estimated companies: {plan['total_estimated']}")
        logger.info(f"  Number of segments: {len(plan['segments'])}")
        logger.info(f"  Estimated credits: {plan['credits_estimate']}")
        
        for i, segment in enumerate(plan["segments"]):
            logger.info(f"  Segment {i+1}: {segment['estimated_count']} companies, {segment['pages']} pages")
        
        job.total_companies = plan["total_estimated"]
        job.estimated_credits = plan["credits_estimate"]
        db.session.commit()
        
        
        credits_used = 0
        companies_processed = 0
        companies_skipped = 0
        
        for segment_idx, segment in enumerate(plan["segments"]):
            if self._stop_requested:
                break
            
            segment_filters = segment["filters"]
            pages = segment["pages"]
            
            logger.info(f"JOB {job.id}: Processing segment {segment_idx + 1}/{len(plan['segments'])}")
            logger.info(f"  Segment filters: {segment_filters}")
            logger.info(f"  Expected companies: {segment['estimated_count']}, Pages: {pages}")
            
            
            actual_companies_in_segment = 0
            
            for page in range(1, pages + 1):
                if self._stop_requested:
                    break
                
                logger.info(f"JOB {job.id}: Requesting page {page}/{pages} of segment {segment_idx + 1}")
                
                response = self.client.search_companies(segment_filters, page=page)
                credits_used += 1
                
                if self.client.is_error(response):
                    logger.warning(f"JOB {job.id}: Page {page} failed: {self.client.get_error_code(response)}")
                    continue
                
                companies_data = self.client.extract_companies(response)
                page_companies = len(companies_data)
                actual_companies_in_segment += page_companies
                
                logger.info(f"JOB {job.id}: Page {page} returned {page_companies} companies")
                
                for company_idx, company_data in enumerate(companies_data):
                    if self._stop_requested:
                        break
                    
                    # Check if company already exists globally
                    existing_company = None
                    if job.skip_existing_companies:
                        existing_company = self._find_existing_company_globally(company_data)
                        
                        if existing_company:
                            # Link existing company to this job
                            self._link_existing_company_to_job(existing_company, job.id)
                            companies_skipped += 1
                            
                            if companies_skipped % 50 == 0:
                                logger.info(f"JOB {job.id}: Skipped {companies_skipped} existing companies so far")
                            continue
                    
                    # Save new company
                    company = self._save_company(job.id, company_data)
                    
                    # Process person counts if needed
                    if job.person_filters:
                        person_credits = self._process_person_counts(job, company)
                        credits_used += person_credits
                    
                    companies_processed += 1
                    
                    # Log progress periodically
                    if companies_processed % 100 == 0:
                        logger.info(f"JOB {job.id}: Progress - Processed: {companies_processed}, "
                                   f"Skipped: {companies_skipped}, Credits: {credits_used}")
                        
                        # Log client tracking stats
                        stats = self.client.get_tracking_stats()
                        logger.info(f"JOB {job.id}: Client stats - Requests: {stats['total_requests']}, "
                                   f"Companies collected: {stats['total_companies_collected']}, "
                                   f"Rate limit delay: {stats['total_rate_limit_delay']:.2f}s")
                    
                    job.processed_companies = companies_processed
                    job.companies_skipped = companies_skipped
                    job.actual_credits = credits_used
                    
                    if companies_processed % 10 == 0:
                        db.session.commit()
                
                db.session.commit()
            
            logger.info(f"JOB {job.id}: Segment {segment_idx + 1} completed - "
                       f"Expected: {segment['estimated_count']}, Actual: {actual_companies_in_segment}")
        
        # Final statistics
        final_stats = self.client.get_tracking_stats()
        logger.info(f"JOB {job.id}: Collection phase completed:")
        logger.info(f"  Total companies processed: {companies_processed}")
        logger.info(f"  Total companies skipped: {companies_skipped}")
        logger.info(f"  Total API requests: {final_stats['total_requests']}")
        logger.info(f"  Total rate limit delay: {final_stats['total_rate_limit_delay']:.2f}s")
        logger.info(f"  Credits used: {credits_used}")
        
        # Run HubSpot enrichment for detailed jobs
        if job.mode == 'detailed':
            try:
                self._enrich_companies_with_hubspot(job)
            except Exception as e:
                logger.error(f"HubSpot enrichment failed for job {job.id}: {e}")
                # Continue job processing even if HubSpot enrichment fails completely

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
        company.name = data.get("name") or company.name
        company.website = data.get("website") or company.website
        company.domain = data.get("domain") or company.domain
        company.description = data.get("description") or company.description
        company.description_seo = data.get("description_seo") or company.description_seo
        company.description_ai = data.get("description_ai") or company.description_ai
        company.company_type = data.get("type") or company.company_type
        company.industry = data.get("industry") or company.industry
        company.employee_count = data.get("employee_count") or company.employee_count
        company.employee_range = data.get("employee_range") or company.employee_range
        company.founded = data.get("founded") or company.founded
        company.other_websites = data.get("other_websites") or company.other_websites
        company.keywords = data.get("keywords") or company.keywords
        company.logo_url = data.get("logo_url") or company.logo_url
        
        # Location (flatten nested object)
        location = data.get("location", {}) if isinstance(data.get("location"), dict) else {}
        company.location_country = location.get("country") or company.location_country
        company.location_city = location.get("city") or company.location_city
        company.location_state = location.get("state") or company.location_state
        company.location_country_code = location.get("country_code") or company.location_country_code
        company.location_raw_address = location.get("raw_address") or company.location_raw_address
        
        company.email_tech = data.get("email_tech") or company.email_tech
        company.phone_hq = data.get("phone_hq") or company.phone_hq
        
        company.linkedin_url = data.get("linkedin_url") or company.linkedin_url
        company.twitter_url = data.get("twitter_url") or company.twitter_url
        company.facebook_url = data.get("facebook_url") or company.facebook_url
        company.crunchbase_url = data.get("crunchbase_url") or company.crunchbase_url
        company.instagram_url = data.get("instagram_url") or company.instagram_url
        company.youtube_url = data.get("youtube_url") or company.youtube_url
        
        # Revenue (flatten nested object)
        revenue_range = data.get("revenue_range", {}) if isinstance(data.get("revenue_range"), dict) else {}
        company.revenue_min = revenue_range.get("min") or company.revenue_min
        company.revenue_max = revenue_range.get("max") or company.revenue_max
        company.revenue_range_printed = data.get("revenue_range_printed") or company.revenue_range_printed
        
        # Attributes (flatten nested object)
        attributes = data.get("attributes", {}) if isinstance(data.get("attributes"), dict) else {}
        company.is_b2b = attributes.get("is_b2b") if attributes.get("is_b2b") is not None else company.is_b2b
        company.has_demo = attributes.get("has_demo") if attributes.get("has_demo") is not None else company.has_demo
        company.has_free_trial = attributes.get("has_free_trial") if attributes.get("has_free_trial") is not None else company.has_free_trial
        company.has_downloadable = attributes.get("has_downloadable") if attributes.get("has_downloadable") is not None else company.has_downloadable
        company.has_mobile_apps = attributes.get("has_mobile_apps") if attributes.get("has_mobile_apps") is not None else company.has_mobile_apps
        company.has_online_reviews = attributes.get("has_online_reviews") if attributes.get("has_online_reviews") is not None else company.has_online_reviews
        company.has_pricing = attributes.get("has_pricing") if attributes.get("has_pricing") is not None else company.has_pricing
        
        company.funding = data.get("funding") or company.funding
        company.technology = data.get("technology") or company.technology
        company.job_postings = data.get("job_postings") or company.job_postings
        
        company.sic_codes = data.get("sic_codes") or company.sic_codes
        company.naics_codes = data.get("naics_codes") or company.naics_codes
        company.linkedin_id = data.get("linkedin_id") or company.linkedin_id

    
    def _prepare_company_search_filters(self, company_filters):
        """Apply location normalization to company search filters using Search Suggestions API."""
        import logging
        logger = logging.getLogger(__name__)
        
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
                    resolved = self.client.resolve_location_format(location)
                    normalized_includes.append(resolved)
                    if resolved != location:
                        logger.info(f"Normalized company location '{location}' -> '{resolved}'")
                location_search["include"] = normalized_includes
            
            # Normalize exclude locations
            if "exclude" in location_search and isinstance(location_search["exclude"], list):
                normalized_excludes = []
                for location in location_search["exclude"]:
                    resolved = self.client.resolve_location_format(location)
                    normalized_excludes.append(resolved)
                    if resolved != location:
                        logger.info(f"Normalized company location exclude '{location}' -> '{resolved}'")
                location_search["exclude"] = normalized_excludes
        
        return normalized_filters
    
    def _find_existing_company_globally(self, company_data):
        """Find existing company by prospeo_company_id, domain, or name."""
        prospeo_id = company_data.get("company_id")
        domain = company_data.get("domain") or company_data.get("website", "")
        name = company_data.get("name", "")
        
        # Primary lookup: by prospeo_company_id
        if prospeo_id:
            existing = Company.query.filter_by(prospeo_company_id=prospeo_id).first()
            if existing:
                return existing
        
        # Secondary lookup: by domain (if available)
        if domain:
            root_domain = registrable_root_domain(domain)
            if root_domain:
                existing = Company.query.filter(
                    or_(
                        Company.domain == root_domain,
                        Company.website == root_domain,
                        Company.domain.like(f'%{root_domain}'),
                        Company.website.like(f'%{root_domain}')
                    )
                ).first()
                if existing:
                    return existing
        
        # Tertiary lookup: by exact name match (if name is unique enough)
        if name and len(name) > 3:
            existing = Company.query.filter_by(name=name).first()
            if existing:
                return existing
        
        return None
    
    def _link_existing_company_to_job(self, company, job_id):
        """Create a reference linking an existing company to the current job."""
        # Check if reference already exists
        existing_ref = CompanyJobReference.query.filter_by(
            company_id=company.id,
            job_id=job_id
        ).first()
        
        if not existing_ref:
            ref = CompanyJobReference(
                company_id=company.id,
                job_id=job_id
            )
            db.session.add(ref)
            db.session.flush()

    def _process_person_counts(self, job, company):
        """Process person counts with domain/website fallback and enhanced filters"""
        import logging
        logger = logging.getLogger(__name__)
        
        credits_used = 0
        person_counts_skipped = 0
        
        for person_config in job.person_filters:
            query_name = person_config.get("name", "Unnamed Query")
            
            # Check for existing person count data
            if job.skip_existing_person_counts:
                existing_count = self._find_existing_person_count(company, query_name, job.max_data_age_days)
                if existing_count and existing_count.status == 'ok':
                    logger.debug(f"Skipping person count for {company.name} - {query_name}: existing successful data found")
                    
                    # Create a reference to the existing data for this job
                    person_count_ref = PersonCount(
                        company_id=company.id,
                        job_id=job.id,
                        query_name=query_name,
                        total_count=existing_count.total_count,
                        status=existing_count.status,
                        error_code=existing_count.error_code,
                        prospeo_company_id=company.prospeo_company_id
                    )
                    db.session.add(person_count_ref)
                    person_counts_skipped += 1
                    continue
                elif existing_count and existing_count.status != 'ok':
                    logger.info(f"Re-running person count for {company.name} - {query_name}: existing data had error status '{existing_count.status}'")
            
            # Prepare filters with location resolution
            filters = self._prepare_person_search_filters(job, person_config)
            
            # Try search with company domain first
            domain_root = registrable_root_domain(company.domain or "")
            result = None
            
            if domain_root:
                logger.debug(f"Trying person search for {company.name} - {query_name} with domain: {domain_root}")
                result = self._execute_person_search(filters, domain_root, company, query_name)
                credits_used += 1
                
                # If we got results, we're done
                if result and result.get("total_count", 0) > 0:
                    logger.debug(f"Person search succeeded with domain for {company.name}: {result['total_count']}")
                    self._save_person_count_result(job, company, query_name, result)
                    continue
            
            # Fallback - try with company website if different
            website_root = registrable_root_domain(company.website or "")
            if website_root and website_root != domain_root:
                logger.debug(f"Trying person search fallback for {company.name} - {query_name} with website: {website_root}")
                result = self._execute_person_search(filters, website_root, company, query_name)
                credits_used += 1
                
                logger.debug(f"Person search fallback result for {company.name}: {result.get('total_count', 0)}")
                self._save_person_count_result(job, company, query_name, result)
            elif result:
                # Save the domain result even if it was 0
                self._save_person_count_result(job, company, query_name, result)
            else:
                # No domain or website available
                logger.warning(f"No domain or website available for {company.name}")
                no_domain_result = {
                    "total_count": 0,
                    "status": "error", 
                    "error_code": "NO_DOMAIN_AVAILABLE"
                }
                self._save_person_count_result(job, company, query_name, no_domain_result)
        
        # Update job tracking
        if person_counts_skipped > 0:
            job.person_counts_skipped = (job.person_counts_skipped or 0) + person_counts_skipped
            logger.info(f"JOB {job.id}: Skipped {person_counts_skipped} person count queries (existing data)")
        
        return credits_used
    
    def _prepare_person_search_filters(self, job, person_config):
        """Prepare filters with dynamic location resolution and UI inputs"""
        filters = dict(person_config.get("filters", {}))
        
        # Handle location formatting via Search Suggestions API
        if "person_location_search" in filters:
            includes = filters["person_location_search"].get("include", [])
            resolved_includes = []
            for location in includes:
                resolved = self.client.resolve_location_format(location)
                resolved_includes.append(resolved)
            filters["person_location_search"]["include"] = resolved_includes
        
        # Note: time_in_role from UI is already handled by the frontend
        # The UI widgets.timeRole.getValues() adds person_time_in_current_role if values provided
        
        return filters
    
    def _execute_person_search(self, filters, root_domain, company, query_name):
        """Execute person search with given domain"""
        import logging
        logger = logging.getLogger(__name__)
        
        # Set up company website filter
        search_filters = dict(filters)
        if "company" not in search_filters:
            search_filters["company"] = {}
        if "websites" not in search_filters["company"]:
            search_filters["company"]["websites"] = {"include": [], "exclude": []}
        
        search_filters["company"]["websites"]["include"] = [root_domain]
        
        logger.debug(f"Executing person search for {company.name} - {query_name} with domain: {root_domain}")
        response = self.client.search_people(search_filters, page=1)
        
        result = {"total_count": 0, "status": "ok", "error_code": None}
        
        if self.client.is_error(response):
            result["status"] = "error"
            result["error_code"] = self.client.get_error_code(response)
            logger.warning(f"Person search failed for {company.name} - {query_name} (domain: {root_domain}): {result['error_code']}")
        else:
            pagination = self.client.get_pagination(response)
            result["total_count"] = pagination.get("total_count", 0)
            logger.debug(f"Person search for {company.name} - {query_name} (domain: {root_domain}): {result['total_count']}")
        
        return result
    
    def _save_person_count_result(self, job, company, query_name, result):
        """Save person count result to database with active record management"""
        # Set previous records for this company and query to inactive
        PersonCount.query.filter(
            PersonCount.company_id == company.id,
            PersonCount.query_name == query_name,
            PersonCount.is_active == True
        ).update({"is_active": False})
        
        # Create new active record
        person_count = PersonCount(
            company_id=company.id,
            job_id=job.id,
            query_name=query_name,
            total_count=result.get("total_count", 0),
            status=result.get("status", "ok"),
            error_code=result.get("error_code"),
            prospeo_company_id=company.prospeo_company_id,
            is_active=True  # New record is active by default
        )
        db.session.add(person_count)
    
    def _find_existing_person_count(self, company, query_name, max_age_days):
        """Find existing person count data for a company and query within age limit."""
        max_age = datetime.utcnow() - timedelta(days=max_age_days)
        
        # Look for existing person count by company identifiers
        existing = None
        
        # Primary: by prospeo_company_id and query name
        if company.prospeo_company_id:
            existing = PersonCount.query.filter(
                PersonCount.prospeo_company_id == company.prospeo_company_id,
                PersonCount.query_name == query_name,
                PersonCount.created_at >= max_age,
                PersonCount.is_active == True
            ).first()
        
        # Secondary: by company domain/website and query name
        if not existing and (company.domain or company.website):
            root_domain = registrable_root_domain(company.domain or company.website or "")
            if root_domain:
                # Find companies with same domain
                related_companies = Company.query.filter(
                    or_(
                        Company.domain == root_domain,
                        Company.website == root_domain,
                        Company.domain.like(f'%{root_domain}'),
                        Company.website.like(f'%{root_domain}')
                    )
                ).all()
                
                if related_companies:
                    company_ids = [c.id for c in related_companies]
                    existing = PersonCount.query.filter(
                        PersonCount.company_id.in_(company_ids),
                        PersonCount.query_name == query_name,
                        PersonCount.created_at >= max_age,
                        PersonCount.is_active == True
                    ).first()
        
        return existing

    def _enrich_companies_with_hubspot(self, job):
        """Enrich companies with HubSpot data using batch processing."""
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"Starting HubSpot enrichment for job {job.id}")
            
            # Lazy load HubSpot client to prevent initialization errors from blocking job execution
            if self.hubspot_client is None:
                try:
                    logger.info("Initializing HubSpot client")
                    self.hubspot_client = HubSpotClient()
                except Exception as e:
                    logger.error(f"Failed to initialize HubSpot client: {e}")
                    logger.info(f"HubSpot enrichment skipped for job {job.id} - client initialization failed")
                    return
            
            # Get all companies for this job that need enrichment
            all_companies = Company.query.filter_by(job_id=job.id).all()
            if not all_companies:
                logger.info(f"No companies to enrich with HubSpot for job {job.id}")
                return
            
            # Filter companies based on deduplication settings
            companies_to_enrich = []
            hubspot_skipped = 0
            
            for company in all_companies:
                if job.skip_existing_hubspot:
                    existing_enrichment = self._find_existing_hubspot_enrichment(company, job.max_data_age_days)
                    if existing_enrichment:
                        logger.debug(f"Skipping HubSpot enrichment for {company.name}: existing data found")
                        
                        # Create reference to existing enrichment for this job
                        hubspot_ref = HubSpotEnrichment(
                            company_id=company.id,
                            job_id=job.id,
                            hubspot_object_id=existing_enrichment.hubspot_object_id,
                            vertical=existing_enrichment.vertical,
                            lookup_method=existing_enrichment.lookup_method,
                            hubspot_created_date=existing_enrichment.hubspot_created_date
                        )
                        db.session.add(hubspot_ref)
                        hubspot_skipped += 1
                        continue
                
                companies_to_enrich.append(company)
            
            # Update job tracking
            if hubspot_skipped > 0:
                job.hubspot_skipped = (job.hubspot_skipped or 0) + hubspot_skipped
                logger.info(f"JOB {job.id}: Skipped {hubspot_skipped} HubSpot enrichments (existing data)")
                db.session.commit()
            
            if not companies_to_enrich:
                logger.info(f"No new companies need HubSpot enrichment for job {job.id} (all have existing data)")
                return
            
            logger.info(f"Found {len(companies_to_enrich)} companies to enrich with HubSpot for job {job.id}")
            
            # Check if HubSpot client is enabled
            if not self.hubspot_client.enabled:
                logger.info(f"HubSpot enrichment skipped for job {job.id} - API key not configured")
                return
            
            # Process companies in batches of 50 for efficiency
            batch_size = 50
            total_enriched = 0
            
            for i in range(0, len(companies_to_enrich), batch_size):
                if self._stop_requested:
                    break
                
                batch = companies_to_enrich[i:i + batch_size]
                
                # Prepare batch data for HubSpot client
                batch_data = []
                for company in batch:
                    batch_data.append({
                        'id': company.id,
                        'linkedin_url': company.linkedin_url,
                        'domain': company.domain
                    })
                
                logger.info(f"JOB {job.id}: Processing HubSpot enrichment batch {len(batch_data)} companies")
                
                # Get HubSpot enrichments for this batch
                enrichments = self.hubspot_client.batch_enrich_companies(batch_data)
                
                # Save enrichment results to database with active record management
                for company_id, enrichment_data in enrichments.items():
                    if enrichment_data:
                        # Set previous HubSpot enrichments for this company to inactive
                        HubSpotEnrichment.query.filter(
                            HubSpotEnrichment.company_id == company_id,
                            HubSpotEnrichment.is_active == True
                        ).update({"is_active": False})
                        
                        # Create new active enrichment record
                        hubspot_enrichment = HubSpotEnrichment(
                            company_id=company_id,
                            job_id=job.id,
                            hubspot_object_id=enrichment_data['hubspot_object_id'],
                            vertical=enrichment_data['vertical'],
                            lookup_method=enrichment_data['lookup_method'],
                            hubspot_created_date=enrichment_data['hubspot_created_date'],
                            is_active=True  # New record is active by default
                        )
                        db.session.add(hubspot_enrichment)
                        total_enriched += 1
                
                # Commit batch results
                db.session.commit()
                logger.info(f"Processed HubSpot enrichment batch {i//batch_size + 1}/{(len(companies_to_enrich) + batch_size - 1)//batch_size}")
            
            logger.info(f"HubSpot enrichment completed: {total_enriched} companies enriched out of {len(companies_to_enrich)} (skipped {hubspot_skipped})")
            
        except Exception as e:
            logger.error(f"HubSpot enrichment failed: {e}")
            # Continue job processing even if HubSpot enrichment fails
    
    def _find_existing_hubspot_enrichment(self, company, max_age_days):
        """Find existing HubSpot enrichment data for a company within age limit."""
        max_age = datetime.utcnow() - timedelta(days=max_age_days)
        
        # Look for existing enrichment by company identifiers
        existing = None
        
        # Primary: by company_id
        existing = HubSpotEnrichment.query.filter(
            HubSpotEnrichment.company_id == company.id,
            HubSpotEnrichment.created_at >= max_age,
            HubSpotEnrichment.hubspot_object_id.isnot(None),
            HubSpotEnrichment.is_active == True
        ).first()
        
        if existing:
            return existing
        
        # Secondary: by domain/website across all companies
        if company.domain or company.website:
            root_domain = registrable_root_domain(company.domain or company.website or "")
            if root_domain:
                # Find companies with same domain that have HubSpot enrichment
                related_companies = Company.query.filter(
                    or_(
                        Company.domain == root_domain,
                        Company.website == root_domain,
                        Company.domain.like(f'%{root_domain}'),
                        Company.website.like(f'%{root_domain}')
                    )
                ).all()
                
                if related_companies:
                    company_ids = [c.id for c in related_companies]
                    existing = HubSpotEnrichment.query.filter(
                        HubSpotEnrichment.company_id.in_(company_ids),
                        HubSpotEnrichment.created_at >= max_age,
                        HubSpotEnrichment.hubspot_object_id.isnot(None),
                        HubSpotEnrichment.is_active == True
                    ).first()
        
        return existing


def start_job_async(job_id, app):
    job_runner = MarketSizingJob(job_id)
    thread = threading.Thread(target=job_runner.run, args=(app,))
    thread.daemon = True
    thread.start()
    return job_runner
