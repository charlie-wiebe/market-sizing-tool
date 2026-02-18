import hashlib
import json
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def generate_query_fingerprint(company_filters, person_filters):
    """Generate a hash fingerprint for a query configuration."""
    normalized = json.dumps({
        'company': company_filters,
        'person': person_filters
    }, sort_keys=True)
    return hashlib.sha256(normalized.encode()).hexdigest()[:32]

class Job(db.Model):
    __tablename__ = 'jobs'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(50), default='pending')  # pending, running, completed, failed
    company_filters = db.Column(db.JSON)
    person_filters = db.Column(db.JSON)  # List of person search configs
    query_fingerprint = db.Column(db.String(32), index=True)  # Hash of filters for duplicate detection
    mode = db.Column(db.String(20), default='quick_tam')  # 'quick_tam' or 'detailed'
    aggregate_results = db.Column(db.JSON)  # For quick_tam mode: {query_name: count}
    total_companies = db.Column(db.Integer, default=0)
    processed_companies = db.Column(db.Integer, default=0)
    estimated_credits = db.Column(db.Integer, default=0)
    actual_credits = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text)
    started_at = db.Column(db.DateTime)
    completed_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    companies = db.relationship('Company', backref='job', lazy='dynamic')
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'status': self.status,
            'mode': self.mode,
            'company_filters': self.company_filters,
            'person_filters': self.person_filters,
            'aggregate_results': self.aggregate_results,
            'total_companies': self.total_companies,
            'processed_companies': self.processed_companies,
            'estimated_credits': self.estimated_credits,
            'actual_credits': self.actual_credits,
            'error_message': self.error_message,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'progress_pct': round(self.processed_companies / self.total_companies * 100, 1) if self.total_companies > 0 else 0
        }


class Company(db.Model):
    __tablename__ = 'companies'
    
    # Core fields
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=False)
    prospeo_company_id = db.Column(db.String(100), index=True)  # company_id from Prospeo API
    
    # Basic company information (always present in Prospeo API)
    name = db.Column(db.String(500))
    website = db.Column(db.String(500))
    domain = db.Column(db.String(255))
    description = db.Column(db.Text)
    description_seo = db.Column(db.Text)
    description_ai = db.Column(db.Text)
    company_type = db.Column(db.String(50))  # "Private", "Public", etc.
    industry = db.Column(db.String(255))
    employee_count = db.Column(db.Integer)  # employee_count from API
    employee_range = db.Column(db.String(50))  # "1001-2000"
    founded = db.Column(db.Integer)  # founded year
    other_websites = db.Column(db.JSON)
    keywords = db.Column(db.JSON)
    logo_url = db.Column(db.String(500))
    
    # Location details (nested object in API)
    location_country = db.Column(db.String(100))
    location_city = db.Column(db.String(255))
    location_state = db.Column(db.String(100))
    location_country_code = db.Column(db.String(10))
    location_raw_address = db.Column(db.Text)
    
    # Contact information (nested objects in API)
    email_tech = db.Column(db.JSON)  # domain, mx_provider, catch_all_domain
    phone_hq = db.Column(db.JSON)    # phone_hq, national, international, country, country_code
    
    # Social media URLs
    linkedin_url = db.Column(db.String(500))
    twitter_url = db.Column(db.String(500))
    facebook_url = db.Column(db.String(500))
    crunchbase_url = db.Column(db.String(500))
    instagram_url = db.Column(db.String(500))
    youtube_url = db.Column(db.String(500))
    
    # Revenue information (nested object in API)
    revenue_min = db.Column(db.BigInteger)
    revenue_max = db.Column(db.BigInteger)
    revenue_range_printed = db.Column(db.String(50))  # "$100M"
    
    # Attributes (nested object in API)
    is_b2b = db.Column(db.Boolean)
    has_demo = db.Column(db.Boolean)
    has_free_trial = db.Column(db.Boolean)
    has_downloadable = db.Column(db.Boolean)
    has_mobile_apps = db.Column(db.Boolean)
    has_online_reviews = db.Column(db.Boolean)
    has_pricing = db.Column(db.Boolean)
    
    # Complex data structures (nested objects in API)
    funding = db.Column(db.JSON)        # count, total_funding, latest_funding_date, funding_events
    technology = db.Column(db.JSON)     # count, technology_names, technology_list
    job_postings = db.Column(db.JSON)   # active_count, active_titles
    
    # Classification codes
    sic_codes = db.Column(db.JSON)
    naics_codes = db.Column(db.JSON)
    linkedin_id = db.Column(db.String(100))
    
    # Meta field
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    person_counts = db.relationship('PersonCount', backref='company', lazy='dynamic')
    
    def to_dict(self):
        return {
            # Core fields
            'id': self.id,
            'prospeo_company_id': self.prospeo_company_id,
            'name': self.name,
            'website': self.website,
            'domain': self.domain,
            
            # Basic company information
            'description': self.description,
            'description_seo': self.description_seo,
            'description_ai': self.description_ai,
            'company_type': self.company_type,
            'industry': self.industry,
            'employee_count': self.employee_count,
            'employee_range': self.employee_range,
            'founded': self.founded,
            'other_websites': self.other_websites,
            'keywords': self.keywords,
            'logo_url': self.logo_url,
            
            # Location details
            'location_country': self.location_country,
            'location_city': self.location_city,
            'location_state': self.location_state,
            'location_country_code': self.location_country_code,
            'location_raw_address': self.location_raw_address,
            
            # Contact information
            'email_tech': self.email_tech,
            'phone_hq': self.phone_hq,
            
            # Social media URLs
            'linkedin_url': self.linkedin_url,
            'twitter_url': self.twitter_url,
            'facebook_url': self.facebook_url,
            'crunchbase_url': self.crunchbase_url,
            'instagram_url': self.instagram_url,
            'youtube_url': self.youtube_url,
            
            # Revenue information
            'revenue_min': self.revenue_min,
            'revenue_max': self.revenue_max,
            'revenue_range_printed': self.revenue_range_printed,
            
            # Attributes
            'is_b2b': self.is_b2b,
            'has_demo': self.has_demo,
            'has_free_trial': self.has_free_trial,
            'has_downloadable': self.has_downloadable,
            'has_mobile_apps': self.has_mobile_apps,
            'has_online_reviews': self.has_online_reviews,
            'has_pricing': self.has_pricing,
            
            # Complex data structures
            'funding': self.funding,
            'technology': self.technology,
            'job_postings': self.job_postings,
            
            # Classification codes
            'sic_codes': self.sic_codes,
            'naics_codes': self.naics_codes,
            'linkedin_id': self.linkedin_id,
            
            # Person counts relationship
            'person_counts': {pc.query_name: pc.total_count for pc in self.person_counts}
        }


class PersonCount(db.Model):
    __tablename__ = 'person_counts'
    
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=False)
    query_name = db.Column(db.String(100), nullable=False)  # e.g., "SDR Count", "Sales Rep Count"
    total_count = db.Column(db.Integer, default=0)
    status = db.Column(db.String(50), default='ok')  # ok, error, no_results
    error_code = db.Column(db.String(100))
    prospeo_company_id = db.Column(db.String(100), index=True)  # Link to Prospeo company ID
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'company_id': self.company_id,
            'query_name': self.query_name,
            'total_count': self.total_count,
            'status': self.status,
            'error_code': self.error_code,
            'prospeo_company_id': self.prospeo_company_id
        }
