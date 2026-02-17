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
    
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=False)
    prospeo_company_id = db.Column(db.String(100), index=True)  # Indexed for upsert lookups
    name = db.Column(db.String(500))
    domain = db.Column(db.String(255))
    website = db.Column(db.String(500))
    root_domain = db.Column(db.String(255))
    industry = db.Column(db.String(255))
    headcount = db.Column(db.Integer)
    headcount_by_department = db.Column(db.JSON)
    location_country = db.Column(db.String(100))
    location_city = db.Column(db.String(255))
    location_state = db.Column(db.String(100))
    founded_year = db.Column(db.Integer)
    funding_stage = db.Column(db.String(100))
    revenue_range = db.Column(db.String(50))
    b2b = db.Column(db.Boolean)
    linkedin_url = db.Column(db.String(500))
    processed = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    person_counts = db.relationship('PersonCount', backref='company', lazy='dynamic')
    
    def to_dict(self):
        return {
            'id': self.id,
            'prospeo_company_id': self.prospeo_company_id,
            'name': self.name,
            'domain': self.domain,
            'website': self.website,
            'root_domain': self.root_domain,
            'industry': self.industry,
            'headcount': self.headcount,
            'headcount_by_department': self.headcount_by_department,
            'location_country': self.location_country,
            'location_city': self.location_city,
            'location_state': self.location_state,
            'founded_year': self.founded_year,
            'funding_stage': self.funding_stage,
            'revenue_range': self.revenue_range,
            'b2b': self.b2b,
            'linkedin_url': self.linkedin_url,
            'person_counts': {pc.query_name: pc.total_count for pc in self.person_counts}
        }


class PersonCount(db.Model):
    __tablename__ = 'person_counts'
    
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=False)
    query_name = db.Column(db.String(100), nullable=False)  # e.g., "SDR Count", "Sales Rep Count"
    query_filters = db.Column(db.JSON)
    total_count = db.Column(db.Integer, default=0)
    sample_titles = db.Column(db.JSON)  # Array of first 25 job titles
    sample_names = db.Column(db.JSON)  # Array of first 25 names
    status = db.Column(db.String(50), default='ok')  # ok, error, no_results
    error_code = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'company_id': self.company_id,
            'query_name': self.query_name,
            'query_filters': self.query_filters,
            'total_count': self.total_count,
            'sample_titles': self.sample_titles,
            'sample_names': self.sample_names,
            'status': self.status,
            'error_code': self.error_code
        }
