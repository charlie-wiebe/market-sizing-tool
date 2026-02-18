import re
from urllib.parse import urlparse

def extract_linkedin_handle(linkedin_url):
    """
    Extract LinkedIn handle from LinkedIn URL.
    
    Args:
        linkedin_url (str): LinkedIn company URL
        
    Returns:
        str: LinkedIn handle in format "company/companyname" or None if invalid
        
    Examples:
        "https://linkedin.com/company/rippling" -> "company/rippling"
        "https://www.linkedin.com/company/hubspot/" -> "company/hubspot"
        "linkedin.com/company/salesforce" -> "company/salesforce"
    """
    if not linkedin_url:
        return None
    
    try:
        # Clean up the URL - add https if missing
        if not linkedin_url.startswith(('http://', 'https://')):
            linkedin_url = 'https://' + linkedin_url
        
        # Parse the URL
        parsed = urlparse(linkedin_url)
        
        # Check if it's a LinkedIn URL
        if 'linkedin.com' not in parsed.netloc.lower():
            return None
        
        # Extract the path and clean it
        path = parsed.path.strip('/')
        
        # Split path components
        path_parts = [part for part in path.split('/') if part]
        
        # Look for company path pattern
        if len(path_parts) >= 2 and path_parts[0].lower() == 'company':
            company_name = path_parts[1].lower()
            # Clean company name (remove special characters, keep alphanumeric and hyphens)
            company_name = re.sub(r'[^a-z0-9\-]', '', company_name)
            if company_name:
                return f"company/{company_name}"
        
        return None
        
    except Exception:
        return None


def normalize_domain(domain):
    """
    Normalize domain for HubSpot lookup.
    
    Args:
        domain (str): Domain name
        
    Returns:
        str: Normalized domain or None if invalid
    """
    if not domain:
        return None
    
    try:
        # Remove protocol if present
        domain = re.sub(r'^https?://', '', domain)
        
        # Remove www prefix
        domain = re.sub(r'^www\.', '', domain)
        
        # Remove path and query parameters
        domain = domain.split('/')[0].split('?')[0]
        
        # Convert to lowercase
        domain = domain.lower().strip()
        
        # Basic validation - must contain at least one dot and valid characters
        if '.' in domain and re.match(r'^[a-z0-9\-\.]+$', domain):
            return domain
        
        return None
        
    except Exception:
        return None
