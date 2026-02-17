import tldextract
from urllib.parse import urlparse

def hostname_from_url(url):
    if not url:
        return ""
    url = url.strip()
    if "://" in url:
        host = urlparse(url).netloc.lower()
    else:
        host = url.lower()
    host = host.split("/")[0].rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host

def registrable_root_domain(url):
    if not url:
        return ""
    
    host = hostname_from_url(url)
    if not host:
        return ""
    
    extracted = tldextract.extract(host)
    
    if extracted.domain and extracted.suffix:
        return f"{extracted.domain}.{extracted.suffix}"
    
    return host
