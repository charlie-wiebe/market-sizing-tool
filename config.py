import os
from dotenv import load_dotenv

load_dotenv()

def get_database_url():
    url = os.getenv("DATABASE_URL")
    
    # Default to SQLite for local dev if no DATABASE_URL set
    if not url:
        basedir = os.path.abspath(os.path.dirname(__file__))
        return f"sqlite:///{os.path.join(basedir, 'market_sizing.db')}"
    
    # Handle Render's DATABASE_URL which uses postgres:// scheme
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://") and "+psycopg" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    
    # Add SSL mode for Render PostgreSQL
    if "render.com" in url or "dpg-" in url:
        if "?" in url:
            url += "&sslmode=require"
        else:
            url += "?sslmode=require"
    
    return url

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-prod")
    SQLALCHEMY_DATABASE_URI = get_database_url()
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Engine options to handle connection pooling and SSL issues
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,  # Test connections before using
        "pool_recycle": 300,    # Recycle connections every 5 min
    }
    
    PROSPEO_API_KEY = os.getenv("PROSPEO_API_KEY")
    PROSPEO_BASE_URL = "https://api.prospeo.io"
    
    # Rate limits
    PROSPEO_MAX_PER_SECOND = 30
    PROSPEO_MAX_PER_MINUTE = 1800
