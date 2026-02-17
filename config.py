import os
from dotenv import load_dotenv

load_dotenv()

def get_database_url():
    # Check for persistent disk on Render (mounted at /data)
    if os.path.exists("/data"):
        return "sqlite:////data/market_sizing.db"
    
    # Local development - use local SQLite file
    basedir = os.path.abspath(os.path.dirname(__file__))
    return f"sqlite:///{os.path.join(basedir, 'market_sizing.db')}"

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-prod")
    SQLALCHEMY_DATABASE_URI = get_database_url()
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    PROSPEO_API_KEY = os.getenv("PROSPEO_API_KEY")
    PROSPEO_BASE_URL = "https://api.prospeo.io"
    
    # Rate limits
    PROSPEO_MAX_PER_SECOND = 30
    PROSPEO_MAX_PER_MINUTE = 1800
