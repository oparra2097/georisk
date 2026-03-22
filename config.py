import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    NEWSAPI_KEY = os.environ.get('NEWSAPI_KEY', '')
    GDELT_BASE_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'
    NEWSAPI_BASE_URL = 'https://newsapi.org/v2'
    REFRESH_INTERVAL_MINUTES = 15
    SCORE_CACHE_TTL_MINUTES = 30
    HOTSPOT_THRESHOLD = 70
    GDELT_TIMESPAN = '24h'
    NEWSAPI_PAGE_SIZE = 20
