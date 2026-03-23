import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    NEWSAPI_KEY = os.environ.get('NEWSAPI_KEY', '')
    GDELT_BASE_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'
    NEWSAPI_BASE_URL = 'https://newsapi.org/v2'
    GDELT_REFRESH_MINUTES = 15
    NEWSAPI_ROTATION_MINUTES = 150  # 2.5 hours between NewsAPI cycles
    REFRESH_INTERVAL_MINUTES = 15   # kept for backward compat
    SCORE_CACHE_TTL_MINUTES = 30
    HOTSPOT_THRESHOLD = 70
    GDELT_TIMESPAN = '24h'
    NEWSAPI_PAGE_SIZE = 20

    # Persistent storage — JSON files survive process restarts on Render
    DATA_DIR = os.environ.get('DATA_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'))
    SCORES_FILE = os.path.join(DATA_DIR, 'scores.json')
    HISTORY_FILE = os.path.join(DATA_DIR, 'history.json')

    # Regional rotation for NewsAPI (10 countries each, ~11 requests per cycle)
    REGIONS = {
        'AMERICAS': ['US', 'BR', 'MX', 'CO', 'VE', 'CU', 'CA', 'AR', 'CL', 'PE'],
        'EUROPE':   ['GB', 'FR', 'DE', 'TR', 'UA', 'RU', 'GE', 'BY', 'PL', 'IT'],
        'MENA':     ['IL', 'PS', 'IR', 'IQ', 'SY', 'SA', 'YE', 'LY', 'EG', 'LB'],
        'AFRICA':   ['NG', 'CD', 'SD', 'SS', 'SO', 'ET', 'ML', 'BF', 'KE', 'ZA'],
        'ASIA_PAC': ['CN', 'IN', 'PK', 'KP', 'TW', 'JP', 'KR', 'TH', 'PH', 'MM'],
    }
    REGION_ORDER = ['AMERICAS', 'EUROPE', 'MENA', 'AFRICA', 'ASIA_PAC']
