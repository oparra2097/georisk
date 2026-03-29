import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # --- API Keys (set as env vars on Render) ---
    NEWSAPI_KEY = os.environ.get('NEWSAPI_KEY', '')
    NEWSDATA_KEY = os.environ.get('NEWSDATA_KEY', '')
    GNEWS_KEY = os.environ.get('GNEWS_KEY', '')
    BLS_API_KEY = os.environ.get('BLS_API_KEY', '')
    ACLED_EMAIL = os.environ.get('ACLED_EMAIL', '')
    ACLED_PASSWORD = os.environ.get('ACLED_PASSWORD', '')
    ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
    ANTHROPIC_MODEL = 'claude-sonnet-4-20250514'

    GDELT_BASE_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'
    NEWSAPI_BASE_URL = 'https://newsapi.org/v2'
    GDELT_REFRESH_MINUTES = 15
    NEWS_ROTATION_MINUTES = 120   # 2 hours between news provider cycles
    REFRESH_INTERVAL_MINUTES = 15
    SCORE_CACHE_TTL_MINUTES = 30
    HOTSPOT_THRESHOLD = 70
    GDELT_TIMESPAN = '72h'         # 3-day rolling window

    # --- Two-tier scoring ---
    BASE_SCORE_WEIGHT = 0.30       # World Bank WGI + macro fundamentals
    NEWS_SCORE_WEIGHT = 0.70       # High-frequency news signal
    EMA_ALPHA = 0.5                # Blending: 50% new data, 50% existing
    WGI_CACHE_DAYS = 30            # Refresh base data monthly

    # Persistent storage — survives process restarts on Render
    DATA_DIR = os.environ.get('DATA_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'))
    SCORES_FILE = os.path.join(DATA_DIR, 'scores.json')
    HISTORY_FILE = os.path.join(DATA_DIR, 'history.json')
    DB_FILE = os.path.join(DATA_DIR, 'georisk.db')
    HISTORY_RETENTION_DAYS = 365

    # --- Multi-provider regional assignment ---
    # Each region is assigned a news API provider to spread request budget.
    #
    # NewsAPI.org  (100 req/day): AMERICAS (10 countries, ~11 req/cycle)
    # NewsData.io  (200 req/day): EUROPE + MENA  (20 countries, ~20 req/cycle)
    # GNews API    (100 req/day): AFRICA + ASIA_PAC (20 countries, ~20 req/cycle)
    #
    # GDELT (unlimited) runs for ALL 50 countries every 15 min — no key needed.

    REGIONS = {
        'AMERICAS': ['US', 'BR', 'MX', 'CO', 'VE', 'CU', 'CA', 'AR', 'CL', 'PE'],
        'EUROPE':   ['GB', 'FR', 'DE', 'TR', 'UA', 'RU', 'GE', 'BY', 'PL', 'IT'],
        'MENA':     ['IL', 'PS', 'IR', 'IQ', 'SY', 'SA', 'YE', 'LY', 'EG', 'LB'],
        'AFRICA':   ['NG', 'CD', 'SD', 'SS', 'SO', 'ET', 'ML', 'BF', 'KE', 'ZA'],
        'ASIA_PAC': ['CN', 'IN', 'PK', 'KP', 'TW', 'JP', 'KR', 'TH', 'PH', 'MM'],
    }
    REGION_ORDER = ['AMERICAS', 'EUROPE', 'MENA', 'AFRICA', 'ASIA_PAC']

    # Maps each region to its news provider
    REGION_PROVIDER = {
        'AMERICAS': 'newsapi',      # NewsAPI.org
        'EUROPE':   'newsdata',     # NewsData.io
        'MENA':     'newsdata',     # NewsData.io
        'AFRICA':   'gnews',        # GNews API
        'ASIA_PAC': 'gnews',        # GNews API
    }
