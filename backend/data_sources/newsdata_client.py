"""
NewsData.io API client.

Free tier: 200 requests/day, 30 requests/15 min.
Used for EUROPE + MENA regions (20 countries).
Endpoint: https://newsdata.io/api/1/latest
"""

import requests
import logging
import threading
from datetime import date
from config import Config
from backend.data_sources.country_codes import iso_alpha2_to_name

logger = logging.getLogger(__name__)

# Daily budget tracking (200 req/day limit, keep 10 buffer)
_budget_lock = threading.Lock()
_daily_budget = {'count': 0, 'date': None}
_DAILY_LIMIT = 190


def _check_budget():
    """Check if we have remaining API budget for today."""
    today = date.today()
    with _budget_lock:
        if _daily_budget['date'] != today:
            _daily_budget['count'] = 0
            _daily_budget['date'] = today
        if _daily_budget['count'] >= _DAILY_LIMIT:
            return False
        _daily_budget['count'] += 1
        return True

NEWSDATA_URL = 'https://newsdata.io/api/1/latest'

# NewsData.io uses lowercase ISO 3166-1 alpha-2 country codes
NEWSDATA_SUPPORTED = {
    'af', 'al', 'dz', 'ao', 'ar', 'am', 'au', 'at', 'az', 'bh', 'bd',
    'bb', 'by', 'be', 'bm', 'bo', 'ba', 'br', 'bn', 'bg', 'bf', 'kh',
    'cm', 'ca', 'cl', 'cn', 'co', 'cd', 'cr', 'ci', 'hr', 'cu', 'cy',
    'cz', 'dk', 'do', 'ec', 'eg', 'sv', 'ee', 'et', 'fi', 'fr', 'ge',
    'de', 'gh', 'gr', 'gt', 'hn', 'hk', 'hu', 'is', 'in', 'id', 'iq',
    'ie', 'il', 'it', 'jm', 'jp', 'jo', 'kz', 'ke', 'kw', 'lv', 'lb',
    'ly', 'lt', 'lu', 'mo', 'mk', 'mg', 'my', 'ml', 'mx', 'mn', 'me',
    'ma', 'mz', 'mm', 'na', 'np', 'nl', 'nz', 'ne', 'ng', 'kp', 'no',
    'om', 'pk', 'pa', 'py', 'pe', 'ph', 'pl', 'pt', 'pr', 'qa', 'ro',
    'ru', 'rw', 'sa', 'sn', 'rs', 'sg', 'sk', 'si', 'so', 'za', 'kr',
    'es', 'lk', 'sd', 'se', 'ch', 'sy', 'tw', 'tz', 'th', 'tn', 'tr',
    'ua', 'ae', 'gb', 'us', 'uy', 've', 'vn', 'ye', 'zm', 'zw',
}


def _get_key():
    return Config.NEWSDATA_KEY


def fetch_headlines_for_country(country_alpha2, page_size=10):
    """Fetch headlines for a specific country from NewsData.io."""
    key = _get_key()
    if not key:
        return []

    if not _check_budget():
        logger.debug("NewsData.io daily budget exhausted — skipping request")
        return []

    code = country_alpha2.lower()
    articles = []

    try:
        params = {
            'apikey': key,
            'language': 'en',
            'size': page_size,
        }

        if code in NEWSDATA_SUPPORTED:
            params['country'] = code
        else:
            # Fallback: search by country name
            country_name = iso_alpha2_to_name(country_alpha2)
            params['q'] = country_name

        resp = requests.get(NEWSDATA_URL, params=params, timeout=12)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success':
                raw = data.get('results', [])
                for art in raw:
                    articles.append({
                        'title': art.get('title', ''),
                        'description': art.get('description', '') or '',
                        'url': art.get('link', ''),
                        'source': art.get('source_id', 'Unknown'),
                        'publishedAt': art.get('pubDate', ''),
                    })
        elif resp.status_code == 429:
            logger.warning("NewsData.io rate limit reached")
        else:
            logger.warning(f"NewsData.io error {resp.status_code} for {country_alpha2}")
    except Exception as e:
        logger.warning(f"NewsData.io fetch failed for {country_alpha2}: {e}")

    return articles
