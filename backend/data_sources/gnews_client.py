"""
GNews API v4 client.

Free tier: 100 requests/day, 10 requests/minute, max 10 articles/request.
Used for AFRICA + ASIA_PAC regions (20 countries).
Endpoint: https://gnews.io/api/v4/search
"""

import requests
import logging
import threading
from datetime import date
from config import Config
from backend.data_sources.country_codes import iso_alpha2_to_name

logger = logging.getLogger(__name__)

# Daily budget tracking (100 req/day limit, keep 10 buffer)
_budget_lock = threading.Lock()
_daily_budget = {'count': 0, 'date': None}
_DAILY_LIMIT = 90


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

GNEWS_SEARCH_URL = 'https://gnews.io/api/v4/search'
GNEWS_HEADLINES_URL = 'https://gnews.io/api/v4/top-headlines'

# GNews uses lowercase ISO 3166-1 alpha-2 for country filter
GNEWS_SUPPORTED = {
    'au', 'at', 'be', 'br', 'ca', 'cn', 'co', 'cu', 'cz', 'eg',
    'fr', 'de', 'gr', 'hk', 'hu', 'in', 'id', 'ie', 'il', 'it',
    'jp', 'kr', 'lv', 'lt', 'my', 'mx', 'ma', 'nl', 'nz', 'ng',
    'no', 'ph', 'pl', 'pt', 'ro', 'ru', 'sa', 'rs', 'sg', 'sk',
    'si', 'za', 'es', 'se', 'ch', 'tw', 'th', 'tr', 'ua', 'ae',
    'gb', 'us', 've',
}


def _get_key():
    return Config.GNEWS_KEY


def fetch_headlines_for_country(country_alpha2, max_articles=10):
    """Fetch headlines for a specific country from GNews API."""
    key = _get_key()
    if not key:
        return []

    if not _check_budget():
        logger.debug("GNews daily budget exhausted — skipping request")
        return []

    code = country_alpha2.lower()
    articles = []

    try:
        if code in GNEWS_SUPPORTED:
            # Use top-headlines with country filter
            params = {
                'apikey': key,
                'country': code,
                'lang': 'en',
                'max': max_articles,
            }
            url = GNEWS_HEADLINES_URL
        else:
            # Fallback: search by country name
            country_name = iso_alpha2_to_name(country_alpha2)
            params = {
                'apikey': key,
                'q': country_name,
                'lang': 'en',
                'max': max_articles,
            }
            url = GNEWS_SEARCH_URL

        resp = requests.get(url, params=params, timeout=12)
        if resp.status_code == 200:
            data = resp.json()
            raw = data.get('articles', [])
            for art in raw:
                source = art.get('source', {})
                articles.append({
                    'title': art.get('title', ''),
                    'description': art.get('description', '') or '',
                    'url': art.get('url', ''),
                    'source': source.get('name', 'Unknown') if isinstance(source, dict) else 'Unknown',
                    'publishedAt': art.get('publishedAt', ''),
                })
        elif resp.status_code == 403:
            logger.warning("GNews API rate limit or auth error")
        elif resp.status_code == 429:
            logger.warning("GNews API rate limit reached")
        else:
            logger.warning(f"GNews error {resp.status_code} for {country_alpha2}")
    except Exception as e:
        logger.warning(f"GNews fetch failed for {country_alpha2}: {e}")

    return articles
