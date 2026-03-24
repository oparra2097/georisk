"""
GDELT DOC 2.0 API client — rate-limit aware.

Two API calls per country:
1. artlist — articles for NLP keyword analysis + source breadth
2. timelinetone — average media tone

Theme volumes are derived from keyword analysis of article titles
instead of separate GDELT theme queries (saves 6 API calls per country).

GDELT rate limits: ~1 request per second sustained.
With 187 countries × 2 calls = 374 requests per refresh.
At 1 req/s that's ~6 minutes. With 2 workers: ~3 minutes.
"""

import requests
import logging
import time
import threading
from backend.data_sources.country_codes import iso_alpha2_to_name
from config import Config

logger = logging.getLogger(__name__)

GDELT_DOC_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'

# Global rate limiter — max ~2 requests/sec to GDELT
_rate_lock = threading.Lock()
_last_request_time = 0.0
_MIN_REQUEST_INTERVAL = 0.6  # seconds between requests (≈1.7 req/s)

# Short names for countries that have problematic full names in GDELT
COUNTRY_SEARCH_NAMES = {
    'US': 'United States',
    'GB': 'United Kingdom',
    'KR': 'South Korea',
    'KP': 'North Korea',
    'CD': 'Congo',
    'CG': 'Congo',
    'CF': 'Central African Republic',
    'BA': 'Bosnia',
    'AE': 'UAE',
    'SA': 'Saudi Arabia',
    'PS': 'Palestine OR Gaza',
    'SS': 'South Sudan',
    'ZA': 'South Africa',
    'NZ': 'New Zealand',
    'TL': 'Timor-Leste OR East Timor',
    'MK': 'North Macedonia',
}


def _get_search_name(country_alpha2):
    """Get the best search term for a country."""
    if country_alpha2 in COUNTRY_SEARCH_NAMES:
        return COUNTRY_SEARCH_NAMES[country_alpha2]
    return iso_alpha2_to_name(country_alpha2)


def _rate_limit():
    """Enforce minimum interval between GDELT requests (thread-safe)."""
    global _last_request_time
    with _rate_lock:
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        _last_request_time = time.time()


def _gdelt_request(params, timeout=15):
    """Make a GDELT API request with exponential backoff on 429s."""
    for attempt in range(4):
        _rate_limit()
        try:
            resp = requests.get(GDELT_DOC_URL, params=params, timeout=timeout)
            if resp.status_code == 200:
                text = resp.text.strip()
                if text and text.startswith('{'):
                    return resp.json()
                # Empty or non-JSON response
                return None
            elif resp.status_code == 429:
                wait = 2 ** attempt + 1  # 2s, 3s, 5s, 9s
                logger.debug(f"GDELT 429, waiting {wait}s (attempt {attempt+1}/4)")
                time.sleep(wait)
                continue
            else:
                logger.debug(f"GDELT {resp.status_code} for {params.get('query', '?')}")
                return None
        except requests.exceptions.Timeout:
            logger.debug(f"GDELT timeout (attempt {attempt+1}/4)")
            time.sleep(1)
        except Exception as e:
            logger.debug(f"GDELT request error: {e}")
            return None
    logger.warning(f"GDELT failed after 4 attempts: {params.get('query', '?')} ({params.get('mode', '?')})")
    return None


def fetch_country_articles(country_alpha2, timespan=None, max_records=75):
    """Fetch recent articles about a country from GDELT."""
    if timespan is None:
        timespan = Config.GDELT_TIMESPAN
    search_name = _get_search_name(country_alpha2)
    params = {
        'query': search_name,
        'mode': 'artlist',
        'maxrecords': max_records,
        'timespan': timespan,
        'format': 'json',
        'sort': 'datedesc'
    }
    data = _gdelt_request(params)
    if data:
        return data.get('articles', [])
    return []


def fetch_country_tone(country_alpha2, timespan=None):
    """
    Fetch average tone for a country using timelinetone mode.
    Returns a float from roughly -10 (very negative) to +10 (positive).
    """
    if timespan is None:
        timespan = Config.GDELT_TIMESPAN
    search_name = _get_search_name(country_alpha2)
    params = {
        'query': search_name,
        'mode': 'timelinetone',
        'timespan': timespan,
        'format': 'json'
    }
    data = _gdelt_request(params)
    if data:
        timeline = data.get('timeline', [])
        if timeline and len(timeline) > 0:
            series = timeline[0].get('data', [])
            if series:
                tones = [pt.get('value', 0) for pt in series
                         if isinstance(pt.get('value'), (int, float))]
                if tones:
                    return sum(tones) / len(tones)
    return 0.0


def fetch_country_data(country_alpha2, timespan=None):
    """
    Fetch GDELT data for a country — only 2 API calls:
    1. Articles (for headlines, NLP analysis, and source breadth)
    2. Tone (separate call since artlist doesn't include tone)

    Theme volumes are NO LONGER fetched here — they're derived from
    keyword analysis of article titles in the scoring engine.
    """
    if timespan is None:
        timespan = Config.GDELT_TIMESPAN
    articles = fetch_country_articles(country_alpha2, timespan)
    tone = fetch_country_tone(country_alpha2, timespan)

    return {
        'articles': articles,
        'article_count': len(articles),
        'avg_tone': tone,
    }
