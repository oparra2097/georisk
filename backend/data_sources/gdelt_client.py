"""
GDELT DOC 2.0 API client — rate-limit aware, single-threaded.

Two API calls per country:
1. artlist — up to 150 articles for NLP keyword analysis + source breadth
2. timelinetone — average media tone

Theme volumes are derived from keyword analysis of article titles
instead of separate GDELT theme queries (saves 6 API calls per country).

GDELT rate limits: ~1 request per second sustained.
With 187 countries × 2 calls = 374 requests per refresh.
At 1.5s interval (single-threaded): ~9.5 minutes per full cycle.
"""

import requests
import logging
import time
import threading
from backend.data_sources.country_codes import iso_alpha2_to_name
from config import Config

logger = logging.getLogger(__name__)

GDELT_DOC_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'

# Global rate limiter — max ~0.67 requests/sec to GDELT
_rate_lock = threading.Lock()
_last_request_time = 0.0
_MIN_REQUEST_INTERVAL = 1.5  # seconds between requests (≈0.67 req/s)

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
    'PS': 'Gaza',
    'SS': 'South Sudan',
    'ZA': 'South Africa',
    'NZ': 'New Zealand',
    'TL': 'Timor-Leste OR East Timor',
    'MK': 'North Macedonia',
}

# Fallback search terms for countries where the primary query returns zero results
COUNTRY_FALLBACK_TERMS = {
    'PS': ['Palestinian', 'West Bank'],
    'CD': ['DRC Congo Kinshasa'],
    'SS': ['South Sudan Juba'],
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
    max_attempts = 5
    for attempt in range(max_attempts):
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
                wait = 3 ** attempt + 2  # 3s, 5s, 11s, 29s, 83s
                logger.debug(f"GDELT 429, waiting {wait}s (attempt {attempt+1}/{max_attempts})")
                time.sleep(wait)
                continue
            else:
                logger.debug(f"GDELT {resp.status_code} for {params.get('query', '?')}")
                return None
        except requests.exceptions.Timeout:
            logger.debug(f"GDELT timeout (attempt {attempt+1}/{max_attempts})")
            time.sleep(2)
        except Exception as e:
            logger.debug(f"GDELT request error: {e}")
            return None
    logger.warning(f"GDELT failed after {max_attempts} attempts: {params.get('query', '?')} ({params.get('mode', '?')})")
    return None


def fetch_country_articles(country_alpha2, timespan=None, max_records=150):
    """Fetch recent articles about a country from GDELT (up to 250 max)."""
    if timespan is None:
        timespan = Config.GDELT_TIMESPAN
    search_name = _get_search_name(country_alpha2)
    params = {
        'query': f'"{search_name}" sourcelang:english',
        'mode': 'artlist',
        'maxrecords': max_records,
        'timespan': timespan,
        'format': 'json',
        'sort': 'datedesc'
    }
    data = _gdelt_request(params)
    articles = data.get('articles', []) if data else []

    # Always merge in fallback-term results when available — this catches
    # city-level and figure-name coverage that the canonical country-name
    # query misses (e.g. "Juba" or "SPLM-IO" stories about South Sudan).
    if country_alpha2 in COUNTRY_FALLBACK_TERMS:
        existing_urls = {a.get('url') for a in articles if a.get('url')}
        for fallback_term in COUNTRY_FALLBACK_TERMS[country_alpha2]:
            params['query'] = f'"{fallback_term}" sourcelang:english'
            fb_data = _gdelt_request(params)
            if not fb_data:
                continue
            fb_articles = fb_data.get('articles', []) or []
            added = 0
            for art in fb_articles:
                url = art.get('url')
                if url and url in existing_urls:
                    continue
                articles.append(art)
                if url:
                    existing_urls.add(url)
                added += 1
            if added:
                logger.debug(f"GDELT fallback '{fallback_term}' added "
                             f"{added} articles for {country_alpha2}")

    return articles


def _extract_tone(data):
    """Extract average tone from GDELT timelinetone response."""
    if not data:
        return None
    timeline = data.get('timeline', [])
    if timeline and len(timeline) > 0:
        series = timeline[0].get('data', [])
        if series:
            tones = [pt.get('value', 0) for pt in series
                     if isinstance(pt.get('value'), (int, float))]
            if tones:
                return sum(tones) / len(tones)
    return None


def fetch_country_tone(country_alpha2, timespan=None):
    """
    Fetch average tone for a country using timelinetone mode.
    Returns a float from roughly -10 (very negative) to +10 (positive).
    """
    if timespan is None:
        timespan = Config.GDELT_TIMESPAN
    search_name = _get_search_name(country_alpha2)
    params = {
        'query': f'"{search_name}" sourcelang:english',
        'mode': 'timelinetone',
        'timespan': timespan,
        'format': 'json'
    }
    data = _gdelt_request(params)
    tone = _extract_tone(data)

    # Fallback: if no tone data and country has alternate search terms, retry
    if tone is None and country_alpha2 in COUNTRY_FALLBACK_TERMS:
        for fallback_term in COUNTRY_FALLBACK_TERMS[country_alpha2]:
            params['query'] = f'"{fallback_term}" sourcelang:english'
            data = _gdelt_request(params)
            tone = _extract_tone(data)
            if tone is not None:
                break

    return tone if tone is not None else 0.0


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
