"""
GDELT DOC 2.0 API client.

GDELT provides two key data signals:
1. Article volume per theme (how many articles mention conflict, protests, etc.)
2. Media tone (how negative/positive is the coverage)

The artlist mode returns article metadata but NO tone.
The timelinetone mode returns tone data over time.
We use both.
"""

import requests
import logging
import time
from backend.data_sources.country_codes import iso_alpha2_to_name

logger = logging.getLogger(__name__)

GDELT_DOC_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'

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

INDICATOR_THEMES = {
    'political_stability': [
        'LEADER', 'ELECTION', 'CORRUPTION'
    ],
    'military_conflict': [
        'MILITARY', 'KILL', 'WOUND'
    ],
    'economic_sanctions': [
        'ECON_SANCTIONS', 'EMBARGO'
    ],
    'protests_civil_unrest': [
        'PROTEST', 'RIOT'
    ],
    'terrorism': [
        'TERROR', 'HOSTAGE'
    ],
    'diplomatic_tensions': [
        'THREAT', 'DEMAND', 'REJECT'
    ]
}


def _get_search_name(country_alpha2):
    """Get the best search term for a country."""
    if country_alpha2 in COUNTRY_SEARCH_NAMES:
        return COUNTRY_SEARCH_NAMES[country_alpha2]
    return iso_alpha2_to_name(country_alpha2)


def _gdelt_request(params, timeout=12):
    """Make a GDELT API request with retry."""
    for attempt in range(2):
        try:
            resp = requests.get(GDELT_DOC_URL, params=params, timeout=timeout)
            if resp.status_code == 200:
                text = resp.text.strip()
                if text and text.startswith('{'):
                    return resp.json()
            elif resp.status_code == 429:
                time.sleep(1)
                continue
        except requests.exceptions.Timeout:
            logger.debug(f"GDELT timeout (attempt {attempt+1})")
        except Exception as e:
            logger.debug(f"GDELT request error: {e}")
    return None


def fetch_country_articles(country_alpha2, timespan='24h', max_records=75):
    """Fetch recent articles about a country from GDELT."""
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


def fetch_country_tone(country_alpha2, timespan='24h'):
    """
    Fetch average tone for a country using timelinetone mode.
    Returns a float from roughly -10 (very negative) to +10 (positive).
    """
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


def fetch_theme_volume(country_alpha2, indicator_name, timespan='24h'):
    """Fetch article volume for specific themes related to an indicator."""
    search_name = _get_search_name(country_alpha2)
    themes = INDICATOR_THEMES.get(indicator_name, [])
    if not themes:
        return 0

    theme_query = ' OR '.join(themes)
    params = {
        'query': f'{search_name} ({theme_query})',
        'mode': 'artlist',
        'maxrecords': 250,
        'timespan': timespan,
        'format': 'json'
    }
    data = _gdelt_request(params)
    if data:
        articles = data.get('articles', [])
        return len(articles) if articles else 0
    return 0


def fetch_country_volume(country_alpha2, timespan='24h'):
    """Fetch total article volume for a country (used for source breadth)."""
    search_name = _get_search_name(country_alpha2)
    params = {
        'query': search_name,
        'mode': 'artlist',
        'maxrecords': 250,
        'timespan': timespan,
        'format': 'json'
    }
    data = _gdelt_request(params)
    if data:
        articles = data.get('articles', [])
        return len(articles) if articles else 0
    return 0


def fetch_country_data(country_alpha2, timespan='24h'):
    """
    Fetch all GDELT data for a country in parallel:
    1. Articles (for headlines and NLP analysis)
    2. Tone (separate API call since artlist doesn't include tone)
    3. Theme volumes per indicator (6 calls)

    All 8 HTTP calls run concurrently via ThreadPoolExecutor.
    """
    from concurrent.futures import ThreadPoolExecutor

    result = {
        'articles': [],
        'article_count': 0,
        'avg_tone': 0.0,
        'theme_volumes': {}
    }

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all calls at once
        art_future = executor.submit(fetch_country_articles, country_alpha2, timespan)
        tone_future = executor.submit(fetch_country_tone, country_alpha2, timespan)

        theme_futures = {}
        for indicator in INDICATOR_THEMES:
            theme_futures[indicator] = executor.submit(
                fetch_theme_volume, country_alpha2, indicator, timespan
            )

        # Collect results
        result['articles'] = art_future.result()
        result['article_count'] = len(result['articles'])
        result['avg_tone'] = tone_future.result()

        for indicator, future in theme_futures.items():
            result['theme_volumes'][indicator] = future.result()

    return result
