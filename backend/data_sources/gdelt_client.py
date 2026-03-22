import requests
import logging
from backend.data_sources.country_codes import iso_alpha2_to_fips, iso_alpha2_to_name

logger = logging.getLogger(__name__)

GDELT_DOC_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'

INDICATOR_THEMES = {
    'political_stability': [
        'TAX_POLITICAL_INSTABILITY', 'LEADER', 'ELECTION', 'COUP', 'CORRUPTION'
    ],
    'military_conflict': [
        'MILITARY', 'ARMED_CONFLICT', 'KILL', 'WOUND'
    ],
    'economic_sanctions': [
        'ECON_SANCTIONS', 'TRADE_DISPUTE', 'EMBARGO'
    ],
    'protests_civil_unrest': [
        'PROTEST', 'CIVIL_UNREST', 'RIOT', 'DEMONSTRATION'
    ],
    'terrorism': [
        'TERROR', 'SUICIDE_ATTACK', 'HOSTAGE'
    ],
    'diplomatic_tensions': [
        'DIPLOMATIC_EXCHANGE', 'THREAT', 'DEMAND', 'REJECT'
    ]
}


def fetch_country_articles(country_alpha2, timespan='24h', max_records=50):
    """Fetch recent articles about a country from GDELT."""
    country_name = iso_alpha2_to_name(country_alpha2)
    try:
        params = {
            'query': f'"{country_name}"',
            'mode': 'artlist',
            'maxrecords': max_records,
            'timespan': timespan,
            'format': 'json',
            'sort': 'datedesc'
        }
        resp = requests.get(GDELT_DOC_URL, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('articles', [])
    except Exception as e:
        logger.warning(f"GDELT article fetch failed for {country_alpha2}: {e}")
    return []


def fetch_country_tone(country_alpha2, timespan='7d'):
    """Fetch average tone for a country from GDELT."""
    country_name = iso_alpha2_to_name(country_alpha2)
    try:
        params = {
            'query': f'"{country_name}"',
            'mode': 'timelinetone',
            'timespan': timespan,
            'format': 'json'
        }
        resp = requests.get(GDELT_DOC_URL, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            timeline = data.get('timeline', [])
            if timeline and len(timeline) > 0:
                series = timeline[0].get('data', [])
                if series:
                    tones = [pt.get('value', 0) for pt in series if 'value' in pt]
                    if tones:
                        return sum(tones) / len(tones)
    except Exception as e:
        logger.warning(f"GDELT tone fetch failed for {country_alpha2}: {e}")
    return 0.0


def fetch_theme_volume(country_alpha2, indicator_name, timespan='24h'):
    """Fetch article volume for specific themes related to an indicator."""
    country_name = iso_alpha2_to_name(country_alpha2)
    themes = INDICATOR_THEMES.get(indicator_name, [])
    if not themes:
        return 0

    total_volume = 0
    theme_query = ' OR '.join(f'theme:{t}' for t in themes)
    try:
        params = {
            'query': f'"{country_name}" ({theme_query})',
            'mode': 'artlist',
            'maxrecords': 250,
            'timespan': timespan,
            'format': 'json'
        }
        resp = requests.get(GDELT_DOC_URL, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            articles = data.get('articles', [])
            total_volume = len(articles) if articles else 0
    except Exception as e:
        logger.warning(f"GDELT theme volume failed for {country_alpha2}/{indicator_name}: {e}")

    return total_volume


def fetch_country_data(country_alpha2, timespan='24h'):
    """Fetch all GDELT data for a country in one pass."""
    country_name = iso_alpha2_to_name(country_alpha2)
    result = {
        'articles': [],
        'article_count': 0,
        'avg_tone': 0.0,
        'theme_volumes': {}
    }

    try:
        params = {
            'query': f'"{country_name}"',
            'mode': 'artlist',
            'maxrecords': 100,
            'timespan': timespan,
            'format': 'json',
            'sort': 'datedesc'
        }
        resp = requests.get(GDELT_DOC_URL, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            articles = data.get('articles', [])
            result['articles'] = articles or []
            result['article_count'] = len(result['articles'])

            if result['articles']:
                tones = []
                for art in result['articles']:
                    tone = art.get('tone', 0)
                    if isinstance(tone, (int, float)):
                        tones.append(tone)
                if tones:
                    result['avg_tone'] = sum(tones) / len(tones)
    except Exception as e:
        logger.warning(f"GDELT fetch failed for {country_alpha2}: {e}")

    for indicator in INDICATOR_THEMES:
        result['theme_volumes'][indicator] = fetch_theme_volume(
            country_alpha2, indicator, timespan
        )

    return result
