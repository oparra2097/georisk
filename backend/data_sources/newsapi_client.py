import requests
import logging
from config import Config
from backend.data_sources.country_codes import iso_alpha2_to_name

logger = logging.getLogger(__name__)

NEWSAPI_SUPPORTED = {
    'ae', 'ar', 'at', 'au', 'be', 'bg', 'br', 'ca', 'ch', 'cn',
    'co', 'cu', 'cz', 'de', 'eg', 'fr', 'gb', 'gr', 'hk', 'hu',
    'id', 'ie', 'il', 'in', 'it', 'jp', 'kr', 'lt', 'lv', 'ma',
    'mx', 'my', 'ng', 'nl', 'no', 'nz', 'ph', 'pl', 'pt', 'ro',
    'rs', 'ru', 'sa', 'se', 'sg', 'si', 'sk', 'th', 'tr', 'tw',
    'ua', 'us', 've', 'za'
}


def _get_key():
    return Config.NEWSAPI_KEY


def fetch_headlines_for_country(country_alpha2, page_size=20):
    """Fetch headlines for a specific country."""
    key = _get_key()
    if not key or key == 'your_api_key_here':
        return []

    code = country_alpha2.lower()
    articles = []

    try:
        if code in NEWSAPI_SUPPORTED:
            url = f'{Config.NEWSAPI_BASE_URL}/top-headlines'
            params = {
                'country': code,
                'pageSize': page_size,
                'apiKey': key
            }
        else:
            country_name = iso_alpha2_to_name(country_alpha2)
            url = f'{Config.NEWSAPI_BASE_URL}/everything'
            params = {
                'q': f'"{country_name}"',
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': page_size,
                'apiKey': key
            }

        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            raw_articles = data.get('articles', [])
            for art in raw_articles:
                articles.append({
                    'title': art.get('title', ''),
                    'description': art.get('description', '') or '',
                    'url': art.get('url', ''),
                    'source': art.get('source', {}).get('name', 'Unknown'),
                    'publishedAt': art.get('publishedAt', ''),
                })
        elif resp.status_code == 429:
            logger.warning("NewsAPI rate limit reached")
        else:
            logger.warning(f"NewsAPI error {resp.status_code} for {country_alpha2}")
    except Exception as e:
        logger.warning(f"NewsAPI fetch failed for {country_alpha2}: {e}")

    return articles


def fetch_global_headlines(page_size=50):
    """Fetch global geopolitical headlines."""
    key = _get_key()
    if not key or key == 'your_api_key_here':
        return []

    articles = []
    try:
        url = f'{Config.NEWSAPI_BASE_URL}/everything'
        params = {
            'q': 'geopolitics OR conflict OR sanctions OR military OR protest OR terrorism',
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': page_size,
            'apiKey': key
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            raw_articles = data.get('articles', [])
            for art in raw_articles:
                articles.append({
                    'title': art.get('title', ''),
                    'description': art.get('description', '') or '',
                    'url': art.get('url', ''),
                    'source': art.get('source', {}).get('name', 'Unknown'),
                    'publishedAt': art.get('publishedAt', ''),
                })
    except Exception as e:
        logger.warning(f"NewsAPI global fetch failed: {e}")

    return articles
