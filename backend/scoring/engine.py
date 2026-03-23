"""
GeoRisk scoring engine with multi-provider news support.

Provider assignment:
  AMERICAS  → NewsAPI.org  (100 req/day)
  EUROPE    → NewsData.io  (200 req/day)
  MENA      → NewsData.io  (200 req/day)
  AFRICA    → GNews API    (100 req/day)
  ASIA_PAC  → GNews API    (100 req/day)

GDELT (unlimited, no key) runs for all 50 countries every 15 min.
News providers rotate through regions every 2 hours.
"""

import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from backend.models import IndicatorScore, CountryRisk, NewsArticle
from backend.cache.store import store
from backend.data_sources import country_codes
from backend.data_sources.gdelt_client import fetch_country_data
from backend.data_sources.newsapi_client import (
    fetch_headlines_for_country as newsapi_fetch,
    fetch_global_headlines
)
from backend.data_sources.newsdata_client import (
    fetch_headlines_for_country as newsdata_fetch
)
from backend.data_sources.gnews_client import (
    fetch_headlines_for_country as gnews_fetch
)
from backend.scoring.keyword_analyzer import analyze_articles
from backend.scoring.indicator_calculators import (
    calculate_indicator_score, get_baseline
)
from backend.scoring.normalizer import calculate_composite_score, normalize_scores_absolute
from backend.cache.persistence import save_scores, save_daily_snapshot
from config import Config

logger = logging.getLogger(__name__)

# Only score the 50 priority countries (5 regions x 10 countries)
PRIORITY_COUNTRIES = []
for _region_codes in Config.REGIONS.values():
    PRIORITY_COUNTRIES.extend(_region_codes)

INDICATORS = [
    'political_stability', 'military_conflict', 'economic_sanctions',
    'protests_civil_unrest', 'terrorism', 'diplomatic_tensions'
]

# Map provider name → fetch function
_PROVIDER_FETCHERS = {
    'newsapi': newsapi_fetch,
    'newsdata': newsdata_fetch,
    'gnews': gnews_fetch,
}

# Build country → provider mapping from config
_COUNTRY_PROVIDER = {}
for _region, _codes in Config.REGIONS.items():
    _provider = Config.REGION_PROVIDER.get(_region, 'newsapi')
    for _code in _codes:
        _COUNTRY_PROVIDER[_code] = _provider


def _fetch_news_for_country(country_alpha2):
    """Fetch news articles using the provider assigned to this country's region."""
    provider = _COUNTRY_PROVIDER.get(country_alpha2, 'newsapi')
    fetcher = _PROVIDER_FETCHERS.get(provider, newsapi_fetch)
    try:
        articles = fetcher(country_alpha2)
        return articles
    except Exception as e:
        logger.warning(f"{provider} failed for {country_alpha2}: {e}")
        return []


def score_single_country(country_alpha2, use_news=False):
    """
    Score a single country using GDELT + keyword analysis.
    If use_news=True, fetch fresh articles from the assigned provider.
    Otherwise, use cached articles from the store.
    """
    country_name = country_codes.iso_alpha2_to_name(country_alpha2)

    gdelt_data = fetch_country_data(country_alpha2)

    # GDELT articles only have title (no description/body)
    gdelt_articles = gdelt_data.get('articles', [])
    gdelt_article_dicts = []
    for art in gdelt_articles:
        title = art.get('title', '')
        gdelt_article_dicts.append({
            'title': title,
            'description': title
        })

    # Get news articles (fresh from provider or from cache)
    news_articles = []
    if use_news:
        news_articles = _fetch_news_for_country(country_alpha2)
        store.set_newsapi_articles(country_alpha2, news_articles)
    else:
        news_articles = store.get_newsapi_articles(country_alpha2)

    all_articles = gdelt_article_dicts + news_articles
    analysis = analyze_articles(all_articles)

    avg_tone = gdelt_data.get('avg_tone', 0.0)

    indicator_scores = {}
    for ind in INDICATORS:
        signal = analysis.get(ind, {'signal_strength': 0.0, 'theme_volume': 0})
        volume = signal.get('theme_volume', 0)
        baseline = get_baseline(ind)
        indicator_scores[ind] = calculate_indicator_score(
            ind, volume, baseline, avg_tone, signal
        )

    indicators = IndicatorScore(
        political_stability=indicator_scores.get('political_stability', 0),
        military_conflict=indicator_scores.get('military_conflict', 0),
        economic_sanctions=indicator_scores.get('economic_sanctions', 0),
        protests_civil_unrest=indicator_scores.get('protests_civil_unrest', 0),
        terrorism=indicator_scores.get('terrorism', 0),
        diplomatic_tensions=indicator_scores.get('diplomatic_tensions', 0)
    )

    composite = calculate_composite_score(indicators)

    # Maintain trend history (last 10 scores)
    existing = store.get_country(country_alpha2)
    trend = []
    if existing and existing.trend:
        trend = list(existing.trend[-9:])
    trend.append(composite)

    risk = CountryRisk(
        country_code=country_alpha2,
        country_name=country_name,
        composite_score=composite,
        indicators=indicators,
        headline_count=len(all_articles),
        gdelt_event_count=gdelt_data.get('article_count', 0),
        avg_tone=avg_tone,
        updated_at=datetime.utcnow(),
        trend=trend
    )

    headlines = []
    for art in news_articles[:10]:
        headlines.append(NewsArticle(
            title=art.get('title', ''),
            description=art.get('description', ''),
            url=art.get('url', ''),
            source=art.get('source', 'Unknown'),
            published_at=art.get('publishedAt', ''),
            country_code=country_alpha2
        ))

    for art in gdelt_articles[:10]:
        headlines.append(NewsArticle(
            title=art.get('title', ''),
            description='',
            url=art.get('url', ''),
            source=art.get('domain', 'GDELT'),
            published_at=art.get('seendate', ''),
            country_code=country_alpha2
        ))

    return risk, headlines[:15]


def _score_country_safe(code, use_news=False):
    """Score one country, catching exceptions."""
    try:
        risk, headlines = score_single_country(code, use_news=use_news)
        return (code, risk, headlines)
    except Exception as e:
        logger.error(f"Failed to score {code}: {e}")
        risk = CountryRisk(
            country_code=code,
            country_name=country_codes.iso_alpha2_to_name(code),
            composite_score=1.0,
            updated_at=datetime.utcnow()
        )
        return (code, risk, [])


def _score_batch(codes, scored_dict, use_news=False):
    """Score a batch of countries in parallel (5 at a time)."""
    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(_score_country_safe, c, use_news): c
                for c in codes
            }
            for future in as_completed(futures):
                try:
                    code, risk, headlines = future.result(timeout=60)
                    store.update_country(code, risk)
                    if headlines:
                        store.update_headlines(code, headlines)
                    scored_dict[code] = risk
                except Exception as e:
                    logger.error(f"Future failed: {e}")
    except RuntimeError as e:
        logger.warning(f"Executor error (likely shutdown): {e}")


def refresh_gdelt_scores():
    """
    GDELT-only refresh (every 15 min).
    Scores all 50 priority countries (2 GDELT calls each = 100 total).
    Uses cached news articles from the store.
    """
    logger.info("Starting GDELT refresh cycle...")
    start = datetime.utcnow()
    scored = {}

    _score_batch(PRIORITY_COUNTRIES, scored, use_news=False)

    normalize_scores_absolute(scored)
    for code, risk in scored.items():
        store.update_country(code, risk)

    store.set_last_refresh(datetime.utcnow())
    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(f"GDELT refresh complete. {len(scored)} countries in {elapsed:.1f}s")

    save_scores(store)
    save_daily_snapshot(store)


def refresh_news_region():
    """
    Multi-provider news refresh (every ~2 hours).
    Rotates through 5 regions. Each region uses its assigned provider:
      AMERICAS  → NewsAPI.org
      EUROPE    → NewsData.io
      MENA      → NewsData.io
      AFRICA    → GNews API
      ASIA_PAC  → GNews API
    """
    region_index = store.get_next_region_index()
    region_name = Config.REGION_ORDER[region_index]
    region_codes = Config.REGIONS[region_name]
    provider = Config.REGION_PROVIDER.get(region_name, 'newsapi')

    logger.info(f"News refresh: {region_name} via {provider} "
                f"({len(region_codes)} countries)...")
    start = datetime.utcnow()
    scored = {}

    _score_batch(region_codes, scored, use_news=True)

    # Also refresh global headlines via NewsAPI (+1 request, only if NewsAPI key set)
    if region_name == 'AMERICAS' and Config.NEWSAPI_KEY:
        try:
            global_articles = fetch_global_headlines()
            global_headlines = []
            for art in global_articles[:30]:
                global_headlines.append(NewsArticle(
                    title=art.get('title', ''),
                    description=art.get('description', ''),
                    url=art.get('url', ''),
                    source=art.get('source', 'Unknown'),
                    published_at=art.get('publishedAt', ''),
                ))
            store.set_global_headlines(global_headlines)
        except Exception as e:
            logger.error(f"Failed to fetch global headlines: {e}")

    normalize_scores_absolute(scored)
    for code, risk in scored.items():
        store.update_country(code, risk)

    store.set_last_refresh(datetime.utcnow())
    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(
        f"News refresh complete: {region_name} via {provider} "
        f"({len(scored)} countries) in {elapsed:.1f}s."
    )

    save_scores(store)


def refresh_all_scores():
    """Initial startup: GDELT all priority countries, then one news region."""
    refresh_gdelt_scores()
    refresh_news_region()
