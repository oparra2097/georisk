import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from backend.models import IndicatorScore, CountryRisk, NewsArticle
from backend.cache.store import store
from backend.data_sources import country_codes
from backend.data_sources.gdelt_client import fetch_country_data
from backend.data_sources.newsapi_client import (
    fetch_headlines_for_country, fetch_global_headlines
)
from backend.scoring.keyword_analyzer import analyze_articles
from backend.scoring.indicator_calculators import (
    calculate_indicator_score, get_baseline
)
from backend.scoring.normalizer import calculate_composite_score, normalize_scores_absolute
from config import Config

logger = logging.getLogger(__name__)

PRIORITY_COUNTRIES = []
for _region_codes in Config.REGIONS.values():
    PRIORITY_COUNTRIES.extend(_region_codes)

INDICATORS = [
    'political_stability', 'military_conflict', 'economic_sanctions',
    'protests_civil_unrest', 'terrorism', 'diplomatic_tensions'
]


def score_single_country(country_alpha2, use_newsapi=False):
    """
    Score a single country using GDELT data.
    If use_newsapi=True, also fetch fresh NewsAPI data and cache it.
    Otherwise, use cached NewsAPI articles from the store.
    """
    country_name = country_codes.iso_alpha2_to_name(country_alpha2)

    gdelt_data = fetch_country_data(country_alpha2)

    # GDELT articles only have title (no description/body field).
    # Use title for both fields so keyword analyzer can match on it.
    gdelt_articles = gdelt_data.get('articles', [])
    gdelt_article_dicts = []
    for art in gdelt_articles:
        title = art.get('title', '')
        gdelt_article_dicts.append({
            'title': title,
            'description': title  # duplicate — GDELT has no description
        })

    # Get NewsAPI articles (either fresh or from cache)
    newsapi_articles = []
    if use_newsapi:
        newsapi_articles = fetch_headlines_for_country(country_alpha2)
        store.set_newsapi_articles(country_alpha2, newsapi_articles)
    else:
        newsapi_articles = store.get_newsapi_articles(country_alpha2)

    all_articles = gdelt_article_dicts + newsapi_articles
    newsapi_signals = analyze_articles(all_articles)

    avg_tone = gdelt_data.get('avg_tone', 0.0)
    theme_volumes = gdelt_data.get('theme_volumes', {})

    indicator_scores = {}
    for ind in INDICATORS:
        volume = theme_volumes.get(ind, 0)
        baseline = get_baseline(ind)
        signal = newsapi_signals.get(ind, {'signal_strength': 0.0})
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
    for art in newsapi_articles[:10]:
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


def _score_country_safe(code, use_newsapi=False):
    """Score one country, catching exceptions."""
    try:
        risk, headlines = score_single_country(code, use_newsapi=use_newsapi)
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


def _score_batch(codes, scored_dict, use_newsapi=False):
    """Score a batch of countries in parallel (10 at a time)."""
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(_score_country_safe, c, use_newsapi): c
            for c in codes
        }
        for future in as_completed(futures):
            code, risk, headlines = future.result()
            store.update_country(code, risk)
            if headlines:
                store.update_headlines(code, headlines)
            scored_dict[code] = risk


def refresh_gdelt_scores():
    """
    GDELT-only refresh (every 15 min).
    Scores ALL countries using GDELT + cached NewsAPI data.
    Makes ZERO NewsAPI requests.
    """
    logger.info("Starting GDELT refresh cycle...")
    start = datetime.utcnow()
    scored = {}

    # Priority countries first
    _score_batch(PRIORITY_COUNTRIES, scored, use_newsapi=False)
    store.set_last_refresh(datetime.utcnow())

    # Remaining countries
    remaining = [c for c in country_codes.get_all_country_codes()
                 if c not in PRIORITY_COUNTRIES]
    _score_batch(remaining, scored, use_newsapi=False)

    normalize_scores_absolute(scored)
    for code, risk in scored.items():
        store.update_country(code, risk)

    store.set_last_refresh(datetime.utcnow())
    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(f"GDELT refresh complete. {len(scored)} countries in {elapsed:.1f}s")


def refresh_newsapi_region():
    """
    NewsAPI regional refresh (every ~2.5 hours).
    Fetches fresh NewsAPI data for ONE region (10 countries) + global headlines.
    Rotates through 5 regions so each gets updated ~2x per day.
    Uses ~11 NewsAPI requests per call.
    """
    region_index = store.get_next_region_index()
    region_name = Config.REGION_ORDER[region_index]
    region_codes = Config.REGIONS[region_name]

    logger.info(f"NewsAPI refresh: {region_name} ({len(region_codes)} countries)...")
    start = datetime.utcnow()
    scored = {}

    # Fetch fresh NewsAPI + rescore these countries only
    _score_batch(region_codes, scored, use_newsapi=True)

    # Also refresh global headlines (+1 request)
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
        f"NewsAPI refresh complete: {region_name} "
        f"({len(scored)} countries) in {elapsed:.1f}s. "
        f"~{len(region_codes) + 1} API requests used."
    )


def refresh_all_scores():
    """Initial startup: GDELT all countries, then one NewsAPI region."""
    refresh_gdelt_scores()
    refresh_newsapi_region()
