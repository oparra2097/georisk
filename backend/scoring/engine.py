"""
GeoRisk v2 scoring engine — two-tier architecture with EMA accumulation.

Composite = Base Score (40%) + News Score (60%)

Tier 1 — Base Score (low-frequency):
  World Bank WGI governance indicators + macro fundamentals.
  Cached 30 days, provides a structural risk floor.

Tier 2 — News Score (high-frequency):
  GDELT 72-hour rolling window + multi-provider news APIs.
  EMA blended: new_score = α * fresh + (1-α) * previous.
  Scores accumulate — crises ramp up fast, decay slowly.

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
from backend.data_sources.world_bank_wgi import fetch_base_scores, get_base_score
from backend.scoring.keyword_analyzer import analyze_articles
from backend.scoring.indicator_calculators import (
    calculate_indicator_score, get_baseline
)
from backend.scoring.normalizer import calculate_news_score, calculate_composite
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


def _ema_blend_indicator(fresh_value, previous_value, alpha=None):
    """EMA blend a single indicator value."""
    if alpha is None:
        alpha = Config.EMA_ALPHA
    if previous_value is None or previous_value == 0.0:
        return fresh_value
    return alpha * fresh_value + (1.0 - alpha) * previous_value


def _ema_blend_indicators(fresh_indicators, previous_indicators, alpha=None):
    """
    EMA blend all 6 indicator scores.
    new = α * fresh + (1-α) * previous
    """
    if alpha is None:
        alpha = Config.EMA_ALPHA

    if previous_indicators is None:
        return fresh_indicators

    return IndicatorScore(
        political_stability=_ema_blend_indicator(
            fresh_indicators.political_stability,
            previous_indicators.political_stability, alpha
        ),
        military_conflict=_ema_blend_indicator(
            fresh_indicators.military_conflict,
            previous_indicators.military_conflict, alpha
        ),
        economic_sanctions=_ema_blend_indicator(
            fresh_indicators.economic_sanctions,
            previous_indicators.economic_sanctions, alpha
        ),
        protests_civil_unrest=_ema_blend_indicator(
            fresh_indicators.protests_civil_unrest,
            previous_indicators.protests_civil_unrest, alpha
        ),
        terrorism=_ema_blend_indicator(
            fresh_indicators.terrorism,
            previous_indicators.terrorism, alpha
        ),
        diplomatic_tensions=_ema_blend_indicator(
            fresh_indicators.diplomatic_tensions,
            previous_indicators.diplomatic_tensions, alpha
        ),
    )


def score_single_country(country_alpha2, use_news=False):
    """
    Score a single country using the two-tier architecture.

    1. Get base score from World Bank WGI (cached, low-frequency)
    2. Compute fresh news indicators from GDELT + news APIs
    3. EMA blend fresh indicators with previous indicators
    4. Compute news_score as weighted average of blended indicators
    5. Composite = base_score * 0.4 + news_score * 0.6
    """
    country_name = country_codes.iso_alpha2_to_name(country_alpha2)

    # --- Tier 1: Base score (from World Bank WGI + macro) ---
    base_data = get_base_score(country_alpha2)
    base_score = 50.0  # neutral default if WGI data unavailable
    if base_data:
        base_score = base_data.get('base_score', 50.0)

    # --- Tier 2: Fresh news signal ---
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

    # Compute fresh indicator scores
    fresh_indicator_scores = {}
    for ind in INDICATORS:
        signal = analysis.get(ind, {'signal_strength': 0.0, 'theme_volume': 0})
        volume = signal.get('theme_volume', 0)
        baseline = get_baseline(ind)
        fresh_indicator_scores[ind] = calculate_indicator_score(
            ind, volume, baseline, avg_tone, signal
        )

    fresh_indicators = IndicatorScore(
        political_stability=fresh_indicator_scores.get('political_stability', 0),
        military_conflict=fresh_indicator_scores.get('military_conflict', 0),
        economic_sanctions=fresh_indicator_scores.get('economic_sanctions', 0),
        protests_civil_unrest=fresh_indicator_scores.get('protests_civil_unrest', 0),
        terrorism=fresh_indicator_scores.get('terrorism', 0),
        diplomatic_tensions=fresh_indicator_scores.get('diplomatic_tensions', 0)
    )

    # --- EMA blending with previous scores ---
    existing = store.get_country(country_alpha2)
    previous_indicators = None
    if existing and existing.indicators:
        previous_indicators = existing.indicators

    blended_indicators = _ema_blend_indicators(fresh_indicators, previous_indicators)

    # --- Compute news score (weighted average of blended indicators) ---
    news_score = calculate_news_score(blended_indicators)

    # --- Two-tier composite ---
    composite = calculate_composite(base_score, news_score)

    # Maintain trend history (last 10 scores)
    trend = []
    if existing and existing.trend:
        trend = list(existing.trend[-9:])
    trend.append(composite)

    risk = CountryRisk(
        country_code=country_alpha2,
        country_name=country_name,
        composite_score=composite,
        base_score=base_score,
        news_score=news_score,
        indicators=blended_indicators,
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
        # Preserve existing score on failure
        existing = store.get_country(code)
        if existing:
            return (code, existing, [])
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
    Scores all 50 priority countries using GDELT 72h window.
    EMA blends with previous indicators — scores accumulate.
    """
    logger.info("Starting GDELT refresh cycle...")
    start = datetime.utcnow()
    scored = {}

    _score_batch(PRIORITY_COUNTRIES, scored, use_news=False)

    store.set_last_refresh(datetime.utcnow())
    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(f"GDELT refresh complete. {len(scored)} countries in {elapsed:.1f}s")

    # Log score ranges for monitoring
    if scored:
        composites = [r.composite_score for r in scored.values()]
        bases = [r.base_score for r in scored.values()]
        news = [r.news_score for r in scored.values()]
        logger.info(
            f"Score ranges — composite: {min(composites):.1f}-{max(composites):.1f}, "
            f"base: {min(bases):.1f}-{max(bases):.1f}, "
            f"news: {min(news):.1f}-{max(news):.1f}"
        )

    save_scores(store)
    save_daily_snapshot(store)


def refresh_news_region():
    """
    Multi-provider news refresh (every ~2 hours).
    Rotates through 5 regions. Each region uses its assigned provider.
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

    store.set_last_refresh(datetime.utcnow())
    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(
        f"News refresh complete: {region_name} via {provider} "
        f"({len(scored)} countries) in {elapsed:.1f}s."
    )

    save_scores(store)


def refresh_all_scores():
    """
    Initial startup: fetch World Bank base scores, then GDELT + one news region.
    """
    # Fetch base scores from World Bank (WGI + macro)
    logger.info("Fetching World Bank base scores for all priority countries...")
    try:
        base_scores = fetch_base_scores(PRIORITY_COUNTRIES)
        logger.info(f"Base scores loaded for {len(base_scores)} countries.")
    except Exception as e:
        logger.error(f"Failed to fetch base scores: {e}")

    # Then run GDELT + news
    refresh_gdelt_scores()
    refresh_news_region()
