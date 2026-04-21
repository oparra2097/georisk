"""
GeoRisk v2 scoring engine — two-tier architecture with EMA accumulation.

Composite = Base Score (30%) + News Score (70%)

Tier 1 — Base Score (low-frequency):
  World Bank WGI governance indicators + macro fundamentals.
  Cached 30 days, provides a structural risk floor.
  Fetched for ALL ~187 countries.

Tier 2 — News Score (high-frequency):
  GDELT 72-hour rolling window + multi-provider news APIs.
  EMA blended: new_score = α * fresh + (1-α) * previous.
  Scores accumulate — crises ramp up fast, decay slowly.

Coverage tiers:
  ALL countries   → World Bank base score + GDELT (both unlimited/free)
  51 priority     → Also get paid news API articles for richer signal

Provider assignment (priority countries only):
  AMERICAS  → NewsAPI.org  (100 req/day)
  EUROPE    → NewsData.io  (200 req/day)
  MENA      → NewsData.io  (200 req/day)
  AFRICA    → GNews API    (100 req/day)
  ASIA_PAC  → GNews API    (100 req/day)

GDELT (unlimited, no key) runs for ALL countries every 15 min.
News providers rotate through regions every 2 hours.
"""

import logging
import threading
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
from backend.scoring.relevance import filter_articles_for_country
from backend.scoring.indicator_calculators import calculate_indicator_score
from backend.scoring.baselines import get_country_baseline
from backend.scoring.normalizer import calculate_news_score, calculate_composite
from backend.scoring.conflict_registry import get_conflict_floors, get_conflict_info
from backend.cache.persistence import save_scores, save_daily_snapshot
from backend.cache.database import archive_articles
from config import Config

logger = logging.getLogger(__name__)

# 50 priority countries get paid news API coverage
PRIORITY_COUNTRIES = []
for _region_codes in Config.REGIONS.values():
    PRIORITY_COUNTRIES.extend(_region_codes)

# ALL countries get GDELT + World Bank coverage
ALL_COUNTRIES = country_codes.get_all_country_codes()

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

# Build country → provider mapping from config (priority countries only)
_COUNTRY_PROVIDER = {}
for _region, _codes in Config.REGIONS.items():
    _provider = Config.REGION_PROVIDER.get(_region, 'newsapi')
    for _code in _codes:
        _COUNTRY_PROVIDER[_code] = _provider

_PRIORITY_SET = set(PRIORITY_COUNTRIES)

# Job-level lock — prevents GDELT refresh and news refresh from running simultaneously
_refresh_lock = threading.Lock()


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


def _ema_blend_indicator(fresh_value, previous_value, alpha=None, floor=None):
    """EMA blend a single indicator value, respecting optional floor."""
    if alpha is None:
        alpha = Config.EMA_ALPHA
    if previous_value is None or previous_value == 0.0:
        blended = fresh_value
    else:
        blended = alpha * fresh_value + (1.0 - alpha) * previous_value
    # Enforce floor AFTER blending so conflicts don't decay below minimum
    if floor is not None and blended < floor:
        blended = floor
    return blended


def _ema_blend_indicators(fresh_indicators, previous_indicators, alpha=None,
                          conflict_floors=None):
    """
    EMA blend all 6 indicator scores.
    new = α * fresh + (1-α) * previous
    Conflict floors (if provided) prevent blended values from decaying below minimums.
    """
    if alpha is None:
        alpha = Config.EMA_ALPHA

    if previous_indicators is None:
        return fresh_indicators

    floors = conflict_floors or {}
    return IndicatorScore(
        political_stability=_ema_blend_indicator(
            fresh_indicators.political_stability,
            previous_indicators.political_stability, alpha,
            floor=floors.get('political_stability')
        ),
        military_conflict=_ema_blend_indicator(
            fresh_indicators.military_conflict,
            previous_indicators.military_conflict, alpha,
            floor=floors.get('military_conflict')
        ),
        economic_sanctions=_ema_blend_indicator(
            fresh_indicators.economic_sanctions,
            previous_indicators.economic_sanctions, alpha,
            floor=floors.get('economic_sanctions')
        ),
        protests_civil_unrest=_ema_blend_indicator(
            fresh_indicators.protests_civil_unrest,
            previous_indicators.protests_civil_unrest, alpha,
            floor=floors.get('protests_civil_unrest')
        ),
        terrorism=_ema_blend_indicator(
            fresh_indicators.terrorism,
            previous_indicators.terrorism, alpha,
            floor=floors.get('terrorism')
        ),
        diplomatic_tensions=_ema_blend_indicator(
            fresh_indicators.diplomatic_tensions,
            previous_indicators.diplomatic_tensions, alpha,
            floor=floors.get('diplomatic_tensions')
        ),
    )


def score_single_country(country_alpha2, use_news=False):
    """
    Score a single country using the two-tier architecture.

    1. Get base score from World Bank WGI (cached, low-frequency)
    2. Compute fresh news indicators from GDELT + news APIs
    3. EMA blend fresh indicators with previous indicators
    4. Compute news_score as weighted average of blended indicators
    5. Composite = base_score * 0.3 + news_score * 0.7
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

    # Get news articles (only for priority countries with paid API access)
    news_articles = []
    if country_alpha2 in _PRIORITY_SET:
        if use_news:
            news_articles = _fetch_news_for_country(country_alpha2)
            store.set_newsapi_articles(country_alpha2, news_articles)
        else:
            news_articles = store.get_newsapi_articles(country_alpha2)

    # Relevance filter: drop articles not actually about this country and
    # collapse duplicate titles across providers.
    gdelt_article_dicts = filter_articles_for_country(
        gdelt_article_dicts, country_alpha2)
    news_articles = filter_articles_for_country(news_articles, country_alpha2)

    all_articles = gdelt_article_dicts + news_articles
    # Cross-provider dedupe on the merged list
    all_articles = filter_articles_for_country(all_articles, country_alpha2)
    analysis = analyze_articles(all_articles)

    # Archive articles for historical training data (once per day per country).
    # Also filter the raw provider lists so archived articles match what
    # actually contributed to the score.
    try:
        filtered_gdelt_raw = filter_articles_for_country(
            gdelt_articles, country_alpha2)
        archive_articles(country_alpha2, filtered_gdelt_raw, analysis,
                         provider='gdelt')
        if news_articles:
            archive_articles(country_alpha2, news_articles, analysis,
                             provider='news')
    except Exception as e:
        logger.debug(f"Article archive failed for {country_alpha2}: {e}")

    avg_tone = gdelt_data.get('avg_tone', 0.0)

    # Compute fresh indicator scores
    fresh_indicator_scores = {}
    for ind in INDICATORS:
        signal = analysis.get(ind, {'signal_strength': 0.0, 'theme_volume': 0})
        volume = signal.get('theme_volume', 0)
        baseline = get_country_baseline(country_alpha2, ind)
        fresh_indicator_scores[ind] = calculate_indicator_score(
            ind, volume, baseline, avg_tone, signal
        )

    # Blend structured conflict data from ACLED (if configured)
    if Config.ACLED_EMAIL and Config.ACLED_PASSWORD:
        try:
            from backend.data_sources.acled_client import get_acled_signal
            acled_signal = get_acled_signal(country_alpha2)
            if acled_signal:
                for ind in INDICATORS:
                    acled_ind = acled_signal.get(ind)
                    if acled_ind and acled_ind['event_count'] > 0:
                        acled_boost = acled_ind['severity'] * 20.0
                        fresh_indicator_scores[ind] = min(100.0,
                            fresh_indicator_scores[ind] + acled_boost)
        except Exception as e:
            logger.debug(f"ACLED signal unavailable for {country_alpha2}: {e}")

    # --- Apply active conflict floors to indicator scores ---
    conflict_floors = get_conflict_floors(country_alpha2)
    if conflict_floors:
        for ind in INDICATORS:
            floor = conflict_floors.get(ind, 0.0)
            if floor > 0 and fresh_indicator_scores[ind] < floor:
                logger.debug(
                    f"{country_alpha2} {ind}: floor applied "
                    f"{fresh_indicator_scores[ind]:.1f} -> {floor:.1f}"
                )
                fresh_indicator_scores[ind] = floor

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

    blended_indicators = _ema_blend_indicators(
        fresh_indicators, previous_indicators,
        conflict_floors=conflict_floors
    )

    # --- Compute news score (weighted average of blended indicators) ---
    news_score = calculate_news_score(blended_indicators)

    # --- Two-tier composite ---
    composite = calculate_composite(base_score, news_score)

    # --- Apply composite floor for active conflicts ---
    if conflict_floors:
        composite_floor = conflict_floors.get('composite_floor', 0.0)
        if composite_floor > 0 and composite < composite_floor:
            logger.debug(
                f"{country_alpha2} composite floor applied: "
                f"{composite:.1f} -> {composite_floor:.1f}"
            )
            composite = composite_floor

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

    # Log conflict scoring diagnostics
    if conflict_floors:
        conflict_info = get_conflict_info(country_alpha2)
        if conflict_info:
            logger.info(
                f"CONFLICT SCORE {country_alpha2} ({conflict_info['conflict']}): "
                f"composite={composite:.1f} "
                f"(floor={conflict_floors.get('composite_floor', 0):.0f}), "
                f"military={blended_indicators.military_conflict:.1f} "
                f"(floor={conflict_floors.get('military_conflict', 0):.0f}), "
                f"articles={len(all_articles)}, tone={avg_tone:.2f}"
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


def _score_batch(codes, scored_dict, use_news=False, max_workers=5):
    """Score a batch of countries in parallel."""
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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


def _base_only_score(country_alpha2):
    """
    Create a score using only the World Bank base score (no GDELT).
    Used to fill the map immediately before GDELT data arrives.
    """
    country_name = country_codes.iso_alpha2_to_name(country_alpha2)
    base_data = get_base_score(country_alpha2)

    if not base_data:
        return None

    base_score = base_data.get('base_score', 50.0)

    # For base-only countries, news_score starts at 0
    # and composite is purely base score until GDELT data arrives
    existing = store.get_country(country_alpha2)
    if existing:
        news_score = existing.news_score
        indicators = existing.indicators
    else:
        news_score = 0.0
        indicators = IndicatorScore()

    composite = calculate_composite(base_score, news_score)

    trend = []
    if existing and existing.trend:
        trend = list(existing.trend[-9:])
    trend.append(composite)

    return CountryRisk(
        country_code=country_alpha2,
        country_name=country_name,
        composite_score=composite,
        base_score=base_score,
        news_score=news_score,
        indicators=indicators,
        headline_count=existing.headline_count if existing else 0,
        gdelt_event_count=existing.gdelt_event_count if existing else 0,
        avg_tone=existing.avg_tone if existing else 0.0,
        updated_at=datetime.utcnow(),
        trend=trend
    )


def seed_base_only_scores():
    """
    Populate the store with base-only scores for all countries that have
    World Bank data but haven't been GDELT-scored yet.
    This fills the map immediately on startup.
    """
    base_scores = fetch_base_scores()
    seeded = 0
    for country_code in base_scores:
        if not store.get_country(country_code):
            risk = _base_only_score(country_code)
            if risk:
                store.update_country(country_code, risk)
                seeded += 1
    logger.info(f"Seeded {seeded} countries with base-only scores "
                f"(total in store: {store.country_count()})")
    return seeded


def refresh_gdelt_scores():
    """
    GDELT refresh (every 15 min).
    Scores ALL countries using GDELT 72h window — single-threaded to
    respect GDELT's ~1 req/s rate limit. Serialized execution avoids
    the burst patterns that cause 429 errors.

    Uses _refresh_lock to prevent overlap with news region refresh.
    """
    if not _refresh_lock.acquire(blocking=False):
        logger.info("GDELT refresh already running, skipping this cycle")
        return

    try:
        logger.info("Starting GDELT refresh cycle...")
        start = datetime.utcnow()
        scored = {}

        # Batch 1: Priority countries (single worker — serialized)
        logger.info(f"  GDELT batch 1: {len(PRIORITY_COUNTRIES)} priority countries...")
        _score_batch(PRIORITY_COUNTRIES, scored, use_news=False, max_workers=1)

        # Batch 2: Remaining countries (single worker — serialized)
        remaining = [c for c in ALL_COUNTRIES if c not in _PRIORITY_SET]
        if remaining:
            logger.info(f"  GDELT batch 2: {len(remaining)} remaining countries...")
            _score_batch(remaining, scored, use_news=False, max_workers=1)

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
    finally:
        _refresh_lock.release()


def refresh_news_region():
    """
    Multi-provider news refresh (every ~2 hours).
    Rotates through 5 regions. Each region uses its assigned provider.
    Only applies to the 50 priority countries.

    Uses _refresh_lock to prevent overlap with GDELT refresh.
    """
    if not _refresh_lock.acquire(blocking=False):
        logger.info("GDELT refresh running, deferring news cycle")
        return

    try:
        region_index = store.get_next_region_index()
        region_name = Config.REGION_ORDER[region_index]
        region_codes = Config.REGIONS[region_name]
        provider = Config.REGION_PROVIDER.get(region_name, 'newsapi')

        logger.info(f"News refresh: {region_name} via {provider} "
                    f"({len(region_codes)} countries)...")
        start = datetime.utcnow()
        scored = {}

        _score_batch(region_codes, scored, use_news=True, max_workers=1)

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
    finally:
        _refresh_lock.release()


def refresh_all_scores():
    """
    Initial startup: fetch World Bank base scores for ALL countries,
    seed store with base-only scores, then run GDELT + one news region.
    """
    # Step 1: Fetch base scores from World Bank for ALL countries
    logger.info("Fetching World Bank base scores for ALL countries...")
    try:
        base_scores = fetch_base_scores()
        logger.info(f"Base scores loaded for {len(base_scores)} countries.")
    except Exception as e:
        logger.error(f"Failed to fetch base scores: {e}")

    # Step 2: Seed store with base-only scores (map fills immediately)
    seed_base_only_scores()

    # Step 3: Run GDELT for all countries + news for one region
    refresh_gdelt_scores()
    refresh_news_region()
