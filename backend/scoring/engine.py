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

logger = logging.getLogger(__name__)

PRIORITY_COUNTRIES = [
    'US', 'CN', 'RU', 'UA', 'IL', 'PS', 'IR', 'IQ', 'SY', 'AF',
    'KP', 'TW', 'IN', 'PK', 'SA', 'YE', 'LY', 'SD', 'SS', 'SO',
    'ET', 'NG', 'CD', 'ML', 'BF', 'MM', 'VE', 'CU', 'TR', 'EG',
    'GB', 'FR', 'DE', 'JP', 'KR', 'BR', 'MX', 'CO', 'TH', 'PH',
    'ID', 'MY', 'AU', 'CA', 'ZA', 'KE', 'LB', 'JO', 'GE', 'BY'
]

INDICATORS = [
    'political_stability', 'military_conflict', 'economic_sanctions',
    'protests_civil_unrest', 'terrorism', 'diplomatic_tensions'
]


def score_single_country(country_alpha2):
    """Score a single country using GDELT data and optionally NewsAPI."""
    country_name = country_codes.iso_alpha2_to_name(country_alpha2)

    gdelt_data = fetch_country_data(country_alpha2)

    gdelt_articles = gdelt_data.get('articles', [])
    gdelt_article_dicts = []
    for art in gdelt_articles:
        gdelt_article_dicts.append({
            'title': art.get('title', ''),
            'description': art.get('seendate', '')
        })

    newsapi_articles = []
    if country_alpha2 in PRIORITY_COUNTRIES:
        newsapi_articles = fetch_headlines_for_country(country_alpha2)

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

    risk = CountryRisk(
        country_code=country_alpha2,
        country_name=country_name,
        composite_score=composite,
        indicators=indicators,
        headline_count=len(all_articles),
        gdelt_event_count=gdelt_data.get('article_count', 0),
        avg_tone=avg_tone,
        updated_at=datetime.utcnow(),
        trend=[]
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


def _score_country_safe(code):
    """Score one country, catching exceptions. Returns (code, risk, headlines)."""
    try:
        risk, headlines = score_single_country(code)
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


def _score_batch(codes, scored_dict):
    """Score a batch of countries in parallel (10 at a time)."""
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_score_country_safe, c): c for c in codes}
        for future in as_completed(futures):
            code, risk, headlines = future.result()
            store.update_country(code, risk)
            if headlines:
                store.update_headlines(code, headlines)
            scored_dict[code] = risk


def refresh_all_scores():
    """Refresh scores for all tracked countries using parallel processing."""
    logger.info("Starting score refresh cycle...")
    start = datetime.utcnow()

    scored = {}

    # Phase 1: Priority countries (parallel, 10 workers)
    logger.info(f"Phase 1: Scoring {len(PRIORITY_COUNTRIES)} priority countries...")
    _score_batch(PRIORITY_COUNTRIES, scored)

    # Signal frontend that priority data is ready
    store.set_last_refresh(datetime.utcnow())
    p1_elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(f"Phase 1 complete: {len(scored)} priority countries in {p1_elapsed:.1f}s")

    # Phase 2: Remaining countries (parallel, 10 workers)
    remaining = [c for c in country_codes.get_all_country_codes()
                 if c not in PRIORITY_COUNTRIES]
    logger.info(f"Phase 2: Scoring {len(remaining)} remaining countries...")
    _score_batch(remaining, scored)

    # Normalize and update cache
    normalize_scores_absolute(scored)
    for code, risk in scored.items():
        store.update_country(code, risk)

    # Global headlines
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

    store.set_last_refresh(datetime.utcnow())
    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(f"Score refresh complete. {len(scored)} countries scored in {elapsed:.1f}s")
