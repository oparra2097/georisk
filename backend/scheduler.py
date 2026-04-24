import logging
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from backend.scoring.engine import (
    refresh_gdelt_scores, refresh_news_region, refresh_all_scores,
    seed_base_only_scores
)
from backend.data_sources.world_bank_wgi import fetch_base_scores
from backend.cache.persistence import load_scores
from backend.cache.store import store
from config import Config

logger = logging.getLogger(__name__)


def _startup_with_persisted():
    """Load WGI base scores for ALL countries, seed missing ones,
    then refresh GDELT (news scores EMA-blend with persisted state)."""
    try:
        logger.info("Loading World Bank base scores for ALL countries (startup with persisted data)...")
        fetch_base_scores()
        # Seed any countries that have WGI data but weren't in persisted scores
        seed_base_only_scores()
        logger.info("Base scores loaded + seeded. Starting GDELT refresh...")
    except Exception as e:
        logger.error(f"Failed to load base scores on startup: {e}")
    refresh_gdelt_scores()


def init_scheduler(app):
    """Initialize background schedulers for GDELT and multi-provider news refreshes."""
    gdelt_interval = app.config.get('GDELT_REFRESH_MINUTES', 15)
    news_interval = app.config.get('NEWS_ROTATION_MINUTES', 120)

    # Initialize SQLite database and migrate history.json if present
    from backend.cache.database import init_db, migrate_from_json, cleanup_old_scores
    init_db()
    migrate_from_json()
    cleanup_old_scores()

    # Load persisted scores from disk (survives restarts/redeploys)
    had_data = load_scores(store)
    if had_data:
        logger.info("Restored persisted scores — EMA state preserved. "
                     "Will load base scores and refresh on next cycle.")

    scheduler = BackgroundScheduler()

    # Job 1: GDELT refresh every 15 min (unlimited, no API key)
    # Scores ALL countries (priority + remaining) — single-threaded
    scheduler.add_job(
        func=refresh_gdelt_scores,
        trigger='interval',
        minutes=gdelt_interval,
        id='refresh_gdelt',
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1
    )

    # Job 2: Multi-provider news rotation every 2 hours
    # Only applies to 50 priority countries
    scheduler.add_job(
        func=refresh_news_region,
        trigger='interval',
        minutes=news_interval,
        id='refresh_news',
        replace_existing=True,
        misfire_grace_time=600,
        max_instances=1
    )

    # Job 3: ACLED conflict data refresh (daily at 6 AM UTC)
    if Config.ACLED_EMAIL and Config.ACLED_PASSWORD:
        from backend.data_sources.acled_client import prefetch_acled_data
        scheduler.add_job(
            func=prefetch_acled_data,
            trigger='cron',
            hour=6,
            id='refresh_acled',
            replace_existing=True,
            max_instances=1
        )
        logger.info("ACLED daily prefetch scheduled (6 AM UTC).")

    # Job 4: Commodity model refit (monthly, 1st of month at 07:00 UTC)
    def _refit_commodity_models():
        try:
            from backend.data_sources import commodity_models, commodities_forecast
            summaries = commodity_models.refit_all()
            fits = sum(1 for s in summaries.values() if not s.get('fit_error'))
            logger.info(f"Commodity model refit: {fits}/{len(summaries)} succeeded")
            # Invalidate the forecast cache so downstream calls pick up new fits
            commodities_forecast._cache.clear()
        except Exception as e:
            logger.error(f"Commodity model refit failed: {e}")

    scheduler.add_job(
        func=_refit_commodity_models,
        trigger='cron',
        day=1, hour=7, minute=0,
        id='refit_commodity_models',
        replace_existing=True,
        max_instances=1,
    )
    logger.info("Commodity model monthly refit scheduled (1st of month, 07:00 UTC).")

    # Job 5: GDP nowcast refresh every 6 hours
    if Config.FRED_API_KEY:
        def _refresh_gdp_nowcast():
            try:
                from backend.data_sources.fred_client import clear_cache as clear_fred
                from backend.data_sources.gdp_nowcast import compute_nowcast
                import backend.data_sources.gdp_nowcast as gnmod
                import time as _time
                clear_fred()
                data = compute_nowcast()
                if not data.get('error'):
                    with gnmod._lock:
                        gnmod._cached_result = data
                        gnmod._cached_at = _time.time()
                    est = data.get('nowcast', {}).get('estimate')
                    logger.info(f"GDP nowcast refreshed: {est}%")
                else:
                    logger.warning(f"GDP nowcast refresh error: {data.get('error')}")
            except Exception as e:
                logger.error(f"GDP nowcast refresh failed: {e}")

        scheduler.add_job(
            func=_refresh_gdp_nowcast,
            trigger='interval',
            hours=6,
            id='refresh_gdp_nowcast',
            replace_existing=True,
            misfire_grace_time=600,
            max_instances=1,
        )
        logger.info("GDP nowcast scheduled (every 6 hours).")

    # Job 6: Country-Risk v2 refresh (every 6 hours)
    # Scores the 12 priority countries + EU aggregate into the v2 service
    # cache so first user visit to /country-risk is served fast.
    def _refresh_country_risk_v2():
        try:
            from backend.country_risk_v2 import service as crv2
            crv2.clear_cache()
            risks = crv2.score_all(force_refresh=True)
            logger.info(f"country-risk v2: {len(risks)} countries refreshed")
        except Exception as e:
            logger.error(f"country-risk v2 refresh failed: {e}")

    scheduler.add_job(
        func=_refresh_country_risk_v2,
        trigger='interval',
        hours=6,
        id='refresh_country_risk_v2',
        replace_existing=True,
        misfire_grace_time=600,
        max_instances=1,
    )
    logger.info("Country-Risk v2 scheduled (every 6 hours).")

    scheduler.start()
    logger.info(
        f"Scheduler started. GDELT every {gdelt_interval}min (ALL countries), "
        f"news region rotation every {news_interval}min (50 priority)."
    )

    if not had_data:
        # No persisted data: full startup (WGI ALL + seed + GDELT ALL + news)
        thread = threading.Thread(target=refresh_all_scores, daemon=True)
        thread.start()
        logger.info("No persisted data found. Full initial refresh started in background.")
    else:
        # Persisted data: load WGI for ALL, seed missing, then GDELT refresh
        thread = threading.Thread(target=_startup_with_persisted, daemon=True)
        thread.start()
        logger.info("Persisted data loaded. Background WGI + GDELT refresh started.")

    # Pre-warm the EM external vulnerability dataset so the first user
    # hit is cache-served (5 World Bank fetches, ~15s in parallel).
    def _warm_em_vuln():
        try:
            from backend.data_sources.em_vulnerability import get_em_vulnerability_data
            get_em_vulnerability_data()
            logger.info("EM vulnerability cache warmed.")
        except Exception as e:
            logger.error(f"EM vulnerability warmup failed: {e}")

    threading.Thread(target=_warm_em_vuln, daemon=True).start()

    # Pre-warm country-risk v2 cache so first visit is instant.
    def _warm_country_risk_v2():
        try:
            from backend.country_risk_v2 import service as crv2
            risks = crv2.score_all()
            logger.info(f"country-risk v2 cache warmed: {len(risks)} countries")
        except Exception as e:
            logger.error(f"country-risk v2 warmup failed: {e}")
    threading.Thread(target=_warm_country_risk_v2, daemon=True).start()

    # Pre-warm GDP nowcast so first visit is instant
    if Config.FRED_API_KEY:
        def _warm_gdp_nowcast():
            try:
                from backend.data_sources.gdp_nowcast import get_gdp_nowcast
                result = get_gdp_nowcast()
                est = result.get('nowcast', {}).get('estimate')
                logger.info(f"GDP nowcast cache warmed: {est}%")
            except Exception as e:
                logger.error(f"GDP nowcast warmup failed: {e}")
        threading.Thread(target=_warm_gdp_nowcast, daemon=True).start()
