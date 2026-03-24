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
