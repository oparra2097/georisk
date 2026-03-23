import logging
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from backend.scoring.engine import refresh_gdelt_scores, refresh_newsapi_region, refresh_all_scores
from backend.cache.persistence import load_scores
from backend.cache.store import store

logger = logging.getLogger(__name__)


def init_scheduler(app):
    """Initialize background schedulers for GDELT and NewsAPI refreshes."""
    gdelt_interval = app.config.get('GDELT_REFRESH_MINUTES', 15)
    newsapi_interval = app.config.get('NEWSAPI_ROTATION_MINUTES', 150)

    # Load persisted scores from disk (survives restarts/redeploys)
    had_data = load_scores(store)
    if had_data:
        logger.info("Restored persisted scores — skipping full initial refresh, "
                     "will update on next scheduled cycle.")

    scheduler = BackgroundScheduler()

    # Job 1: GDELT refresh every 15 min (unlimited, no API key)
    scheduler.add_job(
        func=refresh_gdelt_scores,
        trigger='interval',
        minutes=gdelt_interval,
        id='refresh_gdelt',
        replace_existing=True,
        misfire_grace_time=300
    )

    # Job 2: NewsAPI region rotation every 2.5 hours (~11 requests per cycle)
    scheduler.add_job(
        func=refresh_newsapi_region,
        trigger='interval',
        minutes=newsapi_interval,
        id='refresh_newsapi',
        replace_existing=True,
        misfire_grace_time=600
    )

    scheduler.start()
    logger.info(
        f"Scheduler started. GDELT every {gdelt_interval}min, "
        f"NewsAPI region rotation every {newsapi_interval}min."
    )

    if not had_data:
        # No persisted data — do full initial refresh
        thread = threading.Thread(target=refresh_all_scores, daemon=True)
        thread.start()
        logger.info("No persisted data found. Initial refresh started in background.")
    else:
        # Had persisted data — still kick off a GDELT refresh to get fresh scores
        # but skip the slow NewsAPI call (will happen on next rotation)
        thread = threading.Thread(target=refresh_gdelt_scores, daemon=True)
        thread.start()
        logger.info("Persisted data loaded. Background GDELT refresh started.")
