import logging
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from backend.scoring.engine import refresh_gdelt_scores, refresh_news_region, refresh_all_scores
from backend.cache.persistence import load_scores
from backend.cache.store import store

logger = logging.getLogger(__name__)


def init_scheduler(app):
    """Initialize background schedulers for GDELT and multi-provider news refreshes."""
    gdelt_interval = app.config.get('GDELT_REFRESH_MINUTES', 15)
    news_interval = app.config.get('NEWS_ROTATION_MINUTES', 120)

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

    # Job 2: Multi-provider news rotation every 2 hours
    # Cycles: AMERICAS(NewsAPI) → EUROPE(NewsData) → MENA(NewsData)
    #       → AFRICA(GNews) → ASIA_PAC(GNews) → repeat
    scheduler.add_job(
        func=refresh_news_region,
        trigger='interval',
        minutes=news_interval,
        id='refresh_news',
        replace_existing=True,
        misfire_grace_time=600
    )

    scheduler.start()
    logger.info(
        f"Scheduler started. GDELT every {gdelt_interval}min, "
        f"news region rotation every {news_interval}min."
    )

    if not had_data:
        thread = threading.Thread(target=refresh_all_scores, daemon=True)
        thread.start()
        logger.info("No persisted data found. Initial refresh started in background.")
    else:
        thread = threading.Thread(target=refresh_gdelt_scores, daemon=True)
        thread.start()
        logger.info("Persisted data loaded. Background GDELT refresh started.")
