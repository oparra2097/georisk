import logging
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from backend.scoring.engine import refresh_gdelt_scores, refresh_newsapi_region, refresh_all_scores

logger = logging.getLogger(__name__)


def init_scheduler(app):
    """Initialize background schedulers for GDELT and NewsAPI refreshes."""
    gdelt_interval = app.config.get('GDELT_REFRESH_MINUTES', 15)
    newsapi_interval = app.config.get('NEWSAPI_ROTATION_MINUTES', 150)

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

    # Initial refresh: GDELT all + one NewsAPI region
    thread = threading.Thread(target=refresh_all_scores, daemon=True)
    thread.start()
    logger.info("Initial data refresh started in background.")
