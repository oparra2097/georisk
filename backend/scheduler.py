import logging
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from backend.scoring.engine import refresh_all_scores

logger = logging.getLogger(__name__)


def init_scheduler(app):
    """Initialize the background scheduler for periodic data refresh."""
    interval = app.config.get('REFRESH_INTERVAL_MINUTES', 15)

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=refresh_all_scores,
        trigger='interval',
        minutes=interval,
        id='refresh_scores',
        replace_existing=True,
        misfire_grace_time=300
    )
    scheduler.start()
    logger.info(f"Scheduler started. Refresh every {interval} minutes.")

    # Run initial refresh in background thread so app starts immediately
    thread = threading.Thread(target=refresh_all_scores, daemon=True)
    thread.start()
    logger.info("Initial data refresh started in background.")
