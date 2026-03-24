"""
JSON-based persistent storage for GeoRisk scores.

Saves two files:
1. scores.json  — Latest scores, news cache, region index, EMA state (hot state)
2. history.json — Daily snapshots of composite scores per country (cold state)

On startup, loads from these files so data survives process restarts
and Render redeploys (if using a persistent disk / volume).

EMA-blended indicator scores are persisted so scores accumulate
across restarts — crises don't reset when the process restarts.

Saves automatically after each refresh cycle. Thread-safe.
"""

import json
import os
import logging
import threading
from datetime import datetime, date
from backend.models import IndicatorScore, CountryRisk
from config import Config

logger = logging.getLogger(__name__)

_save_lock = threading.Lock()


def _ensure_data_dir():
    """Create data directory if it doesn't exist."""
    os.makedirs(Config.DATA_DIR, exist_ok=True)


def save_scores(store):
    """
    Persist current scores + news cache + region index to JSON.
    Called after each GDELT/news refresh cycle.
    Includes base_score and news_score for the two-tier architecture.
    """
    with _save_lock:
        try:
            _ensure_data_dir()

            all_scores = store.get_all_scores()
            scores_data = {}
            for code, risk in all_scores.items():
                scores_data[code] = {
                    'country_code': risk.country_code,
                    'country_name': risk.country_name,
                    'composite_score': risk.composite_score,
                    'base_score': risk.base_score,
                    'news_score': risk.news_score,
                    'indicators': {
                        'political_stability': risk.indicators.political_stability,
                        'military_conflict': risk.indicators.military_conflict,
                        'economic_sanctions': risk.indicators.economic_sanctions,
                        'protests_civil_unrest': risk.indicators.protests_civil_unrest,
                        'terrorism': risk.indicators.terrorism,
                        'diplomatic_tensions': risk.indicators.diplomatic_tensions,
                    },
                    'headline_count': risk.headline_count,
                    'gdelt_event_count': risk.gdelt_event_count,
                    'avg_tone': risk.avg_tone,
                    'updated_at': risk.updated_at.isoformat() if risk.updated_at else None,
                    'trend': risk.trend,
                }

            # Also save news cache and region index
            newsapi_cache = {}
            for code in all_scores:
                articles = store.get_newsapi_articles(code)
                if articles:
                    newsapi_cache[code] = articles

            last_refresh = store.get_last_refresh()

            state = {
                'saved_at': datetime.utcnow().isoformat(),
                'scores': scores_data,
                'newsapi_cache': newsapi_cache,
                'newsapi_region_index': store._newsapi_region_index,
                'last_refresh': last_refresh.isoformat() if last_refresh else None,
            }

            # Write atomically (write to temp, then rename)
            tmp_path = Config.SCORES_FILE + '.tmp'
            with open(tmp_path, 'w') as f:
                json.dump(state, f, separators=(',', ':'))
            os.replace(tmp_path, Config.SCORES_FILE)

            logger.info(f"Scores persisted: {len(scores_data)} countries -> {Config.SCORES_FILE}")

        except Exception as e:
            logger.error(f"Failed to persist scores: {e}")


def load_scores(store):
    """
    Load persisted scores into the store on startup.
    Returns True if data was loaded, False if starting fresh.

    Restores EMA-blended indicators, base_score, and news_score
    so scores continue accumulating across restarts.
    """
    if not os.path.exists(Config.SCORES_FILE):
        logger.info("No persisted scores found, starting fresh.")
        return False

    try:
        with open(Config.SCORES_FILE, 'r') as f:
            state = json.load(f)

        scores_data = state.get('scores', {})
        newsapi_cache = state.get('newsapi_cache', {})
        region_index = state.get('newsapi_region_index', 0)
        last_refresh_str = state.get('last_refresh')
        saved_at = state.get('saved_at', '')

        loaded = 0
        for code, data in scores_data.items():
            ind_data = data.get('indicators', {})
            indicators = IndicatorScore(
                political_stability=ind_data.get('political_stability', 0),
                military_conflict=ind_data.get('military_conflict', 0),
                economic_sanctions=ind_data.get('economic_sanctions', 0),
                protests_civil_unrest=ind_data.get('protests_civil_unrest', 0),
                terrorism=ind_data.get('terrorism', 0),
                diplomatic_tensions=ind_data.get('diplomatic_tensions', 0),
            )

            updated_at = None
            if data.get('updated_at'):
                try:
                    updated_at = datetime.fromisoformat(data['updated_at'])
                except (ValueError, TypeError):
                    pass

            risk = CountryRisk(
                country_code=data.get('country_code', code),
                country_name=data.get('country_name', code),
                composite_score=data.get('composite_score', 0),
                base_score=data.get('base_score', 0),
                news_score=data.get('news_score', 0),
                indicators=indicators,
                headline_count=data.get('headline_count', 0),
                gdelt_event_count=data.get('gdelt_event_count', 0),
                avg_tone=data.get('avg_tone', 0),
                updated_at=updated_at,
                trend=data.get('trend', []),
            )
            store.update_country(code, risk)
            loaded += 1

        # Restore news cache
        for code, articles in newsapi_cache.items():
            store.set_newsapi_articles(code, articles)

        # Restore region index
        with store._lock:
            store._newsapi_region_index = region_index

        # Restore last refresh timestamp
        if last_refresh_str:
            try:
                store.set_last_refresh(datetime.fromisoformat(last_refresh_str))
            except (ValueError, TypeError):
                pass

        logger.info(
            f"Loaded {loaded} persisted scores (saved {saved_at}). "
            f"News cache: {len(newsapi_cache)} countries. "
            f"Region index: {region_index}."
        )
        return True

    except Exception as e:
        logger.error(f"Failed to load persisted scores: {e}")
        return False


def save_daily_snapshot(store):
    """
    Save one snapshot per day: date -> {country: composite_score + base + news}.
    Keeps up to 90 days of history for trend analysis.
    """
    with _save_lock:
        try:
            _ensure_data_dir()

            # Load existing history
            history = {}
            if os.path.exists(Config.HISTORY_FILE):
                with open(Config.HISTORY_FILE, 'r') as f:
                    history = json.load(f)

            today = date.today().isoformat()

            # Build today's snapshot
            all_scores = store.get_all_scores()
            snapshot = {}
            for code, risk in all_scores.items():
                snapshot[code] = {
                    'composite': round(risk.composite_score, 1),
                    'base_score': round(risk.base_score, 1),
                    'news_score': round(risk.news_score, 1),
                    'indicators': {
                        'political_stability': round(risk.indicators.political_stability, 1),
                        'military_conflict': round(risk.indicators.military_conflict, 1),
                        'economic_sanctions': round(risk.indicators.economic_sanctions, 1),
                        'protests_civil_unrest': round(risk.indicators.protests_civil_unrest, 1),
                        'terrorism': round(risk.indicators.terrorism, 1),
                        'diplomatic_tensions': round(risk.indicators.diplomatic_tensions, 1),
                    },
                    'avg_tone': round(risk.avg_tone, 2),
                }

            history[today] = snapshot

            # Prune to last 90 days
            sorted_dates = sorted(history.keys())
            if len(sorted_dates) > 90:
                for old_date in sorted_dates[:-90]:
                    del history[old_date]

            tmp_path = Config.HISTORY_FILE + '.tmp'
            with open(tmp_path, 'w') as f:
                json.dump(history, f, separators=(',', ':'))
            os.replace(tmp_path, Config.HISTORY_FILE)

            logger.info(f"Daily snapshot saved: {today}, {len(snapshot)} countries, {len(history)} days total")

        except Exception as e:
            logger.error(f"Failed to save daily snapshot: {e}")


def load_history():
    """Load historical snapshots. Returns {date_str: {country_code: {...}}}."""
    if not os.path.exists(Config.HISTORY_FILE):
        return {}
    try:
        with open(Config.HISTORY_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load history: {e}")
        return {}
