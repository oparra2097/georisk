"""
FRED (Federal Reserve Economic Data) API client.

Free API: https://fred.stlouisfed.org/docs/api/fred/
Requires API key (register at https://fred.stlouisfed.org/docs/api/api_key.html).

Thread-safe cache with 6-hour TTL — most series update daily or less.
"""

import os
import requests
import logging
import threading
import time
from datetime import datetime, timedelta
from config import Config

logger = logging.getLogger(__name__)

FRED_BASE_URL = 'https://api.stlouisfed.org/fred/series/observations'
CACHE_TTL = 21600  # 6 hours


# ── Thread-safe cache ────────────────────────────────────────────────────────

_cache_lock = threading.Lock()
_cache = {}  # {series_id: {'data': [...], 'fetched_at': float}}


def _get_api_key():
    """Resolve the FRED key at *call time* (not import time) so a user who
    sets FRED_API_KEY after Python boots can still pick it up after the
    next request — no Gunicorn restart required.

    Strips whitespace and accepts a few common alternate names in case the
    env var was named slightly differently.
    """
    for source in (
        getattr(Config, 'FRED_API_KEY', ''),
        os.environ.get('FRED_API_KEY', ''),
        os.environ.get('FRED_KEY', ''),
        os.environ.get('FRED_TOKEN', ''),
    ):
        key = (source or '').strip().strip('"').strip("'")
        if key:
            return key
    return ''


_LOGGED_KEY_STATE = {'logged': False}


def _log_key_state_once(key: str):
    if _LOGGED_KEY_STATE['logged']:
        return
    _LOGGED_KEY_STATE['logged'] = True
    if key:
        logger.info(f'FRED API key detected (length={len(key)}, first 4 chars={key[:4]}…)')
    else:
        logger.warning(
            'FRED API key NOT detected. Checked: Config.FRED_API_KEY, '
            'env FRED_API_KEY, FRED_KEY, FRED_TOKEN. '
            'Set FRED_API_KEY in Render Environment to enable macro model + Case-Shiller.'
        )


def fetch_series(series_id, start_date=None, end_date=None):
    """
    Fetch observations for a FRED series.

    Returns list of {'date': 'YYYY-MM-DD', 'value': float} dicts,
    sorted by date ascending. Periods marked '.' (missing) are skipped.
    """
    key = _get_api_key()
    _log_key_state_once(key)
    if not key:
        return []

    # Check cache
    cache_key = f"{series_id}:{start_date}:{end_date}"
    with _cache_lock:
        cached = _cache.get(cache_key)
        if cached and (time.time() - cached['fetched_at']) < CACHE_TTL:
            return cached['data']

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365 * 6)).strftime('%Y-%m-%d')
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    params = {
        'series_id': series_id,
        'api_key': key,
        'file_type': 'json',
        'observation_start': start_date,
        'observation_end': end_date,
        'sort_order': 'asc',
    }

    try:
        resp = requests.get(FRED_BASE_URL, params=params, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"FRED API error {resp.status_code} for {series_id}")
            return []

        raw = resp.json().get('observations', [])
        data = []
        for obs in raw:
            val = obs.get('value', '.')
            if val == '.' or val is None:
                continue
            try:
                data.append({
                    'date': obs['date'],
                    'value': float(val),
                })
            except (ValueError, KeyError):
                continue

        # Update cache
        with _cache_lock:
            _cache[cache_key] = {'data': data, 'fetched_at': time.time()}

        logger.debug(f"FRED {series_id}: fetched {len(data)} observations")
        return data

    except requests.exceptions.Timeout:
        logger.warning(f"FRED timeout for {series_id}")
        return []
    except Exception as e:
        logger.warning(f"FRED fetch error for {series_id}: {e}")
        return []


def fetch_latest_value(series_id):
    """Fetch the most recent observation for a series. Returns (date, value) or (None, None)."""
    data = fetch_series(series_id)
    if data:
        last = data[-1]
        return last['date'], last['value']
    return None, None


def clear_cache():
    """Clear all cached FRED data."""
    with _cache_lock:
        _cache.clear()
