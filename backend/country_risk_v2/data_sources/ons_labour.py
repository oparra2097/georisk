"""
ONS Labour-Market client: UK youth unemployment (16-24, SA, monthly).

Pattern mirrors backend/data_sources/ons_cpi.py — same JSON endpoint format,
just a different series ID and dataset code (LMS instead of MM23).

Series:
  MGWY  16-24 unemployment rate, seasonally adjusted, %
  MGSX  16+ unemployment rate (total), seasonally adjusted, %

ONS publishes monthly; new month usually lands ~6 weeks after quarter-end.
"""

import logging
import threading
import time
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 86400       # 24h
RETRY_BACKOFF = 3600    # 1h

ONS_LMS_BASE = 'https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/{series_id}/lms/data'

SERIES = {
    'youth_rate': 'mgwy',   # 16-24 unemp rate
    'total_rate': 'mgsx',   # 16+  unemp rate
}

_MONTH = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
    'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12,
}

_lock = threading.RLock()
_cache = {}   # {series_id: {'points': [...], 'fetched_at': float, 'last_fail': float}}


def _parse(json_body):
    """ONS 'months' array → chronological list of {date, value} dicts."""
    out = []
    for entry in json_body.get('months', []):
        m = _MONTH.get(entry.get('month', ''))
        try:
            y = int(entry.get('year', ''))
            v = float(entry.get('value', ''))
        except (ValueError, TypeError):
            continue
        if m is None:
            continue
        out.append({'date': f'{y}-{str(m).zfill(2)}-01', 'value': v, 'year': y, 'month': m})
    out.sort(key=lambda p: (p['year'], p['month']))
    return out


def _fetch_series(series_id):
    """Network fetch with 24h cache + 1h retry backoff on failure."""
    now = time.time()
    with _lock:
        cached = _cache.get(series_id)
        if cached:
            if (now - cached['fetched_at']) < CACHE_TTL:
                return cached['points']
            if cached.get('last_fail') and (now - cached['last_fail']) < RETRY_BACKOFF:
                return cached['points']

    url = ONS_LMS_BASE.format(series_id=series_id)
    try:
        resp = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)',
            'Accept': 'application/json',
        })
        if resp.status_code != 200:
            logger.warning(f"ONS LMS {resp.status_code} for {series_id}")
            with _lock:
                _cache.setdefault(series_id, {'points': [], 'fetched_at': 0})['last_fail'] = now
            return _cache[series_id]['points']

        points = _parse(resp.json())
        with _lock:
            _cache[series_id] = {'points': points, 'fetched_at': now, 'last_fail': 0}
        return points
    except Exception as e:
        logger.error(f"ONS LMS fetch failed for {series_id}: {e}")
        with _lock:
            _cache.setdefault(series_id, {'points': [], 'fetched_at': 0})['last_fail'] = now
        return _cache[series_id]['points']


def get_uk_youth_unemployment():
    """
    Return normalized dict matching the youth_unemployment.py contract, or None.

        {'history': [...], 'level': ..., 'delta_12m': ..., 'total_unemp': ...,
         'asof': 'YYYY-MM-DD', 'source': 'ons'}
    """
    youth = _fetch_series(SERIES['youth_rate'])
    if not youth:
        return None

    history = [p['value'] for p in youth]
    level = history[-1]
    delta_12m = level - history[-13] if len(history) > 12 else None

    total = None
    total_series = _fetch_series(SERIES['total_rate'])
    if total_series:
        total = total_series[-1]['value']

    return {
        'history': history,
        'level': level,
        'delta_12m': delta_12m,
        'total_unemp': total,
        'asof': youth[-1]['date'],
        'source': 'ons',
    }


def clear_cache():
    with _lock:
        _cache.clear()
