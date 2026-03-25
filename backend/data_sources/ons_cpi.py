"""
ONS Consumer Price Index data client.

Uses ONS JSON data endpoint to fetch UK CPI data by category.
No authentication needed. Thread-safe cache with 24-hour TTL.
"""

import threading
import time
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24 hours

ONS_DATA_BASE = 'https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/{series_id}/mm23/data'

# Series IDs for UK CPI categories (annual rates)
ONS_SERIES = {
    'all_items': {'id': 'd7g7', 'label': 'All Items (CPI)',          'color': '#3b82f6'},
    'core':      {'id': 'dko8', 'label': 'Core (ex Food & Energy)',  'color': '#10b981'},
    'food':      {'id': 'd7gk', 'label': 'Food',                     'color': '#f59e0b'},
    'energy':    {'id': 'dkl6', 'label': 'Energy',                    'color': '#ef4444'},
    'housing':   {'id': 'd7gq', 'label': 'Housing/Rents',             'color': '#8b5cf6'},
}

MONTH_MAP = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
    'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12,
}


class OnsCpiCache:
    """Thread-safe cache for ONS CPI data."""

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                return self._data
        data = _fetch_ons_cpi()
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
            return data
        with self._lock:
            return self._data or _empty_result()

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0


_cache = OnsCpiCache()


def _empty_result():
    return {
        'series': {},
        'categories': {k: v['label'] for k, v in ONS_SERIES.items()},
        'colors': {k: v['color'] for k, v in ONS_SERIES.items()},
        'meta': {'source': 'Office for National Statistics', 'error': 'No data available'}
    }


def _parse_ons_json(data):
    """Parse ONS JSON data endpoint response.

    The 'months' array contains entries like:
    {'date': '2024 JAN', 'value': '4.0', 'year': '2024', 'month': 'January', ...}
    """
    points = []
    months = data.get('months', [])
    cutoff_year = datetime.utcnow().year - 20

    for entry in months:
        month_name = entry.get('month', '')
        year_str = entry.get('year', '')
        val_str = entry.get('value', '')

        if month_name not in MONTH_MAP:
            continue

        try:
            year = int(year_str)
            value = float(val_str)
        except (ValueError, TypeError):
            continue

        if year < cutoff_year:
            continue

        month = MONTH_MAP[month_name]
        points.append({
            'year': year,
            'month': month,
            'value': value,
            'date': f'{year}-{str(month).zfill(2)}',
        })

    points.sort(key=lambda p: (p['year'], p['month']))
    return points


def _fetch_ons_cpi():
    """Fetch CPI data from ONS JSON data endpoint for all series."""
    try:
        series_data = {}

        for key, series_info in ONS_SERIES.items():
            url = ONS_DATA_BASE.format(series_id=series_info['id'])

            resp = requests.get(url, timeout=30, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)'
            })

            if resp.status_code != 200:
                logger.warning(f"ONS {resp.status_code} for {key} ({series_info['id']})")
                continue

            points = _parse_ons_json(resp.json())
            if points:
                series_data[key] = points
                logger.info(f"ONS {key}: {len(points)} monthly data points")

        if not series_data:
            return None

        # Determine year range from actual data
        all_years = set()
        for points in series_data.values():
            for pt in points:
                all_years.add(pt['year'])

        min_year = min(all_years) if all_years else datetime.utcnow().year - 10
        max_year = max(all_years) if all_years else datetime.utcnow().year

        return {
            'series': series_data,
            'categories': {k: v['label'] for k, v in ONS_SERIES.items()},
            'colors': {k: v['color'] for k, v in ONS_SERIES.items()},
            'meta': {
                'source': 'Office for National Statistics (MM23)',
                'frequency': 'Monthly',
                'year_range': f'{min_year}-{max_year}',
            }
        }

    except requests.exceptions.Timeout:
        logger.error("ONS API timeout")
        return None
    except Exception as e:
        logger.error(f"ONS CPI fetch failed: {e}")
        return None


def get_ons_cpi_data():
    """Public API: returns cached ONS CPI data."""
    return _cache.get()
