"""
ONS Consumer Price Index data client.

Uses ONS CSV generator endpoint to fetch UK CPI data by category.
No authentication needed. Thread-safe cache with 24-hour TTL.
"""

import threading
import time
import logging
import requests
import csv
import io
from datetime import datetime

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24 hours

ONS_CSV_BASE = 'https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/{series_id}/mm23'

# Series IDs for UK CPI categories (annual rates)
ONS_SERIES = {
    'all_items': {'id': 'D7G7', 'label': 'All Items (CPI)',          'color': '#3b82f6'},
    'core':      {'id': 'DKO8', 'label': 'Core (ex Food & Energy)',  'color': '#10b981'},
    'food':      {'id': 'D7GK', 'label': 'Food',                     'color': '#f59e0b'},
    'energy':    {'id': 'DKL6', 'label': 'Energy',                    'color': '#ef4444'},
    'housing':   {'id': 'D7GQ', 'label': 'Housing/Rents',             'color': '#8b5cf6'},
}

MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
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


def _parse_ons_csv(csv_text):
    """Parse ONS CSV format.

    ONS CSV has metadata header rows followed by data rows
    with format: 'YYYY MON', value (e.g. '2024 JAN', '3.4').
    """
    points = []
    reader = csv.reader(io.StringIO(csv_text))

    for row in reader:
        if len(row) < 2:
            continue
        date_str = row[0].strip()
        val_str = row[1].strip()

        # Try to parse 'YYYY MON' format
        parts = date_str.split()
        if len(parts) != 2:
            continue

        year_str, month_str = parts
        month_str = month_str.upper()

        if month_str not in MONTH_MAP:
            continue

        try:
            year = int(year_str)
            value = float(val_str)
        except (ValueError, TypeError):
            continue

        # Only include last 20 years
        if year < datetime.utcnow().year - 20:
            continue

        month = MONTH_MAP[month_str]
        points.append({
            'year': year,
            'month': month,
            'value': value,  # Already a YoY annual rate from ONS
            'date': f'{year}-{str(month).zfill(2)}',
        })

    points.sort(key=lambda p: (p['year'], p['month']))
    return points


def _fetch_ons_cpi():
    """Fetch CPI data from ONS CSV generator for all series."""
    try:
        series_data = {}

        for key, series_info in ONS_SERIES.items():
            url = ONS_CSV_BASE.format(series_id=series_info['id'])

            resp = requests.get(url, timeout=30, headers={
                'User-Agent': 'ParraMacro/1.0'
            })

            if resp.status_code != 200:
                logger.warning(f"ONS CSV {resp.status_code} for {key} ({series_info['id']})")
                continue

            points = _parse_ons_csv(resp.text)
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
