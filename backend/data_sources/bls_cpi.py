"""
BLS Consumer Price Index data client.

Uses BLS Public Data API v2 to fetch US CPI data by category.
Thread-safe cache with 24-hour TTL.

Without API key: 25 queries/day, 10 years max.
With BLS_API_KEY env var: 500 queries/day, 20 years max.
"""

import threading
import time
import logging
import requests
import urllib3
from datetime import datetime
from config import Config

# BLS API has recurring SSL certificate issues; suppress warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24 hours

BLS_API_URL = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

# Series IDs for CPI categories
BLS_SERIES = {
    'all_items': {'id': 'CUSR0000SA0',    'label': 'All Items',              'color': '#3b82f6'},
    'core':      {'id': 'CUSR0000SA0L1E', 'label': 'Core (ex Food & Energy)', 'color': '#10b981'},
    'food':      {'id': 'CUSR0000SAF1',   'label': 'Food',                    'color': '#f59e0b'},
    'energy':    {'id': 'CUSR0000SA0E',   'label': 'Energy',                  'color': '#ef4444'},
    'housing':   {'id': 'CUSR0000SAH1',   'label': 'Housing/Shelter',         'color': '#8b5cf6'},
}

# Month period codes to month numbers
PERIOD_MAP = {f'M{str(i).zfill(2)}': i for i in range(1, 13)}


RETRY_BACKOFF = 600  # Wait 10 minutes before retrying after a failure


class BlsCpiCache:
    """Thread-safe cache for BLS CPI data."""

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0
        self._last_fail = 0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                return self._data
            # Don't retry if we recently failed (avoid burning BLS quota)
            if self._last_fail and (time.time() - self._last_fail) < RETRY_BACKOFF:
                return self._data or _empty_result()
        data = _fetch_bls_cpi()
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
                self._last_fail = 0
            return data
        with self._lock:
            self._last_fail = time.time()
            return self._data or _empty_result()

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0
            self._last_fail = 0


_cache = BlsCpiCache()


def _empty_result():
    return {
        'series': {},
        'categories': {k: v['label'] for k, v in BLS_SERIES.items()},
        'colors': {k: v['color'] for k, v in BLS_SERIES.items()},
        'meta': {'source': 'Bureau of Labor Statistics', 'error': 'No data available'}
    }


def _fetch_bls_cpi():
    """Fetch CPI data from BLS API v2."""
    api_key = Config.BLS_API_KEY
    current_year = datetime.utcnow().year

    start_year = current_year - (20 if api_key else 10)
    logger.info(f"BLS CPI fetch: key={'set' if api_key else 'MISSING'}, range={start_year}-{current_year}")

    series_ids = [s['id'] for s in BLS_SERIES.values()]

    payload = {
        'seriesid': series_ids,
        'startyear': str(start_year),
        'endyear': str(current_year),
    }
    headers = {'Content-Type': 'application/json'}

    if api_key:
        payload['registrationkey'] = api_key

    try:
        resp = requests.post(BLS_API_URL, json=payload, headers=headers, timeout=30, verify=False)
        resp.raise_for_status()
        result = resp.json()

        if result.get('status') != 'REQUEST_SUCCEEDED':
            logger.error(f"BLS API error: {result.get('message', 'Unknown')}")
            return None

        # Build series_id -> category_key lookup
        id_to_key = {v['id']: k for k, v in BLS_SERIES.items()}

        series_data = {}
        for series in result.get('Results', {}).get('series', []):
            series_id = series.get('seriesID', '')
            category_key = id_to_key.get(series_id)
            if not category_key:
                continue

            points = []
            for item in series.get('data', []):
                year = item.get('year', '')
                period = item.get('period', '')
                value = item.get('value', '')

                # Only monthly data (skip annual averages M13)
                if period not in PERIOD_MAP:
                    continue

                # BLS uses "-" for unavailable data
                if value == '-' or value == '':
                    continue

                month = PERIOD_MAP[period]
                try:
                    points.append({
                        'year': int(year),
                        'month': month,
                        'period': period,
                        'value': float(value),
                        'date': f'{year}-{str(month).zfill(2)}',
                    })
                except (ValueError, TypeError):
                    continue

            # BLS returns data newest-first; reverse to chronological
            points.sort(key=lambda p: (p['year'], p['month']))
            series_data[category_key] = points

        # Compute YoY percent change for each series
        for key, points in series_data.items():
            for i, pt in enumerate(points):
                pt['yoy_change'] = None
                # Find same month, previous year
                for j in range(i - 1, -1, -1):
                    prev = points[j]
                    if prev['year'] == pt['year'] - 1 and prev['month'] == pt['month']:
                        if prev['value'] != 0:
                            pt['yoy_change'] = round(
                                ((pt['value'] - prev['value']) / prev['value']) * 100, 2
                            )
                        break

        return {
            'series': series_data,
            'categories': {k: v['label'] for k, v in BLS_SERIES.items()},
            'colors': {k: v['color'] for k, v in BLS_SERIES.items()},
            'meta': {
                'source': 'Bureau of Labor Statistics (CPI-U, Seasonally Adjusted)',
                'frequency': 'Monthly',
                'year_range': f'{start_year}-{current_year}',
                'has_api_key': bool(api_key),
            }
        }

    except requests.exceptions.Timeout:
        logger.error("BLS API timeout")
        return None
    except Exception as e:
        logger.error(f"BLS CPI fetch failed: {e}")
        return None


def get_bls_cpi_data():
    """Public API: returns cached BLS CPI data."""
    return _cache.get()


# ══════════════════════════════════════════════════════════
# CPI COMPONENT BREAKDOWN (8 major expenditure categories)
# ══════════════════════════════════════════════════════════

BLS_COMPONENTS = {
    'food_bev':        {'id': 'CUSR0000SAF', 'label': 'Food & Beverages',          'color': '#f59e0b'},
    'housing':         {'id': 'CUSR0000SAH', 'label': 'Housing',                   'color': '#8b5cf6'},
    'apparel':         {'id': 'CUSR0000SAA', 'label': 'Apparel',                   'color': '#ec4899'},
    'transportation':  {'id': 'CUSR0000SAT', 'label': 'Transportation',            'color': '#06b6d4'},
    'medical':         {'id': 'CUSR0000SAM', 'label': 'Medical Care',              'color': '#ef4444'},
    'recreation':      {'id': 'CUSR0000SAR', 'label': 'Recreation',                'color': '#10b981'},
    'education':       {'id': 'CUSR0000SAE', 'label': 'Education & Communication', 'color': '#6366f1'},
    'other':           {'id': 'CUSR0000SAG', 'label': 'Other Goods & Services',    'color': '#64748b'},
}


class BlsComponentCache:
    """Thread-safe cache for BLS CPI component data."""

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0
        self._last_fail = 0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                return self._data
            if self._last_fail and (time.time() - self._last_fail) < RETRY_BACKOFF:
                return self._data or _empty_components_result()
        data = _fetch_bls_components()
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
                self._last_fail = 0
            return data
        with self._lock:
            self._last_fail = time.time()
            return self._data or _empty_components_result()

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0
            self._last_fail = 0


_component_cache = BlsComponentCache()


def _empty_components_result():
    return {
        'series': {},
        'categories': {k: v['label'] for k, v in BLS_COMPONENTS.items()},
        'colors': {k: v['color'] for k, v in BLS_COMPONENTS.items()},
        'meta': {'source': 'Bureau of Labor Statistics', 'error': 'No data available'}
    }


def _fetch_bls_components():
    """Fetch CPI component data from BLS API v2."""
    api_key = Config.BLS_API_KEY
    current_year = datetime.utcnow().year
    start_year = current_year - (20 if api_key else 10)
    logger.info(f"BLS components fetch: key={'set' if api_key else 'MISSING'}, range={start_year}-{current_year}")

    series_ids = [s['id'] for s in BLS_COMPONENTS.values()]

    payload = {
        'seriesid': series_ids,
        'startyear': str(start_year),
        'endyear': str(current_year),
    }
    headers = {'Content-Type': 'application/json'}

    if api_key:
        payload['registrationkey'] = api_key

    try:
        resp = requests.post(BLS_API_URL, json=payload, headers=headers, timeout=30, verify=False)
        resp.raise_for_status()
        result = resp.json()

        if result.get('status') != 'REQUEST_SUCCEEDED':
            logger.error(f"BLS components API error: {result.get('message', 'Unknown')}")
            return None

        id_to_key = {v['id']: k for k, v in BLS_COMPONENTS.items()}

        series_data = {}
        for series in result.get('Results', {}).get('series', []):
            series_id = series.get('seriesID', '')
            category_key = id_to_key.get(series_id)
            if not category_key:
                continue

            points = []
            for item in series.get('data', []):
                year = item.get('year', '')
                period = item.get('period', '')
                value = item.get('value', '')

                if period not in PERIOD_MAP:
                    continue
                if value == '-' or value == '':
                    continue

                month = PERIOD_MAP[period]
                try:
                    points.append({
                        'year': int(year),
                        'month': month,
                        'period': period,
                        'value': float(value),
                        'date': f'{year}-{str(month).zfill(2)}',
                    })
                except (ValueError, TypeError):
                    continue

            points.sort(key=lambda p: (p['year'], p['month']))
            series_data[category_key] = points

        # Compute YoY percent change
        for key, points in series_data.items():
            for i, pt in enumerate(points):
                pt['yoy_change'] = None
                for j in range(i - 1, -1, -1):
                    prev = points[j]
                    if prev['year'] == pt['year'] - 1 and prev['month'] == pt['month']:
                        if prev['value'] != 0:
                            pt['yoy_change'] = round(
                                ((pt['value'] - prev['value']) / prev['value']) * 100, 2
                            )
                        break

        return {
            'series': series_data,
            'categories': {k: v['label'] for k, v in BLS_COMPONENTS.items()},
            'colors': {k: v['color'] for k, v in BLS_COMPONENTS.items()},
            'meta': {
                'source': 'Bureau of Labor Statistics (CPI-U, Seasonally Adjusted)',
                'frequency': 'Monthly',
                'year_range': f'{start_year}-{current_year}',
                'has_api_key': bool(api_key),
            }
        }

    except requests.exceptions.Timeout:
        logger.error("BLS components API timeout")
        return None
    except Exception as e:
        logger.error(f"BLS components fetch failed: {e}")
        return None


def get_bls_components():
    """Public API: returns cached BLS CPI component data."""
    return _component_cache.get()


def clear_bls_caches():
    """Clear both CPI caches to force fresh fetch on next request."""
    _cache.clear()
    _component_cache.clear()
    logger.info("BLS CPI caches cleared")
