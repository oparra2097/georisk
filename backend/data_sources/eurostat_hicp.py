"""
Eurostat HICP (Harmonised Index of Consumer Prices) data client.

Uses Eurostat Statistics API (JSON-stat 2.0) to fetch Euro Area HICP data.
No authentication needed. Thread-safe cache with 24-hour TTL.
"""

import threading
import time
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24 hours
RETRY_BACKOFF = 3600  # 1 hour before retrying after failure

EUROSTAT_BASE = (
    'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr'
    '?geo=EA&unit=RCH_A&freq=M&sinceTimePeriod=2004-01'
)

# Overview series: 5 headline aggregates
EUROSTAT_SERIES = {
    'all_items': {'coicop': 'CP00',            'label': 'All Items (HICP)',          'color': '#3b82f6'},
    'core':      {'coicop': 'TOT_X_NRG_FOOD', 'label': 'Core (ex Energy & Food)',   'color': '#10b981'},
    'food':      {'coicop': 'CP01',            'label': 'Food',                       'color': '#f59e0b'},
    'energy':    {'coicop': 'NRG',             'label': 'Energy',                     'color': '#ef4444'},
    'housing':   {'coicop': 'CP04',            'label': 'Housing & Utilities',        'color': '#8b5cf6'},
}

# Component breakdown: 12 COICOP divisions
EUROSTAT_COMPONENTS = {
    'food':          {'coicop': 'CP01', 'label': 'Food & Non-Alcoholic Beverages', 'color': '#f59e0b'},
    'alcohol':       {'coicop': 'CP02', 'label': 'Alcoholic Beverages & Tobacco',  'color': '#a855f7'},
    'clothing':      {'coicop': 'CP03', 'label': 'Clothing & Footwear',            'color': '#ec4899'},
    'housing':       {'coicop': 'CP04', 'label': 'Housing, Water & Fuels',         'color': '#8b5cf6'},
    'furniture':     {'coicop': 'CP05', 'label': 'Furnishings & Household',         'color': '#f97316'},
    'health':        {'coicop': 'CP06', 'label': 'Health',                          'color': '#ef4444'},
    'transport':     {'coicop': 'CP07', 'label': 'Transport',                       'color': '#06b6d4'},
    'communication': {'coicop': 'CP08', 'label': 'Communications',                  'color': '#64748b'},
    'recreation':    {'coicop': 'CP09', 'label': 'Recreation & Culture',            'color': '#10b981'},
    'education':     {'coicop': 'CP10', 'label': 'Education',                       'color': '#6366f1'},
    'restaurants':   {'coicop': 'CP11', 'label': 'Restaurants & Hotels',            'color': '#e11d48'},
    'misc':          {'coicop': 'CP12', 'label': 'Miscellaneous Goods & Services',  'color': '#84cc16'},
}


def _build_url(coicop_map):
    """Build Eurostat API URL with multiple COICOP codes."""
    codes = [info['coicop'] for info in coicop_map.values()]
    params = '&'.join(f'coicop={c}' for c in codes)
    return f'{EUROSTAT_BASE}&{params}'


def _parse_jsonstat(resp_json, coicop_map):
    """Parse JSON-stat 2.0 response into per-series monthly data points.

    Returns dict: {series_key: [{year, month, value, date}, ...]}
    """
    dimensions = resp_json.get('dimension', {})
    values = resp_json.get('value', {})

    if not values:
        return None

    # Time dimension
    time_dim = dimensions.get('time', {}).get('category', {}).get('index', {})
    if not time_dim:
        return None

    # COICOP dimension
    coicop_dim = dimensions.get('coicop', {}).get('category', {}).get('index', {})
    if not coicop_dim:
        return None

    num_times = len(time_dim)

    # Reverse map: COICOP code -> series key
    code_to_key = {}
    for key, info in coicop_map.items():
        code_to_key[info['coicop']] = key

    series_data = {}

    for coicop_code, coicop_pos in coicop_dim.items():
        series_key = code_to_key.get(coicop_code)
        if not series_key:
            continue

        points = []
        for period, time_pos in time_dim.items():
            idx = str(coicop_pos * num_times + time_pos)
            val = values.get(idx)
            if val is None:
                continue

            try:
                parts = period.split('-')
                year = int(parts[0])
                month = int(parts[1])
            except (ValueError, IndexError):
                continue

            points.append({
                'year': year,
                'month': month,
                'value': float(val),
                'date': f'{year}-{str(month).zfill(2)}',
            })

        points.sort(key=lambda p: (p['year'], p['month']))
        if points:
            series_data[series_key] = points

    return series_data if series_data else None


def _fetch_eurostat_data(coicop_map):
    """Fetch HICP data from Eurostat for given COICOP map."""
    url = _build_url(coicop_map)
    try:
        resp = requests.get(url, timeout=45, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)',
            'Accept': 'application/json',
        })

        if resp.status_code != 200:
            logger.warning(f"Eurostat API returned {resp.status_code}")
            return None

        data = resp.json()
        series_data = _parse_jsonstat(data, coicop_map)
        if not series_data:
            return None

        # Determine year range
        all_years = set()
        for points in series_data.values():
            for pt in points:
                all_years.add(pt['year'])

        min_year = min(all_years) if all_years else datetime.utcnow().year - 10
        max_year = max(all_years) if all_years else datetime.utcnow().year

        return {
            'series': series_data,
            'categories': {k: v['label'] for k, v in coicop_map.items()},
            'colors': {k: v['color'] for k, v in coicop_map.items()},
            'meta': {
                'source': 'Eurostat (HICP, Euro Area)',
                'frequency': 'Monthly',
                'year_range': f'{min_year}-{max_year}',
            }
        }

    except requests.exceptions.Timeout:
        logger.error("Eurostat API timeout")
        return None
    except Exception as e:
        logger.error(f"Eurostat HICP fetch failed: {e}")
        return None


class EurostatCpiCache:
    """Thread-safe cache for Eurostat HICP overview data."""

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
                return self._data or _empty_result(EUROSTAT_SERIES)
        data = _fetch_eurostat_data(EUROSTAT_SERIES)
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
                self._last_fail = 0
            return data
        with self._lock:
            self._last_fail = time.time()
            return self._data or _empty_result(EUROSTAT_SERIES)

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0
            self._last_fail = 0


class EurostatComponentCache:
    """Thread-safe cache for Eurostat HICP component data."""

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
                return self._data or _empty_result(EUROSTAT_COMPONENTS)
        data = _fetch_eurostat_data(EUROSTAT_COMPONENTS)
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
                self._last_fail = 0
            return data
        with self._lock:
            self._last_fail = time.time()
            return self._data or _empty_result(EUROSTAT_COMPONENTS)

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0
            self._last_fail = 0


def _empty_result(coicop_map):
    return {
        'series': {},
        'categories': {k: v['label'] for k, v in coicop_map.items()},
        'colors': {k: v['color'] for k, v in coicop_map.items()},
        'meta': {'source': 'Eurostat', 'error': 'No data available'}
    }


_cache = EurostatCpiCache()
_component_cache = EurostatComponentCache()


def get_eurostat_cpi_data():
    """Public API: returns cached Eurostat HICP overview data."""
    return _cache.get()


def get_eurostat_components():
    """Public API: returns cached Eurostat HICP component data."""
    return _component_cache.get()
