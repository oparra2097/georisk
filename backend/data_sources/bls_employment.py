"""
BLS US labor market data client.

Fetches monthly Nonfarm Payrolls (Total, SA) and Unemployment Rate (SA, U-3)
from the BLS Public Data API v2.

Series used:
  CES0000000001  All employees, total nonfarm, seasonally adjusted (thousands)
  LNS14000000    Civilian unemployment rate, seasonally adjusted (%)

Without API key: 25 queries/day, 10 years max history.
With BLS_API_KEY: 500 queries/day, 20 years max history.

Thread-safe cache with a 24-hour TTL — BLS releases monthly, so a daily
refresh is plenty.
"""

import threading
import time
import logging
import requests
import urllib3
from datetime import datetime
from config import Config

# BLS API has recurring SSL certificate issues; suppress warnings (mirror bls_cpi.py).
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

CACHE_TTL = 86400          # 24h
RETRY_BACKOFF = 600        # 10 minutes after a failure before retrying

BLS_API_URL = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

BLS_SERIES = {
    'payrolls': {
        'id': 'CES0000000001',
        'label': 'Nonfarm Payrolls',
        'units': 'Thousands',
        'color': '#3b82f6',
    },
    'unemployment': {
        'id': 'LNS14000000',
        'label': 'Unemployment Rate',
        'units': '%',
        'color': '#ef4444',
    },
}

PERIOD_MAP = {f'M{str(i).zfill(2)}': i for i in range(1, 13)}


class _Cache:
    """Thread-safe cache with TTL and failure backoff."""

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
                return self._data or _empty_result()
        data = _fetch()
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


_cache = _Cache()


def _empty_result():
    return {
        'series': {},
        'categories': {k: v['label'] for k, v in BLS_SERIES.items()},
        'colors': {k: v['color'] for k, v in BLS_SERIES.items()},
        'units': {k: v['units'] for k, v in BLS_SERIES.items()},
        'meta': {'source': 'Bureau of Labor Statistics', 'error': 'No data available'},
    }


def _fetch():
    """Fetch payrolls + unemployment from BLS API v2."""
    api_key = Config.BLS_API_KEY
    current_year = datetime.utcnow().year
    start_year = current_year - (20 if api_key else 10)
    logger.info(
        f"BLS employment fetch: key={'set' if api_key else 'MISSING'}, "
        f"range={start_year}-{current_year}"
    )

    payload = {
        'seriesid': [s['id'] for s in BLS_SERIES.values()],
        'startyear': str(start_year),
        'endyear': str(current_year),
    }
    headers = {'Content-Type': 'application/json'}
    if api_key:
        payload['registrationkey'] = api_key

    try:
        resp = requests.post(BLS_API_URL, json=payload, headers=headers,
                             timeout=30, verify=False)
        resp.raise_for_status()
        result = resp.json()
        if result.get('status') != 'REQUEST_SUCCEEDED':
            logger.error(f"BLS employment API error: {result.get('message', 'unknown')}")
            return None

        id_to_key = {v['id']: k for k, v in BLS_SERIES.items()}
        series_data = {}

        for series in result.get('Results', {}).get('series', []):
            sid = series.get('seriesID', '')
            key = id_to_key.get(sid)
            if not key:
                continue

            points = []
            for item in series.get('data', []):
                period = item.get('period', '')
                if period not in PERIOD_MAP:
                    continue
                value = item.get('value', '')
                if value in ('-', ''):
                    continue
                try:
                    year = int(item.get('year', ''))
                    month = PERIOD_MAP[period]
                    points.append({
                        'year': year,
                        'month': month,
                        'period': period,
                        'value': float(value),
                        'date': f'{year}-{str(month).zfill(2)}',
                    })
                except (ValueError, TypeError):
                    continue

            points.sort(key=lambda p: (p['year'], p['month']))

            # MoM change (level + percent) and YoY percent.  For payrolls the
            # MoM level change in thousands is the headline figure markets
            # react to ("payrolls came in at +180k"), so we surface it
            # alongside the rate metrics.
            for i, pt in enumerate(points):
                pt['mom_change'] = None
                pt['mom_pct'] = None
                pt['yoy_change'] = None
                if i > 0:
                    prev = points[i - 1]
                    pt['mom_change'] = round(pt['value'] - prev['value'], 2)
                    if prev['value'] != 0:
                        pt['mom_pct'] = round(
                            ((pt['value'] - prev['value']) / prev['value']) * 100, 3
                        )
                # YoY: same month last year (skip if not present)
                for j in range(i - 1, -1, -1):
                    prev = points[j]
                    if prev['year'] == pt['year'] - 1 and prev['month'] == pt['month']:
                        if prev['value'] != 0:
                            pt['yoy_change'] = round(
                                ((pt['value'] - prev['value']) / prev['value']) * 100, 2
                            )
                        break

            series_data[key] = points

        return {
            'series': series_data,
            'categories': {k: v['label'] for k, v in BLS_SERIES.items()},
            'colors': {k: v['color'] for k, v in BLS_SERIES.items()},
            'units': {k: v['units'] for k, v in BLS_SERIES.items()},
            'meta': {
                'source': 'Bureau of Labor Statistics (CES + CPS, Seasonally Adjusted)',
                'frequency': 'Monthly',
                'year_range': f'{start_year}-{current_year}',
                'has_api_key': bool(api_key),
                'series_ids': {k: v['id'] for k, v in BLS_SERIES.items()},
            },
        }

    except requests.exceptions.Timeout:
        logger.error("BLS employment API timeout")
        return None
    except Exception as e:
        logger.error(f"BLS employment fetch failed: {e}")
        return None


def get_bls_employment_data():
    """Public API: cached BLS employment data."""
    return _cache.get()


def clear_bls_employment_cache():
    """Clear cache to force fresh fetch."""
    _cache.clear()
    logger.info("BLS employment cache cleared")
