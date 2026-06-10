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
    'all_items': {'id': 'CUUR0000SA0',    'label': 'All Items',              'color': '#3b82f6'},
    'core':      {'id': 'CUUR0000SA0L1E', 'label': 'Core (ex Food & Energy)', 'color': '#10b981'},
    'food':      {'id': 'CUUR0000SAF1',   'label': 'Food',                    'color': '#f59e0b'},
    'energy':    {'id': 'CUUR0000SA0E',   'label': 'Energy',                  'color': '#ef4444'},
    'housing':   {'id': 'CUUR0000SAH1',   'label': 'Housing/Shelter',         'color': '#8b5cf6'},
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
                'source': 'Bureau of Labor Statistics (CPI-U, Not Seasonally Adjusted)',
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
    'food_bev':        {'id': 'CUUR0000SAF', 'label': 'Food & Beverages',          'color': '#f59e0b'},
    'housing':         {'id': 'CUUR0000SAH', 'label': 'Housing',                   'color': '#8b5cf6'},
    'apparel':         {'id': 'CUUR0000SAA', 'label': 'Apparel',                   'color': '#ec4899'},
    'transportation':  {'id': 'CUUR0000SAT', 'label': 'Transportation',            'color': '#06b6d4'},
    'medical':         {'id': 'CUUR0000SAM', 'label': 'Medical Care',              'color': '#ef4444'},
    'recreation':      {'id': 'CUUR0000SAR', 'label': 'Recreation',                'color': '#10b981'},
    'education':       {'id': 'CUUR0000SAE', 'label': 'Education & Communication', 'color': '#6366f1'},
    'other':           {'id': 'CUUR0000SAG', 'label': 'Other Goods & Services',    'color': '#64748b'},
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
                'source': 'Bureau of Labor Statistics (CPI-U, Not Seasonally Adjusted)',
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
    _detail_cache.clear()
    logger.info("BLS CPI caches cleared")


# ══════════════════════════════════════════════════════════
# DETAIL COMPONENTS — granular series an economist scans on release day
# ══════════════════════════════════════════════════════════
#
# Anatomy of a BLS CPI series ID:
#   CUUR             — Consumer Price Index for All Urban Consumers (NSA, U)
#   0000             — area (U.S. city average)
#   SAxx / SExx     — major group (S) and item code
#
# These are the items markets focus on around the print: shelter (and
# its two main sub-pieces, rent + OER), food at home vs away, the goods
# rotation (cars old + new, apparel), the energy strip (gasoline,
# electricity, gas), the medical strip, transport services + airfares,
# and the core services proxy.

BLS_DETAIL_COMPONENTS = {
    # ── Shelter ─────────────────────────────────────────────────
    'shelter':            {'id': 'CUUR0000SAH1',  'label': 'Shelter',
                           'group': 'shelter', 'color': '#8b5cf6'},
    'rent_primary':       {'id': 'CUUR0000SEHA',  'label': 'Rent of Primary Residence',
                           'group': 'shelter', 'color': '#a855f7'},
    'oer':                {'id': 'CUUR0000SEHC',  'label': "Owners' Equivalent Rent",
                           'group': 'shelter', 'color': '#c084fc'},
    # ── Food ────────────────────────────────────────────────────
    'food_at_home':       {'id': 'CUUR0000SAF11', 'label': 'Food at Home',
                           'group': 'food', 'color': '#f59e0b'},
    'food_away':          {'id': 'CUUR0000SEFV',  'label': 'Food Away From Home',
                           'group': 'food', 'color': '#fbbf24'},
    # ── Vehicles ────────────────────────────────────────────────
    'new_vehicles':       {'id': 'CUUR0000SETA01', 'label': 'New Vehicles',
                           'group': 'vehicles', 'color': '#0891b2'},
    'used_vehicles':      {'id': 'CUUR0000SETA02', 'label': 'Used Cars & Trucks',
                           'group': 'vehicles', 'color': '#06b6d4'},
    'motor_vehicle_ins':  {'id': 'CUUR0000SETE',   'label': 'Motor Vehicle Insurance',
                           'group': 'vehicles', 'color': '#22d3ee'},
    # ── Energy ──────────────────────────────────────────────────
    'gasoline':           {'id': 'CUUR0000SETB01', 'label': 'Gasoline (All Types)',
                           'group': 'energy', 'color': '#dc2626'},
    'electricity':        {'id': 'CUUR0000SEHF01', 'label': 'Electricity',
                           'group': 'energy', 'color': '#f97316'},
    'natural_gas':        {'id': 'CUUR0000SEHF02', 'label': 'Utility (Piped) Gas',
                           'group': 'energy', 'color': '#fb923c'},
    # ── Goods ───────────────────────────────────────────────────
    'apparel':            {'id': 'CUUR0000SAA',    'label': 'Apparel',
                           'group': 'goods', 'color': '#ec4899'},
    # ── Medical ─────────────────────────────────────────────────
    'medical_services':   {'id': 'CUUR0000SAM2',   'label': 'Medical Care Services',
                           'group': 'medical', 'color': '#ef4444'},
    'medical_goods':      {'id': 'CUUR0000SAM1',   'label': 'Medical Care Commodities',
                           'group': 'medical', 'color': '#f87171'},
    # ── Transport services / leisure ────────────────────────────
    'transport_services': {'id': 'CUUR0000SAS4',   'label': 'Transportation Services',
                           'group': 'services', 'color': '#10b981'},
    'airline_fares':      {'id': 'CUUR0000SETG01', 'label': 'Airline Fares',
                           'group': 'services', 'color': '#34d399'},
    'recreation_svc':     {'id': 'CUUR0000SAR1',   'label': 'Recreation Services',
                           'group': 'services', 'color': '#6366f1'},
    'education_svc':      {'id': 'CUUR0000SEEB',   'label': 'Education',
                           'group': 'services', 'color': '#818cf8'},
    # ── Aggregate core / services / commodities ────────────────
    'core_services':      {'id': 'CUUR0000SASLE',  'label': 'Services Less Energy Services',
                           'group': 'aggregate', 'color': '#94a3b8',
                           'is_aggregate': True},
    'core_goods':         {'id': 'CUUR0000SACL1E', 'label': 'Commodities Less Food & Energy',
                           'group': 'aggregate', 'color': '#64748b',
                           'is_aggregate': True},
}


class _DetailCache:
    """Thread-safe cache for the richer detail-component dataset."""

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
                return self._data or _empty_detail_result()
        data = _fetch_bls_detail()
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
                self._last_fail = 0
            return data
        with self._lock:
            self._last_fail = time.time()
            return self._data or _empty_detail_result()

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0
            self._last_fail = 0


_detail_cache = _DetailCache()


def _empty_detail_result():
    return {
        'series': {},
        'categories': {k: v['label'] for k, v in BLS_DETAIL_COMPONENTS.items()},
        'colors': {k: v['color'] for k, v in BLS_DETAIL_COMPONENTS.items()},
        'groups': {k: v['group'] for k, v in BLS_DETAIL_COMPONENTS.items()},
        'rankings': {'mom': [], 'yoy': []},
        'meta': {'source': 'Bureau of Labor Statistics', 'error': 'No data available'},
    }


def _fetch_bls_detail():
    """Fetch the detail-level CPI components from BLS API v2."""
    api_key = Config.BLS_API_KEY
    current_year = datetime.utcnow().year
    start_year = current_year - (20 if api_key else 10)
    logger.info(
        f"BLS CPI detail fetch: key={'set' if api_key else 'MISSING'}, "
        f"range={start_year}-{current_year}, series={len(BLS_DETAIL_COMPONENTS)}"
    )

    payload = {
        'seriesid': [s['id'] for s in BLS_DETAIL_COMPONENTS.values()],
        'startyear': str(start_year),
        'endyear': str(current_year),
    }
    headers = {'Content-Type': 'application/json'}
    if api_key:
        payload['registrationkey'] = api_key

    try:
        resp = requests.post(BLS_API_URL, json=payload, headers=headers,
                             timeout=45, verify=False)
        resp.raise_for_status()
        result = resp.json()
        if result.get('status') != 'REQUEST_SUCCEEDED':
            logger.error(f"BLS CPI detail API error: {result.get('message', 'unknown')}")
            return None

        id_to_key = {v['id']: k for k, v in BLS_DETAIL_COMPONENTS.items()}
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

            # Compute MoM Δ (index points), MoM %, and YoY %.
            for i, pt in enumerate(points):
                pt['mom_change'] = None
                pt['mom_pct'] = None
                pt['yoy_change'] = None
                if i > 0:
                    prev = points[i - 1]
                    pt['mom_change'] = round(pt['value'] - prev['value'], 3)
                    if prev['value'] != 0:
                        pt['mom_pct'] = round(
                            ((pt['value'] - prev['value']) / prev['value']) * 100, 3
                        )
                for j in range(i - 1, -1, -1):
                    prev = points[j]
                    if prev['year'] == pt['year'] - 1 and prev['month'] == pt['month']:
                        if prev['value'] != 0:
                            pt['yoy_change'] = round(
                                ((pt['value'] - prev['value']) / prev['value']) * 100, 3
                            )
                        break

            series_data[key] = points

        rankings = _build_cpi_rankings(series_data)
        latest_month = ''
        for pts in series_data.values():
            if pts and pts[-1]['date'] > latest_month:
                latest_month = pts[-1]['date']

        return {
            'series': series_data,
            'categories': {k: v['label'] for k, v in BLS_DETAIL_COMPONENTS.items()},
            'colors': {k: v['color'] for k, v in BLS_DETAIL_COMPONENTS.items()},
            'groups': {k: v['group'] for k, v in BLS_DETAIL_COMPONENTS.items()},
            'aggregates': [k for k, v in BLS_DETAIL_COMPONENTS.items()
                           if v.get('is_aggregate')],
            'rankings': rankings,
            'meta': {
                'source': 'Bureau of Labor Statistics (CPI-U, Not Seasonally Adjusted)',
                'frequency': 'Monthly',
                'year_range': f'{start_year}-{current_year}',
                'latest_month': latest_month,
                'has_api_key': bool(api_key),
                'series_ids': {k: v['id'] for k, v in BLS_DETAIL_COMPONENTS.items()},
                'fetched_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
            },
        }

    except requests.exceptions.Timeout:
        logger.error("BLS CPI detail API timeout")
        return None
    except Exception as e:
        logger.error(f"BLS CPI detail fetch failed: {e}")
        return None


def _build_cpi_rankings(series_data: dict) -> dict:
    """Rank components by MoM and YoY at the latest available month.

    Returns two lists ('mom' and 'yoy'), each sorted largest gain →
    largest decline.  Aggregate buckets (services less energy, core
    goods) are kept in but flagged so the UI can group them.
    """
    mom_rows, yoy_rows = [], []
    for key, points in series_data.items():
        if not points:
            continue
        meta = BLS_DETAIL_COMPONENTS.get(key, {})
        latest = points[-1]
        common = {
            'key': key,
            'label': meta.get('label', key),
            'color': meta.get('color', '#64748b'),
            'group': meta.get('group', 'other'),
            'is_aggregate': bool(meta.get('is_aggregate')),
            'date': latest['date'],
            'level': latest['value'],
        }
        mom_rows.append({
            **common,
            'change_pct': latest.get('mom_pct'),
            'change_value': latest.get('mom_change'),
        })
        yoy_rows.append({
            **common,
            'change_pct': latest.get('yoy_change'),
        })

    mom_rows.sort(
        key=lambda r: (r['change_pct'] if r['change_pct'] is not None else -1e9),
        reverse=True,
    )
    yoy_rows.sort(
        key=lambda r: (r['change_pct'] if r['change_pct'] is not None else -1e9),
        reverse=True,
    )
    return {'mom': mom_rows, 'yoy': yoy_rows}


def get_bls_cpi_detail():
    """Public API: cached BLS CPI detail-component data + rankings."""
    return _detail_cache.get()
