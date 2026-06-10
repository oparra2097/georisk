"""
BLS US labor market data client.

Pulls the Employment Situation series an economist actually wants on
release day:

  Headline rates  – U-3 (LNS14000000), U-6 (LNS13327709)
                   labor-force participation (LNS11300000),
                   employment-to-population (LNS12300000)
  Aggregate wages – avg hourly earnings, total private (CES0500000003)
                   avg weekly hours,    total private (CES0500000002)
  Sectoral payrolls (CES, SA, thousands):
    Total nonfarm                 CES0000000001
    Total private                 CES0500000001
    Mining & logging              CES1000000001
    Construction                  CES2000000001
    Manufacturing                 CES3000000001
    Trade, transportation, util.  CES4000000001
    Information                   CES5000000001
    Financial activities          CES5500000001
    Professional & business svc.  CES6000000001
    Education & health services   CES6500000001
    Leisure & hospitality         CES7000000001
    Other services                CES8000000001
    Government                    CES9000000001

Without API key: 25 queries/day, 10 years max history.
With BLS_API_KEY: 500 queries/day, 20 years max.

Thread-safe cache, 24h TTL.  Components are pulled in a single BLS API
call (well under the 50-series limit).
"""

import threading
import time
import logging
import requests
import urllib3
from datetime import datetime
from config import Config
from backend.data_sources.bls_cache_utils import (
    is_stale as _is_stale, months_behind as _months_behind, SOFT_RETRY_SECONDS,
    get_bls_api_key,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

CACHE_TTL = 86400          # 24h
RETRY_BACKOFF = 600        # 10 min after a failure before retrying

BLS_API_URL = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

# Headline rates + hours/earnings + sectoral payrolls.  `kind` groups
# series for the UI so we can render the right widget (rate vs level vs
# wage), and `color` is a stable palette so the sector ranking bar chart
# matches the sector breakdown across renders.
BLS_SERIES = {
    # ── Headline rates ────────────────────────────────────────────
    'unemployment': {
        'id': 'LNS14000000', 'label': 'Unemployment Rate (U-3)',
        'kind': 'rate', 'units': '%', 'color': '#ef4444',
    },
    'u6': {
        'id': 'LNS13327709', 'label': 'U-6 (broad underemployment)',
        'kind': 'rate', 'units': '%', 'color': '#f97316',
    },
    'participation': {
        'id': 'LNS11300000', 'label': 'Labor Force Participation',
        'kind': 'rate', 'units': '%', 'color': '#a855f7',
    },
    'employment_population': {
        'id': 'LNS12300000', 'label': 'Employment-to-Population',
        'kind': 'rate', 'units': '%', 'color': '#8b5cf6',
    },
    # ── Wages and hours ──────────────────────────────────────────
    'avg_hourly_earnings': {
        'id': 'CES0500000003', 'label': 'Avg Hourly Earnings (Total Private)',
        'kind': 'wage', 'units': '$/hr', 'color': '#10b981',
    },
    'avg_weekly_hours': {
        'id': 'CES0500000002', 'label': 'Avg Weekly Hours (Total Private)',
        'kind': 'hours', 'units': 'hrs', 'color': '#14b8a6',
    },
    # ── Headline payroll levels ──────────────────────────────────
    'payrolls': {
        'id': 'CES0000000001', 'label': 'Nonfarm Payrolls (Total)',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#3b82f6',
        'sector': True,
    },
    'payrolls_private': {
        'id': 'CES0500000001', 'label': 'Total Private',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#2563eb',
        'sector': True,
    },
    # ── Sector breakdown ─────────────────────────────────────────
    'mining_logging': {
        'id': 'CES1000000001', 'label': 'Mining & Logging',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#92400e',
        'sector': True, 'sector_group': 'goods',
    },
    'construction': {
        'id': 'CES2000000001', 'label': 'Construction',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#b45309',
        'sector': True, 'sector_group': 'goods',
    },
    'manufacturing': {
        'id': 'CES3000000001', 'label': 'Manufacturing',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#d97706',
        'sector': True, 'sector_group': 'goods',
    },
    'trade_transport_util': {
        'id': 'CES4000000001', 'label': 'Trade, Transport & Utilities',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#0891b2',
        'sector': True, 'sector_group': 'services',
    },
    'information': {
        'id': 'CES5000000001', 'label': 'Information',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#0ea5e9',
        'sector': True, 'sector_group': 'services',
    },
    'financial': {
        'id': 'CES5500000001', 'label': 'Financial Activities',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#6366f1',
        'sector': True, 'sector_group': 'services',
    },
    'professional_business': {
        'id': 'CES6000000001', 'label': 'Professional & Business Services',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#8b5cf6',
        'sector': True, 'sector_group': 'services',
    },
    'education_health': {
        'id': 'CES6500000001', 'label': 'Education & Health Services',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#ec4899',
        'sector': True, 'sector_group': 'services',
    },
    'leisure_hospitality': {
        'id': 'CES7000000001', 'label': 'Leisure & Hospitality',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#f43f5e',
        'sector': True, 'sector_group': 'services',
    },
    'other_services': {
        'id': 'CES8000000001', 'label': 'Other Services',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#64748b',
        'sector': True, 'sector_group': 'services',
    },
    'government': {
        'id': 'CES9000000001', 'label': 'Government',
        'kind': 'payroll', 'units': 'Thousands', 'color': '#475569',
        'sector': True, 'sector_group': 'government',
    },
}

PERIOD_MAP = {f'M{str(i).zfill(2)}': i for i in range(1, 13)}


class _Cache:
    """Thread-safe cache with TTL, failure backoff, and staleness guard.

    Staleness guard: if a successful fetch comes back with `latest_month`
    more than 2 months behind today (see bls_cache_utils.STALE_LAG_MONTHS),
    we still serve the data so the page doesn't go blank, but we mark the
    cache as failed and use a short retry backoff (30 min) so the next
    request automatically re-fetches.  This prevents the 24h TTL from
    locking us into a stale snapshot for a whole day after a bad warm-up.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0
        self._last_fail = 0
        self._retry_backoff = RETRY_BACKOFF

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                return self._data
            if self._last_fail and (time.time() - self._last_fail) < self._retry_backoff:
                return self._data or _empty_result()
        data = _fetch()
        if data:
            latest = (data.get('meta') or {}).get('latest_month')
            stale = _is_stale(latest)
            with self._lock:
                # Annotate the payload so the UI can show a "data may be
                # stale" chip without a second round-trip.
                data['meta']['is_stale'] = stale
                data['meta']['months_behind'] = _months_behind(latest)
                self._data = data
                if stale:
                    logger.warning(
                        f"BLS employment data stale: latest_month={latest}, "
                        f"will retry in {SOFT_RETRY_SECONDS}s"
                    )
                    self._last_fetch = 0
                    self._last_fail = time.time()
                    self._retry_backoff = SOFT_RETRY_SECONDS
                else:
                    self._last_fetch = time.time()
                    self._last_fail = 0
                    self._retry_backoff = RETRY_BACKOFF
            return data
        with self._lock:
            self._last_fail = time.time()
            self._retry_backoff = RETRY_BACKOFF
            return self._data or _empty_result()

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0
            self._last_fail = 0
            self._retry_backoff = RETRY_BACKOFF


_cache = _Cache()


def _empty_result():
    return {
        'series': {},
        'categories': {k: v['label'] for k, v in BLS_SERIES.items()},
        'colors': {k: v['color'] for k, v in BLS_SERIES.items()},
        'units': {k: v['units'] for k, v in BLS_SERIES.items()},
        'kinds': {k: v['kind'] for k, v in BLS_SERIES.items()},
        'sector_keys': [k for k, v in BLS_SERIES.items() if v.get('sector')],
        'rankings': {'mom': [], 'yoy': []},
        'meta': {'source': 'Bureau of Labor Statistics', 'error': 'No data available'},
    }


def _fetch(diagnostic: bool = False):
    """Fetch all employment series from BLS API v2.

    When `diagnostic=True`, also returns a `_diagnostic` block on the
    payload with per-series latest month, BLS API status messages, and
    request metadata — so the /diagnostics endpoint can show exactly
    what came back even when normal logic would discard a partial
    response.
    """
    api_key = get_bls_api_key()
    current_year = datetime.utcnow().year
    start_year = current_year - (20 if api_key else 10)
    logger.info(
        f"BLS employment fetch: key={'set' if api_key else 'MISSING'}, "
        f"range={start_year}-{current_year}, series={len(BLS_SERIES)}"
    )

    payload = {
        'seriesid': [s['id'] for s in BLS_SERIES.values()],
        'startyear': str(start_year),
        'endyear': str(current_year),
    }
    if api_key:
        payload['registrationkey'] = api_key

    diag = {
        'has_api_key': bool(api_key),
        'request': {'startyear': str(start_year), 'endyear': str(current_year),
                    'series_count': len(BLS_SERIES)},
        'bls_api_status': None,
        'bls_api_message': None,
        'http_status': None,
        'series_returned': 0,
        'latest_month_per_series': {},
        'series_with_no_data': [],
        'error': None,
    }

    try:
        resp = requests.post(BLS_API_URL, json=payload,
                             headers={'Content-Type': 'application/json'},
                             timeout=45, verify=False)
        diag['http_status'] = resp.status_code
        resp.raise_for_status()
        result = resp.json()
        diag['bls_api_status'] = result.get('status')
        msg = result.get('message')
        if isinstance(msg, list):
            diag['bls_api_message'] = '; '.join(str(m) for m in msg) if msg else None
        else:
            diag['bls_api_message'] = msg or None
        if result.get('status') != 'REQUEST_SUCCEEDED':
            logger.error(
                f"BLS employment API error: status={result.get('status')}, "
                f"message={diag['bls_api_message']}"
            )
            if diagnostic:
                return {'_diagnostic': diag}
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

            # For every monthly point compute MoM Δ (level + %) and YoY %.
            # Sector-level YoY in *thousands* is the headline number an
            # economist scans, so we also keep it as `yoy_change_level`.
            for i, pt in enumerate(points):
                pt['mom_change'] = None
                pt['mom_pct'] = None
                pt['yoy_change'] = None
                pt['yoy_change_level'] = None
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
                            pt['yoy_change_level'] = round(pt['value'] - prev['value'], 3)
                        break

            series_data[key] = points

        rankings = _build_rankings(series_data)
        latest_month = _latest_month(series_data)

        # Per-series freshness — surfaced both in normal meta (for the
        # stale-banner detail) and in the diagnostic payload so we can
        # see whether *one* bad series is dragging latest_month down.
        latest_per_series = {}
        no_data = []
        for key, pts in series_data.items():
            if pts:
                latest_per_series[key] = pts[-1]['date']
            else:
                no_data.append(key)
        for key in BLS_SERIES:
            if key not in series_data:
                no_data.append(key)
        diag.update({
            'series_returned': len(series_data),
            'latest_month_per_series': latest_per_series,
            'series_with_no_data': no_data,
        })
        logger.info(
            f"BLS employment fetch OK: series_returned={len(series_data)}, "
            f"latest_month={latest_month}, no_data={no_data or 'none'}"
        )

        return {
            'series': series_data,
            'categories': {k: v['label'] for k, v in BLS_SERIES.items()},
            'colors': {k: v['color'] for k, v in BLS_SERIES.items()},
            'units': {k: v['units'] for k, v in BLS_SERIES.items()},
            'kinds': {k: v['kind'] for k, v in BLS_SERIES.items()},
            'sector_keys': [k for k, v in BLS_SERIES.items() if v.get('sector')],
            'sector_groups': {
                k: v.get('sector_group')
                for k, v in BLS_SERIES.items() if v.get('sector_group')
            },
            'rankings': rankings,
            'meta': {
                'source': 'Bureau of Labor Statistics (CES + CPS, Seasonally Adjusted)',
                'frequency': 'Monthly',
                'year_range': f'{start_year}-{current_year}',
                'latest_month': latest_month,
                'latest_month_per_series': latest_per_series,
                'series_with_no_data': no_data,
                'has_api_key': bool(api_key),
                'bls_api_status': diag['bls_api_status'],
                'bls_api_message': diag['bls_api_message'],
                'series_ids': {k: v['id'] for k, v in BLS_SERIES.items()},
                'fetched_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
            },
            '_diagnostic': diag if diagnostic else None,
        }

    except requests.exceptions.Timeout:
        diag['error'] = 'timeout'
        logger.error("BLS employment API timeout")
        if diagnostic:
            return {'_diagnostic': diag}
        return None
    except Exception as e:
        diag['error'] = str(e)
        logger.error(f"BLS employment fetch failed: {e}")
        if diagnostic:
            return {'_diagnostic': diag}
        return None


def run_diagnostic():
    """Force a fresh BLS call, bypassing the cache. Returns the
    diagnostic block so the /diagnostics endpoint can surface raw API
    status + per-series freshness without polluting the regular cache.
    """
    result = _fetch(diagnostic=True)
    if not result:
        return {'error': 'fetch returned None'}
    diag = result.get('_diagnostic') or {}
    diag['payload_received'] = result.get('series') is not None
    diag['latest_month'] = (result.get('meta') or {}).get('latest_month')
    diag['is_stale'] = _is_stale(diag.get('latest_month'))
    diag['months_behind'] = _months_behind(diag.get('latest_month'))
    return diag


def _latest_month(series_data: dict) -> str:
    months = []
    for points in series_data.values():
        if points:
            months.append(points[-1]['date'])
    return max(months) if months else ''


def _build_rankings(series_data: dict) -> dict:
    """Rank sector payrolls by MoM and YoY change at the latest month.

    Returns a dict with 'mom' and 'yoy' lists, each sorted from largest
    gain to largest loss.  Headline payrolls and Total Private are kept
    in the table but flagged so the UI can highlight them separately.
    """
    mom_rows, yoy_rows = [], []
    for key, points in series_data.items():
        meta = BLS_SERIES.get(key, {})
        if not meta.get('sector'):
            continue
        if not points:
            continue
        latest = points[-1]
        mom_rows.append({
            'key': key,
            'label': meta['label'],
            'color': meta['color'],
            'sector_group': meta.get('sector_group', 'headline'),
            'is_headline': key in ('payrolls', 'payrolls_private'),
            'date': latest['date'],
            'level': latest['value'],
            'change_thousands': latest.get('mom_change'),
            'change_pct': latest.get('mom_pct'),
        })
        yoy_rows.append({
            'key': key,
            'label': meta['label'],
            'color': meta['color'],
            'sector_group': meta.get('sector_group', 'headline'),
            'is_headline': key in ('payrolls', 'payrolls_private'),
            'date': latest['date'],
            'level': latest['value'],
            'change_thousands': latest.get('yoy_change_level'),
            'change_pct': latest.get('yoy_change'),
        })

    mom_rows.sort(
        key=lambda r: (r['change_thousands'] if r['change_thousands'] is not None else -1e9),
        reverse=True,
    )
    yoy_rows.sort(
        key=lambda r: (r['change_pct'] if r['change_pct'] is not None else -1e9),
        reverse=True,
    )
    return {'mom': mom_rows, 'yoy': yoy_rows}


def get_bls_employment_data():
    """Public API: cached BLS employment data."""
    return _cache.get()


def clear_bls_employment_cache():
    """Clear cache to force fresh fetch."""
    _cache.clear()
    logger.info("BLS employment cache cleared")
