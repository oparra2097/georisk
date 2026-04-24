"""
Eurostat labour-market client: monthly youth (Y_LT25) and total (TOTAL)
unemployment rates from dataset `une_rt_m`.

No auth. Returns per-country history so one call covers all EU-27 members.
Thread-safe cache with 24h TTL + 1h retry backoff on failure.
"""

import logging
import threading
import time
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 86400
RETRY_BACKOFF = 3600

EUROSTAT_BASE = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/une_rt_m'

# EU-27 ISO2 codes used as Eurostat geo labels (same codes, except UK is 'UK' not 'GB',
# and Greece is 'EL' not 'GR' in Eurostat — we map that on the way in/out).
EU27 = [
    'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE',
    'EL', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT',
    'RO', 'SK', 'SI', 'ES', 'SE',
]

# Map Eurostat geo → our ISO2
EUROSTAT_TO_ISO2 = {'EL': 'GR', 'UK': 'GB'}
ISO2_TO_EUROSTAT = {v: k for k, v in EUROSTAT_TO_ISO2.items()}

_lock = threading.RLock()
_cache = {
    'youth':  {'data': None, 'fetched_at': 0, 'last_fail': 0},
    'total':  {'data': None, 'fetched_at': 0, 'last_fail': 0},
}


def _build_url(age: str, geos: List[str]) -> str:
    geo_params = '&'.join(f'geo={g}' for g in geos)
    return (
        f'{EUROSTAT_BASE}?age={age}&sex=T&unit=PC_ACT&s_adj=SA&freq=M'
        f'&sinceTimePeriod=2010-01&{geo_params}'
    )


def _parse_jsonstat(body) -> Dict[str, List[dict]]:
    """
    Parse Eurostat JSON-stat 2.0 into {iso2: [{date, value}, ...]}.

    The dataset has 5 dimensions we actually vary (age, sex, unit, s_adj, geo, time)
    but we pin everything except geo and time in the URL, so the remaining indexing
    collapses to (geo, time).
    """
    dims = body.get('dimension', {})
    values = body.get('value', {})
    if not values:
        return {}

    # geo index: ES → 0, FR → 1, etc (order from Eurostat, not our list)
    geo_idx = dims.get('geo', {}).get('category', {}).get('index', {})
    time_idx = dims.get('time', {}).get('category', {}).get('index', {})
    if not geo_idx or not time_idx:
        return {}

    num_times = len(time_idx)
    # Build reverse lookups
    geo_to_pos = geo_idx if isinstance(geo_idx, dict) else {g: i for i, g in enumerate(geo_idx)}
    time_to_pos = time_idx if isinstance(time_idx, dict) else {t: i for i, t in enumerate(time_idx)}

    out: Dict[str, List[dict]] = {}
    for geo_code, geo_pos in geo_to_pos.items():
        iso2 = EUROSTAT_TO_ISO2.get(geo_code, geo_code)
        points = []
        for period, t_pos in time_to_pos.items():
            idx = str(geo_pos * num_times + t_pos)
            v = values.get(idx)
            if v is None:
                continue
            try:
                year, month = period.split('-')
                year_i = int(year)
                month_i = int(month[1:]) if month.startswith('M') else int(month)
            except (ValueError, IndexError):
                continue
            points.append({
                'date': f'{year_i}-{str(month_i).zfill(2)}-01',
                'value': float(v),
                'year': year_i,
                'month': month_i,
            })
        points.sort(key=lambda p: (p['year'], p['month']))
        if points:
            out[iso2] = points
    return out


def _fetch(age: str, cache_key: str) -> Dict[str, List[dict]]:
    now = time.time()
    with _lock:
        c = _cache[cache_key]
        if c['data'] is not None and (now - c['fetched_at']) < CACHE_TTL:
            return c['data']
        if c.get('last_fail') and (now - c['last_fail']) < RETRY_BACKOFF:
            return c['data'] or {}

    geos = EU27 + ['UK']  # include UK for completeness even though we use ONS for GB
    url = _build_url(age, geos)
    try:
        resp = requests.get(url, timeout=45, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)',
            'Accept': 'application/json',
        })
        if resp.status_code != 200:
            logger.warning(f"Eurostat une_rt_m {resp.status_code} (age={age})")
            with _lock:
                _cache[cache_key]['last_fail'] = now
            return _cache[cache_key]['data'] or {}
        parsed = _parse_jsonstat(resp.json())
        with _lock:
            _cache[cache_key] = {'data': parsed, 'fetched_at': now, 'last_fail': 0}
        return parsed
    except Exception as e:
        logger.error(f"Eurostat une_rt_m fetch failed (age={age}): {e}")
        with _lock:
            _cache[cache_key]['last_fail'] = now
        return _cache[cache_key]['data'] or {}


def get_youth_unemployment(iso2: str) -> Optional[dict]:
    """
    Normalized youth-unemp dict for a single EU member, or None.
    """
    code = iso2.upper()
    youth_all = _fetch('Y_LT25', 'youth')
    total_all = _fetch('TOTAL', 'total')

    series = youth_all.get(code)
    if not series:
        return None

    history = [p['value'] for p in series]
    level = history[-1]
    delta_12m = level - history[-13] if len(history) > 12 else None

    total = None
    total_series = total_all.get(code)
    if total_series:
        total = total_series[-1]['value']

    return {
        'history': history,
        'level': level,
        'delta_12m': delta_12m,
        'total_unemp': total,
        'asof': series[-1]['date'],
        'source': 'eurostat',
    }


def available_countries() -> List[str]:
    """ISO2 codes for which Eurostat currently has youth data (post-fetch)."""
    with _lock:
        data = _cache['youth']['data'] or {}
    return sorted(data.keys())


def clear_cache():
    with _lock:
        for k in _cache:
            _cache[k] = {'data': None, 'fetched_at': 0, 'last_fail': 0}
