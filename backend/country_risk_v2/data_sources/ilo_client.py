"""
ILOSTAT SDMX client — annual youth (15-24) and total (15+) unemployment rates.

Default source for LatAm countries (MX, CO, BR, CL, AR) in Phase 4.
Also available as a fallback for any EU country missing from Eurostat.

Endpoint: SDMX 2.1 REST at https://sdmx.ilo.org/rest/data/
Dataflow: UNE_2EAP_SEX_AGE_RT — unemployment rate by sex and age (annual).

Key structure (dot-separated):
    {FREQ}.{REF_AREA}.{SEX}.{AGE}.{CLASSIF1?}

Common codes:
    FREQ      = A  (annual)
    REF_AREA  = MEX, COL, BRA, CHL, ARG, ...  (ISO-3)
    SEX       = SEX_T  (total, both sexes)
    AGE       = AGE_YTHADULT_Y15-24  (youth) or AGE_YTHADULT_YGE15 (total 15+)

ILOSTAT typical lag:
    - LatAm annual: 6-12 months behind calendar year
    - Quarterly series exist for BR/MX/CL but the codes differ; wire those
      in a later refinement once we need higher frequency than annual.

Thread-safe cache: 24h TTL, 1h retry backoff. No auth required.
"""

import logging
import threading
import time
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 86400
RETRY_BACKOFF = 3600

SDMX_BASE = 'https://sdmx.ilo.org/rest/data'
DATAFLOW = 'ILO,DF_UNE_2EAP_SEX_AGE_RT,1.0'

# ISO-2 → ISO-3 for ILOSTAT REF_AREA
ISO2_TO_ISO3 = {
    'US': 'USA', 'GB': 'GBR',
    'DE': 'DEU', 'FR': 'FRA', 'IT': 'ITA', 'ES': 'ESP',
    'MX': 'MEX', 'CO': 'COL', 'BR': 'BRA', 'CL': 'CHL', 'AR': 'ARG',
}

AGE_YOUTH = 'AGE_YTHADULT_Y15-24'
AGE_TOTAL = 'AGE_YTHADULT_YGE15'
SEX_TOTAL = 'SEX_T'

_lock = threading.RLock()
_cache: Dict[str, dict] = {}   # {cache_key: {'points': [...], 'fetched_at': float, 'last_fail': float}}


def _build_url(iso3: str, age: str) -> str:
    # Dot-separated SDMX key; blanks are allowed but we pin FREQ/REF/SEX/AGE.
    key = f'A.{iso3}.{SEX_TOTAL}.{age}'
    return f'{SDMX_BASE}/{DATAFLOW}/{key}?format=jsondata&startPeriod=2010'


def _parse_sdmx_json(body: dict) -> List[dict]:
    """
    Minimal SDMX-JSON v2 parser. We pinned every dim except TIME_PERIOD,
    so the 'observations' block is flat (one series, many periods).
    Returns [{'year': int, 'value': float}, ...] sorted ascending.
    """
    try:
        data_sets = body.get('data', {}).get('dataSets') or body.get('dataSets')
        if not data_sets:
            return []
        ds = data_sets[0]
        series = ds.get('series') or {}

        # Find the time dimension position under structure/dimensions/observation
        struct = body.get('data', {}).get('structure') or body.get('structure') or {}
        time_dim = (struct.get('dimensions', {}).get('observation') or [])
        time_values = []
        for d in time_dim:
            if d.get('id') in ('TIME_PERIOD', 'TIME'):
                time_values = [v.get('id') or v.get('name') for v in d.get('values', [])]
                break

        points = []
        # Expected shape: series = {'0:0:0:0': {'observations': {'0': [12.3], '1': [13.4], ...}}}
        for _series_key, series_body in series.items():
            obs = series_body.get('observations', {})
            for t_idx_str, obs_value in obs.items():
                try:
                    t_idx = int(t_idx_str)
                    v = obs_value[0]
                    if v is None:
                        continue
                    if t_idx >= len(time_values):
                        continue
                    label = time_values[t_idx]
                    year = int(str(label)[:4])
                    points.append({'year': year, 'value': float(v)})
                except (ValueError, TypeError, IndexError):
                    continue
        points.sort(key=lambda p: p['year'])
        return points
    except Exception as e:
        logger.warning(f"SDMX parse failed: {e}")
        return []


def _fetch_series(iso3: str, age: str) -> List[dict]:
    """Cached SDMX fetch for one (country, age) series."""
    cache_key = f'{iso3}:{age}'
    now = time.time()

    with _lock:
        c = _cache.get(cache_key)
        if c and (now - c['fetched_at']) < CACHE_TTL:
            return c['points']
        if c and c.get('last_fail') and (now - c['last_fail']) < RETRY_BACKOFF:
            return c.get('points', [])

    url = _build_url(iso3, age)
    try:
        resp = requests.get(url, timeout=45, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)',
            'Accept': 'application/vnd.sdmx.data+json; version=1.0.0, application/json',
        })
        if resp.status_code != 200:
            logger.warning(f"ILOSTAT {resp.status_code} for {iso3}/{age}")
            with _lock:
                _cache.setdefault(cache_key, {'points': [], 'fetched_at': 0})['last_fail'] = now
            return _cache[cache_key].get('points', [])

        points = _parse_sdmx_json(resp.json())
        with _lock:
            _cache[cache_key] = {'points': points, 'fetched_at': now, 'last_fail': 0}
        return points
    except Exception as e:
        logger.error(f"ILOSTAT fetch failed for {iso3}/{age}: {e}")
        with _lock:
            _cache.setdefault(cache_key, {'points': [], 'fetched_at': 0})['last_fail'] = now
        return _cache[cache_key].get('points', [])


def get_youth_unemployment(iso2: str) -> Optional[dict]:
    """
    Normalized dict matching the youth_unemployment.py contract.

    ILO data is annual, so step_for_yoy=1 and history is a list of yearly
    percentages (typically 10-15 points). Returns None if no data.
    """
    code = iso2.upper()
    iso3 = ISO2_TO_ISO3.get(code)
    if iso3 is None:
        return None

    youth = _fetch_series(iso3, AGE_YOUTH)
    if not youth:
        return None

    history = [p['value'] for p in youth]
    level = history[-1]
    delta_12m = level - history[-2] if len(history) >= 2 else None

    total_series = _fetch_series(iso3, AGE_TOTAL)
    total = total_series[-1]['value'] if total_series else None

    return {
        'history': history,
        'level': level,
        'delta_12m': delta_12m,
        'total_unemp': total,
        'asof': f'{youth[-1]["year"]}-12-31',
        'source': 'ilo',
        'step_for_yoy': 1,  # annual series
    }


def clear_cache():
    with _lock:
        _cache.clear()
