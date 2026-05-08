"""
IMF Commodity Terms of Trade (PCTOT) — annual fetcher.

Source: IMF, Commodity Terms of Trade database, mirrored via DBnomics
(`IMF/PCTOT`). 182 economies, 1962–present, free.

We pull the export-to-import commodity-price ratio with rolling weights
(``A.{ISO2}.xm.H_RW_IX``). Higher values mean a country's export
commodities are appreciating faster than its import commodities — a
positive terms-of-trade shock. Hilscher & Nosbusch (2010) document
that **terms-of-trade volatility** is the single most important
fundamental beyond debt and reserves for sovereign risk; we compute
the 5-year rolling std-dev in :mod:`backend.credit_default.data` once
this fetcher returns the level series.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, Optional

from config import Config


_DBNOMICS_BASE = 'https://api.db.nomics.world/v22'
_DATASET = 'IMF/PCTOT'
_CACHE_TTL = 7 * 24 * 3600  # weekly — annual data, no need to refresh more often
_DISK_CACHE_DIR = os.path.join(Config.DATA_DIR, 'pctot_cache')

_cache: Dict[str, Dict] = {}
_cache_lock = threading.Lock()


def _disk_path(key: str) -> str:
    safe = ''.join(c if c.isalnum() else '_' for c in key)
    os.makedirs(_DISK_CACHE_DIR, exist_ok=True)
    return os.path.join(_DISK_CACHE_DIR, f'{safe}.json')


def _load_disk(key: str):
    path = _disk_path(key)
    if not os.path.exists(path):
        return None, 0
    try:
        with open(path) as f:
            j = json.load(f)
        return j.get('data'), float(j.get('ts', 0))
    except (OSError, json.JSONDecodeError):
        return None, 0


def _save_disk(key: str, data, ts: float) -> None:
    try:
        with open(_disk_path(key), 'w') as f:
            json.dump({'data': data, 'ts': ts}, f)
    except OSError as e:
        print(f'[PCTOT] disk write failed for {key}: {e}')


def _iso3_to_iso2_map() -> Dict[str, str]:
    """Build ISO-3 → ISO-2 from the existing country_codes.json. Cached
    in module memory after first call."""
    if hasattr(_iso3_to_iso2_map, '_cache'):
        return _iso3_to_iso2_map._cache
    base_dir = Path(__file__).resolve().parent.parent.parent
    path = base_dir / 'static' / 'data' / 'country_codes.json'
    out: Dict[str, str] = {}
    try:
        with open(path) as f:
            for entry in json.load(f):
                a3 = (entry.get('alpha-3') or '').upper()
                a2 = (entry.get('alpha-2') or '').upper()
                if a3 and a2:
                    out[a3] = a2
    except (OSError, json.JSONDecodeError) as e:
        print(f'[PCTOT] iso3→iso2 map load failed: {e}')
    # Common sub-sovereign / non-ISO entries used in the credit-default
    # panel that aren't in the ISO-3166 list but DO appear in PCTOT.
    out.setdefault('XKX', 'XK')      # Kosovo
    out.setdefault('UVK', 'XK')
    out.setdefault('WBG', 'PS')      # West Bank and Gaza
    _iso3_to_iso2_map._cache = out
    return out


def get_pctot_xm_annual() -> Dict[str, Dict[int, float]]:
    """Return ``{iso3: {year: terms_of_trade_index}}`` for every country
    PCTOT covers. Index is base 100; 5y rolling std-dev of log changes
    captures the volatility feature Hilscher 2010 highlights."""
    cache_key = 'pctot_xm_annual_v1'

    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    disk_data, disk_ts = _load_disk(cache_key)
    if disk_data and (time.time() - disk_ts) < _CACHE_TTL:
        with _cache_lock:
            _cache[cache_key] = {'data': disk_data, 'ts': disk_ts}
        return disk_data

    iso3_to_iso2 = _iso3_to_iso2_map()
    iso2_to_iso3 = {v: k for k, v in iso3_to_iso2.items()}

    # Ask DBnomics for every annual XM-rolling-weights series in one go.
    # The dimension filter pins FREQ=A and INDICATOR=xm; we slice
    # series by ISO-2 client-side.
    url = (
        f'{_DBNOMICS_BASE}/series/{_DATASET}'
        '?dimensions=%7B%22FREQ%22%3A%5B%22A%22%5D%2C%22INDICATOR%22%3A%5B%22xm%22%5D%2C%22TYPE%22%3A%5B%22H_RW_IX%22%5D%7D'
        '&observations=1&limit=1000'
    )
    out: Dict[str, Dict[int, float]] = {}
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'parra-macro/1.0'})
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        print(f'[PCTOT] fetch failed: {e}')
        # Serve stale cache if we have one rather than empty.
        if disk_data:
            return disk_data
        return {}

    docs = (payload.get('series') or {}).get('docs') or []
    for s in docs:
        code = s.get('series_code', '')
        # Pattern: A.{ISO2}.xm.H_RW_IX
        parts = code.split('.')
        if len(parts) < 4:
            continue
        iso2 = parts[1].upper()
        iso3 = iso2_to_iso3.get(iso2)
        if not iso3:
            continue
        periods = s.get('period') or []
        values = s.get('value') or []
        series: Dict[int, float] = {}
        for p, v in zip(periods, values):
            if v is None:
                continue
            try:
                yr = int(str(p)[:4])
                series[yr] = float(v)
            except (ValueError, TypeError):
                continue
        if series:
            out[iso3] = series

    if out:
        now = time.time()
        with _cache_lock:
            _cache[cache_key] = {'data': out, 'ts': now}
        _save_disk(cache_key, out, now)
    return out


def get_pctot_volatility_5y() -> Dict[str, Dict[int, float]]:
    """Return ``{iso3: {year: 5y_rolling_stdev_of_log_returns}}``
    suitable for use as a panel feature. Years where the trailing
    5-year window contains <4 observations are skipped."""
    try:
        import math
        import statistics
    except ImportError:
        return {}
    levels = get_pctot_xm_annual()
    out: Dict[str, Dict[int, float]] = {}
    for iso3, series in levels.items():
        # JSON cache round-trip turns int keys into strings — coerce
        # back so arithmetic on years doesn't blow up.
        clean = {int(y): float(v) for y, v in series.items() if v is not None}
        years = sorted(clean.keys())
        log_returns: Dict[int, float] = {}
        for i in range(1, len(years)):
            y_prev, y_now = years[i - 1], years[i]
            if y_now - y_prev != 1:
                continue
            v_prev, v_now = clean[y_prev], clean[y_now]
            if v_prev > 0 and v_now > 0:
                log_returns[y_now] = math.log(v_now / v_prev)
        for yr in years:
            window = [log_returns[y] for y in range(yr - 4, yr + 1) if y in log_returns]
            if len(window) >= 4:
                out.setdefault(iso3, {})[yr] = float(statistics.stdev(window))
    return out
