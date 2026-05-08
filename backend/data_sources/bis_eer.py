"""
BIS Real Effective Exchange Rate (REER), monthly broad index.

Source: BIS Effective Exchange Rates dataset, mirrored via DBnomics
(``BIS/WS_EER``). Series-code pattern is ``{FREQ}.{TYPE}.{BASIS}.{ISO2}``:

  FREQ:   D / M / Q / A    (we use M = monthly)
  TYPE:   N / R            (we use R = real)
  BASIS:  B / N            (we use B = broad, 64 economies)

We aggregate monthly to annual mean and compute an "REER overvaluation"
feature relative to the trailing 10-year mean (Hilscher 2010, IMF MAC
SRDSF 2022). Higher REER vs trend = currency over-valued = sovereign-
risk loading.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict

from config import Config


_DBNOMICS_BASE = 'https://api.db.nomics.world/v22'
_DATASET = 'BIS/WS_EER'
_CACHE_TTL = 24 * 3600  # daily — REER updates monthly but extra freshness is cheap
_DISK_CACHE_DIR = os.path.join(Config.DATA_DIR, 'reer_cache')

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
        print(f'[REER] disk write failed for {key}: {e}')


def _iso3_to_iso2_map() -> Dict[str, str]:
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
        print(f'[REER] iso3→iso2 map load failed: {e}')
    out.setdefault('XKX', 'XK')
    _iso3_to_iso2_map._cache = out
    return out


def get_reer_annual_mean() -> Dict[str, Dict[int, float]]:
    """Return ``{iso3: {year: annual_mean_real_broad_REER}}``.

    Coverage: 64 economies in BIS's broad basket. Countries outside
    the basket (most LICs) get no entry."""
    cache_key = 'reer_real_broad_annual_v1'

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

    # DBnomics names the dimensions FREQ / EER_TYPE / EER_BASKET — not
    # TYPE / BASIS as the SDMX docs imply. Pin FREQ=M, EER_TYPE=R
    # (real), EER_BASKET=B (broad 64-economy basket).
    url = (
        f'{_DBNOMICS_BASE}/series/{_DATASET}'
        '?dimensions=%7B%22FREQ%22%3A%5B%22M%22%5D%2C%22EER_TYPE%22%3A%5B%22R%22%5D%2C%22EER_BASKET%22%3A%5B%22B%22%5D%7D'
        '&observations=1&limit=200'
    )
    out: Dict[str, Dict[int, float]] = {}
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'parra-macro/1.0'})
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        print(f'[REER] fetch failed: {e}')
        if disk_data:
            return disk_data
        return {}

    docs = (payload.get('series') or {}).get('docs') or []
    for s in docs:
        code = s.get('series_code', '')
        parts = code.split('.')  # M.R.B.{ISO2}
        if len(parts) < 4:
            continue
        iso2 = parts[3].upper()
        iso3 = iso2_to_iso3.get(iso2)
        if not iso3:
            continue
        # Aggregate monthly observations into annual means.
        sums: Dict[int, float] = {}
        counts: Dict[int, int] = {}
        for p, v in zip(s.get('period') or [], s.get('value') or []):
            if v is None:
                continue
            try:
                yr = int(str(p)[:4])
                sums[yr] = sums.get(yr, 0.0) + float(v)
                counts[yr] = counts.get(yr, 0) + 1
            except (ValueError, TypeError):
                continue
        annual = {yr: sums[yr] / counts[yr] for yr in sums if counts[yr] >= 6}
        if annual:
            out[iso3] = annual

    if out:
        now = time.time()
        with _cache_lock:
            _cache[cache_key] = {'data': out, 'ts': now}
        _save_disk(cache_key, out, now)
    return out


def get_reer_overvaluation() -> Dict[str, Dict[int, float]]:
    """Return ``{iso3: {year: overvaluation_pct}}`` — annual REER as
    a percent deviation from its trailing 10-year mean. Positive =
    over-valued (sovereign-risk loading per IMF SRDSF).
    """
    levels = get_reer_annual_mean()
    out: Dict[str, Dict[int, float]] = {}
    for iso3, series in levels.items():
        clean = {int(y): float(v) for y, v in series.items() if v is not None}
        years = sorted(clean.keys())
        for yr in years:
            window = [clean[y] for y in range(yr - 9, yr + 1) if y in clean]
            if len(window) >= 5:
                avg = sum(window) / len(window)
                if avg > 0:
                    out.setdefault(iso3, {})[yr] = (clean[yr] / avg - 1.0) * 100.0
    return out
