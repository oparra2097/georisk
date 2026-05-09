"""
VIX (CBOE Volatility Index) annual mean — global financial-stress
regressor for the credit-default model.

Hilscher & Nosbusch (2010) and Bussière & Fratzscher (2006) both find
global risk-aversion (VIX or equivalent) materially improves out-of-
sample sovereign-spread fits beyond country fundamentals alone.

Source: yfinance ticker ``^VIX``. Free, no API key. Cached on disk for
24h since the value is annual-aggregated.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Dict

from config import Config


_CACHE_TTL = 24 * 3600
_DISK_CACHE_PATH = os.path.join(Config.DATA_DIR, 'vix_annual.json')

_cache: Dict[str, Dict] = {}
_cache_lock = threading.Lock()


def _load_disk():
    if not os.path.exists(_DISK_CACHE_PATH):
        return None, 0
    try:
        with open(_DISK_CACHE_PATH) as f:
            j = json.load(f)
        return j.get('data'), float(j.get('ts', 0))
    except (OSError, json.JSONDecodeError):
        return None, 0


def _save_disk(data, ts):
    try:
        os.makedirs(os.path.dirname(_DISK_CACHE_PATH), exist_ok=True)
        with open(_DISK_CACHE_PATH, 'w') as f:
            json.dump({'data': data, 'ts': ts}, f)
    except OSError as e:
        print(f'[VIX] disk write failed: {e}')


def get_vix_annual_mean() -> Dict[int, float]:
    """Return ``{year: annual_mean_VIX_close}`` from 1990 to present.
    Same value applies to every country at scoring time — VIX is the
    "global stress" regressor."""
    with _cache_lock:
        entry = _cache.get('vix_annual')
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    disk_data, disk_ts = _load_disk()
    if disk_data and (time.time() - disk_ts) < _CACHE_TTL:
        # Coerce string keys (JSON round-trip) back to int.
        clean = {int(y): float(v) for y, v in disk_data.items()}
        with _cache_lock:
            _cache['vix_annual'] = {'data': clean, 'ts': disk_ts}
        return clean

    try:
        import yfinance as yf
    except ImportError:
        print('[VIX] yfinance not installed; skipping')
        return disk_data or {}

    try:
        df = yf.Ticker('^VIX').history(period='max', auto_adjust=False)
    except Exception as e:  # noqa: BLE001
        print(f'[VIX] yfinance fetch failed: {e}')
        return disk_data or {}

    if df is None or df.empty or 'Close' not in df.columns:
        return disk_data or {}

    annual: Dict[int, float] = {}
    counts: Dict[int, int] = {}
    for ts, val in df['Close'].dropna().items():
        try:
            yr = ts.year
            annual[yr] = annual.get(yr, 0.0) + float(val)
            counts[yr] = counts.get(yr, 0) + 1
        except Exception:  # noqa: BLE001
            continue

    out = {yr: annual[yr] / counts[yr] for yr in annual if counts[yr] >= 50}
    if out:
        now = time.time()
        with _cache_lock:
            _cache['vix_annual'] = {'data': out, 'ts': now}
        _save_disk({str(k): v for k, v in out.items()}, now)
    return out
