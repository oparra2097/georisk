"""
USDA NASS Quick Stats adapter for ending-stocks data.

The single most-watched fundamental for grain / oilseed price moves is
the level of physical ending stocks reported by USDA. For wheat, that's
the four-times-yearly **Grain Stocks** report (Mar / Jun / Sep / Dec).
For soybeans, the same. Per Westhoff et al. (2007) and FAO commodity-
market handbooks, stocks-to-use ratios at decade lows precede price
spikes 6-12 months out with empirical correlations near -0.55.

This module pulls those series via the USDA NASS Quick Stats API,
forward-fills the quarterly prints to a monthly index, and exposes
the result through a fail-soft fetcher that ``commodity_models.py``
calls under the new ``'wasde'`` driver kind.

Configuration
-------------
Requires the operator to register a free Quick Stats API key at
https://quickstats.nass.usda.gov/api and set it as
``USDA_NASS_API_KEY`` in the environment. Without a key, every fetch
returns ``None`` and the SARIMAX exog matrix simply doesn't pick up
the WASDE column — the existing driver set keeps working unchanged.

Cache TTL: 12 hours. WASDE projections release monthly (~10th) and the
quarterly Grain Stocks report drops 4× / year — re-fetching twice a
day catches revisions without hammering the API.

Scope
-----
Wheat (``commodity_desc=WHEAT``) and Soybeans (``commodity_desc=SOYBEANS``).
Corn could be added trivially by extending ``WASDE_QUERIES``; left as
future work because Corn isn't currently in our commodity list.
"""

from __future__ import annotations

import os
import time
import logging
import threading
from datetime import date
from typing import Optional

import requests

logger = logging.getLogger(__name__)

NASS_API_BASE = 'https://quickstats.nass.usda.gov/api/api_GET/'
CACHE_TTL = 12 * 3600
HTTP_TIMEOUT = 20

# Per-commodity Quick Stats query parameters. We pull national-aggregate
# ending stocks (point-in-time series) which the quarterly Grain Stocks
# report drives. The unit `BU` keeps wheat and soybeans on the same
# bushels scale; soybean stocks are also reported in pounds for some
# series, but the bushel series is the standard analyst reference.
WASDE_QUERIES: dict[str, dict] = {
    'Wheat': {
        'commodity_desc':    'WHEAT',
        'statisticcat_desc': 'STOCKS',
        'unit_desc':         'BU',
        'agg_level_desc':    'NATIONAL',
        'freq_desc':         'POINT IN TIME',
    },
    'Soybeans': {
        'commodity_desc':    'SOYBEANS',
        'statisticcat_desc': 'STOCKS',
        'unit_desc':         'BU',
        'agg_level_desc':    'NATIONAL',
        'freq_desc':         'POINT IN TIME',
    },
}


# ── API key + cache ─────────────────────────────────────────────────────

def _api_key() -> Optional[str]:
    return os.environ.get('USDA_NASS_API_KEY') or None


class _StocksCache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, tuple[float, object]] = {}    # commodity → (ts, series)

    def get(self, commodity: str):
        with self._lock:
            entry = self._data.get(commodity)
            if entry and (time.time() - entry[0]) < CACHE_TTL:
                return entry[1]
        return None

    def put(self, commodity: str, series) -> None:
        with self._lock:
            self._data[commodity] = (time.time(), series)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_cache = _StocksCache()


# ── Fetcher + parser ────────────────────────────────────────────────────

def fetch_stocks(commodity: str,
                 start: Optional[date] = None,
                 end: Optional[date] = None):
    """Return a monthly pandas Series of USDA ending-stocks for the
    requested commodity, forward-filled across the quarterly print
    cadence. Returns ``None`` if Quick Stats data can't be retrieved.

    Args
    ----
    commodity : str
        One of the keys in ``WASDE_QUERIES`` ('Wheat', 'Soybeans').
    start, end : date, optional
        Inclusive slicing of the output Series.
    """
    if commodity not in WASDE_QUERIES:
        return None

    cached = _cache.get(commodity)
    if cached is not None:
        return _slice(cached, start, end)

    key = _api_key()
    if not key:
        logger.info(f'USDA NASS key not configured — {commodity} WASDE driver inactive')
        return None

    series = _fetch_live(commodity, key)
    if series is None or len(series) == 0:
        return None

    _cache.put(commodity, series)
    return _slice(series, start, end)


def _fetch_live(commodity: str, key: str):
    """Single live HTTP call to NASS Quick Stats + parse into a monthly Series."""
    try:
        import pandas as pd
    except ImportError:
        return None

    params = dict(WASDE_QUERIES[commodity])
    params['key']     = key
    params['format']  = 'JSON'
    params['year__GE'] = 2005   # 20y window — plenty of history for SARIMAX

    try:
        resp = requests.get(NASS_API_BASE, params=params, timeout=HTTP_TIMEOUT)
    except Exception as exc:
        logger.warning(f'USDA NASS fetch {commodity}: {exc}')
        return None
    if resp.status_code != 200:
        logger.warning(f'USDA NASS fetch {commodity}: HTTP {resp.status_code}')
        return None

    try:
        payload = resp.json()
    except ValueError:
        logger.warning(f'USDA NASS {commodity}: non-JSON response')
        return None

    rows = payload.get('data') or []
    if not rows:
        return None

    parsed: list[tuple[pd.Timestamp, float]] = []
    for r in rows:
        # `Value` field is a string with commas, possibly '(D)' / '(NA)' suppressed
        raw_val = (r.get('Value') or '').replace(',', '').strip()
        if not raw_val or raw_val.startswith('('):
            continue
        try:
            value = float(raw_val)
        except ValueError:
            continue

        # Date reconstruction: NASS gives `year` + `reference_period_desc`
        # The Grain Stocks reports use period labels MAR / JUN / SEP / DEC
        year = r.get('year')
        period = (r.get('reference_period_desc') or '').strip().upper()
        if not year:
            continue
        try:
            year = int(year)
        except (ValueError, TypeError):
            continue
        month = _PERIOD_TO_MONTH.get(period)
        if month is None:
            continue
        # Stocks reports are released in the *month following* the as-of
        # date (e.g. Mar 1 stocks released late March). Index by the
        # as-of month for consistency with other monthly drivers.
        try:
            ts = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
        except ValueError:
            continue
        parsed.append((ts, value))

    if not parsed:
        return None

    series = pd.Series({ts: v for ts, v in parsed}).sort_index()
    series = series[~series.index.duplicated(keep='last')]

    # Forward-fill the quarterly prints onto a full monthly index so the
    # SARIMAX exog matrix has a value every month (driver-pipeline
    # convention; the underlying signal updates 4× / year but the model
    # consumes monthly).
    monthly_idx = pd.date_range(series.index.min(), series.index.max(), freq='ME')
    return series.reindex(monthly_idx).ffill()


# NASS uses short 3-letter period codes for the Grain Stocks reports
_PERIOD_TO_MONTH = {
    'MAR': 3, 'JUN': 6, 'SEP': 9, 'DEC': 12,
    # Some queries return generic "YEAR" — treat as Dec of that year
    'YEAR': 12,
    # ANN / ANNUAL aliases
    'ANN': 12, 'ANNUAL': 12,
}


def _slice(series, start: Optional[date], end: Optional[date]):
    try:
        import pandas as pd
    except ImportError:
        return None
    out = series
    if start is not None:
        out = out.loc[out.index >= pd.Timestamp(start)]
    if end is not None:
        out = out.loc[out.index <= pd.Timestamp(end)]
    return out if len(out) > 0 else None


def clear_cache() -> None:
    _cache.clear()


# ── Observability ───────────────────────────────────────────────────────

def latest_stocks(commodity: str) -> Optional[dict]:
    """Return the most-recent stocks reading for tooltip / dashboard text."""
    series = fetch_stocks(commodity)
    if series is None or len(series) == 0:
        return None
    return {
        'commodity': commodity,
        'value_bu':  float(series.iloc[-1]),
        'as_of':     series.index[-1].date().isoformat(),
    }


# ── Smoke test ──────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')
    for commodity in WASDE_QUERIES:
        s = fetch_stocks(commodity)
        if s is None:
            print(f'{commodity}: no data (USDA_NASS_API_KEY not set?)')
        else:
            print(f'{commodity}: {len(s)} obs, latest = {s.iloc[-1]:,.0f} bu @ {s.index[-1].date()}')
            print('  latest_stocks:', latest_stocks(commodity))
