"""
NOAA CPC Oceanic Niño Index (ONI) adapter.

Pulls the monthly ONI series — a 3-month rolling SST anomaly in the
Niño 3.4 region (5°N-5°S, 120°-170°W) — from the NOAA Climate
Prediction Center. ONI is the canonical ENSO indicator: values above
+0.5°C for five consecutive overlapping seasons mark an El Niño event,
below -0.5°C mark La Niña.

ENSO is the single largest macro climate driver for agriculture. El
Niño years tend to bring dry conditions to West Africa (cocoa hit),
Australia (wheat hit), Brazil and Indonesia/Vietnam (coffee, palm).
La Niña years bring drought to Brazil and the southern US (soybeans,
wheat hit) and excess rain to South-East Asia. Per Anyamba et al. and
multiple commodity-trading studies, including ENSO state as an
exogenous driver produces 5-8% RMSE reductions in 1-3 month forecasts
for grains and softs.

Source: NOAA CPC public ASCII file
  - Primary:  https://origin.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt
  - Fallback: https://psl.noaa.gov/data/correlation/oni.data

Format
------
Plain text with a header row and one line per overlapping 3-month
season (DJF, JFM, FMA, MAM, ...), updated by the 5th of each month::

    SEAS YR  TOTAL  ANOM
    DJF 1950 24.72 -1.53
    JFM 1950 25.17 -1.34
    ...

We map each season's `ANOM` to the *center month* of the triplet
(DJF → Jan, JFM → Feb, ..., NDJ → Dec) so the result is a monthly
series. Cache TTL: 7 days (ONI only updates once a month, but we
re-pull weekly to catch revisions).

Fail-soft: any network / parse failure returns None or an empty series
so callers can fall back cleanly.
"""

from __future__ import annotations

import io
import time
import logging
import threading
from datetime import date, datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 7 * 24 * 3600     # 7 days
HTTP_TIMEOUT = 15

ONI_URLS = [
    'https://origin.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt',
    'https://psl.noaa.gov/data/correlation/oni.data',
]

# Seasonal triplet → center month of the trio
_SEASON_TO_MONTH = {
    'DJF': 1, 'JFM': 2, 'FMA': 3, 'MAM': 4, 'AMJ': 5, 'MJJ': 6,
    'JJA': 7, 'JAS': 8, 'ASO': 9, 'SON': 10, 'OND': 11, 'NDJ': 12,
}


# ── Cache ────────────────────────────────────────────────────────────────

class _ONICache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._series = None        # pandas Series
        self._fetched_at = 0.0

    def get(self):
        with self._lock:
            if self._series is not None and (time.time() - self._fetched_at) < CACHE_TTL:
                return self._series
        fresh = _fetch_ascii()
        with self._lock:
            if fresh is not None:
                self._series = fresh
                self._fetched_at = time.time()
            return self._series

    def clear(self) -> None:
        with self._lock:
            self._series = None
            self._fetched_at = 0.0


_cache = _ONICache()


# ── Fetch + parse ────────────────────────────────────────────────────────

def _fetch_ascii():
    """Return a pandas Series of monthly ONI anomalies, or None on failure.

    Tries each URL in ``ONI_URLS`` in order. Returns the first that
    parses cleanly with at least 24 monthly observations.
    """
    try:
        import pandas as pd
    except ImportError:
        return None

    for url in ONI_URLS:
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT)
        except Exception as e:
            logger.debug(f'ONI fetch {url!r} network error: {e}')
            continue
        if resp.status_code != 200 or not resp.text:
            logger.debug(f'ONI fetch {url!r}: HTTP {resp.status_code}')
            continue
        series = _parse_cpc_ascii(resp.text)
        if series is not None and len(series) >= 24:
            return series
        logger.debug(f'ONI parse {url!r}: rejected (insufficient rows)')
    logger.warning('ONI: no usable data from any source URL')
    return None


def _parse_cpc_ascii(text: str):
    """Parse the NOAA CPC ascii triplet format into a monthly Series."""
    try:
        import pandas as pd
    except ImportError:
        return None

    rows: list[tuple[int, int, float]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        season = parts[0].upper()
        if season not in _SEASON_TO_MONTH:
            continue
        try:
            year = int(parts[1])
            anom = float(parts[-1])     # ANOM is always last column
        except ValueError:
            continue
        month = _SEASON_TO_MONTH[season]
        # DJF in year Y straddles Dec(Y-1)/Jan(Y)/Feb(Y) — center is Jan(Y), so use year as-is.
        # NDJ straddles Nov(Y)/Dec(Y)/Jan(Y+1) — center is Dec(Y), so year stays Y.
        rows.append((year, month, anom))

    if not rows:
        return None

    # Build a Series indexed by month-end timestamps
    idx = pd.to_datetime([f'{y}-{m:02d}-01' for y, m, _ in rows]) + pd.offsets.MonthEnd(0)
    vals = [a for _, _, a in rows]
    series = pd.Series(vals, index=idx).sort_index()
    # Drop dupes if any (NOAA file occasionally has overlap during revisions)
    series = series[~series.index.duplicated(keep='last')]
    return series


# ── Public API ────────────────────────────────────────────────────────────

def fetch_oni(start: Optional[date] = None, end: Optional[date] = None):
    """Return a monthly ENSO ONI series sliced to the requested window.

    Returns a pandas Series indexed at month-end (consistent with the
    rest of the commodity-model driver pipeline), or ``None`` if no
    data could be retrieved.
    """
    series = _cache.get()
    if series is None:
        return None
    try:
        import pandas as pd  # noqa: F401
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


def latest_state() -> Optional[dict]:
    """Summary of the most recent ONI reading — useful for observability
    and tooltip text in the frontend.
    """
    series = _cache.get()
    if series is None or len(series) == 0:
        return None
    last_idx = series.index[-1]
    last_val = float(series.iloc[-1])
    if last_val >= 0.5:
        state = 'El Niño'
    elif last_val <= -0.5:
        state = 'La Niña'
    else:
        state = 'Neutral'
    return {
        'value': last_val,
        'state': state,
        'as_of': last_idx.date().isoformat(),
    }


# ── Smoke test ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')
    s = fetch_oni()
    if s is None:
        print('no data')
    else:
        print(f'ONI series: {len(s)} obs, latest = {s.iloc[-1]:+.2f} @ {s.index[-1].date()}')
        print('state:', latest_state())
