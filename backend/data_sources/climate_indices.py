"""
NOAA-published climate-index adapter for agri drivers.

Beyond ENSO (already wired in ``enso_index.py``), several large-scale
NOAA-published indices have documented links to specific commodity-
producing regions:

  * **AMO** — Atlantic Multidecadal Oscillation. Tracks N. Atlantic SST
    anomalies. Positive AMO phases bring above-normal West African
    monsoon rainfall (cocoa) and influence northeast Brazil.
  * **NAO** — North Atlantic Oscillation. Surface-pressure dipole
    between Iceland and the Azores. Positive NAO → wetter / warmer
    winters in N. Europe (helpful for EU wheat) and drier in S. Europe.
  * **PDO** — Pacific Decadal Oscillation. Multi-decade Pacific SST
    pattern. Weaker direct ag impact than ENSO but adds a slow-moving
    climate background factor.

Source: NOAA Physical Sciences Lab + Climate Prediction Center ASCII
files. Standard "year + 12 monthly values per row" format, parsed into
a monthly Series indexed at month-end timestamps.

Fail-soft: any network / parse failure returns ``None`` per index;
callers fall back to the unchanged driver set.
"""

from __future__ import annotations

import time
import logging
import threading
from datetime import date
from typing import Optional

import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 7 * 24 * 3600
HTTP_TIMEOUT = 15

# Per-index source URL tuple — first URL tried first, fallbacks follow.
INDEX_URLS: dict[str, tuple[str, ...]] = {
    'AMO': (
        # NOAA PSL Atlantic Multidecadal Oscillation — long unsmoothed
        'https://psl.noaa.gov/data/correlation/amon.us.long.data',
        'https://www.psl.noaa.gov/data/correlation/amon.us.long.data',
    ),
    'NAO': (
        # NOAA PSL NAO index — standard CPC version
        'https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/norm.nao.monthly.b5001.current.ascii.table',
        'https://psl.noaa.gov/data/correlation/nao.data',
    ),
    'PDO': (
        # NOAA PSL Pacific Decadal Oscillation
        'https://psl.noaa.gov/data/correlation/pdo.timeseries.ersstv5.csv',
        'https://www.ncei.noaa.gov/pub/data/cmb/ersst/v5/index/ersst.v5.pdo.dat',
    ),
}


# ── Cache ────────────────────────────────────────────────────────────────

class _ClimateCache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, tuple[float, object]] = {}

    def get(self, name: str):
        with self._lock:
            entry = self._data.get(name)
            if entry and (time.time() - entry[0]) < CACHE_TTL:
                return entry[1]
        return None

    def put(self, name: str, series) -> None:
        with self._lock:
            self._data[name] = (time.time(), series)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_cache = _ClimateCache()


# ── Fetcher + parser ────────────────────────────────────────────────────

def fetch_index(name: str,
                start: Optional[date] = None,
                end: Optional[date] = None):
    """Return a monthly pandas Series for a NOAA climate index, sliced.

    Args
    ----
    name : str
        One of ``'AMO'``, ``'NAO'``, ``'PDO'``.
    start, end : date, optional
        Inclusive slicing of the output Series.

    Returns ``None`` if the index isn't known or all source URLs fail.
    """
    if name not in INDEX_URLS:
        return None

    cached = _cache.get(name)
    if cached is not None:
        return _slice(cached, start, end)

    series = _fetch_live(name)
    if series is None or len(series) == 0:
        return None

    _cache.put(name, series)
    return _slice(series, start, end)


def _fetch_live(name: str):
    try:
        import pandas as pd
    except ImportError:
        return None

    for url in INDEX_URLS[name]:
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT)
        except Exception as exc:
            logger.debug(f'{name} fetch {url!r}: {exc}')
            continue
        if resp.status_code != 200 or not resp.text:
            logger.debug(f'{name} fetch {url!r}: HTTP {resp.status_code}')
            continue
        # Try CSV first (PSL ersstv5 PDO is CSV), then year-row ASCII fallback
        series = _try_parse(resp.text)
        if series is not None and len(series) >= 24:
            logger.info(f'{name}: parsed {len(series)} monthly obs from {url}')
            return series
    logger.warning(f'{name}: no usable data from any source URL')
    return None


def _try_parse(text: str):
    """Try several common NOAA ASCII formats; return a Series or None."""
    # 1. Year-row format ("YYYY v1 v2 ... v12") — typical PSL .data file
    series = _parse_year_row(text)
    if series is not None and len(series) >= 24:
        return series
    # 2. CSV with date column ("YYYY-MM-DD,value" or "Date,Value")
    series = _parse_csv(text)
    if series is not None and len(series) >= 24:
        return series
    # 3. CPC table format ("YYYY 1 0.01 2 0.05 ..." or similar)
    series = _parse_cpc_table(text)
    if series is not None and len(series) >= 24:
        return series
    return None


def _parse_year_row(text: str):
    """Parse the PSL '.data' format: header, then YYYY v1..v12 rows, then
    a footer line (often '-99.99' missing value or a description)."""
    try:
        import pandas as pd
    except ImportError:
        return None

    rows: list[tuple[int, list[float]]] = []
    missing_codes = {-99.99, -99.9, -999.0, 99.99, 999.9}

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        # Need at least year + 12 values
        if len(parts) < 13:
            continue
        try:
            year = int(parts[0])
            if year < 1850 or year > 2200:
                continue
            vals = [float(p) for p in parts[1:13]]
        except ValueError:
            continue
        # Replace missing-data sentinels with NaN
        vals = [None if abs(v) in missing_codes else v for v in vals]
        rows.append((year, vals))

    if not rows:
        return None

    records = []
    for year, vals in rows:
        for month, v in enumerate(vals, start=1):
            if v is None:
                continue
            ts = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
            records.append((ts, v))

    if not records:
        return None
    series = pd.Series({ts: v for ts, v in records}).sort_index()
    return series[~series.index.duplicated(keep='last')]


def _parse_csv(text: str):
    try:
        import pandas as pd
        import io
    except ImportError:
        return None
    try:
        # Try to detect a typical date-column CSV; pandas does the work.
        df = pd.read_csv(io.StringIO(text), header=0)
    except Exception:
        return None
    if df.empty or df.shape[1] < 2:
        return None
    # Column names commonly seen: Date / date / time / Year-Month
    date_col = None
    for c in df.columns:
        if c.lower() in ('date', 'time', 'year-month', 'yyyymm'):
            date_col = c
            break
    if date_col is None:
        date_col = df.columns[0]
    val_col = None
    for c in df.columns:
        if c == date_col:
            continue
        try:
            pd.to_numeric(df[c])
            val_col = c
            break
        except (ValueError, TypeError):
            continue
    if val_col is None:
        return None
    try:
        idx = pd.to_datetime(df[date_col], errors='coerce')
        vals = pd.to_numeric(df[val_col], errors='coerce')
    except Exception:
        return None
    series = pd.Series(vals.values, index=idx).dropna()
    if len(series) == 0:
        return None
    try:
        series.index = series.index.to_period('M').to_timestamp('M')
    except Exception:
        return None
    return series.sort_index()


def _parse_cpc_table(text: str):
    """CPC NAO table format: header rows, then 'YYYY  m1  m2  m3 ... m12'.

    Same shape as _parse_year_row but with optional header / footer
    lines mixed in. Reuses the row parser.
    """
    return _parse_year_row(text)


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

def latest_state(name: str) -> Optional[dict]:
    series = fetch_index(name)
    if series is None or len(series) == 0:
        return None
    return {
        'index': name,
        'value': float(series.iloc[-1]),
        'as_of': series.index[-1].date().isoformat(),
    }


# ── Smoke test ──────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')
    for idx in INDEX_URLS:
        s = fetch_index(idx)
        if s is None:
            print(f'{idx}: no data')
        else:
            print(f'{idx}: {len(s)} obs, latest = {s.iloc[-1]:+.3f} @ {s.index[-1].date()}')
