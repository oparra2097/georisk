"""
Quarterly panel builder for the macro model.

Pulls each variable from FRED via the existing backend.data_sources.fred_client,
aligns to a common quarterly index (1980Q1+), and applies the transformation
declared in the variable registry. Thread-safe memo cache with a 24h TTL so
re-estimation runs don't hammer FRED.

Quarterly alignment rules:
    M (monthly)  → average of 3 months in quarter (levels) or quarterly avg
                   of the monthly value (rates). NaN if any month missing.
    D (daily)    → simple average of daily observations within the quarter.
    Q (quarterly)→ passed through; FRED already reports on quarter-end dates.

Transformation is applied last so that downstream estimation sees the
series in its modeling form (log-level for quantities, %-level for rates).
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from backend.data_sources import fred_client
from backend.macro_model.variables import VARIABLES, Variable

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24h
DEFAULT_START = '1980-01-01'

_lock = threading.RLock()
_panel_cache: dict[str, tuple[pd.DataFrame, float]] = {}


# ── Helpers ──────────────────────────────────────────────────────────────

def _to_quarterly(raw: list[dict], freq: str) -> pd.Series:
    """
    Convert FRED observations to a quarterly pandas Series keyed by the
    period's end-of-quarter timestamp.
    """
    if not raw:
        return pd.Series(dtype=float)

    df = pd.DataFrame(raw)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')['value'].astype(float)

    if freq == 'Q':
        # FRED quarterly series are already at quarter-start dates; snap to quarter-end.
        out = df.groupby(df.index.to_period('Q')).last().rename_axis('quarter').to_timestamp(how='end')
        out.index = out.index.normalize()
        return out

    # For M and D: average within the quarter.
    grouped = df.groupby(df.index.to_period('Q')).mean()
    out = grouped.rename_axis('quarter').to_timestamp(how='end')
    out.index = out.index.normalize()
    return out


def _apply_transform(series: pd.Series, transform: str) -> pd.Series:
    if transform == 'log':
        out = series.where(series > 0).apply(np.log)
    elif transform == 'level' or transform == 'pct':
        out = series.astype(float)
    else:
        raise ValueError(f'unknown transform: {transform}')
    return out


def _fetch_variable(v: Variable, start: str) -> pd.Series:
    raw = fred_client.fetch_series(v.fred_id, start_date=start)
    if not raw:
        logger.warning(f"macro_model.data: empty FRED response for {v.code}={v.fred_id}")
        return pd.Series(dtype=float, name=v.code)
    q = _to_quarterly(raw, v.freq)
    q = _apply_transform(q, v.transform).rename(v.code)
    return q


# ── Public API ───────────────────────────────────────────────────────────

def build_panel(start: str = DEFAULT_START, codes: Optional[list[str]] = None,
                force_refresh: bool = False) -> pd.DataFrame:
    """
    Build a quarterly panel indexed by quarter-end dates.

    Columns are variable `code`s (not FRED IDs). Each column is already in
    the model form: logs for quantities, percent-levels for rates, etc.

    The full-panel build is cached under the start date; per-variable
    subsets share the same cache entry.
    """
    cache_key = start
    now = time.time()

    with _lock:
        entry = _panel_cache.get(cache_key)
        if entry and not force_refresh and (now - entry[1]) < CACHE_TTL:
            full = entry[0]
            return full if codes is None else full[codes].copy()

    series_list: list[pd.Series] = []
    wanted = [v for v in VARIABLES if codes is None or v.code in codes]
    for v in wanted:
        try:
            series_list.append(_fetch_variable(v, start))
        except Exception as e:
            logger.error(f"macro_model.data: failed to fetch {v.code}: {e}")
            series_list.append(pd.Series(dtype=float, name=v.code))

    panel = pd.concat(series_list, axis=1).sort_index()
    # Defensive: if every series came back empty, `concat` yields a RangeIndex
    # DataFrame. Coerce to an empty DatetimeIndex frame so callers never see a
    # non-datetime index from this module.
    if not isinstance(panel.index, pd.DatetimeIndex) or panel.empty:
        panel = pd.DataFrame(
            {v.code: pd.Series(dtype=float) for v in wanted},
            index=pd.DatetimeIndex([], name='quarter'),
        )
    else:
        panel = panel[panel.index >= pd.Timestamp(start)]
        panel = panel.dropna(how='all')

    with _lock:
        if codes is None:
            _panel_cache[cache_key] = (panel, now)

    logger.info(
        f"macro_model.data: built panel "
        f"{panel.shape[0]} quarters × {panel.shape[1]} vars "
        f"({panel.index.min().date()} → {panel.index.max().date()})"
    )
    return panel


def coverage_report(panel: pd.DataFrame) -> pd.DataFrame:
    """Per-variable diagnostic: first/last observation, coverage pct, N."""
    rows = []
    for col in panel.columns:
        s = panel[col].dropna()
        if s.empty:
            rows.append({'code': col, 'n': 0, 'first': None, 'last': None, 'coverage_pct': 0.0})
            continue
        total = len(panel)
        rows.append({
            'code': col,
            'n': int(len(s)),
            'first': s.index.min().date().isoformat(),
            'last': s.index.max().date().isoformat(),
            'coverage_pct': round(100 * len(s) / total, 1),
        })
    return pd.DataFrame(rows).set_index('code')


def clear_cache():
    with _lock:
        _panel_cache.clear()
