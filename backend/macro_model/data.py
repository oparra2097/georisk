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
from backend.macro_model import diagnostics
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

    # For daily series, forward-fill across holidays/weekends BEFORE aggregating
    # so a single missing day doesn't NaN the whole quarter (root-cause #2 from
    # the Apr 24 audit: DGS10 / DTWEXBGS / DCOILWTICO would lose Q4 if any
    # observation was missing).
    if freq == 'D':
        # Reindex to a daily business calendar and forward-fill (≤ 7 days)
        full = pd.date_range(df.index.min(), df.index.max(), freq='B')
        df = df.reindex(full).ffill(limit=7)

    # For M and D: average within the quarter (post forward-fill for D).
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
    try:
        raw = fred_client.fetch_series(v.fred_id, start_date=start)
    except Exception as e:
        logger.error(f"macro_model.data: FRED fetch raised for {v.code}={v.fred_id}: {e}")
        diagnostics.record_fetch_fail(v.code, v.fred_id, str(e))
        return pd.Series(dtype=float, name=v.code)

    if not raw:
        msg = f"empty FRED response (check FRED_API_KEY or series id '{v.fred_id}' validity)"
        logger.warning(f"macro_model.data: {v.code}={v.fred_id}: {msg}")
        diagnostics.record_fetch_fail(v.code, v.fred_id, msg)
        return pd.Series(dtype=float, name=v.code)

    try:
        q = _to_quarterly(raw, v.freq)
        q = _apply_transform(q, v.transform).rename(v.code)
    except Exception as e:
        logger.exception(f"macro_model.data: transform failed for {v.code}")
        diagnostics.record_fetch_fail(v.code, v.fred_id, f'transform error: {e}')
        return pd.Series(dtype=float, name=v.code)

    # Success — record a quick health snapshot
    non_na = q.dropna()
    diagnostics.record_fetch_ok(
        v.code, v.fred_id,
        n_obs=int(len(non_na)),
        last_date=non_na.index.max().date().isoformat() if len(non_na) else None,
    )
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

        # Trim trailing rows where any ENDOGENOUS variable is NaN. We
        # previously also required exogenous variables (gov, oil, row_gdp,
        # nrou, prod, lfpr) to be non-NaN, but FRED's OECD series
        # (NAEXKP01OEQ661S = row_gdp) often lags 5+ years and that
        # regressed the trim back to ~2015, making the "20-quarter
        # forecast" table show 2016-2020 historical dates. The
        # solver/simulator already use `last_valid` (PR #55) when
        # carrying forward exogenous values into the forecast horizon,
        # so a NaN exog cell at the panel end is now safe.
        endog_codes = [v.code for v in wanted if v.endogenous and v.code in panel.columns]
        all_codes = [v.code for v in wanted if v.code in panel.columns]
        if endog_codes and len(panel) > 0:
            complete = panel[endog_codes].notna().all(axis=1)
            if complete.any():
                last_complete = complete[complete].index.max()
                trimmed = (panel.index > last_complete).sum()
                if trimmed:
                    last_row = panel.iloc[-1]
                    nan_endog = [c for c in endog_codes if pd.isna(last_row[c])]
                    nan_exog  = [c for c in all_codes if c not in endog_codes and pd.isna(last_row[c])]
                    logger.info(
                        f'macro_model.data: trimming {trimmed} trailing partial '
                        f'rows after last-complete-endog quarter {last_complete.date()} '
                        f'(endog NaN: {nan_endog}, exog NaN tolerated: {nan_exog})'
                    )
                    panel = panel.loc[:last_complete]

            # Forward-fill exogenous columns within the kept window so the
            # solver's lag-1 reads from the last historical row never return
            # NaN. The forecast-window fill in solver.forecast (PR #55) only
            # covered FUTURE rows; equations evaluated for the first forecast
            # quarter still read lag-1 from the LAST HISTORICAL row, and a
            # NaN in (e.g.) row_gdp there propagated NaN deltas through every
            # endog and yielded an all-em-dash forecast table again.
            # Endogenous columns are intentionally left untouched — those are
            # the model's dependents and ffilling would mask genuinely-missing
            # observations the regression needs to drop.
            exog_codes = [c for c in all_codes if c not in endog_codes]
            if exog_codes:
                before_nan = int(panel[exog_codes].isna().sum().sum())
                panel[exog_codes] = panel[exog_codes].ffill()
                after_nan = int(panel[exog_codes].isna().sum().sum())
                if before_nan > after_nan:
                    logger.info(
                        f'macro_model.data: forward-filled exog NaN cells '
                        f'{before_nan} → {after_nan} so solver lag-reads see real values'
                    )

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
