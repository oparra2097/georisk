"""
Index math: per-entity time-series → summary statistics.

Given a list of HpiRow entries for a single (level, code) entity in
chronological order, compute:

    latest_index    most recent index value
    mom             month-over-month % change (monthly series only)
    qoq             quarter-over-quarter % change (quarterly series)
    yoy             year-over-year % change
    yoy_3y_avg      average YoY over last 3 years (trend benchmark)
    from_peak       current index / historical peak − 1  (drawdown / runup)
    peak_ts         date of the historical peak
    zscore_yoy      current YoY normalized to its own 10-year history

All as plain dicts — no pandas dependency here so the module is trivial
to test and reuse.
"""

from __future__ import annotations

import math
from statistics import mean, stdev
from typing import Optional

from backend.house_prices.fetchers.fhfa import HpiRow


def _rate(current: float, earlier: float) -> Optional[float]:
    if earlier is None or current is None or earlier <= 0:
        return None
    return (current / earlier - 1.0) * 100.0


def _sort(rows: list[HpiRow]) -> list[HpiRow]:
    return sorted(rows, key=lambda r: (r.year, r.period))


def _pick_index(r: HpiRow) -> Optional[float]:
    """Prefer SA when available (smoother for change calcs), else NSA."""
    return r.index_sa if r.index_sa is not None else r.index_nsa


def _periods_per_year(freq: str) -> int:
    return {'monthly': 12, 'quarterly': 4, 'annual': 1}.get(freq, 4)


def summarize(rows: list[HpiRow]) -> Optional[dict]:
    """
    Summary statistics for a single entity's full history.

    Returns None if no usable data.
    """
    if not rows:
        return None
    rows = _sort(rows)
    # Drop rows where index is missing
    rows = [r for r in rows if _pick_index(r) is not None]
    if not rows:
        return None

    freq = rows[-1].freq
    ppy = _periods_per_year(freq)

    latest = rows[-1]
    latest_val = _pick_index(latest)

    # Period-over-period
    pop = _rate(latest_val, _pick_index(rows[-2])) if len(rows) > 1 else None

    # Year-over-year
    yoy = _rate(latest_val, _pick_index(rows[-1 - ppy])) if len(rows) > ppy else None

    # Trailing 3-year average of YoY (using every point, not just latest)
    yoy_series: list[float] = []
    for i in range(ppy, len(rows)):
        v = _rate(_pick_index(rows[i]), _pick_index(rows[i - ppy]))
        if v is not None:
            yoy_series.append(v)
    yoy_3y_avg = mean(yoy_series[-3 * ppy:]) if len(yoy_series) >= 3 * ppy else None

    # Peak / trough
    peak_r = max(rows, key=lambda r: _pick_index(r))
    peak_val = _pick_index(peak_r)
    from_peak = (latest_val / peak_val - 1.0) * 100.0 if peak_val else None

    # Z-score of current YoY vs a 10-year self-history
    zscore_yoy: Optional[float] = None
    if yoy_series and len(yoy_series) >= 2:
        recent = yoy_series[-10 * ppy:] if len(yoy_series) >= 10 * ppy else yoy_series
        if len(recent) >= 2:
            m = mean(recent)
            s = stdev(recent) if len(recent) > 1 else 0.0
            if s and yoy is not None:
                zscore_yoy = (yoy - m) / s

    return {
        'level': latest.level,
        'code': latest.code,
        'name': latest.name,
        'freq': freq,
        'latest_date': _label(latest),
        'latest_index': round(latest_val, 2),
        'pop_pct':     _round(pop),
        'yoy_pct':     _round(yoy),
        'yoy_3y_avg':  _round(yoy_3y_avg),
        'from_peak_pct': _round(from_peak),
        'peak_date':   _label(peak_r),
        'peak_index':  round(peak_val, 2) if peak_val else None,
        'zscore_yoy':  _round(zscore_yoy, 3),
        'n_obs':       len(rows),
    }


def history(rows: list[HpiRow], min_year: Optional[int] = None) -> list[dict]:
    """
    Flatten a single entity's time series for plotting.

    Returns a list of {'date', 'index', 'yoy_pct'} dicts sorted ascending.
    """
    rows = _sort(rows)
    rows = [r for r in rows if _pick_index(r) is not None]
    if min_year is not None:
        rows = [r for r in rows if r.year >= min_year]

    ppy = _periods_per_year(rows[-1].freq) if rows else 4
    out: list[dict] = []
    for i, r in enumerate(rows):
        yoy = _rate(_pick_index(r), _pick_index(rows[i - ppy])) if i >= ppy else None
        out.append({
            'date':  _label(r),
            'index': round(_pick_index(r), 2),
            'yoy_pct': _round(yoy),
        })
    return out


def _label(r: HpiRow) -> str:
    if r.freq == 'annual':
        return f'{r.year}'
    if r.freq == 'quarterly':
        return f'{r.year}Q{r.period}'
    return f'{r.year}-{r.period:02d}'


def _round(v: Optional[float], d: int = 2) -> Optional[float]:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return None
    return round(v, d)


def group_by_entity(rows: list[HpiRow]) -> dict[tuple[str, str], list[HpiRow]]:
    """Groups a flat list of HpiRow into {(level, code): [rows]} buckets."""
    out: dict[tuple[str, str], list[HpiRow]] = {}
    for r in rows:
        key = (r.level, r.code)
        out.setdefault(key, []).append(r)
    return out
