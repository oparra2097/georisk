"""
Long-run trend anchor for metals forecasts.

For commodities whose structural prices have a reliable multi-decade drift
(gold, silver, platinum, copper — not ag, not oil/gas), the SARIMAX(1,0,1)
model's drift estimate on a 10-year training window can under-capture the
secular uptrend if the recent period happened to consolidate. Example:
gold has compounded ~8-9% nominal over 20+ years driven by central-bank
reserve diversification and real-rate dynamics, but a 2022-2024
consolidation window will make a local drift estimate look near-zero.

This module computes a **long-run price trajectory** for each supported
commodity from 15-20 years of history and exposes it as a shrinkage
target the forecast layer can blend the SARIMAX median toward. Scope:
gold, silver, platinum, copper. Oil/gas already anchor on the futures
curve (see ``forward_curve.py``); agriculture is skipped because
idiosyncratic supply shocks (2024-26 cocoa, 2022 wheat) distort any
long-run CAGR estimate beyond usefulness.

Design choices
--------------
* Compound annual growth rate (CAGR) computed from the endpoint-to-
  endpoint log-return over ``years`` of monthly closes. Winsorised at the
  5th/95th percentile of annual log-returns before averaging, so a single
  crash / spike year doesn't dominate.
* Trend projection is deterministic: ``price(t) = last_price * (1 + CAGR)^(t_years)``
  per forecast month, averaged into quarterly buckets.
* Cache TTL: 24h. Trend shifts slowly; no point re-fetching each call.
* Fail-soft: any yfinance / data failure returns ``None`` and the caller
  (``commodity_models.get_model_forecast``) falls back to pure model output.
"""

from __future__ import annotations

import time
import math
import logging
import calendar
import threading
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_TTL = 24 * 3600

# Per-commodity trend anchor specification.
#   years      — training window for the CAGR estimate
#   ticker     — yfinance ticker (must match commodity_models.TICKERS)
#   notes      — rationale, for the per-commodity white paper
TREND_ANCHOR_SPECS: dict[str, dict] = {
    'Gold': {
        'years': 20, 'ticker': 'GC=F',
        'notes': 'Central-bank reserve diversification since 2022 '
                 'reinforced a 20y ~7-9% CAGR drift. Long window '
                 'prevents short-term consolidation from pulling the '
                 'forecast below structural trend.',
    },
    'Silver': {
        'years': 20, 'ticker': 'SI=F',
        'notes': 'Hybrid monetary + industrial (solar). Tracks gold '
                 'with ~2x volatility.',
    },
    'Platinum': {
        'years': 15, 'ticker': 'PL=F',
        'notes': 'Weaker long-run trend than gold/silver due to ICE '
                 'auto decline; hydrogen demand is still a small share.',
    },
    'Copper': {
        'years': 20, 'ticker': 'HG=F',
        'notes': 'Structural demand lift from EV / grid transition on '
                 'top of China industrial cycle.',
    },
}


# Horizon-weighted shrinkage — the trend gets more weight at the long end,
# where the SARIMAX drift estimate becomes less reliable relative to the
# structural secular trend. Differs from the futures-curve weights because
# the trend is a "slow" anchor, not a market-implied price.
DEFAULT_SHRINKAGE: dict[str, tuple[float, float]] = {
    'Q+1': (0.80, 0.20),   # model dominates near-term
    'Q+2': (0.65, 0.35),
    'Q+3': (0.50, 0.50),
    'Q+4': (0.35, 0.65),   # trend dominates far-term
}


# ── Internal helpers ──────────────────────────────────────────────────────

def _last_price_and_cagr(ticker: str, years: int,
                         anchor: Optional[date] = None) -> Optional[tuple[float, float]]:
    """Fetch ``years`` of monthly closes for ``ticker`` via yfinance,
    return ``(last_price, monthly_log_drift)``.

    ``monthly_log_drift`` is a winsorised average of monthly log-returns so
    one extreme month can't dominate. Returns ``None`` on any fetch
    failure so callers fall back cleanly.
    """
    try:
        import yfinance as yf
        import numpy as np
        import pandas as pd
    except ImportError:
        return None

    end_dt = (anchor or date.today()) + timedelta(days=1)
    start_dt = (anchor or date.today()) - timedelta(days=365 * years)

    try:
        data = yf.download(
            ticker,
            start=start_dt.isoformat(),
            end=end_dt.isoformat(),
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:
        logger.debug(f'{ticker}: long-run fetch failed: {e}')
        return None
    if data is None or data.empty:
        return None

    close = data['Close'] if 'Close' in data.columns else data.iloc[:, 0]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    monthly = close.dropna().resample('ME').mean().dropna()
    if len(monthly) < 36:
        return None

    log_returns = np.log(monthly).diff().dropna()
    if log_returns.empty:
        return None

    low, high = log_returns.quantile(0.05), log_returns.quantile(0.95)
    winsorised = log_returns.clip(lower=low, upper=high)
    drift_per_month = float(winsorised.mean())
    last_price = float(monthly.iloc[-1])

    return last_price, drift_per_month


def _forecast_quarters(anchor: Optional[date], h: int) -> list[tuple[int, int]]:
    anchor = anchor or date.today()
    current_q = (anchor.month - 1) // 3 + 1
    out: list[tuple[int, int]] = []
    for i in range(1, h + 1):
        qn = current_q + i
        yr = anchor.year
        while qn > 4:
            qn -= 4
            yr += 1
        out.append((yr, qn))
    return out


# ── Cache ─────────────────────────────────────────────────────────────────

class _TrendCache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, tuple[float, Optional[dict]]] = {}

    def get(self, key: str):
        with self._lock:
            entry = self._data.get(key)
            if entry and (time.time() - entry[0]) < CACHE_TTL:
                return entry[1]
        return None

    def put(self, key: str, payload: Optional[dict]) -> None:
        with self._lock:
            self._data[key] = (time.time(), payload)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_cache = _TrendCache()


# ── Public API ────────────────────────────────────────────────────────────

def fetch_long_run_trend(commodity: str, h: int = 4,
                          anchor: Optional[date] = None) -> Optional[dict]:
    """Return the projected trend-line price for the next ``h`` quarters,
    or ``None`` if the commodity has no spec or no usable data.

    Result shape::

        {
            'Q+i': {
                'year': int,
                'quarter': int,
                'label': str,
                'mean_price': float,      # projected trend-line price
                'cagr_annual_pct': float, # informational
            },
            ...
        }
    """
    spec = TREND_ANCHOR_SPECS.get(commodity)
    if not spec:
        return None

    cache_key = f'{commodity}:{anchor.isoformat() if anchor else "today"}:{h}'
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    res = _last_price_and_cagr(spec['ticker'], spec['years'], anchor)
    if res is None:
        _cache.put(cache_key, None)
        return None
    last_price, drift_per_month = res

    # Each forecast quarter = 3 months forward from the pivot. The price
    # within a quarter is averaged across its 3 monthly projections so the
    # anchor aligns with the model's quarterly-average convention.
    quarters = _forecast_quarters(anchor, h)
    out: dict[str, dict] = {}
    months_from_pivot = 0
    for i, (yr, q) in enumerate(quarters, start=1):
        quarter_prices = []
        for _m in range(3):
            months_from_pivot += 1
            projected = last_price * math.exp(drift_per_month * months_from_pivot)
            quarter_prices.append(projected)
        mean_q = sum(quarter_prices) / 3.0
        out[f'Q+{i}'] = {
            'year': yr,
            'quarter': q,
            'label': f'Q{q} {yr}',
            'mean_price': round(mean_q, 2),
            'cagr_annual_pct': round((math.exp(drift_per_month * 12) - 1.0) * 100, 2),
        }

    _cache.put(cache_key, out)
    return out


def shrink_to_trend(model_quarterly: dict, trend_quarterly: Optional[dict],
                    weights: Optional[dict] = None) -> dict:
    """Blend the model's central tendency toward the long-run trend line.

    Same pattern as ``forward_curve.shrink_to_curve`` but with different
    default weights — the trend line is deterministic and slower than the
    futures curve, so it weighs in gradually across the horizon. CI bounds
    widen to encompass both the model envelope and the trend so the
    interval doesn't collapse if the two disagree sharply.
    """
    if not trend_quarterly:
        return model_quarterly
    weights = weights or DEFAULT_SHRINKAGE
    out: dict = {}
    for q_key, model_q in model_quarterly.items():
        trend_q = trend_quarterly.get(q_key)
        if not trend_q:
            out[q_key] = model_q
            continue
        w_model, w_trend = weights.get(q_key, (0.5, 0.5))
        trend_price = float(trend_q['mean_price'])
        shifted = {}
        for k, v in model_q.items():
            if k in ('median', 'p10', 'p90'):
                shifted[k] = w_model * float(v) + w_trend * trend_price
            else:
                shifted[k] = v
        original_low  = float(model_q.get('p2_5', shifted.get('median')))
        original_high = float(model_q.get('p97_5', shifted.get('median')))
        shifted['p2_5']  = min(original_low,  shifted['median'] * 0.85, trend_price * 0.85)
        shifted['p97_5'] = max(original_high, shifted['median'] * 1.15, trend_price * 1.15)
        shifted['trend_anchor']       = trend_price
        shifted['trend_anchor_weight'] = w_trend
        shifted['trend_cagr_annual']  = trend_q.get('cagr_annual_pct')
        out[q_key] = shifted
    return out


def clear_cache() -> None:
    _cache.clear()


# ── Smoke test ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')
    for commodity in TREND_ANCHOR_SPECS:
        print(f'\n=== {commodity} ===')
        trend = fetch_long_run_trend(commodity)
        if not trend:
            print('  no trend data')
            continue
        for q, info in trend.items():
            print(f'  {q} ({info["label"]}): ${info["mean_price"]:.2f}  '
                  f'CAGR {info["cagr_annual_pct"]:+.2f}%')
