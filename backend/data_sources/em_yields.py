"""
EM 10Y sovereign yield fetcher.

Primary source: yfinance dated tickers like ``BR10YT=X`` and
``MX10YT=X`` (best-effort — coverage is spotty for smaller EMs).
Fallback: FRED OECD long-term rate series
``IRLTLT01<ISO2>M156N`` (monthly, has coverage for OECD members).

Returns a dict per country::

    {
      'iso3': 'BRA',
      'current': 11.82,           # most-recent yield, %
      'current_date': '2026-09-18',
      'delta_3m_bp': 34.2,        # bp change over the past 3 months
      'delta_12m_bp': -128.5,
      'history': [{'date':'YYYY-MM-DD','value':11.5}, ...],
      'source': 'yfinance',
      'error': None,
    }

Fail-soft — on any error returns a placeholder with ``error`` set and
``current=None``. Callers should skip countries with ``error`` set
rather than blow the whole panel.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from backend.data_sources._cache import cached
from backend.data_sources import fred_client
from backend.rates.em_config import COUNTRIES

logger = logging.getLogger(__name__)


def _yfinance_yield(symbol: str) -> Optional[list[dict]]:
    """Pull ~14 months of daily yield history from Yahoo. Returns None
    on any failure. Yahoo returns yields as % directly for `*10YT=X`
    tickers (no conversion needed)."""
    try:
        import yfinance as yf
    except ImportError:
        return None
    try:
        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=430)  # ~14 months
        data = yf.download(symbol, start=start.isoformat(), end=end.isoformat(),
                           auto_adjust=True, progress=False)
    except Exception as e:  # noqa: BLE001
        logger.debug(f'yfinance yield fetch {symbol!r} failed: {e}')
        return None
    if data is None or data.empty or 'Close' not in data.columns:
        return None
    close = data['Close']
    try:
        import pandas as pd
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
    except ImportError:
        pass
    close = close.dropna()
    if close.empty:
        return None
    return [
        {'date': ts.date().isoformat(), 'value': float(v)}
        for ts, v in close.items()
    ]


def _fred_yield(fred_id: str) -> Optional[list[dict]]:
    data = fred_client.fetch_series(fred_id)
    if not data:
        return None
    return data


def _bp_delta(history: list[dict], months_back: int) -> Optional[float]:
    """Return the basis-point change from ``months_back`` months ago
    to the most recent observation."""
    if not history:
        return None
    last = history[-1]
    if last.get('value') is None:
        return None
    last_date = date.fromisoformat(last['date'])
    target = last_date - timedelta(days=30 * months_back)
    # Find the observation closest to `target` without going past it.
    best = None
    for rec in history:
        try:
            d = date.fromisoformat(rec['date'])
        except (TypeError, ValueError):
            continue
        if d <= target:
            best = rec
    if not best or best.get('value') is None:
        return None
    return (float(last['value']) - float(best['value'])) * 100.0  # % → bp


@cached(namespace='em_yields', ttl=6 * 3600, disk=True)
def get_yield(iso3: str) -> dict:
    """Return current 10Y yield + deltas + short history for one
    country."""
    iso3 = iso3.upper()
    cfg = COUNTRIES.get(iso3)
    if not cfg:
        return {'iso3': iso3, 'current': None, 'error': 'unknown country'}

    history: Optional[list[dict]] = None
    source = None

    # Yahoo first.
    yahoo_symbol = cfg.get('yahoo_10y')
    if yahoo_symbol:
        history = _yfinance_yield(yahoo_symbol)
        if history:
            source = f'yfinance:{yahoo_symbol}'

    # FRED OECD long-term rate fallback.
    if not history and cfg.get('fred_10y'):
        history = _fred_yield(cfg['fred_10y'])
        if history:
            source = f'fred:{cfg["fred_10y"]}'

    if not history:
        return {
            'iso3': iso3, 'current': None, 'error': 'no yield data',
            'delta_3m_bp': None, 'delta_12m_bp': None, 'history': [],
            'source': None,
        }

    last = history[-1]
    return {
        'iso3': iso3,
        'current': round(float(last['value']), 3) if last.get('value') is not None else None,
        'current_date': last.get('date'),
        'delta_3m_bp': round(_bp_delta(history, 3) or 0.0, 1)
                       if _bp_delta(history, 3) is not None else None,
        'delta_12m_bp': round(_bp_delta(history, 12) or 0.0, 1)
                        if _bp_delta(history, 12) is not None else None,
        # Trim history to keep the JSON payload light.
        'history': history[-260:],
        'source': source,
        'error': None,
    }
