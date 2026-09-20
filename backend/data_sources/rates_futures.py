"""
US fed funds futures (ZQ) curve fetcher.

Pulls dated CME fed funds futures contracts from Yahoo Finance for the
next 12-18 months. Each ZQ contract prices the AVERAGE effective fed
funds rate over its contract month. Implied rate = ``100 - settle_price``
so a ZQ contract quoted at 96.75 implies an average fed funds rate of
3.25% over that month.

Design mirrors ``backend/data_sources/forward_curve.py`` (WTI/Brent
forwards) — same yfinance-dated-contract pattern, same fail-soft
philosophy, same 6h ``@cached`` disk persistence via the shared
``_cache`` module. The output is consumed by:

- ``backend/rates/fedwatch.py`` — backs out per-FOMC-meeting hike/cut
  probabilities from the pre/post-meeting day-weighted decomposition.
- ``backend/rates/routes.py::/api/us-rates/curve`` — exposes the raw
  curve for the ``/us-rates`` page's overlay chart.

FRED (``DFF``) provides the spot anchor. When a Yahoo tenor comes back
blank the caller sees ``implied_rate = null`` for that contract; the
FedWatch calc drops any meeting whose contract has no implied rate.

The whole module is fail-soft. Every public function returns a payload
with an ``error`` field rather than raising.
"""

from __future__ import annotations

import calendar
import logging
from datetime import date, timedelta
from typing import Optional

from backend.data_sources._cache import cached
from backend.data_sources import fred_client

logger = logging.getLogger(__name__)

# CME / ICE month codes used across ZQ, SR3, CL, BZ, NG, TTF.
_MONTH_CODE = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
               7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

# Yahoo suffix candidates. Yahoo has been inconsistent about the exchange
# suffix for fed funds futures — we try `.CBT` (Chicago Board of Trade,
# the historical home) first, then `.CBOT`, then bare.
_ZQ_SUFFIXES = ['.CBT', '.CBOT', '']

# How many contracts forward. 15 months = enough to cover ~5 FOMC meetings
# on the near side plus 3 more on the far side for the FedWatch grid.
DEFAULT_HORIZON_MONTHS = 15

# Recent-bars average protects against a stale single settlement.
_RECENT_BARS = 5


def _contract_symbol(year: int, month: int, suffix: str) -> str:
    """``ZQ<M><YY><suffix>`` — e.g. ZQM26.CBT for the June 2026 contract."""
    return f'ZQ{_MONTH_CODE[month]}{str(year)[-2:]}{suffix}'


def _fetch_one_contract(symbol: str) -> Optional[float]:
    """Recent-N-bars average close for one Yahoo ticker, or None if the
    ticker returns nothing."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning('yfinance not installed; ZQ curve fetch skipped')
        return None
    try:
        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=14)
        data = yf.download(
            symbol,
            start=start.isoformat(),
            end=end.isoformat(),
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:  # noqa: BLE001
        logger.debug(f'yfinance ZQ fetch {symbol!r} failed: {e}')
        return None
    if data is None or data.empty or 'Close' not in data.columns:
        return None
    close = data['Close']
    # yfinance sometimes returns a DataFrame (multi-column) even for a
    # single ticker — squeeze to a Series.
    try:
        import pandas as pd
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
    except ImportError:
        pass
    close = close.dropna()
    if close.empty:
        return None
    return float(close.tail(_RECENT_BARS).mean())


def _fetch_zq_with_fallback(year: int, month: int) -> Optional[float]:
    """Try each Yahoo suffix in turn until one returns data."""
    for suffix in _ZQ_SUFFIXES:
        symbol = _contract_symbol(year, month, suffix)
        price = _fetch_one_contract(symbol)
        if price is not None:
            logger.debug(f'ZQ contract {symbol} settled at {price:.4f}')
            return price
    return None


def _forward_months(anchor: Optional[date], h: int) -> list[tuple[int, int]]:
    """(year, month) pairs for the next ``h`` months after the anchor
    month (inclusive of the anchor's own month — near-contract is 'this
    month')."""
    anchor = anchor or date.today()
    out: list[tuple[int, int]] = []
    y, m = anchor.year, anchor.month
    for _ in range(h):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _get_spot_rate() -> tuple[Optional[float], Optional[str]]:
    """Latest DFF (daily effective fed funds rate) from FRED.

    Returns ``(rate_pct, date_str)``. When no FRED key is configured,
    returns ``(None, None)`` — the caller then omits the spot field.
    """
    data = fred_client.fetch_series('DFF')
    if not data:
        return None, None
    last = data[-1]
    try:
        return float(last['value']), str(last['date'])
    except (KeyError, TypeError, ValueError):
        return None, None


@cached(namespace='rates_futures', ttl=6 * 3600, disk=True)
def get_fed_funds_curve(horizon_months: int = DEFAULT_HORIZON_MONTHS) -> dict:
    """Return the fed funds futures curve for the next ``horizon_months``.

    Payload shape::

        {
          'curve': [
            {
              'contract': 'ZQM26.CBT',
              'year': 2026, 'month': 6,
              'month_label': 'Jun 2026',
              'months_ahead': 0,   # 0 = current month
              'implied_rate': 4.32, # % (100 - settle)
              'settle_price': 95.68,
            },
            ...
          ],
          'spot': {'rate': 4.33, 'date': '2026-09-18'},   # from FRED DFF
          'anchor_date': '2026-09-19',
          'source': 'yfinance+fred',
          'last_updated': 1758259200.0,
          'error': None,
        }

    Cached 6h on disk (survives Render redeploys via ``Config.DATA_DIR``).
    """
    import time

    anchor = date.today()
    months = _forward_months(anchor, horizon_months)
    curve: list[dict] = []

    for i, (yr, mo) in enumerate(months):
        settle = _fetch_zq_with_fallback(yr, mo)
        if settle is None:
            curve.append({
                'contract': _contract_symbol(yr, mo, ''),
                'year': yr, 'month': mo,
                'month_label': f'{calendar.month_abbr[mo]} {yr}',
                'months_ahead': i,
                'implied_rate': None,
                'settle_price': None,
            })
            continue
        curve.append({
            'contract': _contract_symbol(yr, mo, _ZQ_SUFFIXES[0]),
            'year': yr, 'month': mo,
            'month_label': f'{calendar.month_abbr[mo]} {yr}',
            'months_ahead': i,
            'implied_rate': round(100.0 - settle, 4),
            'settle_price': round(settle, 4),
        })

    spot_rate, spot_date = _get_spot_rate()
    n_ok = sum(1 for c in curve if c['implied_rate'] is not None)

    return {
        'curve': curve,
        'spot': (
            {'rate': spot_rate, 'date': spot_date}
            if spot_rate is not None else None
        ),
        'anchor_date': anchor.isoformat(),
        'source': 'yfinance+fred',
        'last_updated': time.time(),
        'n_contracts_fetched': n_ok,
        'error': None if n_ok else 'no ZQ contracts returned data',
    }


# ── Smoke test ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')
    result = get_fed_funds_curve()
    print(f"anchor {result['anchor_date']}, source {result['source']}")
    if result.get('spot'):
        print(f"spot DFF {result['spot']['rate']}% ({result['spot']['date']})")
    for c in result['curve']:
        rate = f'{c["implied_rate"]:.3f}%' if c['implied_rate'] is not None else '   —   '
        print(f"  {c['contract']:<12} {c['month_label']:<10} implied {rate}")
