"""
Forward curve fetcher for oil & gas commodities.

Pulls dated futures contract prices and averages them into quarterly
buckets, giving each forecast quarter a market-implied price level. The
quarterly curve is then used by ``commodity_models.CommodityModel.forecast``
as a horizon-weighted shrinkage target so the model's central tendency
anchors to the futures market instead of extrapolating recent drift.

Design choices
--------------
* We fetch one contract per forecast month (12 contracts for h=4 quarters)
  and average the three months in each quarter into one price.
* yfinance is best-effort — individual dated contracts have spotty coverage
  and the symbol shape changes by exchange. We try ``<root><MMM><YY>.NYM``
  first, then fall back to ``<root><MMM><YY>``, then give up on that month.
  Quarters where < 2 of 3 contracts return data are dropped from the curve;
  the caller treats a missing quarter as "no anchor" for that horizon.
* Cached for 6 hours — futures roll daily but the curve shape doesn't move
  enough to justify hammering yfinance per request.
* Currently scoped to WTI / Brent / Henry Hub / TTF. Metals / ag curves
  exist but are less liquid — extend ``CURVE_SPECS`` if you want them.

The whole module is **fail-soft**. Every public function returns ``None``
or an empty dict on failure rather than raising.
"""

from __future__ import annotations

import time
import logging
import threading
import calendar
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_TTL = 6 * 3600          # 6 hours
RECENT_BARS = 5               # average of the last N daily closes per contract

# Per-commodity contract-symbol metadata.
# `root` is the futures root; `suffixes` are exchange suffixes to try in
# order (yfinance is inconsistent — some tickers want '.NYM', some bare).
CURVE_SPECS: dict[str, dict] = {
    'WTI Crude':        {'root': 'CL',  'suffixes': ['.NYM', '']},
    'Brent Crude':      {'root': 'BZ',  'suffixes': ['.NYM', '']},
    'Natural Gas (HH)': {'root': 'NG',  'suffixes': ['.NYM', '']},
    'TTF Gas':          {'root': 'TTF', 'suffixes': ['.NYM', '']},
}

# CME / ICE month codes
_MONTH_CODE = {1:'F', 2:'G', 3:'H', 4:'J', 5:'K', 6:'M',
               7:'N', 8:'Q', 9:'U', 10:'V', 11:'X', 12:'Z'}


def _contract_symbol(root: str, year: int, month: int, suffix: str) -> str:
    return f'{root}{_MONTH_CODE[month]}{str(year)[-2:]}{suffix}'


def _fetch_one_contract(symbol: str) -> Optional[float]:
    """Pull the most recent settlement-ish price for a single contract."""
    try:
        import yfinance as yf
    except ImportError:
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
    except Exception as e:
        logger.debug(f'yfinance contract fetch {symbol!r} failed: {e}')
        return None
    if data is None or data.empty or 'Close' not in data.columns:
        return None
    close = data['Close']
    import pandas as pd
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    close = close.dropna()
    if close.empty:
        return None
    return float(close.tail(RECENT_BARS).mean())


def _fetch_contract_with_fallback(root: str, year: int, month: int,
                                  suffixes: list[str]) -> Optional[float]:
    for suffix in suffixes:
        symbol = _contract_symbol(root, year, month, suffix)
        price = _fetch_one_contract(symbol)
        if price is not None:
            return price
    return None


def _forward_quarters(anchor: Optional[date], h: int) -> list[tuple[int, int]]:
    """Return (year, quarter_num) for the h quarters AFTER the anchor's quarter."""
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


def _quarter_months(year: int, quarter: int) -> list[tuple[int, int]]:
    start_month = (quarter - 1) * 3 + 1
    return [(year, start_month + i) for i in range(3)]


# ── Public API ────────────────────────────────────────────────────────────

class _CurveCache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, tuple[float, dict]] = {}  # name → (fetched_at, payload)

    def get(self, name: str) -> Optional[dict]:
        with self._lock:
            entry = self._data.get(name)
            if entry and (time.time() - entry[0]) < CACHE_TTL:
                return entry[1]
        return None

    def put(self, name: str, payload: dict) -> None:
        with self._lock:
            self._data[name] = (time.time(), payload)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_cache = _CurveCache()


def fetch_curve_quarterly(commodity: str, h: int = 4,
                          anchor: Optional[date] = None) -> Optional[dict]:
    """Return a dict mapping ``Q+i`` → ``{year, quarter, label, mean_price,
    contracts_used}`` for ``i`` in 1..h, or ``None`` if no anchor data could
    be retrieved.

    Cached 6 hours. Anchor defaults to today; pass a historical date for
    backtesting.
    """
    spec = CURVE_SPECS.get(commodity)
    if not spec:
        return None

    cache_key = f'{commodity}:{anchor.isoformat() if anchor else "today"}:{h}'
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    quarters = _forward_quarters(anchor, h)
    out: dict[str, dict] = {}
    any_data = False
    for i, (year, q) in enumerate(quarters, start=1):
        prices: list[float] = []
        for ym in _quarter_months(year, q):
            p = _fetch_contract_with_fallback(spec['root'], ym[0], ym[1], spec['suffixes'])
            if p is not None:
                prices.append(p)
        if len(prices) >= 2:
            any_data = True
            out[f'Q+{i}'] = {
                'year': year,
                'quarter': q,
                'label': f'Q{q} {year}',
                'mean_price': float(sum(prices) / len(prices)),
                'contracts_used': len(prices),
            }
        else:
            logger.info(f'{commodity} curve: dropped Q+{i} (only {len(prices)}/3 contracts returned)')

    if not any_data:
        logger.warning(f'{commodity}: no forward-curve data retrieved')
        _cache.put(cache_key, None)
        return None

    _cache.put(cache_key, out)
    return out


def clear_cache() -> None:
    _cache.clear()


# Default per-quarter shrinkage weights (model_weight, curve_weight).
# The curve dominates at long horizons where the model's drift is least
# reliable; the model dominates near-term where it has just-released info.
DEFAULT_SHRINKAGE: dict[str, tuple[float, float]] = {
    'Q+1': (0.70, 0.30),
    'Q+2': (0.55, 0.45),
    'Q+3': (0.40, 0.60),
    'Q+4': (0.25, 0.75),
}


def shrink_to_curve(model_quarterly: dict, curve_quarterly: Optional[dict],
                    weights: Optional[dict] = None) -> dict:
    """Blend a model-output quarterly forecast (median + p2.5/p10/p90/p97.5)
    toward the curve-implied price via horizon-weighted shrinkage.

    Only the central tendency (``median``, ``p10``, ``p90``) is shifted —
    the outer 95% bounds (``p2.5``, ``p97.5``) widen to reflect both the
    model's uncertainty AND the offset between model and curve.
    """
    if not curve_quarterly:
        return model_quarterly
    weights = weights or DEFAULT_SHRINKAGE
    out: dict = {}
    for q_key, model_q in model_quarterly.items():
        curve_q = curve_quarterly.get(q_key)
        if not curve_q:
            out[q_key] = model_q
            continue
        w_model, w_curve = weights.get(q_key, (0.5, 0.5))
        curve_price = float(curve_q['mean_price'])
        shifted = {}
        for k, v in model_q.items():
            if k in ('median', 'p10', 'p90'):
                shifted[k] = w_model * float(v) + w_curve * curve_price
            else:
                shifted[k] = v
        # Widen the 95% envelope to encompass the original model band, the
        # curve, and the shifted median — so we don't accidentally collapse
        # the CI when model and curve disagree sharply.
        original_low  = float(model_q.get('p2_5', shifted.get('median')))
        original_high = float(model_q.get('p97_5', shifted.get('median')))
        shifted['p2_5']  = min(original_low,  shifted['median'] * 0.85, curve_price * 0.85)
        shifted['p97_5'] = max(original_high, shifted['median'] * 1.15, curve_price * 1.15)
        shifted['curve_anchor'] = curve_price
        shifted['anchor_weight'] = w_curve
        out[q_key] = shifted
    return out


# ── Smoke test ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')
    for commodity in ('WTI Crude', 'Natural Gas (HH)'):
        print(f'\n=== {commodity} ===')
        curve = fetch_curve_quarterly(commodity)
        if not curve:
            print('  no curve data')
            continue
        for q, info in curve.items():
            print(f'  {q} ({info["label"]}): ${info["mean_price"]:.2f} '
                  f'({info["contracts_used"]}/3 contracts)')
