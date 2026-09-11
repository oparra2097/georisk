"""Market data for the options model — prices, option chain, rates.

Everything comes from Yahoo Finance via yfinance (already a dependency of
this app) plus the 13-week T-bill index for the discount rate. Every fetch
fails soft: a missing option chain degrades the page to a pure
random-walk/vol view rather than erroring, because Yahoo's chain endpoint is
the flakiest thing we touch.

Quotes on a mid-cap single name are wide and often stale, so each chain row
carries its own quality flags (no bid, crossed market, wide spread, zero open
interest) and the model can drop the untradeable ones instead of pretending a
$0.05 x $1.20 market is a price.
"""

import logging
import math
import threading
import time
from datetime import datetime, date, timezone

logger = logging.getLogger(__name__)

# Lazy imports — yfinance/pandas are heavy and this module is imported at app
# start by the blueprint.
yf = None
np = None


def _ensure_libs():
    global yf, np
    if yf is None:
        import yfinance as _yf
        yf = _yf
    if np is None:
        import numpy as _np
        np = _np


DEFAULT_TICKER = 'TSEM'

# Tickers the page offers out of the box: high-vol semis/foundry complex that
# trades like TSEM, plus the two obvious benchmarks.
PRESET_TICKERS = [
    {'symbol': 'TSEM', 'name': 'Tower Semiconductor'},
    {'symbol': 'UMC', 'name': 'United Microelectronics'},
    {'symbol': 'GFS', 'name': 'GlobalFoundries'},
    {'symbol': 'AMD', 'name': 'Advanced Micro Devices'},
    {'symbol': 'INTC', 'name': 'Intel'},
    {'symbol': 'MU', 'name': 'Micron'},
    {'symbol': 'SMCI', 'name': 'Super Micro Computer'},
    {'symbol': 'SOXX', 'name': 'iShares Semiconductor ETF'},
    {'symbol': 'SPY', 'name': 'S&P 500 ETF'},
]

RATE_TICKER = '^IRX'          # 13-week T-bill discount rate, quoted in %
FALLBACK_RATE = 0.042         # used only if ^IRX is unavailable

HISTORY_PERIOD = '3y'
CACHE_TTL = 600               # 10 minutes
MAX_EXPIRIES = 6              # keep the chain fetch bounded — 1 HTTP call each

# A quote wider than this fraction of its own mid is treated as indicative
# only: you would give up more crossing the spread than the model's edge.
MAX_REL_SPREAD = 0.35


# ── Thread-safe TTL cache, keyed by ticker ────────────────────────────────

class _TickerCache:
    def __init__(self, ttl=CACHE_TTL):
        self._lock = threading.RLock()
        self._store = {}
        self._ttl = ttl

    def get(self, key, builder):
        with self._lock:
            hit = self._store.get(key)
            if hit and (time.time() - hit[0]) < self._ttl:
                return hit[1]
        value = builder()
        with self._lock:
            # Never overwrite a good payload with a failed refresh.
            if value is not None or key not in self._store:
                self._store[key] = (time.time(), value)
            return self._store[key][1]

    def clear(self, key=None):
        with self._lock:
            if key is None:
                self._store.clear()
            else:
                self._store.pop(key, None)


_cache = _TickerCache()


def clear_cache(ticker=None):
    _cache.clear(ticker.upper() if ticker else None)


# ── History ───────────────────────────────────────────────────────────────

def fetch_history(ticker, period=HISTORY_PERIOD):
    """Daily OHLCV, oldest first. Returns ``None`` on failure."""
    _ensure_libs()
    try:
        hist = yf.Ticker(ticker).history(period=period, interval='1d',
                                         auto_adjust=False)
    except Exception as exc:
        logger.warning('options_model: history fetch failed for %s: %s', ticker, exc)
        return None
    if hist is None or hist.empty:
        logger.warning('options_model: empty history for %s', ticker)
        return None

    hist = hist.dropna(subset=['Close'])
    if len(hist) < 60:
        return None

    # Split/dividend-adjusted closes drive the return series; raw closes drive
    # the strike ladder. For TSEM (no dividend, no recent split) they coincide,
    # but keeping them separate stops a future split from faking a -50% day.
    close_adj = hist['Adj Close'] if 'Adj Close' in hist.columns else hist['Close']
    return {
        'dates': [d.strftime('%Y-%m-%d') for d in hist.index],
        'open': [float(x) for x in hist['Open']],
        'high': [float(x) for x in hist['High']],
        'low': [float(x) for x in hist['Low']],
        'close': [float(x) for x in hist['Close']],
        'adj_close': [float(x) for x in close_adj],
        'volume': [float(x) for x in hist['Volume']] if 'Volume' in hist else [],
    }


# ── Rates, dividends, fundamentals ────────────────────────────────────────

def fetch_risk_free_rate():
    """Continuously-compounded short rate from the 13-week bill."""
    _ensure_libs()
    try:
        h = yf.Ticker(RATE_TICKER).history(period='5d', interval='1d')
        if h is not None and not h.empty:
            pct = float(h['Close'].dropna().iloc[-1]) / 100.0
            if 0.0 <= pct < 0.25:
                # Bill index is a simple discount quote; log-convert so it can
                # be used as a continuous rate in Black-Scholes.
                return float(math.log1p(pct))
    except Exception as exc:
        logger.warning('options_model: risk-free fetch failed: %s', exc)
    return FALLBACK_RATE


def fetch_meta(ticker):
    """Name, dividend yield, next earnings date, shares/market cap.

    Every field is optional — yfinance's ``info`` blob is the least reliable
    endpoint it exposes, so each read is guarded individually.
    """
    _ensure_libs()
    out = {'name': ticker, 'dividend_yield': 0.0, 'earnings_date': None,
           'market_cap': None, 'beta': None, 'sector': None}
    tk = yf.Ticker(ticker)
    try:
        info = tk.info or {}
        out['name'] = info.get('shortName') or info.get('longName') or ticker
        dy = info.get('dividendYield')
        if dy is not None:
            dy = float(dy)
            # yfinance has flip-flopped between 0.021 and 2.1 for 2.1%.
            out['dividend_yield'] = dy / 100.0 if dy > 1.0 else dy
        out['market_cap'] = info.get('marketCap')
        out['beta'] = info.get('beta')
        out['sector'] = info.get('sector')
    except Exception as exc:
        logger.info('options_model: info unavailable for %s: %s', ticker, exc)

    try:
        cal = tk.get_earnings_dates(limit=8)
        if cal is not None and not cal.empty:
            today = datetime.now(timezone.utc)
            future = [d for d in cal.index if d.to_pydatetime() > today]
            if future:
                out['earnings_date'] = min(future).strftime('%Y-%m-%d')
    except Exception as exc:
        logger.info('options_model: earnings date unavailable for %s: %s', ticker, exc)

    return out


# ── Option chain ──────────────────────────────────────────────────────────

def _clean_leg(row, kind, spot, expiry, dte):
    """Normalize one yfinance chain row into our own quote dict."""
    def g(key, default=None):
        try:
            v = row.get(key, default)
            if v is None:
                return default
            fv = float(v)
            return default if (fv != fv) else fv   # NaN guard
        except Exception:
            return default

    strike = g('strike')
    if strike is None or strike <= 0:
        return None
    bid, ask = g('bid', 0.0) or 0.0, g('ask', 0.0) or 0.0
    last = g('lastPrice', 0.0) or 0.0

    mid = 0.5 * (bid + ask) if (bid > 0 and ask > 0) else (last if last > 0 else None)
    rel_spread = ((ask - bid) / mid) if (mid and mid > 0 and ask > bid) else None

    flags = []
    if bid <= 0:
        flags.append('no_bid')
    if ask > 0 and bid > 0 and ask < bid:
        flags.append('crossed')
    if rel_spread is not None and rel_spread > MAX_REL_SPREAD:
        flags.append('wide')
    if not (g('openInterest', 0) or 0):
        flags.append('no_oi')

    return {
        'kind': kind,
        'expiry': expiry,
        'dte': dte,
        'strike': strike,
        'bid': bid,
        'ask': ask,
        'last': last,
        'mid': mid,
        'rel_spread': rel_spread,
        'volume': g('volume', 0.0) or 0.0,
        'open_interest': g('openInterest', 0.0) or 0.0,
        'yahoo_iv': g('impliedVolatility'),
        'in_the_money': bool(row.get('inTheMoney', False)),
        'moneyness': (strike / spot) if spot else None,
        'flags': flags,
        'tradeable': not flags or flags == ['no_oi'],
    }


def fetch_option_chain(ticker, spot, max_expiries=MAX_EXPIRIES):
    """Option chain for the nearest ``max_expiries`` expiries.

    Returns a list of ``{'expiry', 'dte', 'calls': [...], 'puts': [...]}``,
    nearest first. Empty list if Yahoo has no chain (common for thin names
    and always for indices we use only as benchmarks).
    """
    _ensure_libs()
    try:
        tk = yf.Ticker(ticker)
        expiries = list(tk.options or [])
    except Exception as exc:
        logger.warning('options_model: expiry list failed for %s: %s', ticker, exc)
        return []

    today = date.today()
    out = []
    for exp in expiries[:max_expiries]:
        try:
            exp_date = datetime.strptime(exp, '%Y-%m-%d').date()
        except ValueError:
            continue
        dte = (exp_date - today).days
        if dte <= 0:
            continue
        try:
            chain = tk.option_chain(exp)
        except Exception as exc:
            logger.info('options_model: chain fetch failed %s %s: %s', ticker, exp, exc)
            continue

        calls, puts = [], []
        for df, kind, bucket in ((chain.calls, 'call', calls), (chain.puts, 'put', puts)):
            if df is None or df.empty:
                continue
            for _, row in df.iterrows():
                leg = _clean_leg(row, kind, spot, exp, dte)
                if leg:
                    bucket.append(leg)
        if calls or puts:
            out.append({'expiry': exp, 'dte': dte,
                        'calls': sorted(calls, key=lambda x: x['strike']),
                        'puts': sorted(puts, key=lambda x: x['strike'])})
    return out


# ── Bundle ────────────────────────────────────────────────────────────────

def get_market_bundle(ticker=DEFAULT_TICKER, with_chain=True):
    """Cached bundle: history + meta + rate + chain. ``None`` if no history."""
    ticker = (ticker or DEFAULT_TICKER).upper().strip()

    def build():
        hist = fetch_history(ticker)
        if not hist:
            return None
        spot = hist['close'][-1]
        prev = hist['close'][-2] if len(hist['close']) > 1 else spot
        bundle = {
            'ticker': ticker,
            'history': hist,
            'spot': spot,
            'prev_close': prev,
            'change_pct': ((spot / prev) - 1.0) * 100.0 if prev else None,
            'as_of': hist['dates'][-1],
            'risk_free': fetch_risk_free_rate(),
            'meta': fetch_meta(ticker),
            'chain': fetch_option_chain(ticker, spot) if with_chain else [],
            'fetched_at': datetime.now(timezone.utc).isoformat(),
        }
        return bundle

    return _cache.get(ticker, build)
