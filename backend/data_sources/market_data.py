"""
Yahoo Finance market data provider.

Fetches live prices and 5-day history for key macro instruments.
Thread-safe cache with 5-minute TTL to avoid hammering the API.
"""

import threading
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Lazy import yfinance (heavy library)
yf = None

def _ensure_yfinance():
    global yf
    if yf is None:
        import yfinance as _yf
        yf = _yf


SYMBOLS = {
    # Equity
    '^GSPC':    {'name': 'S&P 500',          'type': 'equity'},
    '^IXIC':    {'name': 'NASDAQ',            'type': 'equity'},
    '^N225':    {'name': 'Nikkei 225',        'type': 'equity'},
    # Commodities
    'GC=F':     {'name': 'Gold',              'type': 'commodity'},
    'CL=F':     {'name': 'WTI Crude',         'type': 'commodity'},
    'BZ=F':     {'name': 'Brent Crude',       'type': 'commodity'},
    'NG=F':     {'name': 'Natural Gas',       'type': 'commodity'},
    # Fixed Income
    '^TNX':     {'name': 'US 10Y Treasury',   'type': 'bond'},
    '^FVX':     {'name': 'US 5Y Treasury',    'type': 'bond'},
    '^IRX':     {'name': 'US 3M Treasury',    'type': 'bond'},
    # FX
    'EURUSD=X': {'name': 'EUR/USD',           'type': 'fx'},
    # Indicators
    '^VIX':     {'name': 'VIX',               'type': 'indicator'},
}

CACHE_TTL = 300  # 5 minutes


class MarketCache:
    """Thread-safe cache for market data."""

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                return self._data
        # Fetch outside lock to avoid blocking other reads
        data = _fetch_all_markets()
        with self._lock:
            self._data = data
            self._last_fetch = time.time()
        return data

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0


_cache = MarketCache()


def _fetch_single(symbol):
    """Fetch current price and 5-day history for a single symbol."""
    _ensure_yfinance()
    info = SYMBOLS[symbol]
    result = {
        'symbol': symbol,
        'name': info['name'],
        'type': info['type'],
        'price': None,
        'change': None,
        'change_pct': None,
        'prev_close': None,
        'sparkline': [],
    }
    try:
        ticker = yf.Ticker(symbol)

        # Get 5-day history for sparkline (1h intervals for smooth line)
        hist = ticker.history(period='5d', interval='1h')
        if hist is not None and not hist.empty:
            closes = hist['Close'].dropna().tolist()
            result['sparkline'] = [round(v, 4) for v in closes]

            current = closes[-1] if closes else None
            result['price'] = round(current, 2) if current else None

            # Get previous close for change calculation
            day_hist = ticker.history(period='2d', interval='1d')
            if day_hist is not None and len(day_hist) >= 2:
                prev = day_hist['Close'].iloc[-2]
                result['prev_close'] = round(prev, 2)
                if current and prev:
                    result['change'] = round(current - prev, 2)
                    result['change_pct'] = round(((current - prev) / prev) * 100, 2)
            elif current and len(closes) > 1:
                # Fallback: compare to first point in sparkline
                first = closes[0]
                result['prev_close'] = round(first, 2)
                result['change'] = round(current - first, 2)
                result['change_pct'] = round(((current - first) / first) * 100, 2)

    except Exception as e:
        logger.warning(f"Failed to fetch {symbol}: {e}")

    return result


def _fetch_all_markets():
    """Fetch all market data. Returns list of market dicts."""
    _ensure_yfinance()
    results = []
    for symbol in SYMBOLS:
        data = _fetch_single(symbol)
        results.append(data)

    # Compute term spread (10Y - 3M)
    tnx_data = next((r for r in results if r['symbol'] == '^TNX'), None)
    irx_data = next((r for r in results if r['symbol'] == '^IRX'), None)

    if tnx_data and irx_data and tnx_data['price'] is not None and irx_data['price'] is not None:
        spread_price = round(tnx_data['price'] - irx_data['price'], 3)
        spread_prev = None
        spread_change = None
        spread_change_pct = None

        if tnx_data['prev_close'] is not None and irx_data['prev_close'] is not None:
            spread_prev = round(tnx_data['prev_close'] - irx_data['prev_close'], 3)
            spread_change = round(spread_price - spread_prev, 3)
            if spread_prev != 0:
                spread_change_pct = round(((spread_price - spread_prev) / abs(spread_prev)) * 100, 2)

        # Build sparkline from aligned arrays
        spread_sparkline = []
        tnx_spark = tnx_data.get('sparkline', [])
        irx_spark = irx_data.get('sparkline', [])
        min_len = min(len(tnx_spark), len(irx_spark))
        for i in range(min_len):
            spread_sparkline.append(round(tnx_spark[i] - irx_spark[i], 4))

        results.append({
            'symbol': 'TERM_SPREAD',
            'name': 'Term Spread (10Y-3M)',
            'type': 'spread',
            'price': spread_price,
            'change': spread_change,
            'change_pct': spread_change_pct,
            'prev_close': spread_prev,
            'sparkline': spread_sparkline,
        })

    return {
        'markets': results,
        'updated_at': datetime.utcnow().isoformat(),
    }


def get_market_data():
    """Public API: returns cached market data."""
    return _cache.get()


# ─── Historical Data ─────────────────────────────────────────

HISTORY_PARAMS = {
    '1d':  {'period': '1d',  'interval': '5m'},
    '5d':  {'period': '5d',  'interval': '1h'},
    '1mo': {'period': '1mo', 'interval': '1d'},
    '1y':  {'period': '1y',  'interval': '1d'},
    '5y':  {'period': '5y',  'interval': '1wk'},
    '10y': {'period': '10y', 'interval': '1mo'},
}

HISTORY_CACHE_TTL = 300  # 5 minutes


class HistoryCache:
    """Thread-safe cache for historical price data, keyed by (symbol, period)."""

    def __init__(self):
        self._lock = threading.RLock()
        self._entries = {}

    def get(self, symbol, period):
        key = (symbol, period)
        with self._lock:
            entry = self._entries.get(key)
            if entry and (time.time() - entry['ts']) < HISTORY_CACHE_TTL:
                return entry['data']
        data = _fetch_history(symbol, period)
        if data:
            with self._lock:
                self._entries[key] = {'data': data, 'ts': time.time()}
        return data


_history_cache = HistoryCache()


def _fetch_history(symbol, period):
    """Fetch historical price data for a single symbol or TERM_SPREAD."""
    _ensure_yfinance()
    params = HISTORY_PARAMS.get(period)
    if not params:
        return None

    try:
        if symbol == 'TERM_SPREAD':
            return _fetch_spread_history(params)

        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=params['period'], interval=params['interval'])
        if hist is None or hist.empty:
            return None

        dates = [d.strftime('%Y-%m-%d %H:%M') if params['interval'] in ('5m', '1h')
                 else d.strftime('%Y-%m-%d')
                 for d in hist.index]
        closes = [round(v, 4) if v == v else None for v in hist['Close'].tolist()]

        meta = SYMBOLS.get(symbol, {'name': symbol, 'type': 'unknown'})
        return {
            'symbol': symbol,
            'name': meta['name'],
            'type': meta['type'],
            'period': period,
            'dates': dates,
            'closes': closes,
        }
    except Exception as e:
        logger.warning(f"Failed to fetch history for {symbol}/{period}: {e}")
        return None


def _fetch_spread_history(params):
    """Compute historical term spread (10Y - 3M) from aligned histories."""
    _ensure_yfinance()
    try:
        tnx = yf.Ticker('^TNX').history(period=params['period'], interval=params['interval'])
        irx = yf.Ticker('^IRX').history(period=params['period'], interval=params['interval'])

        if tnx is None or tnx.empty or irx is None or irx.empty:
            return None

        common = tnx.index.intersection(irx.index)
        if len(common) == 0:
            return None

        dates = [d.strftime('%Y-%m-%d %H:%M') if params['interval'] in ('5m', '1h')
                 else d.strftime('%Y-%m-%d')
                 for d in common]
        spreads = [round(tnx.loc[d, 'Close'] - irx.loc[d, 'Close'], 4) for d in common]

        return {
            'symbol': 'TERM_SPREAD',
            'name': 'Term Spread (10Y-3M)',
            'type': 'spread',
            'period': params['period'],
            'dates': dates,
            'closes': spreads,
        }
    except Exception as e:
        logger.warning(f"Failed to fetch spread history: {e}")
        return None


def get_market_history(symbol, period):
    """Public API: returns cached historical data for a symbol+period."""
    return _history_cache.get(symbol, period)
