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
    '^GSPC':    {'name': 'S&P 500',        'type': 'index'},
    '^IXIC':    {'name': 'NASDAQ',          'type': 'index'},
    '^N225':    {'name': 'Nikkei 225',      'type': 'index'},
    'GC=F':     {'name': 'Gold',            'type': 'commodity'},
    'CL=F':     {'name': 'WTI Crude Oil',   'type': 'commodity'},
    'NG=F':     {'name': 'Natural Gas',     'type': 'commodity'},
    '^TNX':     {'name': 'US 10Y Treasury', 'type': 'bond'},
    '^FVX':     {'name': 'US 5Y Treasury',  'type': 'bond'},
    'EURUSD=X': {'name': 'EUR/USD',         'type': 'fx'},
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
    return {
        'markets': results,
        'updated_at': datetime.utcnow().isoformat(),
    }


def get_market_data():
    """Public API: returns cached market data."""
    return _cache.get()
