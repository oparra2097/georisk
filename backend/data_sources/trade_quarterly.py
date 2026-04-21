"""
US Quarterly Trade Data — Exports, Imports, and Net Exports from FRED.

Pulls together the BEA NIPA quarterly series on FRED (requires FRED_API_KEY):

  EXPGS             Exports of Goods and Services (Billions $, SAAR)
  IMPGS             Imports of Goods and Services (Billions $, SAAR)
  NETEXP            Net Exports of Goods and Services (Billions $, SAAR)
  B020RE1Q156NBEA   Exports of G&S as % of GDP
  B021RE1Q156NBEA   Imports of G&S as % of GDP

Output shape matches the other trade endpoints — a single JSON blob the
frontend can render without extra fetches. Quarterly points are labelled
YYYY-Qn (e.g. "2024-Q3") so they sort chronologically alongside monthly
data elsewhere on the page.
"""

import threading
import time
from datetime import datetime

from backend.data_sources.fred_client import fetch_series

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 21600  # 6h — matches FRED client, NIPA updates quarterly

# Series spec: (FRED id, internal key, human label, unit, y-axis group)
_SERIES = [
    ('EXPGS',             'exports',       'Exports of Goods & Services', '$B (SAAR)', 'level'),
    ('IMPGS',             'imports',       'Imports of Goods & Services', '$B (SAAR)', 'level'),
    ('NETEXP',            'net_exports',   'Net Exports',                 '$B (SAAR)', 'level'),
    ('B020RE1Q156NBEA',   'exports_pct',   'Exports (% of GDP)',          '% of GDP',  'pct'),
    ('B021RE1Q156NBEA',   'imports_pct',   'Imports (% of GDP)',          '% of GDP',  'pct'),
]


def _month_to_quarter(month):
    return (int(month) - 1) // 3 + 1


def _quarter_label(date_str):
    """FRED returns the first day of each quarter (YYYY-01-01, -04-01, etc.).
    Convert to YYYY-Qn."""
    y, m, _ = date_str.split('-')
    return f'{y}-Q{_month_to_quarter(m)}'


def _fetch_one(series_id, start_date='1970-01-01'):
    obs = fetch_series(series_id, start_date=start_date)
    out = []
    for row in obs:
        date = row.get('date')
        val = row.get('value')
        if not date or val is None:
            continue
        try:
            label = _quarter_label(date)
            y, qstr = label.split('-Q')
            out.append({
                'date': label,
                'year': int(y),
                'quarter': int(qstr),
                'value': float(val),
            })
        except (ValueError, TypeError):
            continue
    return out


def get_us_trade_quarterly():
    """Return US quarterly trade data (FRED, cached 6h)."""
    cache_key = 'us_trade_q'
    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    series = {}
    latest_date = None
    for fred_id, key, label, unit, group in _SERIES:
        points = _fetch_one(fred_id)
        series[key] = {
            'fred_id': fred_id,
            'label': label,
            'unit': unit,
            'group': group,
            'points': points,
        }
        if points:
            last = points[-1]['date']
            if latest_date is None or last > latest_date:
                latest_date = last

    data = {
        'series': series,
        'meta': {
            'source': 'FRED / BEA NIPA',
            'description': 'US quarterly trade — Bureau of Economic Analysis, National Income and Product Accounts',
            'latest_quarter': latest_date,
            'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
            'series_count': sum(1 for s in series.values() if s['points']),
        }
    }

    with _cache_lock:
        _cache[cache_key] = {'data': data, 'ts': time.time()}
    return data


def clear_cache():
    with _cache_lock:
        _cache.clear()
