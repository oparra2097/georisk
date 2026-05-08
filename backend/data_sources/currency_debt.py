"""
Currency Composition of External Debt — World Bank IDS.

Fetches the WB International Debt Statistics "Currency composition of long-term
debt" group, which gives the share (% of long-term external debt) held in each
of seven currency buckets for ~120 low- and middle-income countries that
report to the WB Debtor Reporting System (DRS).

Indicators (all DT.CUR.*.ZS, unit = % of long-term debt):
  USDL  U.S. dollars
  EURO  Euros
  JYEN  Japanese yen
  SWFR  Swiss francs
  UKPS  Pound sterling
  SDRW  Special Drawing Rights (SDR)
  MULC  Multiple currencies
  OTHC  All other currencies

Data lags by ~2 years (typical WB IDS cadence). Annual back to ~1970.
"""

import threading
import time
from datetime import datetime

from backend.data_sources.world_bank import get_wb_data

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 86400  # 24h, matches world_bank.py

# (WB indicator code, internal key, display label, hex color)
_CURRENCY_SPEC = [
    ('DT.CUR.USDL.ZS', 'USD',   'US Dollar',          '#10b981'),
    ('DT.CUR.EURO.ZS', 'EUR',   'Euro',               '#3b82f6'),
    ('DT.CUR.JYEN.ZS', 'JPY',   'Japanese Yen',       '#f59e0b'),
    ('DT.CUR.UKPS.ZS', 'GBP',   'Pound Sterling',     '#ef4444'),
    ('DT.CUR.SWFR.ZS', 'CHF',   'Swiss Franc',        '#a855f7'),
    ('DT.CUR.SDRW.ZS', 'SDR',   'SDR',                '#06b6d4'),
    ('DT.CUR.MULC.ZS', 'MULTI', 'Multiple Currencies', '#64748b'),
    ('DT.CUR.OTHC.ZS', 'OTHER', 'Other Currencies',   '#94a3b8'),
]


def get_currency_debt():
    """Return per-country currency composition of long-term external debt.

    Shape:
      {
        'currencies': [{key, label, color}, ...],
        'countries': {
            iso3: {
                'name': str,
                'latest_year': str,
                'latest': {USD: pct, EUR: pct, ...},
                'history': {year_str: {USD: pct, ...}},
            }
        },
        'years': [sorted year strings],
        'meta': {...},
      }
    """
    cache_key = 'currency_debt'
    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    # Pull each currency-share indicator. Each call hits the world_bank.py
    # 24h cache, so subsequent calls are free.
    per_currency = {}  # {key: WB-data dict}
    for indicator, key, _label, _color in _CURRENCY_SPEC:
        per_currency[key] = get_wb_data(indicator)

    # Merge into per-country, per-year structure
    countries = {}
    all_years = set()

    for indicator, key, _label, _color in _CURRENCY_SPEC:
        wb = per_currency.get(key, {}) or {}
        wb_countries = wb.get('countries', {}) or {}
        for iso3, cdata in wb_countries.items():
            name = cdata.get('name', iso3)
            values = cdata.get('values', {}) or {}
            if not values:
                continue
            entry = countries.setdefault(iso3, {
                'name': name,
                'history': {},
            })
            entry['name'] = name  # keep last seen, names match across indicators
            for year_str, val in values.items():
                if val is None:
                    continue
                all_years.add(year_str)
                entry['history'].setdefault(year_str, {})[key] = val

    # Compute latest_year per country (greatest year with USD share — USD
    # is reported by every DRS country) and snapshot of latest values.
    keep = {}
    for iso3, entry in countries.items():
        history = entry['history']
        if not history:
            continue
        # Latest year with at least one currency value
        years_with_data = sorted(
            (y for y, vals in history.items() if vals),
            reverse=True,
        )
        if not years_with_data:
            continue
        latest_year = years_with_data[0]
        latest = dict(history[latest_year])
        # Backfill any missing currency keys at 0 (so stacks add up cleanly)
        for _i, key, _l, _c in _CURRENCY_SPEC:
            latest.setdefault(key, None)
        keep[iso3] = {
            'name': entry['name'],
            'latest_year': latest_year,
            'latest': latest,
            'history': history,
        }

    years = sorted(all_years)

    data = {
        'currencies': [
            {'key': key, 'label': label, 'color': color}
            for _i, key, label, color in _CURRENCY_SPEC
        ],
        'countries': keep,
        'years': years,
        'meta': {
            'source': 'World Bank · International Debt Statistics',
            'description': 'Currency composition of long-term external debt (% of total long-term debt). Coverage: DRS-reporting low/middle-income countries.',
            'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
            'country_count': len(keep),
            'year_range': f'{years[0]}–{years[-1]}' if years else '',
        },
    }

    with _cache_lock:
        _cache[cache_key] = {'data': data, 'ts': time.time()}
    return data


def clear_cache():
    with _cache_lock:
        _cache.clear()
