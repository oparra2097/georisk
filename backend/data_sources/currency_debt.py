"""
External Debt Composition — World Bank IDS.

Fetches two complementary slices of WB International Debt Statistics for
each DRS-reporting country (~120 LMICs):

1. Currency composition of long-term PPG external debt (% share)
   Series: DT.CUR.{USDL,EURO,JYEN,UKPS,SWFR,SDRW,MULC,OTHC}.ZS
   This is the only debt category for which WB IDS publishes a currency
   breakdown. ST debt, total external debt, and domestic public debt do
   NOT have a published currency split in IDS.

2. External debt stocks by maturity (USD billions)
   Series: DT.DOD.DECT.CD (total external)
           DT.DOD.DLXF.CD (long-term external)
           DT.DOD.DSTC.CD (short-term external)
           DT.DOD.DPPG.CD (PPG long-term external)

All IDS series live in WB API source=6 and are dimensioned by
Series × Country × Counterpart-Area × Time. The counterpart-area
aggregate "World" is coded ``WLD`` (verified via the sources/6/
counterpart-area metadata endpoint — numeric codes like 907 are
specific creditors, not the totals aggregate).

Source-prefixed SDMX-style endpoint is required because IDS-only series
are invisible to the legacy WDI-style endpoint:
  /v2/sources/6/series/{CODE}/country/all/counterpart-area/WLD/time/all
"""

import threading
import time
from datetime import datetime

import requests

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 86400  # 24h

_WB_API = 'https://api.worldbank.org/v2'
_IDS_SOURCE = 6

# Currency composition (% of long-term PPG external debt)
_CURRENCY_SPEC = [
    ('DT.CUR.USDL.ZS', 'USD',   'US Dollar',           '#10b981'),
    ('DT.CUR.EURO.ZS', 'EUR',   'Euro',                '#3b82f6'),
    ('DT.CUR.JYEN.ZS', 'JPY',   'Japanese Yen',        '#f59e0b'),
    ('DT.CUR.UKPS.ZS', 'GBP',   'Pound Sterling',      '#ef4444'),
    ('DT.CUR.SWFR.ZS', 'CHF',   'Swiss Franc',         '#a855f7'),
    ('DT.CUR.SDRW.ZS', 'SDR',   'SDR',                 '#06b6d4'),
    ('DT.CUR.MULC.ZS', 'MULTI', 'Multiple Currencies', '#64748b'),
    ('DT.CUR.OTHC.ZS', 'OTHER', 'Other Currencies',    '#94a3b8'),
]

# External debt stocks by maturity (USD, current — divide by 1e9 for billions)
_STOCK_SPEC = [
    ('DT.DOD.DECT.CD', 'total_ext', 'Total External Debt',   '#3b82f6'),
    ('DT.DOD.DLXF.CD', 'lt_ext',    'Long-Term External',    '#10b981'),
    ('DT.DOD.DSTC.CD', 'st_ext',    'Short-Term External',   '#f59e0b'),
    ('DT.DOD.DPPG.CD', 'ppg_ext',   'PPG Long-Term External','#a855f7'),
]

_AGGREGATE_CODES = {
    'ARB', 'CEB', 'CSS', 'EAP', 'EAR', 'EAS', 'ECA', 'ECS', 'EMU', 'EUU',
    'FCS', 'HIC', 'HPC', 'IBD', 'IBT', 'IDA', 'IDB', 'IDX', 'INX', 'LAC',
    'LCN', 'LDC', 'LIC', 'LMC', 'LMY', 'LTE', 'MEA', 'MIC', 'MNA', 'NAC',
    'OED', 'OSS', 'PRE', 'PSS', 'PST', 'SAS', 'SSA', 'SSF', 'SST', 'TEA',
    'TEC', 'TLA', 'TMN', 'TSA', 'TSS', 'UMC', 'WLD',
}


def _fetch_ids_series_raw(indicator, counterpart='WLD'):
    """Fetch raw response from the WB IDS source-prefixed endpoint.

    Returns (url, status_code, payload_or_error_str).
    """
    url = (
        f'{_WB_API}/sources/{_IDS_SOURCE}/series/{indicator}'
        f'/country/all/counterpart-area/{counterpart}'
        f'/time/all?format=json&per_page=20000'
    )
    try:
        resp = requests.get(url, timeout=30)
        status = resp.status_code
        try:
            payload = resp.json()
        except Exception:
            payload = resp.text[:2000]
        return url, status, payload
    except Exception as e:
        return url, 0, f'ERROR: {e}'


def _extract_data_rows(payload):
    """Pull data rows out of a WB SDMX-JSON response (handles all variants)."""
    if payload is None:
        return []
    if isinstance(payload, dict):
        if isinstance(payload.get('data'), list):
            return payload['data']
        src = payload.get('source')
        if isinstance(src, dict) and isinstance(src.get('data'), list):
            return src['data']
        if isinstance(src, list) and src and isinstance(src[0], dict):
            inner = src[0].get('data')
            if isinstance(inner, list):
                return inner
        return []
    if isinstance(payload, list) and len(payload) >= 2:
        tail = payload[1]
        if isinstance(tail, list):
            return tail
        if isinstance(tail, dict) and isinstance(tail.get('data'), list):
            return tail['data']
    return []


def _parse_row(row):
    """Parse one SDMX data row → (iso3, name, year_str, value) or None."""
    if not isinstance(row, dict):
        return None
    val = row.get('value')
    if val is None or val == '':
        return None
    try:
        val = float(val)
    except (TypeError, ValueError):
        return None

    iso3, name, year_str = None, None, None
    for var in row.get('variable') or []:
        cid = (var.get('concept') or '').lower()
        vid = var.get('id') or ''
        vlabel = var.get('value') or ''
        if cid in ('country', 'economy', 'ref_area'):
            iso3 = vid
            name = vlabel or vid
        elif cid in ('time', 'year'):
            year_str = vid[2:] if vid.startswith('YR') else vid

    if iso3 is None and 'countryiso3code' in row:
        iso3 = row.get('countryiso3code') or None
        c = row.get('country')
        name = c.get('value') if isinstance(c, dict) else None
    if year_str is None and 'date' in row:
        year_str = row.get('date')

    if not iso3 or iso3 in _AGGREGATE_CODES or not year_str:
        return None
    return (iso3, name or iso3, year_str, val)


def _fetch_ids_series(indicator):
    """Fetch one IDS series → {iso3: {name, values: {year: float}}}."""
    url, status, payload = _fetch_ids_series_raw(indicator)
    if status != 200:
        print(f'[CurrencyDebt] {indicator}: HTTP {status}')
        return {}
    rows = _extract_data_rows(payload)
    countries = {}
    for row in rows:
        parsed = _parse_row(row)
        if not parsed:
            continue
        iso3, name, year_str, val = parsed
        entry = countries.setdefault(iso3, {'name': name, 'values': {}})
        entry['values'][year_str] = val
    print(f'[CurrencyDebt] {indicator}: {len(countries)} countries, '
          f'{sum(len(c["values"]) for c in countries.values())} obs')
    return countries


def _build_currency_per_country(per_currency):
    """Merge per-currency series into {iso3: {name, latest_year, latest, history}}."""
    countries = {}
    all_years = set()
    for _ind, key, _l, _c in _CURRENCY_SPEC:
        wb = per_currency.get(key) or {}
        for iso3, cdata in wb.items():
            entry = countries.setdefault(iso3, {
                'name': cdata.get('name', iso3),
                'history': {},
            })
            for year_str, val in (cdata.get('values') or {}).items():
                all_years.add(year_str)
                entry['history'].setdefault(year_str, {})[key] = val

    keep = {}
    for iso3, entry in countries.items():
        history = entry['history']
        if not history:
            continue
        years_with_data = sorted(
            (y for y, vals in history.items() if vals),
            reverse=True,
        )
        if not years_with_data:
            continue
        latest_year = years_with_data[0]
        latest = dict(history[latest_year])
        for _i, key, _l, _c in _CURRENCY_SPEC:
            latest.setdefault(key, None)
        keep[iso3] = {
            'name': entry['name'],
            'latest_year': latest_year,
            'latest': latest,
            'history': history,
        }
    return keep, sorted(all_years)


def _build_stocks_per_country(per_stock):
    """Merge per-stock series into {iso3: {name, latest_year, latest, history}}.

    Values are in USD; convert to billions for display friendliness.
    """
    countries = {}
    all_years = set()
    for _ind, key, _l, _c in _STOCK_SPEC:
        wb = per_stock.get(key) or {}
        for iso3, cdata in wb.items():
            entry = countries.setdefault(iso3, {
                'name': cdata.get('name', iso3),
                'history': {},
            })
            for year_str, val in (cdata.get('values') or {}).items():
                # WB IDS publishes stocks in USD; convert to $B for display.
                val_b = val / 1e9
                all_years.add(year_str)
                entry['history'].setdefault(year_str, {})[key] = val_b

    keep = {}
    for iso3, entry in countries.items():
        history = entry['history']
        if not history:
            continue
        years_with_data = sorted(
            (y for y, vals in history.items() if vals),
            reverse=True,
        )
        if not years_with_data:
            continue
        latest_year = years_with_data[0]
        latest = dict(history[latest_year])
        for _i, key, _l, _c in _STOCK_SPEC:
            latest.setdefault(key, None)
        keep[iso3] = {
            'name': entry['name'],
            'latest_year': latest_year,
            'latest': latest,
            'history': history,
        }
    return keep, sorted(all_years)


def get_currency_debt():
    """Return currency composition + debt stocks for DRS-reporting countries."""
    cache_key = 'currency_debt'
    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    per_currency = {key: _fetch_ids_series(ind)
                    for ind, key, _l, _c in _CURRENCY_SPEC}
    per_stock = {key: _fetch_ids_series(ind)
                 for ind, key, _l, _c in _STOCK_SPEC}

    countries, ccy_years = _build_currency_per_country(per_currency)
    stocks, stock_years = _build_stocks_per_country(per_stock)

    # Union of country names across currency + stock data
    all_countries = {}
    for iso3, c in countries.items():
        all_countries[iso3] = c.get('name', iso3)
    for iso3, c in stocks.items():
        all_countries.setdefault(iso3, c.get('name', iso3))

    all_years = sorted(set(ccy_years) | set(stock_years))

    data = {
        'currencies': [
            {'key': key, 'label': label, 'color': color}
            for _i, key, label, color in _CURRENCY_SPEC
        ],
        'stock_series': [
            {'key': key, 'label': label, 'color': color}
            for _i, key, label, color in _STOCK_SPEC
        ],
        'countries': countries,    # currency composition (LT PPG only)
        'stocks': stocks,          # debt levels by maturity ($B)
        'country_names': all_countries,
        'years': all_years,
        'meta': {
            'source': 'World Bank · International Debt Statistics',
            'description': (
                'Currency composition of long-term PPG external debt (% share) '
                'and external debt stocks by maturity ($B). Coverage: '
                'DRS-reporting low/middle-income countries. Note: WB IDS '
                'publishes a currency breakdown only for long-term PPG; '
                'short-term, total, and domestic public debt do not have a '
                'currency split in this dataset.'
            ),
            'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
            'country_count': len(all_countries),
            'currency_country_count': len(countries),
            'stock_country_count': len(stocks),
            'year_range': f'{all_years[0]}–{all_years[-1]}' if all_years else '',
        },
    }

    with _cache_lock:
        _cache[cache_key] = {'data': data, 'ts': time.time()}
    return data


def clear_cache():
    with _cache_lock:
        _cache.clear()
