"""
Currency Composition of External Debt — World Bank IDS.

Fetches the WB International Debt Statistics "Currency composition of PPG debt"
group: % share of long-term, public-and-publicly-guaranteed external debt held
in each of seven currency buckets, for ~120 low/middle-income countries that
report to the WB Debtor Reporting System (DRS).

Indicators (all DT.CUR.*.ZS, unit = % of long-term PPG debt):
  USDL  U.S. dollars
  EURO  Euros
  JYEN  Japanese yen
  UKPS  Pound sterling
  SWFR  Swiss francs
  SDRW  Special Drawing Rights
  MULC  Multiple currencies
  OTHC  All other currencies

Verified codes via WB DataBank metadata + ONEcampaign/bblocks IDS importer
(2024 data still flowing). Annual back to ~1970, lags publication by ~2 years.

These are IDS-only series — they're invisible to the standard WDI-style
endpoint (`/v2/country/all/indicator/{code}?source=6`), which silently
returns an empty data array. We have to use the source-prefixed SDMX-style
endpoint (`/v2/sources/6/series/{code}/country/all/time/all`) instead, which
is what wbgapi uses internally.
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

# (WB indicator code, internal key, display label, hex color)
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

# IDS reports country-level aggregates we don't want plotted alongside actual
# countries. Standard WB aggregate codes:
_AGGREGATE_CODES = {
    'ARB', 'CEB', 'CSS', 'EAP', 'EAR', 'EAS', 'ECA', 'ECS', 'EMU', 'EUU',
    'FCS', 'HIC', 'HPC', 'IBD', 'IBT', 'IDA', 'IDB', 'IDX', 'INX', 'LAC',
    'LCN', 'LDC', 'LIC', 'LMC', 'LMY', 'LTE', 'MEA', 'MIC', 'MNA', 'NAC',
    'OED', 'OSS', 'PRE', 'PSS', 'PST', 'SAS', 'SSA', 'SSF', 'SST', 'TEA',
    'TEC', 'TLA', 'TMN', 'TSA', 'TSS', 'UMC', 'WLD',
}


def _fetch_ids_series_raw(indicator):
    """Fetch raw response from the WB IDS source-prefixed endpoint.

    Returns ``(url, status_code, payload_or_error_str)`` for diagnostics.
    """
    url = (
        f'{_WB_API}/sources/{_IDS_SOURCE}/series/{indicator}'
        f'/country/all/time/all?format=json&per_page=20000'
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
    """Pull the list of data rows out of a WB SDMX-JSON response.

    The source-prefixed endpoint can return any of these shapes:
      - {'data': [...]}                                        (single dict)
      - [{...meta...}, {'data': [...]}]                        (list wrapper)
      - [{...meta...}, [...rows...]]                           (legacy WDI-ish)
      - {'source': [{'data': [...]}], ...}                     (deeply nested)
    """
    if payload is None:
        return []
    if isinstance(payload, dict):
        if isinstance(payload.get('data'), list):
            return payload['data']
        # Some responses nest data under source[0].data
        src = payload.get('source')
        if isinstance(src, list) and src and isinstance(src[0], dict):
            inner = src[0].get('data')
            if isinstance(inner, list):
                return inner
        # Or under page wrappers
        return []
    if isinstance(payload, list):
        if len(payload) >= 2:
            tail = payload[1]
            if isinstance(tail, list):
                return tail
            if isinstance(tail, dict) and isinstance(tail.get('data'), list):
                return tail['data']
    return []


def _parse_row(row):
    """Parse one SDMX data row into (iso3, name, year_str, value) or None."""
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
    # Source-prefixed shape: variable=[{concept, id, value}, ...]
    for var in row.get('variable') or []:
        cid = (var.get('concept') or '').lower()
        vid = var.get('id') or ''
        vlabel = var.get('value') or ''
        if cid in ('country', 'economy', 'ref_area'):
            iso3 = vid
            name = vlabel or vid
        elif cid in ('time', 'year'):
            year_str = vid[2:] if vid.startswith('YR') else vid

    # Fallback: legacy WDI shape with flat fields
    if iso3 is None and 'countryiso3code' in row:
        iso3 = row.get('countryiso3code') or None
        name = (row.get('country') or {}).get('value') if isinstance(row.get('country'), dict) else None
    if year_str is None and 'date' in row:
        year_str = row.get('date')

    if not iso3 or iso3 in _AGGREGATE_CODES or not year_str:
        return None
    return (iso3, name or iso3, year_str, val)


def _fetch_ids_series(indicator):
    """Fetch a single IDS series via the source-prefixed endpoint.

    Returns ``{iso3: {name, values: {year_str: float}}}`` or ``{}`` on error.
    """
    url, status, payload = _fetch_ids_series_raw(indicator)
    if status != 200:
        print(f'[CurrencyDebt] {indicator}: HTTP {status} from {url}')
        return {}

    rows = _extract_data_rows(payload)
    if not rows:
        # Diagnostic: log payload shape so we can see what came back.
        if isinstance(payload, dict):
            keys = list(payload.keys())
            print(f'[CurrencyDebt] {indicator}: empty data, payload dict keys={keys}')
        elif isinstance(payload, list):
            shapes = [type(x).__name__ for x in payload[:3]]
            print(f'[CurrencyDebt] {indicator}: empty data, payload list shapes={shapes}')
        else:
            print(f'[CurrencyDebt] {indicator}: empty data, payload type={type(payload).__name__}')
        return {}

    countries = {}
    for row in rows:
        parsed = _parse_row(row)
        if not parsed:
            continue
        iso3, name, year_str, val = parsed
        entry = countries.setdefault(iso3, {'name': name, 'values': {}})
        entry['values'][year_str] = val
    print(f'[CurrencyDebt] {indicator}: {len(countries)} countries, {sum(len(c["values"]) for c in countries.values())} obs')
    return countries

    return countries


def get_currency_debt():
    """Return per-country currency composition of long-term external debt."""
    cache_key = 'currency_debt'
    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    per_currency = {key: _fetch_ids_series(indicator)
                    for indicator, key, _l, _c in _CURRENCY_SPEC}

    # Merge into per-country, per-year structure
    countries = {}
    all_years = set()
    for _ind, key, _l, _c in _CURRENCY_SPEC:
        wb_countries = per_currency.get(key) or {}
        for iso3, cdata in wb_countries.items():
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
            'description': 'Currency composition of PPG long-term external debt (% share). Coverage: DRS-reporting low/middle-income countries.',
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
