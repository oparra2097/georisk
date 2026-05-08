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


def _fetch_ids_series(indicator):
    """Fetch a single IDS series via the source-prefixed endpoint.

    Returns ``{iso3: {name, values: {year_str: float}}}`` or ``{}`` on error.
    """
    url = (
        f'{_WB_API}/sources/{_IDS_SOURCE}/series/{indicator}'
        f'/country/all/time/all?format=json&per_page=20000'
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        print(f'[CurrencyDebt] fetch error {indicator}: {e}')
        return {}

    rows = (payload or {}).get('data') or []
    countries = {}
    for row in rows:
        val = row.get('value')
        if val is None or val == '':
            continue
        try:
            val = float(val)
        except (TypeError, ValueError):
            continue

        iso3, name, year_str = None, None, None
        for var in row.get('variable', []) or []:
            cid = (var.get('concept') or '').lower()
            vid = var.get('id') or ''
            vlabel = var.get('value') or ''
            if cid == 'country' or cid == 'economy':
                iso3 = vid
                name = vlabel or vid
            elif cid == 'time':
                # WB time IDs are like "YR2023" — strip the YR prefix.
                year_str = vid[2:] if vid.startswith('YR') else vid

        if not iso3 or iso3 in _AGGREGATE_CODES or not year_str:
            continue

        entry = countries.setdefault(iso3, {'name': name or iso3, 'values': {}})
        entry['values'][year_str] = val

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
