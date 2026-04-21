"""
World Bank — data fetcher with 24-hour cache.
Fetches indicator data (e.g. NE.EXP.GNFS.ZS = exports % GDP) for all countries.
Uses the World Bank API v2 (no API key required).
"""

import time
import threading
import requests

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 86400  # 24 hours

_WB_API = 'https://api.worldbank.org/v2'

# Transient upstream failures are common on api.worldbank.org; retry before
# giving up so a single 502 doesn't wipe the chart on cold start.
_RETRY_BACKOFFS = (1, 3, 9)  # seconds between attempts 1→2, 2→3, 3→4
_RETRY_STATUS = {500, 502, 503, 504}

# Aggregate / regional codes to exclude from country-level data
_AGGREGATE_CODES = {
    'ARB', 'CEB', 'CSS', 'EAP', 'EAR', 'EAS', 'ECA', 'ECS', 'EMU', 'EUU',
    'FCS', 'HIC', 'HPC', 'IBD', 'IBT', 'IDA', 'IDB', 'IDX', 'INX', 'LAC',
    'LCN', 'LDC', 'LIC', 'LMC', 'LMY', 'LTE', 'MEA', 'MIC', 'MNA', 'NAC',
    'OED', 'OSS', 'PRE', 'PSS', 'PST', 'SAS', 'SSA', 'SSF', 'SST', 'TEA',
    'TEC', 'TLA', 'TMN', 'TSA', 'TSS', 'UMC', 'WLD',
}


def get_wb_data(indicator):
    """Fetch World Bank data for the given indicator. Returns cached if fresh."""
    cache_key = f'wb_{indicator}'

    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    data = _fetch_wb(indicator)

    # If the fetch failed but we have a prior successful payload, serve stale
    # rather than wiping the chart. Only overwrite cache on success.
    if (data.get('meta') or {}).get('error') and not data.get('countries'):
        with _cache_lock:
            entry = _cache.get(cache_key)
        if entry and entry['data'].get('countries'):
            stale = dict(entry['data'])
            stale_meta = dict(stale.get('meta') or {})
            stale_meta['stale'] = True
            stale_meta['stale_age_s'] = int(time.time() - entry['ts'])
            stale_meta['error'] = data['meta'].get('error')
            stale['meta'] = stale_meta
            return stale
        return data

    with _cache_lock:
        _cache[cache_key] = {'data': data, 'ts': time.time()}

    return data


def _get_with_retry(url):
    """GET with retries on 5xx and network errors; raises on final failure."""
    last_exc = None
    for attempt, backoff in enumerate((0,) + _RETRY_BACKOFFS):
        if backoff:
            time.sleep(backoff)
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code in _RETRY_STATUS:
                last_exc = requests.HTTPError(
                    f'{resp.status_code} Server Error for url: {url}'
                )
                print(
                    f'[WorldBank] retry {attempt + 1}/{len(_RETRY_BACKOFFS) + 1} '
                    f'after HTTP {resp.status_code}: {indicator_from_url(url)}'
                )
                continue
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            print(
                f'[WorldBank] retry {attempt + 1}/{len(_RETRY_BACKOFFS) + 1} '
                f'after {type(e).__name__}: {indicator_from_url(url)}'
            )
            continue
    raise last_exc if last_exc else RuntimeError('fetch failed')


def indicator_from_url(url):
    """Extract the indicator code from a WB URL for log readability."""
    marker = '/indicator/'
    i = url.find(marker)
    if i < 0:
        return url
    tail = url[i + len(marker):]
    q = tail.find('?')
    return tail[:q] if q >= 0 else tail


def _fetch_wb(indicator):
    """Fetch and parse World Bank data from API v2."""
    try:
        url = (
            f'{_WB_API}/country/all/indicator/{indicator}'
            f'?format=json&per_page=20000&date=2000:2025'
        )
        resp = _get_with_retry(url)
        raw = resp.json()

        # Response is [metadata, data_array]
        if not isinstance(raw, list) or len(raw) < 2:
            raise ValueError('Unexpected API response format')

        meta_info = raw[0]
        records = raw[1] or []

        # Parse into {iso3: {name, values: {year_str: value}}}
        all_years = set()
        countries = {}

        for rec in records:
            iso3 = rec.get('countryiso3code', '')
            if not iso3 or iso3 in _AGGREGATE_CODES:
                continue

            val = rec.get('value')
            year_str = rec.get('date', '')
            country_name = rec.get('country', {}).get('value', iso3)

            if val is None or year_str == '':
                continue

            try:
                year = int(year_str)
                val_float = float(val)
            except (ValueError, TypeError):
                continue

            if iso3 not in countries:
                countries[iso3] = {
                    'name': country_name,
                    'values': {},
                }

            countries[iso3]['values'][year_str] = val_float
            all_years.add(year)

        years = sorted(all_years)

        indicator_name = _INDICATOR_NAMES.get(indicator, indicator)
        # Try to get name from the first record if available
        if records and not indicator_name:
            indicator_name = records[0].get('indicator', {}).get('value', indicator)

        return {
            'countries': countries,
            'years': years,
            'forecast_start_year': None,  # World Bank has no forecasts
            'meta': {
                'source': 'World Bank',
                'indicator': indicator,
                'indicator_name': indicator_name,
                'last_updated': meta_info.get('lastupdated', time.strftime('%Y-%m-%d')),
                'country_count': len(countries),
            }
        }

    except Exception as e:
        print(f'[WorldBank] Error fetching {indicator}: {e}')
        return {
            'countries': {},
            'years': [],
            'forecast_start_year': None,
            'meta': {
                'source': 'World Bank',
                'indicator': indicator,
                'error': str(e),
            }
        }


_INDICATOR_NAMES = {
    'NE.EXP.GNFS.ZS': 'Exports of Goods & Services (% of GDP)',
    'NE.IMP.GNFS.ZS': 'Imports of Goods & Services (% of GDP)',
    'NE.EXP.GNFS.CD': 'Exports of Goods & Services (Current US$)',
    'NE.IMP.GNFS.CD': 'Imports of Goods & Services (Current US$)',
    'NE.TRD.GNFS.ZS': 'Trade (% of GDP)',
    'NE.RSB.GNFS.ZS': 'External Balance on Goods & Services (% of GDP)',
    'BX.KLT.DINV.WD.GD.ZS': 'FDI Net Inflows (% of GDP)',
    'BX.KLT.DINV.CD.WD': 'FDI Net Inflows (Current US$)',
    'TX.VAL.MRCH.CD.WT': 'Merchandise Exports (Current US$)',
    'TM.VAL.MRCH.CD.WT': 'Merchandise Imports (Current US$)',
}
