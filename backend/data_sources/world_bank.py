"""
World Bank — data fetcher with 24-hour cache.
Fetches indicator data (e.g. NE.EXP.GNFS.ZS = exports % GDP) for all countries.
Uses the World Bank API v2 (no API key required).
"""

import json
import os
import time
import threading
import requests

from config import Config

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 86400  # 24 hours

_WB_API = 'https://api.worldbank.org/v2'

# Transient upstream failures are common on api.worldbank.org; retry before
# giving up so a single 502 doesn't wipe the chart on cold start.
_RETRY_BACKOFFS = (1, 3, 9)  # seconds between attempts 1→2, 2→3, 3→4
_RETRY_STATUS = {500, 502, 503, 504}

# Disk-backed cache survives restarts and sustained WB outages. Once any
# successful fetch lands here, /api/em-vulnerability stays healthy for days
# even if api.worldbank.org is down on cold start.
_DISK_CACHE_DIR = os.path.join(Config.DATA_DIR, 'wb_cache')


def _disk_path(indicator):
    safe = indicator.replace('/', '_')
    return os.path.join(_DISK_CACHE_DIR, f'{safe}.json')


def _load_from_disk(indicator):
    """Return (data, ts) from disk cache or (None, 0) if absent/corrupt."""
    path = _disk_path(indicator)
    if not os.path.exists(path):
        return None, 0
    try:
        with open(path, 'r') as f:
            wrapper = json.load(f)
        return wrapper.get('data'), float(wrapper.get('ts', 0))
    except (OSError, ValueError, json.JSONDecodeError) as e:
        print(f'[WorldBank] disk cache read failed for {indicator}: {e}')
        return None, 0


def _save_to_disk(indicator, data, ts):
    try:
        os.makedirs(_DISK_CACHE_DIR, exist_ok=True)
        path = _disk_path(indicator)
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'ts': ts, 'data': data}, f)
        os.replace(tmp, path)
    except OSError as e:
        print(f'[WorldBank] disk cache write failed for {indicator}: {e}')

# Aggregate / regional codes to exclude from country-level data
_AGGREGATE_CODES = {
    'ARB', 'CEB', 'CSS', 'EAP', 'EAR', 'EAS', 'ECA', 'ECS', 'EMU', 'EUU',
    'FCS', 'HIC', 'HPC', 'IBD', 'IBT', 'IDA', 'IDB', 'IDX', 'INX', 'LAC',
    'LCN', 'LDC', 'LIC', 'LMC', 'LMY', 'LTE', 'MEA', 'MIC', 'MNA', 'NAC',
    'OED', 'OSS', 'PRE', 'PSS', 'PST', 'SAS', 'SSA', 'SSF', 'SST', 'TEA',
    'TEC', 'TLA', 'TMN', 'TSA', 'TSS', 'UMC', 'WLD',
}


def get_wb_data(indicator, source=None):
    """Fetch World Bank data for the given indicator. Returns cached if fresh.

    ``source`` optionally overrides the WB API ``source`` parameter. Default
    is WDI (source=2). Useful sources for this codebase:
      - 22 = Quarterly External Debt Statistics / SDDS
      - 23 = Quarterly External Debt Statistics / GDDS
      - 6  = International Debt Statistics (IDS)
    """
    cache_key = f'wb_{indicator}' + (f'_src{source}' if source else '')

    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    # In-memory miss: try disk before going to the network. Render runs
    # multiple gunicorn workers, so the disk file may have been written by a
    # sibling worker, and it survives restarts/redeploys.
    disk_key = f'{indicator}' + (f'_src{source}' if source else '')
    disk_data, disk_ts = _load_from_disk(disk_key)
    if disk_data and disk_data.get('countries') and (time.time() - disk_ts) < _CACHE_TTL:
        with _cache_lock:
            _cache[cache_key] = {'data': disk_data, 'ts': disk_ts}
        return disk_data

    data = _fetch_wb(indicator, source=source)

    # If the fetch failed but we have a prior successful payload (memory or
    # disk), serve stale rather than wiping the chart. Only overwrite cache
    # on success.
    if (data.get('meta') or {}).get('error') and not data.get('countries'):
        fallback, fallback_ts = None, 0
        with _cache_lock:
            entry = _cache.get(cache_key)
        if entry and entry['data'].get('countries'):
            fallback, fallback_ts = entry['data'], entry['ts']
        elif disk_data and disk_data.get('countries'):
            fallback, fallback_ts = disk_data, disk_ts
        if fallback:
            stale = dict(fallback)
            stale_meta = dict(stale.get('meta') or {})
            stale_meta['stale'] = True
            stale_meta['stale_age_s'] = int(time.time() - fallback_ts)
            stale_meta['error'] = data['meta'].get('error')
            stale['meta'] = stale_meta
            return stale
        return data

    now = time.time()
    with _cache_lock:
        _cache[cache_key] = {'data': data, 'ts': now}
    _save_to_disk(disk_key, data, now)

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


def _fetch_wb(indicator, source=None):
    """Fetch and parse World Bank data from API v2."""
    try:
        # Non-WDI sources (QEDS=22, IDS=6) are often quarterly — annual-style
        # date ranges ("2000:2025") return empty against them. mrv=N grabs the
        # most-recent-N values regardless of frequency, which is what we want
        # for a "latest snapshot" chart.
        if source:
            url = (
                f'{_WB_API}/country/all/indicator/{indicator}'
                f'?format=json&per_page=20000&mrv=20&source={source}'
            )
        else:
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

            # Handle both annual ("2024") and quarterly ("2024Q3") labels —
            # QEDS/IDS sources return quarterly, WDI returns annual. Year
            # used only for the aggregate 'years' list; period key stays
            # as-is so callers can tell annual from quarterly.
            try:
                year = int(year_str[:4])
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
