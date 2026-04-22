"""
IMF International Financial Statistics (IFS) — quarterly indicator fetcher.

Serves as the "more-current-than-annual-WB" data layer for the EM External
Vulnerability chart. Pulls IFS series via DBnomics (a reliable public mirror
that backs several other fetchers in this codebase) because IMF's native
SDMX endpoints are slower and less stable.

Single-entry API: ``get_ifs_data(indicator, freq='Q')`` → dict shaped like
world_bank's output so the two can be used interchangeably upstream.
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

_DBNOMICS_BASE = 'https://api.db.nomics.world/v22'
_IFS_DATASET = 'IMF/IFS'

_RETRY_BACKOFFS = (1, 3, 9)
_RETRY_STATUS = {500, 502, 503, 504}

_DISK_CACHE_DIR = os.path.join(Config.DATA_DIR, 'ifs_cache')


def _disk_path(indicator, freq):
    safe = indicator.replace('/', '_')
    return os.path.join(_DISK_CACHE_DIR, f'{safe}_{freq}.json')


def _load_from_disk(indicator, freq):
    path = _disk_path(indicator, freq)
    if not os.path.exists(path):
        return None, 0
    try:
        with open(path, 'r') as f:
            wrapper = json.load(f)
        return wrapper.get('data'), float(wrapper.get('ts', 0))
    except (OSError, ValueError, json.JSONDecodeError) as e:
        print(f'[IFS] disk cache read failed for {indicator}/{freq}: {e}')
        return None, 0


def _save_to_disk(indicator, freq, data, ts):
    try:
        os.makedirs(_DISK_CACHE_DIR, exist_ok=True)
        path = _disk_path(indicator, freq)
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'ts': ts, 'data': data}, f)
        os.replace(tmp, path)
    except OSError as e:
        print(f'[IFS] disk cache write failed for {indicator}/{freq}: {e}')


def get_ifs_data(indicator, freq='Q'):
    """Return IFS data for one indicator at the given frequency.

    Shape mirrors backend.data_sources.world_bank.get_wb_data so callers can
    swap the two. ``countries`` is keyed by ISO-3 with values mapping period
    strings (e.g. "2025Q3", "2025-03") to floats.
    """
    cache_key = f'ifs_{indicator}_{freq}'

    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    disk_data, disk_ts = _load_from_disk(indicator, freq)
    if disk_data and disk_data.get('countries') and (time.time() - disk_ts) < _CACHE_TTL:
        with _cache_lock:
            _cache[cache_key] = {'data': disk_data, 'ts': disk_ts}
        return disk_data

    data = _fetch_ifs(indicator, freq)

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
    _save_to_disk(indicator, freq, data, now)
    return data


def _get_with_retry(url, params):
    last_exc = None
    for attempt, backoff in enumerate((0,) + _RETRY_BACKOFFS):
        if backoff:
            time.sleep(backoff)
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code in _RETRY_STATUS:
                last_exc = requests.HTTPError(f'{resp.status_code} Server Error for url: {url}')
                print(f'[IFS] retry {attempt + 1}/{len(_RETRY_BACKOFFS) + 1} after HTTP {resp.status_code}')
                continue
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            print(f'[IFS] retry {attempt + 1}/{len(_RETRY_BACKOFFS) + 1} after {type(e).__name__}')
            continue
    raise last_exc if last_exc else RuntimeError('fetch failed')


def _fetch_ifs(indicator, freq):
    """Fetch one IFS indicator from DBnomics, paging through all countries."""
    try:
        all_docs = []
        offset = 0
        page_size = 500
        url = f'{_DBNOMICS_BASE}/series/{_IFS_DATASET}'

        while True:
            params = {
                'dimensions': json.dumps({
                    'FREQ': [freq],
                    'INDICATOR': [indicator],
                }),
                'observations': '1',
                'limit': str(page_size),
                'offset': str(offset),
                'metadata': 'false',
            }
            resp = _get_with_retry(url, params)
            payload = resp.json()
            series = payload.get('series', {})
            docs = series.get('docs', [])
            num_found = series.get('num_found', 0)
            if not docs:
                break
            all_docs.extend(docs)
            offset += page_size
            if offset >= num_found:
                break

        countries = {}
        all_periods = set()
        for doc in all_docs:
            dims = doc.get('dimensions', {}) or {}
            iso = dims.get('REF_AREA') or ''
            # DBnomics uses ISO-3 for most IFS series; filter aggregates.
            if not iso or len(iso) != 3 or iso.upper() != iso:
                continue
            periods = doc.get('period', []) or []
            values = doc.get('value', []) or []
            name = doc.get('series_name', iso).split(' – ')[0] or iso
            country = countries.setdefault(iso, {'name': name, 'values': {}})
            for p, v in zip(periods, values):
                if v is None or v == 'NA':
                    continue
                try:
                    country['values'][p] = float(v)
                    all_periods.add(p)
                except (ValueError, TypeError):
                    continue

        return {
            'countries': countries,
            'periods': sorted(all_periods),
            'meta': {
                'source': 'IMF IFS (via DBnomics)',
                'indicator': indicator,
                'frequency': freq,
                'last_updated': time.strftime('%Y-%m-%d'),
                'country_count': len(countries),
            },
        }

    except Exception as e:
        print(f'[IFS] Error fetching {indicator}/{freq}: {e}')
        return {
            'countries': {},
            'periods': [],
            'meta': {
                'source': 'IMF IFS (via DBnomics)',
                'indicator': indicator,
                'frequency': freq,
                'error': str(e),
            },
        }


def latest_period(values, max_lookback=8):
    """Return (period_str, float) for most recent non-null period, else None.

    Works for quarterly (2025Q3), monthly (2025-03), and annual (2024) keys
    by relying on lexicographic ordering — which is correct for ISO-like
    DBnomics period labels.
    """
    if not values:
        return None
    sorted_periods = sorted(values.keys(), reverse=True)
    for p in sorted_periods[:max_lookback]:
        v = values.get(p)
        if v is not None:
            return p, float(v)
    return None


def period_year(period):
    """Extract the integer year from an IFS period string."""
    if not period:
        return None
    try:
        return int(period[:4])
    except (ValueError, TypeError):
        return None
