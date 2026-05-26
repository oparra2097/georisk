"""
Banco Central do Brasil (BCB) — SGS time-series client.

Reference implementation for fetching monthly macro data directly from a
central bank's own statistical API, rather than waiting for it to propagate
into FRED / IMF IFS (which lag by weeks to months for EM series).

The SGS ("Sistema Gerenciador de Séries Temporais") API is keyless and
returns clean JSON:

    https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json
        → [{"data": "01/03/2025", "valor": "0.56"}, ...]   # dd/mm/yyyy

Use ``get_series(name)`` for a curated monthly indicator from SERIES, or
``fetch_series(code)`` for any raw SGS code. Output mirrors the shape used by
the other data_sources clients (sorted list of {'date': 'YYYY-MM-DD',
'value': float}) plus a thread-safe in-memory + disk cache with 24h TTL.
"""

import json
import os
import time
import threading
import logging
from datetime import datetime

import requests

from config import Config

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24h — SGS monthly series update at most monthly
_BASE_URL = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados'

_RETRY_BACKOFFS = (1, 3, 9)
_RETRY_STATUS = {429, 500, 502, 503, 504}

# BCB rejects requests without a browser-like User-Agent (HTTP 403), which
# also bites datacenter-hosted deployments like Render.
_HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; georisk/1.0)'}

_DISK_CACHE_DIR = os.path.join(Config.DATA_DIR, 'bcb_cache')

# Curated monthly indicators. Codes verified against the SGS catalogue.
#   https://www3.bcb.gov.br/sgspub/
SERIES = {
    'ipca_mom':      {'code': 433,   'label': 'IPCA — monthly inflation',          'unit': '% m/m'},
    'ipca_yoy':      {'code': 13522, 'label': 'IPCA — 12-month accumulated',        'unit': '% y/y'},
    'selic_target':  {'code': 432,   'label': 'Selic target rate (Meta Copom)',     'unit': '% p.a.'},
    'selic_monthly': {'code': 4390,  'label': 'Selic — monthly annualized (252)',   'unit': '% p.a.'},
    'ibc_br':        {'code': 24363, 'label': 'IBC-Br economic activity index',     'unit': 'index'},
    'ibc_br_sa':     {'code': 24364, 'label': 'IBC-Br activity (seasonally adj.)',   'unit': 'index'},
}


# ── Thread-safe cache ──────────────────────────────────────────────────────

_cache_lock = threading.Lock()
_cache = {}  # {code: {'data': [...], 'ts': float}}


def _disk_path(code):
    return os.path.join(_DISK_CACHE_DIR, f'sgs_{code}.json')


def _load_from_disk(code):
    path = _disk_path(code)
    if not os.path.exists(path):
        return None, 0
    try:
        with open(path, 'r') as f:
            wrapper = json.load(f)
        return wrapper.get('data'), float(wrapper.get('ts', 0))
    except (OSError, ValueError, json.JSONDecodeError) as e:
        logger.warning(f'[BCB] disk cache read failed for {code}: {e}')
        return None, 0


def _save_to_disk(code, data, ts):
    try:
        os.makedirs(_DISK_CACHE_DIR, exist_ok=True)
        path = _disk_path(code)
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'ts': ts, 'data': data}, f)
        os.replace(tmp, path)
    except OSError as e:
        logger.warning(f'[BCB] disk cache write failed for {code}: {e}')


def _get_with_retry(url, params):
    last_exc = None
    for attempt, backoff in enumerate((0,) + _RETRY_BACKOFFS):
        if backoff:
            time.sleep(backoff)
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=60)
            if resp.status_code in _RETRY_STATUS:
                last_exc = requests.HTTPError(f'{resp.status_code} for {url}')
                logger.info(f'[BCB] retry {attempt + 1} after HTTP {resp.status_code}')
                continue
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            logger.info(f'[BCB] retry {attempt + 1} after {type(e).__name__}')
            continue
    raise last_exc if last_exc else RuntimeError('fetch failed')


def _parse(rows):
    """Convert SGS rows (dd/mm/yyyy + string value) to sorted ISO observations."""
    out = []
    for row in rows:
        raw_date = row.get('data')
        raw_val = row.get('valor')
        if not raw_date or raw_val in (None, ''):
            continue
        try:
            iso = datetime.strptime(raw_date, '%d/%m/%Y').strftime('%Y-%m-%d')
            out.append({'date': iso, 'value': float(raw_val)})
        except (ValueError, TypeError):
            continue
    out.sort(key=lambda r: r['date'])
    return out


def fetch_series(code, start_date=None, end_date=None, use_cache=True):
    """Fetch one SGS series by numeric code.

    ``start_date`` / ``end_date`` accept 'YYYY-MM-DD' (converted to the API's
    dd/mm/yyyy). Returns a sorted list of {'date': 'YYYY-MM-DD', 'value': float}.
    On failure, falls back to any cached copy (memory then disk) so a transient
    BCB outage doesn't blank the series; returns [] only if nothing is cached.
    """
    code = str(code)
    now = time.time()

    if use_cache and not (start_date or end_date):
        with _cache_lock:
            entry = _cache.get(code)
            if entry and (now - entry['ts']) < CACHE_TTL:
                return entry['data']
        disk_data, disk_ts = _load_from_disk(code)
        if disk_data and (now - disk_ts) < CACHE_TTL:
            with _cache_lock:
                _cache[code] = {'data': disk_data, 'ts': disk_ts}
            return disk_data

    params = {'formato': 'json'}
    if start_date:
        params['dataInicial'] = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d/%m/%Y')
    if end_date:
        params['dataFinal'] = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d/%m/%Y')

    try:
        resp = _get_with_retry(_BASE_URL.format(code=code), params)
        data = _parse(resp.json())
    except Exception as e:
        logger.error(f'[BCB] fetch failed for series {code}: {e}')
        with _cache_lock:
            entry = _cache.get(code)
        if entry:
            return entry['data']
        disk_data, _ = _load_from_disk(code)
        return disk_data or []

    if use_cache and not (start_date or end_date):
        with _cache_lock:
            _cache[code] = {'data': data, 'ts': now}
        _save_to_disk(code, data, now)

    return data


def get_series(name, **kwargs):
    """Fetch a curated indicator from SERIES by its short name."""
    meta = SERIES.get(name)
    if not meta:
        raise KeyError(f'unknown BCB series {name!r}; known: {sorted(SERIES)}')
    return fetch_series(meta['code'], **kwargs)


def latest(name_or_code):
    """Return the most recent {'date', 'value'} for a series, or None."""
    if isinstance(name_or_code, str) and name_or_code in SERIES:
        obs = get_series(name_or_code)
    else:
        obs = fetch_series(name_or_code)
    return obs[-1] if obs else None


def clear_cache():
    with _cache_lock:
        _cache.clear()
