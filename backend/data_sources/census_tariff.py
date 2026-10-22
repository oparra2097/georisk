"""
US effective tariff rate — exact Apollo / Torsten Slok series.

Slok's chart note: "Calculated as monthly census 'Calculated Duty'
divided by total US goods imports, multiplied by 100. Sources: US Census
Bureau, Macrobond, Apollo Chief Economist." Macrobond is just his
charting tool; the underlying data is the Census Bureau's monthly
international trade release (FT-900), published ~5 weeks after month end.

Census International Trade API, imports by HS commodity:
    CAL_DUT_MO  calculated duty, month
    GEN_VAL_MO  general imports value, month   (total US goods imports)
    CON_VAL_MO  imports for consumption, month (reference only)

Runtime: fetch live (12 h cache) and fall back to the committed snapshot
static/data/census_tariff_rate.json, which a daily GitHub Action
refreshes (scripts/refresh_census_tariff.py). Whichever has the later
month wins.
"""

import json
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

CENSUS_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'
START_MONTH = '2021-01'
CACHE_TTL = 12 * 3600

SNAPSHOT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'static', 'data', 'census_tariff_rate.json',
)

SOURCE_NOTE = (
    'Note: Calculated as monthly Census "Calculated Duty" divided by total US '
    'goods imports, multiplied by 100. Sources: US Census Bureau, Apollo Chief '
    'Economist (Torsten Slok) methodology.'
)

_lock = threading.RLock()
_cache = {'data': None, 'at': 0}


def _get(params, verbose):
    params = dict(params)
    key = os.environ.get('CENSUS_API_KEY')
    if key:
        params['key'] = key
    resp = requests.get(CENSUS_URL, params=params, timeout=60)
    if verbose:
        print(f'GET {resp.url.split("&key=")[0]} -> HTTP {resp.status_code}, {len(resp.content)} bytes')
    # Census redirects keyless or bad-key requests to an HTML help page.
    if 'missing_key' in resp.url or 'invalid_key' in resp.url:
        msg = ('Census API rejected the request (missing or invalid key). '
               'Set CENSUS_API_KEY — free signup at https://api.census.gov/data/key_signup.html')
        logger.warning(msg)
        if verbose:
            print('  ' + msg)
        return None
    if resp.status_code != 200:
        if verbose:
            print('  body:', resp.text[:300])
        return None
    rows = resp.json()
    if verbose and rows:
        print('  header:', rows[0])
        for r in rows[1:4]:
            print('  row   :', r)
    return rows


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _totals_by_country_total(verbose):
    """Strategy A: the all-commodity row for CTY_CODE '-' (total, all countries)."""
    rows = _get({
        'get': 'CTY_CODE,CTY_NAME,GEN_VAL_MO,CON_VAL_MO,CAL_DUT_MO',
        'time': f'from {START_MONTH}',
    }, verbose)
    if not rows or len(rows) < 2:
        return {}
    h = {name: i for i, name in enumerate(rows[0])}
    out = {}
    for r in rows[1:]:
        code = (r[h['CTY_CODE']] or '').strip()
        name = (r[h['CTY_NAME']] or '').strip().upper()
        if code == '-' or name == 'TOTAL FOR ALL COUNTRIES':
            out[r[h['time']]] = {
                'gen': _num(r[h['GEN_VAL_MO']]),
                'con': _num(r[h['CON_VAL_MO']]),
                'duty': _num(r[h['CAL_DUT_MO']]),
            }
    if verbose:
        print(f'  strategy A (country total): {len(out)} months')
    return out


def _totals_by_hs2_sum(verbose):
    """Strategy B: sum the HS2 chapters (all countries) per month."""
    rows = _get({
        'get': 'I_COMMODITY,GEN_VAL_MO,CON_VAL_MO,CAL_DUT_MO',
        'COMM_LVL': 'HS2',
        'time': f'from {START_MONTH}',
    }, verbose)
    if not rows or len(rows) < 2:
        return {}
    h = {name: i for i, name in enumerate(rows[0])}
    acc = defaultdict(lambda: {'gen': 0.0, 'con': 0.0, 'duty': 0.0})
    for r in rows[1:]:
        m = acc[r[h['time']]]
        for k, col in (('gen', 'GEN_VAL_MO'), ('con', 'CON_VAL_MO'), ('duty', 'CAL_DUT_MO')):
            m[k] += _num(r[h[col]]) or 0.0
    if verbose:
        print(f'  strategy B (HS2 sum): {len(acc)} months')
    return dict(acc)


def fetch_census_series(verbose=False):
    """Fetch the monthly series from Census. Returns the payload dict or None."""
    totals = {}
    for strategy in (_totals_by_country_total, _totals_by_hs2_sum):
        try:
            totals = strategy(verbose)
        except Exception as e:
            logger.warning(f'Census {strategy.__name__} failed: {e}')
            if verbose:
                print(f'  {strategy.__name__} error: {type(e).__name__}: {e}')
            totals = {}
        if totals:
            break
    if not totals:
        return None

    points = []
    for month in sorted(totals):
        t = totals[month]
        if not t['gen'] or t['duty'] is None:
            continue
        points.append({
            'date': f'{month[:7]}-01',
            'value': round(t['duty'] / t['gen'] * 100.0, 2),
            'rate_consumption': round(t['duty'] / t['con'] * 100.0, 2) if t['con'] else None,
            'duty': t['duty'],
            'gen_imports': t['gen'],
        })
    if not points:
        return None
    return _payload(points)


def _payload(points):
    latest = points[-1]
    peak = max(points, key=lambda p: p['value'])
    return {
        'source': 'US Census Bureau (FT-900), Apollo / Torsten Slok methodology',
        'source_url': 'https://www.census.gov/foreign-trade/index.html',
        'source_detail': 'Monthly Census "Calculated Duty" ÷ total US goods imports (general imports) × 100',
        'title': 'US effective tariff rate',
        'unit': 'Percent of goods imports',
        'frequency': 'Monthly',
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        'latest_date': latest['date'],
        'latest_value': latest['value'],
        'peak_date': peak['date'],
        'peak_value': peak['value'],
        'methodology': (
            'Monthly calculated duty (CAL_DUT_MO) divided by monthly general imports '
            '(GEN_VAL_MO), all commodities and all countries, from the US Census Bureau '
            'International Trade API. This is the series Apollo Chief Economist Torsten '
            'Slok charts in The Daily Spark. Census publishes each month about five weeks '
            'after it ends, so the latest point lags the calendar by one to two months.'
        ),
        'note': SOURCE_NOTE,
        'points': points,
    }


def _load_snapshot():
    try:
        with open(SNAPSHOT_PATH) as f:
            data = json.load(f)
        return data if data.get('points') else None
    except (OSError, ValueError):
        return None


_warming = {'busy': False}


def get_census_tariff_data(block=True):
    """Newest of (live Census fetch, committed snapshot), cached 12 h.

    With block=False and a cold cache, return the snapshot immediately and
    refresh from Census in a background thread, so page loads never wait
    on the Census API.
    """
    with _lock:
        if _cache['data'] and time.time() - _cache['at'] < CACHE_TTL:
            return _cache['data']
        if not block:
            if not _warming['busy']:
                _warming['busy'] = True
                threading.Thread(target=_warm, daemon=True).start()
            snap = _load_snapshot()
            if snap:
                snap = dict(snap)
                snap['origin'] = 'snapshot'
            return snap
    return _refresh()


def _warm():
    try:
        _refresh()
    except Exception as e:
        logger.warning(f'Census background refresh failed: {e}')
    finally:
        with _lock:
            _warming['busy'] = False


def _refresh():
    live = None
    try:
        live = fetch_census_series()
    except Exception as e:
        logger.warning(f'Census live fetch failed: {e}')
    snap = _load_snapshot()

    candidates = [d for d in (live, snap) if d]
    if not candidates:
        return None
    chosen = max(candidates, key=lambda d: d.get('latest_date') or '')
    best = dict(chosen)
    best['origin'] = 'live' if chosen is live else 'snapshot'

    with _lock:
        _cache['data'] = best
        _cache['at'] = time.time()
    return best
