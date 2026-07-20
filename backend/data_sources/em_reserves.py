"""
EM Monthly Reserves — a monthly international-reserves database for the top
emerging markets.

Rebuilt to replace the old Haver-fed "ExternalVulnerabilityData" reserves
table: instead of manually pasting Haver codes each month, we assemble the
latest monthly reserves for the EM universe from public sources and store
them as a database on disk (``data/em_reserves_monthly.json``) that both the
Data tab and the External Vulnerability bubble chart read from.

Hybrid data strategy (per product decision):

  1. Base layer — IMF monthly reserves. "Total official reserve assets,
     including gold" (RAFA_USD) sourced country-by-country from the IMF Data
     API via :func:`backend.data_sources.imf_cofer.get_cofer_data`. Each
     figure is the country's OWN central-bank submission under the IMF SDDS
     reserves template, so this is effectively the free equivalent of the
     Haver central-bank-reserves aggregation. Monthly, ~2–6 week lag. Only
     genuinely-reported months are kept (we stop at each country's
     ``latest_real_period`` so forward-filled placeholder months are never
     presented as fresh data).

  2. Direct central-bank layer — for a few flagship EMs that publish faster
     than the IMF template propagates, we scrape the central bank directly
     and override / extend the base series wherever the CB reports a newer
     month:
       - Brazil  — Banco Central do Brasil SGS API (no key required)
       - Mexico  — Banco de México SIE API      (needs ``BANXICO_TOKEN`` env)
       - Turkey  — TCMB EVDS API                (needs ``TCMB_EVDS_KEY`` env)
     A country whose direct scraper has no credentials configured (or whose
     request fails) simply falls back to the IMF base layer — the direct
     layer is strictly additive.

All reserve values are stored in **USD billions**. Provenance is carried
per country (``source``) and per data point where it matters, so the UI and
Excel export can show exactly where the latest number came from.

Network note: World Bank, IMF and the central-bank hosts are only reachable
from the production egress environment. Everything here follows the same
DBnomics/IMF request patterns already proven in this codebase.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime

import requests

from config import Config
from backend.data_sources.imf_cofer import (
    get_cofer_data,
    COUNTRY_NAMES,
    ISO2_TO_ISO3,
)
# EM_COUNTRIES is a plain set with no side effects; importing it here is safe
# because em_vulnerability imports THIS module lazily (inside its _build),
# so there is no import cycle at module-load time.
from backend.data_sources.em_vulnerability import EM_COUNTRIES

logger = logging.getLogger(__name__)

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 86400  # 24 hours

_DB_PATH = os.path.join(Config.DATA_DIR, 'em_reserves_monthly.json')

# How many months of history to retain in the served database. ~5 years is
# plenty for a "latest monthly reserves" table with a small trend sparkline
# while keeping the JSON payload light.
_HISTORY_MONTHS = 60

_HTTP_TIMEOUT = 30
_RETRY_BACKOFFS = (1, 3)


# ── Direct central-bank scrapers ──────────────────────────────────────────
#
# Each scraper returns ``{ 'YYYY-MM': usd_billions }`` (real reported months
# only) or ``{}`` on any failure / missing credentials. They must NEVER
# raise — a broken scraper degrades to the IMF base layer, it does not break
# the whole database build.

def _http_get_json(url, params=None, headers=None):
    """GET returning parsed JSON, with a couple of retries. Returns None on
    failure rather than raising."""
    last_exc = None
    for attempt, backoff in enumerate((0,) + _RETRY_BACKOFFS):
        if backoff:
            time.sleep(backoff)
        try:
            resp = requests.get(url, params=params, headers=headers,
                                timeout=_HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:  # noqa: BLE001 — deliberately broad; see docstring
            last_exc = e
            continue
    logger.info('[EMReserves] scraper GET failed for %s: %s', url, last_exc)
    return None


def _scrape_brazil():
    """Banco Central do Brasil — SGS series 3546: "Reservas internacionais –
    Conceito liquidez – Total" (monthly, US$ millions). Public JSON API, no
    key required.
    """
    url = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.3546/dados'
    params = {'formato': 'json'}
    doc = _http_get_json(url, params=params)
    if not isinstance(doc, list):
        return {}
    out = {}
    for row in doc:
        try:
            # SGS dates are dd/mm/yyyy; value is US$ millions as a string.
            d = row.get('data', '')
            parts = d.split('/')
            if len(parts) != 3:
                continue
            day, month, year = parts
            period = f'{int(year):04d}-{int(month):02d}'
            val_mn = float(str(row.get('valor')).replace(',', '.'))
            if val_mn > 0:
                out[period] = round(val_mn / 1000.0, 4)  # → USD billions
        except (ValueError, TypeError, AttributeError):
            continue
    return out


def _scrape_banxico():
    """Banco de México — SIE API series ``SF110168`` (Reserva internacional,
    weekly, US$ millions). Requires a free token in ``BANXICO_TOKEN``. We
    collapse the weekly series to month-end (last observation per month).
    """
    token = os.environ.get('BANXICO_TOKEN')
    if not token:
        return {}
    url = 'https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF110168/datos'
    headers = {'Bmx-Token': token, 'Accept': 'application/json'}
    doc = _http_get_json(url, headers=headers)
    if not isinstance(doc, dict):
        return {}
    try:
        series = doc['bmx']['series'][0]['datos']
    except (KeyError, IndexError, TypeError):
        return {}
    # Keep the latest observation within each calendar month.
    by_month = {}
    for row in series or []:
        try:
            d = row.get('fecha', '')  # dd/mm/yyyy
            parts = d.split('/')
            if len(parts) != 3:
                continue
            day, month, year = parts
            period = f'{int(year):04d}-{int(month):02d}'
            raw = str(row.get('dato', '')).replace(',', '')
            if raw in ('', 'N/E', 'N/A'):
                continue
            val_mn = float(raw)
            day_i = int(day)
            prev = by_month.get(period)
            if val_mn > 0 and (prev is None or day_i >= prev[0]):
                by_month[period] = (day_i, val_mn)
        except (ValueError, TypeError, AttributeError):
            continue
    return {p: round(v[1] / 1000.0, 4) for p, v in by_month.items()}  # → USD bn


def _scrape_turkey():
    """TCMB (Türkiye) — EVDS API series ``TP.AB.A01`` (gross FX reserves,
    weekly, US$ millions). Requires ``TCMB_EVDS_KEY``. Collapsed to
    month-end. FX-only (excludes gold), so it is a conservative floor vs the
    IMF total-reserves line; still useful as a fresher print.
    """
    key = os.environ.get('TCMB_EVDS_KEY')
    if not key:
        return {}
    url = 'https://evds2.tcmb.gov.tr/service/evds/series=TP.AB.A01'
    params = {'type': 'json', 'key': key}
    doc = _http_get_json(url, params=params)
    if not isinstance(doc, dict):
        return {}
    items = doc.get('items') or []
    by_month = {}
    for row in items:
        try:
            # EVDS returns "Tarih" like "01-05-2025" (dd-mm-yyyy) or
            # "2025-5" for monthly; handle the common weekly dd-mm-yyyy.
            d = str(row.get('Tarih', ''))
            val = row.get('TP_AB_A01')
            if val in (None, '', 'null'):
                continue
            sep = '-' if '-' in d else '.'
            parts = d.split(sep)
            if len(parts) == 3:
                day, month, year = parts
            elif len(parts) == 2:
                year, month, day = parts[0], parts[1], '28'
            else:
                continue
            period = f'{int(year):04d}-{int(month):02d}'
            val_mn = float(str(val).replace(',', ''))
            day_i = int(day)
            prev = by_month.get(period)
            if val_mn > 0 and (prev is None or day_i >= prev[0]):
                by_month[period] = (day_i, val_mn)
        except (ValueError, TypeError, AttributeError):
            continue
    return {p: round(v[1] / 1000.0, 4) for p, v in by_month.items()}  # → USD bn


# ISO3 → (display source label, scraper fn). Order does not matter; each is
# applied to its own country only.
_DIRECT_SCRAPERS = {
    'BRA': ('Banco Central do Brasil (SGS, monthly)', _scrape_brazil),
    'MEX': ('Banco de México (SIE, monthly)', _scrape_banxico),
    'TUR': ('TCMB (EVDS, monthly)', _scrape_turkey),
}


# ── Base layer extraction (IMF monthly via imf_cofer) ─────────────────────

def _base_series_from_cofer():
    """Return ``{iso3: {'name', 'series': {period: usd_bn}, 'latest_real'}}``
    for the EM universe, using only genuinely-reported months.

    ``get_cofer_data`` returns each country's ``total_reserves`` array (USD
    billions, forward-filled) aligned to a ``years`` list of monthly period
    strings, plus a ``latest_real_period`` marking the last non-filled month.
    We slice each series at ``latest_real_period`` so forward-filled trailing
    placeholders are dropped.
    """
    cofer = get_cofer_data() or {}
    periods = cofer.get('years') or []
    countries = cofer.get('countries') or []
    out = {}
    for c in countries:
        iso3 = c.get('iso3')
        if not iso3 or iso3 not in EM_COUNTRIES:
            continue
        totals = c.get('total_reserves') or []
        latest_real = c.get('latest_real_period')
        # Index of the last real month; anything after it is forward-fill.
        cutoff_idx = len(periods) - 1
        if latest_real in periods:
            cutoff_idx = periods.index(latest_real)
        series = {}
        for i, p in enumerate(periods[:cutoff_idx + 1]):
            v = totals[i] if i < len(totals) else None
            if v is not None:
                series[p] = round(float(v), 4)
        if not series:
            continue
        out[iso3] = {
            'name': c.get('name') or COUNTRY_NAMES.get(iso3, iso3),
            'series': series,
            'latest_real': latest_real,
        }
    return out


# ── Assembly ──────────────────────────────────────────────────────────────

def _latest_period(series):
    """Return the max 'YYYY-MM' key in a period→value dict, or None."""
    return max(series.keys()) if series else None


def _pct_change(series, latest, months_back):
    """Percent change of ``series[latest]`` vs the value ``months_back``
    months earlier, if that month is present. Returns None otherwise."""
    if not latest:
        return None
    try:
        y, m = int(latest[:4]), int(latest[5:7])
    except (ValueError, TypeError):
        return None
    m0 = m - months_back
    y0 = y + (m0 - 1) // 12
    m0 = ((m0 - 1) % 12) + 1
    prev = f'{y0:04d}-{m0:02d}'
    a, b = series.get(latest), series.get(prev)
    if a is None or b is None or b == 0:
        return None
    return round((a - b) / b * 100.0, 2)


def _build():
    """Assemble the EM monthly reserves database."""
    t0 = time.time()
    base = _base_series_from_cofer()
    logger.info('[EMReserves] base layer: %d EM countries from IMF/cofer', len(base))

    records = {}
    for iso3, info in base.items():
        records[iso3] = {
            'iso3': iso3,
            'name': info['name'],
            'series': dict(info['series']),
            'source': 'IMF (monthly reserves template, central-bank submissions)',
            'source_tier': 'imf',
        }

    # Direct central-bank overrides / extensions.
    for iso3, (label, fn) in _DIRECT_SCRAPERS.items():
        try:
            cb_series = fn() or {}
        except Exception as e:  # noqa: BLE001 — never let a scraper break the build
            logger.info('[EMReserves] %s scraper raised: %s', iso3, e)
            cb_series = {}
        if not cb_series:
            continue
        # Trim to the retained window before merging.
        rec = records.get(iso3)
        if rec is None:
            # Country the IMF layer didn't have — create from the CB series.
            rec = {
                'iso3': iso3,
                'name': COUNTRY_NAMES.get(iso3, iso3),
                'series': {},
                'source': label,
                'source_tier': 'central_bank',
            }
            records[iso3] = rec
        base_series = rec['series']
        cb_latest = _latest_period(cb_series)
        base_latest = _latest_period(base_series)
        # Merge CB months on top of the base months (CB wins on overlap).
        merged = dict(base_series)
        merged.update(cb_series)
        rec['series'] = merged
        # Only claim the CB as the headline source when it is genuinely
        # fresher (or the only) data; otherwise keep IMF as the source label
        # but still benefit from any extra history the CB provided.
        if cb_latest and (base_latest is None or cb_latest >= base_latest):
            rec['source'] = label
            rec['source_tier'] = 'central_bank'
        logger.info('[EMReserves] %s: merged CB series (CB latest %s vs base %s)',
                    iso3, cb_latest, base_latest)

    # Finalize each record: trim window, compute latest / MoM / YoY.
    countries_out = {}
    all_periods = set()
    for iso3, rec in records.items():
        series = rec['series']
        if not series:
            continue
        keep = sorted(series.keys())[-_HISTORY_MONTHS:]
        series = {p: series[p] for p in keep}
        rec['series'] = series
        all_periods.update(series.keys())
        latest = _latest_period(series)
        rec['latest_period'] = latest
        rec['latest_usd_bn'] = series.get(latest) if latest else None
        rec['mom_pct'] = _pct_change(series, latest, 1)
        rec['yoy_pct'] = _pct_change(series, latest, 12)
        countries_out[iso3] = rec

    # Rank by latest reserves (largest first) for a sensible default table order.
    ranked = sorted(
        countries_out.values(),
        key=lambda r: r.get('latest_usd_bn') or 0,
        reverse=True,
    )
    for i, rec in enumerate(ranked, 1):
        rec['rank'] = i

    periods_sorted = sorted(all_periods)
    latest_overall = periods_sorted[-1] if periods_sorted else None
    cb_count = sum(1 for r in countries_out.values()
                   if r.get('source_tier') == 'central_bank')

    logger.info('[EMReserves] assembled %d EM countries (%d via direct CB) '
                'latest=%s in %.1fs',
                len(countries_out), cb_count, latest_overall, time.time() - t0)

    result = {
        'countries': countries_out,
        'order': [r['iso3'] for r in ranked],
        'periods': periods_sorted,
        'meta': {
            'unit': 'USD billion',
            'measure': 'Total official reserve assets (incl. gold)',
            'source': ('IMF monthly reserves template (central-bank '
                       'submissions) + direct central-bank feeds for '
                       'Brazil / Mexico / Türkiye'),
            'direct_cb_countries': sorted(
                iso for iso, r in countries_out.items()
                if r.get('source_tier') == 'central_bank'
            ),
            'country_count': len(countries_out),
            'latest_period': latest_overall,
            'history_months': _HISTORY_MONTHS,
            'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        },
    }
    _persist(result)
    return result


def _persist(result):
    """Write the assembled database to disk so it lives as a real file that
    can be inspected / committed / diffed, and can serve as a warm fallback
    if a later live fetch fails."""
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        tmp = _DB_PATH + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(result, f, indent=2, sort_keys=True)
        os.replace(tmp, _DB_PATH)
        logger.info('[EMReserves] persisted database → %s', _DB_PATH)
    except OSError as e:
        logger.warning('[EMReserves] could not persist database: %s', e)


def _load_from_disk():
    if not os.path.exists(_DB_PATH):
        return None
    try:
        with open(_DB_PATH, 'r') as f:
            return json.load(f)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        logger.info('[EMReserves] disk read failed: %s', e)
        return None


def get_em_reserves_data():
    """Return the EM monthly reserves database, cached 24h in memory.

    On a cold build failure (e.g. all upstream sources unreachable) we fall
    back to the last database persisted on disk so the page still renders.
    """
    with _cache_lock:
        entry = _cache.get('main')
        if entry and time.time() - entry['ts'] < _CACHE_TTL:
            return entry['data']

    try:
        data = _build()
    except Exception as e:  # noqa: BLE001
        logger.error('[EMReserves] build failed: %s', e)
        disk = _load_from_disk()
        if disk:
            disk.setdefault('meta', {})['stale'] = True
            return disk
        raise

    with _cache_lock:
        _cache['main'] = {'data': data, 'ts': time.time()}
    return data


def get_em_reserves_latest_map():
    """Compact ``{iso3: (period, usd, source_label)}`` of the latest real
    monthly reserves, in raw USD (not billions), for the External
    Vulnerability chart to consume as its freshest reserves layer.

    Best-effort: any failure yields an empty map so the chart cleanly falls
    back to its IMF-IFS / World-Bank layers.
    """
    try:
        data = get_em_reserves_data()
    except Exception as e:  # noqa: BLE001
        logger.info('[EMReserves] latest-map unavailable: %s', e)
        return {}
    out = {}
    for iso3, rec in (data.get('countries') or {}).items():
        period = rec.get('latest_period')
        usd_bn = rec.get('latest_usd_bn')
        if period and usd_bn is not None:
            out[iso3] = (period, float(usd_bn) * 1e9, rec.get('source'))
    return out


def refresh_cache():
    """Force an immediate rebuild (used by an admin refresh endpoint)."""
    with _cache_lock:
        _cache.pop('main', None)
    return get_em_reserves_data()
