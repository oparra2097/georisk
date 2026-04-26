"""
FHFA House Price Index client.

Pulls the quarterly master file (USA / Census region / State / MSA) and the
separate annual county file, parses to long-format rows, caches to disk.

The FHFA master file is a CSV with the following columns we care about:
    hpi_flavor       'all-transactions' or 'purchase-only'
    frequency        'quarterly' or 'monthly'
    level            'USA or MSA' / 'State' / 'Census Division' / etc.
    place_name       entity name
    place_id         FIPS / CBSA / 2-char state code
    yr               year (int)
    period           1-4 (quarter) or 1-12 (month)
    index_nsa        non-seasonally-adjusted index
    index_sa         seasonally-adjusted index  (may be blank for some levels)

We keep rows where hpi_flavor = 'all-transactions' (covers refis too;
better coverage than purchase-only especially for smaller markets).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import urljoin

import requests

from config import Config
from backend.house_prices import diagnostics
from backend.house_prices.sources import SOURCES

logger = logging.getLogger(__name__)

_CACHE_DAYS = 7
_CACHE_FILE = os.path.join(Config.DATA_DIR, 'fhfa_hpi.json')
_CACHE_FILE_COUNTY = os.path.join(Config.DATA_DIR, 'fhfa_hpi_county.json')
_lock = threading.RLock()
_mem: dict = {'master': None, 'county': None, 'fetched_at': None}


@dataclass
class HpiRow:
    level: str            # 'national' | 'region' | 'state' | 'msa' | 'county'
    code: str             # '00' for nation, 'NE' for region, 'CA' for state, '12420' CBSA, FIPS for county
    name: str
    year: int
    period: int           # 1..4 quarters or 1..12 months
    freq: str             # 'quarterly' | 'annual'
    index_nsa: Optional[float]
    index_sa: Optional[float]

    def to_dict(self):
        return asdict(self)


# ── URLs (from sources.py) ──────────────────────────────────────────────

def _source(src_id: str) -> str:
    for s in SOURCES:
        if s.id == src_id:
            return s.url
    raise KeyError(src_id)


# ── Level mapping ───────────────────────────────────────────────────────
# The master CSV's `level` column uses values like 'USA or Census Division',
# 'State', 'MSA'. Map to our internal level vocabulary.
_LEVEL_MAP = {
    'USA or Census Division': None,     # split below into 'national' vs 'region'
    'USA':                    'national',
    'Census Division':        'region',
    'Census Region':          'region',
    'State':                  'state',
    'MSA':                    'msa',
    'County':                 'county',
}


# ── Master file (national / region / state / MSA) ───────────────────────

def _parse_master_csv(text: str) -> list[HpiRow]:
    """
    Robust parser: case-insensitive column names, lenient flavor/frequency
    matching. Logs aggregate counts so the diagnostics endpoint can surface
    why something is being filtered out.
    """
    rows: list[HpiRow] = []
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = [f for f in (reader.fieldnames or [])]
    if not fieldnames:
        logger.warning('fhfa master: empty or non-CSV response (no fieldnames)')
        return []

    # Build a case-insensitive column lookup so 'HPI_Flavor' / 'hpi_flavor' / 'Flavor' all map.
    col_map = {f.strip().lower(): f for f in fieldnames}
    logger.info(f'fhfa master: columns = {list(col_map.keys())}')

    def col(row, key, default=''):
        actual = col_map.get(key)
        return row.get(actual, default) if actual else default

    flavor_counts: dict[str, int] = {}
    freq_counts: dict[str, int] = {}
    level_counts: dict[str, int] = {}
    kept = 0
    for r in reader:
        flavor = col(r, 'hpi_flavor').strip().lower() or col(r, 'flavor').strip().lower()
        freq = col(r, 'frequency').strip().lower()
        flavor_counts[flavor] = flavor_counts.get(flavor, 0) + 1
        freq_counts[freq] = freq_counts.get(freq, 0) + 1

        if flavor and flavor != 'all-transactions':
            continue
        if freq and freq != 'quarterly':
            continue

        raw_level = col(r, 'level').strip()
        level_counts[raw_level] = level_counts.get(raw_level, 0) + 1
        level = _LEVEL_MAP.get(raw_level)
        if level is None:
            # Fallback for unrecognized 'level' values. FHFA recently
            # introduced prefixed place_ids (`DV_ENC` for divisions,
            # `ST_xx` for states, `RG_xx` for regions). Plus the legacy
            # bare codes for backwards compat.
            pid = col(r, 'place_id').strip()
            up = pid.upper()
            if up in ('USA', '00') or up.startswith('US_'):
                level = 'national'
            elif up.startswith('ST_'):
                level = 'state'
            elif up.startswith('DV_') or up.startswith('RG_') or up.startswith('CD_'):
                level = 'region'
            elif up.startswith('MSA_') or up.startswith('CBSA_'):
                level = 'msa'
            elif up.startswith('CO_') or up.startswith('FIPS_'):
                level = 'county'
            elif pid and len(pid) == 2 and pid.isalpha():
                level = 'state'
            elif pid and len(pid) == 2 and pid.isdigit():
                level = 'state'  # 2-digit numeric state FIPS
            elif pid and len(pid) == 3 and pid.isalpha():
                level = 'region'
            elif pid and len(pid) == 5 and pid.isdigit():
                level = 'msa'
            elif raw_level and 'state' in raw_level.lower():
                level = 'state'
            elif raw_level and ('region' in raw_level.lower() or 'division' in raw_level.lower()):
                level = 'region'
            else:
                level = 'region'

        # Strip recognized prefixes from place_id so the dashboard / map can
        # use the bare code (CA, NE, 31080) without changes downstream.
        bare_pid = col(r, 'place_id').strip()
        for prefix in ('ST_', 'DV_', 'RG_', 'CD_', 'MSA_', 'CBSA_', 'CO_', 'FIPS_', 'US_'):
            if bare_pid.upper().startswith(prefix):
                bare_pid = bare_pid[len(prefix):]
                break

        try:
            yr = int(col(r, 'yr'))
            pd_ = int(col(r, 'period'))
        except (KeyError, ValueError, TypeError):
            continue

        def _f(key):
            v = col(r, key).strip()
            if v in ('', '.', '-'):
                return None
            try:
                return float(v)
            except ValueError:
                return None

        rows.append(HpiRow(
            level=level,
            code=bare_pid,
            name=col(r, 'place_name').strip(),
            year=yr,
            period=pd_,
            freq='quarterly',
            index_nsa=_f('index_nsa'),
            index_sa=_f('index_sa'),
        ))
        kept += 1

    logger.info(f'fhfa master: kept {kept} rows. flavors={flavor_counts}  '
                f'frequencies={freq_counts}  top-levels={list(level_counts.items())[:8]}')
    return rows


def _parse_county_csv(text: str) -> list[HpiRow]:
    """FHFA county file: columns are state abbrev, county, FIPS, year, annual_change_%, HPI, HPI_1990."""
    rows: list[HpiRow] = []
    reader = csv.DictReader(io.StringIO(text))
    for r in reader:
        try:
            yr = int(r.get('Year') or r.get('yr'))
            hpi = r.get('HPI') or r.get('hpi')
            fips = (r.get('FIPS code') or r.get('fips') or '').strip()
            name = (r.get('County') or r.get('county') or '').strip()
            state = (r.get('State') or r.get('state') or '').strip()
        except (KeyError, ValueError, TypeError):
            continue
        if not fips or not hpi:
            continue
        try:
            hpi_f = float(hpi)
        except ValueError:
            continue

        rows.append(HpiRow(
            level='county',
            code=fips.zfill(5),
            name=f'{name}, {state}' if state else name,
            year=yr,
            period=1,             # annual
            freq='annual',
            index_nsa=hpi_f,
            index_sa=None,
        ))
    return rows


def _parse_county_xlsx(content: bytes) -> list[HpiRow]:
    """FHFA developmental county HPI is now published as XLSX only.

    The workbook usually has a title row and a few notes rows ABOVE the
    real column header — we scan the first 15 rows to find the row whose
    cells include a recognizable column name (FIPS / Year / HPI / etc.)
    and treat that as the header. Logs the detected header row so the
    diagnostics endpoint can surface format drift if it shifts again.
    """
    import openpyxl  # local import — only needed when XLSX URL is configured
    from io import BytesIO

    wb = openpyxl.load_workbook(BytesIO(content), read_only=True, data_only=True)
    ws = wb.active

    HEADER_KEYS = {'fips', 'fips code', 'fips_code', 'year', 'yr',
                   'hpi', 'index_nsa', 'county', 'state'}

    rows_iter = ws.iter_rows(values_only=True)
    header = None
    header_row_idx = -1
    for i, row in enumerate(rows_iter):
        if i > 14:
            break
        if not row:
            continue
        cells = [str(c).strip().lower() if c is not None else '' for c in row]
        if any(c in HEADER_KEYS for c in cells):
            header = row
            header_row_idx = i
            break

    if header is None:
        logger.warning('fhfa county XLSX: no recognizable header row in first 15 rows')
        return []
    logger.info(f'fhfa county XLSX: header on row {header_row_idx + 1}: '
                f'{[str(c) if c is not None else None for c in header]}')

    col_idx: dict[str, int] = {}
    for i, c in enumerate(header):
        if c is None:
            continue
        col_idx[str(c).strip().lower()] = i

    def _get(row, *keys):
        for k in keys:
            idx = col_idx.get(k)
            if idx is not None and idx < len(row):
                v = row[idx]
                if v is not None:
                    return v
        return None

    out: list[HpiRow] = []
    for r in rows_iter:
        if r is None:
            continue
        yr_v = _get(r, 'year', 'yr')
        hpi_v = _get(r, 'hpi', 'index_nsa')
        fips_v = _get(r, 'fips code', 'fips', 'fips_code')
        if yr_v is None or hpi_v is None or fips_v is None:
            continue
        try:
            yr = int(yr_v)
            hpi_f = float(hpi_v)
        except (ValueError, TypeError):
            continue
        fips = str(fips_v).strip()
        if fips.endswith('.0'):
            fips = fips[:-2]
        if not fips:
            continue
        name = str(_get(r, 'county') or '').strip()
        state = str(_get(r, 'state') or '').strip()

        out.append(HpiRow(
            level='county',
            code=fips.zfill(5),
            name=f'{name}, {state}' if state else name,
            year=yr,
            period=1,
            freq='annual',
            index_nsa=hpi_f,
            index_sa=None,
        ))
    return out


# ── Cache layer ─────────────────────────────────────────────────────────

def _load_disk(path: str):
    try:
        if not os.path.exists(path):
            return None, None
        with open(path, 'r') as f:
            data = json.load(f)
        fetched_at = datetime.fromisoformat(data['fetched_at'])
        age_days = (datetime.utcnow() - fetched_at).days
        if age_days > _CACHE_DAYS:
            return None, None
        return data['rows'], fetched_at
    except Exception as e:
        logger.warning(f'fhfa cache load failed: {e}')
        return None, None


def _save_disk(path: str, rows: list[HpiRow]):
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'fetched_at': datetime.utcnow().isoformat(),
                       'rows': [r.to_dict() for r in rows]}, f, separators=(',', ':'))
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f'fhfa cache save failed: {e}')


# ── URL resolution (404-tolerant) ───────────────────────────────────────
#
# FHFA reorganizes download paths periodically (the 2025Q4 reshuffle moved
# `quarterly_datasets/` -> `monthly/` and `annually_datasets/` -> `annually/`).
# Hardcoding a single URL makes the dashboard go red the moment FHFA shifts
# things. To survive future reorgs without code changes we:
#   1. Try the URL configured in sources.py (the "primary").
#   2. Fall through to a list of historically-valid candidate paths.
#   3. As a last resort scrape the FHFA HPI Datasets landing page and pick
#      the first anchor whose href matches the target filename.
# Any URL that returns 200 is cached per-process so subsequent fetches in
# the same Render dyno skip straight to the working URL.

_FHFA_DATASETS_URL = 'https://www.fhfa.gov/data/hpi/datasets'
_USER_AGENT = 'Mozilla/5.0 (compatible; ParraMacro/1.0)'

# Ordered fallback URLs — first 200 wins. Keep both pre- and post-2025Q4
# layouts here so we self-heal in either direction if FHFA reverts.
_URL_CANDIDATES: dict[str, list[str]] = {
    'fhfa_master': [
        'https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv',
        'https://www.fhfa.gov/hpi/download/quarterly/hpi_master.csv',
        'https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_master.csv',
    ],
    'fhfa_county': [
        # FHFA's 2025 site refresh: `annually/` -> `annual/` (singular), and
        # the BDL prefix dropped from the filename. The new canonical XLSX
        # is at `/hpi/download/annual/hpi_at_county.xlsx`. We keep the older
        # paths as fallbacks in case FHFA partially reverts.
        'https://www.fhfa.gov/hpi/download/annual/hpi_at_county.xlsx',
        'https://www.fhfa.gov/hpi/download/annually/hpi_at_county.xlsx',
        'https://www.fhfa.gov/hpi/download/annual/hpi_at_bdl_county.xlsx',
        'https://www.fhfa.gov/hpi/download/annually/hpi_at_bdl_county.xlsx',
        'https://www.fhfa.gov/hpi/download/annually/hpi_at_bdl_county.csv',
        'https://www.fhfa.gov/hpi/download/annually_datasets/hpi_at_bdl_county.xlsx',
        'https://www.fhfa.gov/hpi/download/annually_datasets/hpi_at_bdl_county.csv',
        'https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_BDL_county.xlsx',
    ],
    'fhfa_state_fallback': [
        'https://www.fhfa.gov/hpi/download/quarterly/hpi_at_state.csv',
        'https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_at_state.csv',
        'https://www.fhfa.gov/hpi/download/monthly/hpi_at_state.csv',
    ],
    'fhfa_region_fallback': [
        'https://www.fhfa.gov/hpi/download/quarterly/hpi_at_us_and_census.txt',
        'https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_at_us_and_census.txt',
        'https://www.fhfa.gov/hpi/download/monthly/hpi_at_us_and_census.txt',
    ],
}

# Filename patterns used to recognize the right anchor when scraping the
# FHFA datasets landing page. Order of extensions inside the alternation
# expresses preference (CSV preferred over XLSX where both exist, etc.).
_FILENAME_PATTERNS: dict[str, re.Pattern] = {
    'fhfa_master':          re.compile(r'/hpi_master\.(?:csv|xlsx)(?:$|[?#])', re.I),
    'fhfa_county':          re.compile(r'/hpi_at_(?:bdl_)?county\.(?:xlsx|csv)(?:$|[?#])', re.I),
    'fhfa_state_fallback':  re.compile(r'/hpi_at_state\.(?:csv|xlsx)(?:$|[?#])', re.I),
    'fhfa_region_fallback': re.compile(r'/hpi_at_us_and_census\.(?:txt|csv|xlsx)(?:$|[?#])', re.I),
}

_discovered_lock = threading.RLock()
_discovered_urls: dict[str, str] = {}     # source_key -> last URL that returned 200
_landing_cache: dict = {'urls': None, 'fetched_at': 0.0}
_LANDING_TTL_SEC = 6 * 3600               # re-scrape at most every 6h


def _scrape_landing_urls() -> dict[str, str]:
    """Scrape the FHFA HPI Datasets page and return {source_key: absolute_url}
    for any filename pattern we can match. Cached for 6 hours."""
    now = time.time()
    if _landing_cache['urls'] is not None and (now - _landing_cache['fetched_at']) < _LANDING_TTL_SEC:
        return _landing_cache['urls']
    try:
        resp = requests.get(_FHFA_DATASETS_URL, timeout=30, headers={'User-Agent': _USER_AGENT})
        if resp.status_code != 200:
            logger.warning(f'fhfa landing page: HTTP {resp.status_code}')
            _landing_cache['urls'] = {}
            _landing_cache['fetched_at'] = now
            return {}
        hrefs = re.findall(r'href=[\'"]([^\'"]+)[\'"]', resp.text)
        abs_hrefs = [urljoin(_FHFA_DATASETS_URL, h) for h in hrefs]
        out: dict[str, str] = {}
        for key, pat in _FILENAME_PATTERNS.items():
            for h in abs_hrefs:
                if pat.search(h):
                    out[key] = h
                    break
        if out:
            logger.info(f'fhfa landing page: discovered {out}')
        else:
            logger.warning('fhfa landing page: no anchors matched any expected filename')
        _landing_cache['urls'] = out
        _landing_cache['fetched_at'] = now
        return out
    except Exception as e:
        logger.warning(f'fhfa landing page scrape failed: {e}')
        _landing_cache['urls'] = {}
        _landing_cache['fetched_at'] = now
        return {}


def _resolve_and_fetch(source_key: str, primary_url: str,
                       timeout: int = 90) -> tuple[str, Optional[requests.Response]]:
    """Return (final_url, 200-response) by trying primary -> candidates ->
    landing-page scrape. Returns (primary_url, None) when every candidate
    fails so the caller can record a sensible diagnostic."""
    headers = {'User-Agent': _USER_AGENT}

    with _discovered_lock:
        cached = _discovered_urls.get(source_key)

    ordered: list[str] = []
    seen: set[str] = set()
    for u in ([cached] if cached else []) + [primary_url] + _URL_CANDIDATES.get(source_key, []):
        if u and u not in seen:
            seen.add(u)
            ordered.append(u)

    statuses: list[str] = []
    for url in ordered:
        try:
            r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
        except requests.RequestException as e:
            statuses.append(f'{url} -> ERR({type(e).__name__})')
            continue
        if r.status_code == 200:
            with _discovered_lock:
                _discovered_urls[source_key] = url
            if url != primary_url:
                logger.info(f'fhfa {source_key}: primary failed, hit {url}')
            return url, r
        statuses.append(f'{url} -> {r.status_code}')

    # Last resort — scrape FHFA datasets landing page.
    discovered = _scrape_landing_urls().get(source_key)
    if discovered and discovered not in seen:
        try:
            r = requests.get(discovered, timeout=timeout, headers=headers, allow_redirects=True)
            if r.status_code == 200:
                with _discovered_lock:
                    _discovered_urls[source_key] = discovered
                logger.info(f'fhfa {source_key}: scraped URL succeeded: {discovered}')
                return discovered, r
            statuses.append(f'{discovered}(scraped) -> {r.status_code}')
        except requests.RequestException as e:
            statuses.append(f'{discovered}(scraped) -> ERR({type(e).__name__})')

    logger.warning(f'fhfa {source_key}: all candidates failed: {statuses}')
    return primary_url, None


# ── Public API ──────────────────────────────────────────────────────────

# Per-level fallback URLs — FHFA publishes individual files in addition
# to the master CSV. If the master parses 0 rows for a given level (schema
# drift), we fall back to these dedicated files.
#
# State CSV (`hpi_at_state.csv`): comma-delimited, currently HEADERLESS —
#   4 columns: place_id, yr, qtr, index_nsa.
# US-and-Census TXT (`hpi_at_us_and_census.txt`): tab-delimited, currently
#   HEADERLESS — 4 columns: place_id, yr, qtr, index_nsa. Covers national +
#   9 census divisions + 4 census regions.
# Legacy 6-column shape (place_name, place_id, yr, qtr, index_nsa, index_sa)
# is also handled in case FHFA reverts.
_FALLBACK_STATE_URL = 'https://www.fhfa.gov/hpi/download/quarterly/hpi_at_state.csv'
_FALLBACK_DIVISION_URL = 'https://www.fhfa.gov/hpi/download/quarterly/hpi_at_us_and_census.txt'


def _parse_per_level_csv(text: str, level: str) -> list[HpiRow]:
    """Parser for the dedicated per-level FHFA files.

    Detects whether the file has a header row by checking if the second
    field of the first non-empty line parses as a 4-digit year. If so we
    treat the file as headerless and supply explicit fieldnames matching
    the column count.
    """
    first = next((l for l in text.splitlines() if l.strip()), '')
    if not first:
        return []
    delim = '\t' if (first.count('\t') > first.count(',')) else ','
    sample_fields = first.split(delim)

    headerless = False
    if len(sample_fields) >= 2:
        f1 = sample_fields[1].strip()
        if len(f1) == 4 and f1.isdigit():
            try:
                y = int(f1)
                if 1900 < y < 2200:
                    headerless = True
            except ValueError:
                pass

    if headerless:
        n = len(sample_fields)
        if n == 4:
            fieldnames = ['place_id', 'yr', 'qtr', 'index_nsa']
        elif n == 5:
            fieldnames = ['place_id', 'yr', 'qtr', 'index_nsa', 'index_sa']
        elif n >= 6:
            fieldnames = ['place_name', 'place_id', 'yr', 'qtr', 'index_nsa', 'index_sa']
        else:
            logger.warning(f'fhfa per-level ({level}): headerless with only {n} cols, cannot parse')
            return []
        reader = csv.DictReader(io.StringIO(text), fieldnames=fieldnames, delimiter=delim)
        col_map = {f: f for f in fieldnames}
        logger.info(f'fhfa per-level ({level}): headerless, assigned cols = {fieldnames}')
    else:
        reader = csv.DictReader(io.StringIO(text), delimiter=delim)
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            return []
        col_map = {f.strip().lower(): f for f in fieldnames}
        logger.info(f'fhfa per-level ({level}): columns = {list(col_map.keys())}')

    def col(row, key, default=''):
        actual = col_map.get(key)
        v = row.get(actual, default) if actual else default
        return '' if v is None else str(v)

    rows: list[HpiRow] = []
    for r in reader:
        pid = col(r, 'place_id').strip()
        name = col(r, 'place_name').strip()
        if not pid:
            continue
        try:
            yr = int(col(r, 'yr'))
            period = int(col(r, 'qtr') or col(r, 'period'))
        except (ValueError, TypeError):
            continue

        def _f(key):
            v = col(r, key).strip()
            if v in ('', '.', '-'):
                return None
            try:
                return float(v)
            except ValueError:
                return None

        # Strip prefixes (DV_, ST_, RG_, MSA_, etc.) so codes match the
        # master file's bare format (CA, ENC, 31080).
        bare = pid
        for prefix in ('ST_', 'DV_', 'RG_', 'CD_', 'MSA_', 'CBSA_', 'CO_', 'FIPS_', 'US_'):
            if bare.upper().startswith(prefix):
                bare = bare[len(prefix):]
                break

        rows.append(HpiRow(
            level=level,
            code=bare,
            name=name,
            year=yr,
            period=period,
            freq='quarterly',
            index_nsa=_f('index_nsa'),
            index_sa=_f('index_sa'),
        ))
    return rows


def _fetch_fallback(level: str, url: str) -> list[HpiRow]:
    """Download and parse a per-level FHFA file. Records diagnostics."""
    source_key = f'fhfa_{level}_fallback'
    label = f'FHFA {level} (fallback)'
    try:
        final_url, resp = _resolve_and_fetch(source_key, url, timeout=60)
        if resp is None:
            diagnostics.record_fetch_fail(source_key, label, 'HTTP 404 (all candidates failed)')
            return []
        rows = _parse_per_level_csv(resp.text, level)
        logger.info(f'fhfa per-level {level}: parsed {len(rows)} rows from {final_url}')
        if rows:
            diagnostics.record_fetch_ok(source_key, label, len(rows))
        else:
            diagnostics.record_fetch_fail(
                source_key, label,
                f'parsed 0 rows from {final_url}; first 200 chars: {resp.text[:200]!r}',
            )
        return rows
    except Exception as e:
        logger.error(f'fhfa per-level {level} fetch failed: {e}')
        diagnostics.record_fetch_fail(source_key, label, str(e))
        return []


def fetch_master(force: bool = False) -> list[HpiRow]:
    """FHFA quarterly master (national / region / state / MSA)."""
    with _lock:
        if not force and _mem['master'] is not None:
            return _mem['master']
        disk_rows, _ = (_load_disk(_CACHE_FILE) if not force else (None, None))
        if disk_rows is not None:
            parsed = [HpiRow(**r) for r in disk_rows]
            _mem['master'] = parsed
            diagnostics.record_fetch_ok('fhfa_master', 'FHFA HPI master (cached)', len(parsed))
            return parsed

    logger.info('fhfa: downloading master file…')
    try:
        final_url, resp = _resolve_and_fetch('fhfa_master', _source('fhfa_master'))
        if resp is None:
            diagnostics.record_fetch_fail('fhfa_master', 'FHFA HPI master',
                                          'HTTP 404 (all candidates failed)')
            return []
        rows = _parse_master_csv(resp.text)
        logger.info(f'fhfa master: parsed {len(rows)} rows from {final_url}')
        if len(rows) == 0:
            # HTTP 200 but parser produced nothing — usually means FHFA changed
            # the schema (column names) or the URL now returns an HTML/redirect
            # page. Either way we want this loud, not silent.
            preview = resp.text[:200].replace('\n', ' \\n ')
            msg = f'FHFA master CSV parsed to 0 rows (header/schema mismatch). First 200 chars: {preview!r}'
            logger.warning(msg)
            diagnostics.record_fetch_fail('fhfa_master', 'FHFA HPI master', msg)
            return rows
        with _lock:
            _mem['master'] = rows
        _save_disk(_CACHE_FILE, rows)
        diagnostics.record_fetch_ok('fhfa_master', 'FHFA HPI master', len(rows))
        return rows
    except Exception as e:
        logger.error(f'fhfa master fetch failed: {e}')
        diagnostics.record_fetch_fail('fhfa_master', 'FHFA HPI master', str(e))
        return []


def fetch_county(force: bool = False) -> list[HpiRow]:
    """FHFA annual county HPI."""
    with _lock:
        if not force and _mem['county'] is not None:
            return _mem['county']
        disk_rows, _ = (_load_disk(_CACHE_FILE_COUNTY) if not force else (None, None))
        if disk_rows is not None:
            parsed = [HpiRow(**r) for r in disk_rows]
            _mem['county'] = parsed
            diagnostics.record_fetch_ok('fhfa_county', 'FHFA County HPI (cached)', len(parsed))
            return parsed

    logger.info('fhfa: downloading county file…')
    try:
        final_url, resp = _resolve_and_fetch('fhfa_county', _source('fhfa_county'))
        if resp is None:
            diagnostics.record_fetch_fail('fhfa_county', 'FHFA County HPI',
                                          'HTTP 404 (all candidates failed)')
            return []
        if final_url.lower().endswith('.xlsx'):
            rows = _parse_county_xlsx(resp.content)
            fmt = 'XLSX'
        else:
            rows = _parse_county_csv(resp.text)
            fmt = 'CSV'
        logger.info(f'fhfa county ({fmt}): parsed {len(rows)} rows from {final_url}')
        if len(rows) == 0:
            preview = resp.text[:200].replace('\n', ' \\n ') if fmt == 'CSV' else f'<{len(resp.content)} bytes XLSX>'
            msg = f'FHFA county {fmt} parsed to 0 rows (header/schema mismatch). First 200 chars: {preview!r}'
            logger.warning(msg)
            diagnostics.record_fetch_fail('fhfa_county', 'FHFA County HPI', msg)
            return rows
        with _lock:
            _mem['county'] = rows
        _save_disk(_CACHE_FILE_COUNTY, rows)
        diagnostics.record_fetch_ok('fhfa_county', 'FHFA County HPI', len(rows))
        return rows
    except Exception as e:
        logger.error(f'fhfa county fetch failed: {e}')
        diagnostics.record_fetch_fail('fhfa_county', 'FHFA County HPI', str(e))
        return []


def clear_cache():
    with _lock:
        _mem['master'] = None
        _mem['county'] = None
    for path in (_CACHE_FILE, _CACHE_FILE_COUNTY):
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass


def fetch_all(force: bool = False) -> list[HpiRow]:
    """Convenience: master + county combined."""
    return fetch_master(force=force) + fetch_county(force=force)
