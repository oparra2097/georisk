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
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Iterable, Optional

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


# ── Public API ──────────────────────────────────────────────────────────

# Per-level fallback URLs — FHFA publishes individual files in addition
# to the master CSV. If the master parses 0 rows for a given level (schema
# drift), we fall back to these dedicated files. Verified against the
# FHFA HPI Datasets page (April 2026 audit).
#
# State CSV: comma-delimited, columns place_id (e.g. 'CA'), place_name,
#   yr, qtr, index_nsa, index_sa.
# US-and-Census TXT: tab-delimited (NOT comma!), covers national + 9
#   census divisions + 4 census regions. Columns: Place_Name, Place_ID,
#   yr, qtr, index_nsa, index_sa.
_FALLBACK_STATE_URL = 'https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_at_state.csv'
_FALLBACK_DIVISION_URL = 'https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_at_us_and_census.txt'


def _parse_per_level_csv(text: str, level: str) -> list[HpiRow]:
    """Parser for the dedicated per-level FHFA files (state CSV /
    us_and_census TXT).

    Schema (case-insensitive): place_name, place_id, yr, qtr (or period),
    index_nsa, index_sa. The us_and_census file is **tab-delimited**, not
    comma — sniff before parsing.
    """
    # Sniff delimiter — first non-empty line tells us
    first = next((l for l in text.splitlines() if l.strip()), '')
    delim = '\t' if (first.count('\t') > first.count(',')) else ','

    rows: list[HpiRow] = []
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    fieldnames = reader.fieldnames or []
    if not fieldnames:
        return []

    col_map = {f.strip().lower(): f for f in fieldnames}
    logger.info(f'fhfa per-level ({level}): columns = {list(col_map.keys())}')

    def col(row, key, default=''):
        actual = col_map.get(key)
        return row.get(actual, default) if actual else default

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
    try:
        resp = requests.get(url, timeout=60,
                            headers={'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)'})
        if resp.status_code != 200:
            msg = f'HTTP {resp.status_code}'
            logger.warning(f'fhfa per-level {level}: {msg}')
            diagnostics.record_fetch_fail(f'fhfa_{level}_fallback', f'FHFA {level} (fallback)', msg)
            return []
        rows = _parse_per_level_csv(resp.text, level)
        logger.info(f'fhfa per-level {level}: parsed {len(rows)} rows')
        if rows:
            diagnostics.record_fetch_ok(f'fhfa_{level}_fallback', f'FHFA {level} (fallback)', len(rows))
        else:
            diagnostics.record_fetch_fail(
                f'fhfa_{level}_fallback', f'FHFA {level} (fallback)',
                f'parsed 0 rows; first 200 chars: {resp.text[:200]!r}',
            )
        return rows
    except Exception as e:
        logger.error(f'fhfa per-level {level} fetch failed: {e}')
        diagnostics.record_fetch_fail(f'fhfa_{level}_fallback', f'FHFA {level} (fallback)', str(e))
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
        resp = requests.get(_source('fhfa_master'), timeout=90,
                            headers={'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)'})
        if resp.status_code != 200:
            msg = f'HTTP {resp.status_code}'
            logger.warning(f'fhfa master {msg}')
            diagnostics.record_fetch_fail('fhfa_master', 'FHFA HPI master', msg)
            return []
        rows = _parse_master_csv(resp.text)
        logger.info(f'fhfa master: parsed {len(rows)} rows')
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
        resp = requests.get(_source('fhfa_county'), timeout=90,
                            headers={'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)'})
        if resp.status_code != 200:
            msg = f'HTTP {resp.status_code}'
            logger.warning(f'fhfa county {msg}')
            diagnostics.record_fetch_fail('fhfa_county', 'FHFA County HPI', msg)
            return []
        rows = _parse_county_csv(resp.text)
        logger.info(f'fhfa county: parsed {len(rows)} rows')
        if len(rows) == 0:
            preview = resp.text[:200].replace('\n', ' \\n ')
            msg = f'FHFA county CSV parsed to 0 rows (header/schema mismatch). First 200 chars: {preview!r}'
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
