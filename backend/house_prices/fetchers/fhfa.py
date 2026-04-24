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
    rows: list[HpiRow] = []
    reader = csv.DictReader(io.StringIO(text))
    for r in reader:
        if r.get('hpi_flavor', '').strip().lower() != 'all-transactions':
            continue
        if r.get('frequency', '').strip().lower() != 'quarterly':
            continue

        raw_level = r.get('level', '').strip()
        level = _LEVEL_MAP.get(raw_level)
        if level is None:
            # Split 'USA or Census Division' based on place_id
            pid = r.get('place_id', '').strip()
            level = 'national' if pid in ('USA', '00') else 'region'

        try:
            yr = int(r['yr'])
            pd_ = int(r['period'])
        except (KeyError, ValueError, TypeError):
            continue

        def _f(key):
            v = r.get(key, '').strip()
            if v in ('', '.', '-'):
                return None
            try:
                return float(v)
            except ValueError:
                return None

        rows.append(HpiRow(
            level=level,
            code=r.get('place_id', '').strip(),
            name=r.get('place_name', '').strip(),
            year=yr,
            period=pd_,
            freq='quarterly',
            index_nsa=_f('index_nsa'),
            index_sa=_f('index_sa'),
        ))
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

def fetch_master(force: bool = False) -> list[HpiRow]:
    """FHFA quarterly master (national / region / state / MSA)."""
    with _lock:
        if not force and _mem['master'] is not None:
            return _mem['master']
        disk_rows, _ = (_load_disk(_CACHE_FILE) if not force else (None, None))
        if disk_rows is not None:
            parsed = [HpiRow(**r) for r in disk_rows]
            _mem['master'] = parsed
            return parsed

    logger.info('fhfa: downloading master file…')
    try:
        resp = requests.get(_source('fhfa_master'), timeout=90,
                            headers={'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)'})
        if resp.status_code != 200:
            logger.warning(f'fhfa master HTTP {resp.status_code}')
            return []
        rows = _parse_master_csv(resp.text)
        logger.info(f'fhfa master: parsed {len(rows)} rows')
        with _lock:
            _mem['master'] = rows
        _save_disk(_CACHE_FILE, rows)
        return rows
    except Exception as e:
        logger.error(f'fhfa master fetch failed: {e}')
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
            return parsed

    logger.info('fhfa: downloading county file…')
    try:
        resp = requests.get(_source('fhfa_county'), timeout=90,
                            headers={'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)'})
        if resp.status_code != 200:
            logger.warning(f'fhfa county HTTP {resp.status_code}')
            return []
        rows = _parse_county_csv(resp.text)
        logger.info(f'fhfa county: parsed {len(rows)} rows')
        with _lock:
            _mem['county'] = rows
        _save_disk(_CACHE_FILE_COUNTY, rows)
        return rows
    except Exception as e:
        logger.error(f'fhfa county fetch failed: {e}')
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
