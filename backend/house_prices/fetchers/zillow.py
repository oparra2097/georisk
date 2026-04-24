"""
Zillow Home Value Index (ZHVI) client — monthly, CC-BY 4.0.

ZHVI is the smoothed, seasonally-adjusted typical home value for the
middle tier (35th to 65th percentile) of all homes in a geography.
Files are public CSVs on files.zillowstatic.com and update monthly.

Wide-format: one row per region, one column per month. We transpose to
the same long-format HpiRow shape as the other fetchers.

Metro file columns (first few):
    RegionID, SizeRank, RegionName, RegionType, StateName, 2000-01-31, 2000-02-29, ...
County file adds State, Metro, StateCodeFIPS, MunicipalCodeFIPS columns.
ZIP file adds Metro, State, City, CountyName.

The full ZIP file is ~100MB; keep an option to skip it for faster local runs.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import threading
from datetime import datetime
from typing import Literal, Optional

import requests

from config import Config
from backend.house_prices.fetchers.fhfa import HpiRow
from backend.house_prices.sources import SOURCES

logger = logging.getLogger(__name__)

_CACHE_DAYS = 7
_lock = threading.RLock()
_mem: dict[str, list[HpiRow]] = {}

ZScope = Literal['metro', 'county', 'zip']


def _url(scope: ZScope) -> str:
    sid = {'metro': 'zillow_metro', 'county': 'zillow_county', 'zip': 'zillow_zip'}[scope]
    for s in SOURCES:
        if s.id == sid:
            return s.url
    raise KeyError(sid)


def _cache_path(scope: ZScope) -> str:
    return os.path.join(Config.DATA_DIR, f'zillow_{scope}.json')


def _load_disk(scope: ZScope) -> Optional[list[HpiRow]]:
    path = _cache_path(scope)
    try:
        if not os.path.exists(path):
            return None
        with open(path, 'r') as f:
            data = json.load(f)
        fetched_at = datetime.fromisoformat(data['fetched_at'])
        if (datetime.utcnow() - fetched_at).days > _CACHE_DAYS:
            return None
        return [HpiRow(**r) for r in data['rows']]
    except Exception as e:
        logger.warning(f'zillow {scope} cache load failed: {e}')
        return None


def _save_disk(scope: ZScope, rows: list[HpiRow]):
    path = _cache_path(scope)
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'fetched_at': datetime.utcnow().isoformat(),
                       'rows': [r.__dict__ for r in rows]}, f, separators=(',', ':'))
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f'zillow {scope} cache save failed: {e}')


# ── Parser ──────────────────────────────────────────────────────────────

def _parse_wide(text: str, scope: ZScope, max_rows_per_region: Optional[int] = None) -> list[HpiRow]:
    """
    Transpose Zillow's wide CSV to long HpiRow records.

    Drops all dates that have no value for that region. `max_rows_per_region`
    keeps only the most recent N months (useful for ZIP file which is huge).
    """
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header:
        return []

    # Identify metadata columns vs month columns. Month columns are parseable as dates.
    meta_idx: dict[str, int] = {}
    date_idx: list[tuple[int, str]] = []
    for i, col in enumerate(header):
        col = col.strip()
        if _is_date_column(col):
            date_idx.append((i, col))
        else:
            meta_idx[col] = i

    # Keep dates sorted ascending to ease downstream processing
    date_idx.sort(key=lambda t: t[1])

    region_id_col = meta_idx.get('RegionID', 0)
    region_name_col = meta_idx.get('RegionName')
    state_col = meta_idx.get('StateName') or meta_idx.get('State')
    if scope == 'county':
        fips_state_col = meta_idx.get('StateCodeFIPS')
        fips_county_col = meta_idx.get('MunicipalCodeFIPS')
    metro_col = meta_idx.get('Metro')

    level = {'metro': 'msa', 'county': 'county', 'zip': 'zip'}[scope]
    rows: list[HpiRow] = []

    for row in reader:
        if not row:
            continue
        try:
            region_id = row[region_id_col].strip()
        except IndexError:
            continue

        # Build a stable `code` per scope
        if scope == 'county':
            try:
                s = row[fips_state_col].strip().zfill(2)
                c = row[fips_county_col].strip().zfill(3)
                code = s + c
            except (IndexError, KeyError):
                code = region_id
        else:
            code = region_id

        name = row[region_name_col].strip() if region_name_col is not None else region_id
        if state_col is not None:
            try:
                state_val = row[state_col].strip()
                # Only append state if it's not already embedded (e.g. "New York, NY")
                if state_val and not name.endswith(f', {state_val}') and state_val != name:
                    name = f'{name}, {state_val}'
            except IndexError:
                pass

        date_cols_to_use = date_idx[-max_rows_per_region:] if max_rows_per_region else date_idx
        for col_i, date_s in date_cols_to_use:
            try:
                val_s = row[col_i].strip()
            except IndexError:
                continue
            if not val_s:
                continue
            try:
                val = float(val_s)
            except ValueError:
                continue
            try:
                yr, mo, _ = date_s.split('-')
                year = int(yr)
                month = int(mo)
            except (ValueError, IndexError):
                continue
            rows.append(HpiRow(
                level=level,
                code=code,
                name=name,
                year=year,
                period=month,
                freq='monthly',
                index_nsa=val,   # ZHVI is smoothed+SA; we still populate index_nsa
                index_sa=val,
            ))
    return rows


def _is_date_column(s: str) -> bool:
    if len(s) < 10:
        return False
    if s[4] != '-' or s[7] != '-':
        return False
    try:
        int(s[:4]); int(s[5:7]); int(s[8:10])
        return True
    except ValueError:
        return False


# ── Public API ──────────────────────────────────────────────────────────

def fetch(scope: ZScope, force: bool = False,
          max_rows_per_region: Optional[int] = None) -> list[HpiRow]:
    with _lock:
        if not force and scope in _mem:
            return _mem[scope]
        disk = _load_disk(scope) if not force else None
        if disk is not None:
            _mem[scope] = disk
            return disk

    logger.info(f'zillow: downloading {scope} ZHVI…')
    try:
        resp = requests.get(_url(scope), timeout=120,
                            headers={'User-Agent': 'Mozilla/5.0 (compatible; ParraMacro/1.0)'})
        if resp.status_code != 200:
            logger.warning(f'zillow {scope} HTTP {resp.status_code}')
            return []
        rows = _parse_wide(resp.text, scope, max_rows_per_region=max_rows_per_region)
        logger.info(f'zillow {scope}: parsed {len(rows)} rows')
        with _lock:
            _mem[scope] = rows
        _save_disk(scope, rows)
        return rows
    except Exception as e:
        logger.error(f'zillow {scope} fetch failed: {e}')
        return []


def fetch_metro(force: bool = False) -> list[HpiRow]:
    return fetch('metro', force=force)


def fetch_county(force: bool = False) -> list[HpiRow]:
    return fetch('county', force=force)


def fetch_zip(force: bool = False, max_rows_per_region: int = 36) -> list[HpiRow]:
    """ZIP-level default keeps the last 3 years to stay within memory."""
    return fetch('zip', force=force, max_rows_per_region=max_rows_per_region)


def clear_cache():
    with _lock:
        _mem.clear()
    for scope in ('metro', 'county', 'zip'):
        p = _cache_path(scope)
        try:
            if os.path.exists(p):
                os.remove(p)
        except OSError:
            pass
