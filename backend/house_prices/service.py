"""
Service facade: single source of truth for the /house-prices product.

On first use (ensure_built), pulls the three data sources, parses into a
single list of HpiRow records, groups per entity, and computes summary
statistics. In-memory cache is keyed by (level, code); a full rebuild
re-downloads everything.

Built is idempotent; refresh() forces re-download.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from backend.house_prices.fetchers import fhfa, case_shiller, zillow
from backend.house_prices.fetchers.fhfa import HpiRow
from backend.house_prices.indices import group_by_entity, history, summarize
from backend.house_prices.sources import CENSUS_REGIONS, SOURCES

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_state: dict = {
    'rows': None,              # raw list[HpiRow]
    'grouped': None,           # {(level, code): list[HpiRow]}
    'summaries': None,         # {(level, code): summary dict}
    'built_at': None,
    'build_error': None,
}


def _build_locked(include_zillow_zip: bool = False):
    try:
        logger.info('house_prices: fetching FHFA master + county…')
        rows: list[HpiRow] = []
        rows.extend(fhfa.fetch_master())
        rows.extend(fhfa.fetch_county())
        logger.info('house_prices: fetching Case-Shiller (national + 20 cities)…')
        rows.extend(case_shiller.fetch_all())
        logger.info('house_prices: fetching Zillow metro + county…')
        rows.extend(zillow.fetch_metro())
        rows.extend(zillow.fetch_county())
        if include_zillow_zip:
            logger.info('house_prices: fetching Zillow ZIP (last 36 months)…')
            rows.extend(zillow.fetch_zip())

        grouped = group_by_entity(rows)
        summaries = {k: summarize(v) for k, v in grouped.items()}
        summaries = {k: v for k, v in summaries.items() if v is not None}

        _state['rows'] = rows
        _state['grouped'] = grouped
        _state['summaries'] = summaries
        _state['built_at'] = time.time()
        _state['build_error'] = None
        logger.info(f'house_prices: built with {len(rows)} rows across {len(summaries)} entities')
    except Exception as e:
        logger.exception('house_prices build failed')
        _state['build_error'] = str(e)


def ensure_built():
    with _lock:
        if _state['summaries'] is None and _state['build_error'] is None:
            _build_locked()


def refresh(include_zillow_zip: bool = False):
    with _lock:
        fhfa.clear_cache()
        zillow.clear_cache()
        _build_locked(include_zillow_zip=include_zillow_zip)


def status() -> dict:
    with _lock:
        return {
            'built': _state['summaries'] is not None,
            'built_at': _state['built_at'],
            'n_entities': len(_state['summaries']) if _state['summaries'] else 0,
            'n_rows': len(_state['rows']) if _state['rows'] else 0,
            'build_error': _state['build_error'],
        }


# ── Read API ────────────────────────────────────────────────────────────

def get_sources() -> list[dict]:
    return [{
        'id': s.id, 'name': s.name, 'publisher': s.publisher,
        'license': s.license, 'freq': s.freq, 'levels': list(s.levels),
        'lag_days': s.lag_days,
    } for s in SOURCES]


def get_summary() -> dict:
    """National-level summary + top regions for the dashboard hero."""
    ensure_built()
    with _lock:
        summaries = _state['summaries'] or {}
    nat = None
    # Prefer FHFA national ('USA'), fall back to Case-Shiller
    for (lvl, code), s in summaries.items():
        if lvl == 'national' and code == 'USA':
            nat = s; break
    if nat is None:
        for (lvl, code), s in summaries.items():
            if lvl == 'national':
                nat = s; break

    regions = [s for (lvl, code), s in summaries.items() if lvl == 'region']
    regions.sort(key=lambda s: s.get('yoy_pct') or 0, reverse=True)

    return {
        'national': nat,
        'regions': regions,
        'n_entities': len(summaries),
    }


def get_level(level: str) -> list[dict]:
    """All summaries at a given geographic level, sorted by YoY descending."""
    ensure_built()
    with _lock:
        summaries = _state['summaries'] or {}
    out = [s for (lvl, _), s in summaries.items() if lvl == level]
    out.sort(key=lambda s: s.get('yoy_pct') if s.get('yoy_pct') is not None else float('-inf'),
             reverse=True)
    return out


def get_entity(level: str, code: str) -> Optional[dict]:
    """One entity's summary + time-series history."""
    ensure_built()
    with _lock:
        summaries = _state['summaries'] or {}
        grouped = _state['grouped'] or {}
    key = (level, code)
    s = summaries.get(key)
    if s is None:
        return None
    hist = history(grouped.get(key, []), min_year=2000)
    return {**s, 'history': hist}


def get_history(level: str, code: str, min_year: int = 2000) -> Optional[list[dict]]:
    ensure_built()
    with _lock:
        grouped = _state['grouped'] or {}
    rows = grouped.get((level, code))
    if not rows:
        return None
    return history(rows, min_year=min_year)
