"""
Service facade: single source of truth for the /house-prices product.

On first use (ensure_built), pulls data sources, parses into a single
list of HpiRow records, groups per entity, and computes summary
statistics. In-memory cache is keyed by (level, code); a full rebuild
re-downloads everything.

Memory budget: Zillow metro (~100MB) and county (~50MB) CSVs balloon
2-3x in Python object overhead. On Render's 512MB free tier this OOMs;
on a 1GB+ plan it's fine.

Default: Zillow ENABLED (we expect a ≥1GB worker now). To force-disable
on a constrained plan, set HPI_INCLUDE_ZILLOW=0 in the environment.

Built is idempotent; refresh() forces re-download.
"""

from __future__ import annotations

import logging
import os
import pickle
import threading
import time
from typing import Optional

from config import Config

# Cross-worker persistence — pickle the build artifacts so a sibling
# Gunicorn worker reading the same file can hot-load instead of rebuilding.
_PERSIST_PATH = os.path.join(Config.DATA_DIR, 'house_prices_state.pkl')
_PERSIST_MAX_AGE_S = 7 * 24 * 3600   # weekly rebuild matches the FHFA cache TTL

from backend.house_prices import diagnostics
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
    'building': False,
}


# Zillow metro + county load is heavy (~150MB raw, 2-3x in memory after
# Python object overhead). Default ON; set HPI_INCLUDE_ZILLOW=0 to disable
# on a memory-constrained plan. ZIP file (~100MB) is still off by default
# even on 1GB plans because it adds another ~250MB peak; opt in via
# HPI_INCLUDE_ZILLOW_ZIP=1 only on 2GB+ workers.
INCLUDE_ZILLOW = os.environ.get('HPI_INCLUDE_ZILLOW', '1').lower() in ('1', 'true', 'yes')
INCLUDE_ZILLOW_ZIP = os.environ.get('HPI_INCLUDE_ZILLOW_ZIP', '0').lower() in ('1', 'true', 'yes')


def _build_locked(include_zillow_zip: bool = False, force_refresh: bool = False):
    """The actual build. Runs OUTSIDE the global lock — only state mutations
    (the four assignments at the bottom) happen under the lock so concurrent
    /status calls don't block on the long network/parse path."""
    err: Optional[BaseException] = None
    rows: list[HpiRow] = []
    summaries: dict = {}
    grouped: dict = {}
    try:
        diagnostics.record_build_start(clear=force_refresh)
        logger.info('house_prices: fetching FHFA master + county…')
        master_rows = fhfa.fetch_master(force=force_refresh)
        rows.extend(master_rows)
        rows.extend(fhfa.fetch_county(force=force_refresh))

        # If the master parser produced 0 state or 0 region rows (schema
        # drift or a level naming change on FHFA's side), fall back to the
        # dedicated state and division CSVs. Cheap (~2MB each).
        n_states = sum(1 for r in master_rows if r.level == 'state')
        n_regions = sum(1 for r in master_rows if r.level == 'region')
        if n_states == 0:
            logger.warning('house_prices: master returned 0 state rows — falling back to hpi_at_bdl_state.csv')
            rows.extend(fhfa._fetch_fallback('state', fhfa._FALLBACK_STATE_URL))
        if n_regions == 0:
            logger.warning('house_prices: master returned 0 region rows — falling back to hpi_at_bdl_division.csv')
            rows.extend(fhfa._fetch_fallback('region', fhfa._FALLBACK_DIVISION_URL))
        logger.info('house_prices: fetching Case-Shiller (national + 20 cities)…')
        rows.extend(case_shiller.fetch_all())

        if INCLUDE_ZILLOW:
            logger.info('house_prices: fetching Zillow metro + county…')
            rows.extend(zillow.fetch_metro(force=force_refresh))
            rows.extend(zillow.fetch_county(force=force_refresh))
            if include_zillow_zip or INCLUDE_ZILLOW_ZIP:
                logger.info('house_prices: fetching Zillow ZIP (last 36 months)…')
                rows.extend(zillow.fetch_zip(force=force_refresh))
        else:
            logger.info('house_prices: Zillow disabled (HPI_INCLUDE_ZILLOW=0).')

        grouped = group_by_entity(rows)
        summaries = {k: summarize(v) for k, v in grouped.items()}
        summaries = {k: v for k, v in summaries.items() if v is not None}
        logger.info(f'house_prices: parsed {len(rows)} rows across {len(summaries)} entities')
    except Exception as e:
        err = e
        logger.exception('house_prices build failed')

    # Brief locked region: state mutation only.
    with _lock:
        if err is None and summaries:
            _state['rows'] = rows
            _state['grouped'] = grouped
            _state['summaries'] = summaries
            _state['built_at'] = time.time()
            _state['build_error'] = None
        elif err is not None:
            _state['build_error'] = str(err)
        else:
            _state['build_error'] = (
                'no entities produced — likely a parser mismatch on the FHFA CSV. '
                'Hit /api/house-prices/diagnostics to see per-source row counts.'
            )
        _state['building'] = False
    diagnostics.record_build_finish(error=err)


def _save_persist():
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        tmp = _PERSIST_PATH + '.tmp'
        with open(tmp, 'wb') as f:
            pickle.dump({
                'rows': _state['rows'],
                'grouped': _state['grouped'],
                'summaries': _state['summaries'],
                'saved_at': time.time(),
            }, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, _PERSIST_PATH)
        logger.info(f'house_prices: persisted state to {_PERSIST_PATH}')
    except Exception as e:
        logger.warning(f'house_prices: persist failed: {e}')


def _delete_pickle(reason: str = ''):
    try:
        os.remove(_PERSIST_PATH)
        logger.info(f'house_prices: removed pickle at {_PERSIST_PATH}'
                    + (f' ({reason})' if reason else ''))
    except OSError:
        pass


def invalidate_pickle_on_boot():
    """Called once at app boot. Removes any leftover pickle from a previous
    deploy. The pickle is regenerated after the fresh build completes and
    is then useful for sibling-worker hot-loads within this deploy."""
    if os.path.exists(_PERSIST_PATH):
        _delete_pickle('app boot — cross-deploy invalidation')


def _try_load_persist() -> bool:
    """Hot-load a previously-pickled build. Validates that the pickle has
    real data — pickles from older deploys with 0 entities or missing
    state/region rows are treated as corrupt and discarded so the new
    code path actually runs."""
    try:
        if not os.path.exists(_PERSIST_PATH):
            logger.info('house_prices: no pickle on disk, will build fresh')
            return False
        age = time.time() - os.path.getmtime(_PERSIST_PATH)
        if age > _PERSIST_MAX_AGE_S:
            logger.info(f'house_prices: pickle is {age/3600:.1f}h old, ignoring')
            return False
        size_mb = os.path.getsize(_PERSIST_PATH) / 1024 / 1024
        logger.info(f'house_prices: loading pickle ({size_mb:.1f}MB, {age:.0f}s old)…')
        with open(_PERSIST_PATH, 'rb') as f:
            data = pickle.load(f)

        # Sanity-check: pickle from a broken older deploy may have empty
        # summaries or 0 states/regions. Reject and rebuild so PR #52's
        # fallback logic actually fires.
        summaries = data.get('summaries') or {}
        if len(summaries) == 0:
            logger.warning('house_prices: pickle has 0 entities — discarding')
            _delete_pickle('discarded: 0 entities')
            return False
        n_states = sum(1 for k in summaries if k[0] == 'state')
        n_regions = sum(1 for k in summaries if k[0] == 'region')
        if n_states == 0 or n_regions == 0:
            logger.warning(f'house_prices: pickle missing state/region '
                           f'(states={n_states}, regions={n_regions}) — discarding')
            _delete_pickle(f'discarded: states={n_states}, regions={n_regions}')
            return False

        with _lock:
            _state['rows'] = data['rows']
            _state['grouped'] = data['grouped']
            _state['summaries'] = summaries
            _state['build_error'] = None
            _state['built_at'] = data.get('saved_at', time.time())
        logger.info(f'house_prices: hot-loaded persisted state '
                    f'({len(summaries)} entities, {n_states} states, {n_regions} regions)')
        return True
    except Exception as e:
        logger.warning(f'house_prices: persist load failed: {e}')
        _delete_pickle(f'load exception: {e}')
        return False


def _build_in_background(include_zillow_zip: bool = False):
    """Daemon-thread build so the first /level request returns fast.

    CRITICAL: do NOT hold _lock during the actual build. _build_locked
    handles its own locked state-mutation block at the end; holding the
    outer lock for the full 60-120s download would block every concurrent
    /status request and freeze the dashboard."""
    def _run():
        try:
            _build_locked(include_zillow_zip=include_zillow_zip)
            if _state.get('summaries'):
                _save_persist()
        except Exception:
            logger.exception('house_prices background build died')

    with _lock:
        if _state['building'] or _state['summaries'] is not None:
            return
        _state['building'] = True
    threading.Thread(target=_run, daemon=True, name='hpi-build').start()


def ensure_built():
    """Non-blocking. Tries the disk pickle first (cheap), only kicks off
    a background rebuild if that fails."""
    with _lock:
        if _state['summaries'] is not None or _state['build_error'] is not None:
            return
    if _try_load_persist():
        return
    _build_in_background()


def refresh(include_zillow_zip: bool = False):
    """Force-refresh synchronously (used by POST /refresh).

    Lock is held briefly to flip 'building' true, then released so /status
    polls don't block during the rebuild. _build_locked re-acquires the
    lock at the end for the state-mutation step.
    """
    with _lock:
        if _state['building']:
            logger.info('refresh skipped: already building')
            return
        _state['building'] = True
    try:
        fhfa.clear_cache()
        zillow.clear_cache()
        _build_locked(include_zillow_zip=include_zillow_zip, force_refresh=True)
    except Exception:
        with _lock:
            _state['building'] = False
        raise


def get_diagnostics() -> dict:
    return diagnostics.snapshot()


def status() -> dict:
    with _lock:
        return {
            'built': _state['summaries'] is not None,
            'building': _state['building'],
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
