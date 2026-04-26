"""
Cached service facade for the macro model.

The fit (fetch FRED + estimate 11 equations) is expensive, so we memoize
a single fitted Simulator. Three layers of cache, ordered fastest first:

    in-memory _state['simulator']    → 0ms
    disk pickle (this module)        → ~1s, survives worker restarts and
                                       is shared across Gunicorn workers
                                       (each one reads the same file)
    fresh build via fit_all          → 60-120s

The disk cache is what makes workers > 1 safe: after worker A finishes
the cold build and writes the pickle, worker B serves its first request
by reading from disk instead of rebuilding from scratch.
"""

from __future__ import annotations

import logging
import os
import pickle
import threading
import time
from typing import Optional

import numpy as np
import pandas as pd

from config import Config
from backend.macro_model import diagnostics
from backend.macro_model.data import build_panel
from backend.macro_model.equations import derive_auxiliary_columns
from backend.macro_model.fit_runner import ModelFitReport, fit_all
from backend.macro_model.backtest import run_backtest, BacktestResult
from backend.macro_model.simulations import (
    baseline_forecast,
    bootstrap_forecast,
    get_catalogue,
    run_shock,
)
from backend.macro_model.solver import Simulator

# Cross-worker persistence
_PERSIST_PATH = os.path.join(Config.DATA_DIR, 'macro_model_state.pkl')
_PERSIST_MAX_AGE_S = 24 * 3600   # rebuild from scratch if pickle older than 24h

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_state: dict = {
    'report': None,           # type: Optional[ModelFitReport]
    'simulator': None,        # type: Optional[Simulator]
    'baseline': None,         # type: Optional[pd.DataFrame]
    'bootstrap': None,        # type: Optional[dict[str, pd.DataFrame]]
    'shock_results': {},      # {shock_id: result dict}
    'backtests': {},          # {(train_end, flat_exog): BacktestResult}
    'fit_error': None,
    'built_at': None,
    'building': False,        # True while a background build is in flight
}


def _save_persist(report, sim, panel):
    """Pickle the build artifacts so a sibling worker can load them
    instead of rebuilding. Best-effort: any pickle failure is logged
    but not raised."""
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        tmp = _PERSIST_PATH + '.tmp'
        with open(tmp, 'wb') as f:
            pickle.dump({'report': report, 'simulator': sim, 'panel': panel,
                         'saved_at': time.time()}, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, _PERSIST_PATH)
        logger.info(f'macro_model.service: persisted state to {_PERSIST_PATH}')
    except Exception as e:
        logger.warning(f'macro_model.service: persist failed: {e}')


def _try_load_persist() -> bool:
    """Hot-load a previously-pickled build. Returns True on success.

    Validates the loaded data — a pickle with 0 equations or an empty/all-NaN
    panel is treated as corrupt and deleted, forcing a fresh build. This
    protects against the case where an old deploy wrote a hollow pickle
    that would otherwise serve a misleading 'Model built — 11 equations'
    status with empty forecast tables.
    """
    try:
        if not os.path.exists(_PERSIST_PATH):
            logger.info('macro_model.service: no pickle on disk, will build fresh')
            return False
        age = time.time() - os.path.getmtime(_PERSIST_PATH)
        if age > _PERSIST_MAX_AGE_S:
            logger.info(f'macro_model.service: pickle is {age/3600:.1f}h old, ignoring')
            return False
        size_mb = os.path.getsize(_PERSIST_PATH) / 1024 / 1024
        logger.info(f'macro_model.service: loading pickle ({size_mb:.1f}MB, {age:.0f}s old)…')
        with open(_PERSIST_PATH, 'rb') as f:
            data = pickle.load(f)

        # Sanity-check the loaded artifacts before adopting them.
        report = data.get('report')
        sim = data.get('simulator')
        if not report or len(report.fits) == 0:
            logger.warning('macro_model.service: pickle has 0 fitted equations — discarding')
            _delete_pickle('discarded: 0 equations in pickle')
            return False
        if sim is None or sim.panel is None or len(sim.panel) == 0:
            logger.warning('macro_model.service: pickle simulator has empty panel — discarding')
            _delete_pickle('discarded: empty panel')
            return False
        # Last row of every model variable (behavioral + exogenous) must have
        # NO NaN. Even a single NaN in the last row — including in exogenous
        # variables like NROU or productivity — propagates as NaN through
        # every forecast quarter via the flat-exog carry-forward and the
        # solver warm-start. (See #54 for the all-em-dash forecast incident.)
        try:
            from backend.macro_model.variables import VARIABLES
            last_row = sim.panel.iloc[-1]
            check_cols = [v.code for v in VARIABLES if v.code in sim.panel.columns]
            bad = [c for c in check_cols if pd.isna(last_row[c])]
            if bad:
                logger.warning(
                    f'macro_model.service: pickle simulator panel last row has '
                    f'NaN for cols {bad} — discarding'
                )
                _delete_pickle(f'discarded: NaN in final row for {bad}')
                return False
        except Exception as e:
            logger.warning(f'macro_model.service: pickle sanity-check raised ({e}) — discarding')
            _delete_pickle('discarded: sanity-check error')
            return False

        with _lock:
            _state['report'] = report
            _state['simulator'] = sim
            _state['baseline'] = None
            _state['bootstrap'] = None
            _state['shock_results'] = {}
            _state['backtests'] = {}
            _state['fit_error'] = None
            _state['built_at'] = data.get('saved_at', time.time())
        logger.info(f'macro_model.service: hot-loaded persisted state '
                    f'({len(report.fits)} equations, panel {sim.panel.shape})')
        return True
    except Exception as e:
        logger.warning(f'macro_model.service: persist load failed (will rebuild): {e}')
        _delete_pickle(f'load exception: {e}')
        return False


def _delete_pickle(reason: str = ''):
    try:
        os.remove(_PERSIST_PATH)
        logger.info(f'macro_model.service: removed pickle at {_PERSIST_PATH}'
                    + (f' ({reason})' if reason else ''))
    except OSError:
        pass


def invalidate_pickle_on_boot():
    """Called once at app boot. Removes any leftover pickle from a previous
    deploy so the new code always builds fresh against current FRED data
    and current equation specs. The pickle is recreated after the new build
    completes and is then useful for sibling-worker hot-loads within this
    deploy lifetime."""
    if os.path.exists(_PERSIST_PATH):
        _delete_pickle('app boot — cross-deploy invalidation')


def _build_locked(start: str = '1980-01-01', force_refresh: bool = False):
    try:
        logger.info('macro_model.service: building panel + fitting equations…')
        diagnostics.record_build_start(clear=force_refresh)
        panel = build_panel(start=start, force_refresh=force_refresh)
        report = fit_all(panel=panel)

        # Refuse to construct a hollow Simulator. If 0 equations fit, the
        # dashboard would otherwise show 'built: true, n_equations: 0' and
        # render empty tables silently. Surface as a real error instead.
        if len(report.fits) == 0:
            err_msg = (
                'no equations fit successfully — every equation raised. '
                'Check /diagnostics for per-series fetch status; the most '
                'common cause is one or more FRED series returning no data '
                '(stale series ID, FRED rate-limit, or missing FRED_API_KEY).'
            )
            logger.error('macro_model.service: ' + err_msg)
            _state['simulator'] = None
            _state['fit_error'] = err_msg
            return

        sim = Simulator(report.fits, derive_auxiliary_columns(panel))
        _state['report'] = report
        _state['simulator'] = sim
        _state['baseline'] = None
        _state['bootstrap'] = None
        _state['shock_results'] = {}
        _state['backtests'] = {}
        _state['fit_error'] = None
        _state['built_at'] = time.time()
        logger.info(f'macro_model.service: fitted {len(report.fits)} equations')

        # Persist for sibling workers + restart resilience
        _save_persist(report, sim, panel)
    except Exception as e:
        logger.exception('macro_model.service: fit failed')
        _state['fit_error'] = str(e)
        _state['simulator'] = None
        diagnostics.record_build_error(e)
    finally:
        _state['building'] = False


def _build_in_background(start: str = '1980-01-01'):
    """Run the build in a daemon thread so /fit and /forecast return fast
    on the first hit instead of tripping Render's 30s request timeout.

    CRITICAL: do NOT hold _lock during the actual build. The build takes
    60+ seconds; if we held the lock the whole time, every concurrent
    /status request would block on the lock and the dashboard would
    hang on 'Checking model status…' the entire time.
    """
    def _run():
        try:
            # Build itself acquires _lock briefly inside _build_locked at the
            # state-mutation points; see service._build_locked.
            _build_locked(start=start)
        except Exception:
            logger.exception('macro_model.service: background build died')

    with _lock:
        if _state['building'] or _state['simulator'] is not None:
            return
        _state['building'] = True
    threading.Thread(target=_run, daemon=True, name='macro-model-build').start()


def ensure_built():
    """
    Non-blocking: if not built yet, try the disk pickle first (cheap), and
    only kick off a background build if that fails. Callers that need the
    simulator should check status() first.
    """
    with _lock:
        if _state['simulator'] is not None or _state['fit_error'] is not None:
            return
    # Cheap path — sibling worker may have already built and persisted.
    if _try_load_persist():
        return
    _build_in_background()


def ensure_built_sync():
    """Blocking version — only call from POST /refresh."""
    with _lock:
        if _state['simulator'] is None and _state['fit_error'] is None:
            _build_locked()


def refresh():
    """Force rebuild synchronously. Called by POST /refresh."""
    with _lock:
        _build_locked(force_refresh=True)


# ── Public ────────────────────────────────────────────────────────────

def get_fit_report() -> Optional[dict]:
    ensure_built()
    with _lock:
        r = _state['report']
        if r is None:
            return None
        return r.to_dict()


def get_variables() -> list[dict]:
    from backend.macro_model.variables import VARIABLES
    return [
        {'code': v.code, 'fred_id': v.fred_id, 'label': v.label,
         'freq': v.freq, 'transform': v.transform, 'block': v.block,
         'endogenous': v.endogenous, 'unit': v.unit}
        for v in VARIABLES
    ]


def get_shock_catalogue() -> list[dict]:
    return get_catalogue()


def _forecast_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert forecast DataFrame to JSON-safe records, inverting log transforms
    for quantity variables so the API returns interpretable levels."""
    from backend.macro_model.variables import BY_CODE
    out = []
    for ts, row in df.iterrows():
        rec = {'quarter': ts.date().isoformat()}
        for col, val in row.items():
            if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
                rec[col] = None
                continue
            v = BY_CODE.get(col)
            # Invert log transforms for quantities
            if v is not None and v.transform == 'log':
                rec[col] = float(np.exp(val))
            else:
                rec[col] = float(val)
        out.append(rec)
    return out


def get_baseline(horizon: int = 20) -> Optional[list[dict]]:
    ensure_built()
    with _lock:
        sim = _state['simulator']
        if sim is None:
            return None
        cached = _state['baseline']
        if cached is not None and len(cached) >= horizon:
            df = cached.head(horizon)
        else:
            df = baseline_forecast(sim, horizon=horizon)
            _state['baseline'] = df
    return _forecast_to_records(df)


def get_bootstrap(horizon: int = 12, n_draws: int = 30) -> Optional[dict]:
    """
    Returns {variable_code: [{quarter, p10, p50, p90}, ...]}. Smaller default
    draws than a research-grade fan chart to keep UI latency reasonable.
    """
    ensure_built()
    with _lock:
        sim = _state['simulator']
        if sim is None:
            return None

        cache_key = (horizon, n_draws)
        if _state['bootstrap'] is not None and _state['bootstrap'].get('key') == cache_key:
            bands = _state['bootstrap']['bands']
        else:
            bands = bootstrap_forecast(sim, horizon=horizon, n_draws=n_draws)
            _state['bootstrap'] = {'key': cache_key, 'bands': bands}

    from backend.macro_model.variables import BY_CODE
    out: dict = {}
    for code, df in bands.items():
        v = BY_CODE.get(code)
        recs = []
        for ts, row in df.iterrows():
            rec = {'quarter': ts.date().isoformat()}
            for col, val in row.items():
                if v is not None and v.transform == 'log':
                    rec[col] = float(np.exp(val))
                else:
                    rec[col] = float(val)
            recs.append(rec)
        out[code] = recs
    return out


def run_shock_api(shock_id: str, horizon: int = 20) -> Optional[dict]:
    ensure_built()
    with _lock:
        sim = _state['simulator']
        if sim is None:
            return None
        cache_key = (shock_id, horizon)
        if cache_key in _state['shock_results']:
            result = _state['shock_results'][cache_key]
        else:
            result = run_shock(sim, shock_id, horizon=horizon)
            _state['shock_results'][cache_key] = result

    return {
        'shock': result['shock'],
        'baseline': _forecast_to_records(result['baseline']),
        'shocked':  _forecast_to_records(result['shocked']),
        'irf':      _forecast_to_records(result['irf']),
    }


def status() -> dict:
    with _lock:
        return {
            'built': _state['simulator'] is not None,
            'building': _state['building'],
            'fit_error': _state['fit_error'],
            'built_at': _state['built_at'],
            'n_equations': len(_state['report'].fits) if _state['report'] else 0,
        }


def get_diagnostics() -> dict:
    """Per-series fetch status + per-equation fit status."""
    return diagnostics.snapshot()


def get_backtest(train_end: str = '2019-12-31', flat_exog: bool = False) -> Optional[dict]:
    """
    Run (or serve cached) backtest. Note: this requires access to the full
    panel (including data after train_end), which the service already has
    because it built the panel at startup. For repeated calls with the same
    parameters, the result is cached.
    """
    ensure_built()
    with _lock:
        key = (train_end, bool(flat_exog))
        cached = _state['backtests'].get(key)
        if cached is not None:
            return cached.to_dict()

    # Rebuild panel (the simulator's panel may have been extended by forecasts)
    try:
        panel = build_panel()
        result = run_backtest(train_end=train_end, panel=panel, flat_exog=flat_exog)
    except Exception as e:
        logger.exception('backtest failed')
        return {'error': str(e)}

    with _lock:
        _state['backtests'][key] = result
    return result.to_dict()
