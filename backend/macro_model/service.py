"""
Cached service facade for the macro model.

The fit (fetch FRED + estimate 11 equations) is expensive, so we memoize
a single fitted Simulator and rebuild it only on demand (startup warmup
or manual refresh). Baseline forecast, bootstrap, and shock results are
computed lazily and cached until the next rebuild.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import numpy as np
import pandas as pd

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


def _build_locked(start: str = '1980-01-01', force_refresh: bool = False):
    try:
        logger.info('macro_model.service: building panel + fitting equations…')
        diagnostics.record_build_start(clear=force_refresh)
        panel = build_panel(start=start, force_refresh=force_refresh)
        report = fit_all(panel=panel)
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
    except Exception as e:
        logger.exception('macro_model.service: fit failed')
        _state['fit_error'] = str(e)
        _state['simulator'] = None
        diagnostics.record_build_error(e)
    finally:
        _state['building'] = False


def _build_in_background(start: str = '1980-01-01'):
    """Run the build in a daemon thread so /fit and /forecast return fast
    on the first hit instead of tripping Render's 30s request timeout."""
    def _run():
        try:
            with _lock:
                if _state['simulator'] is not None:
                    return
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
    Non-blocking: if not built yet, kick off a background build and return.
    Callers that need the simulator should check status() first.
    """
    with _lock:
        if _state['simulator'] is not None or _state['fit_error'] is not None:
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
