"""
Service facade for the HPI forecast.

Wraps drivers + model with a thread-safe build cache. The fit is cheap
enough (~1s on n=170 quarters) that we don't bother pickling — but we
do memoize per-process so the API serves forecasts without re-fitting.

Build state machine:
    None             not built yet
    'building'       background thread is fitting
    HpiForecastModel ready
    error string     fit failed (returned as-is to the API)
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import numpy as np
import pandas as pd

from backend.house_prices import service as hpi_service
from backend.house_prices.forecast.drivers import build_panel as build_drivers
from backend.house_prices.forecast.model import (
    HpiForecastModel,
    baseline_forecast,
    bootstrap_forecast,
    fit_national,
    get_shock_catalogue,
    shock_forecast,
)

logger = logging.getLogger(__name__)


_lock = threading.RLock()
_state: dict = {
    'model': None,            # type: Optional[HpiForecastModel]
    'fit_error': None,        # str
    'baseline': None,         # DataFrame cache
    'bootstrap': None,        # {(horizon, n_draws): DataFrame}
    'shocks': {},             # {shock_id: dict}
    'building': False,
    'built_at': None,
}


def _national_hpi_log() -> Optional[pd.Series]:
    """Pull the national FHFA HPI series from the existing house_prices
    service and convert to a quarterly log-level pandas Series.

    Returns None if HPI hasn't been built yet (caller should ensure_built
    first).
    """
    grouped = hpi_service._state.get('grouped')
    if not grouped:
        return None
    rows = grouped.get(('national', 'USA'))
    if not rows:
        return None
    # HpiRow.year, .period (1-4), .index_nsa
    records: list[dict] = []
    for r in rows:
        if r.index_nsa is None or r.year is None or r.period is None:
            continue
        # Quarter-end timestamp
        month = r.period * 3
        ts = pd.Timestamp(year=r.year, month=month, day=1) + pd.offsets.MonthEnd(0)
        records.append({'date': ts, 'index': float(r.index_nsa)})
    if not records:
        return None
    df = pd.DataFrame(records).sort_values('date').drop_duplicates('date').set_index('date')
    return np.log(df['index'])


def _build_locked():
    try:
        logger.info('hpi_forecast.service: ensure HPI built (so we can read national series)…')
        hpi_service.ensure_built()
        # ensure_built is non-blocking — wait for the build to finish (max 120s)
        for _ in range(120):
            s = hpi_service.status()
            if s.get('built') or s.get('build_error'):
                break
            time.sleep(1)

        hpi = _national_hpi_log()
        if hpi is None:
            with _lock:
                _state['fit_error'] = (
                    'national FHFA HPI series unavailable; '
                    'check /api/house-prices/diagnostics for fetch failures.'
                )
            return

        drivers = build_drivers(start='1980-01-01')
        if drivers.empty:
            with _lock:
                _state['fit_error'] = (
                    'macro drivers (mortgage30 / real_income / unemp / fedfunds / cpi) '
                    'returned empty — verify FRED_API_KEY in environment.'
                )
            return

        model = fit_national(hpi, drivers)
        with _lock:
            _state['model'] = model
            _state['fit_error'] = None
            _state['baseline'] = None
            _state['bootstrap'] = None
            _state['shocks'] = {}
            _state['built_at'] = time.time()
        logger.info('hpi_forecast.service: national model built')
    except Exception as e:
        logger.exception('hpi_forecast.service: build failed')
        with _lock:
            _state['fit_error'] = str(e)
            _state['model'] = None
    finally:
        with _lock:
            _state['building'] = False


def _build_in_background():
    def _run():
        try:
            _build_locked()
        except Exception:
            logger.exception('hpi_forecast.service: background thread died')
    with _lock:
        if _state['building'] or _state['model'] is not None:
            return
        _state['building'] = True
    threading.Thread(target=_run, daemon=True, name='hpi-forecast-build').start()


def ensure_built():
    with _lock:
        if _state['model'] is not None or _state['fit_error'] is not None:
            return
    _build_in_background()


def refresh():
    with _lock:
        _state['model'] = None
        _state['fit_error'] = None
        _state['baseline'] = None
        _state['bootstrap'] = None
        _state['shocks'] = {}
        _state['building'] = False
    _build_in_background()


# ── Public API ──────────────────────────────────────────────────────────

def status() -> dict:
    with _lock:
        return {
            'built': _state['model'] is not None,
            'building': _state['building'],
            'fit_error': _state['fit_error'],
            'built_at': _state['built_at'],
        }


def get_fit_report() -> Optional[dict]:
    ensure_built()
    with _lock:
        m = _state['model']
        if m is None:
            return None
        return {
            'panel_start': m.panel_start.date().isoformat(),
            'panel_end': m.panel_end.date().isoformat(),
            'fit': m.fit.to_dict(),
        }


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    out = []
    for ts, row in df.iterrows():
        rec = {'quarter': ts.date().isoformat()}
        for col, val in row.items():
            if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
                rec[col] = None
            else:
                rec[col] = float(val)
        out.append(rec)
    return out


def get_baseline(horizon: int = 8) -> Optional[list[dict]]:
    ensure_built()
    with _lock:
        m = _state['model']
        if m is None:
            return None
        cached = _state['baseline']
    if cached is None or len(cached) < horizon:
        df = baseline_forecast(m, horizon=horizon)
        with _lock:
            _state['baseline'] = df
    else:
        df = cached.head(horizon)
    return _df_to_records(df)


def get_fan(horizon: int = 8, n_draws: int = 200) -> Optional[list[dict]]:
    ensure_built()
    with _lock:
        m = _state['model']
        if m is None:
            return None
    df = bootstrap_forecast(m, horizon=horizon, n_draws=n_draws)
    return _df_to_records(df)


def get_shock_list() -> list[dict]:
    return get_shock_catalogue()


def run_shock(shock_id: str, horizon: int = 8) -> Optional[dict]:
    ensure_built()
    with _lock:
        m = _state['model']
        if m is None:
            return None
        cached = _state['shocks'].get((shock_id, horizon))
    if cached is None:
        result = shock_forecast(m, shock_id, horizon=horizon)
        cached = {
            'shock':    result['shock'],
            'baseline': _df_to_records(result['baseline']),
            'shocked':  _df_to_records(result['shocked']),
            'irf':      _df_to_records(result['irf']),
        }
        with _lock:
            _state['shocks'][(shock_id, horizon)] = cached
    return cached
