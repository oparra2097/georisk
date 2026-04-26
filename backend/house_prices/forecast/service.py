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
from backend.house_prices.forecast.drivers import (
    build_panel as build_drivers,
    fetch_national_hpi,
    fetch_state_hpi,
    fetch_state_unemp,
    STATE_HPI_FRED_IDS,
)
from backend.house_prices.forecast.model import (
    HpiForecastModel,
    baseline_forecast,
    bootstrap_forecast,
    fit_national,
    fit_state,
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
    # Per-state forecasts
    'states': {},             # {state_code: HpiForecastModel}
    'state_errors': {},       # {state_code: str}
    'state_baseline': {},     # {(state_code, horizon): DataFrame}
    'state_fan': {},          # {(state_code, horizon, n_draws): DataFrame}
    'states_built_at': None,
    'states_building': False,
}


def _hpi_log_series(level: str, code: str) -> Optional[pd.Series]:
    """Read a quarterly log-HPI series for the given (level, code) from the
    existing house_prices service. Returns None if the entity has no rows
    in the grouped table or if every row's index_nsa is missing."""
    grouped = hpi_service._state.get('grouped')
    if not grouped:
        return None
    rows = grouped.get((level, code))
    if not rows:
        return None
    records: list[dict] = []
    for r in rows:
        if r.index_nsa is None or r.year is None or r.period is None:
            continue
        # Only quarterly rows are usable for the ECM
        if getattr(r, 'freq', '') != 'quarterly':
            continue
        month = r.period * 3
        ts = pd.Timestamp(year=r.year, month=month, day=1) + pd.offsets.MonthEnd(0)
        records.append({'date': ts, 'index': float(r.index_nsa)})
    if not records:
        return None
    # Median-aggregate when multiple records share a date. drop_duplicates
    # would arbitrarily keep whichever record happened to be first in the
    # FHFA master CSV, and FHFA's master can interleave multiple index
    # series under the same place_id — see the +143% Q1 YoY incident.
    # Median is robust to outliers and gives the same answer when there's
    # only one record per date.
    df = (pd.DataFrame(records)
            .groupby('date')['index'].median()
            .sort_index()
            .to_frame('index'))
    return np.log(df['index'])


def _national_hpi_log() -> Optional[pd.Series]:
    """National HPI series in log-level for the forecast model.

    Source priority (matches the dashboard summary tile):
      1. FHFA all-transactions ('USA') from house_prices.grouped — already
         loaded by the house_prices service, no new HTTP call, and matches
         what the hero tile shows.
      2. Case-Shiller National Composite ('CS_NATIONAL') — tile's fallback.
      3. FRED USSTHPI — last resort. Same series as (1) routed through
         FRED's mirror; covers the case where FHFA's master CSV parsed
         empty for any reason.

    The previous ordering put FRED first, which meant a missing FRED key
    or a 429 rate-limit hit on USSTHPI silently aborted the whole build.
    """
    s = _hpi_log_series('national', 'USA')
    if s is not None and len(s) >= 30:
        return s
    s = _hpi_log_series('national', 'CS_NATIONAL')
    if s is not None and len(s) >= 30:
        logger.info('hpi_forecast.service: FHFA USA missing, using Case-Shiller national')
        return s
    raw = fetch_national_hpi(start='1980-01-01')
    if raw is not None and len(raw) >= 30:
        logger.info('hpi_forecast.service: house_prices grouped empty, using FRED USSTHPI')
        return np.log(raw.where(raw > 0)).dropna()
    # Last-ditch: any 'national' entry in grouped
    grouped = hpi_service._state.get('grouped') or {}
    for (lvl, code) in grouped:
        if lvl == 'national':
            s2 = _hpi_log_series('national', code)
            if s2 is not None and len(s2) >= 30:
                logger.info(f'hpi_forecast.service: falling back to national/{code}')
                return s2
    return None


def _national_source_used() -> Optional[str]:
    if _hpi_log_series('national', 'USA') is not None:
        return 'FHFA via house_prices.grouped (USA)'
    if _hpi_log_series('national', 'CS_NATIONAL') is not None:
        return 'Case-Shiller National Composite'
    raw = fetch_national_hpi(start='1980-01-01')
    if raw is not None and len(raw) >= 30:
        return 'FHFA all-transactions via FRED (USSTHPI)'
    return None


def _state_hpi_log(state_code: str) -> Optional[pd.Series]:
    """Per-state HPI in log-level. Same priority logic as national:
    1. house_prices grouped (already loaded), 2. FRED <STATE>STHPI."""
    s = _hpi_log_series('state', state_code)
    if s is not None and len(s) >= 30:
        return s
    raw = fetch_state_hpi(state_code)
    if raw is not None and not raw.empty:
        s2 = np.log(raw.where(raw > 0)).dropna()
        if len(s2) >= 30:
            return s2
    return None


def _list_state_codes() -> list[str]:
    """Every state code that the house_prices service has data for."""
    grouped = hpi_service._state.get('grouped')
    if not grouped:
        return []
    return sorted({code for (lvl, code) in grouped if lvl == 'state'})


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

        # Now fit each state in the same thread — drivers are already loaded
        # and the inner OLS is fast (~50ms per state). We deliberately reuse
        # the same drivers panel across states.
        _build_state_models(drivers)
    except Exception as e:
        logger.exception('hpi_forecast.service: build failed')
        with _lock:
            _state['fit_error'] = str(e)
            _state['model'] = None
    finally:
        with _lock:
            _state['building'] = False


def _build_state_models(drivers: pd.DataFrame):
    """Fit a per-state model for each of 50 states + DC. HPI source
    priority is: house_prices.grouped state row (already loaded) →
    FRED <STATE>STHPI. State unemployment comes from FRED <STATE>UR
    with a fallback to the national 'unemp' column.

    Per-state failures are recorded into _state['state_errors'] but
    NEVER abort the build — even if all 51 states fail, the national
    forecast is still served and the user sees concrete reasons in
    /forecast/debug. This was the failure mode that surfaced as
    "build failed" on the dashboard: when FRED was rate-limited or
    its key was missing, EVERY state errored, and the previous code
    treated the empty fitted dict as a hard failure."""
    state_codes = sorted(STATE_HPI_FRED_IDS.keys())
    with _lock:
        _state['states_building'] = True
    fitted: dict[str, HpiForecastModel] = {}
    errors: dict[str, str] = {}
    fetch_attempts = 0
    for code in state_codes:
        try:
            hpi = _state_hpi_log(code)
            fetch_attempts += 1
            if hpi is None:
                errors[code] = 'no usable HPI series (FHFA grouped + FRED both empty/short)'
                continue
            state_drivers = drivers.copy()
            try:
                su = fetch_state_unemp(code)
            except Exception as e:
                logger.warning(f'hpi_forecast.service: state unemp fetch raised for {code}: {e}')
                su = None
            if su is not None and not su.empty:
                state_drivers['state_unemp'] = su
            else:
                state_drivers['state_unemp'] = state_drivers['unemp']
            model = fit_state(hpi, state_drivers, code)
            fitted[code] = model
        except Exception as e:
            errors[code] = str(e)
            logger.warning(f'hpi_forecast.service: fit_state({code}) failed: {e}')
        # Tiny sleep to stay well under FRED's 120/min rate cap on the
        # cold build (we fetch 2 series per state on top of the 5 driver
        # series; without this we can clip the limit and start getting
        # 429s mid-loop).
        time.sleep(0.05)
    logger.info(f'hpi_forecast.service: state build complete — '
                f'{len(fitted)} fitted, {len(errors)} skipped of {fetch_attempts} attempts')
    with _lock:
        _state['states'] = fitted
        _state['state_errors'] = errors
        _state['state_baseline'] = {}
        _state['state_fan'] = {}
        _state['states_built_at'] = time.time()
        _state['states_building'] = False
    logger.info(f'hpi_forecast.service: fitted {len(fitted)}/{len(state_codes)} state models '
                f'({len(errors)} skipped)')


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


def debug_panel() -> dict:
    """One-shot diagnostic: shows what's in the national model's panel.
    Used to verify HPI levels are consistent with the source-of-truth
    (e.g. Case-Shiller national ≈ 312 in Q4 2025 — if the panel reads
    something wildly different, there's a data feed bug)."""
    ensure_built()
    with _lock:
        m = _state['model']
        if m is None:
            return {'error': 'model not built', 'status': status()}
        panel = m.panel
        last8 = panel.tail(8)
        rows = []
        for ts, row in last8.iterrows():
            rec = {'quarter': ts.date().isoformat()}
            for col in panel.columns:
                v = row[col]
                if isinstance(v, float) and not (np.isnan(v) or np.isinf(v)):
                    # `hpi` is log; expose both raw log and exp level for clarity
                    if col == 'hpi':
                        rec['hpi_log'] = round(float(v), 4)
                        rec['hpi_level'] = round(float(np.exp(v)), 2)
                    else:
                        rec[col] = round(float(v), 4)
            rows.append(rec)
        states_built = list(_state['states'].keys())
        state_errors = dict(_state['state_errors'])
        states_building = bool(_state.get('states_building'))
        return {
            'n_obs': int(len(panel)),
            'panel_start': panel.index.min().date().isoformat(),
            'panel_end': panel.index.max().date().isoformat(),
            'last_8_quarters': rows,
            'raw_hpi_records_USA': _hpi_records_debug('USA'),
            'raw_hpi_records_CS_NATIONAL': _hpi_records_debug('CS_NATIONAL'),
            'national_source_used': _national_source_used(),
            'state_codes_in_grouped': _list_state_codes(),
            'states_building': states_building,
            'states_fitted': sorted(states_built),
            'states_skipped': state_errors,
        }


def _hpi_records_debug(code: str) -> list[dict]:
    """Last 8 quarterly raw HpiRow records for the given code — bypasses the
    log + median aggregation so we can see if FHFA is shipping multiple
    index values under the same place_id."""
    grouped = hpi_service._state.get('grouped') or {}
    rows = grouped.get(('national', code)) or []
    out = []
    for r in rows:
        if getattr(r, 'freq', '') != 'quarterly':
            continue
        if r.year is None or r.period is None or r.index_nsa is None:
            continue
        out.append({
            'year': r.year, 'period': r.period,
            'name': getattr(r, 'name', ''),
            'index_nsa': float(r.index_nsa),
        })
    out.sort(key=lambda d: (d['year'], d['period']))
    return out[-8:]


# ── Per-state accessors ─────────────────────────────────────────────────

def get_state_list() -> dict:
    """All states with a fitted forecast model, plus skipped states with
    the reason they were skipped, plus the source-of-truth notice. Always
    returns a dict so the UI can show 'no states fitted because…' instead
    of just an empty dropdown."""
    ensure_built()
    with _lock:
        fitted = []
        for code, m in _state['states'].items():
            fitted.append({
                'code': code,
                'panel_start': m.panel_start.date().isoformat(),
                'panel_end': m.panel_end.date().isoformat(),
                'rsq': round(float(m.fit.rsq), 3),
                'n_obs': int(m.fit.n_obs),
            })
        fitted.sort(key=lambda d: d['code'])
        skipped = [
            {'code': code, 'reason': reason}
            for code, reason in sorted(_state['state_errors'].items())
        ]
        building = bool(_state.get('states_building'))
        built_at = _state.get('states_built_at')
    return {
        'states': fitted,
        'skipped': skipped,
        'building': building,
        'built_at': built_at,
        'available_state_codes': _list_state_codes(),
    }


def get_state_baseline(state_code: str, horizon: int = 8) -> Optional[list[dict]]:
    ensure_built()
    with _lock:
        m = _state['states'].get(state_code)
        if m is None:
            return None
        cached = _state['state_baseline'].get((state_code, horizon))
    if cached is None:
        df = baseline_forecast(m, horizon=horizon)
        with _lock:
            _state['state_baseline'][(state_code, horizon)] = df
    else:
        df = cached
    return _df_to_records(df)


def get_state_fan(state_code: str, horizon: int = 8, n_draws: int = 200) -> Optional[list[dict]]:
    ensure_built()
    with _lock:
        m = _state['states'].get(state_code)
        if m is None:
            return None
        cached = _state['state_fan'].get((state_code, horizon, n_draws))
    if cached is None:
        df = bootstrap_forecast(m, horizon=horizon, n_draws=n_draws)
        with _lock:
            _state['state_fan'][(state_code, horizon, n_draws)] = df
    else:
        df = cached
    return _df_to_records(df)


def get_state_fit(state_code: str) -> Optional[dict]:
    ensure_built()
    with _lock:
        m = _state['states'].get(state_code)
        if m is None:
            return None
    return {
        'state_code': state_code,
        'panel_start': m.panel_start.date().isoformat(),
        'panel_end': m.panel_end.date().isoformat(),
        'fit': m.fit.to_dict(),
    }


def run_state_shock(state_code: str, shock_id: str, horizon: int = 8) -> Optional[dict]:
    """Per-state shock IRF — uses the same shock catalogue as national."""
    from backend.house_prices.forecast.model import shock_forecast
    ensure_built()
    with _lock:
        m = _state['states'].get(state_code)
        if m is None:
            return None
    result = shock_forecast(m, shock_id, horizon=horizon)
    return {
        'shock':    result['shock'],
        'baseline': _df_to_records(result['baseline']),
        'shocked':  _df_to_records(result['shocked']),
        'irf':      _df_to_records(result['irf']),
    }


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
