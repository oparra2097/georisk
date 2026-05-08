"""
Model diagnostics — per-source and per-equation status, exposed so the
dashboard can show exactly what's failing without forcing the admin to
dig through Render logs.

Mutated by data.py (fetch outcomes) and fit_runner.py (equation fits).
Read by routes.py as a JSON diagnostics endpoint.
"""

from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FetchStatus:
    code: str
    fred_id: str
    ok: bool = False
    n_obs: int = 0
    last_date: Optional[str] = None
    error: Optional[str] = None
    fetched_at: Optional[float] = None


@dataclass
class FitStatus:
    name: str
    ok: bool = False
    error: Optional[str] = None
    # Minimal success summary so /diagnostics can stand alone
    rsq: Optional[float] = None
    n_obs: Optional[int] = None
    chosen_lag: Optional[int] = None
    gamma: Optional[float] = None


_lock = threading.RLock()
_state = {
    'fetches': {},       # {code: FetchStatus}
    'fits': {},          # {name: FitStatus}
    'last_build_at': None,
    'last_build_error': None,   # top-level exception string, if any
}


# ── Fetch recording ──────────────────────────────────────────────────────

def record_fetch_ok(code: str, fred_id: str, n_obs: int, last_date: Optional[str]):
    with _lock:
        _state['fetches'][code] = FetchStatus(
            code=code, fred_id=fred_id, ok=True,
            n_obs=n_obs, last_date=last_date, fetched_at=time.time(),
        )


def record_fetch_fail(code: str, fred_id: str, error: str):
    with _lock:
        _state['fetches'][code] = FetchStatus(
            code=code, fred_id=fred_id, ok=False,
            error=error[:500], fetched_at=time.time(),
        )


# ── Fit recording ────────────────────────────────────────────────────────

def record_fit_ok(name: str, rsq: float, n_obs: int, chosen_lag: int, gamma: Optional[float]):
    with _lock:
        _state['fits'][name] = FitStatus(
            name=name, ok=True, rsq=float(rsq), n_obs=int(n_obs),
            chosen_lag=int(chosen_lag),
            gamma=float(gamma) if gamma is not None else None,
        )


def record_fit_fail(name: str, exc: BaseException):
    with _lock:
        _state['fits'][name] = FitStatus(
            name=name, ok=False,
            error=f'{type(exc).__name__}: {exc}'[:500],
        )


# ── Top-level build status ───────────────────────────────────────────────

def record_build_start(clear: bool = False):
    """Mark a new build. `clear=True` wipes the previous fit results so the
    user sees a clean report (use for explicit /refresh). Default keeps
    sticky state so cache-hits don't zero out per-series diagnostics."""
    with _lock:
        if clear:
            _state['fetches'] = {}
            _state['fits'] = {}
        _state['last_build_at'] = time.time()
        _state['last_build_error'] = None


def record_build_error(exc: BaseException):
    with _lock:
        _state['last_build_error'] = (
            f'{type(exc).__name__}: {exc}\n' + ''.join(traceback.format_tb(exc.__traceback__))[-1200:]
        )


# ── Read API ─────────────────────────────────────────────────────────────

def snapshot() -> dict:
    """Returns a serializable dict for the /diagnostics endpoint."""
    with _lock:
        fetches = sorted(
            (_fetch_to_dict(f) for f in _state['fetches'].values()),
            key=lambda d: (d['ok'], d['code']),
        )
        fits = sorted(
            (_fit_to_dict(f) for f in _state['fits'].values()),
            key=lambda d: (d['ok'], d['name']),
        )
        return {
            'last_build_at': _state['last_build_at'],
            'last_build_error': _state['last_build_error'],
            'n_series_ok':    sum(1 for f in _state['fetches'].values() if f.ok),
            'n_series_fail':  sum(1 for f in _state['fetches'].values() if not f.ok),
            'n_equations_ok':   sum(1 for f in _state['fits'].values() if f.ok),
            'n_equations_fail': sum(1 for f in _state['fits'].values() if not f.ok),
            'fetches': fetches,
            'fits': fits,
        }


def _fetch_to_dict(f: FetchStatus) -> dict:
    return {
        'code': f.code, 'fred_id': f.fred_id, 'ok': f.ok,
        'n_obs': f.n_obs, 'last_date': f.last_date, 'error': f.error,
    }


def _fit_to_dict(f: FitStatus) -> dict:
    return {
        'name': f.name, 'ok': f.ok,
        'rsq': f.rsq, 'n_obs': f.n_obs, 'chosen_lag': f.chosen_lag,
        'gamma': f.gamma, 'error': f.error,
    }
