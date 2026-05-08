"""
HPI diagnostics — per-source fetch status and build state, exposed so the
dashboard can surface exactly which data feed failed without reading
Render logs.

Mutated by the fetcher modules and service.py; read by routes.py.
"""

from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass
from typing import Optional


@dataclass
class FetchStatus:
    source_id: str          # e.g. 'fhfa_master', 'case_shiller', 'zillow_metro'
    label: str
    ok: bool = False
    n_rows: int = 0
    error: Optional[str] = None
    fetched_at: Optional[float] = None


_lock = threading.RLock()
_state = {
    'fetches': {},                # {source_id: FetchStatus}
    'last_build_at': None,
    'last_build_error': None,
    'building': False,
}


def record_fetch_ok(source_id: str, label: str, n_rows: int):
    with _lock:
        _state['fetches'][source_id] = FetchStatus(
            source_id=source_id, label=label, ok=True,
            n_rows=int(n_rows), fetched_at=time.time(),
        )


def record_fetch_fail(source_id: str, label: str, error: str):
    with _lock:
        _state['fetches'][source_id] = FetchStatus(
            source_id=source_id, label=label, ok=False,
            error=str(error)[:500], fetched_at=time.time(),
        )


def record_build_start(clear: bool = False):
    with _lock:
        if clear:
            _state['fetches'] = {}
        _state['last_build_at'] = time.time()
        _state['last_build_error'] = None
        _state['building'] = True


def record_build_finish(error: Optional[BaseException] = None):
    with _lock:
        _state['building'] = False
        if error is not None:
            tb = ''.join(traceback.format_tb(error.__traceback__))[-1200:]
            _state['last_build_error'] = f'{type(error).__name__}: {error}\n{tb}'


def is_building() -> bool:
    with _lock:
        return bool(_state['building'])


def snapshot() -> dict:
    with _lock:
        fetches = sorted(
            (
                {
                    'source_id': f.source_id, 'label': f.label, 'ok': f.ok,
                    'n_rows': f.n_rows, 'error': f.error,
                    'fetched_at': f.fetched_at,
                }
                for f in _state['fetches'].values()
            ),
            key=lambda d: (d['ok'], d['source_id']),
        )
        return {
            'last_build_at': _state['last_build_at'],
            'last_build_error': _state['last_build_error'],
            'building': _state['building'],
            'n_sources_ok':   sum(1 for f in _state['fetches'].values() if f.ok),
            'n_sources_fail': sum(1 for f in _state['fetches'].values() if not f.ok),
            'fetches': fetches,
        }
