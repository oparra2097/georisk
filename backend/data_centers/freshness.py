"""
Per-source data-freshness tracking for the data center risk map.

Records each successful pull and exposes a freshness summary so the
admin block can show stale-data badges. Combines:

  - markets_csv     mtime of data/datacenter_markets.csv
  - facilities_csv  mtime of data/datacenter_facilities.csv
  - drift           scanned_at field from data/datacenter_drift.json
  - sec_edgar       last successful pull recorded here
  - iso_queues      last successful pull recorded here

Stale thresholds (days) per source are configurable in STALE_DAYS.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data',
)
STATE_PATH    = os.path.join(DATA_DIR, 'datacenter_freshness.json')
DRIFT_PATH    = os.path.join(DATA_DIR, 'datacenter_drift.json')
MARKETS_CSV   = os.path.join(DATA_DIR, 'datacenter_markets.csv')
FACILITIES_CSV = os.path.join(DATA_DIR, 'datacenter_facilities.csv')

# Days after which each source is considered stale.
STALE_DAYS = {
    'markets_csv':    90,
    'facilities_csv': 90,
    'drift':           2,
    'sec_edgar':      90,
    'iso_queues':     14,
}


def _now_iso() -> str:
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'


def _read_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f'failed to read freshness state: {e}')
        return {}


def _write_state(state: dict) -> None:
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(STATE_PATH, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.warning(f'failed to write freshness state: {e}')


def record_pull(source: str, meta: dict | None = None) -> None:
    state = _read_state()
    entry = {'last_pulled': _now_iso()}
    if meta:
        entry['meta'] = meta
    state[source] = entry
    _write_state(state)


def _file_mtime_iso(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    try:
        ts = os.path.getmtime(path)
        return _dt.datetime.utcfromtimestamp(ts).replace(microsecond=0).isoformat() + 'Z'
    except OSError:
        return None


def _drift_scanned_at() -> str | None:
    if not os.path.exists(DRIFT_PATH):
        return None
    try:
        with open(DRIFT_PATH, encoding='utf-8') as f:
            j = json.load(f)
        return j.get('scanned_at')
    except Exception:
        return None


def _age_days(iso_ts: str | None) -> float | None:
    if not iso_ts:
        return None
    try:
        ts = _dt.datetime.fromisoformat(iso_ts.rstrip('Z'))
    except ValueError:
        return None
    delta = _dt.datetime.utcnow() - ts
    return round(delta.total_seconds() / 86400.0, 2)


def get_freshness() -> dict:
    """Return a freshness snapshot for all tracked sources."""
    state = _read_state()
    sources: list[dict[str, Any]] = []

    sources.append({
        'source': 'markets_csv',
        'label':  'Markets CSV',
        'last_seen': _file_mtime_iso(MARKETS_CSV),
        'meta':   {},
    })
    sources.append({
        'source': 'facilities_csv',
        'label':  'Facilities CSV',
        'last_seen': _file_mtime_iso(FACILITIES_CSV),
        'meta':   {},
    })
    sources.append({
        'source': 'drift',
        'label':  'Drift watcher',
        'last_seen': _drift_scanned_at(),
        'meta':   {},
    })
    for key, label in (('sec_edgar', 'SEC EDGAR REITs'),
                       ('iso_queues', 'ISO queues')):
        s = state.get(key, {})
        sources.append({
            'source': key,
            'label': label,
            'last_seen': s.get('last_pulled'),
            'meta':   s.get('meta', {}),
        })

    out = []
    for s in sources:
        age = _age_days(s['last_seen'])
        threshold = STALE_DAYS.get(s['source'], 30)
        s['age_days']        = age
        s['threshold_days']  = threshold
        s['status']          = 'never' if age is None else ('stale' if age > threshold else 'fresh')
        out.append(s)
    return {'sources': out, 'as_of': _now_iso()}
