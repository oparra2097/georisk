"""
Admin tools for the Data Center Risk Map.

Atomic CSV swap + diff:  swap in a fresh markets / facilities CSV without
losing the previous one (saved as `.bak`), then compute a top-N MW-change
summary so the admin can see what shifted.

This module does NOT enforce auth; gating happens in routes.py via
auth._is_admin().
"""

from __future__ import annotations

import csv
import io
import logging
import os
import shutil
from typing import Any

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data',
)

# Required headers per CSV — uploaded files must contain at least these columns.
REQUIRED_MARKETS_COLS = {
    'market', 'tier', 'lat', 'lon',
    'inventory_mw', 'under_construction_mw', 'planned_mw',
    'preleased_pct', 'vacancy_pct',
}
REQUIRED_FACILITIES_COLS = {
    'name', 'market', 'lat', 'lon', 'status', 'mw',
    'operator', 'developer', 'funding_type',
}


def _file_for(kind: str) -> str:
    if kind == 'markets':    return os.path.join(DATA_DIR, 'datacenter_markets.csv')
    if kind == 'facilities': return os.path.join(DATA_DIR, 'datacenter_facilities.csv')
    raise ValueError(f'unknown kind: {kind}')


def _validate_csv_bytes(raw: bytes, required_cols: set[str]) -> tuple[list[dict], str | None]:
    """Parse CSV bytes into rows. Returns (rows, error)."""
    try:
        text = raw.decode('utf-8-sig')
    except UnicodeDecodeError:
        return [], 'file is not valid UTF-8'
    try:
        reader = csv.DictReader(io.StringIO(text))
        cols = set(reader.fieldnames or [])
        missing = required_cols - cols
        if missing:
            return [], f'missing required columns: {sorted(missing)}'
        rows = list(reader)
    except csv.Error as e:
        return [], f'CSV parse error: {e}'
    if not rows:
        return [], 'CSV has header but no rows'
    return rows, None


def _index_by(rows: list[dict], key: str) -> dict[str, dict]:
    return {(r.get(key) or '').strip(): r for r in rows if (r.get(key) or '').strip()}


def _safe_float(s: Any) -> float:
    try: return float(s)
    except (TypeError, ValueError): return 0.0


def _markets_diff(old_rows: list[dict], new_rows: list[dict]) -> dict:
    """Return a dict of changes: added markets, removed markets, top MW shifts."""
    a = _index_by(old_rows, 'market')
    b = _index_by(new_rows, 'market')
    added = sorted(set(b) - set(a))
    removed = sorted(set(a) - set(b))
    changes = []
    for k in set(a) & set(b):
        for col in ('inventory_mw', 'under_construction_mw', 'planned_mw'):
            old_v = _safe_float(a[k].get(col))
            new_v = _safe_float(b[k].get(col))
            delta = new_v - old_v
            if abs(delta) >= 5:  # ignore <5 MW noise
                changes.append({
                    'market': k, 'field': col,
                    'old': round(old_v, 1), 'new': round(new_v, 1),
                    'delta': round(delta, 1),
                })
    changes.sort(key=lambda x: abs(x['delta']), reverse=True)
    return {
        'added': added,
        'removed': removed,
        'top_changes': changes[:15],
        'total_old_inventory': round(sum(_safe_float(r.get('inventory_mw')) for r in old_rows), 1),
        'total_new_inventory': round(sum(_safe_float(r.get('inventory_mw')) for r in new_rows), 1),
    }


def _facilities_diff(old_rows: list[dict], new_rows: list[dict]) -> dict:
    """Return a dict of changes: added facilities, removed facilities, top MW shifts."""
    a = _index_by(old_rows, 'name')
    b = _index_by(new_rows, 'name')
    added = sorted(set(b) - set(a))
    removed = sorted(set(a) - set(b))
    changes = []
    for k in set(a) & set(b):
        old_mw = _safe_float(a[k].get('mw'))
        new_mw = _safe_float(b[k].get('mw'))
        delta = new_mw - old_mw
        if abs(delta) >= 5:
            changes.append({
                'name': k, 'field': 'mw',
                'old': round(old_mw, 1), 'new': round(new_mw, 1),
                'delta': round(delta, 1),
            })
        old_status = (a[k].get('status') or '').strip().lower()
        new_status = (b[k].get('status') or '').strip().lower()
        if old_status != new_status and new_status:
            changes.append({
                'name': k, 'field': 'status',
                'old': old_status, 'new': new_status, 'delta': None,
            })
    changes.sort(key=lambda x: abs(x.get('delta') or 0), reverse=True)
    return {
        'added': added,
        'removed': removed,
        'top_changes': changes[:20],
        'total_old_mw': round(sum(_safe_float(r.get('mw')) for r in old_rows), 1),
        'total_new_mw': round(sum(_safe_float(r.get('mw')) for r in new_rows), 1),
    }


def upload_csv(kind: str, raw: bytes) -> dict:
    """Validate + atomically swap a CSV file. Returns a diff summary."""
    if kind == 'markets':
        required = REQUIRED_MARKETS_COLS
    elif kind == 'facilities':
        required = REQUIRED_FACILITIES_COLS
    else:
        return {'ok': False, 'error': f'unknown kind: {kind}'}

    rows, err = _validate_csv_bytes(raw, required)
    if err:
        return {'ok': False, 'error': err}

    target = _file_for(kind)
    backup = target + '.bak'
    tmp    = target + '.tmp'

    # Read OLD rows for the diff before we overwrite.
    old_rows: list[dict] = []
    if os.path.exists(target):
        try:
            with open(target, newline='', encoding='utf-8') as f:
                old_rows = list(csv.DictReader(f))
        except Exception as e:
            logger.warning(f'failed to read existing {target} for diff: {e}')

    # Write the new file atomically: tmp → backup-old → rename.
    try:
        with open(tmp, 'wb') as f:
            f.write(raw)
        if os.path.exists(target):
            shutil.copy2(target, backup)
        os.replace(tmp, target)
    except Exception as e:
        logger.exception(f'failed to swap {target}')
        return {'ok': False, 'error': f'file swap failed: {e}'}

    diff = _markets_diff(old_rows, rows) if kind == 'markets' else _facilities_diff(old_rows, rows)

    # Trigger a rebuild so the API picks up the new data immediately.
    from backend.data_centers import service
    service.build(force=True)

    return {
        'ok': True,
        'kind': kind,
        'rows_loaded': len(rows),
        'backup_path': os.path.relpath(backup, DATA_DIR) if os.path.exists(backup) else None,
        'diff': diff,
    }
