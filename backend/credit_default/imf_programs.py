"""
IMF lending-arrangement status as a credit-default feature.

Numeric encoding (higher = worse credit signal):
  0 = none           — no active arrangement
  1 = on_track       — active EFF/SBA/ECF/FCL etc., reviews on time
  2 = off_track      — active arrangement but reviews stalled or
                       conditions breached
  3 = arrears        — country in arrears to IMF (rare;
                       near-mechanical CCC trigger per agencies)

Source: data/imf_programs.csv (hand-curated from IMF Lending
Arrangements page + Article IV consultations + IMF Country Reports).
"""

from __future__ import annotations

import csv
import os
import threading
from typing import Dict, Optional

from config import Config


_CSV_PATH = os.path.join(Config.DATA_DIR, 'imf_programs.csv')
_REPO_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data', 'imf_programs.csv',
)

_STATUS_TO_NUM = {'none': 0, 'on_track': 1, 'off_track': 2, 'arrears': 3}

_cache: Dict[str, object] = {}
_cache_lock = threading.Lock()


def _resolve_csv() -> Optional[str]:
    if os.path.exists(_CSV_PATH):
        return _CSV_PATH
    if os.path.exists(_REPO_CSV):
        return _REPO_CSV
    return None


def status_by_iso() -> Dict[str, int]:
    """{iso3: status_num}. Missing iso3 → defaults to 0 (none)."""
    with _cache_lock:
        cached = _cache.get('by_iso')
        if cached is not None:
            return cached  # type: ignore[return-value]

    path = _resolve_csv()
    if not path:
        return {}

    out: Dict[str, int] = {}
    try:
        with open(path, newline='', encoding='utf-8') as f:
            cleaned = (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
            reader = csv.DictReader(cleaned)
            for row in reader:
                iso3 = (row.get('iso3') or '').strip().upper()
                status = (row.get('status') or '').strip().lower()
                if not iso3 or len(iso3) != 3:
                    continue
                out[iso3] = _STATUS_TO_NUM.get(status, 0)
    except OSError:
        return {}

    with _cache_lock:
        _cache['by_iso'] = out
    return out
