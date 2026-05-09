"""
% FX-denominated central government debt as a credit-default feature.

Loaded from data/fx_debt_share.csv (BIS WS_GDD + IMF QPSD +
Article IV staff reports). Treated as a slow-moving country
characteristic — same value applied across all years in the panel
(latest snapshot only). Higher = more original-sin = higher PD.
"""

from __future__ import annotations

import csv
import os
import threading
from typing import Dict, Optional

from config import Config


_CSV_PATH = os.path.join(Config.DATA_DIR, 'fx_debt_share.csv')
_REPO_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data', 'fx_debt_share.csv',
)

_cache: Dict[str, object] = {}
_cache_lock = threading.Lock()


def _resolve_csv() -> Optional[str]:
    if os.path.exists(_CSV_PATH):
        return _CSV_PATH
    if os.path.exists(_REPO_CSV):
        return _REPO_CSV
    return None


def fx_debt_share() -> Dict[str, float]:
    with _cache_lock:
        cached = _cache.get('by_iso')
        if cached is not None:
            return cached  # type: ignore[return-value]

    path = _resolve_csv()
    if not path:
        return {}

    out: Dict[str, float] = {}
    try:
        with open(path, newline='', encoding='utf-8') as f:
            cleaned = (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
            reader = csv.DictReader(cleaned)
            for row in reader:
                iso3 = (row.get('iso3') or '').strip().upper()
                v = (row.get('fx_debt_pct') or '').strip()
                if not iso3 or len(iso3) != 3 or not v:
                    continue
                try:
                    out[iso3] = float(v)
                except ValueError:
                    continue
    except OSError:
        return {}

    with _cache_lock:
        _cache['by_iso'] = out
    return out
