"""
Systemic banking-crisis features (Laeven & Valencia 2020 update,
extended through 2024 from IMF FSAP / national disclosures).

Two derived features:
  * ``years_since_banking_crisis``: years since the most recent
    *onset* of a banking crisis. Capped at 25 (anything older
    treated as "no recent crisis"). Lower = more recent = worse.
  * ``currently_in_banking_crisis``: 1 if the country is inside an
    open / not-yet-resolved spell that year, else 0.

Reinhart-Rogoff (2009/2011) and L-V both find banking crises
precede sovereign default by 1-3 years; this is the proxy.
"""

from __future__ import annotations

import csv
import os
import time
import threading
from typing import Dict, List, Optional, Set, Tuple

from config import Config


_CSV_PATH = os.path.join(Config.DATA_DIR, 'banking_crises.csv')
_REPO_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data', 'banking_crises.csv',
)

_cache: Dict[str, object] = {}
_cache_lock = threading.Lock()


def _resolve_csv() -> Optional[str]:
    if os.path.exists(_CSV_PATH):
        return _CSV_PATH
    if os.path.exists(_REPO_CSV):
        return _REPO_CSV
    return None


def load_events() -> List[Dict]:
    """Return parsed banking-crisis rows. Same shape as
    ``credit_default.defaults.load_events`` so callers can mix the two."""
    with _cache_lock:
        cached = _cache.get('events')
        if cached is not None:
            return cached  # type: ignore[return-value]

    path = _resolve_csv()
    if not path:
        return []

    out: List[Dict] = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            cleaned = (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
            reader = csv.DictReader(cleaned)
            for row in reader:
                iso3 = (row.get('iso3') or '').strip().upper()
                start = (row.get('start_year') or '').strip()
                end = (row.get('end_year') or '').strip()
                if not iso3 or len(iso3) != 3 or not start:
                    continue
                try:
                    start_y = int(start)
                except ValueError:
                    continue
                end_y: Optional[int] = None
                if end:
                    try:
                        end_y = int(end)
                    except ValueError:
                        end_y = None
                out.append({'iso3': iso3, 'start_year': start_y, 'end_year': end_y})
    except OSError:
        return []

    with _cache_lock:
        _cache['events'] = out
    return out


def starts_by_iso() -> Dict[str, Set[int]]:
    out: Dict[str, Set[int]] = {}
    for ev in load_events():
        out.setdefault(ev['iso3'], set()).add(ev['start_year'])
    return out


def in_crisis_years() -> Dict[str, Set[int]]:
    """{iso3: {years inside an active banking-crisis spell}}.

    Open end_year extends through current year — same convention as
    sovereign-default spells in ``credit_default.defaults``."""
    cur_yr = time.localtime().tm_year
    out: Dict[str, Set[int]] = {}
    for ev in load_events():
        iso = ev['iso3']
        s = int(ev['start_year'])
        e = ev.get('end_year')
        e_int = int(e) if e is not None else cur_yr
        out.setdefault(iso, set()).update(range(s, e_int + 1))
    return out


def years_since_banking_crisis(max_years: int = 25
                               ) -> Dict[Tuple[str, int], int]:
    """{(iso3, year): years since most-recent banking-crisis onset}.

    Capped at ``max_years``. Used as a feature column in the credit-
    default panel (lower = more recent = higher PD)."""
    starts = starts_by_iso()
    out: Dict[Tuple[str, int], int] = {}
    for iso, ss in starts.items():
        if not ss:
            continue
        sorted_starts = sorted(ss)
        for year in range(min(sorted_starts), 2030 + 1):
            past = [s for s in sorted_starts if s <= year]
            if past:
                out[(iso, year)] = min(max_years, year - max(past))
    return out
