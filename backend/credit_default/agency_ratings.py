"""
Loader for the static agency-ratings snapshot CSV.

The CSV at ``data/agency_ratings.csv`` is a hand-curated snapshot of S&P,
Moody's and Fitch long-term foreign-currency sovereign ratings used purely
for *display* (so the dashboard can put the model rating side-by-side with
the agencies). It is NOT used for fitting.

CSV columns:
    iso3, sp, moodys, fitch, sp_outlook, moodys_outlook, fitch_outlook, as_of

Refresh cadence is manual — agency sovereign actions are infrequent (a few
per quarter globally). To update: paste in the latest values, commit.
"""

from __future__ import annotations

import csv
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional

from config import Config


_DATA_DIR = Path(Config.DATA_DIR if hasattr(Config, 'DATA_DIR') else 'data')
_CSV = Path(__file__).resolve().parent.parent.parent / 'data' / 'agency_ratings.csv'

_cache: Dict[str, Dict] = {}
_cache_ts: float = 0.0
_cache_lock = threading.Lock()
_CACHE_TTL = 6 * 3600


# ── Numeric scale for direct comparison with model output ────────────────
#
# Lower number = better credit. We use the standard 22-notch convention
# (AAA = 1, D = 22). Mapping covers S&P / Fitch nomenclature; Moody's
# notations (Aaa/Aa1/...) are mapped onto the same numeric ladder.

NUMERIC_SCALE = {
    'AAA': 1, 'Aaa': 1,
    'AA+': 2, 'Aa1': 2,
    'AA': 3,  'Aa2': 3,
    'AA-': 4, 'Aa3': 4,
    'A+': 5,  'A1': 5,
    'A': 6,   'A2': 6,
    'A-': 7,  'A3': 7,
    'BBB+': 8, 'Baa1': 8,
    'BBB': 9, 'Baa2': 9,
    'BBB-': 10, 'Baa3': 10,
    'BB+': 11, 'Ba1': 11,
    'BB': 12,  'Ba2': 12,
    'BB-': 13, 'Ba3': 13,
    'B+': 14,  'B1': 14,
    'B': 15,   'B2': 15,
    'B-': 16,  'B3': 16,
    'CCC+': 17, 'Caa1': 17,
    'CCC': 18,  'Caa2': 18,
    'CCC-': 19, 'Caa3': 19,
    'CC': 20,   'Ca': 20,
    'C': 21,
    'D': 22, 'SD': 22, 'RD': 22,
}


def to_numeric(letter: Optional[str]) -> Optional[int]:
    if not letter:
        return None
    letter = letter.strip()
    return NUMERIC_SCALE.get(letter)


def get_agency_ratings() -> Dict[str, Dict]:
    """Return {iso3: {sp, moodys, fitch, ...}}."""
    global _cache, _cache_ts
    with _cache_lock:
        if _cache and (time.time() - _cache_ts) < _CACHE_TTL:
            return _cache

    out: Dict[str, Dict] = {}
    if not _CSV.exists():
        with _cache_lock:
            _cache = out
            _cache_ts = time.time()
        return out

    try:
        with open(_CSV, newline='', encoding='utf-8') as f:
            # Strip blank/comment lines so the file can carry inline notes.
            cleaned = (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
            reader = csv.DictReader(cleaned)
            for row in reader:
                iso3 = (row.get('iso3') or '').strip().upper()
                if not iso3 or len(iso3) != 3:
                    continue
                sp = (row.get('sp') or '').strip() or None
                moodys = (row.get('moodys') or '').strip() or None
                fitch = (row.get('fitch') or '').strip() or None
                # Median of available numeric ratings = "consensus".
                nums = [to_numeric(x) for x in (sp, moodys, fitch)]
                nums = [n for n in nums if n is not None]
                consensus_num = sorted(nums)[len(nums) // 2] if nums else None
                out[iso3] = {
                    'sp': sp, 'moodys': moodys, 'fitch': fitch,
                    'sp_outlook': (row.get('sp_outlook') or '').strip() or None,
                    'moodys_outlook': (row.get('moodys_outlook') or '').strip() or None,
                    'fitch_outlook': (row.get('fitch_outlook') or '').strip() or None,
                    'as_of': (row.get('as_of') or '').strip() or None,
                    'sp_num': to_numeric(sp),
                    'moodys_num': to_numeric(moodys),
                    'fitch_num': to_numeric(fitch),
                    'consensus_num': consensus_num,
                }
    except (OSError, csv.Error) as e:
        print(f'[credit_default.agency_ratings] failed to read {_CSV}: {e}')

    with _cache_lock:
        _cache = out
        _cache_ts = time.time()
    return out
