"""
Loader for the historical agency-ratings panel.

The CSV at ``data/agency_ratings_history.csv`` is a long-format set of
sovereign rating actions (S&P / Moody's / Fitch) used by the dashboard's
country-drilldown chart to overlay agency rating history alongside
model PD. Schema:

    iso3, as_of, sp, moodys, fitch, sp_outlook, moodys_outlook, fitch_outlook

``as_of`` is YYYY-MM. Missing cells are blank, not "—". Append rows to
extend coverage; no other code changes required.
"""

from __future__ import annotations

import csv
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

from backend.credit_default.agency_ratings import to_numeric


_CSV = Path(__file__).resolve().parent.parent.parent / 'data' / 'agency_ratings_history.csv'

_cache: Dict[str, List[Dict]] = {}
_cache_ts: float = 0.0
_cache_lock = threading.Lock()
_CACHE_TTL = 6 * 3600


def _consensus_num(sp_num, moodys_num, fitch_num) -> Optional[int]:
    """Median of available numeric ratings (matches the snapshot loader)."""
    nums = [n for n in (sp_num, moodys_num, fitch_num) if n is not None]
    if not nums:
        return None
    return sorted(nums)[len(nums) // 2]


def get_history() -> Dict[str, List[Dict]]:
    """Return ``{iso3: [{'as_of', 'sp', 'moodys', 'fitch', 'consensus_num',
    ...}, ...]}``, sorted by ``as_of`` ascending."""
    global _cache, _cache_ts
    with _cache_lock:
        if _cache and (time.time() - _cache_ts) < _CACHE_TTL:
            return _cache

    out: Dict[str, List[Dict]] = {}
    if not _CSV.exists():
        with _cache_lock:
            _cache = out
            _cache_ts = time.time()
        return out

    try:
        with open(_CSV, newline='', encoding='utf-8') as f:
            cleaned = (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
            reader = csv.DictReader(cleaned)
            for row in reader:
                iso3 = (row.get('iso3') or '').strip().upper()
                as_of = (row.get('as_of') or '').strip()
                if not iso3 or len(iso3) != 3 or not as_of:
                    continue
                sp = (row.get('sp') or '').strip() or None
                moodys = (row.get('moodys') or '').strip() or None
                fitch = (row.get('fitch') or '').strip() or None
                rec = {
                    'as_of': as_of,
                    'sp': sp,
                    'moodys': moodys,
                    'fitch': fitch,
                    'sp_outlook': (row.get('sp_outlook') or '').strip() or None,
                    'moodys_outlook': (row.get('moodys_outlook') or '').strip() or None,
                    'fitch_outlook': (row.get('fitch_outlook') or '').strip() or None,
                    'sp_num': to_numeric(sp),
                    'moodys_num': to_numeric(moodys),
                    'fitch_num': to_numeric(fitch),
                }
                rec['consensus_num'] = _consensus_num(
                    rec['sp_num'], rec['moodys_num'], rec['fitch_num'],
                )
                out.setdefault(iso3, []).append(rec)
    except (OSError, csv.Error) as e:
        print(f'[credit_default.agency_ratings_history] failed: {e}')

    for iso3 in out:
        out[iso3].sort(key=lambda r: r['as_of'])

    with _cache_lock:
        _cache = out
        _cache_ts = time.time()
    return out


def get_country_history(iso3: str) -> List[Dict]:
    return list(get_history().get((iso3 or '').upper(), []))
