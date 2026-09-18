#!/usr/bin/env python3
"""
Compare your ratings against the Parra Macro credit-default model.

Produces a side-by-side table so you can see, for each sovereign:

  - our model's rating (PM notch + S&P equivalent) and its 1Y / 3Y / 5Y PDs
  - our transparent composite (z-score reference) rating
  - S&P / Moody's / Fitch letters (from data/agency_ratings.csv)
  - YOUR rating (from a CSV you supply, or a manual --my-rating flag)
  - notch delta = your_rating − model_rating on the 20-notch PM scale
    (positive = your rating is more conservative than the model)

Usage
-----
    # Ad-hoc, just show model output for a few countries:
    python scripts/compare_ratings.py --iso3 ATG,BTN,BDI,CAF,COM,GNB,MMR,NPL

    # With your ratings in a CSV (columns: iso3, your_rating):
    python scripts/compare_ratings.py --your-ratings my_ratings.csv

    # Both together — flag one country manually while comparing the whole file:
    python scripts/compare_ratings.py --your-ratings my_ratings.csv \\
        --iso3 IND --my-rating 4

Rating format
-------------
PM notches use the 20-point ladder: ``1, 2+, 2, 2-, 3+, 3, 3-, 4+, 4, 4-, 5+,
5, 5-, 6+, 6, 6-, 7, 8, 9, 10`` (1 = strongest, 10 = default). Your ratings
can be written in the same format, or as S&P letters (``BBB+``, ``B-``,
``CCC``, etc.) — the script accepts either and normalises before comparing.

CSV format
----------
Minimal columns::

    iso3,your_rating
    ATG,4+
    BTN,5-
    BDI,7
    CAF,7
    COM,6
    GNB,6-
    MMR,7
    NPL,5-

Optional column ``as_of`` (YYYY-MM-DD) is passed through to the output.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Allow running as `python scripts/compare_ratings.py` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── PM notch <-> numeric conversion (mirrors rating_model.RATING_BUCKETS) ──


PM_NOTCH_TO_NUM = {
    '1': 1, '2+': 2, '2': 3, '2-': 4,
    '3+': 5, '3': 6, '3-': 7,
    '4+': 8, '4': 9, '4-': 10,
    '5+': 11, '5': 12, '5-': 13,
    '6+': 14, '6': 15, '6-': 16,
    '7': 17, '8': 18, '9': 19, '10': 20,
}
PM_NUM_TO_NOTCH = {v: k for k, v in PM_NOTCH_TO_NUM.items()}


# S&P → PM numeric equivalent (same mapping the deployed dashboard uses).
# CCC family collapses to PM 7 per Tellimer/AIG ORR convention.
SP_TO_PM_NUM = {
    'AAA': 1, 'AA+': 2, 'AA': 3, 'AA-': 4,
    'A+': 5, 'A': 6, 'A-': 7,
    'BBB+': 8, 'BBB': 9, 'BBB-': 10,
    'BB+': 11, 'BB': 12, 'BB-': 13,
    'B+': 14, 'B': 15, 'B-': 16,
    'CCC+': 17, 'CCC': 17, 'CCC-': 17,
    'CC': 18, 'C': 19, 'SD': 19, 'RD': 19, 'D': 20,
}


def normalise_rating(raw: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """Return (canonical_pm_notch, pm_numeric) or (None, None) if not
    recognisable. Accepts PM notches ('4+', '5-', '7', ...) OR S&P letter
    ratings ('BBB+', 'B-', 'CCC', ...)."""
    if raw is None:
        return None, None
    s = str(raw).strip()
    if not s or s.lower() in ('', 'nr', 'n/a', 'na', 'none'):
        return None, None
    # Uppercase, strip spaces.
    key = s.upper().replace(' ', '')
    # Direct PM match?
    if key in PM_NOTCH_TO_NUM:
        return key, PM_NOTCH_TO_NUM[key]
    # S&P letter?
    if key in SP_TO_PM_NUM:
        num = SP_TO_PM_NUM[key]
        return PM_NUM_TO_NOTCH.get(num), num
    # Sometimes users write "BBB +" or "B -" — retry without extra chars.
    key2 = key.replace('.', '').replace('_', '')
    if key2 in SP_TO_PM_NUM:
        num = SP_TO_PM_NUM[key2]
        return PM_NUM_TO_NOTCH.get(num), num
    return None, None


# ── Data loaders ────────────────────────────────────────────────────────


def load_your_ratings(path: Path) -> Dict[str, Dict]:
    """Return ``{iso3: {your_rating, your_pm_num, as_of}}``. Skips rows we
    can't normalise, printing a warning per skipped row so bad input
    surfaces instead of silently dropping."""
    out: Dict[str, Dict] = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(
            (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
        )
        for row in reader:
            iso3 = (row.get('iso3') or '').strip().upper()
            raw = row.get('your_rating') or row.get('rating') or ''
            if len(iso3) != 3:
                continue
            notch, num = normalise_rating(raw)
            if num is None:
                print(f'[compare] skipping {iso3}: unrecognised rating "{raw}"',
                      file=sys.stderr)
                continue
            out[iso3] = {
                'your_rating': notch,
                'your_pm_num': num,
                'as_of': (row.get('as_of') or '').strip() or None,
            }
    return out


def load_country_from_service(iso3: str) -> Optional[Dict]:
    """Score one country through the live model (uses whatever fit state
    is on disk). Returns the same dict the dashboard receives at
    /api/credit-default/country/<iso3>."""
    from backend.credit_default import service
    try:
        return service.get_country(iso3)
    except Exception as e:  # noqa: BLE001
        print(f'[compare] {iso3}: service.get_country failed: {e}',
              file=sys.stderr)
        return None


# ── Rendering ───────────────────────────────────────────────────────────


COLS = [
    ('iso3',      'ISO3',          5, 'l'),
    ('name',      'Country',       26, 'l'),
    ('pm_notch',  'Model',         6, 'l'),
    ('sp_equiv',  'SP eq.',        7, 'l'),
    ('comp_notch','Composite',     10, 'l'),
    ('pd_1y',     'PD 1y',         7, 'r'),
    ('pd_3y',     'PD 3y',         7, 'r'),
    ('pd_5y',     'PD 5y',         7, 'r'),
    ('agency_sp', 'S&P',           6, 'l'),
    ('agency_mo', "Moody's",       7, 'l'),
    ('agency_fi', 'Fitch',         6, 'l'),
    ('your',      'Your',          6, 'l'),
    ('delta',     'Δ notches',     10, 'r'),
    ('interp',    'Verdict',       18, 'l'),
]


def _fmt_cell(value, width: int, align: str) -> str:
    v = '' if value is None else str(value)
    if align == 'r':
        return v.rjust(width)
    return v.ljust(width)


def _fmt_pct(v) -> str:
    if v is None:
        return '—'
    try:
        return f'{float(v)*100:5.2f}%'
    except (TypeError, ValueError):
        return '—'


def _interp(delta_notches: Optional[int]) -> str:
    if delta_notches is None:
        return ''
    if delta_notches == 0:
        return 'aligned'
    if delta_notches > 0:
        # Your rating is worse (higher numeric = worse credit).
        magnitude = 'harsher' if delta_notches >= 3 else 'more cautious'
        return f'you {magnitude} +{delta_notches}'
    magnitude = 'more bullish' if delta_notches <= -3 else 'more optimistic'
    return f'you {magnitude} {delta_notches}'


def render_table(rows: List[Dict]) -> str:
    header = '  '.join(_fmt_cell(name, width, align) for _, name, width, align in COLS)
    sep = '  '.join('-' * width for _, _, width, _ in COLS)
    out = [header, sep]
    for r in rows:
        out.append('  '.join(_fmt_cell(r.get(key), width, align)
                             for key, _, width, align in COLS))
    return '\n'.join(out)


# ── Main ────────────────────────────────────────────────────────────────


def build_row(iso3: str, your: Optional[Dict]) -> Dict:
    c = load_country_from_service(iso3)
    if c is None:
        return {'iso3': iso3, 'name': f'({iso3} not scored)'}

    rating = c.get('rating') or {}
    composite = rating.get('composite') or {}
    agency = c.get('agency') or {}

    model_num = rating.get('pm_numeric')
    delta = None
    if your and your.get('your_pm_num') is not None and model_num is not None:
        delta = int(your['your_pm_num']) - int(model_num)

    return {
        'iso3': iso3,
        'name': c.get('name') or iso3,
        'pm_notch': rating.get('pm_notch'),
        'sp_equiv': rating.get('sp_equiv'),
        'comp_notch': composite.get('pm_notch'),
        'pd_1y': _fmt_pct(rating.get('pd_1y')),
        'pd_3y': _fmt_pct(rating.get('pd_3y')),
        'pd_5y': _fmt_pct(rating.get('pd_5y')),
        'agency_sp': agency.get('sp') or '—',
        'agency_mo': agency.get('moodys') or '—',
        'agency_fi': agency.get('fitch') or '—',
        'your': (your or {}).get('your_rating') or '',
        'delta': ('' if delta is None else f'{delta:+d}'),
        'interp': _interp(delta),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--iso3', default='',
                        help='Comma-separated ISO3 codes to compare (default: all rows in --your-ratings)')
    parser.add_argument('--your-ratings', default='',
                        help='CSV of your ratings (columns: iso3, your_rating, optional as_of)')
    parser.add_argument('--my-rating', default='',
                        help='Ad-hoc single rating for the LAST iso3 in --iso3 (skips CSV)')
    parser.add_argument('--json', action='store_true',
                        help='Emit machine-readable JSON instead of the table')
    parser.add_argument('--out', default='',
                        help='Write output to this file in addition to stdout')
    args = parser.parse_args()

    yours: Dict[str, Dict] = {}
    if args.your_ratings:
        yours = load_your_ratings(Path(args.your_ratings))

    iso_list = [x.strip().upper() for x in args.iso3.split(',') if x.strip()]
    if not iso_list and yours:
        iso_list = sorted(yours.keys())

    if not iso_list:
        parser.error('provide --iso3 CODES or --your-ratings FILE (or both)')

    # --my-rating attaches to the last --iso3 entry (typical ad-hoc use).
    if args.my_rating and iso_list:
        notch, num = normalise_rating(args.my_rating)
        if num is None:
            print(f'[compare] --my-rating {args.my_rating!r} not recognised',
                  file=sys.stderr)
        else:
            yours[iso_list[-1]] = {'your_rating': notch, 'your_pm_num': num, 'as_of': None}

    rows = [build_row(iso3, yours.get(iso3)) for iso3 in iso_list]

    if args.json:
        payload = json.dumps(rows, indent=2, default=str)
        print(payload)
        if args.out:
            Path(args.out).write_text(payload)
        return 0

    table = render_table(rows)
    print(table)
    if args.out:
        Path(args.out).write_text(table + '\n')
        print(f'\n[compare] wrote {args.out}', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
