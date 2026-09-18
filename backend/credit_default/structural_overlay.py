"""
Structural risk overlay for the sovereign credit model.

The fitted macro model captures debt, external, fiscal, growth and
governance signals — but it can't see structural / situational factors
that experienced analysts (and any competent underwriter) apply on top:

  * Small-island developing state (SIDS)   — hurricane / tourism concentration
  * Landlocked developing country (LLDC)   — freight-cost, corridor dependence
  * Active conflict                        — war, insurgency, regime collapse
  * Recent political shock (< 18 months)   — coup, mass violence, regime change
  * Single-commodity / single-sector export — Dutch-disease / terms-of-trade

Rather than force-fit these into the GBM (where they'd be sparse binary
features contributing very little AUC on the 190-country panel), we apply
them as a post-hoc **notch overlay** on top of the fitted model rating.

Notch adjustments per active flag:

    +2   SIDS
    +1   LLDC
    +3   Active conflict
    +2   Recent political shock
    +1   Very high export concentration

Total is capped at +6 notches so a country with every flag active can
never fall more than 6 grades from its base model rating.

The overlay is **display only**. It never touches the fitted PDs — those
stay pristine so we can still backtest / calibrate against actual default
events. Underwriters see BOTH numbers side-by-side: the raw fitted call
and the structural-adjusted call.

Data source: ``data/structural_risk_overlay.csv`` (hand-curated, tracked
in git for auditability). Refresh quarterly.
"""

from __future__ import annotations

import csv
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple


_CSV = Path(__file__).resolve().parent.parent.parent / 'data' / 'structural_risk_overlay.csv'


# Notch penalties per flag. Kept as module constants so the overlay's
# calibration is transparent and easy to tune.
NOTCH_PER_FLAG: Dict[str, int] = {
    'sids': 2,
    'lldc': 1,
    'active_conflict': 3,
    'political_shock_recent': 2,
    'export_conc_high': 1,
}

MAX_NOTCH_ADJUSTMENT = 6  # cap total overlay so a triple-flagged country still lands sensibly


_cache: Dict[str, Dict[str, Dict]] = {}
_cache_lock = threading.Lock()


def _load_overlay_csv() -> Dict[str, Dict[str, bool]]:
    """Return ``{iso3: {sids, lldc, active_conflict, political_shock_recent,
    export_conc_high, notes}}``. Boolean columns are parsed from 'Y'/'N'."""
    with _cache_lock:
        cached = _cache.get('data')
    if cached is not None:
        return cached

    out: Dict[str, Dict[str, bool]] = {}
    if not _CSV.exists():
        with _cache_lock:
            _cache['data'] = out
        return out

    bool_fields = ('sids', 'lldc', 'active_conflict',
                   'political_shock_recent', 'export_conc_high')

    with open(_CSV, newline='', encoding='utf-8') as f:
        cleaned = (ln for ln in f
                   if ln.strip() and not ln.lstrip().startswith('#'))
        reader = csv.DictReader(cleaned)
        for row in reader:
            iso3 = (row.get('iso3') or '').strip().upper()
            if len(iso3) != 3:
                continue
            entry = {'notes': (row.get('notes') or '').strip()}
            for field in bool_fields:
                raw = (row.get(field) or '').strip().upper()
                entry[field] = raw in ('Y', 'YES', 'TRUE', '1')
            out[iso3] = entry

    with _cache_lock:
        _cache['data'] = out
    return out


# ── PM-notch ladder (mirrors rating_model.RATING_BUCKETS) ────────────────
_PM_LADDER = [
    (1, '1'), (2, '2+'), (3, '2'), (4, '2-'),
    (5, '3+'), (6, '3'), (7, '3-'),
    (8, '4+'), (9, '4'), (10, '4-'),
    (11, '5+'), (12, '5'), (13, '5-'),
    (14, '6+'), (15, '6'), (16, '6-'),
    (17, '7'), (18, '8'), (19, '9'), (20, '10'),
]
_NUM_TO_NOTCH = {n: notch for n, notch in _PM_LADDER}
_SP_EQUIV_BY_NUM = {
    1: 'AAA', 2: 'AA+', 3: 'AA', 4: 'AA-',
    5: 'A+', 6: 'A', 7: 'A-',
    8: 'BBB+', 9: 'BBB', 10: 'BBB-',
    11: 'BB+', 12: 'BB', 13: 'BB-',
    14: 'B+', 15: 'B', 16: 'B-',
    17: 'CCC', 18: 'CC', 19: 'SD', 20: 'D',
}


# ── Public API ──────────────────────────────────────────────────────────


def get_overlay_for(iso3: str) -> Optional[Dict]:
    """Return the overlay flags + notch penalties + notes for a country,
    or ``None`` if the country isn't in the overlay file."""
    iso3 = (iso3 or '').strip().upper()
    if len(iso3) != 3:
        return None
    data = _load_overlay_csv()
    return data.get(iso3)


def compute_notch_adjustment(iso3: str) -> Tuple[int, List[Dict]]:
    """Compute total overlay for a country. Returns
    ``(total_notches, breakdown_list)`` where each breakdown entry is
    ``{flag, penalty, description}``. Empty list if the country has no
    entry or all flags are off."""
    entry = get_overlay_for(iso3)
    if not entry:
        return 0, []
    breakdown: List[Dict] = []
    total = 0
    if entry.get('sids'):
        breakdown.append({'flag': 'sids', 'penalty': NOTCH_PER_FLAG['sids'],
                          'description': 'Small-island developing state (hurricane / tourism concentration)'})
        total += NOTCH_PER_FLAG['sids']
    if entry.get('lldc'):
        breakdown.append({'flag': 'lldc', 'penalty': NOTCH_PER_FLAG['lldc'],
                          'description': 'Landlocked (freight-cost premium, single-corridor dependence)'})
        total += NOTCH_PER_FLAG['lldc']
    if entry.get('active_conflict'):
        breakdown.append({'flag': 'active_conflict', 'penalty': NOTCH_PER_FLAG['active_conflict'],
                          'description': 'Active conflict (war, insurgency or regime collapse)'})
        total += NOTCH_PER_FLAG['active_conflict']
    if entry.get('political_shock_recent'):
        breakdown.append({'flag': 'political_shock_recent', 'penalty': NOTCH_PER_FLAG['political_shock_recent'],
                          'description': 'Recent political shock (coup, mass violence, regime change < 18 months)'})
        total += NOTCH_PER_FLAG['political_shock_recent']
    if entry.get('export_conc_high'):
        breakdown.append({'flag': 'export_conc_high', 'penalty': NOTCH_PER_FLAG['export_conc_high'],
                          'description': 'Very high export concentration (>50% single commodity or sector)'})
        total += NOTCH_PER_FLAG['export_conc_high']

    total = min(total, MAX_NOTCH_ADJUSTMENT)
    return total, breakdown


def _has_hard_external_default(iso3: str, current_year: Optional[int] = None) -> bool:
    """True iff the country has an ACTIVE hard external default per CRAG.

    Hard external default = ``event_type == 'default'`` with instrument in
    {external_bond, mixed}, and end_year is either blank (ongoing) or ≥
    current_year - 1. Paris Club rescheduling, London Club bank-loan
    restructuring, and domestic arrears are NOT hard external defaults —
    they signal distress but the country is still servicing its
    hard-currency bonds.

    Used by ``apply_overlay`` to cap the overlay-adjusted rating at
    CC (PM 8) unless the country is genuinely in Eurobond default.
    """
    try:
        from backend.credit_default import defaults as cd_defaults
        import time as _t
        if current_year is None:
            current_year = _t.localtime().tm_year
        events = cd_defaults.load_events(include_distress=False)
        for ev in events:
            if ev.get('iso3') != iso3.upper():
                continue
            if ev.get('event_type') != 'default':
                continue
            # Restrict to external instruments — domestic hard defaults
            # (e.g. GKO 1998) are also hard defaults, so we keep those too.
            instrument = (ev.get('instrument') or '').lower()
            if 'external' not in instrument and 'mixed' not in instrument and instrument != 'domestic':
                continue
            end = ev.get('end_year')
            # Ongoing spell (end blank) or spell that reached the last
            # panel year — count as active.
            if end is None or int(end) >= (current_year - 1):
                return True
        return False
    except Exception:  # noqa: BLE001
        return False


# PM notch numeric ceiling when NOT in hard default. 18 = CC = "default
# imminent". The overlay can push a country to CC but not to SD (19) or
# D (20) unless CRAG confirms an actual hard-currency default event.
NOTCH_CAP_NO_HARD_DEFAULT = 18


def apply_overlay(iso3: str, base_pm_numeric: Optional[int]) -> Dict:
    """Apply the structural overlay to a base rating.

    Returns::

        {
          'base_pm_numeric': int,   # what the fitted model said
          'base_pm_notch': str,     # e.g. '4+'
          'adjustment_notches': int,# how many notches the overlay adds
          'adjusted_pm_numeric': int,
          'adjusted_pm_notch': str, # e.g. '5-'
          'adjusted_sp_equiv': str, # e.g. 'BB-'
          'breakdown': [{flag, penalty, description}, ...],
          'notes': str,             # analyst notes from the CSV
          'capped_at_max': bool,    # True if we hit MAX_NOTCH_ADJUSTMENT
          'capped_at_cc': bool,     # True if the hard-default cap kicked in
          'hard_default_active': bool,  # True if CRAG shows a live external default
        }

    Returns ``None`` if the country isn't in the overlay file OR
    ``base_pm_numeric`` is None.
    """
    if base_pm_numeric is None:
        return None
    entry = get_overlay_for(iso3)
    if not entry:
        return None
    total, breakdown = compute_notch_adjustment(iso3)
    if not breakdown:
        return None

    hard_default = _has_hard_external_default(iso3)
    raw_adjusted = min(20, max(1, int(base_pm_numeric) + total))
    # Cap at CC (18) unless there's a confirmed hard external default —
    # per the AIG ORR mapping, ORR 8 = CC = "default imminent, not yet
    # in one"; only ORR 9 (SD/RD/C) and ORR 10 (D) represent actual
    # default states.
    if hard_default:
        adjusted = raw_adjusted
        capped_at_cc = False
    else:
        adjusted = min(raw_adjusted, NOTCH_CAP_NO_HARD_DEFAULT)
        capped_at_cc = raw_adjusted > NOTCH_CAP_NO_HARD_DEFAULT

    return {
        'base_pm_numeric': int(base_pm_numeric),
        'base_pm_notch': _NUM_TO_NOTCH.get(int(base_pm_numeric)),
        'adjustment_notches': total,
        'raw_adjusted_pm_numeric': raw_adjusted,
        'adjusted_pm_numeric': adjusted,
        'adjusted_pm_notch': _NUM_TO_NOTCH.get(adjusted),
        'adjusted_sp_equiv': _SP_EQUIV_BY_NUM.get(adjusted),
        'breakdown': breakdown,
        'notes': entry.get('notes') or '',
        'capped_at_max': total >= MAX_NOTCH_ADJUSTMENT,
        'capped_at_cc': capped_at_cc,
        'hard_default_active': hard_default,
    }
