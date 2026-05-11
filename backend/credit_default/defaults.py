"""
Loader for the sovereign default-events panel — the *dependent variable*
for the credit default model.

Reads ``data/sovereign_defaults.csv`` and exposes utilities that produce
binary labels suitable for joining onto the (iso3, year) macro panel:

  - ``defaulted_within(iso3, year, horizon)``: did this country experience a
    default starting any time in [year+1, year+horizon]?
  - ``build_label_frame(years, horizons)``: pandas DataFrame with one row
    per (iso3, year) and one column per horizon.

We treat ``event_type in {default, restructuring, arrears}`` as a
"default" signal. Paris/London Club rescheduling, IMF programs, etc. are
*available* in the CSV but turned off by default — the model targets
genuine credit events, not broader distress (those are options #3/#4 we
discussed in the README).
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


_CSV = Path(__file__).resolve().parent.parent.parent / 'data' / 'sovereign_defaults.csv'

# Event types that count as a hard credit event for the binary target.
DEFAULT_EVENT_TYPES = {'default', 'restructuring', 'arrears'}

# Event types that signal *broader distress* but not a default. Available
# via the `include_distress=True` flag for users who want target #3.
DISTRESS_EVENT_TYPES = {'paris_club', 'london_club', 'imf_program'}


def _parse_year(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        return int(str(s).strip()[:4])
    except (ValueError, TypeError):
        return None


def load_events(include_distress: bool = False) -> List[Dict]:
    """Return parsed event rows from the CSV."""
    if not _CSV.exists():
        return []

    rows: List[Dict] = []
    keep_types = set(DEFAULT_EVENT_TYPES)
    if include_distress:
        keep_types |= DISTRESS_EVENT_TYPES

    with open(_CSV, newline='', encoding='utf-8') as f:
        cleaned = (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
        reader = csv.DictReader(cleaned)
        for row in reader:
            iso3 = (row.get('iso3') or '').strip().upper()
            ev_type = (row.get('event_type') or '').strip()
            start = _parse_year(row.get('start_year'))
            if not iso3 or len(iso3) != 3:
                continue
            if not ev_type or start is None:
                # "Reference" rows (USA, DEU, …) carry no event — skip.
                continue
            if ev_type not in keep_types:
                continue
            rows.append({
                'iso3': iso3,
                'start_year': start,
                'end_year': _parse_year(row.get('end_year')),
                'event_type': ev_type,
                'instrument': (row.get('instrument') or '').strip(),
                'source': (row.get('source') or '').strip(),
            })
    return rows


def years_since_last_default(events: Optional[List[Dict]] = None,
                             include_distress: bool = False,
                             max_years: int = 100) -> Dict[Tuple[str, int], int]:
    """``{(iso3, year): years_since_last_default_start}``.

    Reinhart-Rogoff (2009/2010) document serial-default behaviour: two-
    thirds of recurrences happen within 20 years and "graduation"
    requires 50-100 clean years. A smoothly decaying years-since
    feature lets the GBM learn that pattern. Capped at ``max_years``
    when no prior default is on file.
    """
    if events is None:
        events = load_events(include_distress=include_distress)
    starts_by_iso = default_starts_by_country(events)
    out: Dict[Tuple[str, int], int] = {}
    for iso3, starts in starts_by_iso.items():
        if not starts:
            continue
        sorted_starts = sorted(starts)
        for year in range(min(sorted_starts), 2030 + 1):
            past = [s for s in sorted_starts if s <= year]
            if past:
                out[(iso3, year)] = min(max_years, year - max(past))
    return out


def default_count_window(events: Optional[List[Dict]] = None,
                         include_distress: bool = False,
                         window_years: int = 25) -> Dict[Tuple[str, int], int]:
    """``{(iso3, year): count of default-onsets in [year-window, year]}``.
    Cantor-Packer (1996) treat default history as a top-3 predictor.
    """
    if events is None:
        events = load_events(include_distress=include_distress)
    starts_by_iso = default_starts_by_country(events)
    out: Dict[Tuple[str, int], int] = {}
    for iso3, starts in starts_by_iso.items():
        if not starts:
            continue
        sorted_starts = sorted(starts)
        for year in range(min(sorted_starts), 2030 + 1):
            count = sum(1 for s in sorted_starts if year - window_years <= s <= year)
            if count:
                out[(iso3, year)] = count
    return out


def default_starts_by_country(events: Optional[List[Dict]] = None,
                              include_distress: bool = False) -> Dict[str, Set[int]]:
    """{iso3: {start_year, ...}} — useful for fast forward-looking labels."""
    if events is None:
        events = load_events(include_distress=include_distress)
    out: Dict[str, Set[int]] = {}
    for ev in events:
        out.setdefault(ev['iso3'], set()).add(ev['start_year'])
    return out


def in_default_years_by_country(events: Optional[List[Dict]] = None,
                                include_distress: bool = False,
                                current_year: Optional[int] = None) -> Dict[str, Set[int]]:
    """{iso3: {years country was inside an active default spell}}.

    Used to censor the *current-year* indicators of a country that's
    already in default — predicting "PD next year" doesn't make sense if
    we already know the country is in default this year.

    CRAG open ``end_year`` (blank in source) means "spell still active
    as of dataset release". We extend through ``current_year`` so
    ongoing defaulters (LBN 2020+, GHA 2022+, ZMB 2020+, VEN 2017+)
    get correctly flagged as in-default today.
    """
    if events is None:
        events = load_events(include_distress=include_distress)
    if current_year is None:
        import time as _t
        current_year = _t.localtime().tm_year
    out: Dict[str, Set[int]] = {}
    for ev in events:
        start = ev['start_year']
        end = ev.get('end_year') if ev.get('end_year') is not None else current_year
        years = set(range(start, end + 1))
        out.setdefault(ev['iso3'], set()).update(years)
    return out


def defaulted_within(starts: Set[int], year: int, horizon: int) -> int:
    """Did a default *start* in [year+1, year+horizon]?"""
    if not starts:
        return 0
    for h in range(1, horizon + 1):
        if (year + h) in starts:
            return 1
    return 0


def build_label_frame(panel_iso_years: Iterable[Tuple[str, int]],
                      horizons: Iterable[int] = (1, 3, 5),
                      include_distress: bool = False):
    """Return DataFrame with binary defaulted_within_{h}y columns.

    ``panel_iso_years`` is an iterable of (iso3, year) tuples — usually
    ``df[['iso3', 'year']].itertuples(index=False)``.
    """
    try:
        import pandas as pd
    except ImportError:
        return None

    starts_by_iso = default_starts_by_country(include_distress=include_distress)
    in_default_by_iso = in_default_years_by_country(include_distress=include_distress)
    rows = []
    for iso3, year in panel_iso_years:
        starts = starts_by_iso.get(iso3, set())
        in_def = in_default_by_iso.get(iso3, set())
        is_in_default = int(year in in_def)
        rec = {
            'iso3': iso3,
            'year': year,
            'in_default_year': is_in_default,
        }
        # Option-C labelling: a row that's CURRENTLY in default is
        # also positive for every horizon (the spell is ongoing —
        # default within the next h years is trivially true). Without
        # this, ongoing defaulters look like non-events to the
        # trainer and the model has to be tethered at inference time
        # to compensate. With it, the GBM learns the macro signature
        # of "in default" and outputs high PD natively.
        for h in horizons:
            rec[f'defaulted_within_{h}y'] = max(
                is_in_default, defaulted_within(starts, year, h),
            )
        rows.append(rec)
    return pd.DataFrame(rows)


# ── Quarterly labelling ─────────────────────────────────────────────────
# CRAG events carry year-precision boundaries (start_year, end_year), so
# we project them onto a quarterly grid by treating the onset as
# (start_year, Q1) and any quarter inside the spell as "in default".
# That's coarser than true quarterly precision but it's the best the
# upstream data supports.


def _quarter_advance(year: int, quarter: int, h_q: int) -> Tuple[int, int]:
    """Return (year, quarter) advanced by ``h_q`` quarters."""
    total = (quarter - 1) + h_q
    return year + total // 4, (total % 4) + 1


def defaulted_within_quarters(start_quarters: Set[Tuple[int, int]],
                              year: int, quarter: int, h_q: int) -> int:
    """Did a default *start* in any of the next ``h_q`` quarters?"""
    if not start_quarters:
        return 0
    for h in range(1, h_q + 1):
        target = _quarter_advance(year, quarter, h)
        if target in start_quarters:
            return 1
    return 0


def build_quarterly_label_frame(
    panel_iso_quarters: Iterable[Tuple[str, int, int]],
    horizons_quarters: Iterable[int] = (4, 12, 20),
    include_distress: bool = False,
):
    """Return DataFrame with binary ``defaulted_within_{n}q`` columns.

    ``panel_iso_quarters`` is an iterable of (iso3, year, quarter)
    tuples (usually ``df[['iso3', 'year', 'quarter']].itertuples()``).
    Onset for each CRAG event is mapped to (start_year, Q1) — the
    upstream panel doesn't carry sub-year precision.
    """
    try:
        import pandas as pd
    except ImportError:
        return None

    import time as _t
    current_year = _t.localtime().tm_year
    events = load_events(include_distress=include_distress)
    starts_by_iso: Dict[str, Set[Tuple[int, int]]] = {}
    in_default_by_iso: Dict[str, Set[Tuple[int, int]]] = {}
    for ev in events:
        iso = ev['iso3']
        start_yr = ev['start_year']
        # Open spell → still active through current_year (matches the
        # annual fix). Used by the dashboard's currently-in-default
        # override at inference; not used to alter training labels —
        # see the annual path for rationale.
        end_yr = ev.get('end_year') if ev.get('end_year') is not None else current_year
        starts_by_iso.setdefault(iso, set()).add((start_yr, 1))
        for y in range(start_yr, end_yr + 1):
            for q in range(1, 5):
                in_default_by_iso.setdefault(iso, set()).add((y, q))

    rows = []
    for iso3, year, quarter in panel_iso_quarters:
        starts = starts_by_iso.get(iso3, set())
        in_def = in_default_by_iso.get(iso3, set())
        is_in_default = int((int(year), int(quarter)) in in_def)
        rec = {
            'iso3': iso3,
            'year': int(year),
            'quarter': int(quarter),
            'in_default_quarter': is_in_default,
        }
        for h_q in horizons_quarters:
            rec[f'defaulted_within_{h_q}q'] = max(
                is_in_default,
                defaulted_within_quarters(
                    starts, int(year), int(quarter), int(h_q),
                ),
            )
        rows.append(rec)
    return pd.DataFrame(rows)
