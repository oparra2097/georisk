"""
CME FedWatch-style probability derivation.

Standard methodology used by the CME Group's FedWatch Tool:

1. For each FOMC meeting `M` in month `Y-m`, look up the ZQ contract for
   that month. Its `implied_rate` is the market-implied AVERAGE effective
   fed funds rate over the entire month `Y-m`.

2. Decompose that average into pre-meeting and post-meeting daily rates:

       implied = (D_before / D_total) * pre_rate
               + (D_after  / D_total) * post_rate

   where `D_before` = calendar days from Day 1 up to and including the
   day before the meeting, `D_after` = the meeting day plus everything
   after, and `D_total` = days in the month.

3. Solve for `post_rate` — `pre_rate` is known (it's the current target
   range midpoint, or the previous meeting's post_rate carried forward).

4. Compare `post_rate - pre_rate = delta_bp`. This delta lands somewhere
   between two adjacent 25bp buckets. Distribute probability linearly
   between those buckets so their expected value equals `delta_bp`.

   e.g. `delta_bp = -12` sits between hold (0bp) and -25bp cut.
        prob(-25bp) = 12/25 = 0.48
        prob(hold)  = 13/25 = 0.52
        expected = 0.48*(-25) + 0.52*0 = -12  ✓

5. For moves larger than 25bp (`|delta_bp| > 25`), spill into the next
   bucket. We support 5 buckets: -50, -25, hold, +25, +50. Moves beyond
   ±50 get pinned to that outer bucket with probability 1.0 (rare in
   practice; a 75bp inter-meeting move would be a shock, not a
   modelable prior).

Reference: CME Group, "FedWatch Tool Methodology",
https://www.cmegroup.com/education/tools-and-applications/fedwatch-tool.html
"""

from __future__ import annotations

import calendar
from datetime import date
from typing import Dict, List, Optional

from backend.rates.fomc_calendar import get_upcoming_meetings

# Standard 5-bucket FedWatch distribution. Keys are (delta_bp, label).
_BUCKETS = [
    (-50, '-50bp'),
    (-25, '-25bp'),
    (0,   'hold'),
    (25,  '+25bp'),
    (50,  '+50bp'),
]


def _decompose(month_average: float, pre_rate: float, meeting_day: int,
               days_in_month: int) -> float:
    """Back out the post-meeting daily rate from the month-average
    (implied) rate and the pre-meeting rate.

    ``meeting_day`` is 1-indexed day-of-month. ``days_before`` counts
    everything up to and including the day BEFORE the meeting;
    ``days_after`` starts on the meeting day itself (the new rate is
    effective from that day).
    """
    days_before = max(0, meeting_day - 1)
    days_after = days_in_month - days_before
    if days_after <= 0:
        return month_average  # end-of-month meeting; treat as no pre period
    return (month_average * days_in_month - pre_rate * days_before) / days_after


def _bucketize(delta_bp: float) -> Dict[str, float]:
    """Distribute probability across the 5 buckets so their expected
    delta_bp equals the observed one."""
    # Pin to outer buckets when the move exceeds ±50bp.
    if delta_bp <= -50:
        return {label: (1.0 if label == '-50bp' else 0.0) for _, label in _BUCKETS}
    if delta_bp >= 50:
        return {label: (1.0 if label == '+50bp' else 0.0) for _, label in _BUCKETS}

    # Find the two adjacent buckets that straddle delta_bp.
    for i in range(len(_BUCKETS) - 1):
        low_bp, low_lbl = _BUCKETS[i]
        high_bp, high_lbl = _BUCKETS[i + 1]
        if low_bp <= delta_bp <= high_bp:
            span = high_bp - low_bp
            frac_high = (delta_bp - low_bp) / span
            frac_low = 1.0 - frac_high
            return {
                lbl: (frac_low if lbl == low_lbl
                      else frac_high if lbl == high_lbl
                      else 0.0)
                for _, lbl in _BUCKETS
            }

    # Fallback — should be unreachable.
    return {lbl: 0.0 for _, lbl in _BUCKETS}


def compute_fedwatch(curve: List[dict], current_target_mid: float,
                     meetings: Optional[List[date]] = None) -> List[dict]:
    """Build the FedWatch grid.

    Parameters
    ----------
    curve
        Output of ``rates_futures.get_fed_funds_curve()['curve']`` — a
        list of ``{contract, year, month, implied_rate, ...}`` dicts.
    current_target_mid
        Current fed funds target range midpoint in % (e.g. 4.375 for
        4.25-4.50). This is the ``pre_rate`` for the near meeting.
    meetings
        List of FOMC meeting dates to compute for. Defaults to the next
        8 upcoming meetings from ``fomc_calendar``.

    Returns
    -------
    list of dicts, one per meeting::

        {
          'meeting_date': '2026-06-17',
          'contract': 'ZQM26.CBT',
          'pre_rate': 4.375,
          'post_rate': 4.13,
          'delta_bp': -24.5,
          'probs': {'-50bp': 0.0, '-25bp': 0.98, 'hold': 0.02,
                    '+25bp': 0.0, '+50bp': 0.0},
          'implied_rate': 4.20,  # source month-avg
        }

    Meetings whose contract has no implied_rate are returned with
    ``probs = null`` so the frontend can render an empty row.
    """
    meetings = meetings or get_upcoming_meetings(n=8)
    by_ym = {(c['year'], c['month']): c for c in curve}

    rows: List[dict] = []
    pre_rate = current_target_mid

    for m in meetings:
        contract = by_ym.get((m.year, m.month))
        if not contract or contract.get('implied_rate') is None:
            rows.append({
                'meeting_date': m.isoformat(),
                'contract': (contract or {}).get('contract'),
                'pre_rate': round(pre_rate, 4),
                'post_rate': None,
                'delta_bp': None,
                'probs': None,
                'implied_rate': None,
            })
            continue

        days_in_month = calendar.monthrange(m.year, m.month)[1]
        implied = float(contract['implied_rate'])
        post_rate = _decompose(implied, pre_rate, m.day, days_in_month)
        delta_bp = (post_rate - pre_rate) * 100.0  # rate is in %, bp = %/100
        probs = _bucketize(delta_bp)

        rows.append({
            'meeting_date': m.isoformat(),
            'contract': contract['contract'],
            'pre_rate': round(pre_rate, 4),
            'post_rate': round(post_rate, 4),
            'delta_bp': round(delta_bp, 2),
            'probs': {k: round(v, 4) for k, v in probs.items()},
            'implied_rate': implied,
        })

        # Carry the post_rate forward as the next meeting's pre_rate,
        # which is what CME does — assumes the market has priced the
        # near meeting outcome by the time it prices the following one.
        pre_rate = post_rate

    return rows
