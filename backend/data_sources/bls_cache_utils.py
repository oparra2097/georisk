"""
Shared BLS cache helpers.

`is_stale` answers the question: did the BLS fetch return data that's
suspiciously far behind today?  If yes, the cache layer treats the
result as a soft failure — it'll keep serving the (stale) data so the
page doesn't break, but it expires the cache aggressively so the next
request retries.

Two failure modes this guards against:
  1. BLS occasionally returns a successful response with old data
     (transient API glitch, slow data-pipeline propagation).
  2. The deploy warm-up populated the cache from a fetch that came back
     stale, and the 24h TTL then locks us into that stale snapshot for
     an entire day.

Threshold: 2 calendar months behind today.  BLS publishes Employment
Situation in the first week of each month and CPI in the second week —
both for the prior month — so a healthy `latest_month` is at most one
month behind.  Two months gives us a comfortable buffer for normal
release-day timing while still catching a multi-month drift.
"""

from __future__ import annotations

from datetime import date
from typing import Optional


STALE_LAG_MONTHS = 2
SOFT_RETRY_SECONDS = 1800   # 30 min — how long to back off when stale


def is_stale(latest_month: Optional[str], today: Optional[date] = None) -> bool:
    """Return True if `latest_month` ('YYYY-MM') is more than the
    threshold months behind `today` (default: today's date).
    """
    if not latest_month or len(latest_month) < 7:
        return True
    try:
        ly, lm = int(latest_month[:4]), int(latest_month[5:7])
    except (ValueError, TypeError):
        return True
    if today is None:
        today = date.today()
    gap_months = (today.year - ly) * 12 + (today.month - lm)
    return gap_months > STALE_LAG_MONTHS


def months_behind(latest_month: Optional[str], today: Optional[date] = None) -> int:
    if not latest_month or len(latest_month) < 7:
        return -1
    try:
        ly, lm = int(latest_month[:4]), int(latest_month[5:7])
    except (ValueError, TypeError):
        return -1
    if today is None:
        today = date.today()
    return (today.year - ly) * 12 + (today.month - lm)
