"""
BLS news-release calendar.

BLS publishes its release schedule a year in advance.  We hardcode the
2026 schedule for the two releases we surface (Employment Situation and
CPI) and fall back to a formula-based estimator for months we haven't
hardcoded (so the "next release" countdown keeps working into 2027+).

All datetimes are UTC; the helper converts from the 8:30 AM US/Eastern
publication time published by BLS.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional

# ── Hardcoded 2026 BLS schedule (from bls.gov/schedule/news_release) ──
# Format: list of (year, month, day) tuples for the *release* date.
# Each release is at 8:30 AM US/Eastern.  When DST is in effect
# (mid-Mar–early-Nov) 8:30 ET = 12:30 UTC; otherwise 13:30 UTC.  The
# `_to_utc` helper handles the conversion.

EMPLOYMENT_SITUATION_2026 = [
    (2026, 1, 9),    # for Dec 2025 data
    (2026, 2, 6),    # Jan 2026
    (2026, 3, 6),    # Feb 2026
    (2026, 4, 3),    # Mar 2026
    (2026, 5, 1),    # Apr 2026
    (2026, 6, 5),    # May 2026
    (2026, 7, 2),    # Jun 2026 — shifted off Jul 3/4 holiday
    (2026, 8, 7),    # Jul 2026
    (2026, 9, 4),    # Aug 2026
    (2026, 10, 2),   # Sep 2026
    (2026, 11, 6),   # Oct 2026
    (2026, 12, 4),   # Nov 2026
]

CPI_2026 = [
    (2026, 1, 14),   # for Dec 2025 data
    (2026, 2, 11),   # Jan 2026
    (2026, 3, 11),   # Feb 2026
    (2026, 4, 14),   # Mar 2026
    (2026, 5, 12),   # Apr 2026
    (2026, 6, 10),   # May 2026
    (2026, 7, 14),   # Jun 2026
    (2026, 8, 12),   # Jul 2026
    (2026, 9, 10),   # Aug 2026
    (2026, 10, 14),  # Sep 2026
    (2026, 11, 12),  # Oct 2026
    (2026, 12, 10),  # Nov 2026
]

# Generic schedule registry, keyed by release type
SCHEDULE: Dict[str, List[tuple]] = {
    'employment_situation': EMPLOYMENT_SITUATION_2026,
    'cpi': CPI_2026,
}

RELEASE_LABELS = {
    'employment_situation': 'Employment Situation',
    'cpi': 'Consumer Price Index',
}

RELEASE_DATA_LABEL = {
    'employment_situation': 'Nonfarm payrolls + unemployment rate',
    'cpi': 'Consumer Price Index, all items',
}

# US/Eastern offsets vs UTC (no zoneinfo dependency to keep boot tight).
# DST runs from second Sun of March through first Sun of November.
def _is_us_dst(d: date) -> bool:
    if d.month < 3 or d.month > 11:
        return False
    if 4 <= d.month <= 10:
        return True
    if d.month == 3:
        # second Sunday of March
        second_sun = _nth_weekday(d.year, 3, calendar.SUNDAY, 2)
        return d >= second_sun
    if d.month == 11:
        # before first Sunday of November
        first_sun = _nth_weekday(d.year, 11, calendar.SUNDAY, 1)
        return d < first_sun
    return False


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the date of the n-th occurrence of `weekday` in (year, month)."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _to_utc(d: date, hour_local: int = 8, minute_local: int = 30) -> datetime:
    """Convert 8:30 AM US/Eastern on `d` to a UTC datetime."""
    offset_hours = 4 if _is_us_dst(d) else 5  # ET is UTC-4 in DST, else UTC-5
    naive_local = datetime(d.year, d.month, d.day, hour_local, minute_local)
    return (naive_local + timedelta(hours=offset_hours)).replace(tzinfo=timezone.utc)


# ── Fallback estimators (post-2026 / missing months) ──────────────────

def _formula_release(release: str, year: int, month: int) -> Optional[date]:
    """Estimate a release date using BLS's typical pattern."""
    if release == 'employment_situation':
        # First Friday of the month, shifted off federal holidays.
        d = _nth_weekday(year, month, calendar.FRIDAY, 1)
        # July 4 collision: shift to Thursday before
        if month == 7 and d.day in (3, 4):
            d -= timedelta(days=1)
        return d
    if release == 'cpi':
        # Second full week, Wednesday — historically the most common day.
        return _nth_weekday(year, month, calendar.WEDNESDAY, 2)
    return None


# ── Public API ─────────────────────────────────────────────────────────

def upcoming_releases(now: Optional[datetime] = None, limit: int = 12) -> List[dict]:
    """Return the next `limit` releases across all types, oldest first."""
    now = now or datetime.now(timezone.utc)
    out: List[dict] = []
    for rtype, sched in SCHEDULE.items():
        for (y, m, d) in sched:
            dt_utc = _to_utc(date(y, m, d))
            if dt_utc >= now - timedelta(hours=2):
                out.append(_format(rtype, dt_utc, hardcoded=True))
    out.sort(key=lambda r: r['release_at_utc'])
    return out[:limit]


def next_release(release: str, now: Optional[datetime] = None) -> dict:
    """Return the next release for a given type (hardcoded or formula)."""
    now = now or datetime.now(timezone.utc)
    sched = SCHEDULE.get(release, [])
    for (y, m, d) in sched:
        dt_utc = _to_utc(date(y, m, d))
        if dt_utc >= now - timedelta(hours=2):
            return _format(release, dt_utc, hardcoded=True)

    # Fall back to formula for months past the hardcoded schedule.
    cursor = now.date().replace(day=1)
    for _ in range(13):
        d = _formula_release(release, cursor.year, cursor.month)
        if d:
            dt_utc = _to_utc(d)
            if dt_utc >= now - timedelta(hours=2):
                return _format(release, dt_utc, hardcoded=False)
        # advance to next month
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)

    return {'release': release, 'unavailable': True}


def all_releases_for_year(year: int) -> Dict[str, List[dict]]:
    """All known releases for `year`, grouped by release type."""
    out: Dict[str, List[dict]] = {}
    for rtype, sched in SCHEDULE.items():
        out[rtype] = [
            _format(rtype, _to_utc(date(y, m, d)), hardcoded=True)
            for (y, m, d) in sched if y == year
        ]
    return out


def cron_kwargs_for(release: str) -> List[dict]:
    """APScheduler cron-trigger kwargs for every hardcoded release date
    of `release` in the future.  The job will fire at 8:35 AM ET (5min
    after the BLS embargo lifts) on that exact date.
    """
    now = datetime.now(timezone.utc)
    triggers: List[dict] = []
    for (y, m, d) in SCHEDULE.get(release, []):
        dt_utc = _to_utc(date(y, m, d), hour_local=8, minute_local=35)
        if dt_utc < now:
            continue
        triggers.append({
            'year': y, 'month': m, 'day': d,
            'hour': dt_utc.hour, 'minute': dt_utc.minute,
        })
    return triggers


def _format(release: str, dt_utc: datetime, hardcoded: bool) -> dict:
    now = datetime.now(timezone.utc)
    delta = dt_utc - now
    return {
        'release': release,
        'label': RELEASE_LABELS.get(release, release),
        'data_label': RELEASE_DATA_LABEL.get(release, ''),
        'release_at_utc': dt_utc.isoformat().replace('+00:00', 'Z'),
        'release_date': dt_utc.date().isoformat(),
        'time_eastern': '08:30 ET',
        'is_hardcoded': hardcoded,
        'seconds_until': int(delta.total_seconds()),
        'days_until': delta.days,
        'is_imminent': 0 <= delta.total_seconds() <= 3 * 24 * 3600,
        'is_today': dt_utc.date() == now.date(),
    }
