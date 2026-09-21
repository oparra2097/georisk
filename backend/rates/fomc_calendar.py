"""
FOMC meeting calendar.

The Fed publishes its FOMC meeting schedule ~15 months in advance at
https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm. We
hardcode the meeting dates rather than scrape (the page is JS-driven
and the CSS classes drift). Refresh this list by hand once a year —
maintenance is < 5 minutes for the whole schedule.

The dates below are the SECOND day of each two-day FOMC meeting — the
day the statement + SEP release lands and any policy change takes
effect. That's the date the FedWatch calc treats as "meeting day".
"""

from __future__ import annotations

from datetime import date
from typing import List


# 2026 FOMC meetings — published Dec 2025 at
# https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.
# All dates are the announcement day (Wed of two-day meetings).
_MEETINGS_2026: List[date] = [
    date(2026, 1, 28),
    date(2026, 3, 18),
    date(2026, 4, 29),
    date(2026, 6, 17),
    date(2026, 7, 29),
    date(2026, 9, 16),
    date(2026, 10, 28),
    date(2026, 12, 9),
]

# 2027 FOMC meetings — tentative, released Sept 2026 alongside SEP.
# Update when Fed publishes final calendar.
_MEETINGS_2027: List[date] = [
    date(2027, 1, 27),
    date(2027, 3, 17),
    date(2027, 4, 28),
    date(2027, 6, 16),
    date(2027, 7, 28),
    date(2027, 9, 22),
    date(2027, 11, 3),
    date(2027, 12, 15),
]

ALL_MEETINGS: List[date] = sorted(_MEETINGS_2026 + _MEETINGS_2027)


def get_upcoming_meetings(anchor: date | None = None, n: int = 8) -> List[date]:
    """Return the next ``n`` FOMC meeting dates strictly after ``anchor``
    (default today). If the anchor falls between two meetings we return
    the one after it; a meeting DAY is not included in its own list (the
    market has already priced its outcome by the time we look up the
    calendar the same evening).
    """
    anchor = anchor or date.today()
    upcoming = [m for m in ALL_MEETINGS if m > anchor]
    return upcoming[:n]


def get_previous_meeting(anchor: date | None = None) -> date | None:
    """Return the most recent past meeting date (or None if the anchor
    predates our recorded schedule)."""
    anchor = anchor or date.today()
    past = [m for m in ALL_MEETINGS if m <= anchor]
    return past[-1] if past else None
