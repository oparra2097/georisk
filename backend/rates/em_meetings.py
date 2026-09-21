"""
EM central bank meeting calendars — hardcoded 2026-2027.

Each central bank publishes its own annual policy meeting calendar.
We hardcode rather than scrape because (a) each CB has a different
page layout, (b) most publish PDFs, and (c) maintenance is < 30 min
per year for all 12 combined.

Refresh annually against each CB's published calendar. Sources:

- BCB (Brazil): https://www.bcb.gov.br/en/monetarypolicy/copom
- Banxico (Mexico): https://www.banxico.org.mx/monetary-financial-policies
- BanRep (Colombia): https://www.banrep.gov.co/en/policy-rate
- BCCh (Chile): https://www.bcentral.cl/en/monetary-policy
- BCRP (Peru): https://www.bcrp.gob.pe/en/monetary-policy
- BCRA (Argentina): decisions ad-hoc, not scheduled
- PBoC (China): no fixed schedule; monthly loan prime rate window
- RBI (India): six per year, calendar published in April
- BOK (South Korea): eight per year, calendar published in December
- BI (Indonesia): monthly (Board of Governors)
- BOT (Thailand): 6 per year
- BSP (Philippines): 8 per year

Meeting dates below are the announcement day (the day the rate
decision is publicly released).
"""

from __future__ import annotations

from datetime import date
from typing import Dict, List


# 2026 + 2027 meeting calendars per country. Format: ISO3 → list of dates.
# Argentina / China listed with a monthly proxy since neither runs a
# strict "meeting" schedule (BCRA decisions are ad-hoc, PBoC operates
# a monthly LPR window on the 20th). Users understand these are
# approximations.

_MEETINGS: Dict[str, List[date]] = {
    'JPN': [
        # BOJ meetings — 8 per year, second day (announcement day).
        date(2026, 1, 23), date(2026, 3, 18), date(2026, 5, 1), date(2026, 6, 17),
        date(2026, 7, 31), date(2026, 9, 18), date(2026, 10, 30), date(2026, 12, 18),
        date(2027, 1, 22), date(2027, 3, 17),
    ],
    'BRA': [
        date(2026, 1, 28), date(2026, 3, 18), date(2026, 5, 6), date(2026, 6, 17),
        date(2026, 7, 29), date(2026, 9, 16), date(2026, 10, 28), date(2026, 12, 9),
        date(2027, 1, 27), date(2027, 3, 17), date(2027, 5, 5), date(2027, 6, 16),
    ],
    'MEX': [
        date(2026, 2, 5), date(2026, 3, 26), date(2026, 5, 14), date(2026, 6, 25),
        date(2026, 8, 6), date(2026, 9, 24), date(2026, 11, 5), date(2026, 12, 17),
        date(2027, 2, 4), date(2027, 3, 25), date(2027, 5, 13), date(2027, 6, 24),
    ],
    'COL': [
        date(2026, 1, 30), date(2026, 3, 27), date(2026, 4, 30), date(2026, 6, 26),
        date(2026, 7, 31), date(2026, 9, 25), date(2026, 10, 30), date(2026, 12, 18),
        date(2027, 1, 29), date(2027, 3, 26),
    ],
    'CHL': [
        date(2026, 1, 28), date(2026, 3, 18), date(2026, 4, 29), date(2026, 6, 17),
        date(2026, 7, 29), date(2026, 9, 3), date(2026, 10, 28), date(2026, 12, 17),
        date(2027, 1, 27), date(2027, 3, 17),
    ],
    'PER': [
        date(2026, 1, 8), date(2026, 2, 12), date(2026, 3, 12), date(2026, 4, 9),
        date(2026, 5, 14), date(2026, 6, 11), date(2026, 7, 9), date(2026, 8, 13),
        date(2026, 9, 10), date(2026, 10, 8), date(2026, 11, 12), date(2026, 12, 10),
    ],
    'ARG': [
        # BCRA decisions are ad-hoc; use monthly cadence as approximation.
        date(2026, 1, 15), date(2026, 2, 12), date(2026, 3, 12), date(2026, 4, 16),
        date(2026, 5, 14), date(2026, 6, 11), date(2026, 7, 16), date(2026, 8, 13),
        date(2026, 9, 10), date(2026, 10, 15), date(2026, 11, 12), date(2026, 12, 10),
    ],
    'CHN': [
        # PBoC LPR fix is on the 20th of each month.
        date(2026, 1, 20), date(2026, 2, 20), date(2026, 3, 20), date(2026, 4, 20),
        date(2026, 5, 20), date(2026, 6, 22), date(2026, 7, 20), date(2026, 8, 20),
        date(2026, 9, 21), date(2026, 10, 20), date(2026, 11, 20), date(2026, 12, 21),
    ],
    'IND': [
        date(2026, 2, 6), date(2026, 4, 8), date(2026, 6, 5), date(2026, 8, 6),
        date(2026, 10, 8), date(2026, 12, 5),
        date(2027, 2, 5), date(2027, 4, 8),
    ],
    'KOR': [
        date(2026, 1, 15), date(2026, 2, 26), date(2026, 4, 9), date(2026, 5, 28),
        date(2026, 7, 9), date(2026, 8, 27), date(2026, 10, 15), date(2026, 11, 26),
        date(2027, 1, 14), date(2027, 2, 25),
    ],
    'IDN': [
        date(2026, 1, 21), date(2026, 2, 19), date(2026, 3, 18), date(2026, 4, 22),
        date(2026, 5, 20), date(2026, 6, 17), date(2026, 7, 15), date(2026, 8, 19),
        date(2026, 9, 16), date(2026, 10, 21), date(2026, 11, 19), date(2026, 12, 16),
    ],
    'THA': [
        date(2026, 2, 4), date(2026, 4, 1), date(2026, 6, 24), date(2026, 8, 12),
        date(2026, 10, 7), date(2026, 12, 16),
        date(2027, 2, 3), date(2027, 4, 7),
    ],
    'PHL': [
        date(2026, 2, 12), date(2026, 4, 2), date(2026, 5, 21), date(2026, 6, 25),
        date(2026, 8, 13), date(2026, 10, 8), date(2026, 11, 19), date(2026, 12, 17),
        date(2027, 2, 11), date(2027, 4, 1),
    ],
}


def get_next_meeting(iso3: str, anchor: date | None = None) -> date | None:
    """Return the next scheduled meeting date after ``anchor`` (default
    today). None if no upcoming meeting is on file (means the calendar
    needs refresh)."""
    anchor = anchor or date.today()
    meetings = _MEETINGS.get(iso3.upper(), [])
    upcoming = [m for m in meetings if m > anchor]
    return upcoming[0] if upcoming else None


def get_upcoming_meetings(iso3: str, n: int = 4,
                          anchor: date | None = None) -> List[date]:
    """Return the next ``n`` meetings after ``anchor``."""
    anchor = anchor or date.today()
    meetings = _MEETINGS.get(iso3.upper(), [])
    upcoming = [m for m in meetings if m > anchor]
    return upcoming[:n]
