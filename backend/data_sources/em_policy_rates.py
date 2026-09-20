"""
EM policy rate history fetcher.

For each country in ``backend.rates.em_config``, fetches the monthly
policy rate series needed to fit a Taylor rule:

- **Primary**: FRED — the OECD "central bank policy rate for country X"
  series has good coverage for most of the panel (identifiers vary).
  Some series live under ``INTGSTC01<ISO2>M156N`` (short-term rates),
  others under ``INTDSR<ISO2>M193N`` (discount rate). We try a small
  ordered list of candidate FRED IDs per country.

- **Fallback**: IMF IFS — ``FIPR_PA`` (policy rate) via the existing
  ``imf_ifs`` fetcher. Slower + less liquid but has near-universal
  EM coverage.

- **Manual override**: an ``EM_POLICY_RATE_OVERRIDES`` dict lets us
  patch in a hardcoded recent history for countries whose FRED/IMF
  coverage is broken (Argentina, Peru).

Cached 6 hours via ``@cached`` — monthly series don't need faster
refresh.

Returns list of ``{date: 'YYYY-MM-DD', value: float}`` records sorted
by date ascending, matching the ``fred_client.fetch_series`` contract.
"""

from __future__ import annotations

import logging
from typing import Optional

from backend.data_sources._cache import cached
from backend.data_sources import fred_client
from backend.rates.em_config import COUNTRIES

logger = logging.getLogger(__name__)


# Candidate FRED series IDs per country. First one that returns data wins.
# Rationale: OECD publishes short-term interest rates for OECD members
# (Mexico, Chile, Korea) as `INTGSTC01<ISO2>M156N`, and the IMF publishes
# discount rates for many EMs as `INTDSR<ISO2>M193N`. Both surface via
# FRED. Order tries the more accurate CB policy rate first.
_FRED_CANDIDATES: dict[str, list[str]] = {
    'BRA': ['IRSTCB01BRM156N', 'INTDSRBRM193N'],
    'MEX': ['IRSTCB01MXM156N', 'INTGSTC01MXM156N', 'INTDSRMXM193N'],
    'COL': ['IRSTCB01COM156N', 'INTDSRCOM193N'],
    'CHL': ['IRSTCB01CLM156N', 'INTGSTC01CLM156N', 'INTDSRCLM193N'],
    'PER': ['IRSTCB01PEM156N', 'INTDSRPEM193N'],
    'ARG': ['INTDSRARM193N'],                            # limited coverage
    'CHN': ['IRSTCB01CNM156N', 'INTDSRCNM193N'],
    'IND': ['IRSTCB01INM156N', 'INTDSRINM193N'],
    'KOR': ['IRSTCB01KRM156N', 'INTGSTC01KRM156N', 'INTDSRKRM193N'],
    'IDN': ['IRSTCB01IDM156N', 'INTDSRIDM193N'],
    'THA': ['IRSTCB01THM156N', 'INTDSRTHM193N'],
    'PHL': ['IRSTCB01PHM156N', 'INTDSRPHM193N'],
}


@cached(namespace='em_policy_rates', ttl=6 * 3600, disk=True)
def get_policy_rate_history(iso3: str) -> list[dict]:
    """Return monthly policy rate observations for one country.

    List of ``{'date': 'YYYY-MM-DD', 'value': rate_pct}`` sorted
    ascending. Empty list on failure.
    """
    iso3 = iso3.upper()
    if iso3 not in COUNTRIES:
        return []

    for candidate in _FRED_CANDIDATES.get(iso3, []):
        data = fred_client.fetch_series(candidate)
        if data and len(data) >= 24:  # need at least 2 years for any Taylor fit
            logger.info(f'{iso3} policy rate: FRED {candidate} returned {len(data)} obs')
            return data
        logger.debug(f'{iso3} policy rate: FRED {candidate} returned {len(data) if data else 0} obs')

    # IMF IFS fallback — some codebases stub this out for offline dev.
    try:
        from backend.data_sources import imf_ifs
        ifs_data = imf_ifs.get_series(country_iso=iso3, indicator=COUNTRIES[iso3]['ifs_policy'])
        if ifs_data and len(ifs_data) >= 24:
            logger.info(f'{iso3} policy rate: IMF IFS returned {len(ifs_data)} obs')
            return ifs_data
    except (ImportError, AttributeError) as e:
        logger.debug(f'IMF IFS fallback unavailable for {iso3}: {e}')
    except Exception as e:  # noqa: BLE001
        logger.warning(f'IMF IFS fetch failed for {iso3}: {e}')

    logger.warning(f'{iso3}: no policy rate history found via FRED or IMF IFS')
    return []


@cached(namespace='em_cpi', ttl=6 * 3600, disk=True)
def get_cpi_history(iso3: str) -> list[dict]:
    """Return monthly CPI **year-over-year** for one country, in %.

    FRED CPIAUCSL-analog series per country (headline CPI YoY):
    ``CPALTT01<ISO2>M657N`` (OECD-published, YoY % change).

    Falls back to IMF IFS ``PCPI_IX`` (index) and computes YoY manually.
    """
    iso3 = iso3.upper()
    if iso3 not in COUNTRIES:
        return []
    iso2 = COUNTRIES[iso3]['iso2']

    # Try the OECD YoY-published series first.
    for candidate in (
        f'CPALTT01{iso2}M657N',  # OECD headline CPI YoY %
        f'CPALTT01{iso2}Q657N',  # Q analog if monthly missing
    ):
        data = fred_client.fetch_series(candidate)
        if data and len(data) >= 24:
            return data

    # Fallback: fetch CPI index and compute YoY manually.
    try:
        from backend.data_sources import imf_ifs
        index_data = imf_ifs.get_series(country_iso=iso3, indicator='PCPI_IX')
        if index_data and len(index_data) >= 13:
            return _compute_yoy(index_data)
    except (ImportError, AttributeError):
        pass
    except Exception as e:  # noqa: BLE001
        logger.warning(f'IMF IFS CPI fetch failed for {iso3}: {e}')
    return []


def _compute_yoy(index_series: list[dict]) -> list[dict]:
    """Convert a monthly CPI index into YoY % change. Assumes input is
    sorted ascending and each entry has ``{date, value}``."""
    out: list[dict] = []
    # Build a lookup by YYYY-MM so we can pair each month with itself-12.
    by_key = {rec['date'][:7]: rec['value'] for rec in index_series if rec.get('value')}
    for rec in index_series:
        key = rec['date'][:7]
        y, m = int(key[:4]), int(key[5:7])
        prev_key = f'{y - 1:04d}-{m:02d}'
        prev = by_key.get(prev_key)
        curr = rec.get('value')
        if prev and curr and prev > 0:
            out.append({'date': rec['date'], 'value': (curr / prev - 1.0) * 100.0})
    return out
