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

import json
import logging
from typing import Optional

import requests

from backend.data_sources._cache import cached
from backend.data_sources import fred_client
from backend.rates.em_config import COUNTRIES

logger = logging.getLogger(__name__)

# DBnomics mirrors BIS + IMF IFS and already backs other fetchers in this
# codebase (see imf_ifs.py). We use it as the primary source because the
# OECD-on-FRED policy-rate / CPI series were discontinued in 2025, which left
# the previous FRED-first path returning empty for the whole panel.
_DBNOMICS_BASE = 'https://api.db.nomics.world/v22'


def _period_to_date(p: str) -> str:
    """DBnomics monthly period 'YYYY-MM' -> 'YYYY-MM-01'; pass others through."""
    return f'{p}-01' if len(p) == 7 else p


def _dbnomics_monthly(provider: str, dataset: str, iso2: str,
                      extra_dims: Optional[dict] = None) -> list[dict]:
    """Fetch one country's monthly series from DBnomics as a list of
    ``{'date': 'YYYY-MM-01', 'value': float}`` sorted ascending. [] on failure.

    Matches the ``fred_client.fetch_series`` contract so callers are drop-in.
    """
    dims = {'FREQ': ['M'], 'REF_AREA': [iso2]}
    if extra_dims:
        dims.update(extra_dims)
    params = {
        'dimensions': json.dumps(dims),
        'observations': '1',
        'limit': '10',
        'metadata': 'false',
    }
    url = f'{_DBNOMICS_BASE}/series/{provider}/{dataset}'
    try:
        resp = requests.get(url, params=params, timeout=45)
        resp.raise_for_status()
        docs = resp.json().get('series', {}).get('docs', []) or []
    except Exception as e:  # noqa: BLE001
        logger.warning(f'DBnomics {provider}/{dataset} {iso2} fetch failed: {e}')
        return []

    out: list[dict] = []
    for doc in docs:
        if (doc.get('dimensions', {}) or {}).get('REF_AREA') != iso2:
            continue
        for p, v in zip(doc.get('period', []) or [], doc.get('value', []) or []):
            if v is None or v == 'NA':
                continue
            try:
                out.append({'date': _period_to_date(p), 'value': float(v)})
            except (ValueError, TypeError):
                continue
    out.sort(key=lambda r: r['date'])
    return out


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
    iso2 = COUNTRIES[iso3]['iso2']

    # Primary: BIS central-bank policy rates (WS_CBPOL) via DBnomics —
    # authoritative, monthly, full panel coverage.
    data = _dbnomics_monthly('BIS', 'WS_CBPOL', iso2)
    if data and len(data) >= 24:  # need >= 2 years for any Taylor fit
        logger.info(f'{iso3} policy rate: BIS CBPOL returned {len(data)} obs')
        return data

    # Fallback 1: IMF IFS monetary-policy-related rate (FPOLM_PA) via DBnomics.
    ifs = _dbnomics_monthly('IMF', 'IFS', iso2, extra_dims={'INDICATOR': ['FPOLM_PA']})
    if ifs and len(ifs) >= 24:
        logger.info(f'{iso3} policy rate: IMF IFS FPOLM_PA returned {len(ifs)} obs')
        return ifs

    # Fallback 2: FRED OECD candidates. These series were discontinued in
    # 2025 (OECD migrated its database), so they return empty today — kept
    # in case they are restored under the same IDs.
    for candidate in _FRED_CANDIDATES.get(iso3, []):
        fred_data = fred_client.fetch_series(candidate)
        if fred_data and len(fred_data) >= 24:
            logger.info(f'{iso3} policy rate: FRED {candidate} returned {len(fred_data)} obs')
            return fred_data

    logger.warning(f'{iso3}: no policy rate history found via BIS, IMF IFS or FRED')
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

    # Primary: IMF IFS CPI index (PCPI_IX) via DBnomics → compute YoY.
    index_data = _dbnomics_monthly('IMF', 'IFS', iso2, extra_dims={'INDICATOR': ['PCPI_IX']})
    if index_data and len(index_data) >= 13:
        return _compute_yoy(index_data)

    # Fallback: OECD YoY-published series on FRED (discontinued 2025; kept
    # in case the IDs are restored).
    for candidate in (
        f'CPALTT01{iso2}M657N',  # OECD headline CPI YoY %
        f'CPALTT01{iso2}Q657N',  # Q analog if monthly missing
    ):
        data = fred_client.fetch_series(candidate)
        if data and len(data) >= 24:
            return data
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
