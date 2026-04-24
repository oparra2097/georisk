"""
S&P/Case-Shiller Home Price Indices via FRED.

National composite (CSUSHPINSA) + 20 metro members. All series are
non-seasonally-adjusted — we apply X-12 / STL only if needed downstream.

Reuses backend.data_sources.fred_client (already wired with FRED_API_KEY
and the 6-hour cache). Emits HpiRow records in the same shape as FHFA
so downstream code doesn't need to know which source produced what.
"""

from __future__ import annotations

import logging
from typing import Optional

from backend.data_sources import fred_client
from backend.house_prices.fetchers.fhfa import HpiRow
from backend.house_prices.sources import CASE_SHILLER_CITIES, CASE_SHILLER_NATIONAL_FRED

logger = logging.getLogger(__name__)


def _to_monthly_rows(observations, level: str, code: str, name: str) -> list[HpiRow]:
    out: list[HpiRow] = []
    for obs in observations:
        try:
            year_s, month_s = obs['date'].split('-')[:2]
            yr = int(year_s)
            mo = int(month_s)
        except (KeyError, ValueError, IndexError):
            continue
        val = obs.get('value')
        if val is None:
            continue
        out.append(HpiRow(
            level=level,
            code=code,
            name=name,
            year=yr,
            period=mo,
            freq='monthly',
            index_nsa=float(val),
            index_sa=None,
        ))
    return out


def fetch_national() -> list[HpiRow]:
    """CSUSHPINSA — national 20-city composite, monthly NSA."""
    obs = fred_client.fetch_series(CASE_SHILLER_NATIONAL_FRED)
    if not obs:
        logger.warning('case_shiller national: no data (FRED_API_KEY set?)')
        return []
    return _to_monthly_rows(obs, level='national', code='CS_NATIONAL',
                             name='S&P/Case-Shiller US National HPI')


def fetch_cities() -> list[HpiRow]:
    """Each 20-city metro, monthly NSA."""
    rows: list[HpiRow] = []
    for city_key, (metro_name, fred_id) in CASE_SHILLER_CITIES.items():
        obs = fred_client.fetch_series(fred_id)
        if not obs:
            logger.debug(f'case_shiller {city_key}: no data from {fred_id}')
            continue
        rows.extend(_to_monthly_rows(
            obs, level='msa', code=f'CS_{city_key}', name=metro_name,
        ))
    logger.info(f'case_shiller cities: {len(rows)} monthly rows')
    return rows


def fetch_all() -> list[HpiRow]:
    return fetch_national() + fetch_cities()
