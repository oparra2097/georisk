"""
Youth-unemployment fetcher with source cascade.

Phase 1 implements the FRED path for US (LNS14024887 monthly, SA, 16-24y).
Other sources (ONS, Eurostat, ILOSTAT) are wired in Phases 2 & 4.

Returns a normalized dict:
    {
        'history': [float, ...]   # monthly percent values, chronological
        'level':   float,          # most recent value
        'delta_12m': float | None, # current - value 12 months prior
        'total_unemp': float | None,  # latest total unemp rate for gap calc
        'asof': 'YYYY-MM-DD'
        'source': 'fred' | 'ons' | 'eurostat' | 'ilo'
    }

Returns None when no source can supply data for the country.
"""

import logging
from typing import Optional

from backend.data_sources.fred_client import fetch_series
from backend.country_risk_v2.country_configs import YOUTH_UNEMP_SOURCES, FRED_SERIES
from backend.country_risk_v2.data_sources import ons_labour, eurostat_labour

logger = logging.getLogger(__name__)


def _fetch_fred(country_code: str) -> Optional[dict]:
    """US only: FRED LNS14024887 (16-24 unemp rate) + UNRATE (total)."""
    if country_code.upper() != 'US':
        return None

    youth = fetch_series(FRED_SERIES['youth_unemp_rate'])
    if not youth:
        return None

    history = [obs['value'] for obs in youth]
    level = history[-1]
    delta_12m = None
    if len(history) > 12:
        delta_12m = level - history[-13]

    total = None
    total_series = fetch_series(FRED_SERIES['total_unemp_rate'])
    if total_series:
        total = total_series[-1]['value']

    return {
        'history': history,
        'level': level,
        'delta_12m': delta_12m,
        'total_unemp': total,
        'asof': youth[-1]['date'],
        'source': 'fred',
        'step_for_yoy': 12,  # FRED series are monthly
    }


def _with_default_step(result, step):
    """Ensure result dicts expose step_for_yoy so scoring can honor frequency."""
    if result is not None and 'step_for_yoy' not in result:
        result = dict(result)
        result['step_for_yoy'] = step
    return result


from backend.country_risk_v2.data_sources import ilo_client  # noqa: E402

_FETCHERS = {
    'fred': _fetch_fred,
    'ons': lambda code: _with_default_step(
        ons_labour.get_uk_youth_unemployment() if code == 'GB' else None, 12),
    'eurostat': lambda code: _with_default_step(eurostat_labour.get_youth_unemployment(code), 12),
    'ilo': lambda code: _with_default_step(ilo_client.get_youth_unemployment(code), 1),
}


def get_youth_unemployment(country_code: str) -> Optional[dict]:
    """Try configured sources in order; return first that yields data."""
    code = country_code.upper()
    sources = YOUTH_UNEMP_SOURCES.get(code, [])
    for src in sources:
        fn = _FETCHERS.get(src)
        if fn is None:
            continue
        try:
            result = fn(code)
            if result is not None and result.get('history'):
                return result
        except Exception as e:
            logger.warning(f"youth_unemp source={src} failed for {code}: {e}")
    return None
