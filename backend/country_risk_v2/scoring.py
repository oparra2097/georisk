"""
Composite scoring: combines structural (WGI), macro, and labor sub-scores
using per-country weights.
"""

import logging
from datetime import datetime
from typing import Optional

from backend.country_risk_v2.models import CountryRiskV2, SubScore
from backend.country_risk_v2.country_configs import get_weights, DISPLAY_NAMES
from backend.country_risk_v2.macro import get_structural_sub_score, get_macro_sub_score
from backend.country_risk_v2.youth_unemployment import get_youth_unemployment
from backend.country_risk_v2.indicators import youth_unemp_risk

logger = logging.getLogger(__name__)


def _labor_sub_score(country_code: str) -> Optional[dict]:
    """Build the labor sub-score from youth unemp. Returns None if no data."""
    raw = get_youth_unemployment(country_code)
    if not raw:
        return None

    # Build history excluding the most recent point (for delta_12m reference)
    history = raw['history']
    scored = youth_unemp_risk(
        level=raw['level'],
        history=history,
        delta_12m=raw.get('delta_12m'),
        total_unemp=raw.get('total_unemp'),
    )
    return {
        'value': scored['value'],
        'drivers': {
            **scored['drivers'],
            'source': raw['source'],
        },
        'asof': raw.get('asof'),
    }


def compute_composite(country_code: str) -> Optional[CountryRiskV2]:
    """
    Compute v2 country-risk score. Returns None for unsupported countries.

    Note: EU aggregate is handled separately in eu_aggregate.py (Phase 2).
    """
    code = country_code.upper()

    structural_raw = get_structural_sub_score(code)
    macro_raw = get_macro_sub_score(code)
    labor_raw = _labor_sub_score(code)

    weights = get_weights(code)

    structural = SubScore(
        name='structural',
        value=structural_raw['value'],
        weight=weights['structural'],
        drivers=structural_raw['drivers'],
    )
    macro = SubScore(
        name='macro',
        value=macro_raw['value'],
        weight=weights['macro'],
        drivers=macro_raw['drivers'],
    )

    if labor_raw is None:
        labor = SubScore(
            name='labor',
            value=50.0,
            weight=weights['labor'],
            drivers={'note': 'youth-unemployment data unavailable'},
        )
        confidence = 'low'
    else:
        labor = SubScore(
            name='labor',
            value=labor_raw['value'],
            weight=weights['labor'],
            drivers=labor_raw['drivers'],
        )
        confidence = 'high'

    composite = (
        structural.value * structural.weight
        + macro.value * macro.weight
        + labor.value * labor.weight
    )

    return CountryRiskV2(
        country_code=code,
        country_name=DISPLAY_NAMES.get(code, code),
        composite=composite,
        structural=structural,
        macro=macro,
        labor=labor,
        confidence=confidence,
        is_aggregate=False,
        data_asof={
            'structural': structural_raw.get('asof'),
            'macro': macro_raw.get('asof'),
            'labor': labor_raw.get('asof') if labor_raw else None,
        },
        updated_at=datetime.utcnow(),
    )
