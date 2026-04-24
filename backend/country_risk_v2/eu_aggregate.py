"""
GDP-weighted EU aggregate risk score.

Scope defaults to EU-27. Pass scope='ea' for the euro-area (EA-20). The aggregate
is emitted as a pseudo-country with code='EU' so the frontend can list it
alongside real countries; `is_aggregate=True` lets the UI filter it out of
stack-bar views that would otherwise double-count members.

Confidence rules:
  - 'low'    if any of the Big-4 (DE/FR/IT/ES) is missing from the aggregate
  - 'medium' if <80% of member GDP is represented
  - 'high'   otherwise
"""

import logging
from datetime import datetime
from typing import List, Optional

from backend.country_risk_v2.models import CountryRiskV2, SubScore
from backend.country_risk_v2.country_configs import DISPLAY_NAMES
from backend.country_risk_v2.scoring import compute_composite
from backend.country_risk_v2.data_sources.wb_gdp_weights import get_gdp_weights

logger = logging.getLogger(__name__)

EU27 = [
    'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE',
    'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT',
    'RO', 'SK', 'SI', 'ES', 'SE',
]

# Euro-area 20 (as of 2023+): EU-27 minus BG, CZ, DK, HU, PL, RO, SE.
EA20 = [c for c in EU27 if c not in {'BG', 'CZ', 'DK', 'HU', 'PL', 'RO', 'SE'}]

BIG4 = {'DE', 'FR', 'IT', 'ES'}

# Drop members whose sub-score coverage is below this fraction.
MIN_MEMBER_COVERAGE = 0.60

# GDP coverage thresholds for confidence flag.
GDP_COVERAGE_HIGH = 0.80


def _member_coverage(risk: CountryRiskV2) -> float:
    """Fraction of sub-scores backed by real data (not the 50.0 fallback)."""
    scored = 0
    for sub in (risk.structural, risk.macro, risk.labor):
        if sub is None:
            continue
        drivers = sub.drivers or {}
        if drivers.get('note') == 'no data' or drivers.get('note', '').startswith('youth-unemployment data unavailable'):
            continue
        scored += 1
    return scored / 3.0


def build_eu_score(scope: str = 'eu27') -> Optional[CountryRiskV2]:
    """
    Build GDP-weighted EU aggregate. Returns None if no members score.
    """
    members = EA20 if scope == 'ea' else EU27
    weights = get_gdp_weights(members)

    scored: List[CountryRiskV2] = []
    for code in members:
        if code not in weights:
            logger.debug(f"EU aggregate: no GDP weight for {code}, skipping")
            continue
        try:
            risk = compute_composite(code)
        except Exception as e:
            logger.warning(f"EU aggregate: compute_composite({code}) failed: {e}")
            continue
        if risk is None:
            continue
        if _member_coverage(risk) < MIN_MEMBER_COVERAGE:
            continue
        scored.append(risk)

    if not scored:
        return None

    # Renormalize weights over members we actually scored.
    included = [r.country_code for r in scored]
    sub_weights = {c: weights[c] for c in included if c in weights}
    wsum = sum(sub_weights.values())
    if wsum <= 0:
        return None
    sub_weights = {c: w / wsum for c, w in sub_weights.items()}

    def weighted(field: str) -> float:
        return sum(getattr(r, field) * sub_weights[r.country_code] for r in scored)

    structural = SubScore(
        name='structural',
        value=sum(r.structural.value * sub_weights[r.country_code] for r in scored),
        weight=scored[0].structural.weight,
        drivers={'note': f'GDP-weighted over {len(scored)} members'},
    )
    macro = SubScore(
        name='macro',
        value=sum(r.macro.value * sub_weights[r.country_code] for r in scored),
        weight=scored[0].macro.weight,
        drivers={'note': f'GDP-weighted over {len(scored)} members'},
    )
    labor = SubScore(
        name='labor',
        value=sum(r.labor.value * sub_weights[r.country_code] for r in scored),
        weight=scored[0].labor.weight,
        drivers={'note': f'GDP-weighted over {len(scored)} members'},
    )
    composite = structural.value * structural.weight + macro.value * macro.weight + labor.value * labor.weight

    # Confidence: based on Big-4 presence and GDP coverage vs full universe.
    full_weights = get_gdp_weights(members)
    represented_gdp = sum(full_weights.get(c, 0) for c in included)
    missing_big4 = BIG4 - set(included)

    if missing_big4:
        confidence = 'low'
    elif represented_gdp < GDP_COVERAGE_HIGH:
        confidence = 'medium'
    else:
        confidence = 'high'

    name = DISPLAY_NAMES['EU'] if scope == 'eu27' else 'Euro Area (GDP-wtd)'
    return CountryRiskV2(
        country_code='EU' if scope == 'eu27' else 'EA',
        country_name=name,
        composite=composite,
        structural=structural,
        macro=macro,
        labor=labor,
        confidence=confidence,
        is_aggregate=True,
        members_included=sorted(included),
        data_asof={
            'scope': scope,
            'gdp_coverage_pct': round(represented_gdp * 100, 1),
            'missing_big4': sorted(missing_big4) if missing_big4 else [],
        },
        updated_at=datetime.utcnow(),
    )
