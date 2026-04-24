"""
Country Risk v2 — youth unemployment + macro fundamentals + global-shock scenarios.

Lives alongside the existing composite score (/api/scores). Exposes a new
/api/country-risk/* surface. Rolled out country-by-country:
  Phase 1: US
  Phase 2: GB, DE, FR, IT, ES, EU aggregate
  Phase 3: shock engine (oil, rates, USD, China GDP)
  Phase 4: MX, CO, BR, CL, AR (ILOSTAT primary)
  Phase 5: UI template + scheduler refresh
"""

from backend.country_risk_v2.service import score_country, score_all, get_supported_countries
from backend.country_risk_v2.scoring import compute_composite

__all__ = [
    'score_country',
    'score_all',
    'get_supported_countries',
    'compute_composite',
]
