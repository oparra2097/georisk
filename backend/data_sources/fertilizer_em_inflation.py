"""
Fertilizer & EM Inflation Impact data source.

Loads pre-computed analysis from static/data/fertilizer_em_inflation.json
(committed to repo, deployed with the app).

Data: fertilizer price forecasts (Urea, DAP, Potash) and CPI inflation
impact estimates for 21 emerging-market countries under 3 macro scenarios.
"""

import json
from pathlib import Path
from datetime import datetime, timedelta

_STATIC_JSON = Path(__file__).resolve().parent.parent.parent / "static" / "data" / "fertilizer_em_inflation.json"

# ── Cache ────────────────────────────────────────────────────────────────
_CACHE = {}
_CACHE_TIME = None
_CACHE_TTL = timedelta(hours=6)


def get_fertilizer_em_data():
    """
    Return fertilizer forecast + EM inflation impact data.

    Returns dict with:
      - fertilizer_forecasts: {Urea/DAP/Potash: {scenarios, baseline, yoy}}
      - countries: {country_name: {impact metrics, scenarios}}
      - summary: {tier_counts, most_impacted, blended_fert_shock}
      - energy_inputs: {Brent/TTF/HH: {baseline, yoy, weighted_avg}}
      - scenario_weights: {Base Case: 0.7, ...}
    """
    global _CACHE, _CACHE_TIME

    if _CACHE_TIME and datetime.now() - _CACHE_TIME < _CACHE_TTL and _CACHE:
        return _CACHE

    if _STATIC_JSON.exists():
        try:
            with open(_STATIC_JSON) as f:
                result = json.load(f)
            if result.get("countries"):
                _CACHE = result
                _CACHE_TIME = datetime.now()
                return result
        except Exception:
            pass

    return {
        "error": "Fertilizer & EM inflation data not found.",
        "countries": {},
        "fertilizer_forecasts": {},
        "summary": {},
    }
