"""
Fertilizer & EM Inflation Impact data source.

Loads pre-computed analysis from static/data/fertilizer_em_inflation.json
and enriches current_cpi values at serve-time with live IMF WEO data
(PCPIPCH indicator — annual % change in consumer prices).

Data: fertilizer price forecasts (Urea, DAP, Potash) and CPI inflation
impact estimates for 21 emerging-market countries under 3 macro scenarios.
"""

import json
import copy
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_STATIC_JSON = Path(__file__).resolve().parent.parent.parent / "static" / "data" / "fertilizer_em_inflation.json"

# ── Cache ────────────────────────────────────────────────────────────────
_CACHE = {}
_CACHE_TIME = None
_CACHE_TTL = timedelta(hours=6)

# ── ISO3 mapping for the 21 EM countries ─────────────────────────────────
EM_ISO3 = {
    "Ethiopia": "ETH", "Nigeria": "NGA", "Bangladesh": "BGD",
    "Ghana": "GHA", "Pakistan": "PAK", "Zambia": "ZMB",
    "Philippines": "PHL", "Kenya": "KEN", "Mozambique": "MOZ",
    "Sri Lanka": "LKA", "Egypt": "EGY", "Turkey": "TUR",
    "India": "IND", "Tanzania": "TZA", "Thailand": "THA",
    "South Africa": "ZAF", "Indonesia": "IDN", "Brazil": "BRA",
    "Colombia": "COL", "Mexico": "MEX", "Argentina": "ARG",
}


def _enrich_with_live_cpi(data):
    """
    Enrich current_cpi with live IMF WEO inflation data when the WEO has
    newer actual data than our static JSON. Only uses WEO actuals (not
    forecasts) — the static JSON already contains Feb/Mar 2026 reported
    CPI from national statistics agencies, which is more current than
    the WEO's October 2025 vintage estimates.

    Falls back silently to static values if WEO is unavailable.
    """
    try:
        from backend.data_sources.imf_weo import get_weo_data
        weo = get_weo_data('PCPIPCH')
        if not weo or 'countries' not in weo:
            return data

        weo_countries = weo['countries']
        forecast_start = weo.get('forecast_start_year', 2026)

        for country_name, info in data.get('countries', {}).items():
            iso3 = EM_ISO3.get(country_name)
            if not iso3 or iso3 not in weo_countries:
                continue

            values = weo_countries[iso3].get('values', {})
            if not values:
                continue

            # Only use WEO actuals (years before forecast_start_year)
            actual_years = sorted(
                [int(y) for y in values.keys()
                 if y.isdigit() and values[y] is not None and int(y) < forecast_start],
                reverse=True
            )
            if not actual_years:
                continue

            # Only override if WEO has current-year actual data
            # (i.e., forecast_start_year > current year, meaning current year is actual)
            latest_actual_year = actual_years[0]
            latest_cpi = values[str(latest_actual_year)]

            # Store WEO reference for transparency, but don't override
            # static values with older WEO estimates — our static JSON
            # has Feb/Mar 2026 actuals from national stats agencies
            info['weo_latest_actual'] = round(latest_cpi, 1)
            info['weo_latest_year'] = latest_actual_year

    except Exception as e:
        logger.debug(f"WEO enrichment unavailable: {e}")

    return data


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
                # Deep copy so WEO enrichment doesn't mutate the raw JSON
                result = copy.deepcopy(result)
                result = _enrich_with_live_cpi(result)
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
