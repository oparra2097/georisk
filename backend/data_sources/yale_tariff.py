"""
US effective tariff rate data for the tariff chart.

Two series:
  - Statutory: Yale Budget Lab pre-substitution rate, policy-event dated,
    from static/data/yale_tariff_rates.json.
  - Collected: the exact Apollo / Torsten Slok series — monthly Census
    "Calculated Duty" ÷ total US goods imports × 100 (see census_tariff.py).
    Replaces the hardcoded ``collected_series`` in the JSON when available.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_STATIC_JSON = (
    Path(__file__).resolve().parent.parent.parent
    / "static" / "data" / "yale_tariff_rates.json"
)


def get_yale_tariff_data():
    data = {}
    if _STATIC_JSON.exists():
        try:
            with open(_STATIC_JSON) as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load Yale tariff JSON: {e}")

    if not data:
        data = {"error": "Yale tariff data not found.", "points": [], "notes": []}

    try:
        from backend.data_sources.census_tariff import get_census_tariff_data
        census = get_census_tariff_data(block=False) or {}
        points = census.get('points') or []
        if points:
            data['collected_series'] = {
                'source': census.get('source', ''),
                'source_url': census.get('source_url', ''),
                'source_detail': census.get('source_detail', ''),
                'title': census.get('title', ''),
                'unit': census.get('unit', ''),
                'frequency': census.get('frequency', 'Monthly'),
                'last_updated': census.get('last_updated'),
                'latest_date': census.get('latest_date'),
                'latest_value': census.get('latest_value'),
                'peak_date': census.get('peak_date'),
                'peak_value': census.get('peak_value'),
                'methodology': census.get('methodology', ''),
                'note': census.get('note', ''),
                'origin': census.get('origin', ''),
                'points': [
                    {k: p[k] for k in ('date', 'value', 'duty', 'gen_imports') if p.get(k) is not None}
                    for p in points
                ],
            }
            data['latest_collected'] = census.get('latest_value')
    except Exception as e:
        logger.warning(f"Census collected series unavailable, keeping JSON fallback: {e}")

    return data
