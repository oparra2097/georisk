"""
Yale Budget Lab — Average Effective Tariff Rate data source.

Loads a pre-computed daily series of the US average effective tariff rate
from static/data/yale_tariff_rates.json. The series reflects policy-event
changes through November 17, 2025 on a pre-substitution basis.
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
    """
    Return the Yale Budget Lab average effective tariff rate series.

    Returns dict with:
      - source, source_detail, title, subtitle, unit, frequency
      - last_updated, latest_value
      - points: list of { date: "YYYY-MM-DD", value: float }
      - notes: list of explanatory strings
    """
    if _STATIC_JSON.exists():
        try:
            with open(_STATIC_JSON) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load Yale tariff JSON: {e}")

    return {
        "error": "Yale tariff data not found.",
        "points": [],
        "notes": [],
    }
