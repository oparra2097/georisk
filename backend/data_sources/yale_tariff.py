"""
Yale Budget Lab — Average Effective Tariff Rate data source.

Loads a pre-computed series of the US statutory tariff rate from
static/data/yale_tariff_rates.json. The "collected_series" (Apollo /
Torsten Slok methodology) is overlaid live from the US Treasury Fiscal
Data API + FRED so it auto-updates monthly instead of relying on the
hardcoded values in the JSON file.
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
    Return the Yale Budget Lab tariff rate data, with the collected
    (Apollo/Slok) sub-series overlaid live from Treasury + FRED.

    Non-blocking: if the Apollo cache is cold, returns hardcoded data
    immediately and triggers a background fetch so the next call is warm.
    """
    data = {}
    if _STATIC_JSON.exists():
        try:
            with open(_STATIC_JSON) as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load Yale tariff JSON: {e}")

    if not data:
        data = {
            "error": "Yale tariff data not found.",
            "points": [],
            "notes": [],
        }

    # Overlay live Apollo/Slok collected-rate series — non-blocking.
    try:
        from backend.data_sources.apollo_tariff import get_apollo_tariff_data
        import backend.data_sources.apollo_tariff as _apollo_mod
        import time as _time
        import threading

        # If cache is warm, use it. If cold, return hardcoded and warm in background.
        with _apollo_mod._lock:
            cache_warm = (_apollo_mod._cached_data is not None
                          and (_time.time() - _apollo_mod._cached_at) < _apollo_mod.CACHE_TTL)

        if cache_warm:
            apollo = get_apollo_tariff_data() or {}
        else:
            apollo = {}
            # Trigger background warm-up (fire-and-forget)
            def _bg_warm():
                try:
                    get_apollo_tariff_data()
                    logger.info("Apollo tariff cache warmed in background")
                except Exception as ex:
                    logger.warning(f"Apollo background warm failed: {ex}")
            threading.Thread(target=_bg_warm, daemon=True).start()

        apollo_points = apollo.get('points') or []
        if apollo_points:
            existing = data.get('collected_series') or {}
            data['collected_series'] = {
                'source': apollo.get('source') or existing.get('source', ''),
                'source_url': apollo.get('source_url') or existing.get('source_url', ''),
                'source_detail': apollo.get('source_detail') or existing.get('source_detail', ''),
                'title': apollo.get('title') or existing.get('title', ''),
                'subtitle': apollo.get('subtitle') or existing.get('subtitle', ''),
                'unit': apollo.get('unit') or existing.get('unit', ''),
                'frequency': apollo.get('frequency') or existing.get('frequency', ''),
                'last_updated': apollo.get('last_updated'),
                'latest_date': apollo.get('latest_date'),
                'latest_value': apollo.get('latest_value'),
                'peak_date': apollo.get('peak_date'),
                'peak_value': apollo.get('peak_value'),
                'methodology': apollo.get('methodology') or existing.get('methodology', ''),
                'points': apollo_points,
                'notes': existing.get('notes', []),
            }
            if apollo.get('latest_value') is not None:
                data['latest_collected'] = apollo['latest_value']
    except Exception as e:
        logger.warning(
            f"Failed to overlay Apollo live tariff series; "
            f"keeping hardcoded collected_series. Error: {e}"
        )

    return data
