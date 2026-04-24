"""
Macro sub-score adapter.

Reuses existing backend.data_sources.world_bank_wgi.fetch_base_scores() so we
don't duplicate the 12-indicator fetch/cache logic or the per-indicator risk
converters. We extract the macro_score + the raw indicator values as drivers.
"""

import logging
from typing import Optional

from backend.data_sources.world_bank_wgi import (
    fetch_base_scores,
    MACRO_INDICATORS,
    WGI_INDICATORS,
)

logger = logging.getLogger(__name__)


def get_macro_sub_score(country_code: str) -> dict:
    """
    Return {'value': 0-100, 'drivers': {...}, 'asof': None} for the macro sub-score.

    `country_code` is ISO-2 (matches world_bank_wgi's keying).
    """
    all_scores = fetch_base_scores()
    entry = all_scores.get(country_code.upper())
    if not entry:
        return {'value': 50.0, 'drivers': {'note': 'no data'}, 'asof': None}

    macro_score = entry.get('macro_score', 50.0)
    macro_raw = entry.get('macro', {}) or {}

    drivers = {}
    for code, label in MACRO_INDICATORS.items():
        drivers[label] = macro_raw.get(code)

    return {
        'value': macro_score,
        'drivers': drivers,
        'asof': None,  # WB indicators are annual with variable lag; cache is 30d
    }


def get_structural_sub_score(country_code: str) -> dict:
    """WGI governance composite — the 'structural' sub-score."""
    all_scores = fetch_base_scores()
    entry = all_scores.get(country_code.upper())
    if not entry:
        return {'value': 50.0, 'drivers': {'note': 'no data'}, 'asof': None}

    gov_score = entry.get('governance_score', 50.0)
    wgi_raw = entry.get('wgi', {}) or {}

    drivers = {}
    for code, label in WGI_INDICATORS.items():
        drivers[label] = wgi_raw.get(code)

    return {
        'value': gov_score,
        'drivers': drivers,
        'asof': None,
    }
