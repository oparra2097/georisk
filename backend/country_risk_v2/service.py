"""
Service facade: cached top-level API used by the Flask routes.

Caches scored countries for 30 minutes. Mirrors the threading.Lock + TTL
pattern used elsewhere in the codebase (e.g. backend/data_sources/eurostat_hicp.py).
"""

import threading
import time
import logging
from typing import Optional, Dict, List

from backend.country_risk_v2.models import CountryRiskV2
from backend.country_risk_v2.scoring import compute_composite
from backend.country_risk_v2.country_configs import PRIORITY_ORDER, DISPLAY_NAMES

logger = logging.getLogger(__name__)

CACHE_TTL = 1800  # 30 minutes — matches SCORE_CACHE_TTL_MINUTES in config.py

_lock = threading.RLock()
_cache: Dict[str, dict] = {}  # {code: {'risk': CountryRiskV2, 'fetched_at': float}}


def score_country(country_code: str, force_refresh: bool = False, scope: str = 'eu27') -> Optional[CountryRiskV2]:
    """
    Return scored CountryRiskV2, or None if unsupported / no data.

    `scope` only applies to aggregate codes: 'eu27' (default) or 'ea' for
    euro-area 20. Both are cached separately.
    """
    code = country_code.upper()
    cache_key = f'{code}:{scope}' if code in ('EU', 'EA') else code

    with _lock:
        cached = _cache.get(cache_key)
        if cached and not force_refresh and (time.time() - cached['fetched_at']) < CACHE_TTL:
            return cached['risk']

    if code in ('EU', 'EA'):
        from backend.country_risk_v2.eu_aggregate import build_eu_score
        try:
            risk = build_eu_score(scope='ea' if code == 'EA' or scope == 'ea' else 'eu27')
        except Exception as e:
            logger.exception(f"build_eu_score failed: {e}")
            return None
    else:
        try:
            risk = compute_composite(code)
        except Exception as e:
            logger.exception(f"compute_composite failed for {code}: {e}")
            return None

    if risk is None:
        return None

    with _lock:
        _cache[cache_key] = {'risk': risk, 'fetched_at': time.time()}
    return risk


def score_all(force_refresh: bool = False) -> List[CountryRiskV2]:
    """Score every country in PRIORITY_ORDER. Skips any that return None."""
    out = []
    for code in PRIORITY_ORDER:
        r = score_country(code, force_refresh=force_refresh)
        if r is not None:
            out.append(r)
    return out


def get_supported_countries() -> List[dict]:
    """Lightweight coverage listing for /api/country-risk/countries."""
    return [
        {'country_code': code, 'country_name': DISPLAY_NAMES.get(code, code)}
        for code in PRIORITY_ORDER
    ]


def clear_cache():
    with _lock:
        _cache.clear()
