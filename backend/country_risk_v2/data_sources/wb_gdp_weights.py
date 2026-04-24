"""
World Bank nominal GDP fetcher (NY.GDP.MKTP.CD) for computing EU GDP weights.

Disk-persisted 30-day cache — GDP-weights drift slowly and this is only used
for the EU aggregate. One "all-countries" call per refresh.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

import requests

from config import Config

logger = logging.getLogger(__name__)

WB_API = 'https://api.worldbank.org/v2'
INDICATOR = 'NY.GDP.MKTP.CD'
CACHE_TTL_DAYS = 30
CACHE_FILE = os.path.join(Config.DATA_DIR, 'wb_gdp_weights.json')

_lock = threading.RLock()
_mem_cache: Dict[str, float] = {}
_mem_fetched_at: Optional[datetime] = None


def _load_disk_cache():
    try:
        if not os.path.exists(CACHE_FILE):
            return None, None
        with open(CACHE_FILE, 'r') as f:
            data = json.load(f)
        fetched_at = datetime.fromisoformat(data['fetched_at'])
        if (datetime.utcnow() - fetched_at) > timedelta(days=CACHE_TTL_DAYS):
            return None, None
        return data['gdp'], fetched_at
    except Exception as e:
        logger.warning(f"wb_gdp_weights: disk cache load failed: {e}")
        return None, None


def _save_disk_cache(gdp: Dict[str, float]):
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        tmp = CACHE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({
                'fetched_at': datetime.utcnow().isoformat(),
                'gdp': gdp,
            }, f, separators=(',', ':'))
        os.replace(tmp, CACHE_FILE)
    except Exception as e:
        logger.warning(f"wb_gdp_weights: disk cache save failed: {e}")


def _fetch_all() -> Dict[str, float]:
    """Fetch latest nominal GDP (USD) for every country. Returns {iso2: float}."""
    url = f'{WB_API}/country/all/indicator/{INDICATOR}'
    params = {'format': 'json', 'mrv': 1, 'per_page': 500, 'date': '2015:2024'}
    try:
        resp = requests.get(url, params=params, timeout=45)
        if resp.status_code != 200:
            logger.warning(f"WB GDP {resp.status_code}")
            return {}
        body = resp.json()
        if not body or len(body) < 2 or not body[1]:
            return {}

        # We need ISO2; reuse the mapping the existing module loaded.
        from backend.data_sources.world_bank_wgi import _ISO3_TO_ISO2, _load_iso_mapping
        _load_iso_mapping()

        out: Dict[str, float] = {}
        for entry in body[1]:
            v = entry.get('value')
            if v is None:
                continue
            iso3 = entry.get('countryiso3code', '')
            iso2 = _ISO3_TO_ISO2.get(iso3)
            if not iso2:
                cid = entry.get('country', {}).get('id', '')
                iso2 = _ISO3_TO_ISO2.get(cid) or (cid if len(cid) == 2 else None)
            if iso2 and len(iso2) == 2:
                try:
                    out[iso2] = float(v)
                except (TypeError, ValueError):
                    continue
        return out
    except Exception as e:
        logger.error(f"wb_gdp_weights fetch failed: {e}")
        return {}


def get_gdp_usd(iso2: str) -> Optional[float]:
    """Latest nominal GDP (USD) for a country, or None."""
    global _mem_cache, _mem_fetched_at
    code = iso2.upper()

    with _lock:
        if _mem_cache and _mem_fetched_at and (datetime.utcnow() - _mem_fetched_at).days < CACHE_TTL_DAYS:
            return _mem_cache.get(code)

    disk_data, disk_time = _load_disk_cache()
    if disk_data is not None:
        with _lock:
            _mem_cache = disk_data
            _mem_fetched_at = disk_time
        return disk_data.get(code)

    fresh = _fetch_all()
    if fresh:
        with _lock:
            _mem_cache = fresh
            _mem_fetched_at = datetime.utcnow()
        _save_disk_cache(fresh)
        return fresh.get(code)
    return None


def get_gdp_weights(iso2_codes) -> Dict[str, float]:
    """
    Normalized GDP weights for a list of countries. Missing countries are
    dropped, and remaining weights are rescaled to sum to 1.0.
    """
    raw = {c: get_gdp_usd(c) for c in iso2_codes}
    raw = {c: v for c, v in raw.items() if v and v > 0}
    total = sum(raw.values())
    if total <= 0:
        return {}
    return {c: v / total for c, v in raw.items()}


def clear_cache():
    global _mem_cache, _mem_fetched_at
    with _lock:
        _mem_cache = {}
        _mem_fetched_at = None
