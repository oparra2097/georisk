"""
IMF COFER (Currency Composition of Official Foreign Exchange Reserves) client.

Fetches quarterly data from the IMF SDMX JSON API.
Free, no API key required. Data updated quarterly.
Thread-safe cache with 24-hour TTL.
"""

import threading
import time
import logging
import requests

logger = logging.getLogger(__name__)

IMF_BASE = 'https://dataservices.imf.org/REST/SDMX_JSON.svc'
COFER_KEY = 'CompactData/COFER/Q..'

# Indicator codes we care about (currency shares and totals)
# These are discovered from the COFER codelist
CURRENCY_INDICATORS = {
    'USD_Share': {'label': 'US Dollar', 'color': '#3b82f6'},
    'EUR_Share': {'label': 'Euro', 'color': '#10b981'},
    'GBP_Share': {'label': 'British Pound', 'color': '#f59e0b'},
    'JPY_Share': {'label': 'Japanese Yen', 'color': '#ef4444'},
    'CNY_Share': {'label': 'Chinese Renminbi', 'color': '#ec4899'},
    'AUD_Share': {'label': 'Australian Dollar', 'color': '#8b5cf6'},
    'CAD_Share': {'label': 'Canadian Dollar', 'color': '#f97316'},
    'CHF_Share': {'label': 'Swiss Franc', 'color': '#6b7280'},
    'Other_Share': {'label': 'Other', 'color': '#374151'},
}

CACHE_TTL = 86400  # 24 hours


class COFERCache:
    """Thread-safe cache for COFER data."""

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                return self._data
        data = _fetch_cofer()
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
            return data
        # Return cached even if stale, or empty
        with self._lock:
            return self._data or {'quarters': [], 'currencies': [], 'meta': {}}


_cache = COFERCache()


def _fetch_cofer():
    """Fetch COFER data from IMF SDMX API and parse into clean format."""
    try:
        url = f'{IMF_BASE}/{COFER_KEY}'
        params = {'startPeriod': '1999'}
        resp = requests.get(url, params=params, timeout=60)

        if resp.status_code != 200:
            logger.error(f"IMF COFER API returned {resp.status_code}")
            return None

        data = resp.json()
        dataset = data.get('CompactData', {}).get('DataSet', {})
        series_list = dataset.get('Series', [])

        if not series_list:
            logger.warning("IMF COFER: no series returned")
            return None

        # Parse series into a dict keyed by indicator + ref_area
        # We want world-level aggregates (REF_AREA = 'W00' or '1C_901')
        parsed = {}
        for s in series_list:
            indicator = s.get('@INDICATOR', '')
            ref_area = s.get('@REF_AREA', '')

            obs = s.get('Obs', [])
            if isinstance(obs, dict):
                obs = [obs]

            for ob in obs:
                period = ob.get('@TIME_PERIOD', '')
                value = ob.get('@OBS_VALUE', '')
                if period and value:
                    try:
                        val = float(value)
                    except (ValueError, TypeError):
                        continue

                    key = f"{ref_area}:{indicator}"
                    if key not in parsed:
                        parsed[key] = {}
                    parsed[key][period] = val

        # Build time series: find share indicators for world aggregate
        # Try different area codes the IMF uses for world totals
        world_areas = ['W00', '1C_901', '901', 'W0']

        share_series = {}
        amount_series = {}

        for area_code in world_areas:
            for key, ts in parsed.items():
                if not key.startswith(f'{area_code}:'):
                    continue
                indicator = key.split(':')[1]

                # Detect share vs amount indicators
                ind_lower = indicator.lower()
                if 'share' in ind_lower or 'pct' in ind_lower or 'shr' in ind_lower:
                    share_series[indicator] = ts
                elif 'usd' in ind_lower or 'amt' in ind_lower or 'val' in ind_lower:
                    amount_series[indicator] = ts

            if share_series or amount_series:
                break

        # If we couldn't find specific share/amount categories,
        # just return all series for the first world area found
        if not share_series and not amount_series:
            for area_code in world_areas:
                area_series = {k: v for k, v in parsed.items() if k.startswith(f'{area_code}:')}
                if area_series:
                    for key, ts in area_series.items():
                        indicator = key.split(':')[1]
                        share_series[indicator] = ts
                    break

        # If still nothing, take all series
        if not share_series:
            share_series = {k.split(':')[-1]: v for k, v in list(parsed.items())[:20]}

        # Collect all quarters across all series
        all_quarters = set()
        for ts in share_series.values():
            all_quarters.update(ts.keys())
        for ts in amount_series.values():
            all_quarters.update(ts.keys())

        quarters = sorted(all_quarters)

        # Build output
        currencies = []
        for ind_code, ts in share_series.items():
            meta = CURRENCY_INDICATORS.get(ind_code, {
                'label': ind_code.replace('_', ' '),
                'color': '#6b7280'
            })
            values = [ts.get(q) for q in quarters]
            currencies.append({
                'code': ind_code,
                'label': meta['label'],
                'color': meta['color'],
                'type': 'share',
                'values': values,
            })

        for ind_code, ts in amount_series.items():
            values = [ts.get(q) for q in quarters]
            currencies.append({
                'code': ind_code,
                'label': ind_code.replace('_', ' '),
                'color': '#6b7280',
                'type': 'amount',
                'values': values,
            })

        result = {
            'quarters': quarters,
            'currencies': currencies,
            'meta': {
                'source': 'IMF COFER (Currency Composition of Official Foreign Exchange Reserves)',
                'frequency': 'Quarterly',
                'series_count': len(series_list),
                'indicators_found': len(share_series) + len(amount_series),
            }
        }

        logger.info(
            f"IMF COFER loaded: {len(quarters)} quarters, "
            f"{len(currencies)} indicators, {len(series_list)} raw series"
        )
        return result

    except requests.exceptions.Timeout:
        logger.error("IMF COFER API timeout")
        return None
    except Exception as e:
        logger.error(f"IMF COFER fetch failed: {e}")
        return None


def get_cofer_data():
    """Public API: returns cached COFER data."""
    return _cache.get()
