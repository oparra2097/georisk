"""
Central Bank Reserves data client.

Uses World Bank API (primary) - Total reserves by country, annual, free, fast, reliable.
  - FI.RES.TOTL.CD = Total reserves including gold (current US$)
  - FI.RES.XGLD.CD = Foreign exchange reserves excluding gold (current US$)
  - Gold reserves = Total - FX

Thread-safe cache with 24-hour TTL.
"""

import threading
import time
import logging
import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24 hours

# World Bank indicator codes
WB_INDICATORS = {
    'FI.RES.TOTL.CD': {'label': 'Total Reserves (incl. Gold)', 'color': '#3b82f6'},
    'FI.RES.XGLD.CD': {'label': 'Foreign Exchange Reserves', 'color': '#10b981'},
}

# Regions for filtering
WB_REGIONS = {
    'World': [],  # means "top 20"
    'G7': ['USA', 'JPN', 'DEU', 'GBR', 'FRA', 'ITA', 'CAN'],
    'BRICS': ['CHN', 'IND', 'BRA', 'RUS', 'ZAF'],
    'Asia': ['CHN', 'JPN', 'IND', 'KOR', 'IDN', 'THA', 'MYS', 'PHL', 'SGP'],
    'Europe': ['DEU', 'GBR', 'FRA', 'ITA', 'CHE', 'POL', 'NOR', 'SWE', 'CZE', 'ROU'],
    'Americas': ['USA', 'BRA', 'MEX', 'CAN', 'COL', 'CHL', 'PER', 'ARG'],
    'MENA': ['SAU', 'ARE', 'ISR', 'EGY', 'QAT', 'KWT', 'DZA', 'IRQ'],
    'Africa': ['ZAF', 'NGA', 'EGY', 'KEN', 'GHA', 'TZA', 'ETH', 'MAR'],
}

COUNTRY_NAMES = {
    'CHN': 'China', 'JPN': 'Japan', 'CHE': 'Switzerland', 'USA': 'United States',
    'IND': 'India', 'RUS': 'Russia', 'KOR': 'South Korea',
    'SAU': 'Saudi Arabia', 'HKG': 'Hong Kong', 'BRA': 'Brazil', 'SGP': 'Singapore',
    'DEU': 'Germany', 'THA': 'Thailand', 'FRA': 'France', 'GBR': 'United Kingdom',
    'MEX': 'Mexico', 'ITA': 'Italy', 'IDN': 'Indonesia', 'CZE': 'Czech Republic',
    'ISR': 'Israel', 'POL': 'Poland', 'CAN': 'Canada', 'MYS': 'Malaysia',
    'NOR': 'Norway', 'AUS': 'Australia', 'PHL': 'Philippines', 'COL': 'Colombia',
    'ARE': 'UAE', 'PER': 'Peru', 'CHL': 'Chile', 'EGY': 'Egypt',
    'QAT': 'Qatar', 'KWT': 'Kuwait', 'DZA': 'Algeria', 'IRQ': 'Iraq',
    'ZAF': 'South Africa', 'NGA': 'Nigeria', 'KEN': 'Kenya', 'GHA': 'Ghana',
    'TZA': 'Tanzania', 'ETH': 'Ethiopia', 'MAR': 'Morocco', 'SWE': 'Sweden',
    'ROU': 'Romania', 'ARG': 'Argentina',
}

COUNTRY_COLORS = [
    '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#ec4899',
    '#8b5cf6', '#f97316', '#06b6d4', '#84cc16', '#e11d48',
    '#6366f1', '#14b8a6', '#f43f5e', '#a855f7', '#22c55e',
    '#eab308', '#0ea5e9', '#d946ef', '#64748b', '#fb923c',
]


class ReservesCache:
    """Thread-safe cache for reserves data."""

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                return self._data
        data = _fetch_reserves()
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
            return data
        with self._lock:
            return self._data or _empty_result()

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0


_cache = ReservesCache()


def _empty_result():
    return {
        'years': [],
        'countries': [],
        'regions': list(WB_REGIONS.keys()),
        'region_members': WB_REGIONS,
        'meta': {'source': 'World Bank Open Data', 'error': 'No data available'}
    }


def _fetch_wb_indicator(indicator_code):
    """Fetch one indicator for all countries from World Bank API."""
    records_all = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        url = (
            f'https://api.worldbank.org/v2/country/all/indicator/{indicator_code}'
            f'?format=json&per_page=1000&page={page}&date=2000:2025&source=2'
        )
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            logger.error(f"World Bank API {resp.status_code} for {indicator_code} page {page}")
            break

        data = resp.json()
        if not isinstance(data, list) or len(data) < 2:
            break

        meta = data[0]
        records = data[1] or []
        total_pages = meta.get('pages', 1)
        records_all.extend(records)
        page += 1

    return records_all


def _fetch_reserves():
    """Fetch reserves data from World Bank API for all countries."""
    try:
        all_country_data = {}
        years_set = set()

        for indicator_code in WB_INDICATORS:
            records = _fetch_wb_indicator(indicator_code)

            for rec in records:
                iso3 = rec.get('countryiso3code', '')
                year = rec.get('date', '')
                value = rec.get('value')

                if not iso3 or not year or value is None:
                    continue

                # Skip World Bank aggregate regions (their IDs are > 3 chars)
                country_id = rec.get('country', {}).get('id', '')
                if len(country_id) > 3:
                    continue

                years_set.add(year)

                if iso3 not in all_country_data:
                    all_country_data[iso3] = {
                        'iso3': iso3,
                        'name': rec.get('country', {}).get('value', iso3),
                        'data': {}
                    }

                if year not in all_country_data[iso3]['data']:
                    all_country_data[iso3]['data'][year] = {}

                all_country_data[iso3]['data'][year][indicator_code] = value

        years = sorted(years_set)

        # Build country series
        countries = []
        for iso3, cdata in all_country_data.items():
            total_values = []
            fx_values = []
            gold_values = []

            for year in years:
                yr_data = cdata['data'].get(year, {})
                total = yr_data.get('FI.RES.TOTL.CD')
                fx = yr_data.get('FI.RES.XGLD.CD')

                total_b = round(total / 1e9, 2) if total else None
                fx_b = round(fx / 1e9, 2) if fx else None
                gold_b = round((total - fx) / 1e9, 2) if (total and fx) else None

                total_values.append(total_b)
                fx_values.append(fx_b)
                gold_values.append(gold_b)

            display_name = COUNTRY_NAMES.get(iso3, cdata['name'])

            countries.append({
                'iso3': iso3,
                'name': display_name,
                'total_reserves': total_values,
                'fx_reserves': fx_values,
                'gold_reserves': gold_values,
            })

        # Sort by latest total reserves descending
        def latest_val(c):
            for v in reversed(c['total_reserves']):
                if v is not None:
                    return v
            return 0
        countries.sort(key=latest_val, reverse=True)

        result = {
            'years': years,
            'countries': countries,
            'regions': list(WB_REGIONS.keys()),
            'region_members': WB_REGIONS,
            'meta': {
                'source': 'World Bank Open Data (International Financial Statistics)',
                'frequency': 'Annual',
                'country_count': len(countries),
                'year_range': f'{years[0]}-{years[-1]}' if years else '',
            }
        }

        logger.info(
            f"Reserves data loaded: {len(years)} years, {len(countries)} countries"
        )
        return result

    except requests.exceptions.Timeout:
        logger.error("World Bank API timeout")
        return None
    except Exception as e:
        logger.error(f"Reserves data fetch failed: {e}")
        return None


def get_cofer_data():
    """Public API: returns cached reserves data."""
    return _cache.get()
