"""
Central Bank Reserves data client.

Primary: DBnomics IMF/IRFCL (monthly, ~94 countries, free, no auth)
  - RAFA_USD  = Total official reserve assets (USD millions)
  - RAFAFX_USD = Foreign currency reserves (USD millions)
  - Gold = Total - FX

Fallback: World Bank API (annual, ~180 countries)
  - FI.RES.TOTL.CD = Total reserves including gold (current US$)
  - FI.RES.XGLD.CD = Foreign exchange reserves excluding gold (current US$)

Thread-safe cache with 24-hour TTL.
"""

import json
import threading
import time
import logging
import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24 hours

# ── DBnomics / IMF IRFCL (primary — monthly) ──────────────────────────────

DBNOMICS_BASE = 'https://api.db.nomics.world/v22'

IRFCL_INDICATORS = {
    'RAFA_USD': 'total',      # Total official reserve assets
    'RAFAFX_USD': 'fx',       # Foreign currency reserves
}

# ISO 2-letter (IRFCL) → ISO 3-letter (our output)
ISO2_TO_ISO3 = {
    'CN': 'CHN', 'JP': 'JPN', 'CH': 'CHE', 'US': 'USA', 'IN': 'IND',
    'RU': 'RUS', 'KR': 'KOR', 'SA': 'SAU', 'HK': 'HKG', 'BR': 'BRA',
    'SG': 'SGP', 'DE': 'DEU', 'TH': 'THA', 'FR': 'FRA', 'GB': 'GBR',
    'MX': 'MEX', 'IT': 'ITA', 'ID': 'IDN', 'CZ': 'CZE', 'PL': 'POL',
    'IL': 'ISR', 'CA': 'CAN', 'AU': 'AUS', 'NO': 'NOR', 'SE': 'SWE',
    'MY': 'MYS', 'TR': 'TUR', 'AE': 'ARE', 'EG': 'EGY', 'ZA': 'ZAF',
    'NG': 'NGA', 'AR': 'ARG', 'CL': 'CHL', 'CO': 'COL', 'PE': 'PER',
    'PH': 'PHL', 'RO': 'ROU', 'HU': 'HUN', 'DK': 'DNK', 'NZ': 'NZL',
    'QA': 'QAT', 'KW': 'KWT', 'DZ': 'DZA', 'IQ': 'IRQ', 'KE': 'KEN',
    'GH': 'GHA', 'TZ': 'TZA', 'ET': 'ETH', 'MA': 'MAR', 'BG': 'BGR',
    'HR': 'HRV', 'LT': 'LTU', 'LV': 'LVA', 'SK': 'SVK', 'SI': 'SVN',
    'EE': 'EST', 'CY': 'CYP', 'LU': 'LUX', 'MT': 'MLT', 'IS': 'ISL',
    'FI': 'FIN', 'IE': 'IRL', 'PT': 'PRT', 'ES': 'ESP', 'AT': 'AUT',
    'BE': 'BEL', 'NL': 'NLD', 'GR': 'GRC', 'UY': 'URY', 'GT': 'GTM',
    'CR': 'CRI', 'PA': 'PAN', 'DO': 'DOM', 'LK': 'LKA', 'PK': 'PAK',
    'BD': 'BGD', 'KZ': 'KAZ', 'UA': 'UKR', 'GE': 'GEO', 'JO': 'JOR',
    'BH': 'BHR', 'OM': 'OMN', 'LB': 'LBN', 'TN': 'TUN', 'MU': 'MUS',
    'BW': 'BWA', 'MZ': 'MOZ', 'UG': 'UGA', 'SN': 'SEN',
}

# ── World Bank (fallback — annual) ─────────────────────────────────────────

WB_INDICATORS = {
    'FI.RES.TOTL.CD': {'label': 'Total Reserves (incl. Gold)', 'color': '#3b82f6'},
    'FI.RES.XGLD.CD': {'label': 'Foreign Exchange Reserves', 'color': '#10b981'},
}

# ── Shared constants ───────────────────────────────────────────────────────

RESERVES_REGIONS = {
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
    'ROU': 'Romania', 'ARG': 'Argentina', 'TUR': 'Turkey', 'DNK': 'Denmark',
    'HUN': 'Hungary', 'NZL': 'New Zealand', 'BGR': 'Bulgaria', 'HRV': 'Croatia',
    'LTU': 'Lithuania', 'LVA': 'Latvia', 'SVK': 'Slovakia', 'SVN': 'Slovenia',
    'EST': 'Estonia', 'ISL': 'Iceland', 'FIN': 'Finland', 'IRL': 'Ireland',
    'PRT': 'Portugal', 'ESP': 'Spain', 'AUT': 'Austria', 'BEL': 'Belgium',
    'NLD': 'Netherlands', 'GRC': 'Greece', 'UKR': 'Ukraine', 'KAZ': 'Kazakhstan',
    'PAK': 'Pakistan', 'BGD': 'Bangladesh', 'LKA': 'Sri Lanka', 'GEO': 'Georgia',
    'JOR': 'Jordan', 'BHR': 'Bahrain', 'OMN': 'Oman', 'LBN': 'Lebanon',
    'TUN': 'Tunisia', 'URY': 'Uruguay', 'GTM': 'Guatemala', 'CRI': 'Costa Rica',
    'PAN': 'Panama', 'DOM': 'Dominican Republic',
}

COUNTRY_COLORS = [
    '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#ec4899',
    '#8b5cf6', '#f97316', '#06b6d4', '#84cc16', '#e11d48',
    '#6366f1', '#14b8a6', '#f43f5e', '#a855f7', '#22c55e',
    '#eab308', '#0ea5e9', '#d946ef', '#64748b', '#fb923c',
]


# ── Cache ──────────────────────────────────────────────────────────────────

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
        'regions': list(RESERVES_REGIONS.keys()),
        'region_members': RESERVES_REGIONS,
        'meta': {'source': 'No data available', 'error': 'Fetch failed'}
    }


# ══════════════════════════════════════════════════════════════════════════
# PRIMARY: DBnomics IMF/IRFCL (monthly)
# ══════════════════════════════════════════════════════════════════════════

def _fetch_dbnomics_indicator(indicator_code):
    """Fetch one IRFCL indicator for all countries from DBnomics."""
    url = f'{DBNOMICS_BASE}/series/IMF/IRFCL'
    params = {
        'dimensions': json.dumps({
            'FREQ': ['M'],
            'INDICATOR': [indicator_code],
            'REF_SECTOR': ['S1X'],
        }),
        'observations': '1',
        'limit': '200',
        'metadata': 'false',
    }
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return data.get('series', {}).get('docs', [])


def _fetch_reserves_dbnomics():
    """Fetch monthly reserves from DBnomics IMF/IRFCL for all countries."""
    try:
        logger.info("Fetching reserves from DBnomics IMF/IRFCL (monthly)...")

        # Fetch both indicators (2 requests for all countries)
        total_docs = _fetch_dbnomics_indicator('RAFA_USD')
        fx_docs = _fetch_dbnomics_indicator('RAFAFX_USD')

        if not total_docs:
            logger.warning("DBnomics returned no total reserves data")
            return None

        # Parse total reserves: {iso2: {period: value_millions}}
        total_by_country = {}
        for doc in total_docs:
            iso2 = doc.get('dimensions', {}).get('REF_AREA', '')
            periods = doc.get('period', [])
            values = doc.get('value', [])
            if iso2 and periods:
                total_by_country[iso2] = dict(zip(periods, values))

        # Parse FX reserves
        fx_by_country = {}
        for doc in fx_docs:
            iso2 = doc.get('dimensions', {}).get('REF_AREA', '')
            periods = doc.get('period', [])
            values = doc.get('value', [])
            if iso2 and periods:
                fx_by_country[iso2] = dict(zip(periods, values))

        # Build unified period list (from all countries, both indicators)
        all_periods = set()
        for country_data in list(total_by_country.values()) + list(fx_by_country.values()):
            all_periods.update(country_data.keys())

        # Filter to periods >= 2000-01 and sort
        periods = sorted(p for p in all_periods if p >= '2000-01')

        if not periods:
            logger.warning("No periods found in IRFCL data")
            return None

        # Build country series
        countries = []
        for iso2 in total_by_country:
            iso3 = ISO2_TO_ISO3.get(iso2)
            if not iso3:
                continue  # Skip countries not in our mapping

            total_data = total_by_country.get(iso2, {})
            fx_data = fx_by_country.get(iso2, {})

            total_values = []
            fx_values = []
            gold_values = []

            for period in periods:
                raw_total = total_data.get(period)
                raw_fx = fx_data.get(period)

                # Convert from millions to billions, handle non-numeric
                total_b = None
                fx_b = None
                gold_b = None

                if raw_total is not None:
                    try:
                        total_b = round(float(raw_total) / 1000, 2)
                    except (ValueError, TypeError):
                        total_b = None

                if raw_fx is not None:
                    try:
                        fx_b = round(float(raw_fx) / 1000, 2)
                    except (ValueError, TypeError):
                        fx_b = None

                if total_b is not None and fx_b is not None:
                    gold_b = round(total_b - fx_b, 2)

                total_values.append(total_b)
                fx_values.append(fx_b)
                gold_values.append(gold_b)

            display_name = COUNTRY_NAMES.get(iso3, iso3)

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
            'years': periods,
            'countries': countries,
            'regions': list(RESERVES_REGIONS.keys()),
            'region_members': RESERVES_REGIONS,
            'meta': {
                'source': 'IMF IRFCL (via DBnomics)',
                'frequency': 'Monthly',
                'country_count': len(countries),
                'period_range': f'{periods[0]} to {periods[-1]}' if periods else '',
            }
        }

        logger.info(
            f"IRFCL reserves loaded: {len(periods)} months, {len(countries)} countries"
        )
        return result

    except requests.exceptions.Timeout:
        logger.error("DBnomics API timeout")
        return None
    except Exception as e:
        logger.error(f"DBnomics IRFCL fetch failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════
# FALLBACK: World Bank API (annual)
# ══════════════════════════════════════════════════════════════════════════

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


def _fetch_reserves_wb():
    """Fetch reserves data from World Bank API (annual fallback)."""
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

        def latest_val(c):
            for v in reversed(c['total_reserves']):
                if v is not None:
                    return v
            return 0
        countries.sort(key=latest_val, reverse=True)

        result = {
            'years': years,
            'countries': countries,
            'regions': list(RESERVES_REGIONS.keys()),
            'region_members': RESERVES_REGIONS,
            'meta': {
                'source': 'World Bank Open Data (Annual Fallback)',
                'frequency': 'Annual',
                'country_count': len(countries),
                'year_range': f'{years[0]}-{years[-1]}' if years else '',
            }
        }

        logger.info(
            f"WB reserves loaded: {len(years)} years, {len(countries)} countries"
        )
        return result

    except requests.exceptions.Timeout:
        logger.error("World Bank API timeout")
        return None
    except Exception as e:
        logger.error(f"World Bank reserves fetch failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR: try DBnomics first, fall back to World Bank
# ══════════════════════════════════════════════════════════════════════════

def _fetch_reserves():
    """Fetch reserves: DBnomics IRFCL monthly first, World Bank annual fallback."""
    result = _fetch_reserves_dbnomics()
    if result:
        return result
    logger.warning("DBnomics IRFCL failed — falling back to World Bank annual data")
    return _fetch_reserves_wb()


def get_cofer_data():
    """Public API: returns cached reserves data."""
    return _cache.get()
