"""
IMF World Economic Outlook — data fetcher with 24-hour cache.
Fetches indicator data (e.g. NGDP_RPCH = real GDP growth) for all countries.
"""

import time
import threading
import requests

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 86400  # 24 hours

# IMF WEO API base
_WEO_API = 'https://www.imf.org/external/datamapper/api/v1'


def get_weo_data(indicator='NGDP_RPCH'):
    """Fetch WEO data for the given indicator. Returns cached if fresh."""
    cache_key = f'weo_{indicator}'

    with _cache_lock:
        if cache_key in _cache:
            entry = _cache[cache_key]
            if time.time() - entry['ts'] < _CACHE_TTL:
                return entry['data']

    data = _fetch_weo(indicator)

    with _cache_lock:
        _cache[cache_key] = {'data': data, 'ts': time.time()}

    return data


def _fetch_weo(indicator):
    """Fetch and parse WEO data from the IMF API."""
    try:
        # Fetch indicator data
        resp = requests.get(f'{_WEO_API}/{indicator}', timeout=30)
        resp.raise_for_status()
        raw = resp.json()

        # Fetch country names
        countries_resp = requests.get(f'{_WEO_API}/countries', timeout=30)
        countries_resp.raise_for_status()
        countries_raw = countries_resp.json()

        # Parse country name lookup
        country_names = {}
        countries_data = countries_raw.get('countries', {})
        for iso, info in countries_data.items():
            if isinstance(info, dict):
                country_names[iso] = info.get('label', iso)

        # Parse indicator data
        indicator_data = raw.get('values', {}).get(indicator, {})

        all_years = set()
        countries = {}

        for iso, year_values in indicator_data.items():
            if not isinstance(year_values, dict):
                continue
            name = country_names.get(iso, iso)
            values = {}
            for year_str, val in year_values.items():
                try:
                    year = int(year_str)
                    if val is not None and val != '':
                        values[year_str] = float(val)
                        all_years.add(year)
                except (ValueError, TypeError):
                    continue
            if values:
                countries[iso] = {
                    'name': name,
                    'values': values,
                }

        years = sorted(all_years)

        # Determine forecast start year (current year + 1 or based on data)
        current_year = time.localtime().tm_year
        forecast_start = current_year  # WEO forecasts typically start from current year

        return {
            'countries': countries,
            'years': years,
            'forecast_start_year': forecast_start,
            'meta': {
                'source': 'IMF World Economic Outlook',
                'indicator': indicator,
                'indicator_name': _INDICATOR_NAMES.get(indicator, indicator),
                'last_updated': time.strftime('%Y-%m-%d'),
                'country_count': len(countries),
            }
        }

    except Exception as e:
        print(f'[WEO] Error fetching {indicator}: {e}')
        return {
            'countries': {},
            'years': [],
            'forecast_start_year': None,
            'meta': {
                'source': 'IMF World Economic Outlook',
                'indicator': indicator,
                'error': str(e),
            }
        }


_INDICATOR_NAMES = {
    'NGDP_RPCH': 'Real GDP Growth (Annual % Change)',
    'NGDPD': 'GDP, Current Prices (Billions USD)',
    'PPPGDP': 'GDP, PPP (Billions International $)',
    'PPPPC': 'GDP Per Capita, PPP (International $)',
    'PCPIPCH': 'Inflation, Average Consumer Prices (Annual % Change)',
    'LUR': 'Unemployment Rate (%)',
    'BCA_NGDPD': 'Current Account Balance (% of GDP)',
    'BCA': 'Current Account Balance (Billions USD)',
    'GGXWDG_NGDP': 'General Government Gross Debt (% of GDP)',
    'LP': 'Population (Millions)',
    'TX_RPCH': 'Volume of Exports of Goods & Services (Annual % Change)',
    'TM_RPCH': 'Volume of Imports of Goods & Services (Annual % Change)',
    'TXG_RPCH': 'Volume of Exports of Goods (Annual % Change)',
    'TMG_RPCH': 'Volume of Imports of Goods (Annual % Change)',
}
