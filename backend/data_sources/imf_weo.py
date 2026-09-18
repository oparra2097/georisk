"""
IMF World Economic Outlook — data fetcher with 24-hour cache.
Fetches indicator data (e.g. NGDP_RPCH = real GDP growth) for all countries.
"""

import time
import requests

from backend.data_sources._cache import cached

# IMF WEO API base
_WEO_API = 'https://www.imf.org/external/datamapper/api/v1'


@cached(namespace='imf_weo', ttl=86400, disk=True)
def get_weo_data(indicator='NGDP_RPCH'):
    """Fetch WEO data for the given indicator. Returns cached if fresh.

    The shared ``@cached`` decorator handles TTL, disk persistence
    (survives redeploys via ``Config.DATA_DIR/imf_weo_cache/``), per-key
    locking and stale-serve-on-error — so if a fresh fetch fails but
    we already had a prior successful payload, callers get the prior
    payload rather than an empty error dict.
    """
    return _fetch_weo(indicator)


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
        # Return an empty-error dict so existing callers can keep
        # doing ``data.get('countries')`` without exception handling.
        # The @cached decorator will still serve a PRIOR successful
        # payload if one exists on disk — this fallback only lands
        # when we have nothing cached at all.
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
