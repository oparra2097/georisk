"""
World Bank data source for base risk scores.

Two components:
A. Governance (WGI) — 6 Worldwide Governance Indicators
B. Macro Fundamentals — GDP growth, CPI, current account, debt, reserves

Fetched once on startup, cached to data/wgi_cache.json for 30 days.
"""

import os
import json
import logging
import threading
import requests
from datetime import datetime, timedelta
from config import Config

logger = logging.getLogger(__name__)

WB_API = 'https://api.worldbank.org/v2'

# --- Governance indicators (WGI) ---
# Scale: -2.5 (worst) to +2.5 (best)
WGI_INDICATORS = {
    'PV.EST': 'Political Stability',
    'RL.EST': 'Rule of Law',
    'CC.EST': 'Control of Corruption',
    'GE.EST': 'Government Effectiveness',
    'RQ.EST': 'Regulatory Quality',
    'VA.EST': 'Voice & Accountability',
}

# --- Macro indicators ---
MACRO_INDICATORS = {
    'NY.GDP.MKTP.KD.ZG': 'GDP Growth (%)',
    'FP.CPI.TOTL.ZG': 'CPI Inflation YoY (%)',
    'BN.CAB.XOKA.GD.ZS': 'Current Account (% GDP)',
    'GC.DOD.TOTL.GD.ZS': 'Govt Debt (% GDP)',
    'FI.RES.TOTL.MO': 'Reserves (months imports)',
    'NY.GDP.MKTP.PP.CD': 'GDP PPP (current intl $)',
}

# ISO alpha-2 → alpha-3 mapping for World Bank API
_ISO2_TO_ISO3 = {
    'US': 'USA', 'BR': 'BRA', 'MX': 'MEX', 'CO': 'COL', 'VE': 'VEN',
    'CU': 'CUB', 'CA': 'CAN', 'AR': 'ARG', 'CL': 'CHL', 'PE': 'PER',
    'GB': 'GBR', 'FR': 'FRA', 'DE': 'DEU', 'TR': 'TUR', 'UA': 'UKR',
    'RU': 'RUS', 'GE': 'GEO', 'BY': 'BLR', 'PL': 'POL', 'IT': 'ITA',
    'IL': 'ISR', 'PS': 'PSE', 'IR': 'IRN', 'IQ': 'IRQ', 'SY': 'SYR',
    'SA': 'SAU', 'YE': 'YEM', 'LY': 'LBY', 'EG': 'EGY', 'LB': 'LBN',
    'NG': 'NGA', 'CD': 'COD', 'SD': 'SDN', 'SS': 'SSD', 'SO': 'SOM',
    'ET': 'ETH', 'ML': 'MLI', 'BF': 'BFA', 'KE': 'KEN', 'ZA': 'ZAF',
    'CN': 'CHN', 'IN': 'IND', 'PK': 'PAK', 'KP': 'PRK', 'TW': 'TWN',
    'JP': 'JPN', 'KR': 'KOR', 'TH': 'THA', 'PH': 'PHL', 'MM': 'MMR',
}

_cache = {
    'base_scores': {},  # {country_alpha2: {base_score, governance_score, macro_score, ...}}
    'fetched_at': None,
}
_lock = threading.Lock()
_CACHE_FILE = os.path.join(Config.DATA_DIR, 'wgi_cache.json')


def _wgi_to_risk(value):
    """Convert WGI score (-2.5 to +2.5) to risk score (0-100). Inverted."""
    if value is None:
        return 50.0  # neutral default
    # -2.5 → 100, 0 → 50, +2.5 → 0
    risk = ((2.5 - value) / 5.0) * 100.0
    return max(0.0, min(100.0, risk))


def _gdp_growth_to_risk(value):
    """GDP growth → risk. Negative growth = high risk."""
    if value is None:
        return 40.0
    if value >= 5.0:
        return 5.0
    elif value >= 3.0:
        return 15.0
    elif value >= 1.0:
        return 30.0
    elif value >= 0.0:
        return 50.0
    elif value >= -2.0:
        return 65.0
    elif value >= -5.0:
        return 80.0
    else:
        return 95.0


def _inflation_to_risk(value):
    """CPI inflation → risk. Very high or deflation = risk."""
    if value is None:
        return 35.0
    if value < 0:
        return 55.0  # deflation is concerning
    elif value <= 2.0:
        return 10.0
    elif value <= 5.0:
        return 25.0
    elif value <= 10.0:
        return 45.0
    elif value <= 20.0:
        return 65.0
    elif value <= 50.0:
        return 80.0
    else:
        return 95.0  # hyperinflation


def _current_account_to_risk(value):
    """Current account (% GDP) → risk. Deep deficit = risk."""
    if value is None:
        return 40.0
    if value >= 5.0:
        return 10.0  # strong surplus
    elif value >= 0.0:
        return 20.0
    elif value >= -3.0:
        return 35.0
    elif value >= -6.0:
        return 55.0
    elif value >= -10.0:
        return 70.0
    else:
        return 85.0


def _debt_to_risk(value):
    """Govt debt (% GDP) → risk."""
    if value is None:
        return 40.0
    if value <= 30.0:
        return 10.0
    elif value <= 60.0:
        return 25.0
    elif value <= 80.0:
        return 40.0
    elif value <= 100.0:
        return 55.0
    elif value <= 150.0:
        return 70.0
    else:
        return 85.0


def _reserves_to_risk(value):
    """Reserves in months of imports → risk. <3 months = high risk."""
    if value is None:
        return 45.0
    if value >= 12.0:
        return 5.0
    elif value >= 6.0:
        return 20.0
    elif value >= 3.0:
        return 40.0
    elif value >= 1.5:
        return 65.0
    else:
        return 85.0


def _gdp_ppp_to_risk(value):
    """log(GDP PPP) → risk. Larger economies = lower risk (more resilient).
    Uses log scale: <$10B = very small, >$1T = major economy.
    """
    import math
    if value is None or value <= 0:
        return 60.0
    log_gdp = math.log10(value)
    # log10($1B) = 9, log10($10B) = 10, log10($100B) = 11, log10($1T) = 12, log10($25T) = 13.4
    if log_gdp >= 13.0:    # >$10T (US, China)
        return 5.0
    elif log_gdp >= 12.0:  # $1T-$10T (Japan, Germany, India)
        return 15.0
    elif log_gdp >= 11.5:  # $300B-$1T
        return 25.0
    elif log_gdp >= 11.0:  # $100B-$300B
        return 35.0
    elif log_gdp >= 10.5:  # $30B-$100B
        return 50.0
    elif log_gdp >= 10.0:  # $10B-$30B
        return 60.0
    else:                   # <$10B
        return 75.0


_MACRO_CONVERTERS = {
    'NY.GDP.MKTP.KD.ZG': _gdp_growth_to_risk,
    'FP.CPI.TOTL.ZG': _inflation_to_risk,
    'BN.CAB.XOKA.GD.ZS': _current_account_to_risk,
    'GC.DOD.TOTL.GD.ZS': _debt_to_risk,
    'FI.RES.TOTL.MO': _reserves_to_risk,
    'NY.GDP.MKTP.PP.CD': _gdp_ppp_to_risk,
}


def _fetch_wb_indicator(indicator_code, country_codes_iso2):
    """Fetch a single indicator for all countries from World Bank API."""
    # Convert to ISO3 and join with semicolons
    iso3_codes = []
    iso2_to_iso3_map = {}
    for c2 in country_codes_iso2:
        c3 = _ISO2_TO_ISO3.get(c2, c2)
        iso3_codes.append(c3.lower())
        iso2_to_iso3_map[c3] = c2

    codes_str = ';'.join(iso3_codes)
    url = f'{WB_API}/country/{codes_str}/indicator/{indicator_code}'
    params = {
        'format': 'json',
        'mrv': 1,        # most recent value
        'per_page': 200,
        'date': '2015:2024',
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"WB API {resp.status_code} for {indicator_code}")
            return {}

        data = resp.json()
        if not data or len(data) < 2 or not data[1]:
            return {}

        results = {}
        for entry in data[1]:
            if entry.get('value') is not None:
                iso3 = entry.get('countryiso3code', '')
                iso2 = iso2_to_iso3_map.get(iso3)
                if not iso2:
                    # Try reverse lookup from country id
                    cid = entry.get('country', {}).get('id', '')
                    iso2 = iso2_to_iso3_map.get(cid, cid)
                if iso2:
                    results[iso2] = entry['value']

        return results
    except Exception as e:
        logger.error(f"WB API fetch failed for {indicator_code}: {e}")
        return {}


def _load_cache():
    """Load cached base scores from disk."""
    try:
        if os.path.exists(_CACHE_FILE):
            with open(_CACHE_FILE, 'r') as f:
                data = json.load(f)
            fetched_str = data.get('fetched_at')
            if fetched_str:
                fetched_at = datetime.fromisoformat(fetched_str)
                age_days = (datetime.utcnow() - fetched_at).days
                if age_days < Config.WGI_CACHE_DAYS:
                    return data.get('base_scores', {}), fetched_at
                else:
                    logger.info(f"WGI cache is {age_days} days old, refreshing")
    except Exception as e:
        logger.warning(f"Failed to load WGI cache: {e}")
    return None, None


def _save_cache(base_scores):
    """Save base scores to disk."""
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        data = {
            'fetched_at': datetime.utcnow().isoformat(),
            'base_scores': base_scores,
        }
        tmp = _CACHE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f, separators=(',', ':'))
        os.replace(tmp, _CACHE_FILE)
        logger.info(f"Saved WGI cache for {len(base_scores)} countries")
    except Exception as e:
        logger.error(f"Failed to save WGI cache: {e}")


def fetch_base_scores(country_codes_iso2):
    """
    Fetch and compute base risk scores for all countries.
    Returns {country_alpha2: {'base_score': float, 'governance_score': float, 'macro_score': float, ...}}

    Uses disk cache (30 days) then memory cache.
    """
    with _lock:
        if _cache['base_scores'] and _cache['fetched_at']:
            age = (datetime.utcnow() - _cache['fetched_at']).days
            if age < Config.WGI_CACHE_DAYS:
                return _cache['base_scores']

    # Try disk cache
    disk_scores, disk_time = _load_cache()
    if disk_scores:
        with _lock:
            _cache['base_scores'] = disk_scores
            _cache['fetched_at'] = disk_time
        return disk_scores

    # Fresh fetch
    logger.info(f"Fetching World Bank data for {len(country_codes_iso2)} countries...")

    # Fetch governance indicators
    wgi_data = {}  # {country: {indicator: value}}
    for ind_code in WGI_INDICATORS:
        values = _fetch_wb_indicator(ind_code, country_codes_iso2)
        for country, value in values.items():
            if country not in wgi_data:
                wgi_data[country] = {}
            wgi_data[country][ind_code] = value

    # Fetch macro indicators
    macro_data = {}
    for ind_code in MACRO_INDICATORS:
        values = _fetch_wb_indicator(ind_code, country_codes_iso2)
        for country, value in values.items():
            if country not in macro_data:
                macro_data[country] = {}
            macro_data[country][ind_code] = value

    # Compute scores
    base_scores = {}
    for country in country_codes_iso2:
        # Governance score (average of 6 WGI indicators → 0-100)
        gov_values = wgi_data.get(country, {})
        gov_risks = [_wgi_to_risk(gov_values.get(ind)) for ind in WGI_INDICATORS]
        governance_score = sum(gov_risks) / len(gov_risks)

        # Macro score (average of 5 macro indicators → 0-100)
        mac_values = macro_data.get(country, {})
        macro_risks = []
        for ind_code, converter in _MACRO_CONVERTERS.items():
            value = mac_values.get(ind_code)
            macro_risks.append(converter(value))
        macro_score = sum(macro_risks) / len(macro_risks)

        # Combined base score
        base = governance_score * 0.6 + macro_score * 0.4

        base_scores[country] = {
            'base_score': round(base, 1),
            'governance_score': round(governance_score, 1),
            'macro_score': round(macro_score, 1),
            'wgi': {k: round(v, 3) if v else None for k, v in gov_values.items()},
            'macro': {k: round(v, 2) if v else None for k, v in mac_values.items()},
        }

    # Cache
    with _lock:
        _cache['base_scores'] = base_scores
        _cache['fetched_at'] = datetime.utcnow()

    _save_cache(base_scores)
    logger.info(f"Base scores computed for {len(base_scores)} countries")
    return base_scores


def get_base_score(country_alpha2):
    """Get base score for a single country. Returns dict or None."""
    with _lock:
        return _cache['base_scores'].get(country_alpha2)
