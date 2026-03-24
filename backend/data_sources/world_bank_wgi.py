"""
World Bank data source for base risk scores.

Two components:
A. Governance (WGI) — 6 Worldwide Governance Indicators
B. Macro Fundamentals — GDP growth, CPI, current account, debt, reserves, GDP PPP

Fetched once on startup for ALL countries, cached to data/wgi_cache.json for 30 days.
Uses World Bank "all" endpoint so one API call covers every country.
"""

import os
import json
import logging
import math
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

# Dynamically loaded from country_codes.json
_ISO2_TO_ISO3 = {}
_ISO3_TO_ISO2 = {}
_iso_loaded = False


def _load_iso_mapping():
    """Load ISO alpha-2 → alpha-3 mapping from the project's country_codes.json."""
    global _ISO2_TO_ISO3, _ISO3_TO_ISO2, _iso_loaded
    if _iso_loaded:
        return
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        codes_path = os.path.join(base_dir, 'static', 'data', 'country_codes.json')
        with open(codes_path, 'r') as f:
            countries = json.load(f)
        for c in countries:
            a2 = c.get('alpha-2', '')
            a3 = c.get('alpha-3', '')
            if a2 and a3:
                _ISO2_TO_ISO3[a2] = a3
                _ISO3_TO_ISO2[a3] = a2
        _iso_loaded = True
        logger.info(f"Loaded ISO mapping for {len(_ISO2_TO_ISO3)} countries")
    except Exception as e:
        logger.error(f"Failed to load ISO mapping: {e}")


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


def _fetch_wb_indicator_all(indicator_code):
    """
    Fetch a single indicator for ALL countries from World Bank API.
    Uses the 'all' endpoint — one call per indicator, returns every country.
    Returns {iso2: value}.
    """
    _load_iso_mapping()

    url = f'{WB_API}/country/all/indicator/{indicator_code}'
    params = {
        'format': 'json',
        'mrv': 1,         # most recent value
        'per_page': 500,  # enough for all countries
        'date': '2015:2024',
    }

    try:
        resp = requests.get(url, params=params, timeout=45)
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
                iso2 = _ISO3_TO_ISO2.get(iso3)
                if not iso2:
                    # Try country id field
                    cid = entry.get('country', {}).get('id', '')
                    iso2 = _ISO3_TO_ISO2.get(cid)
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


def fetch_base_scores(country_codes_iso2=None):
    """
    Fetch and compute base risk scores for all countries.
    Returns {country_alpha2: {'base_score': float, 'governance_score': float, 'macro_score': float, ...}}

    Uses disk cache (30 days) then memory cache.
    If country_codes_iso2 is None, fetches for ALL countries.
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

    # Fresh fetch — use "all" endpoint (one call per indicator for every country)
    _load_iso_mapping()
    logger.info("Fetching World Bank data for ALL countries (12 API calls)...")

    # Fetch governance indicators for all countries
    wgi_data = {}  # {iso2: {indicator: value}}
    for ind_code in WGI_INDICATORS:
        values = _fetch_wb_indicator_all(ind_code)
        for country, value in values.items():
            if country not in wgi_data:
                wgi_data[country] = {}
            wgi_data[country][ind_code] = value
        logger.debug(f"  WGI {ind_code}: {len(values)} countries")

    # Fetch macro indicators for all countries
    macro_data = {}
    for ind_code in MACRO_INDICATORS:
        values = _fetch_wb_indicator_all(ind_code)
        for country, value in values.items():
            if country not in macro_data:
                macro_data[country] = {}
            macro_data[country][ind_code] = value
        logger.debug(f"  Macro {ind_code}: {len(values)} countries")

    # Determine which countries to compute scores for
    if country_codes_iso2 is None:
        # All countries that have any data
        all_countries = set(list(wgi_data.keys()) + list(macro_data.keys()))
    else:
        all_countries = set(country_codes_iso2)
        # Also include any country with data
        all_countries.update(wgi_data.keys())
        all_countries.update(macro_data.keys())

    # Compute scores
    base_scores = {}
    for country in all_countries:
        # Skip non-standard codes (WB sometimes returns aggregate codes like XK, 1W, etc.)
        if len(country) != 2:
            continue

        # Governance score (average of 6 WGI indicators → 0-100)
        gov_values = wgi_data.get(country, {})
        gov_risks = [_wgi_to_risk(gov_values.get(ind)) for ind in WGI_INDICATORS]
        governance_score = sum(gov_risks) / len(gov_risks)

        # Macro score (average of 6 macro indicators → 0-100)
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
