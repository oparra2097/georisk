"""
ACLED (Armed Conflict Location & Event Data) client.

Free registration at acleddata.com. Provides structured conflict event data:
- Event types: Battles, Protests, Riots, Violence, Explosions, Strategic
- Fatality counts per event
- Actor identification
- Sub-event classification

Auth: OAuth 2.0 password grant (since Sep 2025).
  POST https://acleddata.com/oauth/token
  → access_token (24h) + refresh_token (14d)
  → API calls use Authorization: Bearer {token}

Fetched daily for all priority countries. Results cached in memory for 24 hours.
Used to supplement GDELT's news-based scoring with ground-truth conflict data.

Graceful degradation: if ACLED_EMAIL/ACLED_PASSWORD not set, all functions return None.
"""

import requests
import logging
import threading
import time
from datetime import datetime, timedelta, date
from backend.data_sources.country_codes import iso_alpha2_to_numeric
from config import Config

logger = logging.getLogger(__name__)

ACLED_TOKEN_URL = 'https://acleddata.com/oauth/token'
ACLED_API_URL = 'https://api.acleddata.com/acled/read'

# Event type weights for scoring severity
EVENT_WEIGHTS = {
    'Battles': 1.0,
    'Violence against civilians': 0.9,
    'Explosions/Remote violence': 0.85,
    'Riots': 0.6,
    'Protests': 0.3,
    'Strategic developments': 0.4,
}

# Maps ACLED event types to GeoRisk indicators
EVENT_TO_INDICATOR = {
    'Battles': 'military_conflict',
    'Violence against civilians': 'terrorism',
    'Explosions/Remote violence': 'military_conflict',
    'Riots': 'protests_civil_unrest',
    'Protests': 'protests_civil_unrest',
    'Strategic developments': 'political_stability',
}

# Per-country data cache: {country_code: {'signal': dict, 'fetched_at': datetime}}
_cache = {}
_lock = threading.Lock()

# OAuth token cache (thread-safe)
_token_lock = threading.Lock()
_token_state = {
    'access_token': None,
    'refresh_token': None,
    'expires_at': 0,        # Unix timestamp
    'refresh_expires_at': 0  # Unix timestamp
}


def _is_configured():
    """Check if ACLED OAuth credentials are set."""
    return bool(Config.ACLED_EMAIL and Config.ACLED_PASSWORD)


def _request_token_password():
    """
    Get a new access token using password grant.
    Returns (access_token, refresh_token, expires_in) or (None, None, 0).
    """
    try:
        resp = requests.post(ACLED_TOKEN_URL, data={
            'username': Config.ACLED_EMAIL,
            'password': Config.ACLED_PASSWORD,
            'grant_type': 'password',
            'client_id': 'acled',
        }, headers={
            'Content-Type': 'application/x-www-form-urlencoded',
        }, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            access = data.get('access_token')
            refresh = data.get('refresh_token')
            expires_in = data.get('expires_in', 86400)
            if access:
                logger.info("ACLED OAuth: access token obtained via password grant")
                return access, refresh, expires_in
            else:
                logger.warning(f"ACLED OAuth: no access_token in response")
        elif resp.status_code == 401:
            logger.warning("ACLED OAuth: invalid credentials (401)")
        else:
            logger.warning(f"ACLED OAuth: token request failed HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"ACLED OAuth: token request error: {e}")

    return None, None, 0


def _request_token_refresh(refresh_token):
    """
    Refresh an expired access token using the refresh token.
    Returns (access_token, new_refresh_token, expires_in) or (None, None, 0).
    """
    try:
        resp = requests.post(ACLED_TOKEN_URL, data={
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': 'acled',
        }, headers={
            'Content-Type': 'application/x-www-form-urlencoded',
        }, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            access = data.get('access_token')
            new_refresh = data.get('refresh_token', refresh_token)
            expires_in = data.get('expires_in', 86400)
            if access:
                logger.debug("ACLED OAuth: access token refreshed")
                return access, new_refresh, expires_in
        else:
            logger.debug(f"ACLED OAuth: refresh failed HTTP {resp.status_code}, will re-auth")
    except Exception as e:
        logger.debug(f"ACLED OAuth: refresh error: {e}")

    return None, None, 0


def _get_access_token():
    """
    Get a valid ACLED access token. Thread-safe.
    - Returns cached token if still valid (with 5-min buffer)
    - Attempts refresh if access token expired but refresh token is valid
    - Falls back to full password grant if refresh fails
    - Returns None if unconfigured or auth fails
    """
    if not _is_configured():
        return None

    now = time.time()

    with _token_lock:
        # Check if current token is still valid (5-min buffer)
        if _token_state['access_token'] and now < (_token_state['expires_at'] - 300):
            return _token_state['access_token']

    # Token expired or missing — need to get a new one
    # Release lock during network calls to avoid blocking other threads

    access, refresh, expires_in = None, None, 0

    with _token_lock:
        # Double-check after acquiring lock (another thread may have refreshed)
        if _token_state['access_token'] and now < (_token_state['expires_at'] - 300):
            return _token_state['access_token']

        # Try refresh first if we have a valid refresh token
        if _token_state['refresh_token'] and now < _token_state['refresh_expires_at']:
            access, refresh, expires_in = _request_token_refresh(_token_state['refresh_token'])

    # If refresh didn't work, do full password grant (outside lock for network I/O)
    if not access:
        access, refresh, expires_in = _request_token_password()

    if access:
        with _token_lock:
            _token_state['access_token'] = access
            _token_state['refresh_token'] = refresh
            _token_state['expires_at'] = now + expires_in
            # Refresh tokens typically valid 14 days
            _token_state['refresh_expires_at'] = now + (14 * 86400)
        return access

    return None


def fetch_acled_country(country_alpha2, days_back=7):
    """
    Fetch recent ACLED events for a country.
    Returns list of event dicts or None if unconfigured/error.
    """
    if not _is_configured():
        return None

    token = _get_access_token()
    if not token:
        return None

    numeric_iso = iso_alpha2_to_numeric(country_alpha2)
    if not numeric_iso:
        return None

    since = (date.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    params = {
        'iso': int(numeric_iso),
        'event_date': since,
        'event_date_where': '>=',
        'limit': 500,
    }
    headers = {
        'Authorization': f'Bearer {token}',
    }

    try:
        resp = requests.get(ACLED_API_URL, params=params, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 200 or 'data' in data:
                return data.get('data', [])
            else:
                logger.debug(f"ACLED response status issue for {country_alpha2}: {data.get('status')}")
                return None
        elif resp.status_code == 401:
            # Token might have been invalidated server-side — clear cache
            with _token_lock:
                _token_state['access_token'] = None
                _token_state['expires_at'] = 0
            logger.warning(f"ACLED 401 for {country_alpha2} — token cleared, will retry next cycle")
            return None
        elif resp.status_code == 429:
            logger.warning(f"ACLED rate limited for {country_alpha2}")
            return None
        else:
            logger.debug(f"ACLED {resp.status_code} for {country_alpha2}")
            return None
    except requests.exceptions.Timeout:
        logger.debug(f"ACLED timeout for {country_alpha2}")
        return None
    except Exception as e:
        logger.warning(f"ACLED fetch failed for {country_alpha2}: {e}")
        return None


def get_acled_signal(country_code):
    """
    Get structured conflict signal from ACLED for a country.
    Returns dict of per-indicator signals or None if unavailable:
    {
        'military_conflict': {'event_count': int, 'fatalities': int, 'severity': float},
        'protests_civil_unrest': {...},
        'terrorism': {...},
        'political_stability': {...},
    }
    """
    if not _is_configured():
        return None

    # Check cache (24h TTL)
    with _lock:
        cached = _cache.get(country_code)
        if cached:
            age = (datetime.utcnow() - cached['fetched_at']).total_seconds()
            if age < 86400:  # 24 hours
                return cached['signal']

    events = fetch_acled_country(country_code)
    if events is None:
        return None

    # Initialize signal structure
    signal = {}
    for indicator in ['military_conflict', 'protests_civil_unrest', 'terrorism', 'political_stability']:
        signal[indicator] = {'event_count': 0, 'fatalities': 0, 'severity': 0.0}

    total_severity = {}
    for indicator in signal:
        total_severity[indicator] = 0.0

    for event in events:
        event_type = event.get('event_type', '')
        indicator = EVENT_TO_INDICATOR.get(event_type)
        if indicator and indicator in signal:
            signal[indicator]['event_count'] += 1
            fatalities = 0
            try:
                fatalities = int(event.get('fatalities', 0) or 0)
            except (ValueError, TypeError):
                pass
            signal[indicator]['fatalities'] += fatalities

            weight = EVENT_WEIGHTS.get(event_type, 0.5)
            # Severity: weighted by event importance and fatality count
            fatality_factor = min(1.0, fatalities / 50.0)  # 50 fatalities = max factor
            total_severity[indicator] += weight * (0.5 + 0.5 * fatality_factor)

    # Normalize severity to 0-1 range
    for indicator in signal:
        count = signal[indicator]['event_count']
        if count > 0:
            avg_severity = total_severity[indicator] / count
            # Scale by event count (more events = higher severity, diminishing returns)
            count_factor = min(1.0, count / 20.0)  # 20+ events = max count factor
            signal[indicator]['severity'] = min(1.0, avg_severity * (0.5 + 0.5 * count_factor))

    with _lock:
        _cache[country_code] = {'signal': signal, 'fetched_at': datetime.utcnow()}

    return signal


def prefetch_acled_data():
    """
    Pre-warm ACLED cache for all priority countries.
    Called by scheduler once daily.
    """
    if not _is_configured():
        logger.info("ACLED not configured (no credentials) — skipping prefetch.")
        return

    all_countries = []
    for region_countries in Config.REGIONS.values():
        all_countries.extend(region_countries)

    logger.info(f"ACLED prefetch starting for {len(all_countries)} priority countries...")
    success = 0
    for code in all_countries:
        result = get_acled_signal(code)
        if result is not None:
            success += 1

    logger.info(f"ACLED prefetch complete: {success}/{len(all_countries)} countries cached.")
