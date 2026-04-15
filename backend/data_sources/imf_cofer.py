"""
Central Bank Reserves data client.

Primary: IMF Data API (api.imf.org/external/sdmx/3.0, live monthly)
  - Replaced the retired dataservices.imf.org on 2025-11-05.
  - RAFA_USD   = Total official reserve assets (USD millions)
  - RAFAFX_USD = Foreign currency reserves (USD millions)
  - Gold = Total - FX

Fallback 1: DBnomics IMF/IRFCL mirror (monthly, may be stale)
  - DBnomics' mirror was frozen on 2025-08-31 and has not yet been
    rewritten for the new IMF API. Kept as a secondary in case
    api.imf.org is temporarily unreachable.

Fallback 2: World Bank API (annual, ~180 countries)
  - FI.RES.TOTL.CD = Total reserves including gold (current US$)
  - FI.RES.XGLD.CD = Foreign exchange reserves excluding gold (current US$)

Thread-safe cache with 24-hour TTL. Use refresh_cache() to force a
re-fetch (wired up to /api/cofer/refresh in backend.routes).
"""

import json
import threading
import time
import logging
import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 86400  # 24 hours

# ── IMF Data API (primary — live monthly) ────────────────────────────────
#
# DBnomics' IRFCL mirror was frozen on 2025-08-31 because the legacy IMF API
# (dataservices.imf.org) was retired on 2025-11-05 and DBnomics has not yet
# rewritten its IMF fetcher for the new SDMX Central API. To keep the data
# page current we query the new IMF Data API (api.imf.org) directly and
# only fall back to the DBnomics snapshot / World Bank annual data on
# failure.
#
# NB: sdmxcentral.imf.org is the IMF Fusion Metadata Registry and only
# serves *structures*, not data. Actual observations live on
# api.imf.org/external/sdmx/{2.1,3.0}. The 3.0 endpoint is the current
# recommended one and natively returns SDMX-JSON 2.0.0.
#
# See:
#   - https://git.nomics.world/dbnomics-fetchers/imf-fetcher/-/issues/4
#   - https://github.com/Teal-Insights/r-imfapi (reference implementation)
IMF_DATA_BASE_V3 = 'https://api.imf.org/external/sdmx/3.0'
IMF_DATA_BASE_V21 = 'https://api.imf.org/external/sdmx/2.1'
IMF_DATA_HEADERS_V3 = {
    'Accept': 'application/json, application/vnd.sdmx.data+json;version=2.0.0',
    'User-Agent': 'georisk/1.0 (reserves refresh)',
}
IMF_DATA_HEADERS_V21 = {
    'Accept': 'application/vnd.sdmx.data+json;version=1.0.0',
    'User-Agent': 'georisk/1.0 (reserves refresh)',
}

# ── DBnomics / IMF IRFCL (fallback — monthly mirror) ──────────────────────

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
# Shared result builder (used by all monthly sources)
# ══════════════════════════════════════════════════════════════════════════

def _usd_millions_to_billions(value):
    """Convert a raw USD-millions observation to USD-billions (or None)."""
    if value is None:
        return None
    try:
        return round(float(value) / 1000, 2)
    except (ValueError, TypeError):
        return None


def _build_reserves_result(total_by_country, fx_by_country, source_label, frequency='Monthly'):
    """Assemble the unified reserves payload from parsed country dicts.

    Inputs are mappings of ``{iso2: {period: value_in_usd_millions}}``.
    Returns the standard result dict consumed by /api/cofer, or ``None`` if
    there is no usable data.
    """
    if not total_by_country:
        return None

    all_periods = set()
    for cd in list(total_by_country.values()) + list(fx_by_country.values()):
        all_periods.update(cd.keys())

    periods = sorted(p for p in all_periods if p >= '2000-01')
    if not periods:
        return None

    countries = []
    for code in total_by_country:
        # Accept both ISO2 (DBnomics / legacy) and ISO3 (new api.imf.org)
        if len(code) == 2:
            iso3 = ISO2_TO_ISO3.get(code)
        elif len(code) == 3 and code in COUNTRY_NAMES:
            iso3 = code
        else:
            iso3 = None
        if not iso3:
            continue  # Skip countries not in our display mapping

        total_data = total_by_country.get(code, {})
        fx_data = fx_by_country.get(code, {})

        total_values = []
        fx_values = []
        gold_values = []

        for period in periods:
            total_b = _usd_millions_to_billions(total_data.get(period))
            fx_b = _usd_millions_to_billions(fx_data.get(period))
            gold_b = round(total_b - fx_b, 2) if (total_b is not None and fx_b is not None) else None

            total_values.append(total_b)
            fx_values.append(fx_b)
            gold_values.append(gold_b)

        # Skip countries with no usable total reserves at all
        if not any(v is not None for v in total_values):
            continue

        display_name = COUNTRY_NAMES.get(iso3, iso3)
        countries.append({
            'iso3': iso3,
            'name': display_name,
            'total_reserves': total_values,
            'fx_reserves': fx_values,
            'gold_reserves': gold_values,
        })

    if not countries:
        return None

    def latest_val(c):
        for v in reversed(c['total_reserves']):
            if v is not None:
                return v
        return 0
    countries.sort(key=latest_val, reverse=True)

    return {
        'years': periods,
        'countries': countries,
        'regions': list(RESERVES_REGIONS.keys()),
        'region_members': RESERVES_REGIONS,
        'meta': {
            'source': source_label,
            'frequency': frequency,
            'country_count': len(countries),
            'period_range': f'{periods[0]} to {periods[-1]}',
        }
    }


# ══════════════════════════════════════════════════════════════════════════
# PRIMARY: IMF Data API (live monthly data)
# ══════════════════════════════════════════════════════════════════════════

# Mapping of our internal labels to candidate IMF indicator codes.
# The legacy DBnomics labels ``RAFA_USD`` / ``RAFAFX_USD`` don't exist on
# api.imf.org — the new DSD uses codes shaped like ``IRFCLDT1_IRFCL{N}_{CCY}``
# where N is a row number in the IRFCL template. We try the first-alphabetical
# candidates (which the catalog probe revealed) in order. The catalog probe
# logs all 257 codes, so if none of these are right we can update this list
# from the next diagnostic round.
IMF_IRFCL_INDICATOR_CANDIDATES = {
    'RAFA_USD': (
        # First-alphabetical numeric codes from the catalog probe; one of
        # these is very likely to be the total-reserves headline indicator.
        'IRFCLDT1_IRFCL121_USD',
        'IRFCLDT1_IRFCL31_BOFIORC_USD',
        'IRFCLDT1_IRFCL31_BOFIORCLA_USD',
        'IRFCLDT1_IRFCL32_USD',
        'IRFCLDT1_IRFCL34_USD',
        'IRFCLDT1_IRFCL37_USD',
        'IRFCLDT1_IRFCL40_USD',
        # legacy codes in case the old naming ever comes back
        'RAFA_USD',
        'RAFA',
    ),
    'RAFAFX_USD': (
        # Currency-and-deposits / foreign currency reserves candidates
        'IRFCLDT1_IRFCLCDCFC_USD',
        'IRFCLDT1_IRFCLCDCFCU_USD',
        'IRFCLDT1_IRFCL31_BOFIRC_USD',
        'IRFCLDT1_IRFCL31_BOFIRCLA_USD',
        'RAFAFX_USD',
        'RAFAFX',
    ),
}

# Sector code discovered via the catalog probe. The legacy REF_SECTOR
# was ``S1X`` (Monetary authorities); the new DSD_IRFCL_PUB combines
# S1X with S1311 (Central government sub-sector) into ``S1XS1311``.
IMF_IRFCL_DEFAULT_SECTOR = 'S1XS1311'


def _imf_data_attempt_urls(indicator_code):
    """Yield (label, url, params, headers) tuples to try in order.

    The new IRFCL DSD on api.imf.org (``DSD_IRFCL_PUB(12.0.0)``) uses
    dimension IDs ``COUNTRY.INDICATOR.SECTOR.FREQUENCY`` in that order.
    ``COUNTRY`` is ISO3 (USA, not US), ``SECTOR`` is ``S1XS1311`` (not
    ``S1X``), and indicator codes are row-number based like
    ``IRFCLDT1_IRFCL121_USD``. We walk the candidate list declared in
    :data:`IMF_IRFCL_INDICATOR_CANDIDATES` for each internal label.
    """
    v3_params = {
        'dimensionAtObservation': 'TIME_PERIOD',
        'attributes': 'dsd',
        'measures': 'all',
    }

    candidates = IMF_IRFCL_INDICATOR_CANDIDATES.get(
        indicator_code, (indicator_code,)
    )
    for ind in candidates:
        # Primary: known sector
        yield (
            f'v3 IMF.STA *.{ind}.{IMF_IRFCL_DEFAULT_SECTOR}.M',
            f'{IMF_DATA_BASE_V3}/data/dataflow/IMF.STA/IRFCL/+/*.{ind}.{IMF_IRFCL_DEFAULT_SECTOR}.M',
            v3_params,
            IMF_DATA_HEADERS_V3,
        )
    # Looser fallback: wildcard sector on the first candidate, in case
    # S1XS1311 isn't quite right for every indicator.
    first_candidate = candidates[0]
    yield (
        f'v3 IMF.STA *.{first_candidate}.*.M',
        f'{IMF_DATA_BASE_V3}/data/dataflow/IMF.STA/IRFCL/+/*.{first_candidate}.*.M',
        v3_params,
        IMF_DATA_HEADERS_V3,
    )


def _probe_irfcl_catalog(attempt_log=None):
    """Query IRFCL with a narrow wildcard key to discover valid codelists.

    ``USA.*.*.M?detail=serieskeysonly`` asks IMF for every indicator+
    sector combination available for the USA at monthly frequency,
    without returning any observation values. The response is small
    but includes populated ``values`` arrays for every series
    dimension — so we see the actual INDICATOR, SECTOR, etc. codes
    the new DSD uses. All codelist entries are logged into
    ``attempt_log`` so the /api/cofer/refresh diagnostic can expose
    them if the main fetches still fail.
    """
    # Try a couple of country codes in case the new codelist uses ISO3
    for country in ('USA', 'US'):
        url = f'{IMF_DATA_BASE_V3}/data/dataflow/IMF.STA/IRFCL/+/{country}.*.*.M'
        params = {'detail': 'serieskeysonly'}
        try:
            resp = requests.get(url, params=params, headers=IMF_DATA_HEADERS_V3, timeout=120)
            if resp.status_code != 200:
                if attempt_log is not None:
                    attempt_log.append(
                        f'catalog probe {country}: HTTP {resp.status_code} ({len(resp.content)} bytes)'
                    )
                continue
            doc = resp.json()
            data = doc.get('data') if isinstance(doc.get('data'), dict) else doc
            structures = data.get('structures') or []
            if not structures:
                if attempt_log is not None:
                    attempt_log.append(f'catalog probe {country}: 200 but no structures in response')
                continue
            dims = (structures[0] or {}).get('dimensions', {}).get('series') or []
            codes = {}
            for dim in dims:
                dim_id = dim.get('id')
                values = [v.get('id', '') for v in (dim.get('values') or [])]
                codes[dim_id] = values
            any_populated = any(v for v in codes.values())
            if attempt_log is not None:
                summary = ', '.join(f'{k}={len(v)}' for k, v in codes.items())
                attempt_log.append(f'catalog probe {country}: 200 OK, dims: {summary}')
                # Dump the whole INDICATOR list so we can pick the right
                # code for total reserves and FX reserves (chunked to keep
                # individual log entries readable).
                for dim_id, values in codes.items():
                    if not values:
                        continue
                    if dim_id == 'INDICATOR':
                        # chunk long indicator list into 20-code groups
                        for i in range(0, len(values), 20):
                            chunk = values[i:i + 20]
                            attempt_log.append(f'  INDICATOR[{i}:{i+len(chunk)}]: {chunk}')
                    else:
                        attempt_log.append(f'  {dim_id}: {values}')
            if any_populated:
                return codes
        except requests.exceptions.RequestException as e:
            if attempt_log is not None:
                attempt_log.append(f'catalog probe {country}: {type(e).__name__}: {str(e)[:200]}')
    return None


def _probe_irfcl_sample(attempt_log=None):
    """Fetch one real observation for USA to verify the key format works
    end-to-end and sanity-check the data scale.

    Uses a specific guess for the "total reserves" indicator. If it
    returns numeric data, we know the sector code, indicator code,
    and key layout are all correct. The ``attempt_log`` gets the last
    observation value so we can eyeball whether it looks like
    ~$900B (USA total reserves as of mid-2025).
    """
    # Best guess: IRFCLDT1_IRFCL121_USD is first in the alphabetical
    # catalog sort. In IRFCL templates, row summaries often come first.
    # We'll fetch a short recent window for USA only.
    for ind_code in ('IRFCLDT1_IRFCL121_USD', 'IRFCLDT1_IRFCLCDCFCU_USD'):
        url = f'{IMF_DATA_BASE_V3}/data/dataflow/IMF.STA/IRFCL/+/USA.{ind_code}.S1XS1311.M'
        params = {
            'dimensionAtObservation': 'TIME_PERIOD',
            'attributes': 'dsd',
            'measures': 'all',
            'c[TIME_PERIOD]': 'ge:2025-01',
        }
        try:
            resp = requests.get(url, params=params, headers=IMF_DATA_HEADERS_V3, timeout=60)
            if resp.status_code != 200:
                if attempt_log is not None:
                    attempt_log.append(
                        f'sample probe {ind_code}: HTTP {resp.status_code} ({len(resp.content)} bytes)'
                    )
                continue
            doc = resp.json()
            parsed = _parse_imf_sdmx_series(doc)
            if parsed:
                # Get the last value
                sample_obs = {}
                for iso, periods in parsed.items():
                    if periods:
                        last_period = max(periods.keys())
                        sample_obs[iso] = (last_period, periods[last_period])
                if attempt_log is not None:
                    attempt_log.append(
                        f'sample probe {ind_code}: 200 OK ({len(resp.content)} bytes), '
                        f'parsed={len(parsed)} countries, latest: {sample_obs}'
                    )
                return ind_code
            else:
                if attempt_log is not None:
                    attempt_log.append(
                        f'sample probe {ind_code}: 200 OK ({len(resp.content)} bytes) but 0 series parsed'
                    )
        except requests.exceptions.RequestException as e:
            if attempt_log is not None:
                attempt_log.append(f'sample probe {ind_code}: {type(e).__name__}: {str(e)[:200]}')
    return None


def _fetch_imf_data_indicator(indicator_code, attempt_log=None):
    """Fetch one IRFCL indicator from the IMF Data API.

    Tries each URL in :func:`_imf_data_attempt_urls` and returns the
    first document whose parser yields at least one country. A 200
    response that parses to zero series is treated as "no match" and
    falls through to the next variant (this is what the new IMF API
    returns when the SDMX key positions are right but the *values*
    don't match any catalog codes — e.g. ``RAFA_USD`` vs ``RAFA``).
    Appends a short summary of each attempt to ``attempt_log``.
    """
    for label, url, params, headers in _imf_data_attempt_urls(indicator_code):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=180)
            status = resp.status_code
            size = len(resp.content)
            if status != 200:
                note = (
                    f'{indicator_code} {label}: HTTP {status} '
                    f'({size} bytes) — {resp.text[:200]}'
                )
                logger.warning(note)
                if attempt_log is not None:
                    attempt_log.append(note)
                continue
            try:
                doc = resp.json()
            except ValueError:
                note = f'{indicator_code} {label}: 200 but body is not JSON ({size} bytes)'
                if size < 5000:
                    note += f' | body: {resp.text[:1500]}'
                logger.warning(note)
                if attempt_log is not None:
                    attempt_log.append(note)
                continue

            parsed_sample = _parse_imf_sdmx_series(doc)
            if parsed_sample:
                note = (
                    f'{indicator_code} {label}: 200 OK ({size} bytes), '
                    f'{len(parsed_sample)} countries parsed'
                )
                logger.info(note)
                if attempt_log is not None:
                    attempt_log.append(note)
                return doc

            # 200 but empty — probably a code mismatch. Log body for
            # diagnosis and fall through to the next variant.
            note = f'{indicator_code} {label}: 200 OK ({size} bytes) but 0 series parsed'
            if size < 5000:
                note += f' | body: {resp.text[:1500]}'
            logger.warning(note)
            if attempt_log is not None:
                attempt_log.append(note)
        except requests.exceptions.RequestException as e:
            note = f'{indicator_code} {label}: request error — {type(e).__name__}: {str(e)[:200]}'
            logger.warning(note)
            if attempt_log is not None:
                attempt_log.append(note)
    return None


def _parse_imf_sdmx_series(doc):
    """Parse an SDMX-JSON 2.0 data message into ``{iso2: {period: value}}``.

    Handles the nested ``data.dataSets[0].series`` / ``data.structures[0]``
    layout returned by ``sdmxcentral.imf.org``. Series keys like
    ``"0:12:0:0"`` index into ``structures[0].dimensions.series``; observation
    keys index into ``structures[0].dimensions.observation``.
    """
    if not doc:
        return {}

    # SDMX-JSON 2.0 wraps the payload in a top-level "data" object; 1.0 does not.
    data = doc.get('data') if isinstance(doc.get('data'), dict) else doc
    datasets = data.get('dataSets') or []
    structures = data.get('structures') or data.get('structure') or []
    if not datasets or not structures:
        return {}

    structure = structures[0] if isinstance(structures, list) else structures
    dims = structure.get('dimensions', {}) if structure else {}
    series_dims = dims.get('series') or []
    obs_dims = dims.get('observation') or []

    # Locate the country dimension. The legacy IMF SDMX endpoints called
    # it ``REF_AREA`` but the new api.imf.org DSD (``DSD_IRFCL_PUB``) calls
    # it ``COUNTRY``. Accept either.
    ref_area_pos = None
    ref_area_values = []
    for i, dim in enumerate(series_dims):
        if dim.get('id') in ('REF_AREA', 'COUNTRY'):
            ref_area_pos = i
            ref_area_values = dim.get('values', [])
            break
    if ref_area_pos is None:
        return {}

    # Locate TIME_PERIOD in the observation dimension list
    time_pos = None
    time_values = []
    for i, dim in enumerate(obs_dims):
        if dim.get('id') == 'TIME_PERIOD':
            time_pos = i
            time_values = dim.get('values', [])
            break
    if time_pos is None:
        return {}

    all_periods = [v.get('id', '') for v in time_values]

    result = {}
    series_obj = datasets[0].get('series', {}) or {}
    for series_key, series_data in series_obj.items():
        parts = series_key.split(':')
        if ref_area_pos >= len(parts):
            continue
        try:
            ra_idx = int(parts[ref_area_pos])
        except ValueError:
            continue
        if ra_idx < 0 or ra_idx >= len(ref_area_values):
            continue

        country_code = ref_area_values[ra_idx].get('id', '')
        if not country_code:
            continue
        # Normalize to the ISO3 representation that COUNTRY_NAMES and
        # the rest of the pipeline use. Legacy DBnomics returned ISO2;
        # the new api.imf.org uses ISO3 directly.
        if len(country_code) == 2:
            iso2 = country_code
        elif len(country_code) == 3:
            # Walk the ISO2→ISO3 map to synthesize a fake "iso2" key so
            # downstream _build_reserves_result (which still reads ISO2
            # keys for the DBnomics path) can recognize it. We use the
            # ISO3 as the key directly and handle it in the builder.
            iso2 = country_code
        else:
            continue

        period_values = {}
        for obs_key, obs_arr in (series_data.get('observations') or {}).items():
            obs_parts = obs_key.split(':')
            if time_pos >= len(obs_parts):
                continue
            try:
                t_idx = int(obs_parts[time_pos])
            except ValueError:
                continue
            if t_idx < 0 or t_idx >= len(all_periods):
                continue
            period = all_periods[t_idx]
            if isinstance(obs_arr, list) and obs_arr and obs_arr[0] is not None:
                period_values[period] = obs_arr[0]

        if period_values:
            # Multiple series could exist per country (e.g. different
            # REF_SECTOR buckets); merge observations so the most complete
            # time series wins.
            if iso2 in result:
                result[iso2].update(period_values)
            else:
                result[iso2] = period_values

    return result


def _fetch_reserves_imf_sdmx(attempt_log=None):
    """Fetch monthly reserves from the IMF Data API (primary source)."""
    try:
        logger.info("Fetching reserves from IMF Data API (live)...")

        # Probe the IRFCL catalog first to discover valid indicator and
        # sector codes. This is a narrow query against one country with
        # ``detail=serieskeysonly`` — small response, but enough for
        # IMF to populate the dimension value arrays with the real
        # codelist entries that downstream fetches need to match.
        _probe_irfcl_catalog(attempt_log)
        # Sample probe: fetch one real observation for USA to verify
        # the key format works end-to-end and eyeball the scale.
        _probe_irfcl_sample(attempt_log)

        total_doc = _fetch_imf_data_indicator('RAFA_USD', attempt_log)
        fx_doc = _fetch_imf_data_indicator('RAFAFX_USD', attempt_log)

        total_by_country = _parse_imf_sdmx_series(total_doc)
        fx_by_country = _parse_imf_sdmx_series(fx_doc)

        if attempt_log is not None:
            attempt_log.append(
                f'parser extracted: total={len(total_by_country)} countries, '
                f'fx={len(fx_by_country)} countries'
            )
            # If the parser got nothing, surface the top-level shape of the
            # documents so we can tell what IMF actually sent us.
            if not total_by_country and isinstance(total_doc, dict):
                top_keys = list(total_doc.keys())
                data_obj = total_doc.get('data') if isinstance(total_doc.get('data'), dict) else total_doc
                data_keys = list(data_obj.keys()) if isinstance(data_obj, dict) else []
                datasets = data_obj.get('dataSets') if isinstance(data_obj, dict) else None
                structures = (data_obj.get('structures') or data_obj.get('structure')) if isinstance(data_obj, dict) else None
                attempt_log.append(
                    f'total_doc shape: top_keys={top_keys}, '
                    f'data_keys={data_keys}, '
                    f'dataSets_len={len(datasets) if datasets else 0}, '
                    f'structures_len={len(structures) if isinstance(structures, list) else (1 if structures else 0)}'
                )

        if not total_by_country:
            logger.warning("IMF Data API returned no parseable total reserves data")
            return None

        result = _build_reserves_result(
            total_by_country,
            fx_by_country,
            source_label='IMF IRFCL (api.imf.org)',
        )

        if result:
            logger.info(
                "IMF Data API reserves loaded: %d months, %d countries, latest %s",
                len(result['years']), len(result['countries']), result['years'][-1],
            )
        return result

    except requests.exceptions.Timeout:
        logger.error("IMF Data API timeout")
        return None
    except Exception as e:
        logger.error(f"IMF Data API fetch failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════
# FALLBACK: DBnomics IMF/IRFCL mirror (monthly, may be stale)
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
    """Fetch monthly reserves from DBnomics IMF/IRFCL (fallback mirror)."""
    try:
        logger.info("Fetching reserves from DBnomics IMF/IRFCL (monthly)...")

        total_docs = _fetch_dbnomics_indicator('RAFA_USD')
        fx_docs = _fetch_dbnomics_indicator('RAFAFX_USD')

        if not total_docs:
            logger.warning("DBnomics returned no total reserves data")
            return None

        def _parse(docs):
            by_country = {}
            for doc in docs:
                iso2 = doc.get('dimensions', {}).get('REF_AREA', '')
                periods = doc.get('period', [])
                values = doc.get('value', [])
                if iso2 and periods:
                    by_country[iso2] = dict(zip(periods, values))
            return by_country

        total_by_country = _parse(total_docs)
        fx_by_country = _parse(fx_docs)

        result = _build_reserves_result(
            total_by_country,
            fx_by_country,
            source_label='IMF IRFCL (via DBnomics mirror)',
        )
        if result:
            logger.info(
                "DBnomics IRFCL reserves loaded: %d months, %d countries, latest %s",
                len(result['years']), len(result['countries']), result['years'][-1],
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
# ORCHESTRATOR: IMF Data API → DBnomics mirror → World Bank annual
# ══════════════════════════════════════════════════════════════════════════

def _fetch_reserves():
    """Fetch reserves from the freshest source available.

    Order:
      1. IMF Data API (api.imf.org, live monthly, authoritative)
      2. DBnomics IRFCL mirror (may be stale while their fetcher is rewritten)
      3. World Bank annual (last-resort fallback)
    """
    result = _fetch_reserves_imf_sdmx()
    if result:
        return result
    logger.warning("IMF Data API failed — falling back to DBnomics IRFCL mirror")

    result = _fetch_reserves_dbnomics()
    if result:
        return result
    logger.warning("DBnomics IRFCL failed — falling back to World Bank annual data")
    return _fetch_reserves_wb()


def get_cofer_data():
    """Public API: returns cached reserves data."""
    return _cache.get()


def refresh_cache():
    """Clear the cache and force an immediate re-fetch.

    Returns the freshly-fetched data so callers can surface the new
    ``meta.source`` / ``meta.period_range`` without a second round-trip.
    """
    _cache.clear()
    return _cache.get()


def diagnose_fetch():
    """Run the full fetch chain with per-attempt logging (bypasses cache).

    Returns a dict containing:
      - ``attempts``: list of short strings describing each HTTP attempt
      - ``source``: which source ultimately succeeded (or None)
      - ``latest_period``: most recent period in the returned payload
      - ``country_count``: how many countries made it through
      - ``period_range``: human-readable range string

    Intended for the /api/cofer/refresh endpoint so the user can verify
    the fix worked without having to scrape server logs.
    """
    attempts = []

    result = _fetch_reserves_imf_sdmx(attempts)
    source = None
    if result:
        source = result.get('meta', {}).get('source')
    else:
        attempts.append('IMF Data API chain failed — trying DBnomics mirror')
        result = _fetch_reserves_dbnomics()
        if result:
            source = result.get('meta', {}).get('source')
        else:
            attempts.append('DBnomics mirror failed — falling back to World Bank annual')
            result = _fetch_reserves_wb()
            if result:
                source = result.get('meta', {}).get('source')

    if result:
        # Store in cache so subsequent /api/cofer calls see the same data
        with _cache._lock:
            _cache._data = result
            _cache._last_fetch = time.time()

    return {
        'attempts': attempts,
        'source': source,
        'latest_period': (result.get('years') if result else [''])[-1] if result and result.get('years') else '',
        'period_range': (result or {}).get('meta', {}).get('period_range', ''),
        'country_count': len((result or {}).get('countries') or []),
    }
