"""
Central Bank Reserves data client.

Primary: IMF Data API (api.imf.org/external/sdmx/3.0, live monthly)
  - Replaced the retired dataservices.imf.org on 2025-11-05.
  - RAFA_USD   = Total official reserve assets (USD millions)
  - RAFAFX_USD = Foreign currency reserves (USD millions)
  - Gold = Total - FX

Fallback 1: DBnomics IMF/IFS (monthly, ~140 countries, reliable mirror)
  - RAFA_USD   = Total reserves incl. gold (USD millions)
  - RAXGFX_USD = FX reserves excl. gold (USD millions)
  - IFS is the canonical IMF macro dataflow; the old IRFCL mirror was
    frozen and returned empty results due to REF_SECTOR mismatch.

Fallback 2: World Bank API (annual, ~180 countries)
  - FI.RES.TOTL.CD = Total reserves including gold (current US$)
  - FI.RES.XGLD.CD = Foreign exchange reserves excluding gold (current US$)

Thread-safe cache with 24-hour TTL. Use refresh_cache() to force a
re-fetch (wired up to /api/cofer/refresh in backend.routes).
"""

import datetime as _dt
import json
import os
import threading
import time
import logging
import requests

from config import Config

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

RESERVES_CACHE_FILE = os.path.join(Config.DATA_DIR, 'reserves_cache.json')


class ReservesCache:
    """File-backed cache for reserves data.

    Gunicorn on Render runs multiple workers, each with its own process
    memory. A pure in-memory cache isn't shared — worker A could have
    fresh IMF data (from /api/cofer/refresh) while worker B still has
    stale DBnomics data from the previous deploy, making requests
    round-robin between fresh and stale answers. Persisting to
    ``${DATA_DIR}/reserves_cache.json`` (which is a mounted disk on
    Render) means every worker reads from the same source of truth.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0
        self._load_from_disk()

    def _load_from_disk(self):
        if not os.path.exists(RESERVES_CACHE_FILE):
            return
        try:
            with open(RESERVES_CACHE_FILE, 'r') as f:
                wrapper = json.load(f)
            self._data = wrapper.get('data')
            self._last_fetch = float(wrapper.get('fetched_at', 0))
        except (OSError, ValueError, json.JSONDecodeError):
            logger.warning("Failed to load reserves cache from disk; ignoring")

    def _save_to_disk(self, data):
        try:
            os.makedirs(Config.DATA_DIR, exist_ok=True)
            tmp = RESERVES_CACHE_FILE + '.tmp'
            with open(tmp, 'w') as f:
                json.dump({'fetched_at': time.time(), 'data': data}, f)
            os.replace(tmp, RESERVES_CACHE_FILE)
        except OSError as e:
            logger.warning(f"Failed to persist reserves cache: {e}")

    def get(self):
        with self._lock:
            self._load_from_disk()
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                if not _is_stale(self._data):
                    return self._data
                logger.info("Cached reserves data is stale, re-fetching")
        data = _fetch_reserves()
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
                self._save_to_disk(data)
            return data
        with self._lock:
            return self._data or _empty_result()

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0
            try:
                if os.path.exists(RESERVES_CACHE_FILE):
                    os.remove(RESERVES_CACHE_FILE)
            except OSError:
                pass


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
        v = float(value)
        if v != v:  # NaN check
            return None
        return round(v / 1000, 2)
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

    # Defensive cutoff: drop any period strictly later than the current
    # month. IMF IRFCL codelists sometimes include future months (the
    # template pre-declares them) and a parser misalignment could
    # otherwise bleed real values into future columns like ``2026-08``.
    # We also drop anything before 2000 as a sanity floor.
    today = _dt.date.today()
    cutoff = f'{today.year:04d}-{today.month:02d}'
    periods = sorted(
        p for p in all_periods
        if isinstance(p, str) and p >= '2000-01' and p <= cutoff
    )
    if not periods:
        return None

    # Drop "mostly empty" columns. IMF reporting has a lag — only a
    # handful of countries report within the current month — so the
    # latest columns can appear visually empty in the frontend table.
    # Require each column to have total-reserves data for at least
    # 25% of all countries (with a floor of 5). This hides placeholder
    # months while still showing the most-recent *useful* month at the
    # right edge of the table.
    total_country_count = len(total_by_country)
    min_coverage = max(5, total_country_count // 4)
    def _coverage(period):
        return sum(
            1 for cd in total_by_country.values()
            if cd.get(period) is not None
        )
    periods = [p for p in periods if _coverage(p) >= min_coverage]
    if not periods:
        return None

    # Keep history from 2004 onward (~250 months). This captures the
    # dramatic reserve buildup period (China $600B→$4T) while still
    # excluding the sparse pre-2004 data.
    periods = [p for p in periods if p >= '2004-01']

    countries = []
    for code in total_by_country:
        # Accept both ISO2 (DBnomics / legacy) and ISO3 (new api.imf.org).
        # For ISO3 we don't require COUNTRY_NAMES membership — the
        # display_name fallback below handles unknown codes by using
        # the code itself as the label.
        code_stripped = (code or '').strip().upper()
        if len(code_stripped) == 2:
            iso3 = ISO2_TO_ISO3.get(code_stripped)
        elif len(code_stripped) == 3 and code_stripped.isalpha():
            iso3 = code_stripped
        else:
            iso3 = None
        if not iso3:
            continue

        total_data = total_by_country.get(code, {})
        fx_data = fx_by_country.get(code, {})

        total_values = []
        fx_values = []
        gold_values = []
        latest_real_idx = -1

        for i, period in enumerate(periods):
            total_b = _usd_millions_to_billions(total_data.get(period))
            fx_b = _usd_millions_to_billions(fx_data.get(period))
            gold_b = (
                round(total_b - fx_b, 2)
                if (total_b is not None and fx_b is not None)
                else None
            )
            total_values.append(total_b)
            fx_values.append(fx_b)
            gold_values.append(gold_b)
            if total_b is not None:
                latest_real_idx = i

        # Skip countries with no usable total reserves at all
        if latest_real_idx < 0:
            continue

        # Capped forward-fill: smooth over reporting gaps to keep chart
        # lines continuous. IFS data is dense through ~2025-06 but IRFCL
        # FX only covers 10 countries — without fill, gold (= total − fx)
        # shows dashes for every other country after 2025-06. A 9-month
        # cap carries IFS values far enough to cover the gap between the
        # frozen mirror and today, while still breaking the line for
        # countries that genuinely stopped reporting a long time ago.
        def _capped_fill(arr, max_gap=9):
            last_val = None
            gap = 0
            for i, v in enumerate(arr):
                if v is not None:
                    last_val = v
                    gap = 0
                elif last_val is not None:
                    gap += 1
                    if gap <= max_gap:
                        arr[i] = last_val
        _capped_fill(total_values)
        _capped_fill(fx_values)

        # Recompute gold after filling so short fx gaps don't create
        # spurious null gold entries.
        gold_values = []
        for t, f in zip(total_values, fx_values):
            if t is not None and f is not None:
                gold_values.append(round(t - f, 2))
            else:
                gold_values.append(None)

        latest_real_period = periods[latest_real_idx]

        display_name = COUNTRY_NAMES.get(iso3, iso3)
        countries.append({
            'iso3': iso3,
            'name': display_name,
            'total_reserves': total_values,
            'fx_reserves': fx_values,
            'gold_reserves': gold_values,
            'latest_real_period': latest_real_period,
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
        # Data Template 4 Row 11 = "Official reserve assets" (Section I.A
        # subtotal) — this is the headline total reserves line in the
        # IRFCL template. DT1 only has sub-items rows 31-65 and CDCFC.
        'IRFCLDT4_IRFCL11_DIC_GROUP_USD',
        'IRFCLDT4_IRFCL11_DIC_XDRB_USD',
        'IRFCLDT4_IRFCL11_DIC_XXDR_USD',
        # Row 1 "Official reserve assets and other foreign currency assets"
        # (grand total of Sections I.A + I.B)
        'IRFCLDT4_IRFCL1_LP_USD',
    ),
    'RAFAFX_USD': (
        # Row 12 = "Foreign currency reserves" (Section I.A.1) — the FX
        # reserves headline line
        'IRFCLDT4_IRFCL12_USD',
        'IRFCLDT4_IRFCL12_LP_USD',
        # DT1 candidates as fallback (these are sub-items but some
        # countries only report at the detailed level)
        'IRFCLDT1_IRFCLCDCFC_USD',
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
    # Minimal params: NO ``attributes=dsd`` and NO ``measures=all``.
    # Those are the defaults on some SDMX 3.0 servers but on
    # api.imf.org they cause observation arrays to come back shaped
    # like ``['0', None, 0, None]`` (status + series attributes
    # embedded) instead of the plain ``[value]`` the SDMX-JSON 2.0
    # spec describes. Leaving the params minimal gives us the clean
    # single-element observation arrays the parser expects.
    v3_params = {
        'dimensionAtObservation': 'TIME_PERIOD',
    }

    candidates = IMF_IRFCL_INDICATOR_CANDIDATES.get(
        indicator_code, (indicator_code,)
    )
    for ind in candidates:
        # Primary: wildcard sector. Different countries report under
        # different SECTOR codes (S1X vs S1XS1311), and the previous
        # ``S1XS1311`` filter dropped countries that only publish
        # under the pure monetary-authorities sector S1X. Using ``*``
        # lets the parser merge all sector series per country.
        yield (
            f'v3 IMF.STA *.{ind}.*.M',
            f'{IMF_DATA_BASE_V3}/data/dataflow/IMF.STA/IRFCL/+/*.{ind}.*.M',
            v3_params,
            IMF_DATA_HEADERS_V3,
        )
    # Belt-and-suspenders: also try the legacy known sector in case the
    # wildcard variant returns 0 (some flavors of the SDMX service
    # require an explicit dimension value at position 3).
    first_candidate = candidates[0]
    yield (
        f'v3 IMF.STA *.{first_candidate}.{IMF_IRFCL_DEFAULT_SECTOR}.M',
        f'{IMF_DATA_BASE_V3}/data/dataflow/IMF.STA/IRFCL/+/*.{first_candidate}.{IMF_IRFCL_DEFAULT_SECTOR}.M',
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


def _probe_country_sectors(country, attempt_log=None):
    """Small probe reporting which SECTOR codes a country has available.

    Returns just the SECTOR dimension codes + observation counts so we
    can see whether HKG/CHN report under the same sector as USA. Short
    output so we can afford to probe multiple countries.
    """
    url = f'{IMF_DATA_BASE_V3}/data/dataflow/IMF.STA/IRFCL/+/{country}.*.*.M'
    params = {'detail': 'serieskeysonly'}
    try:
        resp = requests.get(url, params=params, headers=IMF_DATA_HEADERS_V3, timeout=60)
        if resp.status_code != 200:
            if attempt_log is not None:
                attempt_log.append(
                    f'sector probe {country}: HTTP {resp.status_code}'
                )
            return None
        doc = resp.json()
        data = doc.get('data') if isinstance(doc.get('data'), dict) else doc
        structures = data.get('structures') or []
        if not structures:
            return None
        dims = (structures[0] or {}).get('dimensions', {}).get('series') or []
        codes = {}
        for dim in dims:
            dim_id = dim.get('id')
            values = [v.get('id', '') or v.get('value', '') for v in (dim.get('values') or [])]
            codes[dim_id] = values
        if attempt_log is not None:
            attempt_log.append(
                f'sector probe {country}: SECTOR={codes.get("SECTOR")}, '
                f'INDICATOR count={len(codes.get("INDICATOR") or [])}'
            )
        return codes
    except requests.exceptions.RequestException as e:
        if attempt_log is not None:
            attempt_log.append(
                f'sector probe {country}: {type(e).__name__}: {str(e)[:200]}'
            )
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
    for ind_code in ('IRFCLDT1_IRFCL121_USD', 'IRFCLDT1_IRFCLCDCFC_USD'):
        url = f'{IMF_DATA_BASE_V3}/data/dataflow/IMF.STA/IRFCL/+/USA.{ind_code}.S1XS1311.M'
        # No time filter — SDMX 3.0 c[TIME_PERIOD] syntax silently
        # matched zero series here even though the unfiltered query
        # returned ~260KB of data. Accept the extra bytes; this probe
        # only runs when the main path is in trouble anyway.
        params = {
            'dimensionAtObservation': 'TIME_PERIOD',
            'attributes': 'dsd',
            'measures': 'all',
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


def _parse_imf_sdmx_series(doc, attempt_log=None):
    """Parse an SDMX-JSON 2.0 data message into ``{iso2: {period: value}}``.

    Handles the nested ``data.dataSets[0].series`` / ``data.structures[0]``
    layout returned by ``sdmxcentral.imf.org``. Series keys like
    ``"0:12:0:0"`` index into ``structures[0].dimensions.series``.

    Observation keys vary by SDMX-JSON version and ``dimensionAtObservation``
    setting:
      - SDMX-JSON 1.0: numeric index like ``"0"`` into
        ``structures[0].dimensions.observation[0].values`` (indexed).
      - SDMX-JSON 2.0 w/ ``dimensionAtObservation=TIME_PERIOD``: the key IS
        the time period string directly, like ``"2025-M07"`` (keyed).
    We try the indexed path first and fall back to the keyed path.
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

    # Normalize period format. SDMX 3.0 convention is '2025-M07' for
    # monthly periods; our downstream pipeline (and DBnomics path) uses
    # plain '2025-07'. Convert M-dates to hyphen form up front.
    def _norm_period(p):
        if not isinstance(p, str):
            return p
        # '2025-M07' -> '2025-07'; '2025M07' -> '2025-07'
        if '-M' in p:
            return p.replace('-M', '-')
        if len(p) == 7 and p[4] == 'M':
            return p[:4] + '-' + p[5:]
        return p

    # api.imf.org returns dimension ``values`` entries keyed as
    # ``{"value": "USA"}`` rather than ``{"id": "USA"}`` even though
    # the SDMX-JSON spec uses ``id``. Accept either for every
    # dimension-value lookup in this parser.
    def _val_id(entry):
        if not isinstance(entry, dict):
            return ''
        return entry.get('id') or entry.get('value') or entry.get('name') or ''

    all_periods = [_norm_period(_val_id(v)) for v in time_values]

    result = {}
    series_obj = datasets[0].get('series', {}) or {}

    # Diagnostic dump of the raw observation structure. Only runs when
    # a caller passes attempt_log (diagnose_fetch path).
    if attempt_log is not None and series_obj:
        # Dump time dimension values at several positions so we can tell
        # whether IMF orders them ascending, descending, encounter-order,
        # or something else entirely.
        tv_ids = [_val_id(v) for v in time_values]
        def _sample(idx):
            return tv_ids[idx] if 0 <= idx < len(tv_ids) else '-'
        attempt_log.append(
            f'  parser: time_values len={len(time_values)}, '
            f'[0]={_sample(0)}, [1]={_sample(1)}, [26]={_sample(26)}, '
            f'[52]={_sample(52)}, [-1]={_sample(len(tv_ids) - 1)}'
        )

        # Dump a sample observation for the first series AND for USA,
        # CHN specifically (the two countries that were showing stale
        # data in the previous diagnostic round).
        series_items = list(series_obj.items())
        first_series_key, first_series = series_items[0]
        first_obs = list((first_series.get('observations') or {}).items())[:3]
        last_obs = list((first_series.get('observations') or {}).items())[-3:]
        attempt_log.append(
            f'  parser: first series key={first_series_key}, '
            f'obs_count={len(first_series.get("observations") or {})}, '
            f'first3={first_obs}, last3={last_obs}'
        )

        # Find USA / CHN in ref_area_values to dump their series
        target_countries = {'USA', 'CHN'}
        for i, v in enumerate(ref_area_values):
            code = _val_id(v)
            if code not in target_countries:
                continue
            # Match any series whose REF_AREA index == i
            for sk, sd in series_obj.items():
                parts = sk.split(':')
                if ref_area_pos < len(parts):
                    try:
                        if int(parts[ref_area_pos]) != i:
                            continue
                    except ValueError:
                        continue
                obs_items = list((sd.get('observations') or {}).items())
                if not obs_items:
                    continue
                attempt_log.append(
                    f'  parser: {code} series_key={sk} '
                    f'obs_count={len(obs_items)} '
                    f'first2={obs_items[:2]} last2={obs_items[-2:]}'
                )
                break

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

        country_code = _val_id(ref_area_values[ra_idx])
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
            # Two SDMX-JSON observation-key conventions; try both.
            period = None
            # Indexed form: obs_key is a numeric index into time_values.
            try:
                obs_parts = obs_key.split(':')
                idx_str = obs_parts[time_pos] if time_pos < len(obs_parts) else obs_parts[0]
                t_idx = int(idx_str)
                if 0 <= t_idx < len(all_periods):
                    candidate = all_periods[t_idx]
                    if candidate:
                        period = candidate
            except (ValueError, IndexError):
                pass
            # Keyed form: obs_key IS the time period string directly
            # (e.g., "2025-M07"). SDMX-JSON 2.0 w/ dimensionAtObservation
            # =TIME_PERIOD emits this shape.
            if not period:
                period = _norm_period(obs_key)
            if not period:
                continue

            # Extract the primary measure value. SDMX-JSON 2.0 observation
            # arrays normally have shape ``[OBS_VALUE, ...obs_attrs]``, but
            # api.imf.org sometimes prepends series-level attributes when
            # asked for them. Scan the array for the first real number.
            # Skip integer 0 since some obs attributes encode as int 0
            # ("SCALE" / "OBS_STATUS"); a 0-scale of "0 USD millions"
            # doesn't make sense for reserves data anyway.
            value = None
            if isinstance(obs_arr, (int, float)) and obs_arr != 0:
                value = obs_arr
            elif isinstance(obs_arr, list):
                for item in obs_arr:
                    if isinstance(item, (int, float)):
                        if item == 0:
                            continue
                        value = item
                        break
                    if isinstance(item, str):
                        try:
                            n = float(item)
                        except (TypeError, ValueError):
                            continue
                        if n == 0:
                            continue
                        value = n
                        break
            if value is None:
                continue

            # api.imf.org IRFCL ``*_USD`` indicators return values in
            # plain US dollars — for instance USA total reserves came
            # back as 242_257_289_682. The rest of our pipeline
            # (builder, DBnomics path, frontend) works in USD millions,
            # so normalize here by dividing by 1e6. Chain:
            #   IMF raw         242_257_289_682  (USD)
            #   ÷ 1e6    →         242_257.29     (USD millions, stored)
            #   ÷ 1000   →             242.26     (USD billions, rendered)
            period_values[period] = value / 1_000_000

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

        # Diagnostic probes only run when a caller passes ``attempt_log``
        # (i.e. /api/cofer/refresh). They add ~3-5 extra HTTP round-trips
        # before the main fetch and they only serve to log codelist info
        # into the attempt_log, so they shouldn't slow down every
        # /api/cofer request.
        if attempt_log is not None:
            _probe_irfcl_catalog(attempt_log)
            for probe_country in ('CHN', 'HKG', 'JPN'):
                _probe_country_sectors(probe_country, attempt_log)
            _probe_irfcl_sample(attempt_log)

        total_doc = _fetch_imf_data_indicator('RAFA_USD', attempt_log)
        fx_doc = _fetch_imf_data_indicator('RAFAFX_USD', attempt_log)

        # Pass attempt_log for the first parse so we dump a sample of
        # the raw observation shape when diagnose_fetch is running.
        total_by_country = _parse_imf_sdmx_series(total_doc, attempt_log)
        fx_by_country = _parse_imf_sdmx_series(fx_doc)

        # IRFCL FX coverage is typically sparse (~10 countries). If we
        # got good total data but poor FX, REPLACE the entire FX dataset
        # with DBnomics IFS RAXGFX_USD which has ~140 countries. The IRFCL
        # FX values for the few countries it does cover are often wrong
        # (e.g. India showing -$0.61B), so a full replacement is safer
        # than a selective backfill.
        if total_by_country and len(fx_by_country) < len(total_by_country) * 0.5:
            if attempt_log is not None:
                attempt_log.append(
                    f'IRFCL FX sparse ({len(fx_by_country)} vs {len(total_by_country)} total) '
                    f'— replacing FX with DBnomics IFS (RAXGFX_USD)'
                )
            try:
                ifs_fx_docs = _fetch_ifs_indicator('RAXGFX_USD')
                ifs_fx = {}
                for doc in ifs_fx_docs:
                    code = doc.get('series_code', '')
                    parts = code.split('.')
                    iso2 = parts[1] if len(parts) >= 2 else ''
                    if not iso2 or len(iso2) != 2 or not iso2.isalpha() or not iso2.isupper():
                        continue
                    # Match against total_by_country keys (ISO3 from api.imf.org)
                    iso3 = ISO2_TO_ISO3.get(iso2)
                    key = iso3 if iso3 and iso3 in total_by_country else iso2
                    if key not in total_by_country and iso2 not in total_by_country:
                        continue
                    periods = doc.get('period', [])
                    values = doc.get('value', [])
                    if periods:
                        ifs_fx[key] = dict(zip(periods, values))
                if ifs_fx:
                    fx_by_country = ifs_fx
                if attempt_log is not None:
                    attempt_log.append(
                        f'After IFS replacement: fx={len(fx_by_country)} countries'
                    )
            except Exception as e:
                if attempt_log is not None:
                    attempt_log.append(f'IFS FX replacement failed: {e}')

        if attempt_log is not None:
            attempt_log.append(
                f'parser extracted: total={len(total_by_country)} countries, '
                f'fx={len(fx_by_country)} countries'
            )
            # Show sample codes + periods + values so we can eyeball
            # (a) what code format IMF returned and (b) scale sanity
            # for the indicator we picked. If CHN shows up around
            # $3T it's total reserves; if it's $50B it's a sub-item.
            sample_codes = list(total_by_country.keys())[:10]
            attempt_log.append(f'  total_by_country first 10 keys: {sample_codes}')
            if total_by_country:
                first_key = sample_codes[0]
                first_periods = total_by_country[first_key]
                if first_periods:
                    max_p = max(first_periods.keys())
                    attempt_log.append(
                        f'  {first_key} latest: {max_p} = {first_periods[max_p]} (USD millions)'
                    )
            # Check known large countries
            for probe_key in ('CHN', 'USA', 'JPN', 'DEU'):
                if probe_key in total_by_country:
                    tdata = total_by_country[probe_key]
                    if tdata:
                        max_p = max(tdata.keys())
                        attempt_log.append(
                            f'  {probe_key} latest: {max_p} = {tdata[max_p]} (USD millions)'
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
        if result is None and attempt_log is not None:
            attempt_log.append(
                '_build_reserves_result returned None despite '
                f'total={len(total_by_country)}, fx={len(fx_by_country)} — '
                'probably all codes were filtered out or period format mismatch'
            )
            return None

        # Plausibility sanity check. Reject if:
        #  (a) USA total is outside $50B-$1.5T (wrong indicator code), OR
        #  (b) FX data is mostly empty (IRFCL FX indicator not working) —
        #      fewer than 20% of countries have any FX data means the gold
        #      column will be all-None and the chart is broken.
        if result:
            countries_list = result.get('countries', [])

            # Check USA total
            usa = next((c for c in countries_list if c['iso3'] == 'USA'), None)
            usa_latest_total = None
            if usa:
                for v in reversed(usa['total_reserves']):
                    if v is not None:
                        usa_latest_total = v
                        break
            usa_ok = (
                usa_latest_total is not None
                and 50.0 <= usa_latest_total <= 1500.0
            )

            # Check FX coverage
            fx_countries = sum(
                1 for c in countries_list
                if any(v is not None for v in c.get('fx_reserves', []))
            )
            fx_coverage_pct = (fx_countries / len(countries_list) * 100) if countries_list else 0
            fx_ok = fx_coverage_pct >= 20

            plausible = usa_ok and fx_ok

            if attempt_log is not None:
                attempt_log.append(
                    f'  plausibility: USA total={usa_latest_total} B USD '
                    f'(range 50-1500 => {"OK" if usa_ok else "FAIL"}), '
                    f'FX coverage={fx_countries}/{len(countries_list)} '
                    f'({fx_coverage_pct:.0f}%, need >=20% => {"OK" if fx_ok else "FAIL"})'
                )
            if not plausible:
                reasons = []
                if not usa_ok:
                    reasons.append(f'USA total={usa_latest_total}')
                if not fx_ok:
                    reasons.append(f'FX coverage={fx_coverage_pct:.0f}%')
                logger.warning(
                    "IMF Data API result rejected: %s", ', '.join(reasons),
                )
                return None

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
# FALLBACK: DBnomics IMF/IFS (monthly, reliable mirror)
# ══════════════════════════════════════════════════════════════════════════
#
# The old IRFCL mirror on DBnomics used REF_SECTOR='S1X' which returns
# empty results. IFS is the canonical IMF macro dataflow for reserves
# and has ~140 countries with RAFA_USD (total) and RAXGFX_USD (FX only).

def _fetch_ifs_indicator(indicator_code):
    """Fetch one IFS indicator for all countries from DBnomics, paging."""
    all_docs = []
    offset = 0
    page_size = 50

    while True:
        url = f'{DBNOMICS_BASE}/series/IMF/IFS'
        params = {
            'dimensions': json.dumps({
                'FREQ': ['M'],
                'INDICATOR': [indicator_code],
            }),
            'observations': '1',
            'limit': str(page_size),
            'offset': str(offset),
            'metadata': 'false',
        }
        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        docs = data.get('series', {}).get('docs', [])
        num_found = data.get('series', {}).get('num_found', 0)

        if not docs:
            break
        all_docs.extend(docs)

        offset += page_size
        if offset >= num_found:
            break

    return all_docs


def _fetch_reserves_dbnomics():
    """Fetch monthly reserves from DBnomics IMF/IFS (fallback)."""
    try:
        logger.info("Fetching reserves from DBnomics IMF/IFS (monthly)...")

        total_docs = _fetch_ifs_indicator('RAFA_USD')
        fx_docs = _fetch_ifs_indicator('RAXGFX_USD')

        if not total_docs:
            logger.warning("DBnomics IFS returned no total reserves data")
            return None

        def _parse(docs):
            by_country = {}
            for doc in docs:
                code = doc.get('series_code', '')
                parts = code.split('.')
                iso2 = parts[1] if len(parts) >= 2 else ''
                # Skip aggregate/regional codes
                if not iso2 or len(iso2) != 2 or not iso2.isalpha() or not iso2.isupper():
                    continue
                periods = doc.get('period', [])
                values = doc.get('value', [])
                if periods:
                    by_country[iso2] = dict(zip(periods, values))
            return by_country

        total_by_country = _parse(total_docs)
        fx_by_country = _parse(fx_docs)

        result = _build_reserves_result(
            total_by_country,
            fx_by_country,
            source_label='IMF IFS (via DBnomics)',
        )
        if result:
            logger.info(
                "DBnomics IFS reserves loaded: %d months, %d countries, latest %s",
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

def _is_stale(result, max_months=6):
    """Return True if the result's latest period is too old to use."""
    years = result.get('years', []) if result else []
    if not years:
        return True
    latest = years[-1]  # e.g. '2025-06'
    today = _dt.date.today()
    try:
        parts = latest.split('-')
        latest_year, latest_month = int(parts[0]), int(parts[1])
        months_behind = (today.year - latest_year) * 12 + (today.month - latest_month)
        return months_behind > max_months
    except (ValueError, IndexError):
        return True


def _fetch_reserves():
    """Fetch reserves from the best available source.

    Strategy: merge DBnomics IFS (dense history, may be stale) with the
    live IMF IRFCL API (sparse but current). IFS provides a smooth
    monthly backbone for every country back to 2004. IRFCL overlays the
    latest months where IFS has stopped updating.

    For each country and period, IRFCL takes precedence over IFS (it's
    the more authoritative source for recent data). IFS fills historical
    gaps that IRFCL doesn't cover.
    """
    # ── Fetch both sources in sequence ──────────────────────────────
    ifs_total, ifs_fx = {}, {}
    try:
        ifs_docs_total = _fetch_ifs_indicator('RAFA_USD')
        ifs_docs_fx = _fetch_ifs_indicator('RAXGFX_USD')

        def _parse_ifs(docs):
            by_country = {}
            for doc in docs or []:
                code = doc.get('series_code', '')
                parts = code.split('.')
                iso2 = parts[1] if len(parts) >= 2 else ''
                if not iso2 or len(iso2) != 2 or not iso2.isalpha() or not iso2.isupper():
                    continue
                periods = doc.get('period', [])
                values = doc.get('value', [])
                if periods:
                    by_country[iso2] = dict(zip(periods, values))
            return by_country

        ifs_total = _parse_ifs(ifs_docs_total)
        ifs_fx = _parse_ifs(ifs_docs_fx)
        logger.info("DBnomics IFS loaded: total=%d, fx=%d countries", len(ifs_total), len(ifs_fx))
    except Exception as e:
        logger.warning(f"DBnomics IFS fetch failed: {e}")

    irfcl_total, irfcl_fx = {}, {}
    try:
        total_doc = _fetch_imf_data_indicator('RAFA_USD')
        fx_doc = _fetch_imf_data_indicator('RAFAFX_USD')
        if total_doc:
            irfcl_total = _parse_imf_sdmx_series(total_doc)
        if fx_doc:
            irfcl_fx = _parse_imf_sdmx_series(fx_doc)
        logger.info("IMF IRFCL loaded: total=%d, fx=%d countries", len(irfcl_total), len(irfcl_fx))
    except Exception as e:
        logger.warning(f"IMF IRFCL fetch failed: {e}")

    # ── Merge: IFS base + IRFCL overlay ─────────────────────────────
    # Normalize all country codes to ISO3 first, so IFS ('CN') and
    # IRFCL ('CHN') merge into one entry instead of creating duplicates.
    def _to_iso3(code):
        code = (code or '').strip().upper()
        if len(code) == 2:
            return ISO2_TO_ISO3.get(code)
        if len(code) == 3 and code.isalpha():
            return code
        return None

    def _normalize(by_country):
        out = {}
        for code, periods in by_country.items():
            iso3 = _to_iso3(code)
            if not iso3:
                continue
            if iso3 in out:
                out[iso3].update(periods)
            else:
                out[iso3] = dict(periods)
        return out

    ifs_total_n = _normalize(ifs_total)
    ifs_fx_n = _normalize(ifs_fx)
    irfcl_total_n = _normalize(irfcl_total)
    irfcl_fx_n = _normalize(irfcl_fx)

    all_iso3 = set(ifs_total_n) | set(irfcl_total_n)
    if not all_iso3:
        logger.warning("Both IFS and IRFCL returned no data — falling back to World Bank")
        return _fetch_reserves_wb()

    merged_total = {}
    merged_fx = {}

    for iso3 in all_iso3:
        # IFS first (dense history), then IRFCL overlay (current)
        periods_total = {}
        if iso3 in ifs_total_n:
            periods_total.update(ifs_total_n[iso3])
        if iso3 in irfcl_total_n:
            periods_total.update(irfcl_total_n[iso3])
        if periods_total:
            merged_total[iso3] = periods_total

        periods_fx = {}
        if iso3 in ifs_fx_n:
            periods_fx.update(ifs_fx_n[iso3])
        if iso3 in irfcl_fx_n:
            periods_fx.update(irfcl_fx_n[iso3])
        if periods_fx:
            merged_fx[iso3] = periods_fx

    source_parts = []
    if ifs_total:
        source_parts.append('IFS')
    if irfcl_total:
        source_parts.append('IRFCL')
    source_label = 'IMF ' + '+'.join(source_parts) + ' (merged)'

    result = _build_reserves_result(merged_total, merged_fx, source_label)
    if result:
        logger.info(
            "Merged reserves: %d months, %d countries, latest %s",
            len(result['years']), len(result['countries']), result['years'][-1],
        )
        return result

    logger.warning("Merged result empty — falling back to World Bank annual")
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

    # Use the same merged strategy as _fetch_reserves()
    result = _fetch_reserves()
    source = result.get('meta', {}).get('source') if result else None
    if source:
        attempts.append(f'Merged fetch succeeded: {source}')
    else:
        attempts.append('All sources failed')

    if result:
        # Store in cache AND persist to disk so ALL workers (not just the
        # one serving this request) see the fresh data immediately.
        with _cache._lock:
            _cache._data = result
            _cache._last_fetch = time.time()
            _cache._save_to_disk(result)

    # Surface a small sample of the last 3 periods + per-period coverage
    # so the /api/cofer/refresh endpoint can verify period order and
    # country coverage without scraping server logs.
    sample_periods = []
    stale_countries = []
    if result and result.get('years'):
        years = result['years']
        last3 = years[-3:]
        countries = result.get('countries') or []
        for p in last3:
            idx = years.index(p)
            coverage = sum(
                1 for c in countries
                if (c.get('total_reserves') or [None] * len(years))[idx] is not None
            )
            sample_periods.append({'period': p, 'coverage': coverage})

        # Report the top 15 countries' latest real reporting month so
        # we can see which big holders have stopped reporting recently
        # (their lines on the chart will show NAs from that month
        # forward).
        for c in countries[:15]:
            stale_countries.append({
                'iso3': c.get('iso3'),
                'name': c.get('name'),
                'latest_real_period': c.get('latest_real_period'),
            })

    return {
        'attempts': attempts,
        'source': source,
        'latest_period': (result.get('years') if result else [''])[-1] if result and result.get('years') else '',
        'period_range': (result or {}).get('meta', {}).get('period_range', ''),
        'country_count': len((result or {}).get('countries') or []),
        'sample_periods': sample_periods,
        'top15_latest_real': stale_countries,
        'today': _dt.date.today().isoformat(),
    }
