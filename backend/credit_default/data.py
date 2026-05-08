"""
Sovereign indicator harmonizer for the Credit Default model.

Pulls macro/external/fiscal indicators from the IMF WEO API and World Bank
WDI/IDS, aligns them by ISO-3 country code, and merges with the local
sovereign_debt.json (which already carries the shadow-debt estimate).

All upstream calls are cached on disk via the existing world_bank /
imf_weo modules — this layer just orchestrates and harmonizes.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Iterable, Optional

from backend.data_sources import imf_weo, world_bank
from backend.data_sources.sovereign_debt import get_sovereign_debt_data


# ── Indicator catalog ────────────────────────────────────────────────────
#
# Each entry: { source, code, kind, units, label }
#   source — 'WEO' or 'WB'
#   code   — vendor indicator code
#   kind   — 'higher_is_worse' or 'higher_is_better' (drives sign in scoring)
#   units  — short string for UI ('% GDP', 'months', '%', ...)
#   label  — human-readable name shown in the dashboard
#
# The list intentionally covers the seven indicators the user called out
# (debt/GDP, current account, reserves/imports = import cover, interest on
# debt, fiscal deficit, growth, inflation) plus a couple more that are
# standard in sovereign credit models.

INDICATORS: Dict[str, Dict] = {
    # ── External position ───────────────────────────────────────────────
    'current_account_pct_gdp': {
        'source': 'WEO', 'code': 'BCA_NGDPD',
        'kind': 'higher_is_better', 'units': '% GDP',
        'label': 'Current Account Balance',
        'tier': 1,
    },
    'reserves_to_imports_months': {
        # Computed: FI.RES.TOTL.MO is reserves in months of imports (already
        # the right ratio). Falls back to derived ratio if missing.
        'source': 'WB', 'code': 'FI.RES.TOTL.MO',
        'kind': 'higher_is_better', 'units': 'months',
        'label': 'Import Cover (Reserves)',
        'tier': 1,
    },
    'external_debt_pct_gni': {
        'source': 'WB', 'code': 'DT.DOD.DECT.GN.ZS',
        'kind': 'higher_is_worse', 'units': '% GNI',
        'label': 'External Debt / GNI',
        'tier': 1,
    },
    'short_term_debt_pct_reserves': {
        'source': 'WB', 'code': 'DT.DOD.DSTC.IR.ZS',
        'kind': 'higher_is_worse', 'units': '%',
        'label': 'ST External Debt / Reserves',
        'tier': 1,
    },

    # ── Public finances ─────────────────────────────────────────────────
    'gross_debt_pct_gdp': {
        'source': 'WEO', 'code': 'GGXWDG_NGDP',
        'kind': 'higher_is_worse', 'units': '% GDP',
        'label': 'Gross Government Debt',
        'tier': 1,
    },
    'fiscal_balance_pct_gdp': {
        'source': 'WEO', 'code': 'GGXCNL_NGDP',
        'kind': 'higher_is_better', 'units': '% GDP',
        'label': 'Fiscal Balance (Net Lending)',
        'tier': 1,
    },
    'interest_pct_revenue': {
        'source': 'WB', 'code': 'GC.XPN.INTP.RV.ZS',
        'kind': 'higher_is_worse', 'units': '% revenue',
        'label': 'Interest Payments / Revenue',
        'tier': 1,
    },
    'interest_pct_gdp': {
        # Derived later from interest_pct_revenue * (revenue/GDP) when both
        # available. For now we pull the WB series directly when present.
        'source': 'WB', 'code': 'GC.XPN.INTP.ZS',
        'kind': 'higher_is_worse', 'units': '% expense',
        'label': 'Interest Payments / Expense',
        'tier': 2,
    },

    # ── Real economy ────────────────────────────────────────────────────
    'real_gdp_growth': {
        'source': 'WEO', 'code': 'NGDP_RPCH',
        'kind': 'higher_is_better', 'units': '%',
        'label': 'Real GDP Growth',
        'tier': 1,
    },
    'inflation': {
        'source': 'WEO', 'code': 'PCPIPCH',
        'kind': 'higher_is_worse', 'units': '%',
        'label': 'CPI Inflation',
        'tier': 1,
    },
    'gdp_per_capita_ppp': {
        'source': 'WEO', 'code': 'PPPPC',
        'kind': 'higher_is_better', 'units': 'intl $',
        'label': 'GDP per capita (PPP)',
        'tier': 2,
    },
    'unemployment': {
        'source': 'WEO', 'code': 'LUR',
        'kind': 'higher_is_worse', 'units': '%',
        'label': 'Unemployment Rate',
        'tier': 2,
    },

    # ── Governance / institutions (World Bank WGI, -2.5 worst → +2.5 best) ─
    # WGI moved out of WDI into its own database (source=3) and the
    # indicator codes were prefixed with GOV_WGI_. The legacy short codes
    # 404 against the modern API; ``wb_source`` is forwarded to
    # world_bank.get_wb_data below.
    'rule_of_law': {
        'source': 'WB', 'code': 'GOV_WGI_RL.EST', 'wb_source': 3,
        'kind': 'higher_is_better', 'units': 'z',
        'label': 'Rule of Law',
        'tier': 2,
    },
    'control_of_corruption': {
        'source': 'WB', 'code': 'GOV_WGI_CC.EST', 'wb_source': 3,
        'kind': 'higher_is_better', 'units': 'z',
        'label': 'Control of Corruption',
        'tier': 2,
    },
    'govt_effectiveness': {
        'source': 'WB', 'code': 'GOV_WGI_GE.EST', 'wb_source': 3,
        'kind': 'higher_is_better', 'units': 'z',
        'label': 'Government Effectiveness',
        'tier': 2,
    },
    'regulatory_quality': {
        'source': 'WB', 'code': 'GOV_WGI_RQ.EST', 'wb_source': 3,
        'kind': 'higher_is_better', 'units': 'z',
        'label': 'Regulatory Quality',
        'tier': 2,
    },
    'political_stability': {
        'source': 'WB', 'code': 'GOV_WGI_PV.EST', 'wb_source': 3,
        'kind': 'higher_is_better', 'units': 'z',
        'label': 'Political Stability',
        'tier': 2,
    },
    'voice_accountability': {
        'source': 'WB', 'code': 'GOV_WGI_VA.EST', 'wb_source': 3,
        'kind': 'higher_is_better', 'units': 'z',
        'label': 'Voice & Accountability',
        'tier': 2,
    },
}


# ── Cache ────────────────────────────────────────────────────────────────
_cache: Dict[str, object] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 6 * 3600  # 6 hours: matches sovereign_debt cadence


def _latest_value(country_block: Dict) -> Optional[float]:
    """Return the most recent non-null value from a {year: value} block."""
    if not country_block:
        return None
    values = country_block.get('values') or {}
    if not values:
        return None
    # Years are strings — sort lexicographically for annual data ("2024"
    # > "2023") and slice off any quarterly suffix when present.
    try:
        latest_year = max(values.keys())
    except ValueError:
        return None
    return values.get(latest_year)


def _latest_period(country_block: Dict,
                   forecast_start_year: Optional[int] = None) -> Optional[str]:
    """Return the period label of the most recent non-null *actual*
    observation (e.g. ``"2024"`` for annual sources, ``"2024Q3"`` for
    QEDS quarterly). When ``forecast_start_year`` is provided, periods
    at or after that year are skipped — IMF WEO publishes projections
    out to ~+6 years, and surfacing them as freshness misleads the user
    into thinking 2031 numbers are real data."""
    if not country_block:
        return None
    values = country_block.get('values') or {}
    if not values:
        return None
    keys = sorted(values.keys(), reverse=True)
    if forecast_start_year is not None:
        for k in keys:
            try:
                yr = int(str(k)[:4])
            except (TypeError, ValueError):
                continue
            if yr < forecast_start_year:
                return k
    try:
        return keys[0]
    except IndexError:
        return None


def _annual_series(country_block: Dict) -> Dict[int, float]:
    """Return {int_year: float} for annual data; ignores quarterly suffixes."""
    out: Dict[int, float] = {}
    values = (country_block or {}).get('values') or {}
    for k, v in values.items():
        try:
            year = int(str(k)[:4])
            out[year] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def _fetch_indicator(name: str, meta: Dict) -> Dict[str, Optional[float]]:
    """Return {iso3: latest_value} for one indicator."""
    out: Dict[str, Optional[float]] = {}
    if meta['source'] == 'WEO':
        payload = imf_weo.get_weo_data(meta['code'])
    else:  # WB
        payload = world_bank.get_wb_data(meta['code'], source=meta.get('wb_source'))
    countries = (payload or {}).get('countries') or {}
    for iso3, block in countries.items():
        if not iso3 or len(iso3) != 3:
            continue
        out[iso3] = _latest_value(block)
    return out


def _fetch_indicator_periods(name: str, meta: Dict) -> Dict[str, str]:
    """Return {iso3: latest_period_label} so the dashboard can show
    indicator freshness. Period is annual ("2024") or quarterly
    ("2024Q3") depending on the upstream source. Values from years
    inside the WEO forecast horizon are excluded so IMF projections
    don't masquerade as fresh data."""
    out: Dict[str, str] = {}
    if meta['source'] == 'WEO':
        payload = imf_weo.get_weo_data(meta['code'])
    else:
        payload = world_bank.get_wb_data(meta['code'], source=meta.get('wb_source'))
    payload = payload or {}
    forecast_start = payload.get('forecast_start_year')
    try:
        forecast_start = int(forecast_start) if forecast_start else None
    except (TypeError, ValueError):
        forecast_start = None
    countries = payload.get('countries') or {}
    for iso3, block in countries.items():
        if not iso3 or len(iso3) != 3:
            continue
        period = _latest_period(block, forecast_start_year=forecast_start)
        if period:
            out[iso3] = period
    return out


def _fetch_indicator_history(name: str, meta: Dict) -> Dict[str, Dict[int, float]]:
    """Return {iso3: {year: value}} — full annual time series."""
    out: Dict[str, Dict[int, float]] = {}
    if meta['source'] == 'WEO':
        payload = imf_weo.get_weo_data(meta['code'])
    else:
        payload = world_bank.get_wb_data(meta['code'], source=meta.get('wb_source'))
    countries = (payload or {}).get('countries') or {}
    for iso3, block in countries.items():
        if not iso3 or len(iso3) != 3:
            continue
        series = _annual_series(block)
        if series:
            out[iso3] = series
    return out


def get_history_panel(years_back: int = 25):
    """Build a long pandas DataFrame: iso3 × year × indicator columns.

    Used by ``fit.py`` to assemble the training set for the logit / GBM
    fit. Returns ``None`` if pandas isn't installed (the dashboard path
    doesn't need this).

    The shadow-debt overlay is treated as a *current-year* indicator —
    sovereign_debt.json is a snapshot, not a panel. For pre-snapshot
    years we leave it as NaN; the fitter will impute or drop as needed.
    """
    try:
        import pandas as pd
    except ImportError:
        print('[credit_default.data] pandas not installed; skipping history panel')
        return None

    import time as _t
    current_year = _t.localtime().tm_year

    series_by_indicator: Dict[str, Dict[str, Dict[int, float]]] = {}
    for name, meta in INDICATORS.items():
        try:
            series_by_indicator[name] = _fetch_indicator_history(name, meta)
        except Exception as e:
            print(f'[credit_default.data] history fetch failed for {name}: {e}')
            series_by_indicator[name] = {}

    iso_universe = set()
    for series in series_by_indicator.values():
        iso_universe.update(series.keys())

    rows = []
    earliest = current_year - years_back
    for iso3 in sorted(iso_universe):
        for year in range(earliest, current_year + 1):
            row = {'iso3': iso3, 'year': year}
            for ind_name in INDICATORS:
                row[ind_name] = series_by_indicator.get(ind_name, {}).get(iso3, {}).get(year)
            rows.append(row)

    df = pd.DataFrame(rows)

    # Attach the shadow-debt overlay only to the latest year per country.
    debt = get_sovereign_debt_data() or {}
    debt_countries = debt.get('countries') or {}
    df['shadow_debt_gap_pp'] = None
    if debt_countries:
        snapshot_year = current_year  # treat overlay as "as-of latest year"
        for iso3, blk in debt_countries.items():
            mask = (df['iso3'] == iso3) & (df['year'] == snapshot_year)
            df.loc[mask, 'shadow_debt_gap_pp'] = blk.get('debt_gap_pp')

    return df


def get_history_panel_quarterly(years_back: int = 25):
    """Quarterly-grained history panel: 4 rows per (iso3, year) with
    annual indicator values forward-filled across each year's four
    quarters. The schema mirrors :func:`get_history_panel` plus a
    ``quarter`` column ∈ {1,2,3,4} and a ``period`` string ("2024Q3").

    Notes:

    * Most upstream sources (IMF WEO, WB WGI, WB WDI) publish annual
      data only, so within a single year the four quarter rows have
      identical feature values. The grain change is structural — it
      lets the labeler attach onset-quarter precision to defaults and
      gives us a place to hang any future quarterly-native series
      (CDS spreads, FX volatility) without a second refactor. With
      annual features alone the quarterly fit reduces to the annual
      fit at 4× cadence.
    * The shadow-debt overlay is still snapshot-only and attaches to
      the most recent (year, Q4) row.
    """
    df = get_history_panel(years_back)
    if df is None or df.empty:
        return df
    try:
        import pandas as pd
    except ImportError:
        return None
    quarters = pd.DataFrame({'quarter': [1, 2, 3, 4]})
    out = df.merge(quarters, how='cross')
    out['period'] = (
        out['year'].astype(str) + 'Q' + out['quarter'].astype(str)
    )
    # Snap shadow-debt to Q4 of the most recent year only — it's a
    # snapshot, not a quarterly time series.
    if 'shadow_debt_gap_pp' in out.columns:
        snapshot_mask = (out['year'] != out['year'].max()) | (out['quarter'] != 4)
        out.loc[snapshot_mask, 'shadow_debt_gap_pp'] = None
        out.loc[(out['year'] == out['year'].max()) & (out['quarter'] == 4),
                'shadow_debt_gap_pp'] = df.loc[df['year'] == df['year'].max(),
                                                'shadow_debt_gap_pp'].values
    return out.sort_values(['iso3', 'year', 'quarter']).reset_index(drop=True)


def _country_iso3_universe(per_indicator: Dict[str, Dict[str, float]]) -> Iterable[str]:
    universe = set()
    for series in per_indicator.values():
        universe.update(series.keys())
    return sorted(universe)


# Aggregates the upstream APIs surface alongside real sovereigns. The
# dashboard shows sovereigns, sub-sovereigns (HKG/MAC/PRI/GRL/WBG/XKX),
# and island countries that issue their own debt — everything below is
# filtered out. WB aggregate codes come from world_bank._AGGREGATE_CODES;
# the rest are IMF WEO regional groupings observed in the panel.
_AGGREGATE_BLOCKLIST = {
    # WB regional / income groupings
    'ARB','CEB','CSS','EAP','EAR','EAS','ECA','ECS','EMU','EUU','FCS','HIC','HPC',
    'IBD','IBT','IDA','IDB','IDX','INX','LAC','LCN','LDC','LIC','LMC','LMY','LTE',
    'MEA','MIC','MNA','NAC','OED','OSS','PRE','PSS','PST','SAS','SSA','SSF','SST',
    'TEA','TEC','TLA','TMN','TSA','TSS','UMC','WLD',
    # IMF WEO regional sub-aggregates seen on the panel
    'AFE','AFW','AS5','EDE','MAE','OAE',
}


# US / French / British dependencies that don't issue independent debt
# and aren't rated by the agencies as sovereigns. Kept separate from
# _AGGREGATE_BLOCKLIST since these are real ISO-3 codes for places, not
# regional aggregations. Aruba (ABW), Bermuda (BMU), Cayman (CYM),
# Curaçao (CUW) are agency-rated and stay; Hong Kong / Macao / Puerto
# Rico / Greenland / West Bank & Gaza / Kosovo are sub-sovereigns we
# also keep.
_NON_RATED_DEPENDENCY_BLOCKLIST = {
    'ASM',   # American Samoa (US territory)
    'GUM',   # Guam (US territory)
    'MNP',   # Northern Mariana Islands (US territory)
    'VIR',   # US Virgin Islands
    'NCL',   # New Caledonia (French collectivity)
    'PYF',   # French Polynesia (French collectivity)
    'MAF',   # Saint Martin (French collectivity)
    'BLM',   # Saint Barthélemy (French collectivity)
    'VGB',   # British Virgin Islands
    'TCA',   # Turks & Caicos
    'AIA',   # Anguilla
    'MSR',   # Montserrat
    'SXM',   # Sint Maarten (Dutch dependency, no separate rating)
    'BES',   # Bonaire/Sint Eustatius/Saba (Dutch special municipalities)
}


def _is_sovereign_iso(iso3: str) -> bool:
    """Drop continent/income/region aggregates and US/French/British
    dependencies that don't issue independent sovereign debt. Real
    sovereigns and sub-sovereigns the agencies actually rate
    (HKG, MAC, PRI, GRL, WBG, XKX, ABW, BMU, CYM, CUW, …) all pass."""
    if not iso3 or len(iso3) != 3:
        return False
    if iso3 in _AGGREGATE_BLOCKLIST:
        return False
    if iso3 in _NON_RATED_DEPENDENCY_BLOCKLIST:
        return False
    # IMF WEO regional groupings end in 'Q' (e.g. AFQ, APQ, EUQ, WHQ).
    if iso3.endswith('Q'):
        return False
    return True


def get_panel(force_refresh: bool = False) -> Dict:
    """Build the harmonized cross-section panel.

    Returns:
        {
          'as_of': iso8601 timestamp,
          'indicators': INDICATORS metadata,
          'countries': {
            iso3: {
              'name': str, 'region': str,
              'indicators': {indicator_name: float | None, ...},
              'shadow_debt': {
                'official_debt_gdp': float, 'estimated_debt_gdp': float,
                'debt_gap_pp': float, 'risk_tier': str
              } | None,
            },
            ...
          },
        }
    """
    with _cache_lock:
        cached = _cache.get('panel')
        cached_ts = _cache.get('panel_ts', 0)
    if cached and not force_refresh and (time.time() - cached_ts) < _CACHE_TTL:
        return cached

    # 1. Pull every indicator (cached upstream, so this is cheap on warm cache)
    per_indicator: Dict[str, Dict[str, Optional[float]]] = {}
    per_indicator_periods: Dict[str, Dict[str, str]] = {}
    for name, meta in INDICATORS.items():
        try:
            per_indicator[name] = _fetch_indicator(name, meta)
            per_indicator_periods[name] = _fetch_indicator_periods(name, meta)
        except Exception as e:
            print(f'[credit_default.data] failed to fetch {name}: {e}')
            per_indicator[name] = {}
            per_indicator_periods[name] = {}

    # 2. Sovereign-debt overlay (already on disk as static JSON)
    debt_payload = get_sovereign_debt_data() or {}
    debt_countries = debt_payload.get('countries') or {}

    # 3. Resolve country names from sovereign_debt or WEO payload
    name_lookup: Dict[str, str] = {iso: blk.get('name', iso) for iso, blk in debt_countries.items()}
    if not name_lookup:
        # Fall back to a WEO indicator that already loaded country labels.
        for series_payload_key in ('real_gdp_growth', 'inflation'):
            wp = imf_weo.get_weo_data(INDICATORS[series_payload_key]['code'])
            for iso3, blk in (wp.get('countries') or {}).items():
                name_lookup.setdefault(iso3, blk.get('name', iso3))
            if name_lookup:
                break

    region_lookup = {iso: blk.get('region', '') for iso, blk in debt_countries.items()}

    # 4. Assemble
    countries_out: Dict[str, Dict] = {}
    for iso3 in _country_iso3_universe(per_indicator):
        if not _is_sovereign_iso(iso3):
            continue
        ind_values = {name: per_indicator[name].get(iso3) for name in INDICATORS}

        debt_block = debt_countries.get(iso3)
        shadow = None
        if debt_block:
            shadow = {
                'official_debt_gdp': debt_block.get('official_debt_gdp'),
                'estimated_debt_gdp': debt_block.get('estimated_debt_gdp'),
                'debt_gap_pp': debt_block.get('debt_gap_pp'),
                'risk_tier': debt_block.get('risk_tier'),
                'wgi_avg': debt_block.get('wgi_avg'),
                'short_term_pct': debt_block.get('short_term_pct'),
                'reserve_coverage_pct': debt_block.get('reserve_coverage_pct'),
                'debt_service_pct_exports': debt_block.get('debt_service_pct_exports'),
            }

        ind_periods = {
            name: per_indicator_periods.get(name, {}).get(iso3)
            for name in INDICATORS
        }
        countries_out[iso3] = {
            'iso3': iso3,
            'name': name_lookup.get(iso3, iso3),
            'region': region_lookup.get(iso3, ''),
            'indicators': ind_values,
            'indicator_periods': ind_periods,
            'shadow_debt': shadow,
        }

    panel = {
        'as_of': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'indicators': INDICATORS,
        'countries': countries_out,
    }

    with _cache_lock:
        _cache['panel'] = panel
        _cache['panel_ts'] = time.time()

    return panel
