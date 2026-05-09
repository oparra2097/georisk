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

    # ── Research-driven feature additions ───────────────────────────
    # 1. Serial-default features (Reinhart-Rogoff 2009; Cantor-Packer
    #    1996): years since the country's last default onset and count
    #    of onsets in the past 25 years.
    # 2. 5y change in gross debt/GDP (Manasse-Roubini-Schimmelpfennig
    #    2003 + IMF MAC SRDSF): debt-build-up speed beats the level
    #    alone for short-horizon forecasting.
    try:
        from backend.credit_default import defaults as _cd_defaults
        years_since = _cd_defaults.years_since_last_default()
        default_counts = _cd_defaults.default_count_window(window_years=25)
        df['years_since_default'] = df.apply(
            lambda r: years_since.get((r['iso3'], int(r['year'])), 100),
            axis=1,
        )
        df['default_count_25y'] = df.apply(
            lambda r: default_counts.get((r['iso3'], int(r['year'])), 0),
            axis=1,
        )
    except Exception as e:
        print(f'[credit_default.data] serial-default features failed: {e}')

    if 'gross_debt_pct_gdp' in df.columns:
        df = df.sort_values(['iso3', 'year']).reset_index(drop=True)
        df['debt_chg_5y_pp'] = df.groupby('iso3')['gross_debt_pct_gdp'].diff(5)

    # Log GDP per capita PPP. Values span $500 to $130k (250x range);
    # raw standardization compresses both tails. Agencies (S&P, Moody's,
    # Fitch SRM) all use log(GDP/cap) so 1 unit ≈ 2.7x income ratio,
    # which behaves linearly on the rating ladder.
    if 'gdp_per_capita_ppp' in df.columns:
        import numpy as _np
        gdp_pos = df['gdp_per_capita_ppp'].clip(lower=100)  # guard nonpositive
        df['gdp_per_capita_ppp_log'] = _np.log(gdp_pos)

    # Fiscal-balance trajectory (3-year change). Captures
    # deterioration in primary balance even when the level itself
    # isn't yet alarming — Romania case: balance −2.4% (2022) →
    # −7.6% (2025) is a five-percentage-point swing in 3 years that
    # the snapshot fiscal_balance_pct_gdp doesn't see. NEGATIVE
    # change = worsening = higher risk.
    if 'fiscal_balance_pct_gdp' in df.columns:
        df = df.sort_values(['iso3', 'year']).reset_index(drop=True)
        df['fiscal_balance_chg_3y'] = df.groupby('iso3')['fiscal_balance_pct_gdp'].diff(3)

    # 1-year reserves-depletion (months-of-imports basis). Sri Lanka
    # 2021-2022 went from ~7 months of imports to <1 month before
    # default; the level alone (>1 month) didn't flag the speed of
    # the drop. NEGATIVE diff = depletion = worse risk.
    if 'reserves_to_imports_months' in df.columns:
        df = df.sort_values(['iso3', 'year']).reset_index(drop=True)
        df['reserves_chg_1y_pp'] = df.groupby('iso3')['reserves_to_imports_months'].diff(1)

    # Years since most-recent systemic banking crisis (L-V 2020,
    # extended). Reinhart-Rogoff: banking crisis precedes sovereign
    # default by 1-3 years on average (Asia 1997, GFC 2008, EM
    # twin-crises 2022). Capped at 25y; missing → no recent crisis.
    try:
        from backend.credit_default import banking_crises as _bc
        ysbc = _bc.years_since_banking_crisis()
        df['years_since_banking_crisis'] = df.apply(
            lambda r: ysbc.get((r['iso3'], int(r['year'])), 25),
            axis=1,
        )
    except Exception as e:
        print(f'[credit_default.data] banking-crisis feature failed: {e}')

    # IMF program status (latest snapshot, broadcast across years).
    # Numeric: 0=none, 1=on_track, 2=off_track, 3=arrears. Higher =
    # more credit stress; agencies use this directly as a near-
    # mechanical CCC trigger.
    try:
        from backend.credit_default import imf_programs as _imf
        prog = _imf.status_by_iso()
        df['imf_program_status'] = df['iso3'].map(prog).fillna(0).astype(float)
    except Exception as e:
        print(f'[credit_default.data] IMF program feature failed: {e}')

    # % FX-denominated central-government debt (latest snapshot).
    # Original-sin signal — distinguishes ARG (~70% FX) from BRA (~5%).
    # Treated as a slow-moving country characteristic; same value
    # across all panel years.
    try:
        from backend.credit_default import fx_debt as _fxd
        fx = _fxd.fx_debt_share()
        df['fx_debt_share'] = df['iso3'].map(fx).fillna(0.0).astype(float)
    except Exception as e:
        print(f'[credit_default.data] FX-debt-share feature failed: {e}')

    # 3. Terms-of-trade volatility (IMF PCTOT, Hilscher-Nosbusch 2010):
    #    5y rolling std-dev of log changes in commodity export-to-import
    #    price ratio. Best fundamental beyond debt and reserves for
    #    sovereign-spread explanatory power.
    # 4. REER overvaluation (BIS WS_EER, IMF SRDSF + Hilscher 2010):
    #    annual percent deviation of real-effective exchange rate from
    #    its trailing 10-year mean. Positive = currency over-valued.
    try:
        from backend.data_sources import imf_pctot
        tot_vol = imf_pctot.get_pctot_volatility_5y()
        df['tot_volatility_5y'] = df.apply(
            lambda r: tot_vol.get(r['iso3'], {}).get(int(r['year'])),
            axis=1,
        )
    except Exception as e:
        print(f'[credit_default.data] PCTOT terms-of-trade fetch failed: {e}')

    try:
        from backend.data_sources import bis_eer
        reer_ov = bis_eer.get_reer_overvaluation()
        df['reer_overvaluation_pct'] = df.apply(
            lambda r: reer_ov.get(r['iso3'], {}).get(int(r['year'])),
            axis=1,
        )
    except Exception as e:
        print(f'[credit_default.data] BIS REER fetch failed: {e}')

    # 5. Reserve-currency share — IMF COFER 2024Q4 share of allocated
    # global FX reserves held in each country's currency. Captures the
    # structural funding advantage of reserve-currency issuers
    # (USD, EUR, JPY, GBP, CHF, CAD, AUD, CNY) that historically have
    # never defaulted in our panel and shouldn't be flagged as
    # equivalent default risk to non-reserve emerging markets. Without
    # this feature the model penalises USA / Japan / eurozone for high
    # debt/GDP without crediting the offsetting structural advantage.
    df['reserve_currency_share'] = df['iso3'].map(_RESERVE_CURRENCY_SHARE).fillna(0.0)

    # 6. VIX as a global financial-stress regressor — Hilscher-Nosbusch
    # 2010 and Bussière-Fratzscher 2006: global risk-aversion lifts
    # sovereign default probabilities materially above what country
    # fundamentals alone predict (esp. for EM in 2008/2020). Same value
    # applies to every country in a given year.
    try:
        from backend.data_sources import vix_history
        vix_by_year = vix_history.get_vix_annual_mean()
        df['vix_annual'] = df['year'].map(vix_by_year).astype(float)
    except Exception as e:
        print(f'[credit_default.data] VIX fetch failed: {e}')

    # 7. External liquidity ratio (S&P-style proxy). True S&P ELR is
    # gross external financing needs ÷ (current-account receipts +
    # usable reserves); we don't have GDP-USD or CAR-USD, so we
    # combine the two stress signals we DO have into a single index:
    #   pressure = ST_debt_pct_reserves + max(0, -CA_pct_gdp)·10
    #   cushion  = max(1, reserves_to_imports_months)
    #   ELR      = pressure / cushion
    # Higher = more refinancing pressure relative to reserves.
    if {'short_term_debt_pct_reserves', 'current_account_pct_gdp',
            'reserves_to_imports_months'}.issubset(df.columns):
        st_pressure = df['short_term_debt_pct_reserves'].fillna(0.0)
        ca_def = (-df['current_account_pct_gdp']).clip(lower=0).fillna(0.0)
        cushion = df['reserves_to_imports_months'].clip(lower=1).fillna(1.0)
        df['external_liquidity_ratio'] = (st_pressure + ca_def * 10.0) / cushion

    # 8. Regional contagion — % of countries in the same region in an
    # active CRAG default spell that year. Captures Reinhart-Rogoff
    # 2009's clustering finding: defaults in nearby economies are a
    # leading indicator of own-country default risk independent of
    # macros (LatAm 1980s, Asia 1997, eurozone 2010).
    try:
        from backend.credit_default import defaults as _cd_def
        region_by_iso = _build_region_lookup()
        in_def_yrs = _cd_def.in_default_years_by_country(include_distress=False)
        # Pre-build {region: set(iso3)} and {(year, region): n_in_default}.
        region_pop: Dict[str, set] = {}
        for iso, reg in region_by_iso.items():
            if reg:
                region_pop.setdefault(reg, set()).add(iso)

        contagion_cache: Dict[Tuple[int, str], float] = {}

        def _contagion(iso3_, year_):
            reg = region_by_iso.get(iso3_)
            if not reg:
                return 0.0
            key = (int(year_), reg)
            if key in contagion_cache:
                return contagion_cache[key]
            pop = region_pop.get(reg) or set()
            if not pop:
                contagion_cache[key] = 0.0
                return 0.0
            in_def = sum(
                1 for o_iso in pop
                if int(year_) in in_def_yrs.get(o_iso, set())
            )
            rate = in_def / len(pop)
            contagion_cache[key] = rate
            return rate

        df['region_default_rate'] = df.apply(
            lambda r: _contagion(r['iso3'], r['year']),
            axis=1,
        )
    except Exception as e:
        print(f'[credit_default.data] regional-contagion build failed: {e}')

    return df


def _build_region_lookup() -> Dict[str, str]:
    """``{iso3: region}`` from the sovereign-debt overlay. Cached at
    module level so we don't re-read the JSON on every history-panel
    rebuild."""
    global _REGION_LOOKUP_CACHE
    cached = globals().get('_REGION_LOOKUP_CACHE')
    if cached is not None:
        return cached
    payload = get_sovereign_debt_data() or {}
    out = {iso: (blk.get('region') or '')
           for iso, blk in (payload.get('countries') or {}).items()}
    globals()['_REGION_LOOKUP_CACHE'] = out
    return out


_REGION_LOOKUP_CACHE: Optional[Dict[str, str]] = None


# IMF COFER 2024Q4 (released March 2025): allocated global FX reserves
# share by currency, percent. Each eurozone country is assigned the
# full EUR share since it benefits from collective reserve-currency
# status; in practice the GBM learns "share > 0 → near-zero PD" so
# the exact distribution among eurozone members doesn't matter much.
_RESERVE_CURRENCY_SHARE = {
    'USA': 57.4,                                   # USD
    'JPN': 5.8,                                    # JPY
    'GBR': 4.9,                                    # GBP
    'CHE': 0.2,                                    # CHF
    'CAN': 2.7,                                    # CAD
    'AUS': 2.2,                                    # AUD
    'CHN': 2.2,                                    # CNY (RMB)
    # ── Eurozone ──
    'DEU': 19.8, 'FRA': 19.8, 'ITA': 19.8, 'ESP': 19.8, 'NLD': 19.8,
    'BEL': 19.8, 'AUT': 19.8, 'FIN': 19.8, 'IRL': 19.8, 'PRT': 19.8,
    'GRC': 19.8, 'LUX': 19.8, 'EST': 19.8, 'LVA': 19.8, 'LTU': 19.8,
    'SVK': 19.8, 'SVN': 19.8, 'MLT': 19.8, 'CYP': 19.8, 'HRV': 19.8,
}


_QEDS_ST_DEBT_CODES = (
    ('DT.DOD.DSTC.CD.US', 22),     # SDDS — ~120 countries
    ('DT.DOD.DECT.CD.ST.US', 23),  # GDDS — ~50 fallbacks
)


def _fetch_qeds_quarterly_st_debt() -> Dict[str, Dict[str, float]]:
    """Pull quarterly short-term external debt (USD) from QEDS sources.
    Returns ``{iso3: {"2024Q3": value_usd, ...}}``. Falls back from
    SDDS (source 22) to GDDS (source 23) per country."""
    merged: Dict[str, Dict[str, float]] = {}
    for code, src in _QEDS_ST_DEBT_CODES:
        try:
            payload = world_bank.get_wb_data(code, source=src) or {}
        except Exception as e:
            print(f'[credit_default.data] QEDS fetch failed ({code} src={src}): {e}')
            continue
        for iso3, block in (payload.get('countries') or {}).items():
            if not iso3 or len(iso3) != 3:
                continue
            vals = (block or {}).get('values') or {}
            quarter_vals = {k: v for k, v in vals.items()
                            if isinstance(k, str) and 'Q' in k and v}
            if not quarter_vals:
                continue
            # Prefer the SDDS (first) source; only fill in countries
            # that didn't have any SDDS data.
            if iso3 not in merged:
                merged[iso3] = quarter_vals
    return merged


def get_history_panel_quarterly(years_back: int = 25):
    """Quarterly-grained history panel: 4 rows per (iso3, year). Where
    quarterly upstream data is available (currently QEDS short-term
    external debt for ~150 SDDS/GDDS subscribers), the indicator
    actually varies by quarter; annual-only series are forward-filled
    so the row schema stays uniform.

    Notes:

    * QEDS quarterly ST debt overrides ``short_term_debt_pct_reserves``
      where present. The numerator is the QEDS quarterly USD amount;
      the denominator is the latest annual reserves value carried
      forward across the four quarters of that year. Within-year
      variation in the ratio therefore reflects movements in ST debt,
      not reserves.
    * IMF WEO / WB WGI / WB WDI series remain annual; the four quarter
      rows of any single year share their values.
    * Shadow-debt overlay is snapshot-only; attaches to (latest year,
      Q4).
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

    # ── Overlay quarterly QEDS ST debt where available ───────────────
    # We need annual reserves (USD) to compute the ratio; pull from the
    # WB indicator already cached by the panel build.
    try:
        reserves_payload = world_bank.get_wb_data('FI.RES.TOTL.CD') or {}
    except Exception:
        reserves_payload = {}
    reserves_by_iso_year: Dict[str, Dict[int, float]] = {}
    for iso3, block in (reserves_payload.get('countries') or {}).items():
        if not iso3 or len(iso3) != 3:
            continue
        for k, v in ((block or {}).get('values') or {}).items():
            try:
                yr = int(str(k)[:4])
                reserves_by_iso_year.setdefault(iso3, {})[yr] = float(v)
            except (TypeError, ValueError):
                continue

    qeds_st_debt = _fetch_qeds_quarterly_st_debt()
    quarterly_overrides = 0
    for iso3, quarter_vals in qeds_st_debt.items():
        reserves_history = reserves_by_iso_year.get(iso3) or {}
        if not reserves_history:
            continue
        for period, st_debt_usd in quarter_vals.items():
            try:
                yr = int(period[:4])
                qtr = int(period[5])
            except (ValueError, IndexError):
                continue
            # Walk back up to 3 years to find a non-null reserve value;
            # WB's annual reserves often lags 1-2 years.
            reserves_usd = None
            for back in range(0, 4):
                v = reserves_history.get(yr - back)
                if v and v > 0:
                    reserves_usd = v
                    break
            if not reserves_usd:
                continue
            ratio_pct = float(st_debt_usd) / float(reserves_usd) * 100.0
            mask = (
                (out['iso3'] == iso3)
                & (out['year'] == yr)
                & (out['quarter'] == qtr)
            )
            if mask.any():
                out.loc[mask, 'short_term_debt_pct_reserves'] = ratio_pct
                quarterly_overrides += 1
    print(
        f'[credit_default.data] quarterly panel: applied {quarterly_overrides} '
        f'QEDS short_term_debt_pct_reserves overrides across '
        f'{len(qeds_st_debt)} countries'
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

    # Latest VIX annual mean — applied uniformly to every country at
    # scoring time. See get_history_panel for the cross-section.
    try:
        from backend.data_sources import vix_history
        vix_by_year = vix_history.get_vix_annual_mean()
        _vix_latest = float(vix_by_year[max(vix_by_year)]) if vix_by_year else 0.0
    except Exception as e:
        print(f'[credit_default.data] VIX latest fetch failed: {e}')
        _vix_latest = 0.0

    # Pre-compute latest-year values of features that are derived in
    # ``get_history_panel`` (debt_chg_5y_pp, tot_volatility_5y,
    # fiscal_balance_chg_3y, years_since_default, default_count_25y,
    # reer_overvaluation_pct). Without this, live scoring sees these
    # as None and the GBM imputes to the panel median — making
    # countries with deteriorating fiscal/debt trajectories like
    # Romania score the same as a flat-trajectory country.
    _derived_latest: Dict[str, Dict[str, float]] = {}
    try:
        _hist = get_history_panel()
        if _hist is not None and not _hist.empty:
            _derived_cols = [
                'debt_chg_5y_pp', 'tot_volatility_5y',
                'fiscal_balance_chg_3y', 'years_since_default',
                'default_count_25y', 'reer_overvaluation_pct',
                'gdp_per_capita_ppp_log', 'reserves_chg_1y_pp',
                'years_since_banking_crisis', 'imf_program_status',
                'fx_debt_share',
            ]
            _derived_cols = [c for c in _derived_cols if c in _hist.columns]
            for iso3, g in _hist.groupby('iso3'):
                gs = g.sort_values('year')
                latest_for_iso: Dict[str, float] = {}
                for col in _derived_cols:
                    s = gs[col].dropna()
                    if not s.empty:
                        latest_for_iso[col] = float(s.iloc[-1])
                if latest_for_iso:
                    _derived_latest[iso3] = latest_for_iso
    except Exception as e:
        print(f'[credit_default.data] derived-feature latest lookup failed: {e}')

    # Latest regional-contagion rate per country: share of regional
    # peers currently inside an active CRAG hard-default spell.
    _region_contagion_now: Dict[str, float] = {}
    try:
        from backend.credit_default import defaults as _cd_def_now
        cur_yr = time.localtime().tm_year
        in_def = _cd_def_now.in_default_years_by_country(include_distress=False)
        regions = _build_region_lookup()
        # {region: countries_in_region}
        region_pop: Dict[str, set] = {}
        for iso, reg in regions.items():
            if reg:
                region_pop.setdefault(reg, set()).add(iso)
        for iso, reg in regions.items():
            pop = region_pop.get(reg) or set()
            if not pop:
                continue
            n_def = sum(1 for o in pop if cur_yr in in_def.get(o, set()))
            _region_contagion_now[iso] = n_def / len(pop)
    except Exception as e:
        print(f'[credit_default.data] regional-contagion (live) failed: {e}')

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
        # Surface the reserve-currency share alongside the regular
        # indicators so the rating-model score path can apply its
        # logit-discount per country (USA / JPN / GBR / CHE / eurozone /
        # CAD / AUD / CNY).
        ind_values['reserve_currency_share'] = _RESERVE_CURRENCY_SHARE.get(iso3, 0.0)
        ind_values['vix_annual'] = _vix_latest
        ind_values['region_default_rate'] = _region_contagion_now.get(iso3, 0.0)
        # Derived features carried forward from the historical panel
        # (training set computes them; live cross-section also needs
        # them so the GBM doesn't impute to median).
        for col, val in (_derived_latest.get(iso3) or {}).items():
            ind_values[col] = val
        # Same ELR formula as get_history_panel, computed on the
        # live-cross-section indicator values.
        st_pr = ind_values.get('short_term_debt_pct_reserves') or 0.0
        ca_pg = ind_values.get('current_account_pct_gdp') or 0.0
        rim = ind_values.get('reserves_to_imports_months') or 1.0
        if rim < 1.0:
            rim = 1.0
        ind_values['external_liquidity_ratio'] = (
            st_pr + max(0.0, -ca_pg) * 10.0
        ) / rim
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
