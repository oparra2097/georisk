"""
Hybrid statistical model stack for commodity price forecasting.

Methodology
-----------
For each commodity we fit a two-stage hybrid:

  1. SARIMAX(1,0,1) on monthly log-returns of the commodity price, with
     exogenous regressors capturing that commodity's primary macro drivers
     (e.g. DXY and 10Y real yield for gold; oil for TTF gas; gold for silver).

  2. GARCH(1,1) on the SARIMAX residuals, to capture volatility clustering.
     This makes the posterior confidence band widen automatically during
     turbulent regimes (2022 gas crisis, 2024-26 cocoa spike).

Forecast generation
-------------------
We simulate 1000 future 12-month return paths. Exogenous drivers are held
at their last observed monthly level (a naive random walk on levels). For
each simulation we draw innovations from the fitted GARCH conditional
distribution, push them through the SARIMAX state to get log-returns,
compound to prices, and aggregate into four forward quarterly averages.

Outputs per commodity are {Q+1, Q+2, Q+3, Q+4} with median (p50), lower
95% (p2.5), upper 95% (p97.5). These map to the Base / Worst / Best case
scenario rows in the existing forecast API.

Nowcast
-------
Current-quarter estimate blends QTD actuals with the model's Q+0 median:
    nowcast = w * qtd_mean + (1 - w) * model_q0_median
    w = days_elapsed / days_in_quarter

Refit cadence
-------------
Fits are cached to disk and considered stale after 35 days. The scheduler
in backend/scheduler.py triggers monthly refits.
"""

from __future__ import annotations

import os
import json
import pickle
import logging
import calendar
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    from arch import arch_model
    _STATS_OK = True
except ImportError:
    _STATS_OK = False

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────

HISTORY_YEARS = 10
BOOTSTRAP_DRAWS = 1000
FORECAST_MONTHS = 12       # 4 quarters × 3 months
STALE_AFTER_DAYS = 35

# Per-commodity override for the training window. Metals with multi-decade
# structural trends (gold/silver/platinum/copper) fit on a longer window so
# the SARIMAX drift estimate isn't biased by short-term consolidation. The
# rest fall through to HISTORY_YEARS.
HISTORY_YEARS_OVERRIDE: dict[str, int] = {
    'Gold':     20,
    'Silver':   20,
    'Platinum': 15,
    'Copper':   20,
}

def _resolve_cache_dir() -> str:
    """Resolve where commodity-model pickles live.

    On Render the source tree is ephemeral — every deploy wipes it — so
    the previous default of `<repo>/backend/cache/commodity_models/` lost
    every fit on redeploy. Use Config.DATA_DIR (which Render mounts to a
    persistent disk at /data) when available; fall back to the source-
    tree path for local dev.

    Allow `COMMODITY_CACHE_DIR` env override for tests / one-off runs.
    """
    override = os.environ.get('COMMODITY_CACHE_DIR')
    if override:
        return override
    try:
        from config import Config as _Cfg
        data_dir = getattr(_Cfg, 'DATA_DIR', None)
        if data_dir:
            return os.path.join(data_dir, 'commodity_models')
    except Exception:
        pass
    # Local-dev fallback
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'cache',
        'commodity_models',
    )


CACHE_DIR = _resolve_cache_dir()

# yfinance tickers matching backend/data_sources/commodities_forecast.py
TICKERS = {
    'WTI Crude':        'CL=F',
    'Brent Crude':      'BZ=F',
    'Natural Gas (HH)': 'NG=F',
    'TTF Gas':          'TTF=F',
    'Cocoa':            'CC=F',
    'Wheat':            'ZW=F',
    'Soybeans':         'ZS=F',
    'Coffee':           'KC=F',
    'Copper':           'HG=F',
    'Gold':             'GC=F',
    'Silver':           'SI=F',
    'Platinum':         'PL=F',
    'Aluminum':         'ALI=F',
}

# Per-commodity driver configuration. Each driver is one of:
#   ('fred',   series_id)             FRED time series (monthly)
#   ('yf',     ticker)                yfinance close (any freq, resampled)
#   ('gpr',    None)                  Geopolitical Risk Index
#   ('comm',   commodity_name)        another commodity's price
#
# Proxies are used where a true driver has no reliable free API:
#   - OPEC spare capacity → oil price volatility (handled via GARCH)
#   - US crude inventory → absent; rely on SARIMAX AR component
#   - West Africa rainfall → seasonal dummy inside SARIMAX (period=12)
#   - China PMI → copper price itself as leading indicator
#   - LME stocks → absent; rely on AR component
#
DRIVERS: dict[str, list[tuple[str, str]]] = {
    'WTI Crude':        [('fred', 'DTWEXBGS'), ('gpr', ''), ('yf', '^GSPC')],
    'Brent Crude':      [('fred', 'DTWEXBGS'), ('gpr', ''), ('comm', 'WTI Crude')],
    'Natural Gas (HH)': [('fred', 'DTWEXBGS'), ('comm', 'WTI Crude'), ('yf', '^GSPC')],
    'TTF Gas':          [('fred', 'DTWEXBGS'), ('comm', 'Natural Gas (HH)'), ('gpr', '')],
    'Gold':             [('fred', 'DFII10'), ('fred', 'DTWEXBGS'), ('gpr', ''), ('yf', '^GSPC')],
    'Silver':           [('fred', 'DFII10'), ('fred', 'DTWEXBGS'), ('comm', 'Gold'), ('comm', 'Copper')],
    'Platinum':         [('fred', 'DTWEXBGS'), ('comm', 'Gold'), ('fred', 'DFII10')],
    'Copper':           [('fred', 'DTWEXBGS'), ('yf', '^GSPC'), ('fred', 'DFII10')],
    'Aluminum':         [('fred', 'DTWEXBGS'), ('comm', 'Copper'), ('comm', 'Natural Gas (HH)')],
    'Cocoa':            [('fred', 'DTWEXBGS'), ('yf', '^GSPC')],
    'Wheat':            [('fred', 'DTWEXBGS'), ('gpr', ''), ('comm', 'WTI Crude')],
    'Soybeans':         [('fred', 'DTWEXBGS'), ('comm', 'Wheat'), ('yf', '^GSPC')],
    'Coffee':           [('fred', 'DTWEXBGS'), ('yf', '^GSPC')],
}

# How each driver enters the SARIMAX design matrix
DRIVER_TRANSFORM = {
    'fred_DTWEXBGS': 'logret',   # dollar index — returns
    'fred_DFII10':   'diff',     # real yield level — first difference
    'fred_NAPM':     'diff',     # ISM — first difference
    'gpr_':          'loglevel', # GPR index — log of level
    'yf_^GSPC':      'logret',   # equities — returns
    'comm_':         'logret',   # other commodity price — returns
}


# ── Scenario shock catalogue ──────────────────────────────────────────────
#
# Per-commodity menu of "what-if" levers a user can apply on top of the base
# SARIMAX + GARCH forecast. Each shock declares an elasticity (price % per
# unit of shock magnitude) plus a per-quarter decay factor — so a one-time
# OPEC cut bites hardest in Q+1, half as much in Q+3, etc.
#
# Elasticities are anchored on published research and round-numbered for
# transparency. Operators should treat them as "starting point with
# institutional priors", not estimates from this codebase. Citations live
# in the per-commodity white papers under docs/commodities/.
#
# Schema:
#   id            — stable string used in API requests
#   name          — display label for the slider
#   unit          — informational ("mbpd", "%", "bp", "0-1")
#   min/max/step  — slider bounds
#   default       — slider position when nothing is set (always 0)
#   elasticity    — Δprice % per +1 unit of shock magnitude
#   decay_per_q   — multiplier each forward quarter (0.85 = -15% persistence/q)
#   note          — short tooltip for the UI

SHOCKS: dict[str, list[dict]] = {
    'WTI Crude': [
        {'id': 'opec_production', 'name': 'OPEC+ production change',
         'unit': 'mbpd', 'min': -3.0, 'max': 3.0, 'step': 0.25, 'default': 0,
         'elasticity': -0.07, 'decay_per_q': 0.85,
         'note': 'Negative = production cut → higher price. ~7% per mbpd short-run.'},
        {'id': 'spr_flow', 'name': 'SPR release / refill',
         'unit': 'mbpd', 'min': -1.0, 'max': 1.0, 'step': 0.1, 'default': 0,
         'elasticity': -0.04, 'decay_per_q': 0.70,
         'note': 'Positive = refill (bullish), negative = release (bearish).'},
        {'id': 'me_premium', 'name': 'Middle East risk premium',
         'unit': 'severity', 'min': 0.0, 'max': 1.0, 'step': 0.1, 'default': 0,
         'elasticity': 0.20, 'decay_per_q': 0.75,
         'note': '0 = no premium, 1 = sustained Hormuz disruption.'},
        {'id': 'demand_shock', 'name': 'Global demand shock',
         'unit': '% of demand', 'min': -3.0, 'max': 3.0, 'step': 0.25, 'default': 0,
         'elasticity': -0.08, 'decay_per_q': 0.80,
         'note': 'Negative = demand destruction (recession). ~8% per 1% demand.'},
    ],
    'Brent Crude': [
        {'id': 'opec_production', 'name': 'OPEC+ production change',
         'unit': 'mbpd', 'min': -3.0, 'max': 3.0, 'step': 0.25, 'default': 0,
         'elasticity': -0.075, 'decay_per_q': 0.85,
         'note': 'Brent is more sensitive than WTI to OPEC moves.'},
        {'id': 'me_premium', 'name': 'Middle East risk premium',
         'unit': 'severity', 'min': 0.0, 'max': 1.0, 'step': 0.1, 'default': 0,
         'elasticity': 0.25, 'decay_per_q': 0.75,
         'note': 'Brent carries the seaborne risk premium.'},
        {'id': 'china_demand', 'name': 'China demand shift',
         'unit': '% imports', 'min': -10.0, 'max': 10.0, 'step': 1.0, 'default': 0,
         'elasticity': -0.025, 'decay_per_q': 0.85,
         'note': 'Negative = Chinese refining slowdown / quota cut.'},
        {'id': 'demand_shock', 'name': 'Global demand shock',
         'unit': '% of demand', 'min': -3.0, 'max': 3.0, 'step': 0.25, 'default': 0,
         'elasticity': -0.085, 'decay_per_q': 0.80,
         'note': 'Recession risk transmits via Brent first.'},
    ],
    'Natural Gas (HH)': [
        {'id': 'lng_outage', 'name': 'LNG export disruption',
         'unit': 'Bcf/d', 'min': -3.0, 'max': 3.0, 'step': 0.25, 'default': 0,
         'elasticity': -0.04, 'decay_per_q': 0.65,
         'note': 'Outage frees gas for domestic market → bearish HH.'},
        {'id': 'hdd_anomaly', 'name': 'Winter HDD anomaly',
         'unit': '% of normal', 'min': -25.0, 'max': 25.0, 'step': 5.0, 'default': 0,
         'elasticity': 0.006, 'decay_per_q': 0.50,
         'note': 'Cold winter = more storage draw = higher price.'},
        {'id': 'production_change', 'name': 'US dry gas production',
         'unit': 'Bcf/d', 'min': -3.0, 'max': 3.0, 'step': 0.25, 'default': 0,
         'elasticity': -0.025, 'decay_per_q': 0.85,
         'note': 'Lagged response from Haynesville / Appalachia rigs.'},
    ],
    'TTF Gas': [
        {'id': 'norwegian_outage', 'name': 'Norwegian pipeline outage',
         'unit': 'Bcf/d', 'min': -2.0, 'max': 2.0, 'step': 0.25, 'default': 0,
         'elasticity': -0.10, 'decay_per_q': 0.65,
         'note': 'Norway is now ~50% of EU imports. Negative = outage.'},
        {'id': 'qatar_disruption', 'name': 'Qatar LNG disruption',
         'unit': 'severity', 'min': 0.0, 'max': 1.0, 'step': 0.1, 'default': 0,
         'elasticity': 0.30, 'decay_per_q': 0.70,
         'note': '0 = normal flows, 1 = sustained Hormuz / Ras Laffan strike.'},
        {'id': 'eu_storage', 'name': 'EU storage anomaly',
         'unit': '% of normal', 'min': -20.0, 'max': 20.0, 'step': 5.0, 'default': 0,
         'elasticity': -0.008, 'decay_per_q': 0.55,
         'note': 'Below-normal storage entering winter is bullish.'},
        {'id': 'russia_restart', 'name': 'Russian pipeline gas restart',
         'unit': '0-1', 'min': 0.0, 'max': 1.0, 'step': 0.1, 'default': 0,
         'elasticity': -0.30, 'decay_per_q': 0.85,
         'note': 'Highly hypothetical. 1 = full restoration of pre-2022 flows.'},
    ],
    'Gold': [
        {'id': 'real_rates', 'name': 'Real 10Y yield shift',
         'unit': 'bp', 'min': -100.0, 'max': 100.0, 'step': 10.0, 'default': 0,
         'elasticity': -0.0008, 'decay_per_q': 0.95,
         'note': '-10bp ≈ +0.8% gold. Primary macro anchor.'},
        {'id': 'cb_buying', 'name': 'Central bank purchases',
         'unit': 'tonnes/yr', 'min': -500.0, 'max': 1000.0, 'step': 100.0, 'default': 0,
         'elasticity': 0.00015, 'decay_per_q': 0.90,
         'note': '+1000 tonnes vs trend ≈ +15%. WGC data.'},
        {'id': 'usd_shock', 'name': 'USD index shift',
         'unit': '%', 'min': -10.0, 'max': 10.0, 'step': 1.0, 'default': 0,
         'elasticity': -0.007, 'decay_per_q': 0.85,
         'note': 'Inverse relationship; -1% USD ≈ +0.7% gold.'},
        {'id': 'risk_off', 'name': 'Risk-off intensity',
         'unit': 'severity', 'min': 0.0, 'max': 1.0, 'step': 0.1, 'default': 0,
         'elasticity': 0.12, 'decay_per_q': 0.65,
         'note': 'Episodic safe-haven flow; short half-life.'},
    ],
    'Silver': [
        {'id': 'real_rates', 'name': 'Real 10Y yield shift',
         'unit': 'bp', 'min': -100.0, 'max': 100.0, 'step': 10.0, 'default': 0,
         'elasticity': -0.0015, 'decay_per_q': 0.95,
         'note': 'Silver ~2x more sensitive than gold to real rates.'},
        {'id': 'solar_demand', 'name': 'Solar / industrial demand',
         'unit': '%', 'min': -20.0, 'max': 30.0, 'step': 5.0, 'default': 0,
         'elasticity': 0.005, 'decay_per_q': 0.90,
         'note': 'PV demand growth is a structural bid; +10% ≈ +5%.'},
        {'id': 'gold_silver_ratio', 'name': 'Gold/silver ratio target',
         'unit': 'ratio shift', 'min': -15.0, 'max': 15.0, 'step': 1.0, 'default': 0,
         'elasticity': -0.012, 'decay_per_q': 0.80,
         'note': 'Negative = ratio compresses (silver outperforms gold).'},
    ],
    'Platinum': [
        {'id': 'sa_supply_shock', 'name': 'South Africa supply shock',
         'unit': '%', 'min': -25.0, 'max': 10.0, 'step': 2.5, 'default': 0,
         'elasticity': -0.012, 'decay_per_q': 0.75,
         'note': 'Eskom load-shedding cut historically -10% supply → +12%.'},
        {'id': 'auto_production', 'name': 'Auto production change',
         'unit': '%', 'min': -15.0, 'max': 15.0, 'step': 2.5, 'default': 0,
         'elasticity': 0.005, 'decay_per_q': 0.85,
         'note': '~40% of platinum demand is autocatalysts.'},
        {'id': 'hydrogen_demand', 'name': 'Hydrogen economy uptake',
         'unit': '%', 'min': 0.0, 'max': 30.0, 'step': 5.0, 'default': 0,
         'elasticity': 0.002, 'decay_per_q': 0.95,
         'note': 'Long-dated tailwind; impact still small relative to autos.'},
    ],
    'Copper': [
        {'id': 'china_pmi', 'name': 'China manufacturing PMI shift',
         'unit': 'PMI pts', 'min': -3.0, 'max': 3.0, 'step': 0.5, 'default': 0,
         'elasticity': 0.020, 'decay_per_q': 0.80,
         'note': '+1 PMI point ≈ +2% copper. China is ~55% of demand.'},
        {'id': 'lme_stocks', 'name': 'LME stocks change',
         'unit': '%', 'min': -50.0, 'max': 50.0, 'step': 10.0, 'default': 0,
         'elasticity': -0.0015, 'decay_per_q': 0.65,
         'note': 'Stock builds bearish; tightness below 100kt is bullish.'},
        {'id': 'chile_supply', 'name': 'Chile / Peru supply shock',
         'unit': '%', 'min': -15.0, 'max': 5.0, 'step': 2.5, 'default': 0,
         'elasticity': -0.008, 'decay_per_q': 0.75,
         'note': 'Strikes, water rationing, ore-grade decline.'},
    ],
    'Aluminum': [
        {'id': 'china_capacity', 'name': 'China production cap shift',
         'unit': '%', 'min': -10.0, 'max': 10.0, 'step': 1.0, 'default': 0,
         'elasticity': -0.012, 'decay_per_q': 0.85,
         'note': 'China is ~60% of global supply. Yunnan hydro is the swing.'},
        {'id': 'eu_smelters', 'name': 'EU smelter closures',
         'unit': '% capacity', 'min': 0.0, 'max': 25.0, 'step': 2.5, 'default': 0,
         'elasticity': 0.008, 'decay_per_q': 0.85,
         'note': 'Power-cost squeeze drives chronic curtailment.'},
        {'id': 'power_shock', 'name': 'Energy / power price shock',
         'unit': '%', 'min': -30.0, 'max': 50.0, 'step': 10.0, 'default': 0,
         'elasticity': 0.0035, 'decay_per_q': 0.70,
         'note': 'Smelting is 30-40% energy cost.'},
    ],
    'Cocoa': [
        {'id': 'wa_harvest', 'name': 'West Africa harvest shortfall',
         'unit': '%', 'min': -25.0, 'max': 10.0, 'step': 2.5, 'default': 0,
         'elasticity': -0.040, 'decay_per_q': 0.80,
         'note': 'Ghana + CI = ~60% of supply. Inelastic, big elasticity.'},
        {'id': 'disease', 'name': 'Disease pressure (black pod / swollen shoot)',
         'unit': 'severity', 'min': 0.0, 'max': 1.0, 'step': 0.1, 'default': 0,
         'elasticity': 0.20, 'decay_per_q': 0.90,
         'note': 'Multi-year compounding effect on yield.'},
        {'id': 'demand_shift', 'name': 'Grindings / chocolate demand',
         'unit': '%', 'min': -10.0, 'max': 10.0, 'step': 1.0, 'default': 0,
         'elasticity': 0.015, 'decay_per_q': 0.80,
         'note': 'Pass-through to retail capped some downside in 2024-26.'},
    ],
    'Wheat': [
        {'id': 'black_sea', 'name': 'Black Sea export disruption',
         'unit': '% blockade', 'min': 0.0, 'max': 100.0, 'step': 10.0, 'default': 0,
         'elasticity': 0.0025, 'decay_per_q': 0.65,
         'note': '100% blockade ≈ +25% short-run.'},
        {'id': 'us_drought', 'name': 'US drought severity (PDSI shift)',
         'unit': 'PDSI', 'min': -3.0, 'max': 0.0, 'step': 0.5, 'default': 0,
         'elasticity': -0.06, 'decay_per_q': 0.70,
         'note': 'Negative PDSI = drier; HRW belt sensitive.'},
        {'id': 'export_ban', 'name': 'Major exporter ban (binary)',
         'unit': '0-1', 'min': 0.0, 'max': 1.0, 'step': 0.5, 'default': 0,
         'elasticity': 0.15, 'decay_per_q': 0.55,
         'note': 'Russia tax, India ban, etc. Blunt step-function.'},
        {'id': 'stocks_to_use', 'name': 'Stocks-to-use ratio shift',
         'unit': 'pp', 'min': -5.0, 'max': 5.0, 'step': 1.0, 'default': 0,
         'elasticity': -0.013, 'decay_per_q': 0.80,
         'note': 'WASDE; tighter S/U ratio is bullish.'},
    ],
    'Soybeans': [
        {'id': 'brazil_rainfall', 'name': 'Brazil rainfall anomaly',
         'unit': '%', 'min': -30.0, 'max': 20.0, 'step': 5.0, 'default': 0,
         'elasticity': -0.008, 'decay_per_q': 0.75,
         'note': 'Cerrado Jan-Mar planting window is decisive.'},
        {'id': 'china_imports', 'name': 'China imports shift',
         'unit': '%', 'min': -15.0, 'max': 15.0, 'step': 2.5, 'default': 0,
         'elasticity': 0.006, 'decay_per_q': 0.80,
         'note': 'China is ~60% of global trade.'},
        {'id': 'argentina_drought', 'name': 'Argentina drought',
         'unit': 'severity', 'min': 0.0, 'max': 1.0, 'step': 0.1, 'default': 0,
         'elasticity': 0.12, 'decay_per_q': 0.70,
         'note': 'Pampas summer rain; 2021-22 ENSO was severe.'},
    ],
    'Coffee': [
        {'id': 'brazil_frost', 'name': 'Brazil frost damage',
         'unit': '%', 'min': 0.0, 'max': 30.0, 'step': 2.5, 'default': 0,
         'elasticity': 0.020, 'decay_per_q': 0.75,
         'note': '2021 frost (third worst on record) drove the spike.'},
        {'id': 'vietnam_drought', 'name': 'Vietnam Robusta drought',
         'unit': '%', 'min': 0.0, 'max': 25.0, 'step': 2.5, 'default': 0,
         'elasticity': 0.010, 'decay_per_q': 0.80,
         'note': 'Central Highlands El Niño exposure.'},
        {'id': 'ico_shift', 'name': 'ICO indicator shift',
         'unit': '%', 'min': -20.0, 'max': 20.0, 'step': 2.5, 'default': 0,
         'elasticity': 0.008, 'decay_per_q': 0.85,
         'note': 'Composite proxy for global supply-demand balance.'},
    ],
}


def apply_driver_shifts(exog_future: Optional[pd.DataFrame],
                        shifts: Optional[dict]) -> Optional[pd.DataFrame]:
    """Add user-supplied shifts to the held-flat driver path.

    `shifts` is a {column_name: delta} dict. The delta is applied additively
    to every forecast month — appropriate for log-return drivers (DXY,
    equities, commodities), first-difference drivers (real yield), and
    log-level drivers (GPR). Unknown columns are ignored.
    """
    if not shifts or exog_future is None:
        return exog_future
    out = exog_future.copy()
    for col, delta in shifts.items():
        if col in out.columns:
            try:
                out[col] = out[col] + float(delta)
            except (TypeError, ValueError):
                continue
    return out


def apply_shocks(commodity: str,
                 shocks: Optional[list],
                 sim_prices: 'np.ndarray') -> 'np.ndarray':
    """Multiply simulated price paths by an elasticity-based shock factor.

    Each shock is `{id, magnitude}`. Elasticity is looked up in SHOCKS;
    impact decays geometrically per quarter. Multiple shocks compose
    multiplicatively. Unknown ids are skipped.
    """
    if not shocks:
        return sim_prices
    spec_lookup = {s['id']: s for s in SHOCKS.get(commodity, [])}
    n_months = sim_prices.shape[1]
    factor = np.ones(n_months)
    for shock in shocks:
        spec = spec_lookup.get(shock.get('id'))
        if not spec:
            continue
        try:
            magnitude = float(shock.get('magnitude', 0))
        except (TypeError, ValueError):
            continue
        if magnitude == 0:
            continue
        impact_pct = magnitude * spec['elasticity']
        decay = spec.get('decay_per_q', 0.85)
        for m in range(n_months):
            q_idx = m // 3
            factor[m] *= (1.0 + impact_pct * (decay ** q_idx))
    return sim_prices * factor[np.newaxis, :]


def get_shocks_catalogue(commodity: Optional[str] = None) -> dict:
    """Return the shock menu for a single commodity or all commodities.

    Used by the frontend to render the scenario builder sliders.
    """
    if commodity is not None:
        return {'commodity': commodity, 'shocks': SHOCKS.get(commodity, [])}
    return {'all': {name: SHOCKS.get(name, []) for name in TICKERS}}


# ── Driver fetchers ────────────────────────────────────────────────────────

class DriverFetcher:
    """Pulls monthly driver series, caches in-memory for a build."""

    def __init__(self):
        self._cache: dict[str, pd.Series] = {}

    def fetch(self, kind: str, key: str, start: date, end: Optional[date] = None) -> Optional[pd.Series]:
        cache_key = f'{kind}:{key}:{end.isoformat() if end else ""}'
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            if kind == 'fred':
                series = self._fetch_fred(key, start, end)
            elif kind == 'yf':
                series = self._fetch_yf(key, start, end)
            elif kind == 'gpr':
                series = self._fetch_gpr(start, end)
            elif kind == 'comm':
                series = self._fetch_yf(TICKERS[key], start, end)
            else:
                logger.warning(f'Unknown driver kind: {kind}')
                return None
        except Exception as e:
            logger.warning(f'Driver fetch {kind}:{key} failed: {e}')
            series = None

        self._cache[cache_key] = series
        return series

    @staticmethod
    def _fetch_yf(ticker: str, start: date, end: Optional[date] = None) -> Optional[pd.Series]:
        if yf is None:
            return None
        end_dt = (end or date.today()) + timedelta(days=1)
        data = yf.download(
            ticker,
            start=start.isoformat(),
            end=end_dt.isoformat(),
            auto_adjust=True,
            progress=False,
        )
        if data is None or data.empty:
            return None
        close = data['Close'] if 'Close' in data.columns else data.iloc[:, 0]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = close.dropna()
        if close.empty:
            return None
        monthly = close.resample('ME').mean()
        monthly.index = monthly.index.to_period('M').to_timestamp('M')
        if end is not None:
            monthly = monthly.loc[monthly.index <= pd.Timestamp(end)]
        return monthly

    @staticmethod
    def _fetch_fred(series_id: str, start: date, end: Optional[date] = None) -> Optional[pd.Series]:
        try:
            from backend.data_sources import fred_client
        except Exception:
            return None
        obs = fred_client.fetch_series(
            series_id,
            start_date=start.isoformat(),
            end_date=(end or date.today()).isoformat(),
        )
        if not obs:
            return None
        df = pd.DataFrame(obs)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()
        s = pd.to_numeric(df['value'], errors='coerce').dropna()
        monthly = s.resample('ME').mean()
        monthly.index = monthly.index.to_period('M').to_timestamp('M')
        if end is not None:
            monthly = monthly.loc[monthly.index <= pd.Timestamp(end)]
        return monthly

    @staticmethod
    def _fetch_gpr(start: date, end: Optional[date] = None) -> Optional[pd.Series]:
        try:
            from backend.data_sources import gpr_index
            data = gpr_index.fetch_gpr_data()
        except Exception:
            return None
        if not data:
            return None
        # gpr_index stores monthly world GPR under 'world' or similar;
        # fall back to the first available global series.
        series = None
        if isinstance(data, dict):
            for key in ('world', 'GPR', 'global', 'aggregate'):
                if key in data:
                    series = data[key]
                    break
            if series is None:
                # Average all country series as an aggregate proxy
                try:
                    df = pd.DataFrame(data)
                    series = df.mean(axis=1)
                except Exception:
                    return None
        if series is None:
            return None
        try:
            s = pd.Series(series)
            if not isinstance(s.index, pd.DatetimeIndex):
                s.index = pd.to_datetime(s.index)
            s = s.sort_index()
            monthly = s.resample('ME').mean()
            monthly.index = monthly.index.to_period('M').to_timestamp('M')
            monthly = monthly.loc[monthly.index >= pd.Timestamp(start)]
            if end is not None:
                monthly = monthly.loc[monthly.index <= pd.Timestamp(end)]
            return monthly
        except Exception:
            return None


def _transform(series: pd.Series, kind: str) -> pd.Series:
    if kind == 'logret':
        return np.log(series.replace(0, np.nan)).diff()
    if kind == 'diff':
        return series.diff()
    if kind == 'loglevel':
        s = series.replace(0, np.nan)
        return np.log(s)
    return series


# ── CommodityModel ────────────────────────────────────────────────────────

class CommodityModel:
    """SARIMAX(1,0,1) + GARCH(1,1) hybrid with 95% CI bootstrap."""

    def __init__(self, name: str):
        if name not in TICKERS:
            raise ValueError(f'Unknown commodity: {name}')
        self.name = name
        self.ticker = TICKERS[name]
        self.driver_spec = DRIVERS.get(name, [])

        self.price_monthly: Optional[pd.Series] = None
        self.last_price: Optional[float] = None
        self.exog_monthly: Optional[pd.DataFrame] = None
        self.sarimax_res = None
        self.garch_res = None
        self.residuals: Optional[pd.Series] = None
        self.n_obs: Optional[int] = None
        self.rmse: Optional[float] = None
        self.fit_at: Optional[datetime] = None
        self.fit_error: Optional[str] = None
        self.as_of: Optional[date] = None   # endpoint used for fit (backtest pivots)

    # ── fit ────────────────────────────────────────────────────────────

    def fit(self, fetcher: Optional[DriverFetcher] = None, as_of: Optional[date] = None) -> bool:
        """Fit on monthly data up to and including ``as_of`` (default today).

        ``as_of`` lets walk-forward backtests refit the model at historical
        pivots without touching the global "now" state.
        """
        if not _STATS_OK:
            self.fit_error = 'statsmodels/arch not installed'
            return False
        if yf is None:
            self.fit_error = 'yfinance not installed'
            return False

        fetcher = fetcher or DriverFetcher()
        self.as_of = as_of
        end = as_of
        anchor = as_of or date.today()
        history_years = HISTORY_YEARS_OVERRIDE.get(self.name, HISTORY_YEARS)
        start = anchor - timedelta(days=365 * history_years)

        price = DriverFetcher._fetch_yf(self.ticker, start, end)
        if price is None or len(price) < 36:
            self.fit_error = f'Insufficient price history ({0 if price is None else len(price)} months)'
            return False
        self.price_monthly = price
        self.last_price = float(price.iloc[-1])

        y = _transform(price, 'logret').dropna()
        exog = self._build_exog(fetcher, start, end)
        if exog is not None:
            idx = y.index.intersection(exog.index)
            y = y.loc[idx]
            exog = exog.loc[idx]

        if len(y) < 36:
            self.fit_error = f'Insufficient post-align history ({len(y)} months)'
            return False

        try:
            sarimax = SARIMAX(
                y,
                exog=exog,
                order=(1, 0, 1),
                seasonal_order=(0, 0, 0, 0),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            self.sarimax_res = sarimax.fit(disp=False, maxiter=200)
        except Exception as e:
            logger.warning(f'SARIMAX fit failed for {self.name}: {e}. Retrying without exog.')
            try:
                sarimax = SARIMAX(
                    y, order=(1, 0, 1),
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                )
                self.sarimax_res = sarimax.fit(disp=False, maxiter=200)
                exog = None
            except Exception as e2:
                self.fit_error = f'SARIMAX failed: {e2}'
                return False

        self.exog_monthly = exog
        self.residuals = pd.Series(self.sarimax_res.resid, index=y.index).dropna()
        self.n_obs = int(len(y))
        self.rmse = float(np.sqrt(np.mean(self.residuals ** 2)))

        try:
            garch = arch_model(
                self.residuals * 100,  # scale for numerical stability
                mean='Zero', vol='Garch', p=1, q=1, dist='normal',
                rescale=False,
            )
            self.garch_res = garch.fit(disp='off', show_warning=False)
        except Exception as e:
            logger.warning(f'GARCH fit failed for {self.name}: {e}. Using empirical residuals.')
            self.garch_res = None

        self.fit_at = datetime.utcnow()
        self.fit_error = None
        logger.info(
            f'Fit {self.name}: n={self.n_obs}, rmse={self.rmse:.4f}, '
            f'exog={list(exog.columns) if exog is not None else "none"}, '
            f'garch={"yes" if self.garch_res is not None else "no"}'
        )
        return True

    def _build_exog(self, fetcher: DriverFetcher, start: date,
                    end: Optional[date] = None) -> Optional[pd.DataFrame]:
        cols: dict[str, pd.Series] = {}
        for kind, key in self.driver_spec:
            raw = fetcher.fetch(kind, key, start, end)
            if raw is None or len(raw) < 24:
                continue
            transform_key = f'{kind}_{key}' if kind not in ('comm',) else 'comm_'
            transform = DRIVER_TRANSFORM.get(transform_key, 'logret')
            transformed = _transform(raw, transform).dropna()
            if len(transformed) < 24:
                continue
            col_name = f'{kind}:{key}' if key else kind
            cols[col_name] = transformed
        if not cols:
            return None
        df = pd.DataFrame(cols).dropna()
        return df if len(df) >= 24 else None

    # ── forecast ────────────────────────────────────────────────────────

    def forecast(self, h: int = 4, draws: int = BOOTSTRAP_DRAWS,
                 driver_shifts: Optional[dict] = None,
                 shocks: Optional[list] = None) -> dict:
        """Forecast h forward quarterly averages with 95% CIs.

        Optional overrides for interactive scenario building:

        * ``driver_shifts`` — ``{column_name: delta}`` added to the held-flat
          forecast driver path before SARIMAX consumes it. Use the column
          names exposed by ``self.exog_monthly.columns`` (e.g. ``'fred:DXY'``).
        * ``shocks`` — list of ``{id, magnitude}`` dicts looked up against
          the per-commodity ``SHOCKS`` catalogue. Each shock applies an
          elasticity-based price multiplier with geometric per-quarter decay,
          composed multiplicatively across shocks.
        """
        if self.sarimax_res is None or self.price_monthly is None:
            return {}

        rng = np.random.default_rng(seed=42)
        n_months = h * 3

        # Deterministic mean forecast (exog held at last value, then user shifts)
        exog_future = None
        if self.exog_monthly is not None and len(self.exog_monthly) > 0:
            last = self.exog_monthly.iloc[-1]
            exog_future = pd.DataFrame(
                np.tile(last.values, (n_months, 1)),
                columns=self.exog_monthly.columns,
            )
        exog_future = apply_driver_shifts(exog_future, driver_shifts)

        try:
            mean_fc = self.sarimax_res.get_forecast(steps=n_months, exog=exog_future)
            mean_returns = np.asarray(mean_fc.predicted_mean)
        except Exception as e:
            logger.warning(f'Forecast mean failed for {self.name}: {e}')
            return {}

        # Innovation draws for each simulation: GARCH-conditional if available,
        # otherwise bootstrap from empirical residuals.
        if self.garch_res is not None:
            try:
                sims = self.garch_res.forecast(
                    horizon=n_months, reindex=False, method='simulation',
                    simulations=draws,
                )
                innov = sims.simulations.values[0] / 100.0  # back to return scale
            except Exception as e:
                logger.warning(f'GARCH simulate fell back to empirical: {e}')
                innov = rng.choice(self.residuals.values, size=(draws, n_months), replace=True)
        else:
            innov = rng.choice(self.residuals.values, size=(draws, n_months), replace=True)

        # Combine: each sim = mean_returns + innovation path
        sim_returns = mean_returns[np.newaxis, :] + innov  # (draws, n_months)
        sim_log_prices = np.cumsum(sim_returns, axis=1) + np.log(self.last_price)
        sim_prices = np.exp(sim_log_prices)

        # Layer shock-elasticity overlay on top of the simulated paths.
        sim_prices = apply_shocks(self.name, shocks, sim_prices)

        # Aggregate into quarterly averages
        result = {}
        anchor = self.as_of or date.today()
        current_q = (anchor.month - 1) // 3 + 1
        next_q_num = current_q + 1
        next_q_year = anchor.year
        if next_q_num > 4:
            next_q_num -= 4
            next_q_year += 1

        for q_idx in range(h):
            cols = slice(q_idx * 3, (q_idx + 1) * 3)
            q_avg = sim_prices[:, cols].mean(axis=1)
            label = self._q_label(next_q_num + q_idx, next_q_year)
            result[f'Q+{q_idx + 1}'] = {
                'label': label,
                'median': float(np.median(q_avg)),
                'p2_5':   float(np.percentile(q_avg, 2.5)),
                'p10':    float(np.percentile(q_avg, 10)),
                'p90':    float(np.percentile(q_avg, 90)),
                'p97_5':  float(np.percentile(q_avg, 97.5)),
            }
        return result

    @staticmethod
    def _q_label(q_num: int, year: int) -> str:
        while q_num > 4:
            q_num -= 4
            year += 1
        return f'Q{q_num} {year}'

    # ── nowcast ─────────────────────────────────────────────────────────

    def nowcast(self, qtd_mean: Optional[float], days_elapsed: int, days_in_quarter: int) -> Optional[float]:
        if self.sarimax_res is None or self.price_monthly is None or qtd_mean is None:
            return qtd_mean
        w = max(0.0, min(1.0, days_elapsed / max(1, days_in_quarter)))
        # Model's 1-month-ahead mean forecast as a proxy for the current quarter
        try:
            exog_future = None
            if self.exog_monthly is not None and len(self.exog_monthly) > 0:
                exog_future = pd.DataFrame(
                    [self.exog_monthly.iloc[-1].values],
                    columns=self.exog_monthly.columns,
                )
            mean_fc = self.sarimax_res.get_forecast(steps=1, exog=exog_future)
            mean_return = float(mean_fc.predicted_mean.iloc[0])
            model_q0 = float(self.last_price) * float(np.exp(mean_return))
        except Exception:
            model_q0 = float(self.last_price)
        return w * qtd_mean + (1.0 - w) * model_q0

    # ── serialization ───────────────────────────────────────────────────

    def summary(self) -> dict:
        return {
            'name': self.name,
            'ticker': self.ticker,
            'drivers': [f'{k}:{v}' if v else k for k, v in self.driver_spec],
            'n_obs': self.n_obs,
            'rmse': self.rmse,
            'fit_at': self.fit_at.isoformat() if self.fit_at else None,
            'fit_error': self.fit_error,
            'last_price': self.last_price,
            'garch': self.garch_res is not None,
        }

    def save(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, path: str) -> Optional['CommodityModel']:
        try:
            with open(path, 'rb') as f:
                obj = pickle.load(f)
            return obj if isinstance(obj, cls) else None
        except Exception:
            return None


# ── Storage layer ──────────────────────────────────────────────────────────

def _safe_name(name: str) -> str:
    return name.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')


def _cache_path(name: str, cache_dir: str = CACHE_DIR) -> str:
    return os.path.join(cache_dir, f'{_safe_name(name)}.pkl')


def _sidecar_path(name: str, cache_dir: str = CACHE_DIR) -> str:
    return os.path.join(cache_dir, f'{_safe_name(name)}.json')


def _manifest_path(cache_dir: str = CACHE_DIR) -> str:
    return os.path.join(cache_dir, 'manifest.json')


def _write_sidecar(model: 'CommodityModel', cache_dir: str = CACHE_DIR) -> None:
    """Write a human-readable JSON summary + latest forecast alongside the pickle."""
    try:
        payload = {
            'summary': model.summary(),
            'forecast': model.forecast(h=4),
            'written_at': datetime.utcnow().isoformat(),
        }
        os.makedirs(cache_dir, exist_ok=True)
        with open(_sidecar_path(model.name, cache_dir), 'w') as f:
            json.dump(payload, f, indent=2, default=str)
    except Exception as e:
        logger.warning(f'{model.name}: sidecar write failed: {e}')


def _update_manifest(summaries: dict, cache_dir: str = CACHE_DIR) -> None:
    """Maintain a single manifest.json enumerating all cached fits."""
    try:
        os.makedirs(cache_dir, exist_ok=True)
        manifest = {
            'updated_at': datetime.utcnow().isoformat(),
            'stale_after_days': STALE_AFTER_DAYS,
            'cache_dir': cache_dir,
            'models': summaries,
        }
        with open(_manifest_path(cache_dir), 'w') as f:
            json.dump(manifest, f, indent=2, default=str)
    except Exception as e:
        logger.warning(f'manifest write failed: {e}')


def load_cached(name: str, cache_dir: str = CACHE_DIR) -> Optional[CommodityModel]:
    path = _cache_path(name, cache_dir)
    if not os.path.exists(path):
        return None
    model = CommodityModel.load(path)
    if model is None or model.fit_at is None:
        return None
    age = datetime.utcnow() - model.fit_at
    if age > timedelta(days=STALE_AFTER_DAYS):
        logger.info(f'{name}: cached fit is {age.days}d old (> {STALE_AFTER_DAYS}d), treating as stale')
        return None
    return model


def fit_and_cache(name: str, cache_dir: str = CACHE_DIR) -> Optional[CommodityModel]:
    model = CommodityModel(name)
    if not model.fit():
        logger.warning(f'{name}: fit failed ({model.fit_error})')
        return None
    try:
        model.save(_cache_path(name, cache_dir))
        _write_sidecar(model, cache_dir)
    except Exception as e:
        logger.warning(f'{name}: save failed: {e}')
    return model


def get_or_fit(name: str) -> Optional[CommodityModel]:
    return load_cached(name) or fit_and_cache(name)


def list_cached(cache_dir: str = CACHE_DIR) -> list[dict]:
    """Inspect what's currently on disk without deserializing the pickles."""
    if not os.path.isdir(cache_dir):
        return []
    out = []
    for fname in sorted(os.listdir(cache_dir)):
        if not fname.endswith('.json') or fname == 'manifest.json':
            continue
        try:
            with open(os.path.join(cache_dir, fname)) as f:
                out.append(json.load(f).get('summary', {}))
        except Exception:
            continue
    return out


def refit_all(cache_dir: str = CACHE_DIR) -> dict[str, dict]:
    """Monthly scheduler entry point. Refit every known commodity."""
    os.makedirs(cache_dir, exist_ok=True)
    summaries: dict[str, dict] = {}
    fetcher = DriverFetcher()  # share across all fits
    for name in TICKERS:
        try:
            model = CommodityModel(name)
            ok = model.fit(fetcher=fetcher)
            if ok:
                model.save(_cache_path(name, cache_dir))
                _write_sidecar(model, cache_dir)
            summaries[name] = model.summary()
        except Exception as e:
            logger.error(f'{name}: refit crashed: {e}')
            summaries[name] = {'name': name, 'fit_error': str(e)}
    _update_manifest(summaries, cache_dir)
    return summaries


def get_model_forecast(
    name: str,
    qtd_mean: Optional[float] = None,
    days_elapsed: int = 0,
    days_in_quarter: int = 90,
    driver_shifts: Optional[dict] = None,
    shocks: Optional[list] = None,
    use_forward_curve: bool = True,
    use_long_run_trend: bool = True,
) -> Optional[dict]:
    """Public entry used by commodities_forecast.py integration.

    ``driver_shifts`` and ``shocks`` are passed through to
    ``CommodityModel.forecast``; see that docstring for the schema.

    If ``use_forward_curve`` is True (default) and the commodity has a
    forward-curve adapter wired in ``forward_curve.CURVE_SPECS`` (currently
    WTI / Brent / HH / TTF), the model's central tendency for each
    forecast quarter is shrunk toward the curve-implied price using
    horizon-weighted blending.

    If ``use_long_run_trend`` is True (default) and the commodity has a
    trend-anchor spec in ``long_run_trend.TREND_ANCHOR_SPECS`` (currently
    Gold / Silver / Platinum / Copper), the model median is additionally
    shrunk toward a multi-decade CAGR trend line — so e.g. gold keeps
    continuing its historical upward drift after a near-term dip instead
    of extrapolating a flat local slope indefinitely.

    Confidence bands widen to encompass both the model envelope and each
    active anchor. The unmodified model output is preserved alongside
    under ``model_only``; each anchor is exposed under ``forward_curve``
    / ``long_run_trend`` for transparency and frontend overlay.
    """
    model = get_or_fit(name)
    if model is None:
        return None
    forecast = model.forecast(h=4, driver_shifts=driver_shifts, shocks=shocks)
    if not forecast:
        return None
    nowcast_val = model.nowcast(qtd_mean, days_elapsed, days_in_quarter) if qtd_mean is not None else None

    forward_curve = None
    forecast_anchored = forecast
    if use_forward_curve:
        try:
            from backend.data_sources import forward_curve as fc_mod
            forward_curve = fc_mod.fetch_curve_quarterly(name, h=4)
            if forward_curve:
                forecast_anchored = fc_mod.shrink_to_curve(forecast_anchored, forward_curve)
        except Exception as exc:
            logger.warning(f'{name}: forward-curve anchor skipped ({exc})')

    long_run_trend = None
    if use_long_run_trend:
        try:
            from backend.data_sources import long_run_trend as lrt_mod
            long_run_trend = lrt_mod.fetch_long_run_trend(name, h=4)
            if long_run_trend:
                forecast_anchored = lrt_mod.shrink_to_trend(forecast_anchored, long_run_trend)
        except Exception as exc:
            logger.warning(f'{name}: long-run trend anchor skipped ({exc})')

    return {
        'forecast': forecast_anchored,
        'model_only': forecast,
        'forward_curve': forward_curve,
        'long_run_trend': long_run_trend,
        'nowcast': nowcast_val,
        'summary': model.summary(),
        'exog_columns': list(model.exog_monthly.columns) if model.exog_monthly is not None else [],
    }


# ── Smoke test ─────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')
    for commodity in ('Gold', 'WTI Crude'):
        print(f'\n=== {commodity} ===')
        model = CommodityModel(commodity)
        if model.fit():
            print('summary:', model.summary())
            print('forecast:', model.forecast(h=4))
        else:
            print('fit failed:', model.fit_error)
