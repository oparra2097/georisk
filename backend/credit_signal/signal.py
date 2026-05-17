"""Core signal computation.

Pipeline:
    1. Sovereign baseline PD     <- agency ratings + Moody's transition matrix
    2. Sovereign commodity shock <- net export exposure x commodity forecast
    3. Sovereign adjusted PD     <- baseline x shock multiplier
    4. Sector adjusted PD        <- sovereign adjusted PD x sector beta multiplier
    5. Signal label              <- {AVOID, CAUTION, NEUTRAL, STRATEGIC}

The two heuristic constants below convert physical units (% of GDP shock,
PD-notch shift) into PD multipliers. Tune to your house view, document the
override, and keep the methodology sheet honest.
"""

import numpy as np
import pandas as pd

from . import transition_matrix as tm

# Sovereign: 1pp of GDP terms-of-trade improvement reduces PD by 0.5%.
# A Brazil-sized soy exporter (~3.5% of GDP) facing a +20% soy forecast
# would see ~70bp of GDP gain -> ~35% PD scaling factor reduction.
SOVEREIGN_PD_BETA = 0.5

# Sector: 1 notch of commodity-implied shift multiplies/divides PD by 1.5.
# Two notches of deterioration roughly doubles PD; symmetric on the upside.
SECTOR_PD_NOTCH_MULTIPLIER = 1.5

# Caps so a single extreme forecast can't drive PD to zero or infinity.
PD_MULT_FLOOR = 0.10
PD_MULT_CEIL = 10.0


def compute_sovereign_signal(ratings_df, exposure_df, forecast_pct,
                             horizon=1, matrix=None):
    """One row per sovereign.

    Returns columns: iso3, country, broad_rating, baseline_pd,
    tot_shock_pct_gdp, adjusted_pd, pd_change_pct, horizon_years, signal.
    """
    M = matrix if matrix is not None else tm.load_matrix()
    pd_table = tm.pd_at_horizon(M, horizon)

    merged = ratings_df.merge(exposure_df, on='iso3', how='inner')
    commodity_cols = [c for c in exposure_df.columns
                      if c not in ('iso3', 'country')]

    rows = []
    for _, r in merged.iterrows():
        baseline = pd_table.get(r['broad_rating'], np.nan)
        if pd.isna(baseline):
            continue
        shock = 0.0
        for c in commodity_cols:
            exp = r.get(c, 0.0)
            chg = forecast_pct.get(c, 0.0)
            if pd.isna(exp) or pd.isna(chg):
                continue
            shock += float(exp) * float(chg)
        pd_mult = max(PD_MULT_FLOOR,
                      min(PD_MULT_CEIL,
                          1.0 - SOVEREIGN_PD_BETA * shock / 100.0))
        adjusted = float(baseline) * pd_mult
        rows.append({
            'iso3': r['iso3'],
            'country': r.get('country', r['iso3']),
            'broad_rating': r['broad_rating'],
            'baseline_pd': float(baseline),
            'tot_shock_pct_gdp': shock,
            'adjusted_pd': adjusted,
            'pd_change_pct': (adjusted - float(baseline)) / float(baseline) * 100.0
                              if baseline > 0 else 0.0,
            'horizon_years': horizon,
        })
    sov = pd.DataFrame(rows)
    if sov.empty:
        sov['signal'] = []
        return sov
    sov['signal'] = classify_signals(sov['adjusted_pd'], sov['pd_change_pct'])
    return sov


def compute_country_sector_matrix(sovereign_df, weights_df, sensitivity_df,
                                  forecast_pct):
    """Long-form country x sector PD table.

    Each row inherits its country's sovereign-adjusted PD, then applies a
    sector-specific multiplier driven by the sensitivity table.
    """
    if sovereign_df.empty:
        return pd.DataFrame()

    sov_lookup = sovereign_df.set_index('iso3').to_dict('index')

    # sector -> {commodity: beta}
    sector_betas = {}
    for sector, g in sensitivity_df.groupby('sector'):
        sector_betas[sector] = dict(zip(g['commodity'], g['beta']))

    rows = []
    for _, w in weights_df.iterrows():
        iso = w['iso3']
        sector = w['sector']
        sov = sov_lookup.get(iso)
        if sov is None:
            continue
        baseline = sov['baseline_pd']
        notch_shift = 0.0
        for c, beta in sector_betas.get(sector, {}).items():
            chg = forecast_pct.get(c, 0.0)
            if pd.isna(chg):
                continue
            # beta = PD-notch change per 10% price move (positive beta = sector
            # benefits from price up -> negative notch shift = improvement)
            notch_shift += -float(beta) * float(chg) / 0.10
        pd_mult = SECTOR_PD_NOTCH_MULTIPLIER ** notch_shift
        pd_mult = max(PD_MULT_FLOOR, min(PD_MULT_CEIL, pd_mult))
        sector_pd = sov['adjusted_pd'] * pd_mult
        rows.append({
            'iso3': iso,
            'country': sov['country'],
            'sector': sector,
            'book_weight_pct': float(w.get('book_weight_pct', 0.0)),
            'sovereign_baseline_pd': baseline,
            'sovereign_adjusted_pd': sov['adjusted_pd'],
            'sector_notch_shift': notch_shift,
            'sector_pd': sector_pd,
            'pd_change_pct': (sector_pd - baseline) / baseline * 100.0
                              if baseline > 0 else 0.0,
            'horizon_years': sov['horizon_years'],
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df['signal'] = classify_signals(df['sector_pd'], df['pd_change_pct'])
    return df


def classify_signals(pd_values, pct_changes,
                     level_high_q=0.80, level_low_q=0.20,
                     change_threshold=10.0):
    """Two-axis classification.

    Level axis: cohort quintile of PD.
    Trajectory axis: forecast PD change vs baseline.

                       Improving  Stable     Deteriorating
        Top quintile    CAUTION   AVOID      AVOID
        Middle          NEUTRAL   NEUTRAL    CAUTION
        Bottom quint    STRATEGIC STRATEGIC  NEUTRAL
    """
    pd_values = pd.Series(pd_values).reset_index(drop=True)
    pct_changes = pd.Series(pct_changes).reset_index(drop=True)
    high_thr = pd_values.quantile(level_high_q)
    low_thr = pd_values.quantile(level_low_q)
    out = []
    for v, c in zip(pd_values, pct_changes):
        if pd.isna(v):
            out.append('NA')
            continue
        deteriorating = c >= change_threshold
        improving = c <= -change_threshold
        if v >= high_thr:
            out.append('AVOID' if not improving else 'CAUTION')
        elif v <= low_thr:
            out.append('STRATEGIC' if not deteriorating else 'NEUTRAL')
        else:
            if deteriorating:
                out.append('CAUTION')
            elif improving:
                out.append('STRATEGIC')
            else:
                out.append('NEUTRAL')
    return out
