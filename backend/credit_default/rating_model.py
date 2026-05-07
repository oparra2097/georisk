"""
Sovereign credit rating + probability-of-default model.

Design notes
------------

We deliberately do *not* fit against the AIG historical credit rating series
(proprietary). Instead this module produces:

  1. a transparent **macro/external/fiscal score** (0-100, higher = riskier)
     built from publicly-observable indicators with a documented weighting,
  2. a mapped **letter rating** on the standard agency scale (AAA … D), and
  3. a **probability of default** at 1, 3 and 5 year horizons, calibrated to
     long-run historical default frequencies by rating bucket.

The score is intentionally simple and explainable: weighted z-scores of the
input indicators (clipped at ±3σ) blended into a composite, then rescaled.
Once a real default-event panel is wired (see backend/credit_default/README.md
for the candidate sources — Bank of Canada CRAG, S&P sovereign default study,
Moody's sovereign default and recovery), the weights here can be re-fit by a
logistic regression of `defaulted_within_horizon ~ z_indicators` and the
calibration table below replaced with the empirical hazard.

This file has no upstream API dependencies — it operates on the panel
returned by ``backend.credit_default.data.get_panel``.
"""

from __future__ import annotations

import math
import statistics
from typing import Dict, List, Optional, Tuple

# Lazy import — fit.py pulls pandas/sklearn which we don't want to load
# unconditionally on the dashboard request path.
def _load_fit_state(horizon_years: int = 1):
    try:
        from backend.credit_default import fit as cd_fit
    except Exception:
        return None
    return cd_fit.load_state(horizon_years)


# ── Indicator weights ────────────────────────────────────────────────────
#
# Sign convention: every contribution is "amount of risk". For
# `higher_is_worse` indicators we use +z; for `higher_is_better` we use -z.
# Weights sum to ~1.0 across the indicators that actually have a value for
# a given country (renormalized at scoring time so missing indicators don't
# silently zero-out the score).

#
# Macro-only: WGI / governance series are deliberately excluded — every
# weight below is a quantitative macro/external/fiscal indicator. The
# 10% that would have gone to governance is redistributed across the
# debt and external blocks (the highest signal-to-noise blocks in
# sovereign default work).
WEIGHTS: Dict[str, float] = {
    # Public debt sustainability (50%)
    'gross_debt_pct_gdp':            0.20,
    'fiscal_balance_pct_gdp':        0.10,
    'interest_pct_revenue':          0.13,
    'shadow_debt_gap_pp':            0.07,   # estimated − official debt/GDP
    # External vulnerability (35%)
    'current_account_pct_gdp':       0.09,
    'reserves_to_imports_months':    0.09,
    'short_term_debt_pct_reserves':  0.09,
    'external_debt_pct_gni':         0.08,
    # Real economy (15%)
    'real_gdp_growth':               0.07,
    'inflation':                     0.05,
    'gdp_per_capita_ppp':            0.03,
}

# How each weight maps onto the directionality of the risk contribution.
# True  = higher value of the indicator INCREASES sovereign risk
# False = higher value DECREASES risk (we'll flip the sign of z)
HIGHER_IS_WORSE: Dict[str, bool] = {
    'gross_debt_pct_gdp':            True,
    'fiscal_balance_pct_gdp':        False,
    'interest_pct_revenue':          True,
    'shadow_debt_gap_pp':            True,
    'current_account_pct_gdp':       False,
    'reserves_to_imports_months':    False,
    'short_term_debt_pct_reserves':  True,
    'external_debt_pct_gni':         True,
    'real_gdp_growth':               False,
    'inflation':                     True,
    'gdp_per_capita_ppp':            False,
}

# Hard caps on individual indicator z-scores so a single outlier (e.g. Lebanon
# inflation at 200%) doesn't pin the composite at the ceiling.
Z_CLIP = 3.0


# ── Rating scale + PD calibration ────────────────────────────────────────
#
# We use a custom 1–10 scale with +/− modifiers on 2–6, giving a 20-notch
# ladder (1, 2+, 2, 2−, 3+, 3, 3−, …, 6+, 6, 6−, 7, 8, 9, 10). 1 is the
# strongest credit; 10 is in default. + is the strong half-notch within a
# whole grade, − is the weak half-notch — same convention as agency
# +/−/Aa1/Aa3 modifiers.
#
# Each row is mapped to S&P / Moody's equivalents purely for display in
# the comparison panel — agencies use 22 notches, but the bottom four
# (CCC+/CCC/CCC−/CC/C) collapse to a single "7"/"8"/"9" on our scale
# because granularity in deep junk adds little decision value.
#
# PD figures are *long-run historical* sovereign default frequencies from
# S&P's annual sovereign default study and Moody's sovereign default and
# recovery report — placeholders calibrated to plausible orders of
# magnitude. Once a real default-event panel is wired (see README), fit
# the actual hazard.

# (max_score, pm_notch, pm_numeric, sp_equiv, moodys_equiv, pd_1y, pd_3y, pd_5y)
RATING_BUCKETS: List[Tuple[float, str, int, str, str, float, float, float]] = [
    (15,  '1',   1,  'AAA',  'Aaa',  0.000, 0.001, 0.002),
    (22,  '2+',  2,  'AA+',  'Aa1',  0.001, 0.002, 0.005),
    (28,  '2',   3,  'AA',   'Aa2',  0.001, 0.003, 0.007),
    (34,  '2-',  4,  'AA-',  'Aa3',  0.002, 0.005, 0.010),
    (40,  '3+',  5,  'A+',   'A1',   0.003, 0.008, 0.015),
    (46,  '3',   6,  'A',    'A2',   0.005, 0.012, 0.020),
    (52,  '3-',  7,  'A-',   'A3',   0.008, 0.018, 0.030),
    (58,  '4+',  8,  'BBB+', 'Baa1', 0.012, 0.030, 0.050),
    (63,  '4',   9,  'BBB',  'Baa2', 0.018, 0.045, 0.075),
    (68,  '4-',  10, 'BBB-', 'Baa3', 0.025, 0.060, 0.100),
    (72,  '5+',  11, 'BB+',  'Ba1',  0.040, 0.090, 0.150),
    (76,  '5',   12, 'BB',   'Ba2',  0.060, 0.130, 0.200),
    (80,  '5-',  13, 'BB-',  'Ba3',  0.080, 0.170, 0.250),
    (84,  '6+',  14, 'B+',   'B1',   0.110, 0.230, 0.330),
    (87,  '6',   15, 'B',    'B2',   0.150, 0.300, 0.420),
    (90,  '6-',  16, 'B-',   'B3',   0.200, 0.380, 0.520),
    (95,  '7',   17, 'CCC',  'Caa',  0.350, 0.560, 0.700),
    (98,  '8',   18, 'CC',   'Ca',   0.580, 0.750, 0.830),
    (99,  '9',   19, 'C',    'C',    0.700, 0.830, 0.880),
    (100, '10',  20, 'D',    'D',    1.000, 1.000, 1.000),  # in default
]

# Investment-grade boundary on the PM scale: 4− (numeric 10) is the last
# IG notch (BBB−). 5+ and below are speculative grade.
IG_BOUNDARY_NUMERIC = 10


def _letter_and_pd(score: float, defaulted: bool = False) -> Dict:
    if defaulted:
        return {
            'pm_notch': '10', 'pm_numeric': 20,
            'sp_equiv': 'D', 'moodys_equiv': 'D',
            'pd_1y': 1.0, 'pd_3y': 1.0, 'pd_5y': 1.0,
            'is_investment_grade': False,
        }
    for max_s, notch, num, sp, moo, p1, p3, p5 in RATING_BUCKETS:
        if score <= max_s:
            return {
                'pm_notch': notch, 'pm_numeric': num,
                'sp_equiv': sp, 'moodys_equiv': moo,
                'pd_1y': p1, 'pd_3y': p3, 'pd_5y': p5,
                'is_investment_grade': num <= IG_BOUNDARY_NUMERIC,
            }
    last = RATING_BUCKETS[-1]
    return {
        'pm_notch': last[1], 'pm_numeric': last[2],
        'sp_equiv': last[3], 'moodys_equiv': last[4],
        'pd_1y': last[5], 'pd_3y': last[6], 'pd_5y': last[7],
        'is_investment_grade': last[2] <= IG_BOUNDARY_NUMERIC,
    }


# ── Z-scoring helpers ────────────────────────────────────────────────────


def _column_stats(values: List[Optional[float]]) -> Tuple[Optional[float], Optional[float]]:
    """Robust median / MAD-based scale across a cross-section."""
    clean = [v for v in values if v is not None and not math.isnan(v)]
    if len(clean) < 5:
        return None, None
    med = statistics.median(clean)
    mad = statistics.median([abs(v - med) for v in clean])
    # 1.4826 makes MAD a consistent estimator of σ under normality.
    sigma = 1.4826 * mad if mad else (statistics.pstdev(clean) or 1.0)
    return med, sigma if sigma else 1.0


def _z(value: Optional[float], med: Optional[float], sigma: Optional[float]) -> Optional[float]:
    if value is None or med is None or sigma in (None, 0):
        return None
    z = (value - med) / sigma
    if z > Z_CLIP:
        z = Z_CLIP
    elif z < -Z_CLIP:
        z = -Z_CLIP
    return z


# ── Public API ───────────────────────────────────────────────────────────


def score_panel(panel: Dict, horizon_years: int = 1) -> Dict:
    """Add 'rating' block to every country in the panel and return it.

    If a fitted-model state file exists for the requested horizon (created
    by ``backend.credit_default.fit``), uses the fitted coefficients and
    empirical PD calibration. Otherwise falls back to the scaffold weights
    + bucket-table calibration so the dashboard always has something to
    render.

    Mutates a copy of `panel`; the original isn't modified.
    """
    countries = panel.get('countries') or {}
    if not countries:
        return panel

    fit_state = _load_fit_state(horizon_years)
    use_fit = fit_state is not None and bool(fit_state.get('coefficients'))

    # 1. Build cross-section vectors so we can compute z-scores per indicator.
    # We *always* compute the transparent composite (it's the secondary
    # "reference score" the user wants alongside the fitted model output),
    # so we never short-circuit this step on fit availability.
    indicator_keys = list(WEIGHTS.keys())
    cross: Dict[str, List[Optional[float]]] = {k: [] for k in indicator_keys}

    for iso3, c in countries.items():
        ind = c.get('indicators') or {}
        shadow = c.get('shadow_debt') or {}
        for key in indicator_keys:
            if key == 'shadow_debt_gap_pp':
                cross[key].append(shadow.get('debt_gap_pp'))
            else:
                cross[key].append(ind.get(key))

    stats = {k: _column_stats(vals) for k, vals in cross.items()}

    # 2. Compose per-country score.
    out_countries: Dict[str, Dict] = {}

    # Choose the active weight/sign vectors. When the fitter is loaded, use
    # its coefficients (already in σ units of the standardized features) and
    # let the empirical PD calibration table govern the PD output. The
    # scaffold weights remain the fallback.
    if use_fit:
        active_coefs: Dict[str, float] = fit_state.get('coefficients', {})
    else:
        active_coefs = {k: (WEIGHTS[k] if HIGHER_IS_WORSE[k] else -WEIGHTS[k])
                        for k in indicator_keys}

    for iso3, c in countries.items():
        ind = c.get('indicators') or {}
        shadow = c.get('shadow_debt') or {}
        defaulted = (shadow.get('risk_tier') == 'Defaulted')

        contributions: List[Dict] = []
        composite_weighted_sum = 0.0
        composite_weight_total = 0.0
        fit_latent = 0.0
        fit_weight_total = 0.0
        fit_has_data = False

        for key in indicator_keys:
            if key == 'shadow_debt_gap_pp':
                raw = shadow.get('debt_gap_pp')
            else:
                raw = ind.get(key)

            med, sigma = stats[key]
            z = _z(raw, med, sigma)

            # Always compute the transparent composite contribution.
            scaffold_w = WEIGHTS[key]
            scaffold_signed_z = None
            scaffold_contribution = None
            if z is not None:
                scaffold_signed_z = z if HIGHER_IS_WORSE[key] else -z
                scaffold_contribution = scaffold_w * scaffold_signed_z
                composite_weighted_sum += scaffold_contribution
                composite_weight_total += scaffold_w

            # Compute the fitted-model contribution if we have a fit.
            fit_coef = active_coefs.get(key) if use_fit else None
            fit_contribution = None
            if use_fit and z is not None and fit_coef is not None:
                fit_contribution = fit_coef * z
                fit_latent += fit_contribution
                fit_weight_total += abs(fit_coef)
                fit_has_data = True

            # The displayed contribution prefers the fitted model when
            # active (it's what governs the headline rating); otherwise
            # use the scaffold contribution.
            displayed = fit_contribution if use_fit else scaffold_contribution
            displayed_weight = (abs(fit_coef) if use_fit and fit_coef is not None
                                else scaffold_w)
            contributions.append({
                'indicator': key,
                'value': raw,
                'z': None if z is None else round(z, 3),
                'contribution': None if displayed is None else round(displayed, 4),
                'composite_contribution': (None if scaffold_contribution is None
                                           else round(scaffold_contribution, 4)),
                'fit_contribution': (None if fit_contribution is None
                                     else round(fit_contribution, 4)),
                'weight': round(displayed_weight, 4),
            })

        # ── Composite (transparent z-score) score ──────────────────────
        composite_normalized = (composite_weighted_sum / composite_weight_total
                                if composite_weight_total > 0 else 0.0)
        composite_score = 100.0 / (1.0 + math.exp(-0.9 * composite_normalized))
        composite_rating = _letter_and_pd(composite_score, defaulted=defaulted)

        # ── Fitted-model score (the user's primary rating) ─────────────
        model_pd = None
        if use_fit and fit_has_data:
            intercept = float(fit_state.get('intercept') or 0.0)
            estimator = fit_state.get('estimator', 'logit')
            z_total = fit_latent + intercept
            if estimator == 'logit':
                try:
                    model_pd = 1.0 / (1.0 + math.exp(-z_total))
                except OverflowError:
                    model_pd = 0.0 if z_total < 0 else 1.0
            else:
                model_pd = _pd_from_calibration(
                    z_total, fit_state.get('pd_calibration'))
            model_score = 100.0 * model_pd
            model_normalized = z_total
        else:
            model_score = composite_score
            model_normalized = composite_normalized

        model_rating = _letter_and_pd(model_score, defaulted=defaulted)
        if model_pd is not None and not defaulted:
            horizon_key = f'pd_{horizon_years}y'
            if horizon_key in model_rating:
                model_rating[horizon_key] = round(model_pd, 4)

        # Sort contributions by absolute size for the dashboard waterfall.
        contributions.sort(
            key=lambda r: abs(r['contribution']) if r['contribution'] is not None else -1,
            reverse=True,
        )

        out_country = dict(c)
        out_country['rating'] = {
            # Headline rating = the user's fitted model (or scaffold fallback).
            'score': round(model_score, 1),
            'normalized_z': round(model_normalized, 3),
            'defaulted': defaulted,
            'coverage': round(fit_weight_total if use_fit and fit_has_data else composite_weight_total, 2),
            'source': 'fitted' if (use_fit and fit_has_data) else 'composite',
            **model_rating,
            # Always-on transparent reference score (the "separate score").
            'composite': {
                'score': round(composite_score, 1),
                'normalized_z': round(composite_normalized, 3),
                'pm_notch': composite_rating['pm_notch'],
                'pm_numeric': composite_rating['pm_numeric'],
                'sp_equiv': composite_rating['sp_equiv'],
                'pd_1y': composite_rating['pd_1y'],
                'pd_3y': composite_rating['pd_3y'],
                'pd_5y': composite_rating['pd_5y'],
                'is_investment_grade': composite_rating['is_investment_grade'],
            },
            'contributions': contributions,
        }
        out_countries[iso3] = out_country

    out = dict(panel)
    out['countries'] = out_countries
    out['model'] = {
        'name': 'Parra Sovereign Credit Score',
        'version': 'v0.2-fit' if use_fit else 'v0.1-scaffold',
        'estimator': (fit_state or {}).get('estimator') if use_fit else 'scaffold-zscore',
        'horizon_years': horizon_years,
        'method': (
            'fitted coefficients on standardized macro panel (1-step PD)'
            if use_fit else 'z-score weighted composite + logistic squash'
        ),
        'fit_meta': {
            'auc_in_sample': (fit_state or {}).get('auc_in_sample'),
            'n_obs': (fit_state or {}).get('n_obs'),
            'n_events': (fit_state or {}).get('n_events'),
            'trained_at': (fit_state or {}).get('trained_at'),
        } if use_fit else None,
        'scale': '1-10 (with +/- on 2-6); 1 strongest, 10 default',
        'ig_boundary_numeric': IG_BOUNDARY_NUMERIC,
        'weights': WEIGHTS,
        'higher_is_worse': HIGHER_IS_WORSE,
        'fitted_coefficients': (fit_state or {}).get('coefficients') if use_fit else None,
        'rating_buckets': [
            {'max_score': r[0], 'pm_notch': r[1], 'pm_numeric': r[2],
             'sp_equiv': r[3], 'moodys_equiv': r[4],
             'pd_1y': r[5], 'pd_3y': r[6], 'pd_5y': r[7]}
            for r in RATING_BUCKETS
        ],
    }
    return out


def _pd_from_calibration(score: float, calibration) -> float:
    """Lookup empirical PD for a score in the bucket table."""
    if not calibration:
        return 0.0
    for bucket in calibration:
        if score <= bucket.get('score_hi', float('inf')):
            return float(bucket.get('pd_empirical', 0.0))
    return float(calibration[-1].get('pd_empirical', 0.0))
