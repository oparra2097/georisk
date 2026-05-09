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
def _load_fit_state(horizon_years: int = 1, cadence: str = 'annual'):
    try:
        from backend.credit_default import fit as cd_fit
    except Exception:
        return None
    if cadence == 'quarterly':
        return cd_fit.load_state_quarterly(horizon_years)
    return cd_fit.load_state(horizon_years)


# ── Indicator weights ────────────────────────────────────────────────────
#
# Sign convention: every contribution is "amount of risk". For
# `higher_is_worse` indicators we use +z; for `higher_is_better` we use -z.
# Weights sum to ~1.0 across the indicators that actually have a value for
# a given country (renormalized at scoring time so missing indicators don't
# silently zero-out the score).

#
# Mixed-block weighting: macro fundamentals dominate (~85%) but the
# Worldwide Governance Indicators (rule of law + 5 siblings) carry a
# 15% governance block. Rule of law and institutional quality have
# robust empirical signal in academic sovereign-default work.
WEIGHTS: Dict[str, float] = {
    # ── Public debt sustainability ──
    'gross_debt_pct_gdp':            0.18,
    'fiscal_balance_pct_gdp':        0.10,
    'interest_pct_revenue':          0.10,
    # ── External vulnerability ──
    'current_account_pct_gdp':       0.10,
    'reserves_to_imports_months':    0.09,
    'short_term_debt_pct_reserves':  0.08,
    'external_debt_pct_gni':         0.06,
    'external_liquidity_ratio':      0.05,   # S&P-style external financing pressure
    # ── Real economy ──
    'real_gdp_growth':               0.06,
    'inflation':                     0.04,
    'gdp_per_capita_ppp':            0.04,
    # ── Governance (slimmed) — keep rule_of_law, govt_effectiveness,
    # political_stability, control_of_corruption as the independent
    # set. Dropped regulatory_quality / voice_accountability
    # (correlated, <1% GBM importance each). Rule of law retained as
    # an explicit user preference — strong driver across our panel
    # historically (LatAm / SSA distress contexts). ──
    'rule_of_law':                   0.04,
    'govt_effectiveness':            0.03,
    'political_stability':           0.03,
    'control_of_corruption':         0.02,
    # ── Serial-default + research-driven ──
    'years_since_default':           0.04,   # R&R 2009; smaller = worse (recent default)
    'debt_chg_5y_pp':                0.04,   # Manasse 2003 + IMF SRDSF
    'fiscal_balance_chg_3y':         0.05,   # 3y trajectory; catches ROU-style deterioration
    'tot_volatility_5y':             0.03,   # IMF PCTOT, Hilscher-Nosbusch 2010
    'reserve_currency_share':        0.05,   # IMF COFER — reserve-currency offset
    'region_default_rate':           0.04,   # Reinhart-Rogoff 2009 — contagion
    'vix_annual':                    0.02,   # CBOE VIX — global stress
    # Removed (cumulative GBM importance < 4% combined; high
    # multicollinearity with retained features):
    #   shadow_debt_gap_pp (0.0%)        — never split on
    #   default_count_25y (0.3%)         — duplicate of years_since_default
    #   regulatory_quality (0.4%)        — duplicate of govt_effectiveness
    #   reer_overvaluation_pct (0.6%)    — sparse data, weak signal
    #   voice_accountability (0.9%)      — duplicate of political_stability
}

# How each weight maps onto the directionality of the risk contribution.
# True  = higher value of the indicator INCREASES sovereign risk
# False = higher value DECREASES risk (we'll flip the sign of z)
HIGHER_IS_WORSE: Dict[str, bool] = {
    'gross_debt_pct_gdp':            True,
    'fiscal_balance_pct_gdp':        False,
    'interest_pct_revenue':          True,
    'current_account_pct_gdp':       False,
    'reserves_to_imports_months':    False,
    'short_term_debt_pct_reserves':  True,
    'external_debt_pct_gni':         True,
    'real_gdp_growth':               False,
    'inflation':                     True,
    'gdp_per_capita_ppp':            False,
    'rule_of_law':                   False,
    'control_of_corruption':         False,
    'govt_effectiveness':            False,
    'political_stability':           False,
    'years_since_default':           False,  # more years = better
    'debt_chg_5y_pp':                True,   # faster debt build-up = worse
    'fiscal_balance_chg_3y':         False,  # negative chg = deterioration = worse
    'tot_volatility_5y':             True,   # higher ToT vol = worse
    'reserve_currency_share':        False,  # higher share = lower default risk
    'vix_annual':                    True,   # higher VIX = more global stress = worse
    'region_default_rate':           True,   # more regional defaults = worse contagion
    'external_liquidity_ratio':      True,   # higher pressure / lower cushion = worse
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
    (99,  '9',   19, 'SD',   'C',    0.700, 0.830, 0.880),
    (100, '10',  20, 'D',    'D',    1.000, 1.000, 1.000),  # in default
]

# Investment-grade boundary on the PM scale: 4− (numeric 10) is the last
# IG notch (BBB−). 5+ and below are speculative grade.
IG_BOUNDARY_NUMERIC = 10


# Per-horizon sensitivity multiplier on the Platt-rescale shift.
# Reducing the (negative) shift towards 0 lifts PDs without retraining;
# the user wanted 1y to be more sensitive (catches near-term distress
# faster) and 5y elevated for downgrade-prone sovereigns. Numbers
# chosen so 1y PDs roughly triple and 5y PDs roughly double versus
# the pure natural-rate baseline.
PD_SENSITIVITY_BY_YEAR = {1: 0.55, 3: 0.75, 5: 0.65}


def _adjusted_shift(class_balance_log_odds: float, years_eq: int) -> float:
    sens = PD_SENSITIVITY_BY_YEAR.get(int(years_eq), 1.0)
    return class_balance_log_odds * sens


# Reserve-currency-status logit discount. The macro feature panel
# (debt/GDP, fiscal balance, etc.) penalises USA / Japan / eurozone /
# UK for elevated debt without crediting the structural funding
# advantage of being a reserve-currency issuer.
#
# The IMF COFER allocation share is too noisy to use as a coefficient
# directly — GBP at 4.9% would imply a tiny discount, but agencies
# treat the UK as a tier-1 sovereign comparable to USA (USD 57%) at
# AA / AA+. So we use a hand-tuned per-issuer discount instead, with
# magnitudes calibrated against the agency-consensus notch each
# major reserve-currency sovereign sits at.
#
# Tier 1 (USD)         — anchored to USA at AA+ (model 2+, agency AA+)
# Tier 1 (EUR core)    — anchored to DEU/NLD at AAA
# Tier 2 (GBP/JPY/CHF) — anchored to UK at AA, JPN at A+, CHE at AAA
# Tier 3 (CAD/AUD/NZD) — anchored to AAA cluster
# Minor (CNY)          — small bonus, RMB still emerging as reserve
#
# Eurozone periphery members (ITA, ESP, GRC) get a smaller discount
# because their elevated debt + weaker fiscal already differentiates
# them in the macro features; over-discounting would put ITA at AAA
# when the agency is BBB.
_RESERVE_CURRENCY_LOGIT_SHIFT = {
    # Tier 1 ─ reserve currencies. Magnitudes hand-tuned so the
    # post-discount PD lands within the agency's notch window.
    'USA': -2.0,    # → 2+/AA+   (agency AA+/Aaa/AA+)
    'GBR': -1.6,    # → 2-/AA-   (matches Fitch leg of consensus AA)
    'JPN': -1.0,    # → AA-/A+   (agency A+/A1/A)
    'CHE': -1.6,    # → AAA      (agency AAA)
    # Tier 1 ─ EUR core (low-debt members, collective ECB credit)
    'DEU': -1.4, 'NLD': -1.4, 'AUT': -1.2, 'FIN': -1.2, 'LUX': -1.4,
    # Tier 1 ─ EUR upper-mid (agency AA-/A+)
    'FRA': -1.0, 'BEL': -1.0, 'IRL': -1.0,
    # Tier 1 ─ EUR periphery (let macro features dominate so model
    # differentiates them from the core).
    'ITA':  0.0, 'ESP':  0.0, 'PRT':  0.0, 'GRC':  0.0,
    # Eurozone smaller members
    'EST': -0.8, 'LVA': -0.8, 'LTU': -0.8, 'SVK': -0.8, 'SVN': -0.8,
    'MLT': -0.8, 'CYP':  0.0, 'HRV':  0.0,
    # Tier 3 ─ Anglosphere reserve-adjacent (agency AAA)
    'CAN': -1.6, 'AUS': -1.4, 'NZL': -1.2, 'SGP': -1.0,
    # Minor
    'CHN': -0.5,    # CNY — emerging reserve, modest discount
}


def _reserve_currency_shift(country_block: Dict) -> float:
    """Per-country logit-space discount for reserve-currency issuers.
    Hand-tuned against agency consensus notches — see
    ``_RESERVE_CURRENCY_LOGIT_SHIFT`` for the values and the rationale.
    Returns 0 for non-reserve sovereigns (the vast majority).
    """
    iso3 = (country_block.get('iso3') or '').upper()
    return float(_RESERVE_CURRENCY_LOGIT_SHIFT.get(iso3, 0.0))


def _score_with_gbm(payload, indicators, shadow, scaler, medians, shift):
    """Run a country through the persisted GBM tree ensemble. Returns
    the natural-rate-rescaled PD, or ``None`` if the model can't run
    for any reason (sklearn version mismatch, missing numpy, etc.) so
    the caller can fall back to the linear-importance approximation."""
    try:
        import numpy as np
    except ImportError:
        return None
    try:
        features = payload['features']
        model = payload['model']
        vec = []
        for f in features:
            if f == 'shadow_debt_gap_pp':
                raw = shadow.get('debt_gap_pp')
            else:
                raw = (indicators or {}).get(f)
            if raw is None:
                raw = medians.get(f)
            if raw is None:
                std_val = 0.0
            else:
                sc = scaler.get(f) or {}
                mean = float(sc.get('mean', 0.0))
                std = float(sc.get('std', 1.0)) or 1.0
                std_val = (float(raw) - mean) / std
                if std_val > Z_CLIP:
                    std_val = Z_CLIP
                elif std_val < -Z_CLIP:
                    std_val = -Z_CLIP
            vec.append(std_val)
        proba = float(model.predict_proba(np.asarray([vec]))[0, 1])
    except Exception as e:  # noqa: BLE001
        print(f'[credit_default.rating_model] GBM predict_proba failed: {e}')
        return None
    proba = min(max(proba, 1e-9), 1.0 - 1e-9)
    bal_logit = math.log(proba / (1.0 - proba))
    nat_logit = bal_logit + shift
    try:
        return 1.0 / (1.0 + math.exp(-nat_logit))
    except OverflowError:
        return 0.0 if nat_logit < 0 else 1.0


def _letter_and_pd(score: float, defaulted: bool = False,
                   calibrated_buckets: Optional[List[Dict]] = None) -> Dict:
    if defaulted:
        return {
            'pm_notch': '10', 'pm_numeric': 20,
            'sp_equiv': 'D', 'moodys_equiv': 'D',
            'pd_1y': 1.0, 'pd_3y': 1.0, 'pd_5y': 1.0,
            'is_investment_grade': False,
        }

    # If the fit produced empirical max_score boundaries, override the
    # hand-set ones in RATING_BUCKETS. The letter / PD columns and
    # IG/HY split stay as-is — only the score thresholds change so the
    # model-score CDF lines up with the agency-consensus CDF.
    cal_max_score: Dict[int, float] = {}
    if calibrated_buckets:
        for row in calibrated_buckets:
            try:
                cal_max_score[int(row.get('pm_numeric'))] = float(row.get('max_score'))
            except (TypeError, ValueError):
                continue

    for max_s, notch, num, sp, moo, p1, p3, p5 in RATING_BUCKETS:
        threshold = cal_max_score.get(num, max_s)
        if score <= threshold:
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


def score_panel(panel: Dict, horizon_years: int = 1,
                cadence: str = 'annual') -> Dict:
    """Add 'rating' block to every country in the panel and return it.

    Loads either the annual fit_state (cadence='annual', horizon in
    years) or the quarterly fit_state (cadence='quarterly', horizon in
    quarters: 4/12/20). Falls back to the scaffold weights when no
    fit_state is available.
    """
    countries = panel.get('countries') or {}
    if not countries:
        return panel

    fit_state = _load_fit_state(horizon_years, cadence=cadence)
    use_fit = fit_state is not None and bool(fit_state.get('coefficients'))
    cal_buckets = None
    gbm_payload = None
    if use_fit:
        rb = fit_state.get('rating_buckets') or {}
        cal_buckets = rb.get('buckets') if isinstance(rb, dict) else rb
        # Quarterly fits don't generate their own rating_buckets — the
        # PD scale is identical to the annual fit's (both are
        # 100·natural_pd), so reuse the annual calibrated buckets to
        # avoid every country collapsing into AAA against the hand-set
        # max_score table.
        if cadence == 'quarterly' and not cal_buckets:
            annual_horizon = max(1, int(round(horizon_years / 4))) if horizon_years > 5 else 1
            annual_state = _load_fit_state(annual_horizon, cadence='annual')
            if annual_state:
                ann_rb = annual_state.get('rating_buckets') or {}
                cal_buckets = ann_rb.get('buckets') if isinstance(ann_rb, dict) else ann_rb
        # If the fit is a GBM and a pickled model is available, score
        # countries through the actual tree ensemble for the displayed
        # PD. The linear-importance fallback we used before flattens
        # every country to base-rate PD (≈4%), which destroys the
        # discrimination GBM was trained to provide.
        if fit_state.get('estimator') == 'gbm' and fit_state.get('model_pickle'):
            from backend.credit_default import fit as cd_fit
            if cadence == 'quarterly':
                loaded = cd_fit.load_gbm_model_quarterly(horizon_years)
            else:
                loaded = cd_fit.load_gbm_model(horizon_years)
            if loaded:
                gbm_payload = {
                    'model': loaded[0],
                    'features': loaded[1],
                }

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

        # ── Composite (log-odds score on 0–100, 100 = highest risk) ──
        # The composite is a transparent reference score independent of
        # the fitted PD. We map the weighted z-sum into an implied PD
        # via sigmoid, then to a 0–100 score where HIGHER = HIGHER
        # default risk: score = 50 + K·log10(PD/(1-PD)). PD=0.5 → 50,
        # PD≈0.05 → ~0, PD≈0.95 → ~100, with K=38.4 anchoring the
        # ±50-point boundaries near the Z_CLIP=±3 limits on z.
        composite_normalized = (composite_weighted_sum / composite_weight_total
                                if composite_weight_total > 0 else 0.0)
        if defaulted:
            composite_score = 100.0
        else:
            composite_pd_implied = 1.0 / (1.0 + math.exp(-composite_normalized))
            composite_pd_implied = min(max(composite_pd_implied, 1e-6), 1.0 - 1e-6)
            composite_score = 50.0 + 38.4 * math.log10(
                composite_pd_implied / (1.0 - composite_pd_implied)
            )
            composite_score = max(0.0, min(100.0, composite_score))
        # The composite is now a continuous score, not a rating bucket;
        # leave the letter fields empty so no stale notch leaks through.
        composite_rating = {
            'pm_notch': None, 'pm_numeric': None,
            'sp_equiv': None, 'moodys_equiv': None,
            'pd_1y': None, 'pd_3y': None, 'pd_5y': None,
            'is_investment_grade': None,
        }

        # ── Fitted-model score (the user's primary rating) ─────────────
        model_pd = None
        if use_fit and fit_has_data:
            intercept = float(fit_state.get('intercept') or 0.0)
            estimator = fit_state.get('estimator', 'logit')
            z_total = fit_latent + intercept
            raw_shift = float(fit_state.get('class_balance_log_odds') or 0.0)
            # Map the active horizon back to year-equivalent so we
            # apply the right per-horizon sensitivity multiplier (1y
            # gets the biggest lift, 5y a moderate lift).
            if cadence == 'quarterly':
                years_eq = max(1, int(round(horizon_years / 4)))
            else:
                years_eq = horizon_years
            shift = _adjusted_shift(raw_shift, years_eq)
            # Reserve-currency discount applies on top of the Platt
            # shift for sovereigns whose currency is held as global
            # FX reserves (USD / EUR / JPY / GBP / etc.). Captures the
            # structural funding advantage the macro panel otherwise
            # misses.
            shift += _reserve_currency_shift(c)

            model_pd = None
            if gbm_payload is not None:
                # Score with the actual GBM tree ensemble. Builds the
                # standardized feature vector in the same order the
                # model was trained on, runs predict_proba, then Platt-
                # rescales to natural rate so the displayed PD has the
                # same base-rate semantics as the logit path. Returns
                # None if predict_proba fails (e.g. sklearn version
                # mismatch); we fall back to the linear path below.
                model_pd = _score_with_gbm(
                    gbm_payload, ind, shadow,
                    fit_state.get('scaler') or {},
                    fit_state.get('medians') or {}, shift,
                )
                if model_pd is not None:
                    if 0 < model_pd < 1:
                        model_normalized = math.log(model_pd / (1 - model_pd))
                    else:
                        model_normalized = -50.0 if model_pd <= 0 else 50.0
            if model_pd is None:
                # Logit path (or GBM fallback when pickle missing or
                # predict_proba failed): use the linear z_total + Platt
                # shift.
                adj = z_total + shift
                try:
                    model_pd = 1.0 / (1.0 + math.exp(-adj))
                except OverflowError:
                    model_pd = 0.0 if adj < 0 else 1.0
                model_normalized = z_total
            model_score = 100.0 * model_pd
        else:
            model_score = composite_score
            model_normalized = composite_normalized

        model_rating = _letter_and_pd(
            model_score, defaulted=defaulted, calibrated_buckets=cal_buckets,
        )
        if model_pd is not None and not defaulted:
            # Map quarterly horizon (4/12/20) back to year-equivalent so
            # we replace the right pd_*y key on the bucket-anchor PDs.
            if cadence == 'quarterly':
                years_eq = max(1, int(round(horizon_years / 4)))
            else:
                years_eq = horizon_years
            horizon_key = f'pd_{years_eq}y'
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
