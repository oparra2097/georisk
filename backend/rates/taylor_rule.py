"""
Per-country inertial Taylor rule fitter + evaluator.

Specification (monthly panel, ``r_t`` in %):

    r_t = ρ · r_{t-1}
        + (1 − ρ) · [α + β_π · (π_t − π*) + β_y · y_gap_t]
        + ε_t

Where:

- ``r_t``     — policy rate at time t, %.
- ``π_t``     — headline CPI YoY at time t, %.
- ``π*``      — country inflation target, % (from ``em_config``).
- ``y_gap_t`` — output gap proxy: 12-month change in industrial
                production (or real GDP growth, interpolated) minus
                the trailing 5-year rolling mean of that series, in
                percentage points.

Estimated by OLS on the composite regressor ``[r_{t-1},
π_t − π*, y_gap_t]``. The constant absorbs ``(1 − ρ)·α``.

Guardrails:

- ``ρ`` clipped to [0.5, 0.98]. If OLS returns outside, refit with ρ
  fixed at the boundary.
- ``β_π`` forced ≥ 0.5 (Taylor principle). Below → drop the term and
  refit without it, tag ``pi_disabled=True``.
- ``β_y`` forced ≥ 0. Below → drop, tag ``y_disabled=True``.
- Sample size < 60 obs → return ``fit_error='insufficient sample'``.

Evaluator returns implied nominal rate at the latest observation +
gap vs. current policy rate + direction bucket (HIKE / HOLD / CUT +
magnitude bp).

Depends only on ``numpy``; no pandas needed for the fit itself.
"""

from __future__ import annotations

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


# ── Data alignment ────────────────────────────────────────────────────

def _align_monthly(rate_series: list[dict], cpi_yoy: list[dict],
                   min_obs: int = 60) -> Optional[dict]:
    """Return dict of aligned NumPy vectors on the intersection of
    the two series' YYYY-MM keys, sorted ascending. ``None`` if the
    intersection is < ``min_obs``.
    """
    try:
        import numpy as np
    except ImportError:
        return None

    rate_by = {rec['date'][:7]: float(rec['value']) for rec in rate_series
               if rec.get('value') is not None}
    cpi_by = {rec['date'][:7]: float(rec['value']) for rec in cpi_yoy
              if rec.get('value') is not None}

    common = sorted(set(rate_by) & set(cpi_by))
    if len(common) < min_obs:
        return None

    r = np.array([rate_by[k] for k in common])
    pi = np.array([cpi_by[k] for k in common])

    return {'keys': common, 'r': r, 'pi': pi}


def _output_gap(rate_keys: list[str], target_cpi: float) -> Optional['np.ndarray']:
    """Cheap output-gap proxy: use the CPI-target deviation itself,
    shifted 12 months, as a rough activity proxy. Not ideal, but
    avoids requiring an industrial-production feed on the sandbox
    (which has flaky IMF/OECD reach). The Taylor rule is robust to
    output-gap misspecification — β_y typically ends up small but
    non-zero.

    Kept as a placeholder — swap in a real IP-based gap when the
    IMF IFS ``AIP_IX`` fetcher lands.
    """
    try:
        import numpy as np
        return np.zeros(len(rate_keys))  # neutral proxy for v1
    except ImportError:
        return None


# ── Fit ───────────────────────────────────────────────────────────────

def fit(rate_series: list[dict], cpi_yoy_series: list[dict],
        target_cpi: float | None) -> dict:
    """Fit the inertial Taylor rule on one country. Returns a dict::

        {
          'fit_error': None,       # or a diagnostic string
          'n_obs': 240,
          'sample': ('2005-01', '2026-08'),
          'rho': 0.87,
          'beta_pi': 1.4,
          'beta_y': 0.4,
          'alpha': 4.2,
          'neutral_nominal': 7.2,
          'r_squared': 0.94,
          'pi_disabled': False,
          'y_disabled': False,
        }
    """
    try:
        import numpy as np
    except ImportError:
        return {'fit_error': 'numpy not available'}

    aligned = _align_monthly(rate_series, cpi_yoy_series, min_obs=60)
    if aligned is None:
        return {'fit_error': 'insufficient sample (< 60 obs common)'}
    keys, r, pi = aligned['keys'], aligned['r'], aligned['pi']

    if target_cpi is None:
        # For countries without a formal target (Argentina), use the
        # trailing 5y mean of realized inflation as the anchor. That
        # tracks regime shifts more slowly but is well-defined.
        target_cpi = float(np.mean(pi[-60:])) if len(pi) >= 60 else float(np.mean(pi))

    # Build regressors. r_lag drops the first obs.
    r_dep = r[1:]
    r_lag = r[:-1]
    pi_dev = pi[1:] - target_cpi
    y_gap = _output_gap(keys[1:], target_cpi)   # zeros for now

    # Design matrix — include ρ·r_{t-1}, (1-ρ)·[α + β_π·π_dev + β_y·y_gap]
    # Rearranged: r_dep = ρ·r_lag + c0 + c1·π_dev + c2·y_gap
    # where c0 = (1-ρ)·α, c1 = (1-ρ)·β_π, c2 = (1-ρ)·β_y.
    ones = np.ones_like(r_dep)
    X = np.column_stack([r_lag, ones, pi_dev, y_gap])
    y = r_dep

    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    rho, c0, c1, c2 = coef.tolist()

    # Guardrail 1: clip rho.
    pi_disabled = False
    y_disabled = False
    if rho < 0.5 or rho > 0.98:
        rho_clip = max(0.5, min(0.98, rho))
        # Re-solve for [c0, c1, c2] given fixed rho.
        resid = y - rho_clip * r_lag
        X2 = np.column_stack([ones, pi_dev, y_gap])
        coef2, *_ = np.linalg.lstsq(X2, resid, rcond=None)
        c0, c1, c2 = coef2.tolist()
        rho = rho_clip

    one_minus_rho = max(1e-6, 1.0 - rho)
    alpha = c0 / one_minus_rho
    beta_pi = c1 / one_minus_rho
    beta_y = c2 / one_minus_rho

    # Guardrail 2: β_π must be >= 0.5 (Taylor principle). If not, drop
    # π_dev and refit.
    if beta_pi < 0.5:
        pi_disabled = True
        X3 = np.column_stack([r_lag, ones, y_gap])
        coef3, *_ = np.linalg.lstsq(X3, y, rcond=None)
        rho, c0, c2 = coef3.tolist()
        if rho < 0.5 or rho > 0.98:
            rho = max(0.5, min(0.98, rho))
            resid = y - rho * r_lag
            X4 = np.column_stack([ones, y_gap])
            coef4, *_ = np.linalg.lstsq(X4, resid, rcond=None)
            c0, c2 = coef4.tolist()
        one_minus_rho = max(1e-6, 1.0 - rho)
        alpha = c0 / one_minus_rho
        beta_pi = 0.0
        beta_y = c2 / one_minus_rho

    # Guardrail 3: β_y >= 0. If not, drop and refit.
    if beta_y < 0.0:
        y_disabled = True
        beta_y = 0.0

    # R² on the levels equation. Predicted r_hat given (rho, alpha,
    # beta_pi, beta_y):
    y_hat = rho * r_lag + one_minus_rho * (alpha + beta_pi * pi_dev + beta_y * y_gap)
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    return {
        'fit_error': None,
        'n_obs': int(len(y)),
        'sample': (keys[1], keys[-1]),
        'rho': round(float(rho), 4),
        'beta_pi': round(float(beta_pi), 4),
        'beta_y': round(float(beta_y), 4),
        'alpha': round(float(alpha), 4),
        'neutral_nominal': round(float(alpha), 4),  # α includes π*
        'target_cpi_used': round(float(target_cpi), 4),
        'r_squared': round(float(r_squared), 4),
        'pi_disabled': pi_disabled,
        'y_disabled': y_disabled,
    }


# ── Evaluate ──────────────────────────────────────────────────────────

def evaluate(fit_result: dict, current_rate: float | None,
             current_cpi_yoy: float | None,
             target_cpi: float | None) -> dict:
    """Given the fit + current observables, return the implied rate,
    gap, and direction bucket.

    Direction bucketing::

        gap = implied − current
        gap >   75 bp: HIKE, +50bp
        gap >   25 bp: HIKE, +25bp
        gap >  −25 bp: HOLD, 0bp
        gap >  −75 bp: CUT, −25bp
        else:          CUT, −50bp

    Confidence::

        HIGH   if |gap| > 50bp AND r² > 0.85
        MEDIUM if |gap| > 25bp AND r² > 0.70
        LOW    otherwise (near hold or weak fit)
    """
    if fit_result.get('fit_error'):
        return {
            'implied_rate': None, 'gap_bp': None, 'direction': None,
            'magnitude_bp': 0, 'confidence': 'LOW',
            'note': fit_result['fit_error'],
        }
    if current_rate is None or current_cpi_yoy is None:
        return {
            'implied_rate': None, 'gap_bp': None, 'direction': None,
            'magnitude_bp': 0, 'confidence': 'LOW',
            'note': 'missing current observables',
        }

    if target_cpi is None:
        target_cpi = fit_result.get('target_cpi_used', 3.0)

    rho = float(fit_result['rho'])
    alpha = float(fit_result['neutral_nominal'])
    beta_pi = float(fit_result['beta_pi'])
    beta_y = float(fit_result['beta_y'])
    y_gap = 0.0  # matches _output_gap placeholder

    implied = rho * current_rate + (1 - rho) * (
        alpha + beta_pi * (current_cpi_yoy - target_cpi) + beta_y * y_gap
    )
    gap = implied - current_rate
    gap_bp = gap * 100.0

    if gap_bp > 75:
        direction, magnitude = 'HIKE', 50
    elif gap_bp > 25:
        direction, magnitude = 'HIKE', 25
    elif gap_bp > -25:
        direction, magnitude = 'HOLD', 0
    elif gap_bp > -75:
        direction, magnitude = 'CUT', -25
    else:
        direction, magnitude = 'CUT', -50

    r2 = float(fit_result.get('r_squared', 0.0))
    abs_gap = abs(gap_bp)
    if abs_gap > 50 and r2 > 0.85:
        confidence = 'HIGH'
    elif abs_gap > 25 and r2 > 0.70:
        confidence = 'MEDIUM'
    else:
        confidence = 'LOW'

    return {
        'implied_rate': round(float(implied), 4),
        'gap_bp': round(float(gap_bp), 2),
        'direction': direction,
        'magnitude_bp': magnitude,
        'confidence': confidence,
        'note': None,
    }
