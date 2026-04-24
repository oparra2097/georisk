"""
Error-correction OLS estimator for single equations.

FRB/US-lite equation form (Engle-Granger two-step):

    Step 1 (long-run):   Y_t = δ_0 + Σ δ_j X_{j,t} + u_t
    Step 2 (short-run):  ΔY_t = α + γ · u_{t-1}  +  Σ β_{jk} · ΔX_{j,t-k}  +  ε_t

`γ` is the equilibrium-adjustment coefficient; in a well-specified equation
it should be negative and bounded: −1 < γ < 0. A reasonable magnitude
(typically −0.05 to −0.40 for quarterly data) means the system closes the
gap to Y* at a sensible pace.

Diagnostics returned:
    long_run_rsq, cointegration_p   (Engle-Granger on u_t; low p = cointegrated)
    rsq, adj_rsq, rmse, aic, bic    (short-run fit)
    durbin_watson                   (1st-order autocorrelation in residuals)
    breusch_godfrey_p               (up-to-4th-order serial correlation; low p = bad)
    jarque_bera_p                   (residual normality; low p = non-normal)

Lag selection: we search a common lag length k ∈ {1..max_lags} for all
short-run regressors and pick the lowest AIC. Simple, interpretable, and
keeps the equation from over-fitting.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.diagnostic import acorr_breusch_godfrey
from statsmodels.stats.stattools import durbin_watson, jarque_bera
from statsmodels.tsa.stattools import adfuller

logger = logging.getLogger(__name__)


# ── Spec / result containers ─────────────────────────────────────────────

@dataclass
class EquationSpec:
    """Describes one behavioral equation to estimate."""

    dependent: str                           # variable code (e.g. 'pce_core')
    long_run: list[str]                      # drivers in the cointegrating relation
    short_run: list[str]                     # drivers appearing as Δx (lagged)
    max_lags: int = 4                        # highest common lag to search
    include_lagged_dep: bool = True          # include ΔY lagged as regressor
    name: str = ''
    notes: str = ''


@dataclass
class EquationFit:
    spec: EquationSpec

    # Long-run (cointegrating) stage
    long_run_coefs: pd.Series
    long_run_rsq: float
    cointegration_p: float

    # Short-run ECM stage
    short_run_coefs: pd.Series
    short_run_se: pd.Series
    short_run_tvals: pd.Series
    rsq: float
    adj_rsq: float
    rmse: float
    aic: float
    bic: float
    chosen_lag: int

    # Residual diagnostics
    durbin_watson: float
    breusch_godfrey_p: float
    jarque_bera_p: float

    # Data
    residuals: pd.Series
    fitted: pd.Series
    sample_start: pd.Timestamp
    sample_end: pd.Timestamp
    n_obs: int

    def error_correction_coef(self) -> float:
        """γ — the equilibrium-adjustment speed. Should be in (−1, 0)."""
        return float(self.short_run_coefs.get('ec_lag', np.nan))

    def to_dict(self) -> dict:
        return {
            'name': self.spec.name or self.spec.dependent,
            'dependent': self.spec.dependent,
            'long_run': {
                'regressors': self.spec.long_run,
                'coefs': self.long_run_coefs.round(4).to_dict(),
                'rsq': round(self.long_run_rsq, 4),
                'cointegration_p': round(self.cointegration_p, 4),
            },
            'short_run': {
                'chosen_lag': self.chosen_lag,
                'coefs': self.short_run_coefs.round(4).to_dict(),
                'se':    self.short_run_se.round(4).to_dict(),
                't':     self.short_run_tvals.round(2).to_dict(),
                'error_correction_coef': round(self.error_correction_coef(), 4),
                'rsq': round(self.rsq, 4),
                'adj_rsq': round(self.adj_rsq, 4),
                'rmse': round(self.rmse, 6),
                'aic': round(self.aic, 3),
                'bic': round(self.bic, 3),
            },
            'diagnostics': {
                'durbin_watson': round(self.durbin_watson, 3),
                'breusch_godfrey_p': round(self.breusch_godfrey_p, 4),
                'jarque_bera_p': round(self.jarque_bera_p, 4),
            },
            'sample': {
                'start': self.sample_start.date().isoformat(),
                'end':   self.sample_end.date().isoformat(),
                'n':     self.n_obs,
            },
        }


# ── Estimation core ─────────────────────────────────────────────────────

def _fit_long_run(panel: pd.DataFrame, dep: str, regs: list[str]) -> tuple[pd.Series, float, float, pd.Series]:
    """
    Step 1: OLS in levels. Returns (coefs, R², Engle-Granger p, residuals).
    """
    y = panel[dep]
    X = panel[regs].copy()
    X = sm.add_constant(X, has_constant='add')
    fit = sm.OLS(y, X, missing='drop').fit()
    resid = fit.resid.rename('lr_resid')

    # Engle-Granger cointegration test: ADF on the long-run residuals.
    try:
        adf_stat, p_val, _usedlag, _nobs, _crit, _ = adfuller(resid.dropna(), autolag='AIC')
    except Exception as e:
        logger.warning(f"ADF on long-run residuals failed: {e}")
        p_val = np.nan

    return fit.params, float(fit.rsquared), float(p_val), resid


def _build_short_run_matrix(panel: pd.DataFrame, dep: str, short_run: list[str],
                             lag: int, include_lagged_dep: bool,
                             lr_resid: pd.Series) -> tuple[pd.Series, pd.DataFrame]:
    """Build (Δy, X) matrix for the short-run regression at lag order `lag`."""
    dy = panel[dep].diff().rename('d_' + dep)

    cols: dict[str, pd.Series] = {}
    # Contemporaneous Δx (lag 0) and lagged differences up to `lag`
    for reg in short_run:
        dx = panel[reg].diff()
        for k in range(0, lag + 1):
            cols[f'd_{reg}_l{k}'] = dx.shift(k)

    if include_lagged_dep:
        for k in range(1, lag + 1):
            cols[f'd_{dep}_l{k}'] = dy.shift(k)

    # The error-correction term: u_{t-1}
    cols['ec_lag'] = lr_resid.shift(1)

    X = pd.DataFrame(cols)
    return dy, X


def _fit_short_run(dy: pd.Series, X: pd.DataFrame) -> sm.regression.linear_model.RegressionResultsWrapper:
    X_ = sm.add_constant(X, has_constant='add')
    return sm.OLS(dy, X_, missing='drop').fit()


def fit_equation(panel: pd.DataFrame, spec: EquationSpec) -> EquationFit:
    """
    Engle-Granger two-step ECM with AIC lag selection. Returns EquationFit.
    Raises ValueError if dependent or regressors are missing from the panel.
    """
    missing = [c for c in [spec.dependent] + spec.long_run + spec.short_run if c not in panel.columns]
    if missing:
        raise ValueError(f'panel missing columns: {missing}')

    # Step 1 — long-run
    lr_coefs, lr_rsq, coint_p, lr_resid = _fit_long_run(panel, spec.dependent, spec.long_run)

    # Step 2 — short-run with AIC lag search
    best_aic = np.inf
    best_fit = None
    best_lag = None
    for k in range(1, spec.max_lags + 1):
        dy, X = _build_short_run_matrix(
            panel, spec.dependent, spec.short_run, k, spec.include_lagged_dep, lr_resid,
        )
        fit = _fit_short_run(dy, X)
        if np.isfinite(fit.aic) and fit.aic < best_aic:
            best_aic = fit.aic
            best_fit = fit
            best_lag = k

    if best_fit is None:
        raise RuntimeError(f'ECM estimation failed for {spec.dependent}: no finite-AIC fit')

    # Diagnostics
    dw = float(durbin_watson(best_fit.resid))
    try:
        bg_stat, bg_p, _, _ = acorr_breusch_godfrey(best_fit, nlags=4)
    except Exception:
        bg_p = float('nan')
    try:
        _jb, jb_p, _, _ = jarque_bera(best_fit.resid)
    except Exception:
        jb_p = float('nan')

    used_idx = best_fit.resid.index
    return EquationFit(
        spec=spec,
        long_run_coefs=lr_coefs,
        long_run_rsq=lr_rsq,
        cointegration_p=coint_p,
        short_run_coefs=best_fit.params,
        short_run_se=best_fit.bse,
        short_run_tvals=best_fit.tvalues,
        rsq=float(best_fit.rsquared),
        adj_rsq=float(best_fit.rsquared_adj),
        rmse=float(np.sqrt(best_fit.mse_resid)),
        aic=float(best_fit.aic),
        bic=float(best_fit.bic),
        chosen_lag=int(best_lag),
        durbin_watson=dw,
        breusch_godfrey_p=float(bg_p),
        jarque_bera_p=float(jb_p),
        residuals=best_fit.resid,
        fitted=best_fit.fittedvalues,
        sample_start=used_idx.min(),
        sample_end=used_idx.max(),
        n_obs=int(best_fit.nobs),
    )
