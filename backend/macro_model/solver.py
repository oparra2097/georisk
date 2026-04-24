"""
Simultaneous equation solver for FRB/US-lite.

Advances the model one quarter at a time. Each quarter, the endogenous
variables are solved jointly via Gauss-Seidel iteration:

    while not converged:
        for each endogenous variable y_i:
            Δy_i = f_i(lags, current-iteration endogenous, exogenous)
            y_i_new = y_i_prev_quarter + Δy_i
        check max |y_i_new − y_i_old| < tol

Contemporaneous dependencies are resolved iteratively; lags come from the
history table, not the current iteration.

Identities (GDP = C + I + G + X − M) are evaluated after the behavioral
equations each iteration, so GDP reflects the current-iteration spending
components.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Callable, Optional

import numpy as np
import pandas as pd

from backend.macro_model.equations import derive_auxiliary_columns
from backend.macro_model.estimation import EquationFit
from backend.macro_model.variables import endogenous as _endogenous_vars

logger = logging.getLogger(__name__)


# ── Coefficient name parser ─────────────────────────────────────────────
#
# Estimation emits coefficients under these name schemes:
#   const           — intercept
#   ec_lag          — error-correction term u_{t-1}
#   d_{reg}_l{k}    — Δ{reg} at lag k  (reg may itself contain '_')
#   {reg}_l{k}      — {reg} level at lag k
#
# {reg} can include underscores (e.g. 'unemp_gap'), so we anchor on the
# trailing '_l<digits>' pattern.

_LAG_TAIL = re.compile(r'^(?P<prefix>.+?)_l(?P<k>\d+)$')


def _parse_coef_name(name: str) -> tuple[str, Optional[str], int]:
    """
    Returns (kind, regressor, lag) where kind is one of:
      'const', 'ec_lag', 'diff', 'level'.
    For 'const' / 'ec_lag' the regressor is None and lag is 0.
    """
    if name == 'const':
        return 'const', None, 0
    if name == 'ec_lag':
        return 'ec_lag', None, 1  # u_{t-1} by construction
    m = _LAG_TAIL.match(name)
    if not m:
        raise ValueError(f'unparseable coefficient name: {name}')
    prefix = m.group('prefix')
    lag = int(m.group('k'))
    if prefix.startswith('d_'):
        return 'diff', prefix[2:], lag
    return 'level', prefix, lag


# ── Identities ──────────────────────────────────────────────────────────

@dataclass
class Identity:
    """
    Non-estimated equation assigning one endogenous variable as a
    deterministic function of others. Evaluated each Gauss-Seidel pass.

    The callable receives (panel, t_idx) so identities can look at prior
    quarters (e.g. CPI inherits last quarter's level + this quarter's
    core-PCE growth).
    """
    dependent: str
    evaluate: Callable[[pd.DataFrame, int], float]
    notes: str = ''


def gdp_identity(panel: pd.DataFrame, t_idx: int) -> float:
    """gdp = log(exp(cons) + exp(inv) + exp(gov) + exp(exp) − exp(imp)).

    All inputs are log-levels (from the variable registry); output is also
    log-level. A tiny epsilon floor guards against imports > non-import GDP
    in edge cases during early iterations.
    """
    row = panel.iloc[t_idx]
    c = np.exp(row['cons'])
    i = np.exp(row['inv'])
    g = np.exp(row['gov'])
    x = np.exp(row['exp'])
    m = np.exp(row['imp'])
    y = c + i + g + x - m
    return float(np.log(max(y, 1e-6)))


def cpi_identity(panel: pd.DataFrame, t_idx: int) -> float:
    """Headline CPI tracks core PCE in Δ terms.

    log(cpi)_t = log(cpi)_{t-1} + Δlog(pce_core)_t.

    Energy-and-food CPI moves more than core PCE around shocks, but at the
    aggregate, quarterly Δlog(cpi) and Δlog(pce_core) correlate ~0.9 and
    differ mainly by a roughly-stable level shift (CPI basket vs PCE
    basket weights). v1 uses equal growth rates; a future refinement can
    add an oil-price term to capture energy-CPI dispersion.
    """
    d_pce = panel.iloc[t_idx]['pce_core'] - panel.iloc[t_idx - 1]['pce_core']
    return float(panel.iloc[t_idx - 1]['cpi'] + d_pce)


IDENTITIES: list[Identity] = [
    Identity(dependent='gdp', evaluate=gdp_identity,
             notes='National accounts identity in log-levels.'),
    Identity(dependent='cpi', evaluate=cpi_identity,
             notes='Headline CPI inherits core-PCE quarterly growth.'),
]


# ── Solver core ─────────────────────────────────────────────────────────

class Simulator:
    """
    Holds an extended panel and advances it one quarter at a time.

    Inputs:
      fits         dict[str, EquationFit] keyed by equation name
      panel        historical quarterly panel (model-form, incl. auxiliaries)
      identities   list of Identity objects evaluated each pass

    The panel is extended row-by-row; exogenous columns must be populated
    for every future quarter before calling `simulate_quarter`.
    """

    def __init__(self, fits: dict[str, EquationFit], panel: pd.DataFrame,
                 identities: list[Identity] = None):
        self.fits_by_dep = {f.spec.dependent: f for f in fits.values()}
        ident_list = identities if identities is not None else IDENTITIES
        self.identities = {i.dependent: i for i in ident_list}
        self.panel = panel.copy()
        # Derive auxiliary columns if not already present
        if 'unemp_gap' not in self.panel.columns:
            self.panel = derive_auxiliary_columns(self.panel)
        # Solution order: behavioral equations first (in spec order), then identities
        self.behavioral_order = [f.spec.dependent for f in fits.values()]
        self.identity_order = [i.dependent for i in ident_list]
        # Safety-net: any endogenous variable in the registry that lacks both an
        # equation and an identity gets a random-walk warm start (flat) so it
        # never produces NaN when another equation references it as a regressor.
        covered = set(self.behavioral_order) | set(self.identity_order)
        self.rw_endogenous = [v.code for v in _endogenous_vars() if v.code not in covered]

    # -- RHS evaluator -----------------------------------------------------

    def _eval_rhs(self, fit: EquationFit, t_idx: int) -> float:
        """Compute predicted Δy_t from an equation's estimated coefficients."""
        coefs = fit.short_run_coefs
        total = 0.0
        for name, coef in coefs.items():
            kind, reg, lag = _parse_coef_name(name)
            if kind == 'const':
                total += coef
            elif kind == 'ec_lag':
                dep = fit.spec.dependent
                y_lag = self.panel.iloc[t_idx - 1][dep]
                lr_pred = float(fit.long_run_coefs.get('const', 0.0))
                for r in fit.spec.long_run:
                    lr_pred += float(fit.long_run_coefs[r]) * self.panel.iloc[t_idx - 1][r]
                total += coef * (y_lag - lr_pred)
            elif kind == 'diff':
                if t_idx - lag - 1 < 0:
                    continue
                dreg = (self.panel.iloc[t_idx - lag][reg]
                        - self.panel.iloc[t_idx - lag - 1][reg])
                total += coef * dreg
            elif kind == 'level':
                if t_idx - lag < 0:
                    continue
                total += coef * self.panel.iloc[t_idx - lag][reg]
        return total

    # -- Gauss-Seidel iteration for a single quarter ----------------------

    def simulate_quarter(self, t_idx: int, max_iter: int = 100, tol: float = 1e-3,
                         relax: float = 0.5,
                         shock: Optional[dict[str, float]] = None) -> dict:
        """
        Solve for all endogenous variables at quarter `t_idx`. Caller is
        responsible for having populated all EXOGENOUS columns at that row.

        `shock` is an optional dict {dep: add_to_delta} that perturbs the
        predicted Δy_i for that equation at this quarter only (used by the
        shock/IRF engine in Phase F). For persistent shocks, the caller
        applies them each quarter.

        Returns a small diagnostics dict.
        """
        # Warm start: carry forward last quarter's values for every endogenous
        # variable (behavioral, identity, or random-walk fallback).
        for dep in self.behavioral_order + self.identity_order + self.rw_endogenous:
            self.panel.iloc[t_idx, self.panel.columns.get_loc(dep)] = self.panel.iloc[t_idx - 1][dep]

        # Refresh auxiliary columns BEFORE the first iteration so equations
        # that reference gaps/inflation rates (Phillips curve, Fed funds,
        # etc.) read valid values in pass 0 rather than NaN.
        self._refresh_aux_at(t_idx)

        converged = False
        for iteration in range(max_iter):
            max_change = 0.0

            # Behavioral equations — SOR damping to avoid oscillation in
            # a tightly-coupled macro system: y_new = y_old + relax·(y_hat − y_old).
            for dep in self.behavioral_order:
                fit = self.fits_by_dep[dep]
                d = self._eval_rhs(fit, t_idx)
                if shock and dep in shock:
                    d += shock[dep]
                y_hat = float(self.panel.iloc[t_idx - 1][dep] + d)
                old_val = float(self.panel.iloc[t_idx][dep])
                new_val = old_val + relax * (y_hat - old_val)
                self.panel.iloc[t_idx, self.panel.columns.get_loc(dep)] = new_val
                change = abs(new_val - old_val)
                if np.isnan(change):
                    max_change = float('inf')
                else:
                    max_change = max(max_change, change)

            # Identities (after behaviorals so RHS is current). Also damped.
            for dep in self.identity_order:
                ident = self.identities[dep]
                y_hat = float(ident.evaluate(self.panel, t_idx))
                old_val = float(self.panel.iloc[t_idx][dep])
                new_val = old_val + relax * (y_hat - old_val)
                self.panel.iloc[t_idx, self.panel.columns.get_loc(dep)] = new_val
                change = abs(new_val - old_val)
                if np.isnan(change):
                    max_change = float('inf')
                else:
                    max_change = max(max_change, change)

            # Refresh auxiliary columns from this iteration's state
            self._refresh_aux_at(t_idx)

            if max_change < tol:
                converged = True
                break

        return {'iterations': iteration + 1, 'converged': converged, 'max_change': max_change}

    def _refresh_aux_at(self, t_idx: int):
        """Recompute derived columns (unemp_gap, pi_yoy, etc.) for one row."""
        row = self.panel.iloc[t_idx]
        locs = {c: self.panel.columns.get_loc(c) for c in self.panel.columns}
        if 'unemp' in row and 'nrou' in row:
            self.panel.iloc[t_idx, locs['unemp_gap']] = float(row['unemp'] - row['nrou'])
        if 'pce_core' in row:
            prev_pce = self.panel.iloc[t_idx - 1]['pce_core']
            pi_yoy = 400.0 * (row['pce_core'] - prev_pce)
            self.panel.iloc[t_idx, locs['pi_yoy']] = float(pi_yoy)
            self.panel.iloc[t_idx, locs['pi_gap']] = float(pi_yoy - 2.0)
        if 'tsy10' in row and 'pi_yoy' in row:
            self.panel.iloc[t_idx, locs['real_tsy10']] = float(row['tsy10'] - self.panel.iloc[t_idx]['pi_yoy'])
        if 'fedfunds' in row and 'pi_yoy' in row:
            self.panel.iloc[t_idx, locs['real_fedfunds']] = float(row['fedfunds'] - self.panel.iloc[t_idx]['pi_yoy'])

    # -- Multi-quarter forecast ------------------------------------------

    def forecast(self, horizon: int, exog_paths: Optional[pd.DataFrame] = None,
                 shocks_over_time: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Extend the panel `horizon` quarters into the future and solve.

        `exog_paths` should have the exogenous variable columns (gov, oil,
        row_gdp, nrou, prod, lfpr) indexed by the new quarterly dates.
        If omitted, exogenous variables are carried forward from the last
        historical value (flat assumption).

        `shocks_over_time` (optional) gives per-quarter shocks keyed by
        dependent variable: rows indexed by the new quarter, columns are
        dep-variable codes, values add to Δy at that quarter.
        """
        exog_cols = [c for c in ('gov', 'oil', 'row_gdp', 'nrou', 'prod', 'lfpr')
                     if c in self.panel.columns]
        last = self.panel.index[-1]
        new_dates = pd.date_range(
            start=(last + pd.offsets.QuarterEnd(1)).normalize(),
            periods=horizon, freq='QE',
        )
        # Extend panel with NaN rows for the horizon
        new_rows = pd.DataFrame(index=new_dates, columns=self.panel.columns, dtype=float)
        # Fill exogenous: either from supplied paths or flat from last value
        for col in exog_cols:
            if exog_paths is not None and col in exog_paths.columns:
                new_rows[col] = exog_paths[col].reindex(new_dates).values
            else:
                new_rows[col] = float(self.panel.iloc[-1][col])
        self.panel = pd.concat([self.panel, new_rows])

        diagnostics = []
        for d in new_dates:
            t_idx = self.panel.index.get_loc(d)
            shock = None
            if shocks_over_time is not None and d in shocks_over_time.index:
                shock = shocks_over_time.loc[d].dropna().to_dict()
            diag = self.simulate_quarter(t_idx, shock=shock)
            diag['quarter'] = d
            diagnostics.append(diag)

        forecast_df = self.panel.loc[new_dates].copy()
        forecast_df.attrs['diagnostics'] = diagnostics
        return forecast_df
