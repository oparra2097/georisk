"""
Stochastic simulation + shock engine.

Built on top of solver.Simulator. Two public entry points:

- bootstrap_forecast(): residual-bootstrap to produce forecast fan charts
  with confidence bands. For each draw, we resample residuals from each
  equation's fitted-sample distribution and add them to the deterministic
  Δy path each quarter.

- run_shock(): deterministic IRF. Runs two simulations — baseline and
  shocked — and reports the per-quarter difference for every endogenous
  variable. Shock shape can be one-time (impulse) or persistent (step).

Shock library is declarative: the same engine handles "oil +20%",
"fed +100bp", "USD +10%", "ROW GDP −2pp" by writing to the appropriate
exogenous path before solving.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from typing import Callable, Literal, Optional

import numpy as np
import pandas as pd

from backend.macro_model.solver import Simulator

logger = logging.getLogger(__name__)


# ── Shock catalogue ─────────────────────────────────────────────────────

ShockShape = Literal['impulse', 'step']


@dataclass
class Shock:
    """
    Declarative shock spec. Applied by writing to an exogenous variable's
    path before simulation.

    variable    which exogenous to shock ('oil', 'fedfunds', 'dxy', 'row_gdp', ...)
    magnitude   size of shock, in the variable's model-form units (log for
                quantities, pct-point for rates)
    shape       'impulse' = one-quarter shock at t=0; 'step' = persistent
    t0          quarters from start of forecast horizon when shock hits (default 0)
    """
    variable: str
    magnitude: float
    shape: ShockShape = 'step'
    t0: int = 0
    label: str = ''

    def apply_to_path(self, path: pd.Series, horizon: int) -> pd.Series:
        out = path.copy()
        if self.shape == 'impulse':
            if self.t0 < len(out):
                out.iloc[self.t0] += self.magnitude
        else:
            out.iloc[self.t0:] += self.magnitude
        return out


CATALOGUE: list[dict] = [
    {'id': 'oil_plus_20',        'variable': 'oil',       'magnitude': np.log(1.20), 'shape': 'step',     'label': 'Oil +20% (persistent)'},
    {'id': 'oil_minus_20',       'variable': 'oil',       'magnitude': np.log(0.80), 'shape': 'step',     'label': 'Oil −20% (persistent)'},
    {'id': 'fed_plus_100bp',     'variable': 'fedfunds',  'magnitude': 1.0,          'shape': 'step',     'label': 'Fed funds +100 bp (persistent)'},
    {'id': 'fed_minus_100bp',    'variable': 'fedfunds',  'magnitude': -1.0,         'shape': 'step',     'label': 'Fed funds −100 bp (persistent)'},
    {'id': 'usd_plus_10',        'variable': 'dxy',       'magnitude': np.log(1.10), 'shape': 'step',     'label': 'USD +10% (persistent)'},
    {'id': 'row_gdp_minus_2pct', 'variable': 'row_gdp',   'magnitude': np.log(0.98), 'shape': 'step',     'label': 'ROW GDP −2% level (persistent)'},
    {'id': 'oil_spike_impulse',  'variable': 'oil',       'magnitude': np.log(1.20), 'shape': 'impulse',  'label': 'Oil +20% (one quarter)'},
]


def get_catalogue() -> list[dict]:
    """Shock catalogue for API/UI display."""
    return [dict(s) for s in CATALOGUE]


def _spec_from_id(shock_id: str) -> Shock:
    for s in CATALOGUE:
        if s['id'] == shock_id:
            return Shock(variable=s['variable'], magnitude=s['magnitude'],
                         shape=s['shape'], label=s.get('label', ''))
    raise KeyError(f'unknown shock id: {shock_id}')


# ── Simulation primitives ───────────────────────────────────────────────

def _make_baseline_exog_paths(sim: Simulator, horizon: int) -> pd.DataFrame:
    """Flat-carry-forward exogenous paths for the forecast horizon."""
    exog_cols = [c for c in ('gov', 'oil', 'row_gdp', 'nrou', 'prod', 'lfpr')
                 if c in sim.panel.columns]
    last = sim.panel.index[-1]
    dates = pd.date_range(
        start=(last + pd.offsets.QuarterEnd(1)).normalize(),
        periods=horizon, freq='QE',
    )
    last_row = sim.panel.iloc[-1]
    return pd.DataFrame(
        {c: np.full(horizon, float(last_row[c])) for c in exog_cols},
        index=dates,
    )


def baseline_forecast(sim: Simulator, horizon: int = 20) -> pd.DataFrame:
    """Deterministic baseline — wraps Simulator.forecast with flat exogenous."""
    paths = _make_baseline_exog_paths(sim, horizon)
    # Deep-copy the simulator so we don't mutate the caller's panel
    local = copy.deepcopy(sim)
    return local.forecast(horizon=horizon, exog_paths=paths)


# ── Residual bootstrap ──────────────────────────────────────────────────

def bootstrap_forecast(
    sim: Simulator,
    horizon: int = 20,
    n_draws: int = 200,
    percentiles: tuple[float, ...] = (10, 50, 90),
    rng: Optional[np.random.Generator] = None,
) -> dict[str, pd.DataFrame]:
    """
    Residual-bootstrap forecast. For each of n_draws simulations, draw a
    residual from each equation's fitted-residual distribution at each
    quarter and add it to that equation's Δy. Return percentile bands
    per endogenous variable.

    Returns {variable_code: DataFrame with one column per percentile}.
    """
    if rng is None:
        rng = np.random.default_rng(42)

    baseline_paths = _make_baseline_exog_paths(sim, horizon)
    # Pre-collect residuals per equation
    resid_by_dep: dict[str, np.ndarray] = {
        fit.spec.dependent: fit.residuals.dropna().to_numpy()
        for fit in sim.fits_by_dep.values()
    }

    # Collect draws: outer list is per draw, each is a DataFrame of shape
    # horizon × len(endogenous_cols)
    endog_cols = sim.behavioral_order + sim.identity_order
    draws: dict[str, list[np.ndarray]] = {c: [] for c in endog_cols}

    for draw in range(n_draws):
        local = copy.deepcopy(sim)
        shocks_per_q = pd.DataFrame(index=baseline_paths.index,
                                    columns=list(resid_by_dep.keys()), dtype=float)
        for q in baseline_paths.index:
            for dep, resids in resid_by_dep.items():
                shocks_per_q.loc[q, dep] = float(rng.choice(resids))
        fcst = local.forecast(horizon=horizon,
                              exog_paths=baseline_paths,
                              shocks_over_time=shocks_per_q)
        for c in endog_cols:
            draws[c].append(fcst[c].to_numpy())

    # Compute percentiles
    out: dict[str, pd.DataFrame] = {}
    for c in endog_cols:
        arr = np.vstack(draws[c])  # n_draws × horizon
        cols = {f'p{int(p)}': np.percentile(arr, p, axis=0) for p in percentiles}
        out[c] = pd.DataFrame(cols, index=baseline_paths.index)
    return out


# ── Impulse-response / shock engine ─────────────────────────────────────

def run_shock(
    sim: Simulator,
    shock_id: str,
    horizon: int = 20,
) -> dict:
    """
    Run baseline vs. shocked simulation; return IRFs for every endogenous
    variable.

    Returns:
        {
          'shock':     dict spec,
          'baseline':  DataFrame,
          'shocked':   DataFrame,
          'irf':       DataFrame of (shocked − baseline) per quarter,
                       per endogenous variable,
        }
    """
    shock = _spec_from_id(shock_id)

    baseline_paths = _make_baseline_exog_paths(sim, horizon)
    shocked_paths = baseline_paths.copy()
    if shock.variable in shocked_paths.columns:
        shocked_paths[shock.variable] = shock.apply_to_path(
            shocked_paths[shock.variable], horizon
        ).values
    else:
        # If the shocked variable isn't exogenous in v1, we can still shock
        # it via the Simulator's per-equation `shock` kwarg — not wired here
        # but noted as a future extension.
        raise ValueError(f'v1 shocks only supported on exogenous variables; '
                         f'{shock.variable} is not exogenous.')

    base_sim = copy.deepcopy(sim)
    shocked_sim = copy.deepcopy(sim)
    baseline = base_sim.forecast(horizon=horizon, exog_paths=baseline_paths)
    shocked = shocked_sim.forecast(horizon=horizon, exog_paths=shocked_paths)

    endog_cols = [c for c in sim.behavioral_order + sim.identity_order if c in baseline.columns]
    irf = (shocked[endog_cols] - baseline[endog_cols]).copy()

    return {
        'shock': {
            'id': shock_id,
            'variable': shock.variable,
            'magnitude': shock.magnitude,
            'shape': shock.shape,
            'label': next((s['label'] for s in CATALOGUE if s['id'] == shock_id), ''),
        },
        'baseline': baseline,
        'shocked': shocked,
        'irf': irf,
    }
