"""
National HPI forecasting model — Engle-Granger ECM.

Long-run cointegrating relation:
    log(HPI)_t = δ0 + δ1·log(real_income)_t + δ2·mortgage30_t + δ3·unemp_t

Short-run dynamics (via ECM):
    Δlog(HPI)_t = α + γ·u_{t-1}
                + β1·Δlog(real_income)_t + β2·Δmortgage30_t + β3·Δunemp_t
                + Σ φ_k · Δlog(HPI)_{t-k}        (lagged dep, k = 1..K)
                + ε_t

`γ` is the equilibrium-adjustment speed; should land in (-1, 0) — typically
−0.05 to −0.30 for quarterly housing data. Reasonable economic priors:
    δ1 (income) > 0 (homes are normal goods)
    δ2 (mortgage rate) < 0 (financing cost)
    δ3 (unemployment) < 0 (income / sentiment)

The same residual-bootstrap fan-chart approach used by the FRB/US-lite
macro model is applied here. v1 keeps it national-only; per-state ECM
loops will reuse the same driver panel.

Forecast inputs:
- Baseline: drivers carry forward their last historical values flat
  (equivalent to Hold-Steady scenario). Reasonable for short horizons.
- Shocks: callers supply per-quarter additive overrides on the driver
  paths (mortgage30 +100bp persistent, etc.).
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from backend.house_prices.forecast.drivers import BY_CODE as DRIVERS_BY_CODE
from backend.macro_model.estimation import EquationSpec, EquationFit, fit_equation

logger = logging.getLogger(__name__)


# ── Spec ────────────────────────────────────────────────────────────────
#
# **Pure short-run dynamic spec — no Engle-Granger cointegration.**
#
# We tried two cointegrating long-run specs and both produced wild first-
# quarter snap-backs:
#   - long_run = [real_income]            → +50% then -40%/-45%/-47% YoY
#                                            (income-only under-explained
#                                             the post-1995 HPI climb,
#                                             EC residual ~+18% at end of
#                                             sample, γ ≈ -0.41 snapped
#                                             HPI down hard)
#   - long_run = [real_income, cpi]       → +40% snap UP next quarter
#                                            (CPI's post-2020 surge made
#                                             current HPI look "below
#                                             equilibrium", reversed sign
#                                             of the snap)
#
# Root cause: the 1980-2025 sample has multiple structural breaks
# (post-1995 financialization, 2007-09 crash, 2020 COVID boom) and a
# constant-coefficient cointegrating relationship between HPI and any
# fixed combination of macro variables doesn't actually hold — OLS
# finds *some* line, but the implied "equilibrium" residual is fragile
# and produces snap-back forecasts that don't match recent dynamics.
#
# Pure short-run dynamic (long_run=[]) replaces that with a stationary
# AR(p) + exogenous-cyclical model:
#
#   Δlog(HPI)_t = α + Σ φ_k Δlog(HPI)_{t-k}
#               + Σ β  Δdriver_t (mortgage30, real_income, unemp)
#               + ε_t
#
# With drivers carried forward flat, the forecast naturally converges
# to the unconditional mean Δlog(HPI) — historically ~5%/yr — instead
# of snapping toward a spurious target. Shock IRFs still work via the
# β coefficients on the driver differences.

# short_run_levels matters under the flat-driver baseline. With drivers
# carried forward at their last historical values, every Δdriver term in
# the equation evaluates to zero — so the only thing keeping the baseline
# tied to the current macro environment is the LEVEL terms.
#
# Why include_lagged_dep=False: an AR(p) component fit on 1980-2025 data
# picks up the COVID-boom shock (2021-2022 saw 6%+ quarterly Δlog(HPI))
# as if it were a persistent dynamic, and at max_lags=4 the autoregressive
# polynomial sometimes has roots near the unit circle. That projects 30%+
# quarterly jumps in the first forecast quarter (visible on the dashboard
# as +101% YoY snap-up). Dropping lagged_dep and shrinking max_lags to 2
# eliminates that pathology — the model now relates Δlog(HPI) directly to
# current macro state and short-run cyclicals.
#
# CLIP_DELTA is a defensive guardrail in `_forecast_path` for the
# DETERMINISTIC paths only (baseline + shock IRFs). It catches spec
# pathologies that would otherwise produce ±30%/q snap forecasts. The
# bootstrap path skips the clip entirely so the residual draws flow
# through to the p10/p90 fan unmodified — clipping in bootstrap was
# collapsing the fan to a thin ribbon (the symmetric clip + frequent
# residual hits past the threshold made the variance much narrower
# than the actual fitted-residual distribution).

CLIP_DELTA = 0.04   # ±4%/q in log terms (~16% annualized)


NATIONAL_SPEC = EquationSpec(
    name='HPI national',
    dependent='hpi',
    long_run=[],                                                  # no cointegration
    short_run_diffs=['real_income', 'mortgage30', 'unemp'],       # cyclical impulses
    short_run_levels=['mortgage30', 'unemp'],                     # current macro state
    max_lags=2,
    include_lagged_dep=False,
    notes='Macro-driven Δlog(HPI) — short-run diffs + current level state. No AR component.',
)


def _state_spec(state_code: str) -> EquationSpec:
    """Per-state spec — same shape as national, with the state's own
    unemployment rate (FRED <STATE>UR) as both a cyclical impulse and a
    persistent level driver."""
    return EquationSpec(
        name=f'HPI {state_code}',
        dependent='hpi',
        long_run=[],
        short_run_diffs=['real_income', 'mortgage30', 'state_unemp'],
        short_run_levels=['mortgage30', 'state_unemp'],
        max_lags=2,
        include_lagged_dep=False,
        notes=f'State-level macro-driven Δlog(HPI) ({state_code}); state-specific unemp.',
    )


# ── Shock catalogue ─────────────────────────────────────────────────────

@dataclass
class HpiShock:
    id: str
    label: str
    driver: str            # which driver to perturb
    magnitude: float       # additive in driver units (% pts for rates, log-pts for log vars)
    shape: str = 'step'    # 'step' (persistent) or 'impulse' (one quarter)
    t0: int = 0


CATALOGUE: list[HpiShock] = [
    HpiShock('mortgage_plus_100bp', 'Mortgage rate +100 bp (persistent)', 'mortgage30',  +1.0,         'step'),
    HpiShock('mortgage_minus_100bp','Mortgage rate −100 bp (persistent)', 'mortgage30',  -1.0,         'step'),
    HpiShock('mortgage_plus_200bp', 'Mortgage rate +200 bp (persistent)', 'mortgage30',  +2.0,         'step'),
    HpiShock('income_minus_2pct',   'Real income −2% level (persistent)', 'real_income', np.log(0.98),'step'),
    HpiShock('unemp_plus_2pp',      'Unemployment +2 pp (persistent)',    'unemp',       +2.0,         'step'),
    HpiShock('fedfunds_plus_100bp', 'Fed funds +100 bp (persistent)',     'fedfunds',    +1.0,         'step'),
]


def get_shock_catalogue() -> list[dict]:
    return [
        {'id': s.id, 'label': s.label, 'driver': s.driver, 'magnitude': s.magnitude, 'shape': s.shape}
        for s in CATALOGUE
    ]


# ── Fit ─────────────────────────────────────────────────────────────────

@dataclass
class HpiForecastModel:
    fit: EquationFit
    panel: pd.DataFrame                # full panel used for fitting (drivers + hpi)
    panel_start: pd.Timestamp = field(default=None)
    panel_end: pd.Timestamp = field(default=None)

    def __post_init__(self):
        self.panel_start = self.panel.index.min()
        self.panel_end = self.panel.index.max()


def _fit_with_spec(hpi_log: pd.Series, drivers: pd.DataFrame,
                   spec: EquationSpec, label: str,
                   min_obs: int = 40) -> HpiForecastModel:
    panel = drivers.join(hpi_log.rename('hpi'), how='inner')
    panel = panel.dropna(how='any')
    if len(panel) < min_obs:
        raise RuntimeError(f'panel too short for {label} fit (n={len(panel)} < {min_obs})')
    fit = fit_equation(panel, spec)
    gamma = fit.error_correction_coef()
    has_ec = bool(spec.long_run)        # γ is meaningful only when there's a long_run stage
    label_kind = 'ECM' if has_ec else 'short-run dynamic'
    logger.info(f'hpi_forecast.model: fitted {label} {label_kind}, '
                f'γ={gamma:+.3f}, R²={fit.rsq:.3f}, '
                f'N={fit.n_obs}, sample {panel.index.min().date()}→{panel.index.max().date()}')
    # Sanity check: a well-specified housing ECM should have γ in (-0.4, 0).
    # Skip when there's no long_run stage — γ is NaN by construction in
    # the pure short-run dynamic spec.
    if has_ec and not (-0.4 < gamma < 0):
        logger.warning(
            f'hpi_forecast.model: {label} γ={gamma:+.3f} outside (-0.4, 0) — '
            f'forecasts may overshoot. Check long_run spec for confounded regressors.'
        )
    return HpiForecastModel(fit=fit, panel=panel)


def fit_national(hpi_log: pd.Series, drivers: pd.DataFrame) -> HpiForecastModel:
    """Fit the national ECM. `hpi_log` must be a quarterly log-HPI series
    indexed by end-of-quarter Timestamps. `drivers` is the macro driver
    panel from `forecast.drivers.build_panel`.
    """
    return _fit_with_spec(hpi_log, drivers, NATIONAL_SPEC, label='national')


def fit_state(hpi_log: pd.Series, drivers: pd.DataFrame, state_code: str) -> HpiForecastModel:
    """Fit a per-state ECM. State HPI series are typically shorter than the
    national one (some Sun Belt states only start ~1980), so we cap the
    minimum sample size at 30 quarters (~7.5 years).
    """
    return _fit_with_spec(hpi_log, drivers, _state_spec(state_code),
                          label=f'state {state_code}', min_obs=30)


# ── Forecast ────────────────────────────────────────────────────────────

def _make_baseline_driver_paths(panel: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Flat-carry-forward each driver from its last non-NaN historical value.

    Iterate ALL panel columns except the dependent (`hpi`) — not just the
    five registered DRIVERS — so per-state extra columns like `state_unemp`
    are also carried forward into forecast quarters. Without this the
    state spec's `Δstate_unemp` regressor read NaN at the first forecast
    quarter and produced an all-null per-state forecast (PR #64
    regression).
    """
    drivers = [c for c in panel.columns if c != 'hpi']
    last = panel.index[-1]
    new_dates = pd.date_range(
        start=(last + pd.offsets.QuarterEnd(1)).normalize(),
        periods=horizon, freq='QE',
    )
    fills: dict[str, float] = {}
    for c in drivers:
        valid = panel[c].dropna()
        fills[c] = float(valid.iloc[-1]) if len(valid) else 0.0
    return pd.DataFrame({c: np.full(horizon, fills[c]) for c in drivers}, index=new_dates)


def _apply_shock_to_paths(paths: pd.DataFrame, shock: HpiShock) -> pd.DataFrame:
    out = paths.copy()
    if shock.driver not in out.columns:
        return out
    if shock.shape == 'impulse':
        if shock.t0 < len(out):
            out.iloc[shock.t0, out.columns.get_loc(shock.driver)] += shock.magnitude
    else:  # step
        out.iloc[shock.t0:, out.columns.get_loc(shock.driver)] += shock.magnitude
    return out


def _forecast_path(model: HpiForecastModel, driver_paths: pd.DataFrame,
                   resid_draws: Optional[np.ndarray] = None) -> pd.Series:
    """Roll the ECM forward `len(driver_paths)` quarters.

    `resid_draws[k]` (optional) is added to Δhpi at quarter k — used by the
    bootstrap to inject a draw from the fitted residual distribution.
    """
    fit = model.fit
    panel = pd.concat(
        [model.panel.copy(), pd.DataFrame(index=driver_paths.index, columns=model.panel.columns, dtype=float)]
    )
    # Fill exogenous (driver) columns in the new rows
    for col in driver_paths.columns:
        if col in panel.columns:
            panel.loc[driver_paths.index, col] = driver_paths[col].values

    forecast: list[float] = []
    coefs = fit.short_run_coefs
    long_run_coefs = fit.long_run_coefs

    for i, ts in enumerate(driver_paths.index):
        t_idx = panel.index.get_loc(ts)
        delta = 0.0
        for name, coef in coefs.items():
            # Skip NaN/Inf coefficients. statsmodels produces NaN coefs when
            # the design matrix has near-rank-deficiency — for state models
            # this happens when fetch_state_unemp falls back to the national
            # 'unemp' series (multiple states share an identical column),
            # leaving some short-run coefs undefined. Without this guard the
            # NaN propagates into delta, the `delta > CLIP_DELTA` comparison
            # raises, and the per-state /baseline endpoint returns 500 →
            # dashboard surfaces "Build failed".
            if not np.isfinite(coef):
                continue
            if name == 'const':
                delta += coef
                continue
            if name == 'ec_lag':
                # u_{t-1} = log(HPI)_{t-1} − [δ0 + δ1·real_income_{t-1} + δ2·mortgage30_{t-1} + …]
                hpi_lag = panel.iloc[t_idx - 1]['hpi']
                lr_pred = float(long_run_coefs.get('const', 0.0))
                for r in fit.spec.long_run:
                    lr_pred += float(long_run_coefs[r]) * panel.iloc[t_idx - 1][r]
                term = coef * (hpi_lag - lr_pred)
                if np.isfinite(term):
                    delta += term
                continue
            # Parse 'd_<reg>_l<k>' or '<reg>_l<k>' or 'd_hpi_l<k>'
            if name.startswith('d_') and '_l' in name:
                tail = name.rfind('_l')
                reg = name[2:tail]
                lag = int(name[tail + 2:])
                if t_idx - lag - 1 < 0:
                    continue
                d_reg = panel.iloc[t_idx - lag][reg] - panel.iloc[t_idx - lag - 1][reg]
                term = coef * d_reg
                if np.isfinite(term):
                    delta += term
            elif '_l' in name:
                tail = name.rfind('_l')
                reg = name[:tail]
                lag = int(name[tail + 2:])
                if t_idx - lag < 0:
                    continue
                term = coef * panel.iloc[t_idx - lag][reg]
                if np.isfinite(term):
                    delta += term
        is_bootstrap = resid_draws is not None
        if is_bootstrap and i < len(resid_draws):
            delta += float(resid_draws[i])
        # Defensive clip — DETERMINISTIC paths only (baseline + shock).
        # Skipped under bootstrap so the residual draws flow through to
        # the p10/p90 fan unmodified. With the clip on, residuals past
        # the ±4%/q threshold get truncated symmetrically and the fan
        # collapses to a ribbon much thinner than the actual fitted-
        # residual distribution. Clipping baseline (where residuals are
        # zero) still catches spec pathologies that would surface as
        # ±30%/q snap forecasts.
        if not is_bootstrap:
            if delta > CLIP_DELTA:
                logger.warning(f'hpi_forecast.model: clipping baseline Δlog({fit.spec.name}) '
                               f'{delta:+.4f} → {CLIP_DELTA:+.4f} at q={driver_paths.index[i].date()}')
                delta = CLIP_DELTA
            elif delta < -CLIP_DELTA:
                logger.warning(f'hpi_forecast.model: clipping baseline Δlog({fit.spec.name}) '
                               f'{delta:+.4f} → {-CLIP_DELTA:+.4f} at q={driver_paths.index[i].date()}')
                delta = -CLIP_DELTA
        new_hpi = float(panel.iloc[t_idx - 1]['hpi'] + delta)
        panel.iloc[t_idx, panel.columns.get_loc('hpi')] = new_hpi
        forecast.append(new_hpi)
    return pd.Series(forecast, index=driver_paths.index, name='hpi')


def baseline_forecast(model: HpiForecastModel, horizon: int = 8) -> pd.DataFrame:
    """Deterministic baseline forecast under flat-carry-forward drivers.

    Returns a DataFrame indexed by future quarter-end dates, with columns
    `hpi_log`, `hpi_index`, `yoy_pct`.
    """
    paths = _make_baseline_driver_paths(model.panel, horizon)
    hpi_log = _forecast_path(model, paths)
    return _format_forecast(hpi_log, model)


def bootstrap_forecast(model: HpiForecastModel, horizon: int = 8,
                       n_draws: int = 200, percentiles: tuple = (10, 50, 90),
                       rng: Optional[np.random.Generator] = None) -> pd.DataFrame:
    """Residual-bootstrap fan chart. Returns a DataFrame indexed by quarter
    with columns p10/p50/p90 (or whatever percentiles caller passed) for
    the HPI index level.
    """
    if rng is None:
        rng = np.random.default_rng(42)
    residuals = model.fit.residuals.dropna().to_numpy()
    if residuals.size < 8:
        raise RuntimeError(f'too few residuals for bootstrap (n={residuals.size})')

    paths = _make_baseline_driver_paths(model.panel, horizon)
    draws = np.zeros((n_draws, horizon))
    for k in range(n_draws):
        eps = rng.choice(residuals, size=horizon, replace=True)
        hpi_log = _forecast_path(model, paths, resid_draws=eps)
        draws[k, :] = np.exp(hpi_log.values)        # back to index level

    cols = {f'p{int(p)}': np.percentile(draws, p, axis=0) for p in percentiles}
    return pd.DataFrame(cols, index=paths.index)


def shock_forecast(model: HpiForecastModel, shock_id: str, horizon: int = 8) -> dict:
    """Run baseline vs. shocked simulation; return both paths and the IRF.

    Returns:
        {
          'shock':    {id, label, driver, magnitude, shape},
          'baseline': DataFrame with hpi_log/hpi_index/yoy_pct,
          'shocked':  same shape,
          'irf':      DataFrame with hpi_index_diff, yoy_diff_pp,
        }
    """
    shock = next((s for s in CATALOGUE if s.id == shock_id), None)
    if shock is None:
        raise KeyError(f'unknown shock id: {shock_id}')

    baseline_paths = _make_baseline_driver_paths(model.panel, horizon)
    shocked_paths = _apply_shock_to_paths(baseline_paths, shock)

    base_log = _forecast_path(model, baseline_paths)
    shock_log = _forecast_path(model, shocked_paths)
    base_df = _format_forecast(base_log, model)
    shock_df = _format_forecast(shock_log, model)
    irf = pd.DataFrame({
        'hpi_index_diff': shock_df['hpi_index'].values - base_df['hpi_index'].values,
        'yoy_diff_pp':    shock_df['yoy_pct'].values   - base_df['yoy_pct'].values,
    }, index=base_df.index)
    return {
        'shock': {'id': shock.id, 'label': shock.label, 'driver': shock.driver,
                  'magnitude': float(shock.magnitude), 'shape': shock.shape},
        'baseline': base_df,
        'shocked': shock_df,
        'irf': irf,
    }


def _format_forecast(hpi_log: pd.Series, model: HpiForecastModel) -> pd.DataFrame:
    """Convert a log-HPI forecast into a UI-friendly frame: index level +
    yoy_pct (using the historical panel for the lag-4 reference)."""
    last_hist = model.panel['hpi'].iloc[-4:]                # last 4 historical quarters
    combined = pd.concat([last_hist, hpi_log])
    yoy = (np.exp(combined) / np.exp(combined.shift(4)) - 1.0) * 100.0
    out = pd.DataFrame({
        'hpi_log': hpi_log.values,
        'hpi_index': np.exp(hpi_log.values),
        'yoy_pct': yoy.loc[hpi_log.index].values,
    }, index=hpi_log.index)
    return out
