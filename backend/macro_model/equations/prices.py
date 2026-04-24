"""
Price-block equations.

v1 ships the core-PCE Phillips curve. Wage equation and headline-CPI
identity follow in Phase D.

Phillips curve (accelerationist / NKPC-hybrid):

    Long-run (cointegrating):  log(pce_core) = δ₀ + δ₁·log(wage) + δ₂·log(oil)
        — core prices are pinned over the long run by unit labor costs and
          energy cost pass-through.

    Short-run:  Δlog(pce_core)_t = α + γ·u_{t-1}
                                  + β₁·Δlog(wage)_{t..t-k}
                                  + β₂·Δlog(oil)_{t..t-k}
                                  + β₃·unemp_gap_{t..t-k}    (level regressor)
                                  + ρ·Δlog(pce_core)_{t-1..t-k}
                                  + ε_t

    unemp_gap = unemp − nrou. Negative gap = tight labor market → higher
    inflation (β₃ expected negative).
"""

from backend.macro_model.estimation import EquationSpec


phillips_curve = EquationSpec(
    name='Core PCE Phillips curve',
    dependent='pce_core',
    long_run=['wage', 'oil'],
    short_run_diffs=['wage', 'oil'],
    short_run_levels=['unemp_gap'],
    max_lags=4,
    include_lagged_dep=True,
    notes=(
        'Long-run: price level pinned by unit labor cost (wage) and energy '
        'pass-through (oil). Short-run dynamics: wage growth, oil growth, '
        'and labor-market slack enter the inflation rate. γ (error-correction) '
        'should be in (-0.3, -0.02). β₃ on unemp_gap expected negative.'
    ),
)


SPECS = [phillips_curve]
