"""
Labor-block equations: employment and unemployment.

LFPR, NROU, and productivity are exogenous in v1 (slow-moving trends
dominated by demographics and structural factors).

Employment — production-function cointegration
    LR: log(emp) = δ0 + δ1·log(gdp) − δ2·log(prod)
        In the long run, employment tracks output scaled by productivity
        (higher productivity → less labor needed per unit output).
        Expect δ1 ≈ 1 and δ2 ≈ −1 in principle, weaker empirically.

Unemployment rate — Okun-type reversion
    No cointegrating relation (unemp is bounded and stationary-ish).
    Pure short-run:
        Δunemp = α + β·Δlog(gdp)   (Okun: faster growth → lower unemp)
               + λ·(unemp − nrou)   (reversion to NAIRU)
               + ρ·Δunemp_lag
"""

from backend.macro_model.estimation import EquationSpec


employment = EquationSpec(
    name='Nonfarm payrolls',
    dependent='emp',
    long_run=['gdp', 'prod'],
    short_run_diffs=['gdp', 'prod'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Production-function LR. Expect δ_gdp positive, δ_prod negative.',
)


unemployment = EquationSpec(
    name='Unemployment rate',
    dependent='unemp',
    long_run=[],                     # no cointegration; pure short-run
    short_run_diffs=['gdp'],
    short_run_levels=['unemp_gap'],  # reversion-to-NAIRU term
    max_lags=3,
    include_lagged_dep=True,
    notes='Okun (β on Δlog(gdp) negative) + reversion to NAIRU via unemp_gap.',
)


SPECS = [employment, unemployment]
