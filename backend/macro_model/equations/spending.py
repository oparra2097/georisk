"""
Spending-block equations: consumption, investment, exports, imports.

All four are in log-level form. GDP is recovered identity-style in the
solver:  gdp = log(exp(cons) + exp(inv) + exp(gov) + exp(exp) − exp(imp))
with gov, pop, and productivity treated as exogenous inputs.

Consumption — life-cycle / permanent income
    LR: log(cons) = δ0 + δ1·log(gdp) + δ2·real_tsy10
        Consumption tracks permanent income (GDP proxy) plus a real-rate
        wealth-effect channel. δ1 should be close to 1 (rough income
        elasticity), δ2 negative (higher real rates → lower consumption).

Business + residential investment (total private) — accelerator + cost of capital
    LR: log(inv) = δ0 + δ1·log(gdp) + δ2·real_tsy10
        Investment scales with output (accelerator), discounted by real
        financing cost. δ1 typically 1-2 (more elastic than C), δ2 negative.

Exports — foreign demand + competitiveness
    LR: log(exp) = δ0 + δ1·log(row_gdp) + δ2·log(dxy)
        Rising foreign income lifts exports (δ1 > 0); stronger dollar
        hurts them (δ2 < 0).

Imports — domestic demand + relative prices
    LR: log(imp) = δ0 + δ1·log(gdp) + δ2·log(dxy)
        Rising US demand pulls imports in (δ1 > 0); stronger dollar
        makes imports cheaper so increases real imports (δ2 > 0).
"""

from backend.macro_model.estimation import EquationSpec


consumption = EquationSpec(
    name='Real consumption',
    dependent='cons',
    long_run=['gdp', 'real_tsy10'],
    short_run_diffs=['gdp', 'real_tsy10'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Permanent-income / life-cycle. δ1 on gdp ≈ 1; δ2 on real rate < 0.',
)


investment = EquationSpec(
    name='Real private investment',
    dependent='inv',
    long_run=['gdp', 'real_tsy10'],
    short_run_diffs=['gdp', 'real_tsy10'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Accelerator + real user-cost. δ1 on gdp > 1; δ2 on real rate < 0.',
)


exports = EquationSpec(
    name='Real exports',
    dependent='exp',
    long_run=['row_gdp', 'dxy'],
    short_run_diffs=['row_gdp', 'dxy'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Foreign income pulls exports up; stronger USD (↑dxy) hurts them.',
)


imports = EquationSpec(
    name='Real imports',
    dependent='imp',
    long_run=['gdp', 'dxy'],
    short_run_diffs=['gdp', 'dxy'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Domestic demand pulls imports in; stronger USD makes them cheaper.',
)


SPECS = [consumption, investment, exports, imports]
