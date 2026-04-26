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


# Each long-run includes `trend` (decimal years since panel start) — this
# absorbs the secular productivity + population growth that the OLS would
# otherwise dump into the cointegrating residual. Over 1980-2025 GDP grew
# ~3.5x in real terms; without a trend, OLS finds the best static line
# through that drift, the residual at sample end sits 5-15% positive, and
# γ ≈ -0.1 snaps the first forecast quarter downward — surfacing on the
# dashboard as "GDP trends negative" in the baseline.

consumption = EquationSpec(
    name='Real consumption',
    dependent='cons',
    long_run=['gdp', 'real_tsy10', 'trend'],
    short_run_diffs=['gdp', 'real_tsy10'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Permanent-income / life-cycle. δ1 on gdp ≈ 1; δ2 on real rate < 0; +trend.',
)


investment = EquationSpec(
    name='Real private investment',
    dependent='inv',
    long_run=['gdp', 'real_tsy10', 'trend'],
    short_run_diffs=['gdp', 'real_tsy10'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Accelerator + real user-cost. δ1 on gdp > 1; δ2 on real rate < 0; +trend.',
)


exports = EquationSpec(
    name='Real exports',
    dependent='exp',
    long_run=['row_gdp', 'dxy', 'trend'],
    short_run_diffs=['row_gdp', 'dxy'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Foreign income pulls exports up; stronger USD (↑dxy) hurts them; +trend.',
)


imports = EquationSpec(
    name='Real imports',
    dependent='imp',
    long_run=['gdp', 'dxy', 'trend'],
    short_run_diffs=['gdp', 'dxy'],
    max_lags=3,
    include_lagged_dep=True,
    notes='Domestic demand pulls imports in; stronger USD makes them cheaper; +trend.',
)


SPECS = [consumption, investment, exports, imports]
