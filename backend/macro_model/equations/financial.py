"""
Financial-block equations: policy rate, long rate, and dollar.

Fed funds — inertial Taylor rule
    No cointegration (fedfunds is the policy instrument, not trending).
    Pure short-run dynamics:
        Δfedfunds = α + ρ·fedfunds_{t-1}     (policy inertia)
                  + β1·pi_gap_{t-1}            (inflation response)
                  + β2·unemp_gap_{t-1}          (slack response, β2 < 0)
                  + ε

10Y Treasury — expectations hypothesis + term premium
    LR: tsy10 = δ0 + δ1·fedfunds + δ2·pi_yoy
        Long rate tracks the path of short rates plus inflation (term
        premium absorbed in δ0 and residuals).
    SR: Δtsy10 responds to Δfedfunds, Δpi_yoy, and ECM.

Broad dollar index — rate differential + risk
    Simplified v1 without foreign rate data:
    Short-run only:
        Δlog(dxy) = α + β·Δfedfunds + γ·Δtsy10 + ρ·Δlog(dxy)_{t-1}
        Higher Fed policy / long rates → USD appreciation (β, γ > 0).
"""

from backend.macro_model.estimation import EquationSpec


fed_funds = EquationSpec(
    name='Federal Funds rate (Taylor rule)',
    dependent='fedfunds',
    long_run=[],                        # policy rate, not cointegrated
    short_run_levels=['pi_gap', 'unemp_gap'],
    max_lags=2,
    include_lagged_dep=True,
    notes='Inertial Taylor rule. β_pi_gap > 0 (lean against inflation); β_unemp_gap < 0 (ease when slack).',
)


tsy10 = EquationSpec(
    name='10Y Treasury yield',
    dependent='tsy10',
    long_run=['fedfunds', 'pi_yoy'],
    short_run_diffs=['fedfunds', 'pi_yoy'],
    max_lags=2,
    include_lagged_dep=True,
    notes='Expectations hypothesis: long rate ≈ avg future short rate + term premium.',
)


dxy = EquationSpec(
    name='Broad USD index',
    dependent='dxy',
    long_run=[],                        # no natural cointegration without ROW rates
    short_run_diffs=['fedfunds', 'tsy10'],
    max_lags=2,
    include_lagged_dep=True,
    notes='Rate-differential proxy via domestic policy and long rates (β > 0).',
)


SPECS = [fed_funds, tsy10, dxy]
