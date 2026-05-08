# WTI Crude Oil — `CL=F` · $/bbl

West Texas Intermediate light sweet crude, delivered Cushing, Oklahoma. The
benchmark for US crude pricing and the reference for most North American
domestic barrels.

## Drivers

- **US Dollar Index (DXY, FRED: DTWEXBGS)** — crude is globally dollar-
  denominated. A stronger dollar tightens buying power for non-US refiners
  and correlates negatively with oil on medium horizons.
- **Geopolitical Risk Index (Caldara & Iacoviello)** — captures Middle East
  tensions, sanctions, shipping lane disruptions (Strait of Hormuz, Bab
  al-Mandab). Positive price response skewed to the upside tail.
- **S&P 500 (yfinance ^GSPC)** — proxy for global risk appetite and cyclic
  oil demand. Growth shocks propagate through equities before they land in
  inventory data.

> Drivers we'd like but don't have a clean free API for: EIA weekly US
> commercial crude inventories, OPEC+ spare capacity estimates, and US
> rig count. The SARIMAX AR component absorbs much of the mean-reverting
> dynamics these would explain; the GARCH residuals absorb the shock
> volatility. If an operator wires the EIA API v2 in, adding
> `WCESTUS1` (weekly stocks) to the driver spec is a 2-line change.

## Structural story

WTI trades at a Cushing-delivery discount to Brent, typically $2–$7 in
normal balance, wider when US shale production surges or export
infrastructure bottlenecks. Demand shocks (2020, Asian slowdowns) drive the
downside tail; Middle East supply disruption (Iran sanctions, Saudi
infrastructure strikes, Strait of Hormuz) drives the upside tail. The SPR
is now a less reliable shock absorber after 2022 drawdowns — refills are
slow, which structurally compresses the downside cushion.

The scenario mapping reflects the consumer/inflation perspective:

- **Base Case (p50)** — OPEC+ discipline holds, balanced market.
- **Severe Case (p90)** — moderate supply disruption, $15-25/bbl premium.
- **Worst Case (p97.5)** — sustained infrastructure attack, $30-50/bbl
  premium.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `CL=F` close.
- Exogenous: DXY log-returns, GPR log-level, ^GSPC log-returns.
- GARCH(1,1) on residuals for volatility clustering.
- 1,000-path bootstrap, 12-month horizon, aggregated to 4 quarterly means.

## Consensus benchmarks

- **EIA STEO** — monthly, official forecast, Q+6 horizon.
- **World Bank Pink Sheet** — annual averages.
- **Manual YAML**: Goldman Sachs, J.P. Morgan, UBS quarterly forecasts.

## Caveats

- The model will underprice ME tail risk relative to realized outcomes in
  2019 (Abqaiq), 2022 (Russia invasion), 2024-26 (Red Sea / Iran-Israel).
  Operators should overlay scenario judgment during heightened geopolitical
  risk.
- SPR refill pace is not in the driver set — a policy shift would not be
  captured.
- WTI-Brent spread dynamics are not modeled explicitly; crossover regimes
  where the spread inverts (as occurred briefly in 2023) may leave
  residuals larger than the CIs suggest.
