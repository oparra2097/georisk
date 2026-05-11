# WTI Crude Oil — `CL=F` · $/bbl

West Texas Intermediate light sweet crude, delivered Cushing, Oklahoma. The
benchmark for US crude pricing and the reference for most North American
domestic barrels.

## Drivers

- **US Dollar Index (DXY, FRED: DTWEXBGS)** — crude is globally dollar-
  denominated. A stronger dollar tightens buying power for non-US refiners
  and correlates negatively with oil on medium horizons.
- **Weekly U.S. crude inventories (FRED: WCESTUS1)** — the single most-
  watched fundamental for short-horizon WTI moves. EIA's weekly Thursday
  print drives ±2-4% next-week price reactions when the build/draw misses
  expectations. Enters the model as a log-return on the stocks level.
- **CBOE Crude Oil Volatility Index (FRED: OVXCLS)** — implied vol from
  USO ETF options, a forward-looking signal for realised WTI/Brent
  volatility. Enters as a log-level signal in the SARIMAX mean equation;
  feeding it into the GARCH variance equation directly (true GARCH-X)
  would require a custom volatility model and is left as future work.
- **Geopolitical Risk Index (Caldara & Iacoviello)** — captures Middle East
  tensions, sanctions, shipping lane disruptions (Strait of Hormuz, Bab
  al-Mandab). Positive price response skewed to the upside tail.
- **S&P 500 (yfinance ^GSPC)** — proxy for global risk appetite and cyclic
  oil demand. Growth shocks propagate through equities before they land in
  inventory data.

> Drivers we'd still like but don't yet have wired: OPEC+ spare capacity
> (no clean free API; the shock catalogue handles it instead), US Baker
> Hughes rig count, and EIA STEO official quarterly forecasts. STEO
> integration would require an EIA Open Data API v2 key; tracked under
> "What we don't model yet" below.

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
- Exogenous regressors: DXY log-returns, U.S. crude inventory log-returns
  (`WCESTUS1`), OVX log-level, GPR log-level, `^GSPC` log-returns.
- GARCH(1,1) on residuals for volatility clustering. (Future work: feed
  OVX into the variance equation directly — true GARCH-X — via a custom
  arch volatility model so confidence bands react to forward-looking
  implied vol, not only past residuals.)
- 1,000-path bootstrap, 12-month horizon, aggregated to 4 quarterly means.
- Forecasts anchored to the WTI futures curve via horizon-weighted
  shrinkage (Q+1 30% curve weight → Q+4 75%; see `forward_curve.py`).
- Scenario shocks (OPEC+ production, SPR flow, ME risk premium, demand
  shock) compose on top of the anchored base via the elasticity
  catalogue in `commodity_models.SHOCKS`. The framework mirrors the
  Kilian (2009) / Baumeister & Kilian (2015) supply / aggregate-demand /
  oil-specific-demand decomposition.

## What we don't model yet (and why)

- **VECM on spot + 12M futures** — Kilian-Baumeister-style cointegrating
  model that consistently beats SARIMAX at 1-3M oil price horizons in the
  literature. Adds a second model to the stack; tracked as a future PR.
- **EIA STEO official forecast blend** — requires an EIA Open Data API
  v2 key (free but operator-registered). World Bank Pink Sheet + manual
  YAML cover the consensus benchmark surface for now.
- **OPEC+ spare capacity / production data** — no clean free API; the
  `opec_production` shock in the scenario catalogue handles operator-
  driven what-ifs.
- **Forecast combination** — inverse-RMSE-weighted blend of {SARIMAX,
  VECM, forward curve, EIA STEO}. Standard literature practice; queued
  for after the model-stack expansion.

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
