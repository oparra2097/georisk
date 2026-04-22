# Copper — `HG=F` · ¢/lb

COMEX copper futures ("Dr. Copper"). The industrial metal bellwether —
widely used as a leading indicator for global manufacturing activity
because copper appears in virtually every electrified and built
application.

## Drivers

- **US Dollar Index (DXY)** — dollar-priced, inverse correlation.
- **S&P 500 (^GSPC)** — risk-on proxy. In normal regimes copper and
  equities co-move; in commodity-specific shocks they diverge.
- **10Y TIPS real yield (FRED: DFII10)** — weak direct link, but lower
  real yields support industrial capex which drives copper demand.

> Drivers we'd like: China Caixin manufacturing PMI, LME copper stocks,
> SHFE copper stocks, Codelco / BHP production guidance, Chile water
> levels. China is ~55% of global copper consumption and drives the
> price at the margin. LME + SHFE stocks are the clearest near-term
> tightness signal.

## Structural story

Copper is the commodity where structural demand and cyclical demand
collide most visibly.

- **Cyclical demand**: Chinese property construction, global auto
  production, industrial machinery. Downside from property slowdowns
  (2022-24), upside from stimulus cycles.
- **Structural demand**: EV production (EVs use ~4x the copper of ICE
  vehicles), grid expansion for renewables (wind and solar require
  ~2-5x more copper per MWh than fossil thermal), AI data centers
  (increasingly a swing factor).
- **Supply**: Chile and Peru produce ~40% of world copper. Chile faces
  declining ore grades, water rationing, and labor tension; Peru has
  chronic community protests at Las Bambas and Antamina. DRC (Katanga)
  is the fastest-growing source.
- **Inventory cycle**: LME stocks swing from 150 kt (tight) to 400+ kt
  (loose). Stock draws below 100 kt historically precede price spikes.

The model captures the macro drivers but not inventory or Chinese PMI
explicitly. Copper-as-proxy is a standard pattern: because its price is
a leading indicator for activity, the S&P 500 in the driver set
partially double-books the signal. Consider it a "where is the global
cycle?" model more than a copper-specific one.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `HG=F` close.
- Exogenous: DXY log-returns, ^GSPC log-returns, 10Y TIPS first-difference.
- GARCH(1,1) on residuals.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual.
- **LME** spot and basis — reference benchmark.
- **Manual YAML**: GS, JPM, UBS, Citi publish copper quarterly targets;
  ICSG annual supply-demand statement.

## Caveats

- China manufacturing PMI would be the highest-value additional driver;
  the model blurs China-specific signal into global macro.
- Copper squeezes (LME 2021 Glencore) and backwardation regimes carry
  price signals invisible to the monthly model.
- Long-horizon forecasts will struggle across the EV/grid transition
  which is creating a structural demand lift not in historical data.
