# Aluminum — `ALI=F` · $/MT

CME Group Aluminum futures (LME reference). Industrial metal, extremely
energy-intensive to produce (~14 MWh per tonne of primary aluminum).
Used in transportation (cars, planes), packaging, construction, and
electrical transmission.

## Drivers

- **US Dollar Index (DXY)** — dollar-priced, inverse correlation.
- **Copper (`HG=F`, cross-commodity)** — industrial metal complex
  co-movement. Copper and aluminum share demand drivers (EV, grid,
  construction) though copper is more cyclically sensitive.
- **Henry Hub gas (`NG=F`, cross-commodity)** — energy price proxy.
  Aluminum smelting is 30-40% energy cost; power prices correlate with
  gas prices in most regions.

> Drivers we'd like: China aluminum production monthly (CNIA), LME
> aluminum stocks, SHFE aluminum stocks, EU power prices, Yunnan hydro
> utilization. China is ~60% of global primary aluminum; Yunnan
> province hydro rationing in dry seasons has driven multiple supply
> shocks since 2022.

## Structural story

Aluminum is the poster child for "green premium" commodity economics:

- **Supply**: ~60% of global primary aluminum is Chinese, with capacity
  capped at 45 Mt/year by central policy. Yunnan (hydro-powered) has
  faced recurring power rationing in dry seasons, pushing production
  west to Gansu/Xinjiang (coal-powered) with capacity-swap rules.
- **Energy cost is 30-40% of smelting economics** — European smelters
  have been squeezed chronically since 2022 gas prices spiked. Multiple
  EU smelters curtailed; restart economics require sustained sub-€80/MWh
  power which is not currently available.
- **Green / low-carbon aluminum premium** — EU CBAM (Carbon Border
  Adjustment Mechanism) phasing in 2026; creates a premium for
  low-emission (hydro or renewable-powered) aluminum vs coal-powered.
  Russian aluminum (Rusal) discount persists post-2022.
- **Demand**: Autos (lightweighting for EVs adds ~50 kg Al per vehicle),
  packaging (stable), construction (cyclical), electrical (grid
  expansion is bullish structural).

Primary production is inelastic on the supply side — smelters are slow
to start and stop, and once stopped are very expensive to restart
(molten metal bath freezes). This creates asymmetric price response:
slow downside adjustment when demand weakens, sharp upside when supply
is curtailed.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `ALI=F` close.
- Exogenous: DXY log-returns, Copper log-returns, HH log-returns.
- GARCH(1,1) on residuals.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual.
- **LME** spot and basis.
- **Manual YAML**: Goldman Sachs, JPM, CRU, Harbor Aluminum publish
  quarterly targets.

## Caveats

- **Yunnan hydro risk** is the single largest uncaptured driver; dry
  seasons in Southwest China reliably create supply scares.
- **European smelter restart / further curtailment** is path-dependent
  on EU power prices — not clean in the current driver set.
- CBAM phase-in creates structural price divergence between green and
  brown aluminum that the single LME benchmark doesn't reflect.
- `ALI=F` has thinner trading volume than copper or gold — microstructure
  noise is larger.
