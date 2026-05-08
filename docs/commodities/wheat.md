# Wheat — `ZW=F` · ¢/bu

CBOT Chicago soft red winter wheat futures. Global staple grain; unlike
cocoa or coffee, wheat is fungible across many major producers (US, EU,
Russia, Ukraine, Canada, Australia, Argentina), so regional shocks are
buffered by global stocks-to-use dynamics.

## Drivers

- **US Dollar Index (DXY)** — dollar-priced; inverse correlation. USD
  weakness makes US wheat more competitive vs Black Sea and Australian
  wheat on the export market.
- **Geopolitical Risk Index** — captures the Black Sea (Ukraine / Russia)
  risk premium that became dominant after Feb 2022.
- **WTI Crude (`CL=F`, cross-commodity)** — energy cost in farming (diesel,
  fertilizer feedstock, irrigation). Grains and oil co-move on
  reflation/inflation themes.

> Drivers we'd like: USDA WASDE stocks-to-use ratio, US Drought Monitor
> Palmer index for HRW/SRW regions, EU drought index, Black Sea export
> volumes, Russian export tax / quota announcements. Stocks-to-use is
> the classic price anchor; a ratio below 20% historically coincides
> with price spikes.

## Structural story

Wheat is the most genuinely global grain, which both limits shock
transmission and makes any single-country supply shock important:

- **Black Sea** — Ukraine was ~8-12% of global wheat exports pre-war;
  Russia is now ~18-20%. The 2022-24 Black Sea Grain Initiative and
  subsequent withdrawal created recurring export uncertainty. In 2026
  the corridor functions with humanitarian cargo coordination, but
  physical infrastructure (Odesa port, insurance premiums) remains
  fragile.
- **US** — Hard red winter wheat (Kansas, Oklahoma, Nebraska) has faced
  recurring drought in 2022-25; HRW acreage has declined, replaced by
  corn and sorghum in parts of the plains. Soft red winter (Midwest,
  South) is more stable.
- **Australia** — Swings from 30+ Mt crop (wet year) to sub-20 Mt (El
  Niño drought). Tight correlation with ENSO state.
- **EU + UK** — Structural producer; wet harvests in France and Germany
  2023-24 reduced milling quality, pushed up the feed-to-milling spread.
- **Substitution elasticity**: Wheat competes with corn and rice in feed
  and with rice in human consumption in parts of Asia and Africa. Demand
  elasticity across grains is moderate.

The model's drivers proxy broad signal (USD, GPR, oil) but miss the
agronomic specifics.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `ZW=F` close.
- Exogenous: DXY log-returns, GPR log-level, WTI log-returns.
- GARCH(1,1) on residuals.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual (US HRW benchmark).
- **USDA WASDE** — monthly (official supply-demand).
- **IGC (International Grains Council)** — monthly.
- **Manual YAML**: Commerzbank, UBS, Rabobank publish grain targets.

## Caveats

- Weather signal missing — US Drought Monitor and El Niño indices would
  materially improve the model.
- Black Sea corridor risk is step-function, not continuous — GPR is a
  weak proxy.
- Trade policy shocks (Russian export tax, Indian wheat ban, EU tariffs)
  move prices instantly and aren't captured by any continuous driver.
- Different wheat classes (HRW, SRW, hard red spring, durum) have
  divergent price dynamics; the model uses the ZW=F single benchmark.
