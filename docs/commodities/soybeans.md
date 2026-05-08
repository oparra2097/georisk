# Soybeans — `ZS=F` · ¢/bu

CBOT soybean futures. The largest oilseed globally; Brazil is now the
dominant producer ahead of the US. Key feed protein for livestock and
increasingly for biofuel feedstock.

## Drivers

- **US Dollar Index (DXY)** — dollar-priced; inverse correlation on
  global export competitiveness.
- **Wheat (`ZW=F`, cross-commodity)** — grain/oilseed complex co-
  movement. Grain-oilseed substitution in feed rations and shared
  planting-acreage competition on US farms.
- **S&P 500 (^GSPC)** — risk-on proxy for global demand, especially
  Chinese crush margin economics.

> Drivers we'd like: Brazilian + Argentine rainfall anomalies (Cerrado,
> Pampas), Chinese import volumes monthly (GACC), USDA WASDE
> stocks-to-use, Argentine crush rates. China imports ~60% of global
> traded soybeans — its demand is the price anchor.

## Structural story

The US/Brazil production swap is the defining 21st century dynamic:

- **Brazil** — surpassed the US in 2018 and now produces ~155 Mt/yr
  vs US ~115 Mt/yr. Cerrado expansion continues but slower as prime
  land fills. Mato Grosso dominates production; rainfall during
  January-March planting window (summer season) is decisive.
- **Argentina** — structurally 35-50 Mt, heavily dependent on
  Pampas summer rain. Has a large crush industry (Rosario) which makes
  meal/oil export rather than whole beans.
- **US** — Midwest Corn Belt, Illinois and Iowa especially. Summer
  July-August weather drives final yield. Acreage rotates with corn;
  soybean-corn price ratio is a planting signal.
- **China** — crush demand for pig feed (hog herd recovery from ASF)
  + soybean oil for food. Reserve release policy can swing prices
  sharply. Political hedging: US-China tariff cycles drove Brazil's
  market share gains.
- **Biofuel demand** — US renewable diesel feedstock demand has grown
  materially (2022-26); soybean oil now ~14% of US demand vs ~5% a
  decade ago. Argentine biodiesel policy affects meal/oil spread.

Argentine / Brazilian summer rainfall is the single biggest uncaptured
driver — drought years (2021-22 Argentina, 2015-16 Brazil) spike prices
20-40%.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `ZS=F` close.
- Exogenous: DXY log-returns, Wheat log-returns, ^GSPC log-returns.
- GARCH(1,1) on residuals.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual.
- **USDA WASDE** — monthly.
- **IGC** — monthly.
- **Manual YAML**: Rabobank, UBS, JPM publish grain targets.

## Caveats

- Weather is the missing driver. South American summer (Dec-Mar) and US
  summer (Jul-Aug) rainfall would materially tighten the CI.
- Trade policy (China tariff retaliation, Brazil export licensing)
  creates step-function moves.
- Biofuel policy path (federal RVO, state-level LCFS) is a structural
  demand uplift not captured in the AR component.
- Crush margins (soybean oil + soybean meal vs whole bean) affect
  spread dynamics the single-benchmark model cannot see.
