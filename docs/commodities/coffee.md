# Coffee — `KC=F` · ¢/lb

ICE Coffee C Arabica futures. Tropical soft commodity; Brazil is the
dominant Arabica producer, Vietnam dominates Robusta. Consecutive
supply shocks in 2023-25 drove Arabica prices to multi-decade highs.

## Drivers

- **US Dollar Index (DXY)** — dollar-priced; inverse correlation. BRL
  (Brazilian Real) weakness also matters heavily because Brazilian
  farmers sell in USD but cost in BRL — weak BRL incentivizes selling,
  adds supply.
- **S&P 500 (^GSPC)** — weak proxy for discretionary coffee demand
  (café consumption, specialty) and general risk appetite.

> Drivers we'd like: Brazilian Cerrado rainfall anomaly, Brazilian frost
> incidence (winter months May-August), Vietnamese Central Highlands
> rainfall, ICO indicator prices (Arabica / Robusta composite), coffee
> stocks (ICE certified + roaster inventories).

## Structural story

Coffee's dual-species market (Arabica vs Robusta) and hyper-concentrated
supply make it one of the more volatile softs:

- **Brazil** — ~40% of global coffee, predominantly Arabica in Cerrado
  Mineiro, South Minas, Mogiana. Biennial yield cycle (on-year vs
  off-year) creates structural volatility. Winter frost risk (May-Aug)
  is the major tail — the 2021 frost (third-worst on record) drove the
  2021-22 price spike.
- **Vietnam** — ~17% of global coffee, overwhelmingly Robusta in
  Central Highlands (Dak Lak, Lam Dong). El Niño 2023-24 dried the
  growing regions; 2024-25 harvest was the smallest in a decade.
  Vietnamese producer hoarding when prices rise tightens supply
  further.
- **Colombia, Ethiopia, Honduras, Guatemala** — secondary Arabica
  suppliers; collectively 20% of global output. Coffee rust disease
  (La Roya) hits Central America cyclically.
- **Arabica vs Robusta spread**: Historically Arabica trades at a
  premium; in 2024 Robusta briefly approached parity as Vietnamese
  supply collapsed. The spread is a useful state signal.
- **Roasting / end demand**: Starbucks, JAB Holding (Keurig, Peet's),
  Nestlé Nespresso drive premium Arabica; blends use Robusta. Price
  pass-through to retail ran ~20-30% of the farmgate spike.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `KC=F` close (Arabica).
- Exogenous: DXY log-returns, ^GSPC log-returns.
- GARCH(1,1) on residuals — coffee has significant volatility clustering.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual (Arabica benchmark).
- **ICO** — monthly composite indicator prices.
- **USDA FAS** — semi-annual crop outlook.
- **Manual YAML**: Rabobank, Marex, BNP Paribas, Commerzbank publish
  coffee targets.

## Caveats

- **Weather driver missing** — Brazilian Cerrado rainfall and frost
  incidence are the two single largest price drivers and invisible to
  SARIMAX. Operators should treat coffee CIs as directional only.
- Arabica/Robusta divergence is not captured; a Robusta shock (2024)
  affects global specialty/commercial coffee economics in ways the
  Arabica-only model cannot see.
- Biennial cycle adds predictable alternation the model's single
  seasonal term does not perfectly capture.
- Futures expiry rollover can add ~1-2 cent noise to the front-month
  series.
