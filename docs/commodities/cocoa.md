# Cocoa — `CC=F` · $/MT

ICE cocoa futures, New York contract. Tropical soft commodity with
extreme supply concentration: Ghana and Côte d'Ivoire together produce
roughly 60% of global output. 2023-24 saw prices quadruple on
consecutive failed harvests — an episode the model will remember in its
residuals for years.

## Drivers

- **US Dollar Index (DXY)** — dollar-priced, inverse correlation.
- **ENSO ONI (NOAA CPC Oceanic Niño Index)** — single biggest macro
  climate driver for West African cocoa. El Niño years drive dry
  conditions in Ghana + Côte d'Ivoire growing regions; positive ONI
  readings precede yield-shortfall episodes by 2-6 months. Enters as
  a signed-anomaly level (°C).
- **S&P 500 (^GSPC)** — weak proxy for discretionary chocolate demand.

> Drivers we'd still like: NOAA CPC African Rainfall Climatology
> (ARC2) for Ghana + Côte d'Ivoire — *queued* in Phase 21. Harmattan
> wind intensity, black pod disease pressure, swollen shoot virus
> prevalence, ICCO stocks-to-grindings ratio — no clean free APIs;
> covered indirectly via the scenario shock catalogue.

## Structural story

Cocoa is the single most production-concentrated soft commodity, and
the 2023-26 episode illustrates how badly that can bite:

- **Weather**: El Niño patterns 2023-24 dried Ghana and CI; main crop
  output fell 20-30%. Harmattan dry-season winds arrived earlier and
  harder than normal.
- **Disease**: Black pod (fungal) and swollen shoot virus have steadily
  compounded in both countries; infected trees cannot be easily
  recovered. Replanting cycle is 3-5 years before new yield.
- **Smuggling leakage**: When the Ghana-CI farmgate price spread widens
  (due to different state marketing regimes — Cocobod in Ghana, CCC in
  CI), beans smuggle across the border. This distorts reported
  production and creates volatility in the Ghana supply count.
- **Smallholder economics**: Ghana's farmgate is administered; CI is
  reference-priced but effectively near-administered. When world prices
  spike, farmgate lags by 1-2 seasons — so farmers don't get signal to
  plant more until prices have already peaked.
- **Processing**: Ivorian and Ghanaian domestic grinding capacity is
  growing. Vertical integration (Cargill, Barry Callebaut, ECOM) drives
  structural demand.
- **Demand elasticity**: Chocolate retail prices passed through ~15-25%
  of the cocoa spike; some substitution (less cocoa content in
  confectionery) is emerging.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `CC=F` close.
- Exogenous: DXY log-returns, ENSO ONI level, `^GSPC` log-returns.
- GARCH(1,1) on residuals — cocoa has enormous volatility clustering,
  so GARCH is material here.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual.
- **ICCO (International Cocoa Organization)** — quarterly
  supply-demand statements.
- **Manual YAML**: Commerzbank, UBS, agricultural research shops (Rabobank)
  publish cocoa targets.

## Caveats

- **CI is the biggest model limitation here**. The 2023-24 price spike
  was 4+ standard deviations from the estimation period — backtests will
  show Q+1 CIs materially under-covering realized.
- **Weather-dependent commodity with no weather driver in the model** —
  the single most important factor is invisible to SARIMAX. Operators
  should treat cocoa forecasts as directional only, with much wider
  practical uncertainty than the CI suggests.
- Mean reversion after the 2024-26 peak is a regime assumption — if
  Ghana / CI yields do not recover (disease / climate permanent), prices
  will not revert.
