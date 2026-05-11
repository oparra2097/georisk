# Natural Gas (Henry Hub) — `NG=F` · $/MMBtu

US natural gas benchmark, delivered at the Henry Hub pipeline
interconnect in Louisiana. Reference for North American physical gas and
most US LNG exports.

## Drivers

- **US Dollar Index (DXY)** — indirect channel via LNG export
  competitiveness; stronger dollar → higher LNG cost → softer global pull
  on US cargos → more domestic gas → lower HH.
- **Henry Hub spot price (FRED: DHHNGSP)** — the EIA-maintained daily
  Henry Hub spot benchmark, resampled monthly. Spot serves as a leading
  indicator for the front-month futures contract and dampens divergence
  between the spot fundamental and the rolling futures roll cost.
- **WTI Crude price** — cross-commodity driver capturing associated gas
  production from oil-weighted basins (Permian, Eagle Ford). Higher oil
  prices → more shale rig activity → more associated gas → bearish HH.
- **S&P 500 (^GSPC)** — weak proxy for industrial demand and power burn
  from economic growth.

> Drivers we'd still like but don't yet have wired: EIA weekly working
> gas in storage (Working Gas in Underground Storage; requires EIA Open
> Data API v2), cooling / heating degree days (NOAA — FRED's coverage is
> too sparse to use directly), LNG feedgas / export capacity utilization.
> HDD/CDD alone would materially improve winter forecasts; the SARIMAX
> seasonal component is a weak proxy.

## Structural story

HH is the most volatile liquid commodity in this set. Key dynamics:

- **Winter heating demand** — storage draws during December-February can
  spike prices 3-5x; a warm winter can cut them by half. The SARIMAX
  seasonal component is weak without explicit HDD; expect larger winter
  CIs than summer.
- **LNG exports** — Sabine Pass, Cameron, Freeport, Plaquemines have moved
  the US from a closed market to a globally-linked one. LNG feedgas demand
  is ~14-16 Bcf/d in 2026 and growing; outages (Freeport 2022) tighten the
  domestic market.
- **Associated gas** — Permian associated gas is price-inelastic on the
  supply side; producers drill for oil. This sets a floor for HH and
  creates chronic basis weakness at Waha.
- **Rig count / dry gas producers** — Haynesville and Appalachian rigs
  respond to prices with a 6-12 month lag.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `NG=F` close.
- Exogenous: DXY log-returns, HH spot log-returns (`DHHNGSP`), WTI
  log-returns, `^GSPC` log-returns.
- GARCH(1,1) on residuals — winter regime and storage shocks create
  volatility clustering the GARCH term captures.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.
- Forecasts anchored to the HH futures curve via horizon-weighted
  shrinkage (see `forward_curve.py`).
- Scenario shocks (LNG outage, HDD anomaly, US dry-gas production) layer
  on top via the elasticity catalogue.

## What we don't model yet (and why)

- **EIA Working Gas in Storage** — the single most-watched HH
  fundamental. Requires EIA Open Data API v2 integration; queued.
- **NOAA HDD / CDD** — explicit weather signal. Would materially tighten
  winter Q+1/Q+2 forecasts; NOAA direct adapter needed.
- **LNG feedgas / export utilization** — currently captured indirectly
  via the `lng_outage` scenario shock; a continuous driver would help.
- **VECM on spot + 12M futures** — same as oil; queued for future PR.

## Consensus benchmarks

- **EIA STEO** — monthly, includes HH forecast.
- **World Bank Pink Sheet** — annual.
- **Manual YAML**: US banks + hedge funds publish seasonal HH calls.

## Caveats

- Winter tail events (Feb 2021 Uri, Dec 2022 Elliott) are fat-tail and
  under-covered by the 95% CI.
- Storage pre-announcement is a major weekly price driver (EIA Thursday
  release); the monthly model cannot see it.
- LNG outage risk is a step-function event (Freeport fire 2022 cut HH
  40% over 8 weeks); not captured by any continuous driver.
