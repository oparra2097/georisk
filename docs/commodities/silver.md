# Silver — `SI=F` · $/troy oz

COMEX silver futures. A hybrid asset — roughly half monetary (tracks
gold), half industrial (tracks copper and solar demand). Historically
more volatile than gold, with wider price ranges and larger drawdowns.

## Drivers

- **10Y TIPS real yield (FRED: DFII10)** — monetary component, same
  mechanism as gold.
- **US Dollar Index (DXY)** — dollar-priced, inverse correlation.
- **Gold (`GC=F`, cross-commodity)** — the gold-silver ratio exhibits
  mean reversion on 3-5 year horizons. Silver tracks gold on macro flows.
- **Copper (`HG=F`, cross-commodity)** — industrial demand proxy.
  Captures the second half of silver's demand curve: solar panels,
  electronics, electrical contacts.

> Drivers we'd like: Silver Institute industrial demand stats, ETF
> holdings, gold-silver ratio explicitly (currently implicit through
> both gold and silver being in the data). Solar panel demand is the
> fastest-growing component but tracked only annually.

## Structural story

Silver has always had this dual nature, but the mix has shifted. In the
1980s it was ~40% industrial / 60% monetary. In 2026 it is more like
55% industrial / 45% monetary, driven by:

- **Solar panel demand** — PV cells use ~15 grams of silver per panel.
  Global PV deployment grew from ~170 GW/year in 2022 to ~600 GW/year
  projected in 2026. Silver demand from solar alone is ~200 million
  ounces/year, roughly 20% of total global demand.
- **Electronics and electrical** — EVs use 25-50g silver per vehicle;
  5G infrastructure and data centers add marginal demand.
- **Monetary / investment** — ETFs, COMEX vault holdings, retail bar
  and coin demand. Tracks gold with amplified vol.

The gold-silver ratio (GSR) ran at 80-90 in 2023, compressed toward 70
in 2024-25 on industrial demand, widened to 75-80 in early 2026 on
gold's structural bid. Model doesn't force mean reversion in the ratio —
that's captured via the gold cross-commodity driver.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `SI=F` close.
- Exogenous: 10Y TIPS first-difference, DXY log-returns, Gold log-returns,
  Copper log-returns.
- GARCH(1,1) on residuals — silver has meaningful volatility clustering.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual.
- **Manual YAML**: bullion banks publish silver targets; Silver Institute
  issues supply/demand forecasts.

## Caveats

- **Industrial cycle risk** — a solar demand pullback (e.g. China policy
  shift) would decouple silver from gold in ways the cross-commodity
  driver captures imperfectly.
- **Squeeze risk** — silver has historically seen speculative squeezes
  (1980 Hunt, Jan 2021 reddit). These are regime events not captured by
  continuous drivers; expect residuals >3 std dev in such episodes.
- CI will likely be under-coverage during reflationary regime shifts
  where gold and copper both move sharply; silver amplifies both.
