# TTF Gas — `TTF=F` · €/MWh

Title Transfer Facility, Netherlands — the European wholesale gas
benchmark. Reference for Northwest European physical and most European gas
hedges.

## Drivers

- **US Dollar Index (DXY)** — indirectly through LNG cargo pull between
  Europe and Asia; stronger USD weakens European purchasing power.
- **Henry Hub gas (`NG=F`)** — cross-commodity driver. US LNG is the
  marginal supplier to Europe post-2022; HH + liquefaction + shipping
  sets the floor for TTF.
- **Geopolitical Risk Index** — captures Russia-NATO tension risk to
  remaining pipeline supply (TurkStream), plus Middle East / Red Sea
  shipping disruption affecting Qatar LNG routing.

> Drivers we'd like: GIE AGSI+ EU gas storage, Norwegian Continental
> Shelf pipeline flows, Qatar LNG outage schedule. Norwegian supply is
> now ~50% of EU imports; any extended outage at Troll or Aasta Hansteen
> would dominate TTF. Storage levels drive the pre-winter basis.

## Structural story

Pre-2022 TTF was an afterthought, tracking HH loosely with a shipping
premium. The Russian pipeline cut inverted the market: Europe now
competes with JKM (Japan/Korea) for flexible LNG cargos at prices well
above HH + freight. Key dynamics in the current regime:

- **Norwegian pipeline supply** — now the single largest source;
  Equinor-operated with scheduled maintenance and occasional unscheduled
  outages (Nyhamna).
- **LNG arb vs JKM** — TTF must clear above JKM + shipping differential
  to pull cargos. When Asia is tight, TTF premium widens.
- **EU storage levels** — 80% full by Oct mandate (post-2022 regulation).
  Storage bufferedraughts during winter drive the summer-winter spread.
- **Qatar tail risk** — Qatar supplies ~30% of global LNG; any strike on
  Ras Laffan or disruption in the Strait of Hormuz routes would spike
  TTF as Asia competes harder for remaining supply.
- **Industrial demand destruction** — ammonia, glass, aluminum smelters
  curtailed in 2022-23; partial recovery but structurally lower.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `TTF=F` close.
- Exogenous: DXY log-returns, HH log-returns, GPR log-level.
- GARCH(1,1) on residuals; expect wide CIs given regime volatility.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **EIA STEO** does not cover TTF directly.
- **World Bank Pink Sheet** — European gas annual.
- **Manual YAML**: European banks, especially UBS and BNP Paribas,
  publish TTF quarterly views.

## Caveats

- **Short history in the current regime**: the model has only ~3 years
  of post-2022 data where TTF and HH are cleanly linked. Pre-2022
  observations should ideally be down-weighted, which the current
  implementation does not do.
- Pipeline outage risk is bi-modal (operational or not) — normal
  Gaussian residuals will under-state impact.
- Qatar-specific tail risk is not explicitly modeled; GPR gives some
  signal but is global, not Qatar-specific.
- European winter weather anomalies (2025-26 mild autumn compressed TTF
  ~30%) are not captured without an HDD driver.
