# Central Bank Monthly Data — Extension Plan

**Goal:** upgrade the Credit Default model from annual/quarterly to
monthly-cadence macro inputs, sourced primarily from central bank
websites where the IMF/WB pipeline is too slow.

## What already exists (don't duplicate)

The `backend/data_sources/` layer already has the pattern in place —
this plan slots into it, not around it.

| Layer | Existing module | What it does |
| --- | --- | --- |
| Monthly reserves | `em_reserves.py` | IMF SDDS base + direct scrape of BCB, Banxico, TCMB |
| Quarterly IFS | `imf_ifs.py` | DBnomics-fronted IMF IFS pull (`RAFA_USD`, `BCA_NGDPD_BP6_PT`, ...) |
| Quarterly trade | `trade_quarterly.py` | X/M and current account higher-frequency |
| EM FX | `em_fx_rates.py` | Daily EM FX pairs |
| VIX / GPR / ENSO | `vix_history.py`, `gpr_index.py`, `enso_index.py` | Global stress and geo indices |
| BLS / ONS / Eurostat | `bls_*.py`, `ons_cpi.py`, `eurostat_hicp.py` | US / UK / EU consumer prices |
| FRED | `fred_client.py` | Anything US-adjacent that FRED carries |

The credit-default fitter already has a `get_history_panel_quarterly()`
and `build_training_panel_quarterly()` (fit.py:156-…), so the quarterly
grain is wired end-to-end. Monthly is the next step down.

## Preferred data-source hierarchy

For each indicator we want at monthly cadence, work down this list until
you get a hit:

1. **IMF SDMX / DBnomics** — free, one library call, ~2–6 week lag. Use
   this first for reserves, IIP, IFS money & banking.
2. **FRED** — for anything USA-adjacent (already wired via `fred_client`).
3. **Eurostat / OECD SDMX** — for EA/OECD sovereigns, same style as IMF.
4. **Central bank direct** — for EMs that publish faster than IMF SDDS
   propagates. Follow the `em_reserves.py` template: JSON/CSV API where
   possible, HTML/PDF scraping only as last resort.
5. **Bloomberg / market data** — if a client has entitlements. Not
   assumed available.

## Central bank endpoints worth wiring (ranked by coverage impact)

Everything below is free, public, and no auth unless flagged.

### Already done (in em_reserves.py)

- **Brazil — BCB** (Banco Central do Brasil) SGS API
  `https://api.bcb.gov.br/dados/serie/bcdata.sgs.<CODE>/dados?formato=json`
  No key. ~6h latency on release day.
- **Mexico — Banxico** SIE API
  `https://www.banxico.org.mx/SieAPIRest/service/v1/series/<ID>/datos`
  Requires `BANXICO_TOKEN`. Free after email registration.
- **Turkey — CBRT / TCMB** EVDS API
  `https://evds2.tcmb.gov.tr/service/evds/series=<CODE>`
  Requires `TCMB_EVDS_KEY`. Free after registration.

### High-priority additions (largest EMs)

- **India — RBI**. Two options:
  - RBI's "Database on Indian Economy" (DBIE): CSV downloads at
    `https://data.rbi.org.in/DBIE/#/dbie/reports/statistics/Time%20Series/*`
    (scrape-only — no API)
  - Cleaner: DBnomics carries RBI series under provider `RBI`. Prefer this.
- **South Africa — SARB**. Web query at
  `https://www.resbank.co.za/en/home/what-we-do/statistics/releases/*`.
  DBnomics provider `SARB` mirrors monthly bulletin series — use that.
- **Indonesia — BI (Bank Indonesia)**. Statistics portal at
  `https://www.bi.go.id/en/statistik/*`. No API; HTML tables. Alternative:
  IMF IFS carries `IDN` monthly at ~3 week lag — probably sufficient.
- **Nigeria — CBN**. `https://www.cbn.gov.ng/rates/*`. HTML/PDF only.
  IMF IFS also carries `NGA` — start there.
- **China — PBoC / NBS**. Notoriously fragmented. `stats.gov.cn` has some
  monthly series; official reserves at `http://www.pbc.gov.cn/en`. CEIC /
  DBnomics `CN` mirrors.
- **Argentina — BCRA**. `https://www.bcra.gob.ar/PublicacionesEstadisticas/*`.
  DBnomics `BCRA` carries the daily FX and monthly monetary base.

### G10 / developed (for benchmarking)

- **Fed** — via FRED (already wired).
- **ECB** — SDMX at `https://data-api.ecb.europa.eu/service/data/<flow>`.
  No key. Cleanest SDMX in the world, use directly.
- **BOE** — `https://www.bankofengland.co.uk/boeapps/database/*`. Ugly
  but scriptable; DBnomics `BOE` is better.
- **BOJ** — `https://www.stat-search.boj.or.jp/index_en.html`. HTML scrape
  or CSV download.
- **BOC / BdF / Bundesbank** — all SDMX-native.

## Indicators worth pulling monthly

For each, the marginal signal-to-noise vs. our current annual/quarterly:

| Indicator | Current cadence | Target cadence | Rationale |
| --- | --- | --- | --- |
| Reserves (`RAFA_USD`) | Monthly (done) | ✓ | Early warning: reserves drain 3–6 months before default |
| CPI YoY | Annual (WEO) | Monthly | Inflation surprises are near-real-time distress signals |
| Policy rate | — | Monthly | Rate hikes to defend the FX peg = pre-default signal |
| FX (USD/local, spot & 12m fwd) | — | Daily → monthly avg | FX vol is the sharpest EM distress signal |
| Monetary base | — | Monthly | Base expansion → inflation → credit stress |
| Trade balance | Annual (WEO) | Monthly | X/M gap widening is a lead indicator |
| Bond yield (10y local) | — | Daily → monthly avg | Domestic funding cost pressure |
| CDS spread (5y USD) | — | Daily → monthly avg | Market-implied PD — direct cross-check on our model output |
| Sovereign bond spread vs UST | — | Daily → monthly avg | EMBI-style spread, forward-looking |

Skipping monthly: debt/GDP (annual makes sense), governance/WGI (survey,
annual), interest/revenue (fiscal series, quarterly at best).

## Implementation plan (staged)

### Stage 1 — Extend `em_reserves.py` pattern to 3 more central banks

Add `sarb_client.py`, `rbi_client.py`, `bi_client.py` following the
BCB/Banxico/TCMB template already in `em_reserves.py`. Each exposes a
single `get_monthly_series(indicator)` returning
`{ISO3: {YYYY-MM: value}}`. About 100 lines each.

### Stage 2 — Add a `get_history_panel_monthly()` in `data.py`

Long-format DataFrame keyed on `(iso3, year, month)`. Reuse the merge
logic in the existing quarterly panel. Indicator columns fall back
gracefully to quarterly then annual values when a monthly series is
missing — the fitter already handles NaN imputation.

### Stage 3 — Extend the fitter to monthly horizons

Add `build_training_panel_monthly()` and `fit_gbm_monthly()`. Horizons
in months (3m, 6m, 12m) — sovereign defaults are rare-event enough
that <3m windows aren't worth fitting.

### Stage 4 — Extend the dashboard chart

The `renderHistoryChart()` in `static/js/credit_default.js` already
handles horizon toggling. Add a `cadence` toggle (Annual / Quarterly /
Monthly) to the country drilldown; the backend `get_country_history`
already accepts `cadence`.

### Stage 5 — Add a CDS spread comparison line

For the ~70 sovereigns with liquid 5y CDS, overlay market-implied PD
(`spread / (1 - LGD)`, LGD ≈ 60%) as a second line on the drilldown
chart. Data source: DBnomics `MARKIT/CDS` or scrape from
`https://www.assa.org.uk/committees/*` (free daily CDS for 30 sovereigns).

## What NOT to do

- **Don't scrape HTML tables when SDMX/DBnomics has the same series.**
  Cheap looking; brittle in practice. HTML scrapers break every 6 months.
- **Don't add monthly data for governance/WGI.** They're annual surveys.
  A "monthly WGI" is fabrication.
- **Don't fit at monthly cadence with the current default panel.** Only
  ~250 sovereign default events exist total; monthly grain multiplies
  the observation count without adding events. Use quarterly for the
  fit; use monthly for the dashboard chart and the early-warning
  indicators.
- **Don't build a scraper before checking DBnomics.** DBnomics
  (`api.db.nomics.world/v22`) mirrors ~30 central banks and hundreds of
  statistical offices with a uniform JSON API. It should cover 80% of
  the target list.

## Effort estimate

- Stage 1 (3 CB clients + reserves extension): ~1 day of engineering.
- Stage 2 (monthly panel): ~half a day.
- Stage 3 (monthly fit): ~half a day if the quarterly path already works.
- Stage 4 (dashboard cadence toggle): ~2 hours.
- Stage 5 (CDS overlay): ~half a day, mostly data-quality work.

Total: **~3 days of focused work** to have a monthly-cadence credit
default model with the current architecture.
