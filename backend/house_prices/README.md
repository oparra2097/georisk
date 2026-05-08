# Parra Macro — US House Prices

A unified US house-price product under the Parra Macro umbrella. Served at
`/house-prices` (admin-gated) and under the `/api/house-prices/*` namespace.

## Why this product

Most HPI tools show one index at one level (FHFA state or Zillow metro).
This product fuses three authoritative sources so the same dashboard can
answer a regional macro question ("how's the West?"), a state-level
business question ("where are prices rolling over?"), and a granular
local question ("what's happening in Los Angeles County?") without
leaving the page.

## Data sources

| Source | Levels | Frequency | Lag | License |
|---|---|---|---|---|
| **FHFA HPI** (all-transactions) | national · region · state · MSA | quarterly | ~75 days | Public domain |
| **FHFA County HPI** | county | annual | ~180 days | Public domain |
| **S&P/Case-Shiller** via FRED | national · 20 metros | monthly | ~60 days | FRED public use |
| **Zillow ZHVI — Metro** | MSA · national | monthly | ~30 days | CC BY 4.0 |
| **Zillow ZHVI — County** | county | monthly | ~30 days | CC BY 4.0 |
| **Zillow ZHVI — ZIP** | ZIP | monthly | ~30 days | CC BY 4.0 |

All sources are free. Zillow requires attribution (shown in the dashboard
footer and in `sources.py`).

## Architecture

```
backend/house_prices/
├── sources.py            Source catalogue (URLs, licenses, levels)
├── fetchers/
│   ├── fhfa.py           FHFA master (CSV) + county (CSV) parsers with
│   │                     7-day disk cache in $DATA_DIR/fhfa_hpi*.json
│   ├── case_shiller.py   20-city composite via the existing fred_client
│   └── zillow.py         Metro/county/ZIP ZHVI wide→long with disk cache
├── indices.py            Pure-stdlib: summarize(), history(), group_by_entity()
├── service.py            Cached facade: ensure_built(), refresh(), get_*()
├── routes.py             Flask blueprint (7 endpoints, admin-gated)
└── README.md             ← you are here
```

Zero dependency on `backend/macro_model/` — this product is fully
independent, mirrors the `macro_model` package layout, and ships on the
same deploy.

## Metrics surfaced per entity

For every (level, code) entity, `indices.summarize` returns:

- `latest_index` – most recent index value
- `pop_pct` – period-over-period % change (MoM for monthly, QoQ for quarterly)
- `yoy_pct` – year-over-year % change
- `yoy_3y_avg` – mean YoY over last 3 years (trend benchmark)
- `from_peak_pct` – current vs historical peak (negative = correction depth)
- `peak_date` – when the peak occurred
- `zscore_yoy` – current YoY normalized to its own 10-year history
- `n_obs` – total observations used

`history()` returns the full time series for charting.

## API

All endpoints under `/api/house-prices`. All require admin-granted
`hpi_access` on the user account. Shape is consistent JSON.

| Method | Path | Returns |
|---|---|---|
| GET | `/status` | Build state, entity count, build error if any |
| GET | `/sources` | Source catalogue |
| GET | `/summary` | National + 4 census regions for the hero block |
| GET | `/level/<level>` | All entities at a level, sorted by YoY desc |
| GET | `/entity/<level>/<code>` | One entity summary + full history |
| GET | `/history/<level>/<code>?min_year=2000` | Just the time series |
| POST | `/refresh?zip=0` | Force re-download (set zip=1 to include ZIP-level) |

`<level>` is one of: `national`, `region`, `state`, `msa`, `county`, `zip`.

## Access control

Mirrors the Macro Model gate exactly.

- New `users.hpi_access` column with auto-migration on startup
- `User.has_hpi_access()` — admin email auto-granted, everyone else opt-in
- `/auth/admin/toggle-hpi/<user_id>` endpoint exposed as an **HPI.Grant /
  HPI.Revoke** button in the admin user table

## Deploy

No new env vars required beyond what's already in production. First
call to `/api/house-prices/status` triggers the initial build (~1-2
minutes: downloads FHFA + Case-Shiller + Zillow metro/county CSVs,
parses, computes summaries). Subsequent calls serve from a 7-day disk
cache.

To warm the cache after deploy:
```bash
curl -X POST https://<host>/api/house-prices/refresh \
  -H 'Cookie: <admin session>'
```

The ZIP-level file is ~100MB and not downloaded by default — call
`POST /refresh?zip=1` if you need it.

## Known limitations (v1)

- **Annual county cadence.** FHFA county is annual; Zillow fills it in
  monthly for counties Zillow covers (~3,000 of ~3,100 US counties).
- **No seasonal adjustment.** FHFA ships both SA and NSA; we prefer SA
  when available. Case-Shiller is NSA; Zillow is already
  smoothed+SA. Mixing SA conventions in cross-source comparisons is
  noted but not corrected.
- **No affordability overlay.** Price-to-income ratios are a Phase 2
  addition once we wire in BLS median-income by MSA.
- **No forecasts.** v1 is a data product. A state-level AR(1) or BVAR
  forecast is a natural v2 enhancement.
- **No automatic refresh.** Call `POST /refresh` manually (or add a
  scheduler job in `backend/scheduler.py`).

## What a Phase 2 upgrade looks like

Rough ordering by impact/effort:

1. **Nightly refresh job** in `backend/scheduler.py` (1 day)
2. **Affordability index**: HPI divided by MSA-level median household income
   from BLS (2 days)
3. **Choropleth state map** on the dashboard (2-3 days)
4. **Simple forecast**: state-level AR(1) or BVAR, 4-quarter horizon
   (4-5 days)
5. **Feed into the Macro Model**: add a housing-wealth term to the
   consumption equation so HPI shocks propagate through the macro
   model's IRFs (1 week)
