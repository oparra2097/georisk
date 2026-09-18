# static/vendor/

Vendored copies of third-party dataviz libraries that were previously loaded
from CDNs (jsdelivr / d3js.org / unpkg). Consolidating to `/static/vendor/`
gives us:

- **One HTTP/2 connection** to the app origin instead of 3+ CDN TLS handshakes.
- **CSP-friendly** — `script-src 'self'` alone is enough; no CDN allowlist.
- **CDN outage resilience** — jsdelivr / unpkg both have periodic 5xx spells.

## Pinned versions

| File | Package | Version |
| ---- | ------- | ------- |
| `chart.umd.js` | chart.js | 4.4.4 (UMD build) |
| `d3.v7.min.js` | d3 | 7.x |
| `topojson-client.min.js` | topojson-client | 3.x |
| `world-atlas/countries-110m.json` | world-atlas | 2.x |

## How to (re)populate

```bash
bash scripts/fetch_vendor.sh
```

Requires outbound HTTPS access to `cdn.jsdelivr.net`. Run after cloning
the repo the first time, and again whenever a version above is bumped.

## Where they're referenced

- `chart.umd.js` — `home.html`, `credit_default.html`, `data.html`,
  `georisk.html`, `labor_market.html`, `cpi_release.html`, `em_fx_rates.html`.
- `d3.v7.min.js` — `georisk.html`, `data.html`, `house_prices.html`,
  `macro_model.html`, `data_centers.html`.
- `topojson-client.min.js` — `georisk.html`, `data.html`, `house_prices.html`,
  `data_centers.html`.
- `world-atlas/countries-110m.json` — fetched at runtime from
  `map.js` and `data.js`.

## Not vendored

- Substack RSS is fetched server-side (`backend/data_sources/substack_feed.py`)
  so no client-side vendor is needed.
- Yahoo Finance / FRED / IMF / WB data — server-side only.
