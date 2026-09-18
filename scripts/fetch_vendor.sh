#!/usr/bin/env bash
# Fetch vendored dataviz libraries into static/vendor/.
#
# Run this AFTER cloning the repo (once) and after upgrading library versions.
# Committed vendor files replace the previous CDN <script> tags so:
#   - one HTTP/2 connection (same origin as the app) instead of 3+ CDN handshakes
#   - no CDN outage risk (jsdelivr / d3js.org / unpkg all get flaky)
#   - CSP-friendly (script-src 'self' is enough, no allowlist)
#
# Pinned versions here should match the Jinja <script> tags in templates.
set -euo pipefail
cd "$(dirname "$0")/.."

VENDOR="static/vendor"
mkdir -p "$VENDOR"

echo "→ Chart.js 4 (UMD)"
curl -sSL "https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.js" \
  -o "$VENDOR/chart.umd.js"

echo "→ D3 v7"
curl -sSL "https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js" \
  -o "$VENDOR/d3.v7.min.js"

echo "→ topojson-client v3"
curl -sSL "https://cdn.jsdelivr.net/npm/topojson-client@3/dist/topojson-client.min.js" \
  -o "$VENDOR/topojson-client.min.js"

echo "→ world-atlas countries-110m.json"
mkdir -p "$VENDOR/world-atlas"
curl -sSL "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json" \
  -o "$VENDOR/world-atlas/countries-110m.json"

echo "→ us-atlas states-10m.json + counties-10m.json (house_prices choropleth)"
mkdir -p "$VENDOR/us-atlas"
curl -sSL "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json" \
  -o "$VENDOR/us-atlas/states-10m.json"
curl -sSL "https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json" \
  -o "$VENDOR/us-atlas/counties-10m.json"

echo
echo "Vendored files:"
ls -lh "$VENDOR"/*.js "$VENDOR/world-atlas/"*.json
echo
echo "Now commit static/vendor/ if you haven't already."
