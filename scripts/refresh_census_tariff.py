"""
Refresh the US effective tariff rate snapshot from the Census Bureau.

Exact Apollo / Torsten Slok methodology ("The Daily Spark"):
    monthly Census "Calculated Duty" ÷ total US goods imports × 100

Census International Trade API, imports by HS commodity:
    CAL_DUT_MO  calculated duty, month
    GEN_VAL_MO  general imports value, month   (total US goods imports)
    CON_VAL_MO  imports for consumption, month (kept for reference)

Writes static/data/census_tariff_rate.json. The app reads that file as
a committed fallback and also fetches Census live (see
backend/data_sources/census_tariff.py).

Usage:
    python scripts/refresh_census_tariff.py             # write snapshot
    python scripts/refresh_census_tariff.py --diagnose  # also print the
        latest months and what the production endpoint is serving
"""

import argparse
import json
import os
import sys

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from backend.data_sources.census_tariff import fetch_census_series  # noqa: E402

OUT_PATH = os.path.join(ROOT, 'static', 'data', 'census_tariff_rate.json')


def _diagnose(series):
    print('\n=== Census (exact Slok series) ===')
    pts = series['points']
    print(f'months: {len(pts)}  first: {pts[0]["date"]}  latest: {pts[-1]["date"]}')
    for p in pts[-8:]:
        print(f'  {p["date"][:7]}  duty=${p["duty"]/1e9:6.2f}B  gen_imports=${p["gen_imports"]/1e9:7.2f}B'
              f'  rate={p["value"]:5.2f}%  (vs consumption imports: {p["rate_consumption"]:5.2f}%)')

    print('\n=== Production /api/yale-tariff collected_series ===')
    try:
        r = requests.get('https://parramacro.com/api/yale-tariff', timeout=60)
        d = r.json()
        cs = d.get('collected_series') or {}
        print(f'  HTTP {r.status_code}  source={cs.get("source")!r}  latest_collected={d.get("latest_collected")}')
        for p in (cs.get('points') or [])[-6:]:
            print(f'  {p.get("date")}  {p.get("value")}  ({p.get("source", "")})')
    except Exception as e:
        print(f'  error: {type(e).__name__}: {e}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--diagnose', action='store_true')
    args = ap.parse_args()

    series = fetch_census_series(verbose=True)
    if not series or not series.get('points'):
        print('Census fetch returned no data; snapshot left unchanged.')
        sys.exit(1)

    with open(OUT_PATH, 'w') as f:
        json.dump(series, f, indent=1)
    print(f'wrote {OUT_PATH}: {len(series["points"])} months, latest {series["latest_date"]} = {series["latest_value"]}%')

    if args.diagnose:
        _diagnose(series)


if __name__ == '__main__':
    main()
