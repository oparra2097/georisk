# Cached model outputs

## How to enable (one-time setup)

Two steps in the GitHub UI:

1. **Add `FRED_API_KEY`** — repo Settings → Secrets and variables →
   Actions → **New repository secret**. Name: `FRED_API_KEY`,
   value: the same FRED key configured on Render. Without this,
   every FRED-derived fetch (Fed target midpoint, EM CPI, EM policy
   rates via OECD, OECD 10Y yields) returns empty on the runner
   and the corresponding cache files never populate.

2. **Trigger the first refresh** — Actions → **Refresh cache** →
   **Run workflow** → leave scope=all → Run. Runs in ~5-10 min.
   When it completes, `data/cache/` fills with per-namespace
   `<namespace>_cache/<key>.json` files. The workflow commits them
   to main; Render auto-deploys with the warm cache in place.

After that, the 4-hour cron keeps the full cache fresh and the
06:00 UTC daily cron refreshes the trades subset (bond ranker,
EM panel) before NY market open.

## Layout

This directory is populated by `.github/workflows/refresh_cache.yml`
running `scripts/refresh_cache.py`. **Don't edit by hand** — commits
land here on cron.

Layout mirrors `backend/data_sources/_cache.py`:

    data/cache/<namespace>_cache/<key>.json

Each JSON file is `{ts: <unix seconds>, data: <payload>}`. Flask's
`_cache._load_from_disk` reads from either this dir or the live
`Config.DATA_DIR` cache and returns whichever entry is newer, so
these committed snapshots warm-seed every namespace on cold boot
and get displaced automatically the moment a live Flask hit writes
a fresher value.

To trigger a manual refresh: **Actions → Refresh cache → Run
workflow** in the GitHub UI. Optional `scope` input toggles between
`all` (full refresh, everything the site serves) and `trades`
(subset — bond ranker, EM panel, US rates edge; runs in ~2 min).

The daily 06:00 UTC cron runs `scope=trades` so the trade board is
fresh before NY market open even if the 4-hour full cron fails.
