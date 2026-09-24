# Cached model outputs

This directory is populated by `.github/workflows/refresh_cache.yml`
running `scripts/refresh_cache.py`. **Don't edit by hand** — commits
land here every 4 hours from GH Actions.

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
