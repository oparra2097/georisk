import logging
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from backend.scoring.engine import (
    refresh_gdelt_scores, refresh_news_region, refresh_all_scores,
    seed_base_only_scores
)
from backend.data_sources.world_bank_wgi import fetch_base_scores
from backend.cache.persistence import load_scores
from backend.cache.store import store
from config import Config

logger = logging.getLogger(__name__)


def _startup_with_persisted():
    """Load WGI base scores for ALL countries, seed missing ones,
    then refresh GDELT (news scores EMA-blend with persisted state)."""
    try:
        logger.info("Loading World Bank base scores for ALL countries (startup with persisted data)...")
        fetch_base_scores()
        # Seed any countries that have WGI data but weren't in persisted scores
        seed_base_only_scores()
        logger.info("Base scores loaded + seeded. Starting GDELT refresh...")
    except Exception as e:
        logger.error(f"Failed to load base scores on startup: {e}")
    refresh_gdelt_scores()


def init_scheduler(app):
    """Initialize background schedulers for GDELT and multi-provider news refreshes."""
    gdelt_interval = app.config.get('GDELT_REFRESH_MINUTES', 15)
    news_interval = app.config.get('NEWS_ROTATION_MINUTES', 120)

    # Initialize SQLite database and migrate history.json if present
    from backend.cache.database import init_db, migrate_from_json, cleanup_old_scores
    init_db()
    migrate_from_json()
    cleanup_old_scores()

    # Load persisted scores from disk (survives restarts/redeploys)
    had_data = load_scores(store)
    if had_data:
        logger.info("Restored persisted scores — EMA state preserved. "
                     "Will load base scores and refresh on next cycle.")

    scheduler = BackgroundScheduler()

    # Job 1: GDELT refresh every 15 min (unlimited, no API key)
    # Scores ALL countries (priority + remaining) — single-threaded
    scheduler.add_job(
        func=refresh_gdelt_scores,
        trigger='interval',
        minutes=gdelt_interval,
        id='refresh_gdelt',
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1
    )

    # Job 2: Multi-provider news rotation every 2 hours
    # Only applies to 50 priority countries
    scheduler.add_job(
        func=refresh_news_region,
        trigger='interval',
        minutes=news_interval,
        id='refresh_news',
        replace_existing=True,
        misfire_grace_time=600,
        max_instances=1
    )

    # Job 3: ACLED conflict data refresh (daily at 6 AM UTC)
    if Config.ACLED_EMAIL and Config.ACLED_PASSWORD:
        from backend.data_sources.acled_client import prefetch_acled_data
        scheduler.add_job(
            func=prefetch_acled_data,
            trigger='cron',
            hour=6,
            id='refresh_acled',
            replace_existing=True,
            max_instances=1
        )
        logger.info("ACLED daily prefetch scheduled (6 AM UTC).")

    # Job 4: Commodity model refit (monthly, 1st of month at 07:00 UTC)
    def _refit_commodity_models():
        try:
            from backend.data_sources import commodity_models, commodities_forecast
            summaries = commodity_models.refit_all()
            fits = sum(1 for s in summaries.values() if not s.get('fit_error'))
            logger.info(f"Commodity model refit: {fits}/{len(summaries)} succeeded")
            # Invalidate the forecast cache so downstream calls pick up new fits
            commodities_forecast._cache.clear()
        except Exception as e:
            logger.error(f"Commodity model refit failed: {e}")

    scheduler.add_job(
        func=_refit_commodity_models,
        trigger='cron',
        day=1, hour=7, minute=0,
        id='refit_commodity_models',
        replace_existing=True,
        max_instances=1,
    )
    logger.info("Commodity model monthly refit scheduled (1st of month, 07:00 UTC).")

    # Job 5: GDP nowcast refresh every 6 hours
    # Use the dynamic resolver — Config.FRED_API_KEY is frozen at import
    # time and may be empty even when env var is set.
    from backend.data_sources.fred_client import _get_api_key as _fred_key
    if _fred_key():
        def _refresh_gdp_nowcast():
            try:
                from backend.data_sources.fred_client import clear_cache as clear_fred
                from backend.data_sources.gdp_nowcast import compute_nowcast
                import backend.data_sources.gdp_nowcast as gnmod
                import time as _time
                clear_fred()
                data = compute_nowcast()
                if not data.get('error'):
                    with gnmod._lock:
                        gnmod._cached_result = data
                        gnmod._cached_at = _time.time()
                    est = data.get('nowcast', {}).get('estimate')
                    logger.info(f"GDP nowcast refreshed: {est}%")
                else:
                    logger.warning(f"GDP nowcast refresh error: {data.get('error')}")
            except Exception as e:
                logger.error(f"GDP nowcast refresh failed: {e}")

        scheduler.add_job(
            func=_refresh_gdp_nowcast,
            trigger='interval',
            hours=6,
            id='refresh_gdp_nowcast',
            replace_existing=True,
            misfire_grace_time=600,
            max_instances=1,
        )
        logger.info("GDP nowcast scheduled (every 6 hours).")

        # Job 5b: Labor market nowcast refresh every 6 hours.  Same cadence
        # as GDP — both rely on FRED leading indicators, so we drop the
        # FRED cache once and recompute each.
        def _refresh_labor_nowcast():
            try:
                from backend.data_sources.bls_employment import (
                    clear_bls_employment_cache,
                )
                from backend.data_sources.labor_nowcast import (
                    clear_cache as clear_lm_cache, get_labor_nowcast,
                )
                clear_bls_employment_cache()
                clear_lm_cache()
                data = get_labor_nowcast()
                if not data.get('error'):
                    nc = (data.get('nowcast') or {})
                    logger.info(
                        f"Labor nowcast refreshed: payroll Δ "
                        f"{nc.get('payroll_estimate_change')}k for {nc.get('month')}"
                    )
                else:
                    logger.warning(f"Labor nowcast refresh error: {data.get('error')}")
            except Exception as e:
                logger.error(f"Labor nowcast refresh failed: {e}")

        scheduler.add_job(
            func=_refresh_labor_nowcast,
            trigger='interval',
            hours=6,
            id='refresh_labor_nowcast',
            replace_existing=True,
            misfire_grace_time=600,
            max_instances=1,
        )
        logger.info("Labor market nowcast scheduled (every 6 hours).")

    # Job 6: Data center drift scan once daily at 07:30 UTC.
    def _scan_dc_drift():
        try:
            from backend.data_centers import drift
            out = drift.scan()
            n = len(out.get('drift_flags', []))
            logger.info(f"Data center drift scan complete: {n} drift flag(s) across "
                         f"{out.get('urls_scanned', 0)} URL(s).")
        except Exception as e:
            logger.error(f"Data center drift scan failed: {e}")

    scheduler.add_job(
        func=_scan_dc_drift,
        trigger='cron',
        hour=7, minute=30,
        id='dc_drift_scan',
        replace_existing=True,
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Data center drift scan scheduled (daily 07:30 UTC).")

    # Job 7: SEC EDGAR REIT 10-K pull weekly (Sunday 08:00 UTC).
    def _pull_dc_sec_edgar():
        try:
            from backend.data_centers import sec_edgar
            out = sec_edgar.pull_all()
            ok = sum(1 for v in out.get('reits', {}).values() if v.get('ok'))
            logger.info(f"Data center SEC EDGAR pull: {ok}/{len(out.get('reits', {}))} REITs OK, "
                         f"{out.get('total_rows', 0)} rows total.")
        except Exception as e:
            logger.error(f"Data center SEC EDGAR pull failed: {e}")

    scheduler.add_job(
        func=_pull_dc_sec_edgar,
        trigger='cron',
        day_of_week='sun', hour=8, minute=0,
        id='dc_sec_edgar_pull',
        replace_existing=True,
        misfire_grace_time=7200,
        max_instances=1,
    )
    logger.info("Data center SEC EDGAR pull scheduled (Sunday 08:00 UTC).")

    # Job 8: ISO interconnection queue pull weekly (Monday 08:00 UTC).
    def _pull_dc_iso_queues():
        try:
            from backend.data_centers import iso_queue
            out = iso_queue.pull_all()
            isos_ok = []
            for k in ('pjm', 'ercot', 'miso', 'caiso'):
                if out.get(k, {}).get('ok'):
                    isos_ok.append(k.upper())
            logger.info(f"Data center ISO queue pull: {','.join(isos_ok) or 'none'} OK, "
                         f"{out.get('total_rows', 0)} requests aggregated.")
        except Exception as e:
            logger.error(f"Data center ISO queue pull failed: {e}")

    scheduler.add_job(
        func=_pull_dc_iso_queues,
        trigger='cron',
        day_of_week='mon', hour=8, minute=0,
        id='dc_iso_queue_pull',
        replace_existing=True,
        misfire_grace_time=7200,
        max_instances=1,
    )
    logger.info("Data center ISO queue pull scheduled (Monday 08:00 UTC).")

    scheduler.start()
    logger.info(
        f"Scheduler started. GDELT every {gdelt_interval}min (ALL countries), "
        f"news region rotation every {news_interval}min (50 priority)."
    )

    if not had_data:
        # No persisted data: full startup (WGI ALL + seed + GDELT ALL + news)
        thread = threading.Thread(target=refresh_all_scores, daemon=True)
        thread.start()
        logger.info("No persisted data found. Full initial refresh started in background.")
    else:
        # Persisted data: load WGI for ALL, seed missing, then GDELT refresh
        thread = threading.Thread(target=_startup_with_persisted, daemon=True)
        thread.start()
        logger.info("Persisted data loaded. Background WGI + GDELT refresh started.")

    # Pre-warm the EM external vulnerability dataset so the first user
    # hit is cache-served (5 World Bank fetches, ~15s in parallel).
    def _warm_em_vuln():
        try:
            from backend.data_sources.em_vulnerability import get_em_vulnerability_data
            get_em_vulnerability_data()
            logger.info("EM vulnerability cache warmed.")
        except Exception as e:
            logger.error(f"EM vulnerability warmup failed: {e}")

    threading.Thread(target=_warm_em_vuln, daemon=True).start()

    # Pre-warm GDP nowcast so first visit is instant
    # Use the dynamic resolver — Config.FRED_API_KEY is frozen at import
    # time and may be empty even when env var is set.
    from backend.data_sources.fred_client import _get_api_key as _fred_key
    if _fred_key():
        def _warm_gdp_nowcast():
            try:
                from backend.data_sources.gdp_nowcast import get_gdp_nowcast
                result = get_gdp_nowcast()
                est = result.get('nowcast', {}).get('estimate')
                logger.info(f"GDP nowcast cache warmed: {est}%")
            except Exception as e:
                logger.error(f"GDP nowcast warmup failed: {e}")
        threading.Thread(target=_warm_gdp_nowcast, daemon=True).start()

        def _warm_labor_nowcast():
            try:
                from backend.data_sources.labor_nowcast import get_labor_nowcast
                from backend.data_sources.bls_employment import get_bls_employment_data
                # Pull BLS first (24h cache) so the page only ever
                # fires the FRED-heavy nowcast on cold start.
                get_bls_employment_data()
                result = get_labor_nowcast()
                if result.get('error'):
                    logger.warning(f"Labor nowcast warmup error: {result.get('error')}")
                else:
                    nc = (result.get('nowcast') or {})
                    logger.info(
                        f"Labor nowcast cache warmed: Δpay "
                        f"{nc.get('payroll_estimate_change')}k @ {nc.get('month')}"
                    )
            except Exception as e:
                logger.error(f"Labor nowcast warmup failed: {e}")
        threading.Thread(target=_warm_labor_nowcast, daemon=True).start()

    # Cross-deploy pickle invalidation: every fresh app boot wipes any
    # leftover pickles from an earlier deploy so new code always builds
    # fresh against current FRED data and current equation specs. Pickles
    # are regenerated after the new build completes and are then useful
    # for sibling-worker hot-loads within this deploy.
    try:
        from backend.macro_model.service import invalidate_pickle_on_boot as _mm_invalidate
        _mm_invalidate()
    except Exception as e:
        logger.warning(f'macro_model pickle invalidation failed: {e}')
    try:
        from backend.house_prices.service import invalidate_pickle_on_boot as _hpi_invalidate
        _hpi_invalidate()
    except Exception as e:
        logger.warning(f'house_prices pickle invalidation failed: {e}')

    # Pre-warm macro-model: try the disk pickle first (instant if a sibling
    # worker built recently), only kick off the expensive fit_all if no
    # fresh pickle exists. Runs as a daemon thread so it doesn't block boot.
    # Use the dynamic resolver — Config.FRED_API_KEY is frozen at import
    # time and may be empty even when env var is set.
    from backend.data_sources.fred_client import _get_api_key as _fred_key
    if _fred_key():
        def _warm_macro_model():
            try:
                from backend.macro_model import service as mm_svc
                mm_svc.ensure_built()
                # If the pickle wasn't loadable, ensure_built started a bg
                # thread; we're done. If it was loadable, the simulator is
                # ready and status() will show built=True.
                logger.info(f"macro_model warmup: status={mm_svc.status()}")
            except Exception as e:
                logger.error(f"macro_model warmup failed: {e}")
        threading.Thread(target=_warm_macro_model, daemon=True).start()

    # Pre-warm HPI: same pattern.
    def _warm_hpi():
        try:
            from backend.house_prices import service as hpi_svc
            hpi_svc.ensure_built()
            logger.info(f"house_prices warmup: status={hpi_svc.status()}")
        except Exception as e:
            logger.error(f"house_prices warmup failed: {e}")
    threading.Thread(target=_warm_hpi, daemon=True).start()

    # Pre-warm commodities: the monthly cron at job 4 only runs on the
    # 1st of the month, so a deploy on any other day starts with no fits
    # and /api/v1/health reports commodities=null. Use get_or_fit per
    # commodity so already-fresh disk fits are reused (~instant) and only
    # missing/stale ones do the slow ~30-90s fit. Daemon thread so boot
    # doesn't block.
    def _warm_commodities():
        try:
            from backend.data_sources import commodity_models, commodities_forecast
            ok, fail = 0, 0
            for name in commodity_models.TICKERS:
                try:
                    m = commodity_models.get_or_fit(name)
                    if m is not None and m.fit_error is None:
                        ok += 1
                    else:
                        fail += 1
                except Exception as e:
                    logger.warning(f"commodities warmup: {name} failed: {e}")
                    fail += 1
            logger.info(f"commodities warmup: {ok}/{ok+fail} fits ready "
                        f"(cache_dir={commodity_models.CACHE_DIR})")
            # Drop the daily forecast cache, then proactively rebuild it
            # so /api/v1/health can report a real `commodities` timestamp
            # without the algotrader having to first hit the forecast
            # endpoint to trigger lazy population.
            commodities_forecast._cache.clear()
            data = commodities_forecast.get_forecast_data()
            logger.info(
                f"commodities warmup: daily cache primed "
                f"(last_updated={data.get('last_updated') if data else 'none'})"
            )
        except Exception as e:
            logger.error(f"commodities warmup failed: {e}")
    threading.Thread(target=_warm_commodities, daemon=True).start()
