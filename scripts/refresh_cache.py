#!/usr/bin/env python3
"""
Refresh every cached model output. Runs in GitHub Actions on cron.

Points ``_cache.py``'s disk writes at ``<repo>/data/cache/`` via
``CACHE_DIR_OVERRIDE``, so every ``@cached`` function's return value
gets committed into the repo. Flask's ``_cache._load_from_disk`` on
Render then reads from that repo dir on cold boot — no live IMF /
FRED / yfinance calls in the critical path.

Two scopes:

  --scope all     Full refresh — every heavy fetch + fit + panel
                  assembly. Runs on the every-4h cron.
  --scope trades  Trade-ideas subset — ZQ curve, Fed target midpoint,
                  EM fits, EM panel, sovereign bond ranker, credit
                  default dashboard. Runs on the daily 06:00 UTC cron
                  so the trade board is fresh before NY open.

Each step is best-effort: a failure is logged and the run continues.
Errors don't fail the workflow — the resulting cache file is simply
missing and the previous commit's copy stays in the tree.

Usage locally:

    python scripts/refresh_cache.py --scope trades
    python scripts/refresh_cache.py           # defaults to all
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

# Resolve repo root and point the cache writer at data/cache/.
_HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(_HERE)
os.environ['CACHE_DIR_OVERRIDE'] = os.path.join(REPO_ROOT, 'data', 'cache')

sys.path.insert(0, REPO_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-5s %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger('refresh')


# ── Steps ────────────────────────────────────────────────────────────

def _step_fed_funds_curve():
    from backend.data_sources import rates_futures
    rates_futures.get_fed_funds_curve()


def _step_us_target_mid():
    from backend.rates.routes import _get_current_target_mid
    _get_current_target_mid()


def _step_em_rates_fits():
    from backend.rates import em_config, em_rates_service
    for iso in em_config.ALL_ISO3:
        em_rates_service.get_fit(iso)


def _step_em_rates_panel():
    from backend.rates import em_rates_service
    em_rates_service.get_panel()


def _step_bond_trades_default():
    from backend.credit_default import bond_trades
    bond_trades.get_bond_trades(top_n=12)


def _step_bond_trades_home():
    from backend.credit_default import bond_trades
    # Match the parameter shape the home page uses so the cache key
    # aligns and the frontend hits a warm entry.
    bond_trades.get_bond_trades(top_n=3, min_edge_pct=0.5)


def _step_credit_default_dashboard():
    from backend.credit_default import service as cd_service
    cd_service.get_dashboard()


def _step_macro_model_ensure():
    from backend.macro_model import service as mm
    # Blocking build — the whole point of this script is to eat the
    # cost here so the first user visit is free.
    mm.ensure_built()


def _step_macro_model_fan_8():
    from backend.macro_model import service as mm
    mm.get_bootstrap(horizon=8, n_draws=30)


def _step_macro_model_fan_12():
    from backend.macro_model import service as mm
    mm.get_bootstrap(horizon=12, n_draws=30)


def _step_commodities_forecast():
    from backend.data_sources import commodities_forecast
    commodities_forecast.get_forecast_data()


# ── Scope tables ─────────────────────────────────────────────────────

# TRADES scope: everything the trade-ideas surfaces depend on. Runs
# daily. Kept short and reliable so the 06:00 UTC job is under 3-4 min.
TRADES_STEPS = [
    ('fed_funds_curve',         _step_fed_funds_curve),
    ('us_target_mid',           _step_us_target_mid),
    ('em_rates_fits',           _step_em_rates_fits),
    ('em_rates_panel',          _step_em_rates_panel),
    ('bond_trades_default',     _step_bond_trades_default),
    ('bond_trades_home',        _step_bond_trades_home),
    ('credit_default_dashboard', _step_credit_default_dashboard),
]

# FULL scope adds the macro-model fit + commodities forecast. Runs on
# the every-4h cron.
FULL_ONLY_STEPS = [
    ('macro_model_ensure',      _step_macro_model_ensure),
    ('macro_model_fan_8',       _step_macro_model_fan_8),
    ('macro_model_fan_12',      _step_macro_model_fan_12),
    ('commodities_forecast',    _step_commodities_forecast),
]


# ── Runner ───────────────────────────────────────────────────────────

def _run_step(name: str, fn) -> bool:
    t0 = time.time()
    try:
        fn()
        log.info(f'ok    {name:32s} {time.time() - t0:5.1f}s')
        return True
    except Exception as e:  # noqa: BLE001
        log.error(f'fail  {name:32s} {time.time() - t0:5.1f}s  {e}')
        return False


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--scope', choices=('all', 'trades'), default='all',
                    help="Which refresh scope to run (default: all)")
    args = ap.parse_args()

    log.info(f'Refresh scope={args.scope}')
    log.info(f'Cache dir  ={os.environ["CACHE_DIR_OVERRIDE"]}')

    steps = list(TRADES_STEPS)
    if args.scope == 'all':
        steps += FULL_ONLY_STEPS

    n_ok = sum(_run_step(name, fn) for name, fn in steps)
    n_total = len(steps)
    log.info(f'--- {n_ok}/{n_total} ok ---')

    # Exit 0 even when a step fails — GH Actions still commits whatever
    # cache files DID land. Individual step failures shouldn't block
    # the whole run.
    return 0


if __name__ == '__main__':
    sys.exit(main())
