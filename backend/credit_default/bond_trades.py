"""
Rank sovereign bond trades by comparing our model's PD to the market-
implied PD backed out from published bond spreads.

Logic
-----
For each sovereign we cover, we take:

  * ``model_pd`` — our fitted 1-year probability of default (from the
                   dashboard's rating engine).
  * ``market_pd`` — implied from the sovereign's bond spread using the
                    standard hazard-rate formulation::

                        market_pd_1y = 1 - exp(-spread / (1 - LGD))

                    where LGD defaults to 60% (i.e. 40% recovery). This is
                    the Tellimer §4.4 formulation and matches what a
                    trader eyeballs when they say "spread / (1 - R)".

The **edge** is ``market_pd - model_pd``:

  * edge > 0  → market is pricing MORE default risk than the model thinks
                is warranted. Bond is cheap → **LONG** the bond (buy).
  * edge < 0  → market is pricing LESS default risk than the model thinks
                is warranted. Bond is expensive → **SHORT** the bond (or
                stay away).

Sovereigns are ranked by absolute edge; the module returns two lists
(top-N longs, top-N shorts) plus every sovereign's full row so the
frontend can also plot the model-vs-market scatter.

Data sources
------------

Spreads are loaded from ``data/sovereign_bond_yields.csv`` (hand-curated,
tracked in git so history is auditable). The UST 10Y benchmark is pulled
from FRED at request time via ``em_fx_rates.get_us_curve`` when the
network is available; else the file's ``USA`` row is used as a fallback.

Local-currency yields carry an embedded FX-devaluation premium that isn't
credit risk — we flag those rows with ``currency != 'USD'`` and mark the
``fx_risk_tag`` = True so the frontend can grey them out.
"""

from __future__ import annotations

import csv
import math
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple


_CSV = Path(__file__).resolve().parent.parent.parent / 'data' / 'sovereign_bond_yields.csv'

_cache: Dict[str, object] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 30 * 60   # 30 minutes


# ── Loaders ─────────────────────────────────────────────────────────────


def load_yield_snapshot() -> List[Dict]:
    """Return the current sovereign yield snapshot from
    ``data/sovereign_bond_yields.csv``. Rows with unparseable yields are
    skipped so a bad entry can't take down the module."""
    rows: List[Dict] = []
    if not _CSV.exists():
        return rows
    with open(_CSV, newline='', encoding='utf-8') as f:
        cleaned = (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
        reader = csv.DictReader(cleaned)
        for r in reader:
            iso3 = (r.get('iso3') or '').strip().upper()
            if len(iso3) != 3:
                continue
            try:
                yld = float(r.get('yield_pct') or 'nan')
            except (TypeError, ValueError):
                continue
            spread_bps_raw = r.get('spread_bps')
            spread_bps: Optional[float] = None
            if spread_bps_raw:
                try:
                    spread_bps = float(spread_bps_raw)
                except (TypeError, ValueError):
                    pass
            rows.append({
                'iso3': iso3,
                'currency': (r.get('currency') or 'USD').strip().upper(),
                'maturity_years': int(float(r.get('maturity_years') or 10)),
                'yield_pct': yld,
                'spread_bps_prefilled': spread_bps,
                'as_of': (r.get('as_of') or '').strip(),
                'note': (r.get('note') or '').strip(),
            })
    return rows


def _us_10y_benchmark(snapshot: List[Dict]) -> float:
    """Latest UST 10Y yield in %. Tries em_fx_rates.get_us_curve for the
    live number; falls back to whatever the CSV's USA row says (default
    4.20 if that's also missing)."""
    try:
        from backend.data_sources import em_fx_rates
        curve = em_fx_rates.get_us_curve() if hasattr(em_fx_rates, 'get_us_curve') else None
        if curve:
            for pt in curve:
                if pt.get('tenor') == '10Y' and pt.get('yield') is not None:
                    return float(pt['yield'])
    except Exception:  # noqa: BLE001
        pass
    for r in snapshot:
        if r['iso3'] == 'USA':
            return float(r['yield_pct'])
    return 4.20


# ── Market-implied PD ───────────────────────────────────────────────────


def market_implied_pd(spread_bps: float, horizon_years: int = 1,
                      lgd: float = 0.60) -> float:
    """Hazard-rate PD from a spread quote.

    ``spread_bps`` is over the risk-free (UST) benchmark. Uses constant
    hazard: ``PD(T) = 1 - exp(-λ · T)`` where ``λ = spread / (1 - R) =
    spread / LGD``.
    """
    if spread_bps is None or spread_bps != spread_bps:
        return 0.0
    lgd = max(0.05, min(0.95, float(lgd)))
    spread_frac = float(spread_bps) / 10000.0
    if spread_frac <= 0:
        return 0.0
    hazard = spread_frac / lgd
    try:
        return 1.0 - math.exp(-hazard * horizon_years)
    except OverflowError:
        return 1.0


# ── Ranking ─────────────────────────────────────────────────────────────


def get_bond_trades(top_n: int = 10,
                    lgd: float = 0.60,
                    min_edge_pct: float = 2.0,
                    cadence: str = 'annual',
                    horizon: int = 1) -> Dict:
    """Rank sovereign bond trades by (market_pd - model_pd).

    Args
    ----
    top_n:        how many longs and shorts to return
    lgd:          loss-given-default assumption (default 0.60 = 40% recovery)
    min_edge_pct: minimum |edge| in percentage points to appear in the
                  ranked lists (default 2pp)
    cadence:      passthrough to service.get_table_rows
    horizon:      1 / 3 / 5 (years). The market-implied PD is scaled to
                  the same horizon via the constant-hazard formula so
                  comparisons are apples-to-apples.

    Returns
    -------
    Dict with:
      - all: every sovereign's row (model_pd, market_pd, edge, tags)
      - longs: top_n by edge desc (market pricing more risk than model)
      - shorts: top_n by edge asc (market pricing less risk than model)
      - summary: benchmark_yield_pct, lgd, horizon_years, n_covered
    """
    cache_key = f'bond_trades_h{horizon}_lgd{lgd:.2f}_n{top_n}_edge{min_edge_pct:.1f}'
    with _cache_lock:
        cached = _cache.get(cache_key)
        cached_ts = _cache.get(f'{cache_key}_ts', 0)
    if cached and (time.time() - cached_ts) < _CACHE_TTL:
        return cached

    snapshot = load_yield_snapshot()
    ust_10y = _us_10y_benchmark(snapshot)

    # Pull model PDs — reuse the existing table-rows path.
    from backend.credit_default import service
    model_rows = service.get_table_rows(cadence=cadence, horizon=horizon)
    model_by_iso3 = {r['iso3']: r for r in model_rows}

    pd_key = f'pd_{horizon}y'
    combined: List[Dict] = []

    for r in snapshot:
        iso3 = r['iso3']
        if iso3 == 'USA':
            continue   # UST is the benchmark, not a sovereign trade

        # Spread: use pre-filled if present, else derive from yield.
        if r['spread_bps_prefilled'] is not None:
            spread_bps = r['spread_bps_prefilled']
        else:
            spread_bps = max(0.0, (r['yield_pct'] - ust_10y) * 100.0)

        market_pd = market_implied_pd(spread_bps, horizon_years=horizon, lgd=lgd)

        model_row = model_by_iso3.get(iso3) or {}
        model_pd = model_row.get(pd_key)
        if model_pd is None:
            # Skip sovereigns we don't model — the ranking would be moot.
            continue

        edge_pct = (market_pd - float(model_pd)) * 100.0
        combined.append({
            'iso3': iso3,
            'name': model_row.get('name') or iso3,
            'region': model_row.get('region') or '',
            'pm_notch': model_row.get('pm_notch'),
            'sp_equiv': model_row.get('sp_equiv'),
            'agency_sp': model_row.get('agency_sp'),
            'currency': r['currency'],
            'yield_pct': r['yield_pct'],
            'spread_bps': round(spread_bps, 1),
            'market_pd': round(market_pd, 4),
            'model_pd': round(float(model_pd), 4),
            'edge_pp': round(edge_pct, 2),
            'signal': _signal(edge_pct, min_edge_pct),
            'fx_risk_tag': r['currency'] != 'USD',
            'as_of': r['as_of'],
            'note': r['note'],
        })

    # Rank.
    longs = sorted(combined, key=lambda x: x['edge_pp'], reverse=True)[:top_n]
    shorts = sorted(combined, key=lambda x: x['edge_pp'])[:top_n]
    # Drop entries with |edge| < threshold from the ranked lists (but
    # keep them in 'all' so the scatter still shows every dot).
    longs = [x for x in longs if x['edge_pp'] >= min_edge_pct]
    shorts = [x for x in shorts if x['edge_pp'] <= -min_edge_pct]

    out = {
        'longs': longs,
        'shorts': shorts,
        'all': sorted(combined, key=lambda x: x['edge_pp'], reverse=True),
        'summary': {
            'benchmark_ust_10y_pct': round(ust_10y, 3),
            'lgd_assumption': lgd,
            'horizon_years': horizon,
            'n_covered': len(combined),
            'min_edge_pp': min_edge_pct,
        },
    }
    with _cache_lock:
        _cache[cache_key] = out
        _cache[f'{cache_key}_ts'] = time.time()
    return out


def _signal(edge_pct: float, threshold: float) -> str:
    if edge_pct >= threshold:
        return 'LONG'
    if edge_pct <= -threshold:
        return 'SHORT'
    return 'HOLD'
