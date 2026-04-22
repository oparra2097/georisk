#!/usr/bin/env python3
"""
Walk-forward backtest for the commodity forecast model.

For every month-end pivot in the backtest window we refit the SARIMAX +
GARCH hybrid using only data up to that pivot, generate 4 forward
quarterly forecasts, and compare to realized quarterly averages computed
from the full price history. Metrics reported per horizon (Q+1..Q+4):

  * MAE, RMSE, MAPE
  * Bias (mean of forecast - realized)
  * 95% CI hit rate (realized inside [p2.5, p97.5])

Output: ``docs/backtest_results.md`` — per-commodity tables plus a
summary leaderboard sorted by Q+1 MAPE. The script exits non-zero if any
commodity's Q+1 MAPE exceeds ``--mape-fail-threshold`` (default 25%),
which doubles as a CI smoke test.

Usage
-----
    python scripts/backtest_commodities.py                  # all 13 commodities, 5y window
    python scripts/backtest_commodities.py --months-back 36 # 3y window
    python scripts/backtest_commodities.py --commodity Gold
    python scripts/backtest_commodities.py --fail-fast

The consensus comparison pulls entries from data/consensus.yaml + EIA
STEO + World Bank Pink Sheet; missing / mis-aligned horizons are skipped
silently.
"""

from __future__ import annotations

import os
import sys
import json
import math
import logging
import argparse
import warnings
from datetime import date, timedelta
from typing import Optional

# Silence SARIMAX ConvergenceWarnings — we expect them occasionally and
# the backtest reports RMSE + CI hit rate, which tell the real story.
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd

# Make the `backend` package importable when run from repo root
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.data_sources import commodity_models as cm  # noqa: E402
from backend.data_sources import consensus_tracker as ct  # noqa: E402

logger = logging.getLogger('backtest')


# ── Core walk-forward loop ─────────────────────────────────────────────────

def _quarterly_averages(price_monthly: pd.Series) -> pd.Series:
    """Collapse monthly price levels to quarterly averages, indexed by
    the quarter's end timestamp."""
    q = price_monthly.resample('QE').mean().dropna()
    return q


def _pivots(start: date, end: date) -> list[date]:
    """Monthly pivots (month-end) between start and end inclusive."""
    rng = pd.date_range(start=start, end=end, freq='ME')
    return [ts.date() for ts in rng]


def _horizon_quarter_ends(pivot: date, h: int) -> list[pd.Timestamp]:
    """Return the h quarter-end timestamps immediately after pivot."""
    out: list[pd.Timestamp] = []
    current_q = (pivot.month - 1) // 3 + 1
    year = pivot.year
    for i in range(1, h + 1):
        qn = current_q + i
        yr = year
        if qn > 4:
            qn -= 4
            yr += 1
        month = qn * 3
        import calendar
        day = calendar.monthrange(yr, month)[1]
        out.append(pd.Timestamp(year=yr, month=month, day=day))
    return out


def _run_one_commodity(name: str, pivots: list[date], h: int = 4) -> pd.DataFrame:
    """Return a tidy DataFrame of (pivot, horizon, realized, median, p2_5, p97_5)."""
    # Fetch the *full* price series once — the fetcher honours as_of slicing,
    # but we reload via a sliced fetch per pivot. This means 60 pivots × 1
    # yf.download = 60 fetches. Acceptable for a monthly cron.
    full_fetcher = cm.DriverFetcher()
    try:
        full = cm.DriverFetcher._fetch_yf(cm.TICKERS[name], date(2005, 1, 1))
    except Exception as exc:
        logger.error(f'{name}: full price fetch failed: {exc}')
        return pd.DataFrame()
    if full is None or full.empty:
        logger.error(f'{name}: no price data')
        return pd.DataFrame()

    quarterly = _quarterly_averages(full)

    rows: list[dict] = []
    for pivot in pivots:
        # Skip pivots too recent to have Q+4 realized
        if pivot > (date.today() - timedelta(days=366)):
            continue

        model = cm.CommodityModel(name)
        try:
            ok = model.fit(fetcher=cm.DriverFetcher(), as_of=pivot)
        except Exception as e:
            logger.warning(f'{name}@{pivot}: fit crashed: {e}')
            continue
        if not ok:
            logger.info(f'{name}@{pivot}: fit failed ({model.fit_error})')
            continue

        fc = model.forecast(h=h)
        if not fc:
            continue

        horizon_ends = _horizon_quarter_ends(pivot, h)
        for i, qend in enumerate(horizon_ends):
            bucket = fc.get(f'Q+{i + 1}')
            if not bucket:
                continue
            if qend not in quarterly.index:
                continue
            realized = float(quarterly.loc[qend])
            rows.append({
                'commodity': name,
                'pivot': pivot.isoformat(),
                'horizon': i + 1,
                'q_end': qend.date().isoformat(),
                'realized': realized,
                'median': bucket['median'],
                'p2_5': bucket['p2_5'],
                'p97_5': bucket['p97_5'],
            })
    return pd.DataFrame(rows)


# ── Metrics ────────────────────────────────────────────────────────────────

def _metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Per-horizon metrics for a single commodity's walk-forward results."""
    if df.empty:
        return pd.DataFrame()
    out = []
    for horizon, sub in df.groupby('horizon'):
        err = sub['median'] - sub['realized']
        pct_err = err.abs() / sub['realized']
        hit = (sub['realized'] >= sub['p2_5']) & (sub['realized'] <= sub['p97_5'])
        out.append({
            'horizon':       int(horizon),
            'n':             int(len(sub)),
            'mae':           float(err.abs().mean()),
            'rmse':          float(np.sqrt((err ** 2).mean())),
            'mape_pct':      float(pct_err.mean() * 100),
            'bias':          float(err.mean()),
            'ci95_hit_pct':  float(hit.mean() * 100),
        })
    return pd.DataFrame(out).sort_values('horizon').reset_index(drop=True)


# ── Consensus comparison (light-touch, annual only) ────────────────────────

def _consensus_errors(name: str, quarterly: pd.Series) -> Optional[pd.DataFrame]:
    """For each consensus entry with quarterly forecasts, measure absolute
    percent error vs realized quarterly averages. Bank consensus numbers
    are quarterly; World Bank provides annual FYs only (skipped here)."""
    entries = ct.get_consensus(name)
    if not entries:
        return None
    rows: list[dict] = []
    for entry in entries:
        src = entry.get('source')
        quarters = entry.get('quarters') or {}
        if not quarters:
            continue
        for q_key, forecast_val in quarters.items():
            try:
                q_num, year = q_key.split('_')
                q_num = int(q_num.replace('Q', ''))
                year = int(year)
            except Exception:
                continue
            import calendar
            month = q_num * 3
            day = calendar.monthrange(year, month)[1]
            q_end = pd.Timestamp(year=year, month=month, day=day)
            if q_end not in quarterly.index:
                continue
            realized = float(quarterly.loc[q_end])
            rows.append({
                'source':   src,
                'q_key':    q_key,
                'forecast': float(forecast_val),
                'realized': realized,
                'pct_err':  abs(float(forecast_val) - realized) / realized * 100,
            })
    return pd.DataFrame(rows) if rows else None


# ── Report writer ──────────────────────────────────────────────────────────

def _write_report(results: dict[str, dict], out_path: str, window_months: int) -> None:
    lines: list[str] = []
    today = date.today().isoformat()
    lines.append('# Commodity Forecast Backtest Results')
    lines.append('')
    lines.append(f'_Generated: {today} · walk-forward window: {window_months} months · 4-quarter horizon_')
    lines.append('')
    lines.append('Methodology: for every month-end pivot in the window we refit the ')
    lines.append('SARIMAX(1,0,1) + GARCH(1,1) hybrid using data only up to that pivot, ')
    lines.append('generate 4 forward quarterly forecasts, and compare to the realized ')
    lines.append('quarterly averages. Metrics: MAE, RMSE, MAPE, bias, and 95% CI hit ')
    lines.append('rate per horizon. A 95% hit rate of ~95% indicates well-calibrated ')
    lines.append('uncertainty bands; materially lower means the CIs are too tight, ')
    lines.append('materially higher means they are too wide.')
    lines.append('')

    # ── Leaderboard ──
    lines.append('## Leaderboard (Q+1 MAPE, lower is better)')
    lines.append('')
    lines.append('| Commodity | n pivots | Q+1 MAPE % | Q+1 CI95 hit % | Q+4 MAPE % | Q+4 CI95 hit % |')
    lines.append('|---|---:|---:|---:|---:|---:|')
    leaderboard = []
    for commodity, bundle in results.items():
        m = bundle['metrics']
        if m.empty:
            continue
        q1 = m[m['horizon'] == 1]
        q4 = m[m['horizon'] == 4]
        q1_mape = float(q1['mape_pct'].iloc[0]) if not q1.empty else math.nan
        q1_hit  = float(q1['ci95_hit_pct'].iloc[0]) if not q1.empty else math.nan
        q4_mape = float(q4['mape_pct'].iloc[0]) if not q4.empty else math.nan
        q4_hit  = float(q4['ci95_hit_pct'].iloc[0]) if not q4.empty else math.nan
        n       = int(q1['n'].iloc[0]) if not q1.empty else 0
        leaderboard.append((commodity, n, q1_mape, q1_hit, q4_mape, q4_hit))
    leaderboard.sort(key=lambda r: (math.isnan(r[2]), r[2]))
    for commodity, n, q1m, q1h, q4m, q4h in leaderboard:
        lines.append(f'| {commodity} | {n} | {q1m:.2f} | {q1h:.1f} | {q4m:.2f} | {q4h:.1f} |')
    lines.append('')

    # ── Per-commodity ──
    lines.append('## Per-commodity breakdown')
    lines.append('')
    for commodity, bundle in results.items():
        m = bundle['metrics']
        consensus = bundle.get('consensus')
        lines.append(f'### {commodity}')
        lines.append('')
        if m.empty:
            lines.append('_No completed backtest observations (insufficient history or fit failures)._')
            lines.append('')
            continue
        lines.append('**Model**')
        lines.append('')
        lines.append('| horizon | n | MAE | RMSE | MAPE % | bias | CI95 hit % |')
        lines.append('|---:|---:|---:|---:|---:|---:|---:|')
        for _, r in m.iterrows():
            lines.append(
                f"| Q+{int(r['horizon'])} | {int(r['n'])} | "
                f"{r['mae']:.2f} | {r['rmse']:.2f} | {r['mape_pct']:.2f} | "
                f"{r['bias']:+.2f} | {r['ci95_hit_pct']:.1f} |"
            )
        lines.append('')
        if consensus is not None and not consensus.empty:
            lines.append('**Consensus comparison (bank entries with realized quarters)**')
            lines.append('')
            lines.append('| source | quarter | forecast | realized | abs error % |')
            lines.append('|---|---|---:|---:|---:|')
            for _, r in consensus.iterrows():
                lines.append(
                    f"| {r['source']} | {r['q_key']} | "
                    f"{r['forecast']:.2f} | {r['realized']:.2f} | {r['pct_err']:.2f} |"
                )
            lines.append('')
        lines.append('')

    lines.append('---')
    lines.append('')
    lines.append('_Generated by `scripts/backtest_commodities.py`._')
    lines.append('')

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        f.write('\n'.join(lines))


# ── CLI ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--months-back', type=int, default=60,
                        help='Backtest window length in months (default 60).')
    parser.add_argument('--commodity', default=None,
                        help='Filter to a single commodity name (case-sensitive).')
    parser.add_argument('--out', default='docs/backtest_results.md',
                        help='Output markdown path (default docs/backtest_results.md).')
    parser.add_argument('--mape-fail-threshold', type=float, default=25.0,
                        help='Exit non-zero if any commodity Q+1 MAPE exceeds this (default 25).')
    parser.add_argument('--fail-fast', action='store_true',
                        help='Stop at the first commodity failure.')
    parser.add_argument('--log-level', default='INFO')
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )

    commodities = [args.commodity] if args.commodity else list(cm.TICKERS.keys())
    end_pivot = date.today().replace(day=1) - timedelta(days=1)
    start_pivot = end_pivot - timedelta(days=30 * args.months_back)
    pivots = _pivots(start_pivot, end_pivot)
    logger.info(f'Running backtest on {len(commodities)} commodities × {len(pivots)} pivots')

    results: dict[str, dict] = {}
    for commodity in commodities:
        logger.info(f'── {commodity} ──')
        df = _run_one_commodity(commodity, pivots)
        if df.empty:
            logger.warning(f'{commodity}: no backtest rows produced')
            results[commodity] = {'metrics': pd.DataFrame(), 'consensus': None}
            continue
        metrics = _metrics(df)

        # Consensus comparison — skip gracefully if tracker unavailable
        consensus = None
        try:
            full = cm.DriverFetcher._fetch_yf(cm.TICKERS[commodity], date(2005, 1, 1))
            if full is not None:
                consensus = _consensus_errors(commodity, _quarterly_averages(full))
        except Exception as e:
            logger.warning(f'{commodity}: consensus compare skipped: {e}')

        results[commodity] = {'metrics': metrics, 'consensus': consensus, 'rows': df}

        if args.fail_fast:
            q1 = metrics[metrics['horizon'] == 1]
            if not q1.empty and float(q1['mape_pct'].iloc[0]) > args.mape_fail_threshold:
                logger.error(f'{commodity}: Q+1 MAPE {q1["mape_pct"].iloc[0]:.1f}% > threshold — failing fast')
                break

    out_path = os.path.join(_ROOT, args.out) if not os.path.isabs(args.out) else args.out
    _write_report(results, out_path, args.months_back)
    logger.info(f'Report written to {out_path}')

    # Exit code
    worst_q1 = -1.0
    for bundle in results.values():
        m = bundle['metrics']
        if m.empty:
            continue
        q1 = m[m['horizon'] == 1]
        if not q1.empty:
            worst_q1 = max(worst_q1, float(q1['mape_pct'].iloc[0]))
    if worst_q1 >= 0 and worst_q1 > args.mape_fail_threshold:
        logger.error(f'Worst Q+1 MAPE {worst_q1:.2f}% exceeds threshold {args.mape_fail_threshold}%')
        sys.exit(1)
    logger.info(f'Backtest complete. Worst Q+1 MAPE: {worst_q1:.2f}%')


if __name__ == '__main__':
    main()
