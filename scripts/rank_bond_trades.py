#!/usr/bin/env python3
"""
Rank sovereign bond trades by comparing model PDs to market-implied PDs.

Long the bonds where market_pd > model_pd (market pricing more risk than
warranted, bonds are cheap). Short — or at minimum avoid — bonds where
model_pd > market_pd (market complacent, bonds are rich).

Usage
-----
    # Top 10 longs and top 10 shorts, 1Y horizon, 60% LGD
    python scripts/rank_bond_trades.py

    # Wider net with a smaller edge threshold, 3Y horizon
    python scripts/rank_bond_trades.py --horizon 3 --top-n 20 --min-edge 1.0

    # 40% recovery (higher LGD) and Excel export
    python scripts/rank_bond_trades.py --lgd 0.7 --out data/bond_trades.xlsx
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Allow running from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _fmt_pct(v, dp=2):
    if v is None:
        return '—'
    try:
        return f'{float(v)*100:>{6}.{dp}f}%'
    except (TypeError, ValueError):
        return '—'


def _fmt_pp(v, dp=2):
    try:
        return f'{float(v):+7.{dp}f}pp'
    except (TypeError, ValueError):
        return '—'


def print_side(title: str, rows, horizon: int):
    print(f'\n═══ {title} ({len(rows)} names) ═══')
    if not rows:
        print('  (no names cleared the min-edge threshold)')
        return
    header = f'{"iso3":<5} {"country":<24} {"model":<5} {"yield":>7} {"spread":>8} {"model PD":>9} {"market PD":>10} {"edge (pp)":>10}  signal'
    print(header)
    print('-' * len(header))
    for r in rows:
        print(f'{r["iso3"]:<5} {r["name"][:24]:<24} '
              f'{(r["pm_notch"] or "?"):<5} '
              f'{r["yield_pct"]:>6.2f}% '
              f'{r["spread_bps"]:>6.0f}bp '
              f'{_fmt_pct(r["model_pd"])} '
              f'{_fmt_pct(r["market_pd"])} '
              f'{_fmt_pp(r["edge_pp"])}  '
              f'{r["signal"]}')


def write_excel(report: dict, path: Path) -> None:
    try:
        from openpyxl import Workbook
    except ImportError:
        print(f'[bond-trades] openpyxl not installed; skipping Excel export ({path})')
        return
    wb = Workbook()
    ws = wb.active
    ws.title = 'All'
    cols = ['iso3', 'name', 'region', 'currency', 'pm_notch', 'agency_sp',
            'yield_pct', 'spread_bps', 'model_pd', 'market_pd',
            'edge_pp', 'signal', 'fx_risk_tag']
    ws.append(cols)
    for r in report.get('all', []):
        ws.append([r.get(c) for c in cols])
    for side in ('longs', 'shorts'):
        ws2 = wb.create_sheet(side.title())
        ws2.append(cols)
        for r in report.get(side, []):
            ws2.append([r.get(c) for c in cols])
    ws3 = wb.create_sheet('Summary')
    for k, v in (report.get('summary') or {}).items():
        ws3.append([k, v])
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    print(f'\n[bond-trades] wrote {path}')


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--horizon', type=int, choices=[1, 3, 5], default=1)
    parser.add_argument('--top-n', type=int, default=10)
    parser.add_argument('--lgd', type=float, default=0.60,
                        help='Loss-given-default assumption (default 0.60 = 40%% recovery)')
    parser.add_argument('--min-edge', type=float, default=2.0,
                        help='Minimum |edge| in percentage points to appear in ranked lists')
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--out', default='')
    args = parser.parse_args()

    from backend.credit_default import bond_trades as bt

    report = bt.get_bond_trades(
        top_n=args.top_n, lgd=args.lgd, min_edge_pct=args.min_edge,
        horizon=args.horizon,
    )

    summary = report.get('summary') or {}
    print(f'UST 10Y benchmark  : {summary.get("benchmark_ust_10y_pct")}%')
    print(f'LGD assumption     : {int(args.lgd*100)}% (recovery {int((1-args.lgd)*100)}%)')
    print(f'Horizon            : {args.horizon}Y')
    print(f'Sovereigns covered : {summary.get("n_covered")}')

    print_side('LONGS — market pricing MORE risk than model', report['longs'], args.horizon)
    print_side('SHORTS — market pricing LESS risk than model', report['shorts'], args.horizon)

    if args.json:
        print()
        print(json.dumps(report, indent=2, default=str))

    if args.out:
        p = Path(args.out)
        if p.suffix.lower() in ('.xlsx',):
            write_excel(report, p)
        else:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(report, indent=2, default=str))
            print(f'[bond-trades] wrote {p}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
