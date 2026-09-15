#!/usr/bin/env python3
"""
Backtest the sovereign credit-default model with Tellimer-style
event-time analysis (2025 white paper §3).

Produces a JSON report + optional Excel with the sensitivity table
per horizon at 5 operational thresholds (0.20 / 0.30 / 0.40 / 0.50 /
0.60), matching Tellimer's Tables 1 / 2 / 3 layout.

Usage
-----
    # Fastest — country-level 5-fold OOS, all three horizons.
    python scripts/backtest_credit_default.py --horizon all

    # Tighter — expanding-window walk-forward (Tellimer's headline mode).
    python scripts/backtest_credit_default.py --horizon all --method walkforward

    # Custom threshold sweep.
    python scripts/backtest_credit_default.py --horizon 1 \\
        --thresholds 0.15,0.20,0.25,0.30,0.35,0.40,0.45,0.50

    # Write Excel report alongside the JSON.
    python scripts/backtest_credit_default.py --horizon all --excel report.xlsx
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List

# Allow running as `python scripts/backtest_credit_default.py` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.credit_default import backtest as cd_backtest


DEFAULT_OUT = Path('data/backtest_report.json')


def parse_horizons(raw: str) -> List[int]:
    if raw == 'all':
        return [1, 3, 5]
    return [int(x) for x in raw.split(',') if x.strip()]


def parse_thresholds(raw: str) -> List[float]:
    return [float(x) for x in raw.split(',') if x.strip()]


def write_excel(report: dict, path: Path) -> None:
    try:
        from openpyxl import Workbook
    except ImportError:
        print(f'[backtest] openpyxl not available — skipping Excel export ({path})')
        return
    wb = Workbook()
    ws = wb.active
    ws.title = 'Summary'
    ws.append(['Horizon', 'Method', 'N obs OOS', 'N countries OOS',
               'AUC OOS', 'Brier OOS', 'Unconditional PD'])
    for hz in report.get('horizons', []):
        ws.append([hz['horizon_years'], hz['method'], hz['n_obs_oos'],
                   hz['n_countries_oos'], hz.get('auc_oos'),
                   hz.get('brier_oos'), hz.get('unconditional_pd')])

    for hz in report.get('horizons', []):
        sheet_name = f'Sensitivity {hz["horizon_years"]}Y'[:31]
        ws2 = wb.create_sheet(sheet_name)
        if not hz['sensitivity_table']:
            continue
        cols = list(hz['sensitivity_table'][0].keys())
        ws2.append(cols)
        for row in hz['sensitivity_table']:
            ws2.append([row.get(c) for c in cols])
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    print(f'[backtest] wrote {path}')


def print_summary(report: dict) -> None:
    for hz in report.get('horizons', []):
        h = hz['horizon_years']
        print(f'\n─── Horizon {h}Y ({hz["method"]}) '
              f'│ AUC OOS {hz.get("auc_oos"):.3f} '
              f'│ Brier OOS {hz.get("brier_oos"):.4f} '
              f'│ base rate {hz.get("unconditional_pd"):.3%} ───')
        table = hz['sensitivity_table']
        if not table:
            print('  (no sensitivity rows produced)')
            continue
        header = f'{"thresh":>6}  {"crises":>6}  {"signalled":>9}  {"true":>6}  {"falseA":>6}  {"NSR":>5}  {"P(C|S)":>7}  {"lead":>5}  {"persist":>7}'
        print(header)
        print('-' * len(header))
        for r in table:
            print(f'{r["threshold"]:>6.2f}  '
                  f'{r["n_crises"]:>6}  '
                  f'{r["pct_crises_signalled"]:>8.1f}%  '
                  f'{r["true_signals_pct"]:>5.1f}%  '
                  f'{r["false_alarms_pct"]:>5.2f}%  '
                  f'{r["noise_to_signal"]:>5.2f}  '
                  f'{r["precision_pct"]:>6.1f}%  '
                  f'{r["lead_time_months"]:>5.1f}  '
                  f'{r["persistence_months"]:>7.1f}')


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--horizon', default='all',
                        help='"all" for 1,3,5 (default) or comma-separated e.g. 1,3')
    parser.add_argument('--method', choices=['groupkfold', 'walkforward'],
                        default='groupkfold',
                        help='groupkfold = country-level 5-fold (fast); '
                             'walkforward = expanding-window by year (Tellimer headline).')
    parser.add_argument('--thresholds', default='0.20,0.30,0.40,0.50,0.60')
    parser.add_argument('--years-back', type=int, default=25)
    parser.add_argument('--out', default=str(DEFAULT_OUT))
    parser.add_argument('--excel', default='',
                        help='Optional path to write an Excel report alongside the JSON.')
    args = parser.parse_args()

    horizons = parse_horizons(args.horizon)
    thresholds = parse_thresholds(args.thresholds)

    report = {
        'method': args.method,
        'thresholds': thresholds,
        'horizons': [],
    }

    for h in horizons:
        print(f'\n[backtest] {args.method} for horizon={h}y …')
        try:
            hz = cd_backtest.backtest_horizon(
                horizon_years=h, method=args.method,
                thresholds=thresholds, years_back=args.years_back,
            )
        except Exception as e:
            print(f'[backtest] FAILED (h={h}): {e}')
            hz = {'horizon_years': h, 'method': args.method, 'error': str(e)}
        report['horizons'].append(hz)

    print_summary(report)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, 'w') as f:
        json.dump(report, f, indent=2)
    print(f'\n[backtest] wrote {args.out}')

    if args.excel:
        write_excel(report, Path(args.excel))

    return 0


if __name__ == '__main__':
    sys.exit(main())
