#!/usr/bin/env python3
"""
Fit the sovereign credit-default model from the IMF/WB indicator panel
joined to data/sovereign_defaults.csv.

Usage:
    # Logistic regression for 1-year horizon
    python scripts/fit_credit_default.py --estimator logit --horizon 1

    # Gradient-boosted classifier for 3-year horizon
    python scripts/fit_credit_default.py --estimator gbm --horizon 3

    # All horizons, both estimators
    python scripts/fit_credit_default.py --estimator both --horizon all

The fitted state is written to ``data/credit_default_fit/fit_state_h{H}.json``
and the live dashboard picks it up automatically on the next request
(rating_model.py loads it lazily; refresh the /api/credit-default/refresh
endpoint or restart the worker to clear the cached panel).
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Allow running as `python scripts/fit_credit_default.py` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.credit_default import fit as cd_fit


def parse_horizons(raw: str):
    if raw == 'all':
        return [1, 3, 5]
    return [int(x) for x in raw.split(',') if x.strip()]


def parse_horizons_quarters(raw: str):
    if raw == 'all':
        return [4, 12, 20]   # quarterly equivalents of 1y / 3y / 5y
    return [int(x) for x in raw.split(',') if x.strip()]


def parse_estimators(raw: str):
    if raw == 'both':
        return ['logit', 'gbm']
    return [raw]


def main() -> int:
    parser = argparse.ArgumentParser(description='Fit sovereign credit-default model')
    parser.add_argument('--estimator', choices=['logit', 'gbm', 'both'],
                        default='logit')
    parser.add_argument('--horizon', default='1',
                        help='Comma-separated horizons (years). Use "all" for 1,3,5.')
    parser.add_argument('--years-back', type=int, default=25,
                        help='How many years of history to include in the panel.')
    parser.add_argument('--cadence', choices=['annual', 'quarterly'],
                        default='annual',
                        help='Panel grain. quarterly forward-fills annuals to '
                             'quarter-rows and uses defaulted_within_Nq targets.')
    parser.add_argument('--horizon-quarters', default='all',
                        help='Comma-separated horizons in quarters (used when '
                             '--cadence quarterly). "all" maps to 4,12,20 '
                             '(matches 1y/3y/5y).')
    parser.add_argument('--label-mode', choices=['state', 'onset'],
                        default='state',
                        help='state = Option-C (positive if currently in '
                             'default OR onset within h years); '
                             'onset = Bloomberg/Moodys CreditEdge convention '
                             '(positive only on new onset; in-default rows '
                             'dropped). Onset state files use the _onset suffix.')
    args = parser.parse_args()

    if args.cadence == 'quarterly':
        return _run_quarterly(args)

    horizons = parse_horizons(args.horizon)
    estimators = parse_estimators(args.estimator)

    summary = {}
    for h in horizons:
        for est in estimators:
            print(f'\n[fit] running {est} for horizon={h}y …')
            try:
                if est == 'logit':
                    state = cd_fit.fit_logit(horizon_years=h, years_back=args.years_back)
                else:
                    state = cd_fit.fit_gbm(
                        horizon_years=h, years_back=args.years_back,
                        label_mode=args.label_mode,
                    )
            except Exception as e:
                print(f'[fit] FAILED ({est}, h={h}): {e}')
                summary[f'{est}_h{h}'] = {'error': str(e)}
                continue
            summary[f'{est}_h{h}'] = {
                'auc_in_sample': state.get('auc_in_sample'),
                'auc_oos': state.get('auc_oos'),
                'brier_oos': state.get('brier_oos'),
                'oos_method': state.get('oos_method'),
                'n_obs': state.get('n_obs'),
                'n_events': state.get('n_events'),
                'top_features': sorted(
                    state.get('feature_importance', {}).items(),
                    key=lambda kv: kv[1], reverse=True,
                )[:5],
            }

    print('\n[fit] summary:')
    print(json.dumps(summary, indent=2, default=str))
    return 0


def _run_quarterly(args) -> int:
    horizons_q = parse_horizons_quarters(args.horizon_quarters)
    summary = {}
    for h_q in horizons_q:
        print(f'\n[fit] running quarterly GBM for horizon={h_q}q ({h_q/4:.1f}y) …')
        try:
            state = cd_fit.fit_gbm_quarterly(
                horizon_quarters=h_q, years_back=args.years_back,
            )
        except Exception as e:
            print(f'[fit] FAILED (quarterly h={h_q}q): {e}')
            summary[f'gbm_q{h_q}'] = {'error': str(e)}
            continue
        summary[f'gbm_q{h_q}'] = {
            'auc': state.get('auc_in_sample'),
            'n_obs': state.get('n_obs'),
            'n_events': state.get('n_events'),
            'top_features': sorted(
                state.get('feature_importance', {}).items(),
                key=lambda kv: kv[1], reverse=True,
            )[:5],
        }
    print('\n[fit] quarterly summary:')
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main())
