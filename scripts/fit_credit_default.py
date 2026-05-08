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
    args = parser.parse_args()

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
                    state = cd_fit.fit_gbm(horizon_years=h, years_back=args.years_back)
            except Exception as e:
                print(f'[fit] FAILED ({est}, h={h}): {e}')
                summary[f'{est}_h{h}'] = {'error': str(e)}
                continue
            summary[f'{est}_h{h}'] = {
                'auc': state.get('auc_in_sample'),
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


if __name__ == '__main__':
    sys.exit(main())
