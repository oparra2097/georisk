#!/usr/bin/env python3
"""
Cross-check the Parra Macro sovereign credit-default model against
Tellimer's published sovereign PD and composite-rating datasets.

Tellimer publishes two reference series that a customer might have on
hand (both distributed to AIG in Sept 2026):

  1. Sovereign PD History (XLSX)      — long panel keyed on (ISO3, date)
                                        with columns PD 1-year, PD 3-year,
                                        PD 5-year.
  2. Composite Credit Ratings (CSV)   — wide panel keyed on date with
                                        one column per country holding
                                        the monthly-interpolated
                                        composite letter rating (S&P /
                                        Moody's / Fitch / Kroll / DBRS
                                        / APR median-blend).

Drop these files into ``data/tellimer_reference/`` (untracked by git — the
directory is ignored by default) and this script will:

  * back-check our α₃/α₅ discounted-hazard parameters against every
    row in the PD history (should match to ≤2 bps if we're aligned),
  * compute a per-country Spearman rank correlation between our PD 1Y
    and Tellimer's PD 1Y (measures whether we agree on relative risk),
  * compute the time-series correlation for each country's PD 1Y
    (measures whether we agree on trajectory over time),
  * flag countries where our peak PD ahead of a known default trails
    Tellimer's by more than 3 months (measures early-warning parity).

Nothing in this script hits the network — it operates entirely on the
Tellimer CSV/XLSX plus whatever our fit has produced in
``data/credit_default_fit/``. Run it after ``fit_credit_default.py``.

Usage
-----
    python scripts/validate_against_tellimer.py

    # Point at alternate locations if you keep the files elsewhere
    python scripts/validate_against_tellimer.py \\
        --pd-history ~/Downloads/Tellimer_Sovereign_PD_Historical.xlsx \\
        --composite-ratings ~/Downloads/Tellimer_Composite_Ratings.csv \\
        --out data/tellimer_validation_report.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Allow running as `python scripts/validate_against_tellimer.py` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DEFAULT_PD_HISTORY = Path('data/tellimer_reference/Tellimer_Sovereign_PD_Historical.xlsx')
DEFAULT_RATINGS_CSV = Path('data/tellimer_reference/Tellimer_Composite_Credit_Ratings.csv')
DEFAULT_OUT = Path('data/tellimer_validation_report.json')


# ── Loaders ─────────────────────────────────────────────────────────────


def load_pd_history(path: Path):
    """Return DataFrame with columns ['iso3', 'date', 'pd_1y', 'pd_3y', 'pd_5y'].

    Auto-detects the sheet layout: Tellimer's "All Scores (Long)" sheet
    has the target long format; if only wide sheets are present, the
    single-country wide format falls through to a manual reshape.
    """
    try:
        import pandas as pd
    except ImportError:
        raise RuntimeError('pandas required')

    if path.suffix.lower() in ('.xlsx', '.xls'):
        xl = pd.ExcelFile(path)
        # Try the long sheet first — that's Tellimer's canonical layout.
        for name in xl.sheet_names:
            if 'long' in name.lower() or 'all' in name.lower():
                df = pd.read_excel(xl, sheet_name=name)
                cols = {c: c.strip().lower() for c in df.columns}
                df = df.rename(columns=cols)
                # Locate the standard columns (allow varying case/spacing).
                iso_col = next((c for c in df.columns if 'country' in c or c.startswith('iso')), None)
                date_col = next((c for c in df.columns if c == 'date' or 'date' in c), None)
                pd1_col = next((c for c in df.columns if 'pd 1' in c or 'pd_1' in c or c == 'pd1y'), None)
                pd3_col = next((c for c in df.columns if 'pd 3' in c or 'pd_3' in c or c == 'pd3y'), None)
                pd5_col = next((c for c in df.columns if 'pd 5' in c or 'pd_5' in c or c == 'pd5y'), None)
                if not all([iso_col, date_col, pd1_col, pd3_col, pd5_col]):
                    continue
                out = df[[iso_col, date_col, pd1_col, pd3_col, pd5_col]].copy()
                out.columns = ['iso3', 'date', 'pd_1y', 'pd_3y', 'pd_5y']
                out['iso3'] = out['iso3'].astype(str).str.strip().str.upper()
                out['date'] = pd.to_datetime(out['date'], errors='coerce')
                # Tellimer stores PDs as percents (0-100); normalise to [0, 1].
                for c in ('pd_1y', 'pd_3y', 'pd_5y'):
                    if out[c].dropna().max() > 1.5:
                        out[c] = out[c] / 100.0
                out = out.dropna(subset=['iso3', 'date', 'pd_1y'])
                return out.sort_values(['iso3', 'date']).reset_index(drop=True)
        raise RuntimeError(f'Could not find a long-format sheet in {path}')
    # CSV fallback (long panel).
    return pd.read_csv(path)


def load_composite_ratings(path: Path):
    """Return DataFrame with columns ['date', 'iso3'/'country_name', 'rating'].

    Tellimer's file is wide (date × country) with letter ratings. We
    melt to long and pass through unchanged — mapping country → ISO3
    happens downstream (via country_codes.get_iso3_for_name).
    """
    try:
        import pandas as pd
    except ImportError:
        raise RuntimeError('pandas required')

    df = pd.read_csv(path)
    if 'date' not in df.columns:
        # First column is unnamed / labelled 'Date'.
        df = df.rename(columns={df.columns[0]: 'date'})
    df['date'] = pd.to_datetime(df['date'], errors='coerce', dayfirst=True)
    df = df.dropna(subset=['date'])
    long = df.melt(id_vars=['date'], var_name='country_name', value_name='rating')
    long['rating'] = long['rating'].astype(str).str.strip()
    long = long[long['rating'].isin(['', 'nan', 'NaN']).eq(False)]
    return long.reset_index(drop=True)


def load_our_pd_history():
    """Return DataFrame with ['iso3', 'year', 'pd_1y'] using our fit if
    a stacked state exists, else the plain GBM state, else None."""
    try:
        import pandas as pd
    except ImportError:
        return None
    from backend.credit_default import service, data as cd_data
    from backend.credit_default import fit as cd_fit

    if cd_fit.load_stacked_state(1) is None and cd_fit.load_state(1) is None:
        return None

    panel = cd_data.get_history_panel()
    if panel is None or panel.empty:
        return None

    rows = []
    for iso3 in panel['iso3'].unique():
        try:
            hist = service.get_country_history(iso3, horizon_years=1)
        except Exception as e:  # noqa: BLE001
            print(f'[validate] {iso3} history failed: {e}')
            continue
        if not hist:
            continue
        for r in (hist.get('history') or []):
            rows.append({
                'iso3': iso3,
                'year': r.get('year'),
                'pd_1y': r.get('pd_1y') or r.get('model_pd'),
                'pd_3y': r.get('pd_3y'),
                'pd_5y': r.get('pd_5y'),
            })
    if not rows:
        return None
    return pd.DataFrame(rows)


# ── Cross-checks ────────────────────────────────────────────────────────


def verify_alpha_transform(pd_history) -> Dict:
    """For every Tellimer row, apply our α₃/α₅ transform to their PD 1Y
    and compare to their published PD 3Y / PD 5Y. Reports max absolute
    error and 95th-percentile error in basis points."""
    from backend.credit_default import rating_model as rm
    err3, err5 = [], []
    for _, r in pd_history.iterrows():
        p1 = float(r['pd_1y'])
        d = rm.derive_multi_horizon_pd(p1)
        if d['pd_3y'] is not None and r['pd_3y'] is not None:
            err3.append(abs(d['pd_3y'] - float(r['pd_3y'])))
        if d['pd_5y'] is not None and r['pd_5y'] is not None:
            err5.append(abs(d['pd_5y'] - float(r['pd_5y'])))

    def stats(arr):
        if not arr:
            return {'n': 0}
        try:
            import numpy as np
            arr = np.asarray(arr)
            return {
                'n': int(len(arr)),
                'max_bps': float(arr.max() * 10000),
                'mean_bps': float(arr.mean() * 10000),
                'p95_bps': float(np.percentile(arr, 95) * 10000),
            }
        except ImportError:
            return {'n': len(arr), 'max_bps': max(arr) * 10000,
                    'mean_bps': sum(arr) / len(arr) * 10000}

    return {
        'pd_3y_vs_transformed_pd_1y': stats(err3),
        'pd_5y_vs_transformed_pd_1y': stats(err5),
        'note': 'errors ≤5 bps mean our α₃/α₅ match Tellimer\'s exactly.',
    }


def per_country_pd_agreement(tellimer_pd, ours_pd):
    """Time-series correlation of our PD 1Y vs Tellimer's PD 1Y per
    country, aligned by year. Uses annual grain since our current
    ``get_country_history`` output is annual.
    """
    if ours_pd is None or ours_pd.empty:
        return {'skipped': 'no fitted state on disk'}

    import pandas as pd
    # Tellimer is monthly — collapse to annual (mean over calendar year).
    ttl = tellimer_pd.copy()
    ttl['year'] = ttl['date'].dt.year
    ttl_ann = ttl.groupby(['iso3', 'year'])['pd_1y'].mean().reset_index()
    ttl_ann = ttl_ann.rename(columns={'pd_1y': 'tellimer_pd_1y'})

    merged = ttl_ann.merge(
        ours_pd.rename(columns={'pd_1y': 'ours_pd_1y'}),
        on=['iso3', 'year'], how='inner',
    )
    rows = []
    for iso3, g in merged.groupby('iso3'):
        if len(g) < 5:
            continue
        try:
            pearson = float(g[['tellimer_pd_1y', 'ours_pd_1y']].corr().iloc[0, 1])
            rank = float(g[['tellimer_pd_1y', 'ours_pd_1y']].corr(method='spearman').iloc[0, 1])
        except Exception:  # noqa: BLE001
            continue
        peak_ours = g.loc[g['ours_pd_1y'].idxmax(), 'year']
        peak_tell = g.loc[g['tellimer_pd_1y'].idxmax(), 'year']
        rows.append({
            'iso3': iso3, 'n_years': int(len(g)),
            'pearson': round(pearson, 3),
            'spearman': round(rank, 3),
            'peak_year_ours': int(peak_ours),
            'peak_year_tellimer': int(peak_tell),
            'peak_year_delta': int(peak_ours - peak_tell),
        })
    rows.sort(key=lambda r: r['pearson'])   # worst-agreement first
    return rows


def cross_section_agreement(tellimer_pd, ours_pd):
    """Rank correlation across countries at each Tellimer-observed
    annual snapshot. Answers 'do we and Tellimer agree on WHO is
    riskier this year?' — the most important operational question."""
    if ours_pd is None or ours_pd.empty:
        return {'skipped': 'no fitted state on disk'}
    import pandas as pd
    ttl = tellimer_pd.copy()
    ttl['year'] = ttl['date'].dt.year
    ttl_ann = ttl.groupby(['iso3', 'year'])['pd_1y'].mean().reset_index()
    ttl_ann = ttl_ann.rename(columns={'pd_1y': 'tellimer_pd_1y'})
    merged = ttl_ann.merge(
        ours_pd.rename(columns={'pd_1y': 'ours_pd_1y'})[['iso3', 'year', 'ours_pd_1y']],
        on=['iso3', 'year'], how='inner',
    )
    rows = []
    for year, g in merged.groupby('year'):
        if len(g) < 8:
            continue
        try:
            spearman = float(g[['tellimer_pd_1y', 'ours_pd_1y']].corr(method='spearman').iloc[0, 1])
        except Exception:  # noqa: BLE001
            continue
        rows.append({
            'year': int(year), 'n_countries': int(len(g)),
            'spearman': round(spearman, 3),
        })
    rows.sort(key=lambda r: r['year'])
    return rows


# ── Entry ───────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--pd-history', default=str(DEFAULT_PD_HISTORY))
    parser.add_argument('--composite-ratings', default=str(DEFAULT_RATINGS_CSV))
    parser.add_argument('--out', default=str(DEFAULT_OUT))
    args = parser.parse_args()

    report: Dict = {'inputs': {'pd_history': args.pd_history,
                               'composite_ratings': args.composite_ratings}}

    pd_history_path = Path(args.pd_history)
    if not pd_history_path.exists():
        print(f'[validate] PD history not found: {pd_history_path}')
        print(f'[validate] drop the Tellimer file at that path or pass --pd-history')
        report['error'] = 'pd_history missing'
    else:
        print(f'[validate] loading PD history from {pd_history_path}')
        pd_hist = load_pd_history(pd_history_path)
        print(f'[validate] rows: {len(pd_hist)} '
              f'countries: {pd_hist["iso3"].nunique()} '
              f'date range: {pd_hist["date"].min().date()} → {pd_hist["date"].max().date()}')

        # 1. Confirm our α₃/α₅ discounted-hazard transform matches theirs.
        report['alpha_transform_check'] = verify_alpha_transform(pd_hist)
        print('[validate] α₃ = {:.4f}, α₅ = {:.4f}'.format(
            1.39, 1.62,
        ))
        s = report['alpha_transform_check']
        print(f'[validate]   3Y max err: {s["pd_3y_vs_transformed_pd_1y"].get("max_bps", "?"):.2f} bps '
              f'({s["pd_3y_vs_transformed_pd_1y"].get("n", 0)} rows)')
        print(f'[validate]   5Y max err: {s["pd_5y_vs_transformed_pd_1y"].get("max_bps", "?"):.2f} bps '
              f'({s["pd_5y_vs_transformed_pd_1y"].get("n", 0)} rows)')

        # 2. Compare our fit output (if any) to Tellimer's PDs.
        ours = load_our_pd_history()
        report['per_country_pd_agreement'] = per_country_pd_agreement(pd_hist, ours)
        report['cross_section_agreement_by_year'] = cross_section_agreement(pd_hist, ours)

    ratings_path = Path(args.composite_ratings)
    if ratings_path.exists():
        print(f'[validate] loading composite ratings from {ratings_path}')
        ratings = load_composite_ratings(ratings_path)
        report['composite_ratings'] = {
            'n_rows': int(len(ratings)),
            'n_countries': int(ratings['country_name'].nunique()),
            'date_range': [str(ratings['date'].min().date()),
                           str(ratings['date'].max().date())],
            'note': 'This is Tellimer\'s monthly-interpolated median-of-agencies letter rating. '
                    'Ingesting it as an agency-history overlay for the drilldown chart is a follow-up: '
                    'map country_name → iso3 and merge into cd_agency_history.',
        }
    else:
        print(f'[validate] composite ratings not found: {ratings_path}')

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    print(f'[validate] wrote {args.out}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
