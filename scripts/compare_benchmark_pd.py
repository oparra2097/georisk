"""
Compare the live 1y PD output of the credit-default model against an
external benchmark matrix (54 ISOs × 9 monthly columns, Jul 2025 –
Mar 2026, values in percent).

Our model is annual / quarterly. We map each benchmark month onto the
closest panel period:
  Jul-25..Sep-25 → 2025Q3
  Oct-25..Dec-25 → 2025Q4
  Jan-26..Mar-26 → 2026Q1
  Apr-26..Jun-26 → 2026Q2
We use the quarterly fit (h=4q == 1y horizon) so each month gets the
quarterly PD for its enclosing quarter. Then compute per-country,
aggregate (MAE/RMSE/bias/corr) and top-10 deviations.
"""

import csv
import math
import os
import sys


def _parse_month(col):
    """'Jul-25' → (2025, 3, '2025Q3')."""
    name, yy = col.split('-')
    y = 2000 + int(yy)
    m_idx = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'].index(name) + 1
    q = (m_idx - 1) // 3 + 1
    return y, m_idx, f'{y}Q{q}'


def load_benchmark(path):
    with open(path) as f:
        reader = csv.reader(f)
        header = next(reader)
        months = header[1:]
        rows = {}
        for row in reader:
            if not row or not row[0].strip():
                continue
            iso = row[0].strip().upper()
            vals = {}
            for i, raw in enumerate(row[1:]):
                raw = (raw or '').strip()
                if not raw:
                    continue
                try:
                    vals[months[i]] = float(raw)
                except ValueError:
                    continue
            rows[iso] = vals
        return months, rows


def load_model_pd(iso_list):
    """For each ISO, fetch the quarterly model PD trajectory and return
    {iso3: {YYYYQq: pd_percent}}."""
    from backend.credit_default import service as svc
    out = {}
    for iso in iso_list:
        h = svc.get_country_history(iso, horizon_years=4, cadence='quarterly')
        if not h:
            continue
        out[iso] = {}
        for r in (h.get('history') or []):
            period = r.get('period')
            pd_val = r.get('model_pd')
            if period and pd_val is not None:
                out[iso][period] = float(pd_val) * 100.0  # to percent
    return out


def spearman(x, y):
    n = len(x)
    if n < 3:
        return None
    rx = _rank(x)
    ry = _rank(y)
    return _pearson(rx, ry)


def _rank(arr):
    paired = sorted([(v, i) for i, v in enumerate(arr)])
    ranks = [0.0] * len(arr)
    i = 0
    while i < len(paired):
        j = i
        while j + 1 < len(paired) and paired[j + 1][0] == paired[i][0]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[paired[k][1]] = avg
        i = j + 1
    return ranks


def _pearson(x, y):
    n = len(x)
    if n < 2:
        return None
    mx = sum(x) / n
    my = sum(y) / n
    cov = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    vx = sum((xi - mx) ** 2 for xi in x)
    vy = sum((yi - my) ** 2 for yi in y)
    denom = math.sqrt(vx * vy)
    return cov / denom if denom else None


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else 'benchmark_pd_1yr.csv'
    months, bench = load_benchmark(path)
    print(f'Benchmark: {len(bench)} ISOs × {len(months)} months')
    model_pd = load_model_pd(list(bench.keys()))
    print(f'Model PD loaded for {len(model_pd)} of {len(bench)} ISOs\n')

    # Build paired cells.
    paired = []   # (iso, month, mine, theirs, diff)
    for iso, monthvals in bench.items():
        if iso not in model_pd:
            continue
        for m, theirs in monthvals.items():
            _, _, qkey = _parse_month(m)
            mine = model_pd[iso].get(qkey)
            if mine is None:
                continue
            paired.append((iso, m, mine, theirs, mine - theirs))

    if not paired:
        print('No overlapping cells — check ISO list / month mapping.')
        return

    # ── 1. Per-country Mar 2026 ───────────────────────────────────────
    print('=' * 78)
    print('Per-country snapshot — Mar 2026')
    print('=' * 78)
    print(f"{'iso':>4}  {'mine':>7}  {'theirs':>7}  {'abs_diff':>8}  {'ratio':>6}")
    rows_mar = sorted(
        [p for p in paired if p[1] == 'Mar-26'],
        key=lambda p: -abs(p[4]),
    )
    for iso, m, mine, theirs, diff in rows_mar:
        ratio = mine / theirs if theirs else float('inf')
        print(f"  {iso:>4}  {mine:>6.2f}%  {theirs:>6.2f}%  {abs(diff):>6.2f}pp  {ratio:>6.2f}")

    # ── 2. Aggregate fit stats ────────────────────────────────────────
    mine_arr = [p[2] for p in paired]
    theirs_arr = [p[3] for p in paired]
    diffs = [p[4] for p in paired]
    mae = sum(abs(d) for d in diffs) / len(diffs)
    rmse = math.sqrt(sum(d * d for d in diffs) / len(diffs))
    bias = sum(diffs) / len(diffs)
    pearson = _pearson(mine_arr, theirs_arr)
    spear = spearman(mine_arr, theirs_arr)
    n_iso = len({p[0] for p in paired})
    print()
    print('=' * 78)
    print('Aggregate fit stats (all overlapping cells)')
    print('=' * 78)
    print(f'  n cells           : {len(paired)}  ({n_iso} ISOs × ~{len(paired) // max(1, n_iso)} months)')
    print(f'  MAE               : {mae:.2f} pp')
    print(f'  RMSE              : {rmse:.2f} pp')
    print(f'  Bias (mine-theirs): {bias:+.2f} pp')
    print(f'  Pearson r         : {pearson:.3f}' if pearson is not None else '  Pearson r         : —')
    print(f'  Spearman ρ        : {spear:.3f}' if spear is not None else '  Spearman ρ        : —')

    # ── 3. Top-10 absolute deviations ─────────────────────────────────
    print()
    print('=' * 78)
    print('Top-10 absolute deviations')
    print('=' * 78)
    top = sorted(paired, key=lambda p: -abs(p[4]))[:10]
    print(f"{'iso':>4}  {'month':>7}  {'mine':>7}  {'theirs':>7}  {'diff':>7}")
    for iso, m, mine, theirs, diff in top:
        print(f"  {iso:>4}  {m:>7}  {mine:>6.2f}%  {theirs:>6.2f}%  {diff:+6.2f}pp")


if __name__ == '__main__':
    main()
