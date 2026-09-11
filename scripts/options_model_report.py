#!/usr/bin/env python3
"""
Terminal report for the equity volatility / options model.

Usage:
    # Default: TSEM, one-month horizon, GARCH engine, risk-neutral drift
    python scripts/options_model_report.py

    # Another name, three-month horizon, the stock's own return history
    python scripts/options_model_report.py MU --horizon 63 --engine bootstrap

    # Impose a view: +25%/yr, and skip the (slow) walk-forward test
    python scripts/options_model_report.py TSEM --drift custom --mu 0.25 --no-backtest

    # Raw payload for piping elsewhere
    python scripts/options_model_report.py TSEM --json > tsem.json

Needs a live Yahoo Finance connection — the same data the /options-model page
uses. Nothing is written to disk; this is the same payload the API serves,
rendered for a terminal.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Allow running as `python scripts/options_model_report.py` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.options_model import service  # noqa: E402


def _pct(v, dp=1):
    return '—' if v is None else f'{v * 100:.{dp}f}%'


def _money(v):
    return '—' if v is None else f'${v:,.2f}'


def _rule(title=''):
    line = '─' * 78
    return f'\n{title}\n{line}' if title else line


def report(p):
    m, sim = p['meta'], p['simulation']
    s = sim['settings']
    out = []

    out.append(_rule(f"{m['ticker']} — {m['name']}"))
    chg = '' if m['change_pct'] is None else f"{m['change_pct']:+.2f}% on the day   "
    out.append(f"Spot {_money(m['spot'])}   {chg}"
               f"close {m['as_of']}   {m['history_days']} sessions")
    if m.get('earnings_date'):
        out.append(f"Next earnings: {m['earnings_date']}")
    out.append(f"Engine {s['engine_label']}   horizon {s['horizon_days']}d "
               f"({s['horizon_calendar_days']} cal)   drift {s['drift_mode']}   "
               f"{s['n_paths']:,} paths   r={_pct(m['risk_free'], 2)}")

    # ── Volatility ───────────────────────────────────────────────────────
    out.append(_rule('VOLATILITY'))
    rv = p['vol']['realized']
    for key, label in (('close_21', 'Close-to-close 21d'),
                       ('close_63', 'Close-to-close 63d'),
                       ('close_252', 'Close-to-close 252d'),
                       ('ewma', 'EWMA (0.94)'),
                       ('yang_zhang_21', 'Yang-Zhang 21d')):
        if rv.get(key) is not None:
            out.append(f'  {label:<24} {_pct(rv[key], 1)}')
    if p['vol'].get('percentile') is not None:
        out.append(f"  {'Vol percentile (2y)':<24} {p['vol']['percentile']:.0f}th")
    g = p['vol'].get('garch')
    if g:
        out.append(f"  GARCH(1,1): alpha {g['alpha']:.3f}  beta {g['beta']:.3f}  "
                   f"persistence {g['persistence']:.3f}  half-life "
                   f"{g['half_life_days']:.1f}d  t-df {g['nu']:.1f}")
        out.append(f"  Spot vol {_pct(g['spot_vol'])} -> long-run {_pct(g['long_run_vol'])}")

    # ── Direction ────────────────────────────────────────────────────────
    out.append(_rule('DIRECTION PROBABILITY'))
    out.append(f"  {'Horizon':<10}{'Vol':>8}{'P(up)':>9}{'1sd':>9}"
               f"{'68% range':>22}{'Drag':>9}")
    for d in p['direction']:
        band = f"{_money(d['band68'][0])}–{_money(d['band68'][1])}"
        out.append(f"  {str(d['days']) + 'd':<10}{_pct(d['vol'], 0):>8}"
                   f"{_pct(d['p_up']):>9}{_pct(d['one_sd_move'], 1):>9}"
                   f"{band:>22}{_pct(d['variance_drag'], 2):>9}")

    out.append('')
    out.append('  Engine comparison at the primary horizon:')
    for e in sim['engines']:
        out.append(f"    {e['engine_label']:<18} P(up) {_pct(e['p_up'])}  "
                   f"move ±{_pct(e['expected_abs_move'])}  "
                   f"kurtosis {e['excess_kurtosis']:.1f}  "
                   f"CVaR5 {_pct(e['cvar05'])}")

    # ── Chain ────────────────────────────────────────────────────────────
    if p.get('chain_vol'):
        out.append(_rule('IMPLIED vs MODEL VOL'))
        out.append(f"  {'Expiry':<12}{'DTE':>5}{'ATM IV':>9}{'Model':>9}"
                   f"{'VRP':>8}{'Skew':>8}{'Implied move':>15}")
        for c in p['chain_vol']:
            vrp = '—' if c['vrp'] is None else f"{c['vrp'] * 100:+.1f}"
            skew = '—' if c['skew_25d'] is None else f"{c['skew_25d'] * 100:+.1f}"
            im = '—' if c['implied_move_pct'] is None else f"±{c['implied_move_pct']:.1f}%"
            out.append(f"  {c['expiry']:<12}{c['dte']:>5}{_pct(c['atm_iv'], 0):>9}"
                       f"{_pct(c['model_vol'], 0):>9}{vrp:>8}{skew:>8}{im:>15}")

    # ── Trades ───────────────────────────────────────────────────────────
    st = p.get('strategies') or {}
    rows = st.get('ranked') or []
    out.append(_rule('TRADE SCREEN — structures surviving the stress test'))
    stress = st.get('vol_stress_points')
    if stress:
        out.append(f"  Vol stress: ±{stress * 100:.1f} vol points "
                   f"(the model's own out-of-sample forecast error)")
    if not rows:
        out.append('  Nothing survives. No structure keeps a positive expected value once you')
        out.append('  pay the spread and allow the vol forecast to be wrong by its historical')
        out.append('  error. That is a finding, not a gap in the data.')
    else:
        out.append(f"  {'Structure':<34}{'DTE':>5}{'EV':>9}{'Vol−':>9}{'Vol+':>9}"
                   f"{'POP':>8}{'EV/cap':>9}{'MaxLoss':>10}")
        for t in rows:
            sx = t.get('ev_stress') or {}
            ml = 'unbnd' if t['max_loss'] is None else f"${abs(t['max_loss']):,.0f}"
            out.append(f"  {t['name'][:33]:<34}{t['dte']:>5}{t['ev']:>9.0f}"
                       f"{sx.get('vol_down', 0):>9.0f}{sx.get('vol_up', 0):>9.0f}"
                       f"{_pct(t['pop'], 0):>8}{_pct(t['ev_on_capital'], 1):>9}{ml:>10}")
        out.append(f"\n  {st.get('n_credible', 0)} of {st.get('n_total', 0)} structures "
                   f"scored. Legs of the top trade:")
        for l in rows[0]['legs']:
            if l['kind'] == 'stock':
                out.append('    +100 shares')
            else:
                out.append(f"    {l['qty']:+d} {l['kind']:<4} {l['strike']:g} "
                           f"@ {_money(l['price'])}  IV {_pct(l['iv'], 1)}")

    # ── Calibration ──────────────────────────────────────────────────────
    bt = p.get('backtest') or {}
    if bt.get('available'):
        out.append(_rule('WALK-FORWARD CALIBRATION'))
        d = bt.get('direction') or {}
        out.append(f"  Direction  Brier {d.get('brier_model', 0):.4f} vs "
                   f"{d.get('brier_coinflip', 0):.4f} for a coin flip "
                   f"(skill {d.get('skill_vs_coinflip', 0):+.1%}) over "
                   f"{bt.get('n_windows')} windows")
        for name, v in (bt.get('volatility') or {}).items():
            mark = ' <- best' if name == bt.get('best_vol_model') else ''
            out.append(f"  Vol {name:<8} RMSE {v['rmse'] * 100:5.1f} pts   "
                       f"R2 {v['r2']:5.2f}   MZ slope {v['mz_slope']:5.2f}{mark}")
        for iv in (bt.get('coverage') or {}).get('intervals') or []:
            out.append(f"  Coverage {iv['nominal']:.0%} nominal -> "
                       f"{iv['empirical']:.0%} actual")

    out.append(_rule('READ'))
    for line in _wrap(p.get('narrative', ''), 76):
        out.append('  ' + line)
    out.append('')
    out.append('  Research output, not investment advice. Short structures can lose')
    out.append('  more than the capital shown.')
    out.append('')
    return '\n'.join(out)


def _wrap(text, width):
    words, line, lines = text.split(), '', []
    for w in words:
        if len(line) + len(w) + 1 > width:
            lines.append(line)
            line = w
        else:
            line = f'{line} {w}'.strip()
    if line:
        lines.append(line)
    return lines


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('ticker', nargs='?', default='TSEM')
    ap.add_argument('--horizon', type=int, default=service.DEFAULT_HORIZON,
                    help='simulation horizon in trading days (default 21)')
    ap.add_argument('--engine', default=service.DEFAULT_ENGINE,
                    choices=['gbm', 'student_t', 'garch_t', 'bootstrap'])
    ap.add_argument('--drift', default=service.DEFAULT_DRIFT,
                    choices=['risk_neutral', 'zero', 'historical', 'custom'])
    ap.add_argument('--mu', type=float, default=None,
                    help='annual simple return, used with --drift custom (0.25 = +25%%)')
    ap.add_argument('--paths', type=int, default=service.DEFAULT_PATHS)
    ap.add_argument('--no-backtest', action='store_true',
                    help='skip the walk-forward test (several seconds faster)')
    ap.add_argument('--json', action='store_true', help='dump the raw payload')
    args = ap.parse_args()

    payload = service.build_payload(
        ticker=args.ticker, horizon=args.horizon, engine=args.engine,
        drift_mode=args.drift, n_paths=args.paths, custom_drift=args.mu,
        with_backtest=not args.no_backtest)

    if payload is None:
        print(f'No price history for {args.ticker.upper()} — check the symbol '
              f'and that Yahoo Finance is reachable.', file=sys.stderr)
        return 1

    print(json.dumps(payload, indent=2, default=str) if args.json else report(payload))
    return 0


if __name__ == '__main__':
    sys.exit(main())
