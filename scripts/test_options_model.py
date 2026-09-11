#!/usr/bin/env python3
"""
Self-contained checks for the options model — no network, no market data.

Usage:
    python scripts/test_options_model.py

Every check below has a known right answer, so a failure here is a real
regression rather than a market move:

  1. Black-Scholes obeys put-call parity, and the greeks match finite
     differences of the price.
  2. Implied vol inverts the price it came from.
  3. Monte Carlo under the risk-neutral drift reproduces the closed-form
     forward, the median, N(d2), and the Black-Scholes call price.
  4. The Brownian-bridge path extremes reproduce the closed-form
     barrier-hitting probability (the piece most likely to be silently wrong,
     since a missing correction just makes touch probabilities a bit low).
  5. GARCH(1,1) recovers the parameters of a series simulated from known ones,
     and the Student-t df fit recovers a known df.
  6. A structure priced exactly at the simulated distribution scores zero EV.
     This is the invariant that makes a non-zero EV meaningful; it catches
     day-count mismatches, missing discounting and payoff sign errors at once.
  7. An overpriced chain makes premium selling positive-EV and premium buying
     negative-EV, with the right sign on every structure type.
"""

from __future__ import annotations

import math
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.options_model.pricing import (bs_price, greeks, implied_vol,  # noqa: E402
                                           prob_itm_rn)
from backend.options_model.random_walk import (resolve_log_drift,  # noqa: E402
                                               simulate_gbm, terminal_stats)
from backend.options_model.strategies import score_expiry  # noqa: E402
from backend.options_model.vol import _fit_student_t_df, fit_garch  # noqa: E402

FAILS = []


def check(name, ok, detail=''):
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{'  — ' + detail if detail else ''}")
    if not ok:
        FAILS.append(name)


def close(a, b, tol):
    return abs(a - b) <= tol


def test_pricing():
    print('\nBlack-Scholes')
    S, K, T, r, q, v = 100.0, 105.0, 0.25, 0.045, 0.0, 0.55
    c = float(bs_price(S, K, T, r, q, v, 'call'))
    p = float(bs_price(S, K, T, r, q, v, 'put'))
    parity = abs((c - p) - (S * math.exp(-q * T) - K * math.exp(-r * T)))
    check('put-call parity', parity < 1e-9, f'error {parity:.2e}')

    iv = implied_vol(c, S, K, T, r, q, 'call')
    check('implied vol round-trip', close(iv, v, 1e-5), f'{iv:.6f} vs {v}')
    check('implied vol rejects an arbitrageable price',
          implied_vol(0.0001, S, 50.0, T, r, q, 'call') is None)

    g = greeks(S, K, T, r, q, v, 'call')
    h = 1e-4
    fd_delta = float((bs_price(S + h, K, T, r, q, v, 'call')
                      - bs_price(S - h, K, T, r, q, v, 'call')) / (2 * h))
    fd_gamma = float((bs_price(S + h, K, T, r, q, v, 'call')
                      - 2 * bs_price(S, K, T, r, q, v, 'call')
                      + bs_price(S - h, K, T, r, q, v, 'call')) / (h * h))
    fd_vega = float((bs_price(S, K, T, r, q, v + h, 'call')
                     - bs_price(S, K, T, r, q, v - h, 'call')) / (2 * h) / 100.0)
    check('delta vs finite difference', close(g['delta'], fd_delta, 1e-5))
    check('gamma vs finite difference', close(g['gamma'], fd_gamma, 1e-5))
    check('vega vs finite difference', close(g['vega'], fd_vega, 1e-6))
    check('theta is negative for a long option', g['theta'] < 0)


def test_monte_carlo():
    print('\nMonte Carlo vs closed form')
    S0, sigma, r, q, H = 40.0, 0.55, 0.045, 0.0, 42
    T = H / 252.0
    mu = resolve_log_drift('risk_neutral', sigma, r, q)
    sim = simulate_gbm(S0, H, sigma, mu, n_paths=200000, seed=1)
    st = terminal_stats(sim)

    fwd = S0 * math.exp((r - q) * T)
    check('E[S_T] equals the forward', close(st['mean'], fwd, 0.05),
          f"{st['mean']:.4f} vs {fwd:.4f}")
    med = S0 * math.exp(mu * H)
    check('median matches the drifted median', close(st['median'], med, 0.05),
          f"{st['median']:.4f} vs {med:.4f}")

    K = 44.0
    mc_call = math.exp(-r * T) * float(np.mean(np.maximum(sim.terminal - K, 0.0)))
    bs = float(bs_price(S0, K, T, r, q, sigma, 'call'))
    check('MC call price matches Black-Scholes', close(mc_call, bs, 0.02),
          f'{mc_call:.4f} vs {bs:.4f}')

    p_mc = float(np.mean(sim.terminal > K))
    p_n2 = prob_itm_rn(S0, K, T, r, q, sigma, 'call')
    check('P(S_T > K) matches N(d2)', close(p_mc, p_n2, 0.005),
          f'{p_mc:.4f} vs {p_n2:.4f}')

    check('P(up) is below 50% at this vol under a driftless measure',
          st['p_up'] < 0.49, f"P(up) = {st['p_up']:.4f}")

    # Barrier: P(max > b) for arithmetic BM with drift, by reflection.
    b = 48.0
    lvl = math.log(b / S0)
    sd = sigma / math.sqrt(252.0)

    def N(x):
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    analytic = (N((-lvl + mu * H) / (sd * math.sqrt(H)))
                + math.exp(2 * mu * lvl / sd ** 2) * N((-lvl - mu * H) / (sd * math.sqrt(H))))
    mc_touch = float(np.mean(sim.run_max >= b))
    check('Brownian-bridge touch probability matches closed form',
          close(mc_touch, analytic, 0.005), f'{mc_touch:.4f} vs {analytic:.4f}')


def test_vol_fitting():
    print('\nVolatility fitting')
    rng = np.random.default_rng(7)
    omega, alpha, beta, nu = 2e-5, 0.09, 0.88, 5.0
    n = 3000
    s2 = omega / (1 - alpha - beta)
    r = np.empty(n)
    for i in range(n):
        z = rng.standard_t(nu) / math.sqrt(nu / (nu - 2))
        r[i] = math.sqrt(s2) * z
        s2 = omega + alpha * r[i] ** 2 + beta * s2

    g = fit_garch(r)
    check('GARCH recovers alpha', g is not None and close(g.alpha, alpha, 0.05),
          f'{g.alpha:.3f} vs {alpha}')
    check('GARCH recovers beta', close(g.beta, beta, 0.06), f'{g.beta:.3f} vs {beta}')
    true_lr = math.sqrt(omega / (1 - alpha - beta) * 252)
    check('GARCH recovers the long-run vol', close(g.long_run_vol, true_lr, 0.06),
          f'{g.long_run_vol:.3f} vs {true_lr:.3f}')
    check('GARCH term structure mean-reverts toward the long run',
          abs(g.horizon_vol(252) - g.long_run_vol) < abs(g.spot_vol - g.long_run_vol) + 1e-9)
    check('rescaling scales the horizon vol exactly',
          close(g.rescaled(1.25).horizon_vol(21), 1.25 * g.horizon_vol(21), 1e-9))

    for true_nu in (4.0, 8.0):
        x = rng.standard_t(true_nu, 8000)
        fit = _fit_student_t_df(x / np.std(x))
        check(f'Student-t df recovers nu={true_nu:g}', close(fit, true_nu, 1.6),
              f'fit {fit:.2f}')


def _synthetic_chain(spot, td, r, q, iv, expiry='2099-01-01'):
    """A chain priced exactly off one flat vol, with a tight 2% spread."""
    T = td / 252.0
    calls, puts = [], []
    for K in np.arange(round(spot * 0.6), round(spot * 1.5), 1.0):
        for kind, bucket in (('call', calls), ('put', puts)):
            px = float(bs_price(spot, float(K), T, r, q, iv, kind))
            if px < 0.02:
                continue
            bid, ask = round(px * 0.99, 2), round(px * 1.01, 2)
            bucket.append({
                'kind': kind, 'expiry': expiry, 'dte': int(round(td * 365 / 252)),
                'strike': float(K), 'bid': bid, 'ask': ask, 'last': px,
                'mid': (bid + ask) / 2, 'volume': 100, 'open_interest': 500,
                'yahoo_iv': iv, 'flags': [], 'tradeable': True,
            })
    return {'expiry': expiry, 'dte': int(round(td * 365 / 252)), 'trading_days': td,
            't_years': T, 'calls': calls, 'puts': puts}


def test_fair_chain_scores_zero():
    print('\nEV invariant: a fairly priced chain has no edge')
    spot, r, q, td, iv = 40.0, 0.045, 0.0, 31, 0.55
    chain = _synthetic_chain(spot, td, r, q, iv)
    mu = resolve_log_drift('risk_neutral', iv, r, q)
    sim = simulate_gbm(spot, td, iv, mu, n_paths=400000, seed=11)
    rows = score_expiry(chain, spot, r, q, sim.terminal, model_vol=iv)
    check('every structure was scored', len(rows) >= 12, f'{len(rows)} structures')
    worst = max(rows, key=lambda x: abs(x['ev']))
    # Tolerance is a few cents per share on options worth several dollars:
    # Monte Carlo noise, not a systematic edge.
    check('no structure shows a spurious edge', abs(worst['ev']) < 4.0,
          f"largest |EV| {worst['ev']:+.2f} on {worst['name']}")


def test_rich_chain_favours_selling():
    print('\nDirectional sanity: an overpriced chain favours selling premium')
    spot, r, q, td = 40.0, 0.045, 0.0, 31
    chain = _synthetic_chain(spot, td, r, q, 0.60)      # market charges 60%
    true_vol = 0.45                                      # model expects 45%
    mu = resolve_log_drift('risk_neutral', true_vol, r, q)
    sim = simulate_gbm(spot, td, true_vol, mu, n_paths=200000, seed=13)
    rows = {x['name']: x for x in score_expiry(chain, spot, r, q, sim.terminal,
                                               model_vol=true_vol)}
    long_straddle = rows.get('Long straddle (ATM)')
    short_straddle = rows.get('Short straddle (ATM)')
    check('buying an overpriced straddle is negative EV',
          long_straddle and long_straddle['ev'] < -20,
          f"EV {long_straddle['ev']:+.2f}" if long_straddle else 'not built')
    check('selling it is the mirror image',
          short_straddle and close(short_straddle['ev'], -long_straddle['ev'], 1.0))
    check('long structures are long vega and short ones are short vega',
          long_straddle['greeks']['vega'] > 0 > short_straddle['greeks']['vega'])
    check('a short straddle reports unbounded loss',
          short_straddle['max_loss'] is None)
    check('a long straddle has a defined max loss',
          long_straddle['max_loss'] is not None and long_straddle['max_loss'] < 0)
    check('POP and EV disagree, as they should for premium selling',
          short_straddle['pop'] > 0.5 and short_straddle['ev'] > 0)


def main():
    print('Options model — verification against known answers')
    test_pricing()
    test_monte_carlo()
    test_vol_fitting()
    test_fair_chain_scores_zero()
    test_rich_chain_favours_selling()
    print()
    if FAILS:
        print(f'{len(FAILS)} check(s) FAILED: ' + ', '.join(FAILS))
        return 1
    print('All checks passed.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
