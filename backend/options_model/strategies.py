"""Option structures scored against the simulated distribution.

The chain gives the market's risk-neutral prices. The simulation gives our
real-world distribution. Every number here is the difference between the two:

* **EV** — mean P&L of the structure across the simulated terminal prices, at
  the actual quoted entry price. Positive EV means *our distribution* thinks
  the market is charging the wrong price. It is only as good as the
  distribution, which is why the calibration backtest sits next to it.
* **POP** — probability of finishing profitable. Deliberately shown beside EV,
  because they routinely disagree: a short strangle wins 80% of the time and
  can still be a negative-EV trade, and a long strangle is the mirror image.
* **Fill sensitivity** — every structure is evaluated at the mid *and* at the
  marketable price (buy the ask, sell the bid). On a mid-cap single name the
  spread is usually larger than the edge; if a trade only works at the mid it
  does not work.

Sign convention: ``qty`` is +1 long / -1 short per leg, prices are per share,
and P&L is reported per contract (x100) to match how the trade is placed.
"""

import math

import numpy as np

from .pricing import bs_price, greeks, implied_vol, prob_itm_rn

CONTRACT_MULT = 100

# Short-option margin proxy (Reg-T style): 20% of notional less out-of-the-money
# amount, floored at 10% of notional, plus premium. Brokers differ; this is a
# reasonable planning number for sizing, not a margin quote.
MARGIN_PCT = 0.20
MARGIN_FLOOR_PCT = 0.10


# ── Payoff machinery ──────────────────────────────────────────────────────

def _intrinsic(kind, strike, prices):
    if kind == 'call':
        return np.maximum(prices - strike, 0.0)
    if kind == 'put':
        return np.maximum(strike - prices, 0.0)
    return prices                      # stock leg


def leg_entry_price(leg, fill='mid'):
    """Per-share entry price under the chosen fill assumption."""
    if leg['kind'] == 'stock':
        return float(leg.get('price') or 0.0)
    bid, ask, mid = leg.get('bid') or 0.0, leg.get('ask') or 0.0, leg.get('mid')
    if fill == 'marketable' and bid > 0 and ask > 0:
        return ask if leg['qty'] > 0 else bid
    if mid:
        return float(mid)
    return float(leg.get('last') or 0.0)


def net_cost(legs, fill='mid'):
    """Net debit (>0 you pay) / credit (<0 you receive), per share."""
    return float(sum(l['qty'] * leg_entry_price(l, fill) for l in legs))


def gross_payoff(legs, prices):
    """Value of the structure at expiry, per share, before the entry cost."""
    prices = np.asarray(prices, dtype=float)
    total = np.zeros_like(prices)
    for l in legs:
        total += l['qty'] * _intrinsic(l['kind'], l.get('strike', 0.0), prices)
    return total


def payoff_at(legs, prices, fill='mid'):
    """P&L per share at expiry across a vector of terminal prices."""
    return gross_payoff(legs, prices) - net_cost(legs, fill)


def _breakevens(legs, spot, fill='mid'):
    """Sign changes of the payoff on a dense grid — works for any structure."""
    grid = np.linspace(max(spot * 0.05, 0.01), spot * 3.0, 4000)
    pnl = payoff_at(legs, grid, fill)
    sign = np.sign(pnl)
    idx = np.where(np.diff(sign) != 0)[0]
    outs = []
    for i in idx:
        x0, x1, y0, y1 = grid[i], grid[i + 1], pnl[i], pnl[i + 1]
        if y1 != y0:
            outs.append(float(x0 - y0 * (x1 - x0) / (y1 - y0)))
    return outs


def _extremes(legs, spot, fill='mid'):
    """Max profit / max loss at expiry, and whether either is unbounded.

    Unboundedness is decided from the net upside exposure rather than by
    probing a large price: a naked short call has no worst case to report, and
    quoting the payoff at some arbitrary "high enough" price would dress an
    unlimited loss up as a number.
    """
    net_upside = sum(l['qty'] for l in legs if l['kind'] in ('call', 'stock'))
    up_unbounded = net_upside > 0
    up_unlimited_loss = net_upside < 0
    body = payoff_at(legs, np.linspace(0.01, spot * 3.0, 3000), fill)
    return {
        'max_profit': None if up_unbounded else float(np.max(body)),
        'max_loss': None if up_unlimited_loss else float(np.min(body)),
        'unbounded_profit': bool(up_unbounded),
        'unbounded_loss': bool(up_unlimited_loss),
    }


def capital_at_risk(legs, spot, extremes, fill='mid'):
    """Money that has to be on the table, per share.

    Defined-risk structures use the actual max loss. Naked short options use
    the Reg-T margin proxy, since "max loss" is the whole underlying and would
    make every ratio meaningless.
    """
    ml = extremes.get('max_loss')
    if ml is not None and ml < 0:
        return abs(ml)
    short_notional = sum(abs(l['qty']) * spot for l in legs
                         if l['qty'] < 0 and l['kind'] in ('call', 'put'))
    if short_notional:
        credit = max(-net_cost(legs, fill), 0.0)
        return max(MARGIN_PCT * short_notional, MARGIN_FLOOR_PCT * short_notional) + credit
    return max(abs(net_cost(legs, fill)), 0.01)


def _kelly_fraction(pnl_per_share, capital):
    """Growth-optimal fraction of bankroll, solved on the simulated P&L.

    Not the coin-flip Kelly formula: the expected-log-growth objective is
    maximised numerically over the actual simulated distribution, which is the
    only version that respects fat tails. Reported as a *ceiling* — full Kelly
    on an options book is far too aggressive in practice, and the number is
    only as trustworthy as the distribution behind it.
    """
    if capital <= 0:
        return 0.0
    r = np.asarray(pnl_per_share, dtype=float) / capital
    if float(np.mean(r)) <= 0:
        return 0.0
    worst = float(np.min(r))
    f_max = 0.999 if worst >= -1.0 else min(0.999, 0.999 / abs(worst))
    grid = np.linspace(0.001, f_max, 200)
    best_f, best_g = 0.0, 0.0
    for f in grid:
        v = 1.0 + f * r
        if np.any(v <= 0):
            continue
        g = float(np.mean(np.log(v)))
        if g > best_g:
            best_g, best_f = g, float(f)
    return best_f


def evaluate(name, kind_label, legs, terminal, spot, r, q, dte, t_years,
             model_vol=None, stress_terminals=None):
    """Full scorecard for one structure against the simulated terminal prices.

    ``t_years`` is the pricing year fraction (trading days / 252) and must be
    the same span the simulation was run over; ``dte`` is calendar days and is
    used only for display and per-calendar-day theta.

    ``stress_terminals`` maps a label to an alternative terminal-price array
    (the same structure simulated at a higher and lower vol). The EV under
    those is the number that matters: the t-statistic only measures Monte
    Carlo noise, which is tiny and not the risk you are running. Being wrong
    about the vol forecast is the risk you are running.
    """
    T = max(float(t_years), 1e-6)
    ext_mid = _extremes(legs, spot, 'mid')
    gross = gross_payoff(legs, terminal)
    pnl_mid = gross - net_cost(legs, 'mid')
    pnl_mkt = gross - net_cost(legs, 'marketable')

    cap = capital_at_risk(legs, spot, ext_mid, 'mid')
    # EV is a *present value*: the expiry payoff is discounted back before the
    # entry cost is subtracted. Without this, every long option shows a small
    # positive "edge" that is only the carry on the premium, and a covered call
    # shows the whole risk-free drift on the stock leg as alpha. Discounted,
    # a structure priced at the simulated distribution scores exactly zero,
    # so a non-zero EV means a genuine disagreement with the market.
    disc = math.exp(-r * T)
    ev_mid = float(disc * np.mean(gross)) - net_cost(legs, 'mid')
    ev_mkt = float(disc * np.mean(gross)) - net_cost(legs, 'marketable')
    se = float(disc * np.std(gross) / math.sqrt(max(terminal.size, 1)))

    # Net greeks at entry, per share of underlying.
    g_tot = {'delta': 0.0, 'gamma': 0.0, 'vega': 0.0, 'theta': 0.0}
    net_vega_iv = 0.0
    for l in legs:
        if l['kind'] == 'stock':
            g_tot['delta'] += l['qty']
            continue
        iv = l.get('iv') or model_vol or 0.5
        g = greeks(spot, l['strike'], T, r, q, iv, l['kind'])
        for k in g_tot:
            g_tot[k] += l['qty'] * g[k]
        net_vega_iv += l['qty'] * g['vega'] * iv

    # Vol edge: the position's vega times (our forecast vol - the vol it was
    # priced at). Positive = the structure is long the cheap side of vol.
    avg_iv = _weighted_iv(legs)
    vol_edge = None
    if model_vol and avg_iv:
        vol_edge = float(g_tot['vega'] * (model_vol - avg_iv) * 100.0)

    losses = pnl_mid[pnl_mid < 0]
    out = {
        'name': name,
        'type': kind_label,
        'dte': int(dte),
        'expiry': legs[0].get('expiry'),
        'legs': [{
            'kind': l['kind'], 'qty': l['qty'], 'strike': l.get('strike'),
            'price': leg_entry_price(l, 'mid'), 'iv': l.get('iv'),
            'bid': l.get('bid'), 'ask': l.get('ask'),
            'open_interest': l.get('open_interest'),
        } for l in legs],
        'net_debit': net_cost(legs, 'mid') * CONTRACT_MULT,
        'net_debit_marketable': net_cost(legs, 'marketable') * CONTRACT_MULT,
        'max_profit': None if ext_mid['max_profit'] is None else ext_mid['max_profit'] * CONTRACT_MULT,
        'max_loss': None if ext_mid['max_loss'] is None else ext_mid['max_loss'] * CONTRACT_MULT,
        'unbounded_profit': ext_mid['unbounded_profit'],
        'unbounded_loss': ext_mid['unbounded_loss'],
        'capital': cap * CONTRACT_MULT,
        'ev': ev_mid * CONTRACT_MULT,
        'ev_marketable': ev_mkt * CONTRACT_MULT,
        'ev_se': se * CONTRACT_MULT,
        # How many standard errors the simulated edge is from zero. Below ~2
        # the "edge" is simulation noise, not a trade.
        'ev_tstat': (ev_mid / se) if se > 0 else None,
        'ev_on_capital': (ev_mid / cap) if cap > 0 else None,
        # POP, max loss, breakevens and CVaR are expiry-value concepts and stay
        # undiscounted -- that is the P&L that actually shows in the account.
        'pop': float(np.mean(pnl_mid > 0)),
        'pop_marketable': float(np.mean(pnl_mkt > 0)),
        'p_max_loss': float(np.mean(pnl_mid <= (ext_mid['max_loss'] or -1e18) * 0.999)),
        'cvar05': float(np.mean(np.sort(pnl_mid)[:max(int(0.05 * terminal.size), 1)])) * CONTRACT_MULT,
        'expected_loss_given_loss': float(np.mean(losses)) * CONTRACT_MULT if losses.size else 0.0,
        'breakevens': _breakevens(legs, spot, 'mid'),
        'greeks': {k: float(v) for k, v in g_tot.items()},
        'avg_iv': avg_iv,
        'vol_edge': vol_edge,
        'kelly': _kelly_fraction(pnl_mid, cap),
        'worst_fill_flag': bool(ev_mid > 0 and ev_mkt <= 0),
        'liquidity': _liquidity_flag(legs),
    }
    if stress_terminals:
        stress = {}
        for label, term in stress_terminals.items():
            g = gross_payoff(legs, term)
            stress[label] = (float(disc * np.mean(g)) - net_cost(legs, 'mid')) * CONTRACT_MULT
        out['ev_stress'] = stress
        out['ev_worst_stress'] = min(stress.values()) if stress else None
        # Survives being wrong about vol by the model's own historical
        # forecast error, and survives paying the spread.
        out['robust'] = bool(out['ev_worst_stress'] is not None
                             and out['ev_worst_stress'] > 0
                             and ev_mkt > 0)
    return out


def _weighted_iv(legs):
    """Vega-weighted average IV of the option legs (sign-aware)."""
    num = den = 0.0
    for l in legs:
        if l['kind'] == 'stock' or not l.get('iv'):
            continue
        w = abs(l['qty'])
        num += w * l['iv']
        den += w
    return (num / den) if den else None


def _liquidity_flag(legs):
    """Worst quote quality across the legs — the one you will actually pay."""
    worst = 'ok'
    for l in legs:
        if l['kind'] == 'stock':
            continue
        flags = l.get('flags') or []
        if 'no_bid' in flags or 'crossed' in flags:
            return 'untradeable'
        if 'wide' in flags:
            worst = 'wide'
        elif 'no_oi' in flags and worst == 'ok':
            worst = 'thin'
    return worst


# ── Strike selection ──────────────────────────────────────────────────────

def attach_ivs(legs_pool, spot, r, q, t_years):
    """Re-imply vol from the mid quote for every row in one expiry.

    Yahoo's own ``impliedVolatility`` is computed off the last trade, which on
    a thin chain can be days old; re-implying from the current mid keeps the
    surface internally consistent with the prices the trade would actually pay.
    """
    T = max(float(t_years), 1e-6)
    for leg in legs_pool:
        iv = None
        if leg.get('mid') and leg['mid'] > 0:
            iv = implied_vol(leg['mid'], spot, leg['strike'], T, r, q, leg['kind'])
        if iv is None:
            yv = leg.get('yahoo_iv')
            iv = float(yv) if yv and 0.01 < float(yv) < 6.0 else None
        leg['iv'] = iv
        if iv:
            g = greeks(spot, leg['strike'], T, r, q, iv, leg['kind'])
            leg['delta'] = g['delta']
            leg['model_delta'] = abs(g['delta'])
        else:
            leg['delta'] = None
    return legs_pool


def _pick_by_delta(pool, target_abs_delta, kind):
    """Closest tradeable strike to a target absolute delta."""
    cands = [l for l in pool
             if l['kind'] == kind and l.get('delta') is not None
             and l.get('tradeable', True) and (l.get('mid') or 0) > 0.01]
    if not cands:
        return None
    return min(cands, key=lambda l: abs(abs(l['delta']) - target_abs_delta))


def _pick_atm(pool, spot, kind):
    cands = [l for l in pool if l['kind'] == kind
             and l.get('tradeable', True) and (l.get('mid') or 0) > 0.01]
    if not cands:
        return None
    return min(cands, key=lambda l: abs(l['strike'] - spot))


def _leg(row, qty):
    out = dict(row)
    out['qty'] = qty
    return out


def build_candidates(expiry_block, spot, r, q):
    """Assemble the standard structure menu for one expiry.

    Deliberately a fixed menu rather than an exhaustive strike search: with
    ~40 strikes and 6 expiries an unconstrained search finds thousands of
    "edges" that are pure quote noise. Anchoring on delta (25/30/50) keeps the
    comparison honest across expiries and against how these are actually
    quoted.
    """
    pool = attach_ivs(list(expiry_block['calls']) + list(expiry_block['puts']),
                      spot, r, q, expiry_block['t_years'])
    calls = [l for l in pool if l['kind'] == 'call']
    puts = [l for l in pool if l['kind'] == 'put']
    if not calls or not puts:
        return []

    atm_c, atm_p = _pick_atm(calls, spot, 'call'), _pick_atm(puts, spot, 'put')
    c25, p25 = _pick_by_delta(calls, 0.25, 'call'), _pick_by_delta(puts, 0.25, 'put')
    c30, p30 = _pick_by_delta(calls, 0.30, 'call'), _pick_by_delta(puts, 0.30, 'put')
    c10, p10 = _pick_by_delta(calls, 0.10, 'call'), _pick_by_delta(puts, 0.10, 'put')

    out = []

    def add(name, label, legs):
        if all(legs) and len({(l['kind'], l['strike']) for l in legs}) == len(legs):
            out.append((name, label, legs))

    if atm_c:
        add('Long ATM call', 'directional-long', [_leg(atm_c, 1)])
    if atm_p:
        add('Long ATM put', 'directional-short', [_leg(atm_p, 1)])
    if c25:
        add(f'Long {c25["strike"]:g} call (25Δ)', 'directional-long', [_leg(c25, 1)])
    if p25:
        add(f'Long {p25["strike"]:g} put (25Δ)', 'directional-short', [_leg(p25, 1)])
    if atm_c and c25:
        add('Bull call spread (ATM/25Δ)', 'directional-long', [_leg(atm_c, 1), _leg(c25, -1)])
    if atm_p and p25:
        add('Bear put spread (ATM/25Δ)', 'directional-short', [_leg(atm_p, 1), _leg(p25, -1)])
    if p30 and p10:
        add('Bull put credit spread (30Δ/10Δ)', 'premium-sell', [_leg(p30, -1), _leg(p10, 1)])
    if c30 and c10:
        add('Bear call credit spread (30Δ/10Δ)', 'premium-sell', [_leg(c30, -1), _leg(c10, 1)])
    if atm_c and atm_p:
        add('Long straddle (ATM)', 'long-vol', [_leg(atm_c, 1), _leg(atm_p, 1)])
        add('Short straddle (ATM)', 'premium-sell', [_leg(atm_c, -1), _leg(atm_p, -1)])
    if c25 and p25:
        add('Long strangle (25Δ)', 'long-vol', [_leg(c25, 1), _leg(p25, 1)])
        add('Short strangle (25Δ)', 'premium-sell', [_leg(c25, -1), _leg(p25, -1)])
    if c30 and c10 and p30 and p10:
        add('Iron condor (30Δ/10Δ)', 'premium-sell',
            [_leg(p30, -1), _leg(p10, 1), _leg(c30, -1), _leg(c10, 1)])
    if p30:
        add(f'Cash-secured put {p30["strike"]:g} (30Δ)', 'premium-sell', [_leg(p30, -1)])
    if c30:
        add(f'Covered call {c30["strike"]:g} (30Δ)', 'premium-sell',
            [{'kind': 'stock', 'qty': 1, 'price': spot, 'strike': 0.0,
              'expiry': expiry_block['expiry']}, _leg(c30, -1)])
    return out


def score_expiry(expiry_block, spot, r, q, terminal, model_vol=None,
                 stress_terminals=None):
    """Evaluate every candidate structure for one expiry."""
    rows = []
    for name, label, legs in build_candidates(expiry_block, spot, r, q):
        try:
            rows.append(evaluate(name, label, legs, terminal, spot, r, q,
                                 expiry_block['dte'], expiry_block['t_years'],
                                 model_vol, stress_terminals))
        except Exception:
            continue
    return rows


# Strikes reported per side, per expiry, centred on the money. The far wings
# of a single-name chain are quote noise and would dominate the table by count.
MAX_EDGE_STRIKES = 18


def strike_edge_table(expiry_block, spot, r, q, terminal, model_vol=None):
    """Per-strike comparison of model vs market probability.

    ``N(d2)`` is the market's risk-neutral probability of finishing ITM; the
    simulated column is our real-world probability of the same event. The gap
    is the raw directional edge before costs — and because the two are under
    different measures, a persistent gap on *both* wings is the variance risk
    premium, not a signal.
    """
    dte = expiry_block['dte']
    T = max(float(expiry_block['t_years']), 1e-6)
    pool = attach_ivs(list(expiry_block['calls']) + list(expiry_block['puts']),
                      spot, r, q, T)
    n = terminal.size
    rows = []
    for leg in pool:
        if not leg.get('iv') or not leg.get('mid'):
            continue
        K = leg['strike']
        if not (0.6 * spot <= K <= 1.6 * spot):
            continue
        if leg['kind'] == 'call':
            p_model = float(np.sum(terminal > K) / n)
        else:
            p_model = float(np.sum(terminal < K) / n)
        p_mkt = prob_itm_rn(spot, K, T, r, q, leg['iv'], leg['kind'])
        model_price = None
        if model_vol:
            model_price = float(bs_price(spot, K, T, r, q, model_vol, leg['kind']))
        rows.append({
            'kind': leg['kind'], 'strike': K, 'dte': dte,
            'moneyness': K / spot if spot else None,
            'iv': leg['iv'], 'delta': leg.get('delta'),
            'mid': leg['mid'], 'bid': leg.get('bid'), 'ask': leg.get('ask'),
            'open_interest': leg.get('open_interest'),
            'p_itm_market': p_mkt,
            'p_itm_model': p_model,
            'prob_edge': p_model - p_mkt,
            'model_price': model_price,
            'price_edge': (model_price - leg['mid']) if model_price is not None else None,
            'flags': leg.get('flags') or [],
        })
    kept = []
    for kind in ('call', 'put'):
        side = [x for x in rows if x['kind'] == kind]
        side.sort(key=lambda x: abs(x['strike'] - spot))
        kept.extend(sorted(side[:MAX_EDGE_STRIKES], key=lambda x: x['strike']))
    kept.sort(key=lambda x: (x['kind'], x['strike']))
    return kept


def rank_strategies(rows, top=12):
    """Rank by EV per dollar of capital among trades that survive scrutiny.

    Three filters, in order of how often they bite: the quotes have to be
    tradeable, the edge has to survive paying the spread and being wrong about
    vol by the model's own historical forecast error, and only then does the
    Monte Carlo t-statistic matter. A trade that ranks top on EV and fails the
    vol stress is not a trade, it is a leveraged opinion about the vol
    forecast.
    """
    def sort_key(x):
        return -(x.get('ev_on_capital') or -9e9)

    def is_robust(x):
        if x['liquidity'] == 'untradeable':
            return False
        if (x.get('ev_on_capital') or 0) <= 0 or (x.get('ev_tstat') or 0) <= 2.0:
            return False
        # When no stress was run, fall back to the spread test alone.
        if 'robust' in x:
            return bool(x['robust'])
        return x.get('ev_marketable', 0) > 0

    robust = [x for x in rows if is_robust(x)]
    robust.sort(key=sort_key)
    return {
        'ranked': robust[:top],
        'all': sorted(rows, key=sort_key),
        'n_credible': len(robust),
        'n_total': len(rows),
    }
