"""Orchestration: one payload with the vol picture, the simulated
distribution, the direction probabilities and the ranked option structures.

Flow for a request:

    prices ──> volatility estimates ──> GARCH fit ──> forward vol per expiry
                                             │
                                             ├──> Monte Carlo (4 engines)
                                             │         │
    option chain ──> re-implied vols ────────┤         ├──> direction table
                                             │         ├──> strike edge table
                                             └─────────┴──> ranked structures

The walk-forward backtest is computed on a separate, longer-lived cache: it is
the slowest piece and its answer barely moves intraday.
"""

import logging
import math
import threading
import time
from datetime import datetime, timezone

import numpy as np

from . import data as market
from .backtest import summarize as backtest_summarize
from .random_walk import (ENGINES, ENGINE_LABELS, fan_chart, histogram,
                          resolve_log_drift, simulate, terminal_stats)
from .strategies import (attach_ivs, rank_strategies, score_expiry,
                         strike_edge_table)
from .vol import (TRADING_DAYS, ewma_vol, fit_garch, garman_klass_vol,
                  log_returns, parkinson_vol, realized_vol,
                  realized_vol_term_structure, return_moments,
                  rogers_satchell_vol, trading_days_until, vol_cone,
                  vol_percentile, yang_zhang_vol)

logger = logging.getLogger(__name__)

DEFAULT_HORIZON = 21            # trading days ~ one month
DEFAULT_ENGINE = 'garch_t'
DEFAULT_DRIFT = 'risk_neutral'
DEFAULT_PATHS = 20000
MAX_PATHS = 100000

# Horizons the direction table always reports, in trading days.
DIRECTION_HORIZONS = (5, 10, 21, 42, 63)

# Expiries scored for structures. Each one costs a simulation plus a full
# strategy sweep, so the near part of the curve is where the work goes.
MAX_SCORED_EXPIRIES = 4

PAYLOAD_TTL = 600               # 10 minutes
BACKTEST_TTL = 21600            # 6 hours

_payload_lock = threading.RLock()
_payload_store = {}
_backtest_store = {}


# ── Cache ─────────────────────────────────────────────────────────────────

def _cached(store, key, ttl, builder):
    with _payload_lock:
        hit = store.get(key)
        if hit and (time.time() - hit[0]) < ttl:
            return hit[1], True
    value = builder()
    with _payload_lock:
        if value is not None:
            store[key] = (time.time(), value)
    return value, False


def clear_cache(ticker=None):
    with _payload_lock:
        if ticker is None:
            _payload_store.clear()
            _backtest_store.clear()
        else:
            up = ticker.upper()
            for store in (_payload_store, _backtest_store):
                for k in [k for k in store if k[0] == up]:
                    store.pop(k, None)
    market.clear_cache(ticker)


# ── Implied-vol surface helpers ───────────────────────────────────────────

def _otm_iv_curve(block, spot):
    """IV by strike using out-of-the-money options only.

    ITM quotes on a thin chain are mostly intrinsic value with a wide spread,
    so their implied vols are noise. Every desk builds the smile from the OTM
    wing of each side; doing anything else makes the skew look like whatever
    the widest ITM quote happened to be.
    """
    rows = []
    for leg in block['calls']:
        if leg.get('iv') and leg['strike'] >= spot:
            rows.append(leg)
    for leg in block['puts']:
        if leg.get('iv') and leg['strike'] < spot:
            rows.append(leg)
    rows.sort(key=lambda x: x['strike'])
    return rows


def _interp_atm_iv(curve, spot):
    """IV at the money, linearly interpolated in strike."""
    if not curve:
        return None
    ks = [r['strike'] for r in curve]
    vs = [r['iv'] for r in curve]
    if spot <= ks[0]:
        return vs[0]
    if spot >= ks[-1]:
        return vs[-1]
    return float(np.interp(spot, ks, vs))


def _iv_at_delta(block, target, kind):
    cands = [l for l in block[kind + 's']
             if l.get('iv') and l.get('delta') is not None
             and l.get('tradeable', True)]
    if not cands:
        return None
    best = min(cands, key=lambda l: abs(abs(l['delta']) - target))
    return best['iv']


def _expiry_vol_block(block, spot, r, q, garch, ewma, rv21):
    """Per-expiry: market vol vs model vol, skew, and the premium between."""
    attach_ivs(list(block['calls']) + list(block['puts']), spot, r, q,
               block['t_years'])
    curve = _otm_iv_curve(block, spot)
    atm_iv = _interp_atm_iv(curve, spot)
    iv_p25 = _iv_at_delta(block, 0.25, 'put')
    iv_c25 = _iv_at_delta(block, 0.25, 'call')

    td = block['trading_days']
    model_vol = (garch.horizon_vol(td) if garch else None) or ewma or rv21

    vrp = (atm_iv - model_vol) if (atm_iv and model_vol) else None
    return {
        'expiry': block['expiry'],
        'dte': block['dte'],
        'trading_days': td,
        't_years': block['t_years'],
        'atm_iv': atm_iv,
        'iv_25d_put': iv_p25,
        'iv_25d_call': iv_c25,
        # Skew as the 25-delta risk reversal: put IV over call IV. Persistently
        # positive on equities (crash insurance costs more); a spike is the
        # market paying up for downside.
        'skew_25d': (iv_p25 - iv_c25) if (iv_p25 and iv_c25) else None,
        'model_vol': model_vol,
        'vrp': vrp,
        'vrp_ratio': (atm_iv / model_vol) if (atm_iv and model_vol) else None,
        # The straddle the market is charging vs the move the model expects.
        'implied_move_pct': (atm_iv * math.sqrt(block['t_years']) * 100.0) if atm_iv else None,
        'model_move_pct': (model_vol * math.sqrt(block['t_years']) * 100.0) if model_vol else None,
        'smile': [{'strike': x['strike'], 'iv': x['iv'], 'kind': x['kind'],
                   'delta': x.get('delta'), 'moneyness': x['strike'] / spot,
                   'open_interest': x.get('open_interest')} for x in curve],
        'n_quotes': len(curve),
    }


# ── Payload ───────────────────────────────────────────────────────────────

def _vol_stress_size(bt):
    """How wrong the vol forecast has actually been, in vol points.

    Taken from the walk-forward RMSE of the best forecaster rather than a
    round number, so the stress test is calibrated to this stock's own
    behaviour. Bounded either side: a suspiciously small error usually means
    too few windows, and a huge one would reject every trade.
    """
    rmse = None
    if bt and bt.get('available'):
        best = bt.get('best_vol_model')
        stats = (bt.get('volatility') or {}).get(best or '', {})
        rmse = stats.get('rmse')
    if not rmse or not math.isfinite(rmse):
        rmse = 0.08
    return float(min(max(rmse, 0.04), 0.20))


def _direction_table(spot, horizons, garch, sigma_fallback, nu, returns,
                     drift_mode, r, q, custom_drift, n_paths, engine, seed=7):
    """P(up) and the expected move at each horizon, plus the reason it is not
    50%: the variance drag on the median."""
    rows = []
    for h in horizons:
        sigma = (garch.horizon_vol(h) if garch else None) or sigma_fallback
        if not sigma:
            continue
        mu = resolve_log_drift(drift_mode, sigma, r, q,
                               hist_mu_daily=float(np.mean(returns)) if returns.size else 0.0,
                               custom_annual=custom_drift)
        sim = simulate(engine, spot, h, sigma_ann=sigma, log_drift_daily=mu,
                       garch=garch, returns=returns, nu=nu,
                       n_paths=max(n_paths // 2, 4000), seed=seed + h, band_qs=())
        if sim is None:
            continue
        st = terminal_stats(sim)
        T = h / TRADING_DAYS
        rows.append({
            'days': h,
            'calendar_days': int(round(h * 365 / TRADING_DAYS)),
            'vol': sigma,
            'p_up': st['p_up'],
            'p_down': st['p_down'],
            'median_return': st['median_return'],
            'mean_return': st['mean_return'],
            'expected_abs_move': st['expected_abs_move'],
            'one_sd_move': sigma * math.sqrt(T),
            'band68': st['band68'],
            'p90_move_up': st['quantiles']['p90'] / spot - 1.0,
            'p10_move_down': st['quantiles']['p10'] / spot - 1.0,
            # The half-variance drag alone, in log terms: why a driftless
            # high-vol stock is below its start price more often than not.
            'variance_drag': -0.5 * sigma * sigma * T,
        })
    return rows


def _narrative(ctx):
    """Plain-English read of the numbers the page is showing."""
    t = ctx['ticker']
    bits = []
    vol = ctx['vol']
    rv21 = vol['realized'].get('close_21')
    yz = vol['realized'].get('yang_zhang_21')
    pct = vol.get('percentile')
    g = vol.get('garch')

    if rv21:
        line = f"{t} has realized {rv21:.0%} annualized vol over the last month"
        if yz:
            line += f" ({yz:.0%} on the gap-aware Yang-Zhang estimator)"
        if pct is not None:
            line += f", the {pct:.0f}th percentile of its own two-year range"
        bits.append(line + '.')

    if g and g.get('half_life_days'):
        bits.append(
            f"The fitted GARCH(1,1) puts long-run vol at {g['long_run_vol']:.0%} with a "
            f"{g['half_life_days']:.0f}-day half-life on shocks, so today's "
            f"{g['spot_vol']:.0%} spot vol decays toward that over the life of a "
            f"typical monthly option.")

    near = (ctx.get('chain_vol') or [None])[0]
    if near and near.get('atm_iv') and near.get('model_vol'):
        gap = near['vrp']
        direction = 'above' if gap > 0 else 'below'
        bits.append(
            f"The {near['dte']}-day options are priced at {near['atm_iv']:.0%} implied, "
            f"{abs(gap) * 100:.1f} vol points {direction} the model's "
            f"{near['model_vol']:.0%} forecast — "
            + ("the market is paying you to sell that gap, if the forecast holds."
               if gap > 0.02 else
               "options look cheap against the forecast, which favours owning gamma."
               if gap < -0.02 else
               "close enough to fair that the vol trade is not the edge here."))

    primary = ctx.get('primary_stats')
    if primary:
        gap = (primary['mean_return'] - primary['median_return']) * 100.0
        bits.append(
            f"Over {primary['horizon_days']} trading days the simulation puts "
            f"P(up) at {primary['p_up']:.1%} with a 68% range of "
            f"${primary['band68'][0]:.2f}-${primary['band68'][1]:.2f}. "
            f"Anything away from 50% here is arithmetic, not a forecast: at this vol "
            f"the median outcome sits {gap:.1f} percentage points below the mean, "
            f"which is the single most useful thing a random walk tells an options "
            f"trader.")

    bt = ctx.get('backtest') or {}
    if bt.get('available') and bt.get('verdict'):
        bits.append(bt['verdict'])

    return ' '.join(bits)


def build_payload(ticker=market.DEFAULT_TICKER, horizon=DEFAULT_HORIZON,
                  engine=DEFAULT_ENGINE, drift_mode=DEFAULT_DRIFT,
                  n_paths=DEFAULT_PATHS, custom_drift=None,
                  with_backtest=True, with_chain=True):
    """Build the full model payload. Returns ``None`` if there is no history."""
    ticker = (ticker or market.DEFAULT_TICKER).upper().strip()
    horizon = max(1, min(int(horizon), 504))
    n_paths = max(2000, min(int(n_paths), MAX_PATHS))
    engine = engine if engine in ENGINES else DEFAULT_ENGINE

    bundle = market.get_market_bundle(ticker, with_chain=with_chain)
    if not bundle:
        return None

    hist = bundle['history']
    spot = bundle['spot']
    r = bundle['risk_free']
    q = float(bundle['meta'].get('dividend_yield') or 0.0)
    rets = log_returns(hist['adj_close'])

    # ── Volatility ───────────────────────────────────────────────────────
    garch = fit_garch(rets)
    rv = {f'close_{w}': realized_vol(rets, w) for w in (5, 10, 21, 63, 126, 252)
          if rets.size >= w}
    rv['ewma'] = ewma_vol(rets)
    rv['yang_zhang_21'] = yang_zhang_vol(hist['open'], hist['high'], hist['low'],
                                         hist['close'], 21)
    rv['parkinson_21'] = parkinson_vol(hist['high'], hist['low'], 21)
    rv['garman_klass_21'] = garman_klass_vol(hist['open'], hist['high'],
                                             hist['low'], hist['close'], 21)
    rv['rogers_satchell_21'] = rogers_satchell_vol(hist['open'], hist['high'],
                                                   hist['low'], hist['close'], 21)

    sigma_fallback = rv.get('close_21') or rv.get('ewma') or 0.5
    nu = garch.nu if garch else 5.0

    vol_block = {
        'realized': rv,
        'term_structure': realized_vol_term_structure(rets),
        'garch': garch.to_dict() if garch else None,
        'garch_term_structure': (garch.term_structure([5, 10, 21, 42, 63, 126, 252])
                                 if garch else []),
        'cone': vol_cone(rets),
        'percentile': vol_percentile(rets),
        'moments': return_moments(rets),
    }

    # ── Chain, annotated with the model's own time convention ────────────
    chain = []
    for block in bundle.get('chain') or []:
        td = trading_days_until(block['expiry'])
        if td <= 0:
            continue
        block = dict(block)
        block['trading_days'] = td
        block['t_years'] = td / TRADING_DAYS
        chain.append(block)

    chain_vol = [_expiry_vol_block(b, spot, r, q, garch, rv.get('ewma'),
                                   rv.get('close_21')) for b in chain]

    # ── Primary simulation: every engine, same horizon and drift ─────────
    hist_mu = float(np.mean(rets)) if rets.size else 0.0
    sigma_primary = (garch.horizon_vol(horizon) if garch else None) or sigma_fallback
    log_drift = resolve_log_drift(drift_mode, sigma_primary, r, q,
                                  hist_mu_daily=hist_mu, custom_annual=custom_drift)

    engine_stats, primary_sim = [], None
    for name in ENGINES:
        sim = simulate(name, spot, horizon, sigma_ann=sigma_primary,
                       log_drift_daily=log_drift, garch=garch, returns=rets,
                       nu=nu, n_paths=n_paths, seed=11,
                       band_qs=((5, 25, 50, 75, 95) if name == engine else ()))
        if sim is None:
            continue
        engine_stats.append(terminal_stats(sim))
        if name == engine:
            primary_sim = sim
    if primary_sim is None and engine_stats:
        primary_sim = simulate('gbm', spot, horizon, sigma_ann=sigma_primary,
                               log_drift_daily=log_drift, n_paths=n_paths, seed=11)
    if primary_sim is None:
        return None

    primary_stats = terminal_stats(primary_sim)

    # ── Out-of-sample validation (separate, longer cache) ────────────────
    # Computed before the structures because its measured vol-forecast error
    # sets how hard each structure gets stressed below.
    bt = None
    if with_backtest:
        bt, _ = _cached(_backtest_store, (ticker, horizon), BACKTEST_TTL,
                        lambda: backtest_summarize(rets, horizon=horizon))
    vol_error = _vol_stress_size(bt)

    # ── Structures, one simulation per scored expiry ─────────────────────
    strategy_rows, edge_rows = [], []
    for block, vblock in zip(chain[:MAX_SCORED_EXPIRIES],
                             chain_vol[:MAX_SCORED_EXPIRIES]):
        td = block['trading_days']
        sigma_h = vblock.get('model_vol') or sigma_primary
        mu_h = resolve_log_drift(drift_mode, sigma_h, r, q,
                                 hist_mu_daily=hist_mu, custom_annual=custom_drift)
        sim_h = simulate(engine, spot, td, sigma_ann=sigma_h, log_drift_daily=mu_h,
                         garch=garch, returns=rets, nu=nu, n_paths=n_paths,
                         seed=23 + td, band_qs=())
        if sim_h is None:
            continue

        # Re-run the same structure at the vol the model has historically been
        # wrong by, in both directions. Short-vol trades die on the high side,
        # long-vol trades on the low side; anything that survives both is the
        # only kind of edge worth putting on.
        stress = {}
        for label, bump in (('vol_down', -vol_error), ('vol_up', +vol_error)):
            s_bump = max(sigma_h + bump, 0.05)
            sim_b = simulate(engine, spot, td, sigma_ann=sigma_h,
                             log_drift_daily=resolve_log_drift(
                                 drift_mode, s_bump, r, q, hist_mu_daily=hist_mu,
                                 custom_annual=custom_drift),
                             garch=garch, returns=rets, nu=nu,
                             n_paths=max(n_paths // 2, 4000), seed=31 + td,
                             band_qs=(), vol_scale=s_bump / sigma_h)
            if sim_b is not None:
                stress[label] = sim_b.terminal

        strategy_rows.extend(score_expiry(block, spot, r, q, sim_h.terminal,
                                          model_vol=sigma_h,
                                          stress_terminals=stress or None))
        edge_rows.extend(strike_edge_table(block, spot, r, q, sim_h.terminal,
                                           model_vol=sigma_h))

    ranked = rank_strategies(strategy_rows)
    ranked['vol_stress_points'] = vol_error

    direction = _direction_table(spot, DIRECTION_HORIZONS, garch, sigma_fallback,
                                 nu, rets, drift_mode, r, q, custom_drift,
                                 n_paths, engine)

    payload = {
        'meta': {
            'ticker': ticker,
            'name': bundle['meta'].get('name') or ticker,
            'spot': spot,
            'prev_close': bundle['prev_close'],
            'change_pct': bundle['change_pct'],
            'as_of': bundle['as_of'],
            'risk_free': r,
            'dividend_yield': q,
            'earnings_date': bundle['meta'].get('earnings_date'),
            'market_cap': bundle['meta'].get('market_cap'),
            'beta': bundle['meta'].get('beta'),
            'sector': bundle['meta'].get('sector'),
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'source': 'Yahoo Finance (prices, option chain), 13-week T-bill for the discount rate',
            'history_days': len(hist['close']),
            'presets': market.PRESET_TICKERS,
        },
        'price_history': {
            'dates': hist['dates'][-120:],
            'close': hist['close'][-120:],
        },
        'vol': vol_block,
        'chain_vol': chain_vol,
        'simulation': {
            'settings': {
                'engine': engine,
                'engine_label': ENGINE_LABELS.get(engine, engine),
                'horizon_days': horizon,
                'horizon_calendar_days': int(round(horizon * 365 / TRADING_DAYS)),
                'n_paths': n_paths,
                'drift_mode': drift_mode,
                'custom_drift': custom_drift,
                'log_drift_daily': log_drift,
                'sigma_ann': sigma_primary,
                'nu': nu,
            },
            'engines': engine_stats,
            'primary': primary_stats,
            'fan': fan_chart(primary_sim),
            'histogram': histogram(primary_sim),
        },
        'direction': direction,
        'strategies': ranked,
        'strike_edges': edge_rows,
        'backtest': bt,
    }
    payload['narrative'] = _narrative({
        'ticker': ticker, 'vol': vol_block, 'chain_vol': chain_vol,
        'primary_stats': primary_stats, 'backtest': bt,
    })
    return payload


def get_model(ticker=market.DEFAULT_TICKER, horizon=DEFAULT_HORIZON,
              engine=DEFAULT_ENGINE, drift_mode=DEFAULT_DRIFT,
              n_paths=DEFAULT_PATHS, custom_drift=None, with_backtest=True):
    """Cached entry point used by the routes."""
    key = (ticker.upper().strip(), int(horizon), engine, drift_mode,
           int(n_paths), custom_drift, bool(with_backtest))

    def build():
        try:
            return build_payload(ticker, horizon, engine, drift_mode, n_paths,
                                 custom_drift, with_backtest)
        except Exception:
            logger.exception('options_model: payload build failed for %s', ticker)
            return None

    payload, cached = _cached(_payload_store, key, PAYLOAD_TTL, build)
    if payload is not None:
        payload = dict(payload)
        payload['meta'] = dict(payload['meta'], cached=cached)
    return payload
