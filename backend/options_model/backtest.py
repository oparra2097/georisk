"""Walk-forward validation — does any of this actually work?

Three questions, asked strictly out of sample (every estimate at date *t* uses
only data up to *t*):

1. **Can the model call direction?** Brier score of P(up over the next h days)
   against the realized outcome, versus a flat 50% coin flip. The honest prior
   is that it cannot, and the test is here to show that rather than hide it —
   if the model's directional Brier does not beat 0.25, the trades that follow
   must be vol trades, not bets on direction.
2. **Can it forecast volatility?** Mincer-Zarnowitz regression of realized
   forward vol on forecast vol, for GARCH against EWMA and trailing realized.
   This is where the skill usually is, and it is what makes a premium-selling
   or premium-buying decision defensible.
3. **Is the whole distribution right?** Coverage of the model's 50/80/90/95%
   intervals. An option is a bet on a region of the distribution, so a model
   whose 90% interval only contains the outcome 78% of the time is
   systematically underpricing wings, no matter how good its point forecast.

The GARCH refit cadence is a deliberate compromise: refitting daily is honest
but slow in pure Python, and parameters on a single name barely move week to
week, so the default refits monthly and re-filters the conditional variance
daily in between (which uses no future information either way).
"""

import math

import numpy as np

from .vol import (TRADING_DAYS, ewma_vol, fit_garch, realized_vol)

# Default walk-forward settings. Kept small enough that a full backtest runs
# inside one cached web request.
MIN_TRAIN = 400          # trading days before the first out-of-sample call
STEP = 5                 # evaluate every week
REFIT_EVERY = 21         # refit GARCH monthly, re-filter daily in between


def _filter_forward(garch, returns):
    """Run the fitted GARCH filter over ``returns`` to get today's conditional
    variance without refitting. Uses only the parameters and the data, so it
    stays strictly out of sample."""
    sigma2 = garch.var_bar
    for x in returns:
        eps = x - garch.mu
        sigma2 = garch.omega + garch.alpha * eps * eps + garch.beta * sigma2
    return sigma2


def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def run_walk_forward(returns, horizon=21, min_train=MIN_TRAIN, step=STEP,
                     refit_every=REFIT_EVERY, r_annual=0.04):
    """Out-of-sample forecasts at each evaluation date.

    Returns a list of records, one per evaluation date, each holding the
    competing vol forecasts, the model's directional probability and the
    realized outcome over the next ``horizon`` days.
    """
    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    n = r.size
    if n < min_train + horizon + 20:
        return []

    records = []
    garch = None
    last_fit_at = -10 ** 9

    for t in range(min_train, n - horizon, step):
        train = r[:t]

        if garch is None or (t - last_fit_at) >= refit_every:
            fitted = fit_garch(train, min_obs=min(min_train, 250))
            if fitted is not None:
                garch, last_fit_at = fitted, t

        # --- competing volatility forecasts for the next `horizon` days ---
        f = {}
        f['rv21'] = realized_vol(train, 21)
        f['rv63'] = realized_vol(train, 63)
        f['ewma'] = ewma_vol(train)
        if garch is not None:
            # Re-filter with the stored parameters so the conditional variance
            # is current even between refits.
            sigma2_now = _filter_forward(garch, train[max(0, t - 750):t])
            garch.sigma2_last = sigma2_now
            f['garch'] = garch.horizon_vol(horizon)
        else:
            f['garch'] = None

        model_vol = f['garch'] or f['ewma'] or f['rv21']
        if not model_vol:
            continue

        # --- realized outcome over the next `horizon` days ---
        fwd = r[t:t + horizon]
        realized_total = float(np.sum(fwd))                  # log return
        realized_vol_fwd = math.sqrt(float(np.mean(fwd * fwd)) * TRADING_DAYS)

        T = horizon / TRADING_DAYS
        sd = model_vol * math.sqrt(T)

        # Directional probability under the risk-neutral convention: the
        # -sigma^2/2 drag is the only thing moving it off 50%, so this is
        # literally "how much does the variance drain the median".
        mu_log = (r_annual - 0.5 * model_vol ** 2) * T
        p_up_model = _norm_cdf(mu_log / sd) if sd > 0 else 0.5

        # A momentum tilt, included so the "no directional skill" claim is
        # tested against something rather than asserted.
        mom = float(np.sum(train[-63:]))
        p_up_mom = _norm_cdf((mu_log + 0.15 * mom) / sd) if sd > 0 else 0.5

        z = (realized_total - mu_log) / sd if sd > 0 else 0.0
        records.append({
            'i': int(t),
            'forecasts': f,
            'model_vol': model_vol,
            'realized_vol_fwd': realized_vol_fwd,
            'realized_return': float(math.expm1(realized_total)),
            'up': bool(realized_total > 0),
            'p_up_model': p_up_model,
            'p_up_momentum': p_up_mom,
            'z': z,
        })
    return records


# ── Scoring ───────────────────────────────────────────────────────────────

def direction_scores(records):
    """Brier scores and a reliability table for the directional calls."""
    if not records:
        return {}
    y = np.array([1.0 if rec['up'] else 0.0 for rec in records])
    p_model = np.array([rec['p_up_model'] for rec in records])
    p_mom = np.array([rec['p_up_momentum'] for rec in records])
    base = float(np.mean(y))

    def brier(p):
        return float(np.mean((p - y) ** 2))

    # Reliability: bucket the forecasts and compare predicted to realized.
    edges = [0.0, 0.40, 0.45, 0.475, 0.50, 0.525, 0.55, 0.60, 1.0]
    buckets = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        m = (p_model >= lo) & (p_model < hi)
        if m.sum() >= 5:
            buckets.append({
                'lo': lo, 'hi': hi, 'n': int(m.sum()),
                'predicted': float(np.mean(p_model[m])),
                'realized': float(np.mean(y[m])),
            })

    return {
        'n': len(records),
        'base_rate_up': base,
        'brier_model': brier(p_model),
        'brier_coinflip': brier(np.full_like(p_model, 0.5)),
        'brier_climatology': brier(np.full_like(p_model, base)),
        'brier_momentum': brier(p_mom),
        # Positive = the model beats a coin flip; the usual result is ~0.
        'skill_vs_coinflip': float(1.0 - brier(p_model) / max(brier(np.full_like(p_model, 0.5)), 1e-12)),
        'skill_momentum_vs_coinflip': float(1.0 - brier(p_mom) / max(brier(np.full_like(p_mom, 0.5)), 1e-12)),
        'mean_p_up': float(np.mean(p_model)),
        'reliability': buckets,
    }


def vol_scores(records):
    """Mincer-Zarnowitz comparison of the competing vol forecasts.

    A perfect forecast regresses onto the realized value with intercept 0 and
    slope 1; the slope is the more diagnostic number, since a slope below 1
    means the forecast over-reacts (extrapolates spikes too far).
    """
    if not records:
        return {}
    realized = np.array([rec['realized_vol_fwd'] for rec in records])
    out = {}
    for key in ('garch', 'ewma', 'rv21', 'rv63'):
        vals = [rec['forecasts'].get(key) for rec in records]
        mask = np.array([v is not None for v in vals])
        if mask.sum() < 20:
            continue
        f = np.array([v for v in vals if v is not None], dtype=float)
        y = realized[mask]
        # OLS y = a + b f
        A = np.column_stack([np.ones_like(f), f])
        coef, *_ = np.linalg.lstsq(A, y, rcond=None)
        pred = A @ coef
        ss_res = float(np.sum((y - pred) ** 2))
        ss_tot = float(np.sum((y - y.mean()) ** 2))
        out[key] = {
            'n': int(mask.sum()),
            'rmse': float(np.sqrt(np.mean((f - y) ** 2))),
            'mae': float(np.mean(np.abs(f - y))),
            'bias': float(np.mean(f - y)),
            'r2': float(1.0 - ss_res / ss_tot) if ss_tot > 0 else None,
            'mz_intercept': float(coef[0]),
            'mz_slope': float(coef[1]),
            'corr': float(np.corrcoef(f, y)[0, 1]),
        }
    return out


def coverage_scores(records):
    """Interval coverage from the standardized outcomes.

    ``z`` is the realized horizon return standardized by the model's own
    forecast. If the model is right, z is standard normal, so the share of
    outcomes inside +/- 1.28 sd should be 80%, and so on. A shortfall in the
    outer intervals is the signature that matters for options: it means the
    wings are underpriced by the model and the tails are fatter than it thinks.
    """
    if not records:
        return {}
    z = np.array([rec['z'] for rec in records])
    levels = [(0.50, 0.6745), (0.80, 1.2816), (0.90, 1.6449), (0.95, 1.9600)]
    rows = []
    for nominal, crit in levels:
        rows.append({
            'nominal': nominal,
            'empirical': float(np.mean(np.abs(z) <= crit)),
            'n': int(z.size),
        })
    return {
        'intervals': rows,
        'z_mean': float(np.mean(z)),
        'z_std': float(np.std(z)),
        'z_skew': float(np.mean(((z - z.mean()) / (z.std() + 1e-12)) ** 3)),
        'z_excess_kurtosis': float(np.mean(((z - z.mean()) / (z.std() + 1e-12)) ** 4) - 3.0),
        'pct_beyond_2sd': float(np.mean(np.abs(z) > 2.0)),
        'normal_pct_beyond_2sd': 0.0455,
    }


def summarize(returns, horizon=21, **kwargs):
    """Run the walk-forward and score all three questions."""
    records = run_walk_forward(returns, horizon=horizon, **kwargs)
    if not records:
        return {'available': False,
                'reason': 'not enough history for an out-of-sample test'}
    direction = direction_scores(records)
    vols = vol_scores(records)
    cover = coverage_scores(records)

    best_vol = None
    if vols:
        best_vol = min(vols.items(), key=lambda kv: kv[1]['rmse'])[0]

    return {
        'available': True,
        'horizon_days': horizon,
        'n_windows': len(records),
        'direction': direction,
        'volatility': vols,
        'best_vol_model': best_vol,
        'coverage': cover,
        'verdict': _verdict(direction, vols, cover, best_vol),
    }


def _verdict(direction, vols, cover, best_vol):
    """Plain-language read of the three tests — the conclusion the trade
    selection has to live with."""
    parts = []
    skill = direction.get('skill_vs_coinflip')
    if skill is not None:
        if skill > 0.01:
            parts.append(
                'Directional calls beat a coin flip out of sample '
                f'(Brier skill {skill:+.1%}) — small, and worth re-checking before sizing on it.')
        else:
            parts.append(
                'Directional calls do NOT beat a coin flip out of sample '
                f'(Brier skill {skill:+.1%}). Treat direction as unforecastable; '
                'the only question left is whether the range is priced right.')
    if best_vol and best_vol in vols:
        v = vols[best_vol]
        r2 = v.get('r2')
        stats = (f'{best_vol} has the lowest error (RMSE {v["rmse"]:.1%}, '
                 f'R^2 {r2:.2f}, MZ slope {v["mz_slope"]:.2f})')
        if r2 is not None and r2 >= 0.15 and v['mz_slope'] > 0.3:
            parts.append(f'Volatility is forecastable here: {stats}.')
        else:
            # Do not claim skill the regression does not show. On a short
            # sample of a jumpy single name this is the common outcome, and it
            # means the vol forecast is a weak anchor -- size accordingly.
            parts.append(
                f'Volatility forecasts are weak on this sample: {stats}. '
                'Treat every probability above as a wide range, not a number, '
                'and lean on the vol stress column rather than the EV ranking.')
    if cover.get('intervals'):
        worst = max(cover['intervals'], key=lambda x: abs(x['empirical'] - x['nominal']))
        gap = worst['empirical'] - worst['nominal']
        if gap < -0.02:
            read = ('the model is too narrow there, so tail strikes are more likely '
                    'to be reached than it says.')
        elif gap > 0.02:
            read = ('the model is too wide there, so it overstates what the wings '
                    'are worth and will look for premium to sell that is not there.')
        else:
            read = 'close enough that strike probabilities can be taken at face value.'
        parts.append(
            f'Interval coverage at {worst["nominal"]:.0%} came in at '
            f'{worst["empirical"]:.0%} — {read}')
    return ' '.join(parts)
