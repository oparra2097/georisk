"""Random-walk simulation engines for a single equity.

Four engines, run on the same spot, horizon and drift so their answers are
directly comparable. The spread between them *is* the model risk:

* ``gbm``       — constant vol, Gaussian steps. The textbook random walk and
                  the distribution Black-Scholes assumes. Included as the
                  benchmark, not because it is right.
* ``student_t`` — same constant vol, Student-t steps fitted to the data's own
                  tail. Same centre, fatter wings.
* ``garch_t``   — vol follows the fitted GARCH(1,1) forward, steps are
                  Student-t. Reproduces vol clustering *and* mean reversion in
                  vol, so a long-dated horizon sees vol drift back to the
                  long-run level instead of freezing today's spike.
* ``bootstrap`` — stationary block bootstrap of the stock's own historical
                  returns. No distributional assumption at all: the tails,
                  skew and clustering are whatever the stock actually did.

Every engine returns terminal prices *and* the running path extremes, because
an option position is rarely held to expiry — the probability of *touching* a
level matters as much as the probability of closing beyond it.

Path extremes use an exact Brownian-bridge correction: sampling daily and
taking the max of the closes understates the true running max, which would
systematically flatter every stop-loss and barrier estimate.
"""

import math

import numpy as np

from .vol import TRADING_DAYS

DEFAULT_PATHS = 20000

# Quantiles recorded per step for the fan chart. Computing them costs a sort
# per step, so simulations that only need the terminal distribution pass
# ``band_qs=()``.
BAND_QS = (5, 25, 50, 75, 95)

# Floor on the Student-t degrees of freedom used for *simulation*. A single
# name's returns sometimes fit nu below 3, where kurtosis is undefined and a
# handful of paths dominate every tail statistic. The fitted value is still
# reported -- it is informative about the stock -- but simulating with it
# produces numbers no one should size a position on.
MIN_SIM_NU = 3.0

ENGINES = ('gbm', 'student_t', 'garch_t', 'bootstrap')

ENGINE_LABELS = {
    'gbm': 'Gaussian GBM',
    'student_t': 'Student-t GBM',
    'garch_t': 'GARCH(1,1)-t',
    'bootstrap': 'Block bootstrap',
}


# ── Drift ─────────────────────────────────────────────────────────────────

def resolve_log_drift(mode, sigma_ann, r=0.0, q=0.0, hist_mu_daily=0.0,
                      custom_annual=None, shrink=0.25):
    """Daily *log* drift for the chosen convention.

    The distinction matters more than it looks. Under the risk-neutral measure
    the log drift is ``r - q - sigma^2/2``: the arithmetic expected return is
    ``r - q``, but the *median* path is dragged below it by half the variance.
    At TSEM-like vol that drag is the single biggest driver of "probability
    up" over short horizons, and it is why a driftless high-vol stock finishes
    below its starting price more often than not while still having a positive
    expected value.

    Modes:
      ``risk_neutral`` — what the option market must use to avoid arbitrage.
      ``zero``         — pure coin flip: zero log drift, P(up) = 50% exactly.
      ``historical``   — the stock's own mean log return, heavily shrunk.
      ``custom``       — your view, as an annual simple return.
    """
    sigma_d2 = (float(sigma_ann) ** 2) / TRADING_DAYS
    if mode == 'zero':
        return 0.0
    if mode == 'historical':
        # Shrink hard toward zero. A sample mean return has a standard error of
        # roughly sigma/sqrt(T); over two years of a 50%-vol stock that is
        # ~35%/yr, so the raw estimate is almost pure noise.
        return float(hist_mu_daily) * float(shrink)
    if mode == 'custom' and custom_annual is not None:
        mu_annual_log = math.log1p(float(custom_annual))
        return mu_annual_log / TRADING_DAYS - 0.5 * sigma_d2
    # risk_neutral (default)
    return (float(r) - float(q)) / TRADING_DAYS - 0.5 * sigma_d2


# ── Simulation result ─────────────────────────────────────────────────────

class SimResult:
    """Terminal prices, running extremes and per-step quantile bands."""

    def __init__(self, engine, spot, horizon, terminal, run_max, run_min,
                 bands, sigma_ann, log_drift_daily, n_paths, extra=None):
        self.engine = engine
        self.spot = float(spot)
        self.horizon = int(horizon)
        self.terminal = terminal
        self.run_max = run_max
        self.run_min = run_min
        self.bands = bands               # {'q05': [...], 'q25': [...], ...}
        self.sigma_ann = sigma_ann
        self.log_drift_daily = log_drift_daily
        self.n_paths = int(n_paths)
        self.extra = extra or {}


def _bridge_extremes(log_prev, log_next, var_step, u_hi, u_lo):
    """Exact running max/min of the Brownian bridge between two daily closes.

    For a bridge from ``a`` to ``b`` with variance ``v`` over the step,
    ``P(max > m) = exp(-2 (m - a)(m - b) / v)``, which inverts in closed form.
    Without this the simulated intraday extremes are biased toward the closes
    and every touch probability comes out too low.
    """
    diff = log_next - log_prev
    disc = diff * diff - 2.0 * var_step * np.log(np.maximum(u_hi, 1e-15))
    hi = 0.5 * (log_prev + log_next + np.sqrt(np.maximum(disc, 0.0)))
    disc_lo = diff * diff - 2.0 * var_step * np.log(np.maximum(u_lo, 1e-15))
    lo = 0.5 * (log_prev + log_next - np.sqrt(np.maximum(disc_lo, 0.0)))
    return hi, lo


def _run_paths(spot, horizon, n_paths, step_fn, var_fn, rng, band_qs=BAND_QS):
    """Shared path loop.

    ``step_fn(t, rng)`` returns the vector of log returns for day ``t``;
    ``var_fn(t)`` returns that day's variance (for the bridge correction).
    Memory stays O(n_paths): only the running state and the per-day quantiles
    are kept, never the full (paths x days) matrix.
    """
    log_spot = math.log(spot)
    log_s = np.full(n_paths, log_spot)
    run_hi = np.full(n_paths, log_spot)
    run_lo = np.full(n_paths, log_spot)
    bands = {f'q{q:02d}': [] for q in band_qs}

    for t in range(horizon):
        step = step_fn(t, rng)
        log_next = log_s + step
        v = max(float(var_fn(t)), 1e-12)
        hi, lo = _bridge_extremes(log_s, log_next, v,
                                  rng.random(n_paths), rng.random(n_paths))
        run_hi = np.maximum(run_hi, hi)
        run_lo = np.minimum(run_lo, lo)
        log_s = log_next
        prices = np.exp(log_s)
        for q in band_qs:
            bands[f'q{q:02d}'].append(float(np.percentile(prices, q)))

    return np.exp(log_s), np.exp(run_hi), np.exp(run_lo), bands


# ── Engines ───────────────────────────────────────────────────────────────

def simulate_gbm(spot, horizon, sigma_ann, log_drift_daily,
                 n_paths=DEFAULT_PATHS, seed=None, band_qs=BAND_QS):
    rng = np.random.default_rng(seed)
    sd = float(sigma_ann) / math.sqrt(TRADING_DAYS)
    var = sd * sd
    terminal, hi, lo, bands = _run_paths(
        spot, horizon, n_paths,
        step_fn=lambda t, g: log_drift_daily + sd * g.standard_normal(n_paths),
        var_fn=lambda t: var, rng=rng, band_qs=band_qs)
    return SimResult('gbm', spot, horizon, terminal, hi, lo, bands,
                     sigma_ann, log_drift_daily, n_paths)


def simulate_student_t(spot, horizon, sigma_ann, log_drift_daily, nu,
                       n_paths=DEFAULT_PATHS, seed=None, band_qs=BAND_QS):
    rng = np.random.default_rng(seed)
    sd = float(sigma_ann) / math.sqrt(TRADING_DAYS)
    var = sd * sd
    nu = max(float(nu), MIN_SIM_NU)
    scale = math.sqrt(nu / (nu - 2.0))     # rescale t to unit variance

    def step(t, g):
        return log_drift_daily + sd * (g.standard_t(nu, n_paths) / scale)

    terminal, hi, lo, bands = _run_paths(spot, horizon, n_paths, step,
                                         lambda t: var, rng, band_qs)
    return SimResult('student_t', spot, horizon, terminal, hi, lo, bands,
                     sigma_ann, log_drift_daily, n_paths, {'nu': nu})


def simulate_garch(spot, horizon, garch, log_drift_daily,
                   n_paths=DEFAULT_PATHS, seed=None, band_qs=BAND_QS):
    """Simulate the fitted GARCH(1,1) forward with Student-t innovations.

    Each path carries its own conditional variance, so paths that get hit
    early stay volatile — the clustering that makes a one-week and a
    three-month option on the same stock price off different vols.
    """
    rng = np.random.default_rng(seed)
    nu = max(float(garch.nu), MIN_SIM_NU)
    scale = math.sqrt(nu / (nu - 2.0))
    sigma2 = np.full(n_paths, float(garch.sigma2_last))
    mean_var = []                       # cross-path mean variance, for bridges

    state = {'sigma2': sigma2}

    def step(t, g):
        s2 = state['sigma2']
        mean_var.append(float(np.mean(s2)))
        eps = np.sqrt(s2) * (g.standard_t(nu, n_paths) / scale)
        state['sigma2'] = garch.omega + garch.alpha * eps * eps + garch.beta * s2
        return log_drift_daily + eps

    def var_fn(t):
        return mean_var[t] if t < len(mean_var) else float(garch.sigma2_last)

    terminal, hi, lo, bands = _run_paths(spot, horizon, n_paths, step, var_fn, rng,
                                         band_qs)
    sigma_ann = garch.horizon_vol(horizon)
    return SimResult('garch_t', spot, horizon, terminal, hi, lo, bands,
                     sigma_ann, log_drift_daily, n_paths,
                     {'nu': nu, 'spot_vol': garch.spot_vol,
                      'long_run_vol': garch.long_run_vol})


def simulate_bootstrap(spot, horizon, returns, log_drift_daily,
                       n_paths=DEFAULT_PATHS, mean_block=10, seed=None,
                       sigma_ann=None, band_qs=BAND_QS, vol_scale=1.0):
    """Stationary block bootstrap (Politis & Romano, 1994).

    Blocks of geometric length (mean ``mean_block`` days) are resampled from
    the historical return series, so volatility clustering and the actual
    shape of the gaps survive resampling; iid resampling would destroy both.
    The historical mean is removed and the chosen drift added back, so this
    engine is comparable to the others rather than inheriting whatever the
    trailing sample happened to do.
    """
    rng = np.random.default_rng(seed)
    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    if r.size < 60:
        return None
    # Scaling the demeaned returns moves the vol level while keeping the
    # empirical shape (skew, tails, clustering) exactly as it was.
    r = (r - float(np.mean(r))) * float(vol_scale)
    n = r.size
    p_new = 1.0 / max(int(mean_block), 1)

    idx = rng.integers(0, n, size=n_paths)
    daily_var = float(np.mean(r * r))
    state = {'idx': idx, 'first': True}

    def step(t, g):
        if state['first']:
            state['first'] = False
        else:
            # With prob p start a fresh block, else continue the current one.
            cont = g.random(n_paths) >= p_new
            nxt = (state['idx'] + 1) % n
            fresh = g.integers(0, n, size=n_paths)
            state['idx'] = np.where(cont, nxt, fresh)
        return log_drift_daily + r[state['idx']]

    terminal, hi, lo, bands = _run_paths(spot, horizon, n_paths, step,
                                         lambda t: daily_var, rng, band_qs)
    ann = sigma_ann if sigma_ann is not None else math.sqrt(daily_var * TRADING_DAYS)
    return SimResult('bootstrap', spot, horizon, terminal, hi, lo, bands,
                     ann, log_drift_daily, n_paths, {'mean_block': mean_block})


# ── Reading the simulated distribution ────────────────────────────────────

def terminal_stats(sim, move_grid=(0.02, 0.05, 0.10, 0.15, 0.20, 0.30)):
    """Direction, quantiles, expected move and touch probabilities."""
    s0 = sim.spot
    term = sim.terminal
    n = term.size
    ret = term / s0 - 1.0

    qs = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    quantiles = {f'p{q:02d}': float(np.percentile(term, q)) for q in qs}

    moves = []
    for m in move_grid:
        moves.append({
            'move': m,
            'p_up_beyond': float(np.mean(term >= s0 * (1 + m))),
            'p_down_beyond': float(np.mean(term <= s0 * (1 - m))),
            'p_touch_up': float(np.mean(sim.run_max >= s0 * (1 + m))),
            'p_touch_down': float(np.mean(sim.run_min <= s0 * (1 - m))),
        })

    p_up = float(np.mean(term > s0))
    losses = ret[ret < 0]
    return {
        'engine': sim.engine,
        'engine_label': ENGINE_LABELS.get(sim.engine, sim.engine),
        'horizon_days': sim.horizon,
        'n_paths': n,
        'spot': s0,
        'p_up': p_up,
        'p_down': float(np.mean(term < s0)),
        'mean': float(np.mean(term)),
        'median': float(np.median(term)),
        'mean_return': float(np.mean(ret)),
        'median_return': float(np.median(ret)),
        'stdev_return': float(np.std(ret)),
        'expected_abs_move': float(np.mean(np.abs(ret))),
        'quantiles': quantiles,
        # 68% band == the "expected move" an option desk quotes off the straddle
        'band68': [float(np.percentile(term, 16)), float(np.percentile(term, 84))],
        'moves': moves,
        'skew': float(np.mean(((ret - ret.mean()) / (ret.std() + 1e-12)) ** 3)),
        'excess_kurtosis': float(np.mean(((ret - ret.mean()) / (ret.std() + 1e-12)) ** 4) - 3.0),
        'cvar05': float(np.mean(np.sort(ret)[:max(int(0.05 * n), 1)])),
        'prob_loss_gt_20pct': float(np.mean(ret <= -0.20)),
        'sigma_ann_used': sim.sigma_ann,
        'log_drift_daily': sim.log_drift_daily,
        'worst_path_return': float(np.min(ret)),
        'best_path_return': float(np.max(ret)),
        'downside_dev': float(np.sqrt(np.mean(losses ** 2))) if losses.size else 0.0,
    }


def prob_above(sim, level):
    return float(np.mean(sim.terminal >= float(level)))


def prob_below(sim, level):
    return float(np.mean(sim.terminal <= float(level)))


def histogram(sim, bins=60):
    """Terminal-price histogram for the distribution chart."""
    counts, edges = np.histogram(sim.terminal, bins=bins)
    centers = 0.5 * (edges[:-1] + edges[1:])
    total = counts.sum() or 1
    return {
        'centers': [float(c) for c in centers],
        'density': [float(c / total) for c in counts],
    }


def fan_chart(sim):
    """Per-day quantile bands for the cone chart."""
    return {k: [float(x) for x in v] for k, v in sim.bands.items()}


def simulate(engine, spot, horizon, *, sigma_ann=None, log_drift_daily=0.0,
             garch=None, returns=None, nu=None, n_paths=DEFAULT_PATHS,
             seed=None, band_qs=BAND_QS, vol_scale=1.0):
    """Dispatch to one engine by name.

    ``vol_scale`` multiplies the volatility level while leaving the engine's
    shape alone, so a stress test perturbs one thing at a time: a GARCH stress
    stays fat-tailed and mean-reverting, a bootstrap stress keeps the stock's
    own empirical return shape. Returns ``None`` if the engine's inputs are
    missing (no GARCH fit, too little history to bootstrap).
    """
    scale = float(vol_scale)
    if engine == 'garch_t':
        if garch is None:
            return None
        model = garch.rescaled(scale) if scale != 1.0 else garch
        return simulate_garch(spot, horizon, model, log_drift_daily,
                              n_paths=n_paths, seed=seed, band_qs=band_qs)
    if engine == 'student_t':
        if sigma_ann is None:
            return None
        return simulate_student_t(spot, horizon, sigma_ann * scale,
                                  log_drift_daily, nu or 5.0, n_paths=n_paths,
                                  seed=seed, band_qs=band_qs)
    if engine == 'bootstrap':
        if returns is None:
            return None
        return simulate_bootstrap(spot, horizon, returns, log_drift_daily,
                                  n_paths=n_paths, seed=seed,
                                  sigma_ann=(sigma_ann * scale) if sigma_ann else None,
                                  band_qs=band_qs, vol_scale=scale)
    if sigma_ann is None:
        return None
    return simulate_gbm(spot, horizon, sigma_ann * scale, log_drift_daily,
                        n_paths=n_paths, seed=seed, band_qs=band_qs)
