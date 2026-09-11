"""Volatility estimation for a single equity.

Three layers, in increasing order of how much structure they assume:

1. **Realized estimators** — close-to-close plus the range-based family
   (Parkinson, Garman-Klass, Rogers-Satchell, Yang-Zhang). Range estimators
   use the intraday high/low and are 4-8x more efficient per observation,
   which matters for a name like TSEM where a 21-day close-to-close number is
   noisy enough to move the whole option book around.
2. **EWMA** (RiskMetrics) — one parameter, no fitting, reacts immediately.
3. **GARCH(1,1)** — fitted by Gaussian QMLE with variance targeting. This is
   the only one that produces a *term structure*: it knows vol mean-reverts,
   so a 7-day forecast after a spike and a 60-day forecast after the same
   spike are different numbers. That distinction is the whole reason a
   calendar-aware option model beats "plug trailing vol into Black-Scholes".

Everything takes plain numpy arrays of daily data, oldest first.
"""

import math

import numpy as np

TRADING_DAYS = 252

# RiskMetrics daily decay. Kept as a module constant so the API payload can
# report the exact parameterisation used.
EWMA_LAMBDA = 0.94

# Windows (trading days) the UI shows as a realized-vol term structure.
RV_WINDOWS = (5, 10, 21, 63, 126, 252)


def trading_days_until(expiry, today=None):
    """Trading days from ``today`` (exclusive) to ``expiry`` (inclusive).

    Weekdays via numpy's business-day count, then a 252/260 haircut for market
    holidays. Exact holiday calendars change the answer by under a day, which
    moves an option's vol by well under 1% — not worth carrying a calendar
    that goes stale.

    This matters more than it looks: the simulation steps in trading days and
    Black-Scholes takes a year fraction, so the two must agree or the model
    "finds" an edge that is only a day-count mismatch. Everything downstream
    uses ``T = trading_days / 252``.
    """
    import datetime as _dt

    if isinstance(expiry, str):
        expiry = _dt.datetime.strptime(expiry, '%Y-%m-%d').date()
    today = today or _dt.date.today()
    if expiry <= today:
        return 0
    weekdays = int(np.busday_count(today, expiry)) + 1
    return max(int(round(weekdays * 252.0 / 260.0)), 1)


def log_returns(closes):
    """Daily log returns from a close series (oldest first)."""
    c = np.asarray(closes, dtype=float)
    c = c[np.isfinite(c) & (c > 0)]
    if c.size < 2:
        return np.array([])
    return np.diff(np.log(c))


def annualize(daily_sigma):
    return float(daily_sigma) * math.sqrt(TRADING_DAYS)


def realized_vol(returns, window=None):
    """Annualized close-to-close vol over the last ``window`` returns.

    Uses the zero-mean convention (``sqrt(mean(r^2))``) rather than the sample
    standard deviation: over an option's life the drift is not identifiable
    and subtracting a noisy sample mean only adds estimation error.
    """
    r = np.asarray(returns, dtype=float)
    if window:
        r = r[-window:]
    if r.size < 2:
        return None
    return annualize(math.sqrt(float(np.mean(r * r))))


def realized_vol_term_structure(returns, windows=RV_WINDOWS):
    out = []
    for w in windows:
        if len(returns) >= w:
            out.append({'window': w, 'vol': realized_vol(returns, w)})
    return out


# ── Range-based estimators (need OHLC) ────────────────────────────────────

def parkinson_vol(high, low, window=21):
    """Parkinson (1980): uses the high-low range only. ~5x more efficient
    than close-to-close but blind to overnight gaps, so it *understates* a
    name that moves on earnings gaps."""
    h = np.asarray(high, dtype=float)[-window:]
    l = np.asarray(low, dtype=float)[-window:]
    mask = np.isfinite(h) & np.isfinite(l) & (l > 0)
    h, l = h[mask], l[mask]
    if h.size < 2:
        return None
    var = np.mean(np.log(h / l) ** 2) / (4.0 * math.log(2.0))
    return annualize(math.sqrt(var))


def garman_klass_vol(open_, high, low, close, window=21):
    """Garman-Klass (1980): range plus open-close. Assumes no drift and no
    overnight jump."""
    o = np.asarray(open_, dtype=float)[-window:]
    h = np.asarray(high, dtype=float)[-window:]
    l = np.asarray(low, dtype=float)[-window:]
    c = np.asarray(close, dtype=float)[-window:]
    mask = np.isfinite(o) & np.isfinite(h) & np.isfinite(l) & np.isfinite(c) & (l > 0) & (o > 0)
    o, h, l, c = o[mask], h[mask], l[mask], c[mask]
    if c.size < 2:
        return None
    hl = np.log(h / l) ** 2
    co = np.log(c / o) ** 2
    var = np.mean(0.5 * hl - (2.0 * math.log(2.0) - 1.0) * co)
    if var <= 0:
        return None
    return annualize(math.sqrt(var))


def rogers_satchell_vol(open_, high, low, close, window=21):
    """Rogers-Satchell (1991): drift-independent, so it does not mistake a
    strong trend for volatility."""
    o = np.asarray(open_, dtype=float)[-window:]
    h = np.asarray(high, dtype=float)[-window:]
    l = np.asarray(low, dtype=float)[-window:]
    c = np.asarray(close, dtype=float)[-window:]
    mask = np.isfinite(o) & np.isfinite(h) & np.isfinite(l) & np.isfinite(c) & (l > 0) & (o > 0)
    o, h, l, c = o[mask], h[mask], l[mask], c[mask]
    if c.size < 2:
        return None
    var = np.mean(np.log(h / c) * np.log(h / o) + np.log(l / c) * np.log(l / o))
    if var <= 0:
        return None
    return annualize(math.sqrt(var))


def yang_zhang_vol(open_, high, low, close, window=21):
    """Yang-Zhang (2000): overnight + open-to-close + Rogers-Satchell.

    The right default for a gappy single name — it is the only estimator here
    that is both drift-independent and gap-aware, which is exactly TSEM's
    return profile (quiet tape, violent earnings and foundry-cycle gaps).
    """
    o = np.asarray(open_, dtype=float)[-(window + 1):]
    h = np.asarray(high, dtype=float)[-(window + 1):]
    l = np.asarray(low, dtype=float)[-(window + 1):]
    c = np.asarray(close, dtype=float)[-(window + 1):]
    n = min(len(o), len(h), len(l), len(c))
    if n < 3:
        return None
    o, h, l, c = o[-n:], h[-n:], l[-n:], c[-n:]
    if not np.all(np.isfinite([o, h, l, c])) or np.any(o <= 0) or np.any(l <= 0):
        return None

    # Overnight (previous close -> open) and open-to-close components.
    on = np.log(o[1:] / c[:-1])
    oc = np.log(c[1:] / o[1:])
    k_n = len(on)
    if k_n < 2:
        return None
    v_on = float(np.sum((on - on.mean()) ** 2) / (k_n - 1))
    v_oc = float(np.sum((oc - oc.mean()) ** 2) / (k_n - 1))
    rs = (np.log(h[1:] / c[1:]) * np.log(h[1:] / o[1:])
          + np.log(l[1:] / c[1:]) * np.log(l[1:] / o[1:]))
    v_rs = float(np.mean(rs))

    k = 0.34 / (1.34 + (k_n + 1) / (k_n - 1))
    var = v_on + k * v_oc + (1.0 - k) * v_rs
    if var <= 0:
        return None
    return annualize(math.sqrt(var))


# ── EWMA ──────────────────────────────────────────────────────────────────

def ewma_vol(returns, lam=EWMA_LAMBDA):
    """RiskMetrics EWMA conditional vol (annualized), latest observation."""
    r = np.asarray(returns, dtype=float)
    if r.size < 10:
        return None
    var = float(np.mean(r[:20] * r[:20])) if r.size >= 20 else float(np.mean(r * r))
    for x in r:
        var = lam * var + (1.0 - lam) * x * x
    return annualize(math.sqrt(var))


def ewma_series(returns, lam=EWMA_LAMBDA):
    """Full EWMA conditional-vol path (annualized) for charting."""
    r = np.asarray(returns, dtype=float)
    if r.size < 10:
        return np.array([])
    var = float(np.mean(r[:20] * r[:20])) if r.size >= 20 else float(np.mean(r * r))
    out = np.empty(r.size)
    for i, x in enumerate(r):
        var = lam * var + (1.0 - lam) * x * x
        out[i] = var
    return np.sqrt(out) * math.sqrt(TRADING_DAYS)


# ── GARCH(1,1) ────────────────────────────────────────────────────────────

class Garch11:
    """GARCH(1,1) fitted by Gaussian quasi-MLE with variance targeting.

    ``sigma2_t = omega + alpha * eps_{t-1}^2 + beta * sigma2_{t-1}``

    Variance targeting pins the unconditional variance to the sample variance
    (``omega = var_bar * (1 - alpha - beta)``), which removes one parameter and
    stops the optimiser wandering into the near-unit-root corner that a short
    single-name sample invites. Gaussian QMLE stays consistent under fat tails,
    so the tail thickness is estimated separately as a Student-t df on the
    standardized residuals and used only for simulation.
    """

    def __init__(self, omega, alpha, beta, mu, var_bar, sigma2_last, nu,
                 loglik=None, n_obs=0):
        self.omega = omega
        self.alpha = alpha
        self.beta = beta
        self.mu = mu
        self.var_bar = var_bar
        self.sigma2_last = sigma2_last      # conditional variance for t+1
        self.nu = nu                        # Student-t df of standardized resid
        self.loglik = loglik
        self.n_obs = n_obs

    # -- properties an options desk actually reads ------------------------
    @property
    def persistence(self):
        return self.alpha + self.beta

    @property
    def half_life(self):
        """Trading days for a vol shock to decay halfway to the long-run level."""
        p = self.persistence
        if not (0 < p < 1):
            return None
        return math.log(0.5) / math.log(p)

    @property
    def long_run_vol(self):
        return annualize(math.sqrt(max(self.var_bar, 1e-12)))

    @property
    def spot_vol(self):
        """Annualized conditional vol for the next day."""
        return annualize(math.sqrt(max(self.sigma2_last, 1e-12)))

    def forecast_path(self, horizon):
        """Daily variance forecasts for t+1 .. t+horizon.

        Mean reversion is geometric in the persistence:
        ``E[sigma2_{t+k}] = var_bar + p^(k-1) * (sigma2_{t+1} - var_bar)``.
        """
        p = self.persistence
        out = np.empty(int(horizon))
        v = self.sigma2_last
        for k in range(int(horizon)):
            out[k] = v
            v = self.var_bar + p * (v - self.var_bar)
        return out

    def horizon_vol(self, horizon):
        """Annualized vol of the *aggregate* return over ``horizon`` days.

        This is the number to put in Black-Scholes for an option expiring in
        ``horizon`` days — the average forward variance, not today's spot vol.
        """
        if horizon <= 0:
            return None
        path = self.forecast_path(horizon)
        return annualize(math.sqrt(float(np.mean(path))))

    def term_structure(self, horizons):
        return [{'days': int(h), 'vol': self.horizon_vol(int(h))}
                for h in horizons if h > 0]

    def rescaled(self, factor):
        """Copy of the model with every variance scaled by ``factor**2``.

        Forecast variance is linear in both the current conditional variance
        and the long-run level, so scaling both scales the horizon vol by
        exactly ``factor`` while leaving persistence, half-life and the tail
        parameter untouched. That is what makes a vol stress test a *vol*
        stress test rather than a different model.
        """
        f2 = float(factor) ** 2
        return Garch11(omega=self.omega * f2, alpha=self.alpha, beta=self.beta,
                       mu=self.mu, var_bar=self.var_bar * f2,
                       sigma2_last=self.sigma2_last * f2, nu=self.nu,
                       loglik=self.loglik, n_obs=self.n_obs)

    def to_dict(self):
        return {
            'omega': self.omega, 'alpha': self.alpha, 'beta': self.beta,
            'mu_daily': self.mu, 'persistence': self.persistence,
            'half_life_days': self.half_life,
            'long_run_vol': self.long_run_vol,
            'spot_vol': self.spot_vol,
            'nu': self.nu, 'loglik': self.loglik, 'n_obs': self.n_obs,
        }


def _garch_nll(params, r, var_bar):
    """Negative Gaussian log-likelihood of GARCH(1,1) with variance targeting."""
    alpha, beta = params
    if alpha <= 0 or beta <= 0 or alpha + beta >= 0.9999:
        return 1e12
    omega = var_bar * (1.0 - alpha - beta)
    sigma2 = var_bar
    nll = 0.0
    for x in r:
        if sigma2 <= 1e-14:
            return 1e12
        nll += math.log(sigma2) + (x * x) / sigma2
        sigma2 = omega + alpha * x * x + beta * sigma2
    return 0.5 * nll


def _fit_student_t_df(z):
    """MLE of Student-t degrees of freedom on unit-variance residuals.

    ``z`` is standardized to unit variance, so the t is rescaled by
    ``sqrt(nu / (nu - 2))`` to keep that variance as nu changes.
    """
    from math import lgamma, log, pi

    def nll(nu):
        if nu <= 2.05 or nu > 200:
            return 1e12
        s = math.sqrt(nu / (nu - 2.0))
        # z = t_nu / s, so log f(z) = log f_t(z*s) + log s; the log-s Jacobian
        # is already folded into `const` below -- do not add it twice.
        const = lgamma((nu + 1) / 2) - lgamma(nu / 2) - 0.5 * log(pi * (nu - 2.0))
        return -float(np.sum(const - (nu + 1) / 2 * np.log1p((z * s) ** 2 / nu)))

    best_nu, best = 8.0, nll(8.0)
    # Coarse grid then golden-section refine: the likelihood in nu is smooth
    # and unimodal, and a grid keeps this robust without a scipy dependency.
    for nu in np.arange(2.2, 40.0, 0.2):
        v = nll(float(nu))
        if v < best:
            best, best_nu = v, float(nu)
    return best_nu


def fit_garch(returns, min_obs=250):
    """Fit GARCH(1,1) to daily log returns. Returns ``Garch11`` or ``None``."""
    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    if r.size < min_obs:
        return None

    mu = float(np.mean(r))
    eps = r - mu
    var_bar = float(np.mean(eps * eps))
    if var_bar <= 0:
        return None

    try:
        from scipy.optimize import minimize
        best = None
        # Multi-start: the (alpha, beta) surface has a flat ridge along the
        # persistence, so a single start can land on a silly split.
        for a0, b0 in ((0.08, 0.88), (0.05, 0.93), (0.15, 0.80), (0.03, 0.80)):
            res = minimize(_garch_nll, np.array([a0, b0]), args=(eps, var_bar),
                           method='L-BFGS-B',
                           bounds=[(1e-4, 0.6), (1e-4, 0.999)])
            if res.success or np.isfinite(res.fun):
                if best is None or res.fun < best.fun:
                    best = res
        if best is None:
            return None
        alpha, beta = float(best.x[0]), float(best.x[1])
        loglik = -float(best.fun)
    except Exception:
        # No scipy: fall back to a coarse grid search over the same box.
        grid = [(a, b) for a in np.arange(0.02, 0.31, 0.02)
                for b in np.arange(0.50, 0.98, 0.02) if a + b < 0.999]
        vals = [(_garch_nll((a, b), eps, var_bar), a, b) for a, b in grid]
        f, alpha, beta = min(vals)
        loglik = -f

    if alpha + beta >= 0.9999:
        beta = 0.9999 - alpha
    omega = var_bar * (1.0 - alpha - beta)

    # Replay the filter to get the conditional variance for t+1 and residuals.
    sigma2 = var_bar
    z = np.empty(eps.size)
    for i, x in enumerate(eps):
        z[i] = x / math.sqrt(sigma2)
        sigma2 = omega + alpha * x * x + beta * sigma2

    nu = _fit_student_t_df(z / np.std(z))
    return Garch11(omega=omega, alpha=alpha, beta=beta, mu=mu, var_bar=var_bar,
                   sigma2_last=sigma2, nu=nu, loglik=loglik, n_obs=int(r.size))


# ── Regime context ────────────────────────────────────────────────────────

def vol_percentile(returns, window=21, lookback=504):
    """Where today's ``window``-day realized vol sits in its own history.

    A percentile, not a level: "50% vol" means nothing without knowing that
    TSEM's own two-year range is (say) 30-90%.
    """
    r = np.asarray(returns, dtype=float)
    if r.size < window + 60:
        return None
    hist = []
    start = max(window, r.size - lookback)
    for i in range(start, r.size + 1):
        seg = r[i - window:i]
        hist.append(math.sqrt(float(np.mean(seg * seg))))
    if len(hist) < 30:
        return None
    current = hist[-1]
    return float(np.mean(np.asarray(hist) <= current) * 100.0)


def vol_cone(returns, windows=RV_WINDOWS, lookback=756):
    """Realized-vol cone: the historical distribution of realized vol at each
    horizon, with today's reading marked. The standard way to see whether an
    option is expensive *for its tenor* rather than in the abstract."""
    r = np.asarray(returns, dtype=float)
    cone = []
    for w in windows:
        if r.size < w + 40:
            continue
        seg_start = max(w, r.size - lookback)
        vals = [math.sqrt(float(np.mean(r[i - w:i] ** 2))) * math.sqrt(TRADING_DAYS)
                for i in range(seg_start, r.size + 1)]
        if len(vals) < 20:
            continue
        a = np.asarray(vals)
        cone.append({
            'window': int(w),
            'min': float(a.min()), 'p25': float(np.percentile(a, 25)),
            'median': float(np.median(a)), 'p75': float(np.percentile(a, 75)),
            'max': float(a.max()), 'current': float(a[-1]),
            'percentile': float(np.mean(a <= a[-1]) * 100.0),
        })
    return cone


def return_moments(returns):
    """Skew, excess kurtosis and tail counts — the shape a normal misses."""
    r = np.asarray(returns, dtype=float)
    if r.size < 30:
        return {}
    sd = float(np.std(r))
    if sd <= 0:
        return {}
    z = (r - float(np.mean(r))) / sd
    return {
        'skew': float(np.mean(z ** 3)),
        'excess_kurtosis': float(np.mean(z ** 4) - 3.0),
        'worst_day': float(np.min(r)),
        'best_day': float(np.max(r)),
        'pct_beyond_3sd': float(np.mean(np.abs(z) > 3.0) * 100.0),
        # A normal puts 0.27% of days beyond 3sd; the gap is the fat tail the
        # simulation has to reproduce or every wing option is mispriced.
        'normal_pct_beyond_3sd': 0.27,
        'n_obs': int(r.size),
    }
