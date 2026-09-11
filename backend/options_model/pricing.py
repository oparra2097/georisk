"""Black-Scholes pricing, greeks and implied-vol inversion.

Pure numpy — no market data, no I/O — so everything here is testable in
isolation. All rates and vols are continuously-compounded annual decimals and
``T`` is in years (252 trading days = 1.0).

Convention used throughout the module: ``q`` is the continuous dividend yield,
so a forward is ``F = S * exp((r - q) * T)``. TSEM pays no dividend, but the
argument is kept explicit so the same code prices index and dividend names.
"""

import math

import numpy as np

SQRT_2PI = math.sqrt(2.0 * math.pi)

# Below this many years (or this much vol) the Black-Scholes formulas collapse
# to the intrinsic value; guarding avoids divide-by-zero in d1/d2.
_MIN_T = 1e-8
_MIN_VOL = 1e-8


def _norm_cdf(x):
    """Standard normal CDF, vectorised (scipy-free)."""
    return 0.5 * (1.0 + np.vectorize(math.erf)(np.asarray(x, dtype=float) / math.sqrt(2.0)))


def _norm_pdf(x):
    x = np.asarray(x, dtype=float)
    return np.exp(-0.5 * x * x) / SQRT_2PI


def d1_d2(S, K, T, r, q, sigma):
    """The two Black-Scholes arguments. Scalars or arrays."""
    S = np.asarray(S, dtype=float)
    K = np.asarray(K, dtype=float)
    T = np.maximum(np.asarray(T, dtype=float), _MIN_T)
    sigma = np.maximum(np.asarray(sigma, dtype=float), _MIN_VOL)
    vol_t = sigma * np.sqrt(T)
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / vol_t
    return d1, d1 - vol_t


def bs_price(S, K, T, r, q, sigma, kind='call'):
    """Black-Scholes-Merton price of a European option."""
    d1, d2 = d1_d2(S, K, T, r, q, sigma)
    disc_r = np.exp(-r * np.maximum(T, 0.0))
    disc_q = np.exp(-q * np.maximum(T, 0.0))
    if kind == 'call':
        return S * disc_q * _norm_cdf(d1) - K * disc_r * _norm_cdf(d2)
    return K * disc_r * _norm_cdf(-d2) - S * disc_q * _norm_cdf(-d1)


def greeks(S, K, T, r, q, sigma, kind='call'):
    """Delta, gamma, vega, theta, rho for one European option.

    Vega is per 1 vol point (0.01), theta is per calendar day and rho is per
    1bp — i.e. the units an options desk actually quotes, not the raw
    derivatives.
    """
    d1, d2 = d1_d2(S, K, T, r, q, sigma)
    T_eff = max(float(T), _MIN_T)
    sigma_eff = max(float(sigma), _MIN_VOL)
    sqrt_t = math.sqrt(T_eff)
    disc_r = math.exp(-r * T_eff)
    disc_q = math.exp(-q * T_eff)
    pdf_d1 = float(_norm_pdf(d1))

    gamma = disc_q * pdf_d1 / (S * sigma_eff * sqrt_t)
    vega = S * disc_q * pdf_d1 * sqrt_t                     # per 1.00 of vol
    common_theta = -(S * disc_q * pdf_d1 * sigma_eff) / (2.0 * sqrt_t)

    if kind == 'call':
        delta = disc_q * float(_norm_cdf(d1))
        theta = (common_theta
                 - r * K * disc_r * float(_norm_cdf(d2))
                 + q * S * disc_q * float(_norm_cdf(d1)))
        rho = K * T_eff * disc_r * float(_norm_cdf(d2))
    else:
        delta = -disc_q * float(_norm_cdf(-d1))
        theta = (common_theta
                 + r * K * disc_r * float(_norm_cdf(-d2))
                 - q * S * disc_q * float(_norm_cdf(-d1)))
        rho = -K * T_eff * disc_r * float(_norm_cdf(-d2))

    return {
        'delta': delta,
        'gamma': gamma,
        'vega': vega / 100.0,        # per 1 vol point
        'theta': theta / 365.0,      # per calendar day
        'rho': rho / 10000.0,        # per 1bp
    }


def prob_itm_rn(S, K, T, r, q, sigma, kind='call'):
    """Risk-neutral probability of finishing in the money.

    N(d2) for a call, N(-d2) for a put. This is what the *market* is charging
    for — the benchmark the simulated real-world probability gets compared
    against. It is not a forecast: it is a price expressed as a probability.
    """
    _, d2 = d1_d2(S, K, T, r, q, sigma)
    p = _norm_cdf(d2) if kind == 'call' else _norm_cdf(-d2)
    return float(p)


def dual_delta(S, K, T, r, q, sigma, kind='call'):
    """d(price)/dK — the discounted risk-neutral density's CDF at K.

    Used to read the market-implied distribution straight off a strike ladder
    without assuming the whole surface is one lognormal.
    """
    _, d2 = d1_d2(S, K, T, r, q, sigma)
    disc_r = math.exp(-r * max(float(T), 0.0))
    if kind == 'call':
        return -disc_r * float(_norm_cdf(d2))
    return disc_r * float(_norm_cdf(-d2))


def implied_vol(price, S, K, T, r, q, kind='call',
                lo=1e-4, hi=6.0, tol=1e-7, max_iter=100):
    """Invert Black-Scholes for sigma by bisection.

    Bisection rather than Newton because a Newton step blows up on the deep
    wings where vega is ~0, which is exactly where a wide, stale quote on an
    illiquid single-name chain lives. Returns ``None`` when the price is
    outside the no-arbitrage bounds (a crossed or stale quote).
    """
    price = float(price)
    if not np.isfinite(price) or price <= 0.0 or T <= 0.0:
        return None

    disc_r = math.exp(-r * T)
    disc_q = math.exp(-q * T)
    if kind == 'call':
        intrinsic = max(S * disc_q - K * disc_r, 0.0)
        upper = S * disc_q
    else:
        intrinsic = max(K * disc_r - S * disc_q, 0.0)
        upper = K * disc_r
    if price < intrinsic - 1e-9 or price > upper + 1e-9:
        return None

    f_lo = bs_price(S, K, T, r, q, lo, kind) - price
    f_hi = bs_price(S, K, T, r, q, hi, kind) - price
    if f_lo * f_hi > 0:
        return None

    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        f_mid = bs_price(S, K, T, r, q, mid, kind) - price
        if abs(f_mid) < tol or (hi - lo) < tol:
            return float(mid)
        if f_lo * f_mid <= 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return float(0.5 * (lo + hi))
