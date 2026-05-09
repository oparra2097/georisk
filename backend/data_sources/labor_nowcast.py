"""
US labor market nowcast — in-house monthly payroll & unemployment estimates.

Methodology
-----------
A small bridge regression maps high-frequency leading indicators to two
targets:

  Δ payrolls (thousands, MoM)   — BLS PAYEMS
  Δ unemployment rate (pp, MoM) — BLS UNRATE

Indicators (all monthly, aggregated from FRED to calendar-month mean):

  ICSA       Initial jobless claims                      (negative slope)
  CCSA       Continuing claims                           (negative slope)
  AWHMAN     Avg weekly hours, manufacturing             (positive slope)
  INDPRO     Industrial production index                 (positive slope)
  UMCSENT    Consumer sentiment                          (positive slope)
  JTSJOL     JOLTS job openings  (1-month lagged)        (positive slope)

For each historical month we run the same bridge equation that we would
run live, so the user gets a like-for-like backcast they can compare
against the BLS realisation.  The current month is treated identically —
indicators for the in-progress month are averaged over whatever data we
have so far, and fed through the fitted coefficients.

Models are linear regressions fit at refresh time on ~12y of FRED
history.  Caching is 6 hours, matching gdp_nowcast.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from backend.data_sources.fred_client import fetch_series

logger = logging.getLogger(__name__)

CACHE_TTL = 21600                      # 6 hours
HISTORY_MONTHS = 144                   # ~12 years of training data
TRACK_RECORD_MONTHS = 24               # months shown in the BLS-vs-estimate panel

# Indicators feeding both models.  `lag` shifts the X column relative to
# the target month — JTSJOL is published with a one-month delay so we use
# month T-1 openings to explain month T payrolls.
INDICATORS = [
    {'id': 'ICSA',    'label': 'Initial Jobless Claims',     'lag': 0},
    {'id': 'CCSA',    'label': 'Continued Jobless Claims',   'lag': 0},
    {'id': 'AWHMAN',  'label': 'Avg Weekly Hours (Mfg)',     'lag': 0},
    {'id': 'INDPRO',  'label': 'Industrial Production',      'lag': 0},
    {'id': 'UMCSENT', 'label': 'Consumer Sentiment',         'lag': 0},
    {'id': 'JTSJOL',  'label': 'JOLTS Job Openings',         'lag': 1},
]

PAYROLLS_SERIES = 'PAYEMS'             # SA, thousands, monthly
UNRATE_SERIES = 'UNRATE'               # SA, %, monthly


# ── Cache ────────────────────────────────────────────────────────────────────

_lock = threading.Lock()
_cached_result: Optional[dict] = None
_cached_at: float = 0.0


# ── Helpers ──────────────────────────────────────────────────────────────────

def _month_key(d: str) -> str:
    """Return 'YYYY-MM' from a 'YYYY-MM-DD' date string."""
    return d[:7]


def _shift_month(month_key: str, by: int) -> str:
    y, m = int(month_key[:4]), int(month_key[5:7])
    total = y * 12 + (m - 1) + by
    ny, nm = total // 12, total % 12 + 1
    return f'{ny:04d}-{nm:02d}'


def _aggregate_monthly(obs: List[dict]) -> Dict[str, float]:
    """Bucket observations by calendar month and return the mean per month."""
    buckets: Dict[str, List[float]] = defaultdict(list)
    for o in obs:
        buckets[_month_key(o['date'])].append(o['value'])
    return {m: sum(v) / len(v) for m, v in buckets.items() if v}


def _solve_ols(X: List[List[float]], y: List[float]) -> Optional[List[float]]:
    """Solve OLS via numpy.linalg.lstsq.

    Returns coefficient vector (length = n_features + 1, with intercept first)
    or None if the system is singular / under-determined.
    """
    if len(X) < 12 or not X:
        return None
    try:
        import numpy as np
        X_arr = np.array(X, dtype=float)
        y_arr = np.array(y, dtype=float)
        # Augment with intercept column
        X_aug = np.hstack([np.ones((X_arr.shape[0], 1)), X_arr])
        coefs, *_ = np.linalg.lstsq(X_aug, y_arr, rcond=None)
        return coefs.tolist()
    except Exception as e:
        logger.warning(f"labor nowcast OLS failed: {e}")
        return None


def _predict(coefs: List[float], features: List[float]) -> float:
    out = coefs[0]
    for c, f in zip(coefs[1:], features):
        out += c * f
    return out


def _build_feature_panel(
    indicator_monthly: Dict[str, Dict[str, float]],
    target_months: List[str],
) -> Tuple[List[List[float]], List[str], List[str]]:
    """For each target month, return its feature row, dropping months where
    any indicator is missing (after lag).  Returns (X, kept_months, dropped).
    """
    X: List[List[float]] = []
    kept: List[str] = []
    dropped: List[str] = []
    for m in target_months:
        row: List[float] = []
        ok = True
        for ind in INDICATORS:
            src = indicator_monthly.get(ind['id'], {})
            mk = _shift_month(m, -ind['lag'])
            v = src.get(mk)
            if v is None:
                ok = False
                break
            row.append(v)
        if ok:
            X.append(row)
            kept.append(m)
        else:
            dropped.append(m)
    return X, kept, dropped


# ── Public ───────────────────────────────────────────────────────────────────

def compute_labor_nowcast() -> dict:
    """Compute the labor-market nowcast bundle.

    Returns a dict with payroll + unemployment backcasts (BLS actual vs our
    estimate by month) and a forward nowcast for the latest in-progress
    month.
    """
    from backend.data_sources.fred_client import _get_api_key
    if not _get_api_key():
        return {'error': 'FRED_API_KEY not configured'}

    today = date.today()
    start_date = (today - timedelta(days=int(HISTORY_MONTHS * 31))).strftime('%Y-%m-%d')

    # ── Fetch + aggregate ──────────────────────────────────────────────
    try:
        payems = fetch_series(PAYROLLS_SERIES, start_date=start_date)
        unrate = fetch_series(UNRATE_SERIES, start_date=start_date)
    except Exception as e:
        logger.error(f"labor nowcast: PAYEMS/UNRATE fetch failed: {e}")
        return {'error': f'FRED fetch failed: {e}'}

    if not payems or not unrate:
        return {'error': 'PAYEMS/UNRATE empty from FRED'}

    payems_m = _aggregate_monthly(payems)        # level (thousands)
    unrate_m = _aggregate_monthly(unrate)        # rate (%)

    indicator_monthly: Dict[str, Dict[str, float]] = {}
    for ind in INDICATORS:
        try:
            obs = fetch_series(ind['id'], start_date=start_date)
        except Exception as e:
            logger.warning(f"labor nowcast: {ind['id']} fetch failed: {e}")
            obs = []
        indicator_monthly[ind['id']] = _aggregate_monthly(obs)

    # ── Build target series (Δpayroll, ΔUR) per month ─────────────────
    payems_months = sorted(payems_m.keys())
    pay_diffs: Dict[str, float] = {}
    for i in range(1, len(payems_months)):
        m, prev = payems_months[i], payems_months[i - 1]
        pay_diffs[m] = payems_m[m] - payems_m[prev]   # thousands

    unrate_months = sorted(unrate_m.keys())
    unrate_diffs: Dict[str, float] = {}
    for i in range(1, len(unrate_months)):
        m, prev = unrate_months[i], unrate_months[i - 1]
        unrate_diffs[m] = unrate_m[m] - unrate_m[prev]   # pp

    # ── Fit OLS for each target ────────────────────────────────────────
    fit_months = [m for m in payems_months[1:] if m in pay_diffs]
    X_pay, X_pay_months, _ = _build_feature_panel(indicator_monthly, fit_months)
    y_pay = [pay_diffs[m] for m in X_pay_months]
    pay_coefs = _solve_ols(X_pay, y_pay)

    fit_months_ur = [m for m in unrate_months[1:] if m in unrate_diffs]
    X_ur, X_ur_months, _ = _build_feature_panel(indicator_monthly, fit_months_ur)
    y_ur = [unrate_diffs[m] for m in X_ur_months]
    ur_coefs = _solve_ols(X_ur, y_ur)

    if pay_coefs is None or ur_coefs is None:
        return {'error': 'Unable to fit bridge regression — insufficient FRED history'}

    # ── Backcast: estimate every month with the fitted coefficients ────
    payroll_track: List[dict] = []
    for m, x in zip(X_pay_months, X_pay):
        est = _predict(pay_coefs, x)
        actual = pay_diffs.get(m)
        payroll_track.append({
            'month': m,
            'actual_change': round(actual, 1) if actual is not None else None,
            'estimate_change': round(est, 1),
            'error': round(est - actual, 1) if actual is not None else None,
        })

    unrate_track: List[dict] = []
    for m, x in zip(X_ur_months, X_ur):
        est = _predict(ur_coefs, x)
        actual = unrate_diffs.get(m)
        unrate_track.append({
            'month': m,
            'actual_change': round(actual, 3) if actual is not None else None,
            'estimate_change': round(est, 3),
            'error': round(est - actual, 3) if actual is not None else None,
        })

    # ── Forward nowcast: latest month available across indicators ──────
    latest_target_month = _shift_month(
        f'{today.year:04d}-{today.month:02d}', 0
    )
    nowcast_payload = _build_forward_nowcast(
        latest_target_month, indicator_monthly, pay_coefs, ur_coefs,
        payems_m, unrate_m,
    )

    # ── Summary stats over the most recent 12-month window ─────────────
    summary = _summary_stats(payroll_track[-12:], unrate_track[-12:])

    # ── Final payload ─────────────────────────────────────────────────
    return {
        'payroll_track': payroll_track[-TRACK_RECORD_MONTHS:],
        'payroll_track_full': payroll_track,
        'unrate_track': unrate_track[-TRACK_RECORD_MONTHS:],
        'unrate_track_full': unrate_track,
        'nowcast': nowcast_payload,
        'summary': summary,
        'indicators': [
            {
                'id': ind['id'],
                'label': ind['label'],
                'lag': ind['lag'],
                'coefficient_payrolls': round(pay_coefs[i + 1], 4),
                'coefficient_unrate': round(ur_coefs[i + 1], 6),
            }
            for i, ind in enumerate(INDICATORS)
        ],
        'methodology': (
            'Bridge OLS regression of monthly Δpayrolls (thousands) and '
            'Δunemployment rate (pp) on six leading indicators averaged to '
            'calendar-month frequency. Fitted on the past ~12 years of FRED '
            'data; refit on every cache cycle.'
        ),
        'sample_size': {
            'payrolls': len(X_pay_months),
            'unrate': len(X_ur_months),
        },
        'last_refreshed': datetime.now().isoformat(timespec='seconds'),
    }


def _build_forward_nowcast(
    month_key: str,
    indicator_monthly: Dict[str, Dict[str, float]],
    pay_coefs: List[float],
    ur_coefs: List[float],
    payems_m: Dict[str, float],
    unrate_m: Dict[str, float],
) -> dict:
    """Estimate Δpayrolls and ΔUR for `month_key` using the fitted bridge.

    If a given indicator hasn't published its `month_key` value yet (common
    for in-progress months), fall back to the most recent prior monthly
    average for that indicator and flag it.
    """
    features: List[float] = []
    indicator_status: List[dict] = []
    for ind in INDICATORS:
        src = indicator_monthly.get(ind['id'], {})
        mk = _shift_month(month_key, -ind['lag'])
        v = src.get(mk)
        used_month = mk
        is_partial = False
        if v is None:
            available = sorted(src.keys())
            if not available:
                # No history at all — model can't fire.  Bail and let the
                # caller render an "unavailable" state instead of a junk
                # estimate built off zeros.
                return {
                    'month': month_key,
                    'available': False,
                    'reason': f"No data for indicator {ind['id']}",
                }
            used_month = available[-1]
            v = src[used_month]
            is_partial = used_month != mk
        features.append(v)
        indicator_status.append({
            'id': ind['id'],
            'label': ind['label'],
            'month': used_month,
            'value': round(v, 3),
            'partial': is_partial,
        })

    pay_est = _predict(pay_coefs, features)
    ur_est = _predict(ur_coefs, features)

    # Latest BLS reading we can chain off of for headline level estimates
    latest_pay_month = max(payems_m.keys()) if payems_m else None
    latest_unrate_month = max(unrate_m.keys()) if unrate_m else None
    latest_pay_value = payems_m.get(latest_pay_month) if latest_pay_month else None
    latest_unrate_value = unrate_m.get(latest_unrate_month) if latest_unrate_month else None

    return {
        'month': month_key,
        'available': True,
        'payroll_estimate_change': round(pay_est, 1),
        'unrate_estimate_change': round(ur_est, 3),
        'implied_payroll_level': (
            round(latest_pay_value + pay_est, 1)
            if latest_pay_value is not None else None
        ),
        'implied_unrate_level': (
            round(latest_unrate_value + ur_est, 2)
            if latest_unrate_value is not None else None
        ),
        'last_bls_payroll_month': latest_pay_month,
        'last_bls_unrate_month': latest_unrate_month,
        'indicators': indicator_status,
    }


def _summary_stats(payroll_window: List[dict], unrate_window: List[dict]) -> dict:
    """MAE and directional-hit-rate over a recent track-record window."""
    def _mae(rows: List[dict]) -> Optional[float]:
        errs = [abs(r['error']) for r in rows if r.get('error') is not None]
        return round(sum(errs) / len(errs), 2) if errs else None

    def _hit_rate(rows: List[dict]) -> Optional[float]:
        hits = total = 0
        for r in rows:
            a, e = r.get('actual_change'), r.get('estimate_change')
            if a is None or e is None:
                continue
            total += 1
            if (a >= 0 and e >= 0) or (a < 0 and e < 0):
                hits += 1
        return round(100 * hits / total, 1) if total else None

    return {
        'payroll_mae_thousands': _mae(payroll_window),
        'unrate_mae_pp': _mae(unrate_window),
        'payroll_direction_hit_rate_pct': _hit_rate(payroll_window),
        'unrate_direction_hit_rate_pct': _hit_rate(unrate_window),
        'window_months': len(payroll_window),
    }


def get_labor_nowcast() -> dict:
    """Thread-safe cached labor nowcast."""
    global _cached_result, _cached_at
    with _lock:
        if _cached_result and (time.time() - _cached_at) < CACHE_TTL:
            return _cached_result
    result = compute_labor_nowcast()
    with _lock:
        _cached_result = result
        _cached_at = time.time()
    return result


def clear_cache():
    """Drop the cached nowcast — used by /refresh + scheduler."""
    global _cached_result, _cached_at
    with _lock:
        _cached_result = None
        _cached_at = 0.0
