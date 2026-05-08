"""
US GDP Nowcast — simplified bridge equation model.

Estimates current-quarter real GDP growth (annualized QoQ %) using
high-frequency economic indicators from FRED.

Methodology:
  1. For each indicator, compute QoQ % change (current quarter partial avg
     vs prior quarter full avg).
  2. Apply pre-calibrated weights (based on historical GDP correlation).
  3. Add intercept (trend growth ~2.0%) and sum weighted contributions.
  4. Result: annualized quarterly GDP growth estimate.

Indicators (FRED series):
  PAYEMS  — Nonfarm Payrolls (monthly, thousands)
  ICSA    — Initial Jobless Claims (weekly, inverted)
  INDPRO  — Industrial Production Index (monthly)
  RSXFS   — Advance Retail Sales ex-auto (monthly, millions)
  HOUST   — Housing Starts (monthly, thousands)
  UMCSENT — Consumer Sentiment (monthly)
  UNRATE  — Unemployment Rate (monthly, inverted)

Thread-safe cache with 6-hour TTL.
"""

import threading
import time
import logging
from datetime import datetime, date
from backend.data_sources.fred_client import fetch_series

logger = logging.getLogger(__name__)

CACHE_TTL = 21600  # 6 hours


# ── Indicator Configuration ──────────────────────────────────────────────────

INDICATORS = [
    {
        'id': 'PAYEMS',
        'label': 'Nonfarm Payrolls',
        'weight': 0.25,
        'inverted': False,
        'units': 'Thousands',
    },
    {
        'id': 'RSXFS',
        'label': 'Retail Sales (ex-auto)',
        'weight': 0.20,
        'inverted': False,
        'units': 'Millions $',
    },
    {
        'id': 'INDPRO',
        'label': 'Industrial Production',
        'weight': 0.20,
        'inverted': False,
        'units': 'Index',
    },
    {
        'id': 'HOUST',
        'label': 'Housing Starts',
        'weight': 0.10,
        'inverted': False,
        'units': 'Thousands',
    },
    {
        'id': 'ICSA',
        'label': 'Initial Jobless Claims',
        'weight': 0.10,
        'inverted': True,  # higher claims = worse
        'units': 'Thousands',
    },
    {
        'id': 'UMCSENT',
        'label': 'Consumer Sentiment',
        'weight': 0.08,
        'inverted': False,
        'units': 'Index',
    },
    {
        'id': 'UNRATE',
        'label': 'Unemployment Rate',
        'weight': 0.07,
        'inverted': True,  # higher unemployment = worse
        'units': '%',
    },
]

# Trend growth intercept (long-run US potential GDP ~2%)
GDP_INTERCEPT = 2.0

# Sensitivity: how much a 1% change in each indicator maps to GDP impact
# Calibrated from historical correlations
SENSITIVITY = {
    'PAYEMS':  8.0,   # 1% payroll growth ≈ 8pp GDP contribution (weighted)
    'RSXFS':   5.0,   # Retail spending has high multiplier
    'INDPRO':  6.0,   # Industrial output tracks GDP closely
    'HOUST':   3.0,   # Housing is volatile but lower GDP share
    'ICSA':    4.0,   # Claims are leading indicator
    'UMCSENT': 2.0,   # Sentiment is softer signal
    'UNRATE':  5.0,   # Unemployment is lagging but important
}


# ── Thread-safe Cache ────────────────────────────────────────────────────────

_lock = threading.Lock()
_cached_result = None
_cached_at = 0.0


def _quarter_for_date(d):
    """Return (year, quarter) for a date."""
    return d.year, (d.month - 1) // 3 + 1


def _quarter_start_end(year, q):
    """Return (start_date, end_date) strings for a quarter."""
    start_month = (q - 1) * 3 + 1
    end_month = start_month + 2
    if end_month == 12:
        end_date = f"{year}-12-31"
    else:
        # Last day of end_month
        import calendar
        last_day = calendar.monthrange(year, end_month)[1]
        end_date = f"{year}-{end_month:02d}-{last_day:02d}"
    start_date = f"{year}-{start_month:02d}-01"
    return start_date, end_date


def _prev_quarter(year, q):
    """Return (year, quarter) for the previous quarter."""
    if q == 1:
        return year - 1, 4
    return year, q - 1


def _quarter_label(year, q):
    return f"Q{q} {year}"


def _compute_indicator_contribution(series_id, curr_q_start, curr_q_end,
                                     prev_q_start, prev_q_end, indicator_cfg):
    """
    Compute one indicator's contribution to the GDP nowcast.

    Returns dict with: series_id, label, prev_avg, curr_avg, pct_change,
    contribution, signal, latest_date, latest_value
    """
    # Fetch enough history to cover both quarters
    data = fetch_series(series_id, start_date=prev_q_start, end_date=curr_q_end)
    if not data:
        return None

    # Split into previous quarter and current quarter observations
    prev_obs = [d['value'] for d in data if prev_q_start <= d['date'] <= prev_q_end]
    curr_obs = [d['value'] for d in data if curr_q_start <= d['date'] <= curr_q_end]

    if not prev_obs:
        return None

    prev_avg = sum(prev_obs) / len(prev_obs)
    if prev_avg == 0:
        return None

    # Current quarter might have partial data (that's the whole point of a nowcast)
    if curr_obs:
        curr_avg = sum(curr_obs) / len(curr_obs)
    else:
        # No current quarter data yet — use last available observation
        curr_avg = data[-1]['value']

    pct_change = ((curr_avg - prev_avg) / abs(prev_avg)) * 100

    # Invert for indicators where higher = worse
    if indicator_cfg['inverted']:
        pct_change = -pct_change

    # Contribution = weight × sensitivity × pct_change
    weight = indicator_cfg['weight']
    sensitivity = SENSITIVITY.get(series_id, 4.0)
    contribution = weight * sensitivity * pct_change

    # Signal label
    if contribution > 0.3:
        signal = 'positive'
    elif contribution < -0.3:
        signal = 'negative'
    else:
        signal = 'neutral'

    latest = data[-1]

    return {
        'series_id': series_id,
        'label': indicator_cfg['label'],
        'weight': weight,
        'units': indicator_cfg['units'],
        'prev_avg': round(prev_avg, 2),
        'curr_avg': round(curr_avg, 2),
        'pct_change': round(pct_change, 2),
        'contribution': round(contribution, 2),
        'signal': signal,
        'latest_date': latest['date'],
        'latest_value': latest['value'],
        'observations_current_q': len(curr_obs),
    }


def _fetch_gdp_history():
    """Fetch actual GDP growth rates for historical comparison."""
    data = fetch_series('A191RL1Q225SBEA')  # Real GDP % change (annualized)
    if not data:
        return []

    history = []
    for obs in data[-12:]:  # Last 12 quarters (3 years)
        d = datetime.strptime(obs['date'], '%Y-%m-%d')
        year, q = _quarter_for_date(d)
        history.append({
            'quarter': _quarter_label(year, q),
            'actual': round(obs['value'], 1),
        })
    return history


def compute_nowcast():
    """
    Compute the GDP nowcast. Returns structured dict with:
    - nowcast: current quarter estimate
    - prior_quarter: last quarter's actual GDP
    - contributions: per-indicator breakdown
    - history: past quarters actual GDP
    """
    # Use the dynamic resolver instead of stale Config.FRED_API_KEY,
    # so a key added in Render after boot is picked up without redeploy.
    from backend.data_sources.fred_client import _get_api_key
    if not _get_api_key():
        return {'error': 'FRED_API_KEY not configured'}

    today = date.today()
    curr_year, curr_q = _quarter_for_date(today)
    prev_year, prev_q = _prev_quarter(curr_year, curr_q)

    curr_q_start, curr_q_end = _quarter_start_end(curr_year, curr_q)
    prev_q_start, prev_q_end = _quarter_start_end(prev_year, prev_q)

    contributions = []
    total_contribution = 0.0

    for ind in INDICATORS:
        result = _compute_indicator_contribution(
            ind['id'], curr_q_start, curr_q_end,
            prev_q_start, prev_q_end, ind
        )
        if result:
            contributions.append(result)
            total_contribution += result['contribution']
        else:
            contributions.append({
                'series_id': ind['id'],
                'label': ind['label'],
                'weight': ind['weight'],
                'units': ind['units'],
                'contribution': 0.0,
                'signal': 'unavailable',
                'pct_change': None,
                'latest_date': None,
                'latest_value': None,
                'observations_current_q': 0,
            })

    # Nowcast = intercept + sum of weighted contributions
    nowcast_estimate = GDP_INTERCEPT + total_contribution

    # Fetch actual GDP history
    history = _fetch_gdp_history()

    # Compute a retroactive estimate for the prior quarter (Q-1) using the
    # same bridge methodology.  Q-1 data is complete, so this acts as a
    # "backcast" until BEA publishes the official advance estimate.
    pp_year, pp_q = _prev_quarter(prev_year, prev_q)
    pp_start, pp_end = _quarter_start_end(pp_year, pp_q)
    prior_total = 0.0
    prior_valid = 0
    for ind in INDICATORS:
        r = _compute_indicator_contribution(
            ind['id'], prev_q_start, prev_q_end,
            pp_start, pp_end, ind
        )
        if r:
            prior_total += r['contribution']
            prior_valid += 1
    prior_estimate = round(GDP_INTERCEPT + prior_total, 1) if prior_valid >= 3 else None

    # Get prior quarter actual — use most recent quarter with data
    # (the immediately prior quarter may not be released yet due to BEA lag)
    prior_actual = None
    prior_label = _quarter_label(prev_year, prev_q)
    for h in history:
        if h['quarter'] == prior_label:
            prior_actual = h['actual']
            break
    # If prior quarter not yet released, use the latest available
    if prior_actual is None and history:
        latest_h = history[-1]
        prior_actual = latest_h['actual']
        prior_label = latest_h['quarter'] + ' (latest)'

    # Sort contributions by absolute contribution (largest impact first)
    contributions.sort(key=lambda x: abs(x['contribution']), reverse=True)

    prior_q_label = _quarter_label(prev_year, prev_q)

    return {
        'nowcast': {
            'quarter': _quarter_label(curr_year, curr_q),
            'estimate': round(nowcast_estimate, 1),
            'as_of': today.isoformat(),
        },
        'prior_quarter': {
            'quarter': prior_label,
            'actual': prior_actual,
            'model_estimate': prior_estimate,
            'official_quarter': prior_q_label,
        },
        'contributions': contributions,
        'history': history,
        'methodology': 'Bridge equation: weighted high-frequency indicators + trend intercept',
        'last_refreshed': datetime.now().isoformat(timespec='seconds'),
    }


def get_gdp_nowcast():
    """Thread-safe cached GDP nowcast."""
    global _cached_result, _cached_at

    with _lock:
        if _cached_result and (time.time() - _cached_at) < CACHE_TTL:
            return _cached_result

    result = compute_nowcast()

    with _lock:
        _cached_result = result
        _cached_at = time.time()

    return result
