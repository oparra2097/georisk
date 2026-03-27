"""
Commodities Forecast data source.

Dynamic, date-aware forecast engine:
  - Detects today's date to determine completed/current quarters
  - Fetches YTD actual prices from yfinance (live data)
  - Computes current quarter-end estimate from partial data
  - Forecasts next 4 months using scenario spread assumptions
  - Calculates year-end (FY) weighted average
  - Thread-safe cache with 24-hour TTL

Scenario weights: Base Case 70% | Severe Case 20% | Worst Case 10%
"""

import threading
import time
import logging
import calendar
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

CACHE_TTL = 86400   # 24 hours
RETRY_BACKOFF = 3600  # 1 hour after failure
HISTORY_YEARS = 10   # years of historical quarterly data

MONTH_NAMES = [
    '', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
]

# ── Forecast Configuration ──────────────────────────────────────────────────

SCENARIO_WEIGHTS = {
    'Worst Case': 0.10,
    'Severe Case': 0.20,
    'Base Case': 0.70,
}

SCENARIO_LABELS = {
    'Worst Case': 'Iran targets critical ME production \u00b7 Brent >$130 peak \u00b7 stays $110s',
    'Severe Case': 'Hormuz closed through year-end \u00b7 No ceasefire \u00b7 Brent $115-118 sustained',
    'Base Case': 'Gradual de-escalation \u00b7 OPEC+ discipline \u00b7 Brent drifts to ~$80 Q4',
}

# (display_name, yfinance_ticker, unit, group)
COMMODITIES = {
    'WTI Crude':        ('CL=F',  '$/bbl',     'Oil & Gas'),
    'Brent Crude':      ('BZ=F',  '$/bbl',     'Oil & Gas'),
    'Natural Gas (HH)': ('NG=F',  '$/MMBtu',   'Oil & Gas'),
    'TTF Gas':          ('TTF=F', '\u20ac/MWh', 'Oil & Gas'),
    'Cocoa':            ('CC=F',  '$/MT',       'Agriculture'),
    'Wheat':            ('ZW=F',  '\u00a2/bu',  'Agriculture'),
    'Soybeans':         ('ZS=F',  '\u00a2/bu',  'Agriculture'),
    'Coffee':           ('KC=F',  '\u00a2/lb',  'Agriculture'),
    'Copper':           ('HG=F',  '\u00a2/lb',  'Metals'),
    'Gold':             ('GC=F',  '$/troy oz',  'Metals'),
}

# ── Scenario Spread Targets ─────────────────────────────────────────────────
# Quarterly targets: cumulative % change from current price by end of quarter.
# These are keyed Q1-Q4 and represent year-end targets from the start of each
# quarter.  The engine interpolates monthly values from these.

SCENARIO_SPREADS = {
    'WTI Crude': {
        'Worst Case':  {'Q1': 0.00, 'Q2': +0.9531, 'Q3': +0.8780, 'Q4': +0.7278},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.6226, 'Q3': +0.6827, 'Q4': +0.6526},
        'Base Case':   {'Q1': 0.00, 'Q2': +0.03,   'Q3': +0.05,   'Q4': +0.07},
    },
    'Brent Crude': {
        'Worst Case':  {'Q1': 0.00, 'Q2': +0.9358, 'Q3': +0.8516, 'Q4': +0.6833},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.6131, 'Q3': +0.6552, 'Q4': +0.6272},
        'Base Case':   {'Q1': 0.00, 'Q2': +0.03,   'Q3': +0.04,   'Q4': +0.05},
    },
    'Natural Gas (HH)': {
        'Worst Case':  {'Q1': 0.00, 'Q2': +1.1008, 'Q3': +1.2409, 'Q4': +1.3810},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.6246, 'Q3': +0.7367, 'Q4': +0.8207},
        'Base Case':   {'Q1': 0.00, 'Q2': -0.05,   'Q3': +0.05,   'Q4': +0.10},
    },
    'TTF Gas': {
        'Worst Case':  {'Q1': 0.00, 'Q2': +1.50, 'Q3': +1.80, 'Q4': +1.60},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.80, 'Q3': +0.90, 'Q4': +0.70},
        'Base Case':   {'Q1': 0.00, 'Q2': +0.05, 'Q3': +0.00, 'Q4': +0.15},
    },
    'Cocoa': {
        'Worst Case':  {'Q1': 0.00, 'Q2': -0.10, 'Q3': -0.12, 'Q4': -0.08},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.08, 'Q3': +0.12, 'Q4': +0.15},
        'Base Case':   {'Q1': 0.00, 'Q2': -0.03, 'Q3': -0.05, 'Q4': +0.02},
    },
    'Wheat': {
        'Worst Case':  {'Q1': 0.00, 'Q2': -0.08, 'Q3': -0.10, 'Q4': -0.06},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.10, 'Q3': +0.15, 'Q4': +0.12},
        'Base Case':   {'Q1': 0.00, 'Q2': +0.02, 'Q3': +0.00, 'Q4': +0.03},
    },
    'Soybeans': {
        'Worst Case':  {'Q1': 0.00, 'Q2': -0.07, 'Q3': -0.09, 'Q4': -0.05},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.09, 'Q3': +0.12, 'Q4': +0.10},
        'Base Case':   {'Q1': 0.00, 'Q2': +0.01, 'Q3': +0.02, 'Q4': +0.03},
    },
    'Coffee': {
        'Worst Case':  {'Q1': 0.00, 'Q2': -0.08, 'Q3': -0.10, 'Q4': -0.07},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.12, 'Q3': +0.15, 'Q4': +0.18},
        'Base Case':   {'Q1': 0.00, 'Q2': +0.02, 'Q3': +0.01, 'Q4': +0.04},
    },
    'Copper': {
        'Worst Case':  {'Q1': 0.00, 'Q2': -0.08, 'Q3': -0.10, 'Q4': -0.07},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.08, 'Q3': +0.12, 'Q4': +0.14},
        'Base Case':   {'Q1': 0.00, 'Q2': +0.01, 'Q3': +0.02, 'Q4': +0.03},
    },
    'Gold': {
        'Worst Case':  {'Q1': 0.00, 'Q2': -0.05, 'Q3': -0.06, 'Q4': -0.04},
        'Severe Case': {'Q1': 0.00, 'Q2': +0.06, 'Q3': +0.09, 'Q4': +0.12},
        'Base Case':   {'Q1': 0.00, 'Q2': +0.01, 'Q3': +0.02, 'Q4': +0.03},
    },
}

# Scenario colors for frontend
SCENARIO_COLORS = {
    'Actual':       '#94a3b8',
    'Base Case':    '#3b82f6',
    'Severe Case':  '#f59e0b',
    'Worst Case':   '#ef4444',
    'Weighted Avg': '#10b981',
}

# Group colors for commodity lines in group overview
GROUP_COMMODITY_COLORS = {
    'Oil & Gas': {
        'WTI Crude':        '#3b82f6',
        'Brent Crude':      '#10b981',
        'Natural Gas (HH)': '#f59e0b',
        'TTF Gas':          '#ef4444',
    },
    'Agriculture': {
        'Cocoa':    '#f59e0b',
        'Wheat':    '#10b981',
        'Soybeans': '#3b82f6',
        'Coffee':   '#ef4444',
    },
    'Metals': {
        'Copper': '#f97316',
        'Gold':   '#eab308',
    },
}


# ── Time Context ────────────────────────────────────────────────────────────

def _get_time_context():
    """Determine current quarter, completed quarters, and forecast months."""
    today = date.today()
    year = today.year
    current_month = today.month
    current_quarter = (current_month - 1) // 3 + 1  # 1-4

    # Build quarter date ranges for current year
    quarters = {}
    for q in range(1, 5):
        q_start = date(year, (q - 1) * 3 + 1, 1)
        if q < 4:
            q_end_month = q * 3
            q_end_day = calendar.monthrange(year, q_end_month)[1]
            q_end = date(year, q_end_month, q_end_day)
        else:
            q_end = date(year, 12, 31)
        quarters[f'Q{q}'] = (q_start, q_end)

    # Completed quarters: Q1..Q(current-1)
    completed = [f'Q{q}' for q in range(1, current_quarter)]

    # Current quarter label
    current_q_label = f'Q{current_quarter}'

    # Next 4 months for forecast
    forecast_months = []
    for i in range(1, 5):
        m = current_month + i
        y = year
        if m > 12:
            m -= 12
            y += 1
        forecast_months.append((y, m))

    # Build labels array for the API response
    labels = []
    label_types = []

    # Completed quarters
    for q_label in completed:
        labels.append(q_label)
        label_types.append('actual')

    # Current quarter (with asterisk to indicate estimate)
    labels.append(current_q_label + '*')
    label_types.append('current_q')

    # Next 4 forecast months
    for (y, m) in forecast_months:
        labels.append(MONTH_NAMES[m])
        label_types.append('forecast')

    return {
        'year': year,
        'today': today,
        'quarters': quarters,
        'completed_quarters': completed,
        'current_quarter': current_q_label,
        'current_quarter_num': current_quarter,
        'forecast_months': forecast_months,
        'labels': labels,
        'label_types': label_types,
    }


# ── Cache ────────────────────────────────────────────────────────────────────

class ForecastCache:
    """Thread-safe cache for commodity forecast data."""

    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._last_fetch = 0
        self._last_fail = 0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._last_fetch) < CACHE_TTL:
                return self._data
            if self._last_fail and (time.time() - self._last_fail) < RETRY_BACKOFF:
                return self._data or _empty_result()
        data = _fetch_forecasts()
        if data:
            with self._lock:
                self._data = data
                self._last_fetch = time.time()
                self._last_fail = 0
            return data
        with self._lock:
            self._last_fail = time.time()
            return self._data or _empty_result()

    def clear(self):
        with self._lock:
            self._data = None
            self._last_fetch = 0
            self._last_fail = 0


_cache = ForecastCache()


def _empty_result():
    ctx = _get_time_context()
    return {
        'forecast_year': ctx['year'],
        'time_context': {
            'today': ctx['today'].isoformat(),
            'current_quarter': ctx['current_quarter'],
            'labels': ctx['labels'],
            'label_types': ctx['label_types'],
            'year_end_label': f"FY {ctx['year']}",
        },
        'scenario_weights': SCENARIO_WEIGHTS,
        'scenario_labels': SCENARIO_LABELS,
        'scenario_colors': SCENARIO_COLORS,
        'groups': {},
        'meta': {
            'source': 'ParraMacro Commodities Forecast',
            'error': 'No data available',
        }
    }


# ── Core Fetch Logic ─────────────────────────────────────────────────────────

def _extract_series(data, ticker, num_tickers):
    """Extract a single ticker's Close series from yfinance multi-download."""
    try:
        if num_tickers == 1:
            series = data['Close'].dropna()
        else:
            series = data['Close'][ticker].dropna()
        return series if len(series) > 0 else None
    except Exception:
        return None


def _fetch_all_data(time_ctx):
    """
    Fetch historical + YTD data in a single yfinance call.

    Returns:
        (historical, actuals) tuple where:
        - historical: {commodity_name: [{year, quarter, label, avg_price}, ...]}
        - actuals: {commodity_name: {completed, latest_close, current_q_avg}}
    """
    import yfinance as yf

    year = time_ctx['year']
    quarters = time_ctx['quarters']
    today = time_ctx['today']

    # Fetch from 10 years ago through today
    hist_start = date(year - HISTORY_YEARS, 1, 1)
    tickers = list(set(t for t, _, _ in COMMODITIES.values()))

    logger.info(
        f"Fetching data ({hist_start} to {today}) for "
        f"{len(tickers)} tickers from yfinance"
    )

    try:
        data = yf.download(
            tickers,
            start=hist_start.isoformat(),
            end=(today + timedelta(days=1)).isoformat(),
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:
        logger.error(f"yfinance download failed: {e}")
        return {}, {}

    historical = {}
    actuals = {}

    for name, (ticker, unit, group) in COMMODITIES.items():
        series = _extract_series(data, ticker, len(tickers))
        if series is None:
            logger.warning(f"No data for {name} ({ticker})")
            continue

        # ── Historical quarterly averages (past 10 years) ──
        hist_records = []
        for y in range(year - HISTORY_YEARS, year):
            for q in range(1, 5):
                q_start = date(y, (q - 1) * 3 + 1, 1)
                q_end_month = q * 3
                q_end_day = calendar.monthrange(y, q_end_month)[1]
                q_end = date(y, q_end_month, q_end_day)

                q_data = series[q_start.isoformat():q_end.isoformat()]
                if len(q_data) > 0:
                    hist_records.append({
                        'year': y,
                        'quarter': q,
                        'label': f"{y} Q{q}",
                        'avg_price': round(float(q_data.mean()), 2),
                    })

        historical[name] = hist_records

        # ── Current year actuals ──
        ytd_series = series[date(year, 1, 1).isoformat():]
        if len(ytd_series) == 0:
            continue

        result = {
            'completed': {},
            'latest_close': round(float(ytd_series.iloc[-1]), 2),
        }

        # Completed quarter averages
        for q_label in time_ctx['completed_quarters']:
            q_start, q_end = quarters[q_label]
            q_data = ytd_series[q_start.isoformat():q_end.isoformat()]
            if len(q_data) > 0:
                result['completed'][q_label] = round(float(q_data.mean()), 2)

        # Current quarter partial average
        cq = time_ctx['current_quarter']
        cq_start, _ = quarters[cq]
        cq_data = ytd_series[cq_start.isoformat():]
        if len(cq_data) > 0:
            result['current_q_avg'] = round(float(cq_data.mean()), 2)

        actuals[name] = result
        logger.info(
            f"  {name}: latest={result['latest_close']} {unit}, "
            f"current_q_avg={result.get('current_q_avg', 'N/A')}, "
            f"hist_quarters={len(hist_records)}"
        )

    return historical, actuals


def _interpolate_monthly_spread(commodity_name, scenario, time_ctx):
    """
    Interpolate monthly spread values for the next 4 forecast months.

    Uses the quarterly spread targets and linearly interpolates based on
    how many months remain until each target quarter-end.

    Returns a list of 4 spread values (one per forecast month).
    """
    spreads_cfg = SCENARIO_SPREADS.get(commodity_name, {}).get(scenario, {})
    if not spreads_cfg:
        return [0.0] * 4

    year = time_ctx['year']
    forecast_months = time_ctx['forecast_months']

    # Build a month -> cumulative spread mapping from the quarterly targets
    # Quarter targets map to the end-month of each quarter
    quarter_end_months = {
        'Q1': 3, 'Q2': 6, 'Q3': 9, 'Q4': 12
    }

    # Build interpolation points: (month_number, spread)
    # Month 0 = start of year, current state = 0.0 spread
    # We use month-of-year as the x-axis for interpolation
    current_month = time_ctx['today'].month
    interp_points = [(current_month, 0.0)]  # baseline = today

    for q_label in ['Q1', 'Q2', 'Q3', 'Q4']:
        end_month = quarter_end_months[q_label]
        spread = spreads_cfg.get(q_label, 0.0)
        if end_month > current_month:
            interp_points.append((end_month, spread))

    # Sort by month
    interp_points.sort(key=lambda x: x[0])

    # For forecast months that go into next year, extend the last known spread
    last_spread = interp_points[-1][1] if interp_points else 0.0

    monthly_spreads = []
    for (fy, fm) in forecast_months:
        # Effective month position (can be >12 for next year)
        effective_month = fm if fy == year else fm + 12

        # Find surrounding interpolation points
        spread = _lerp_spread(effective_month, interp_points, last_spread)
        monthly_spreads.append(spread)

    return monthly_spreads


def _lerp_spread(target_month, interp_points, last_spread):
    """Linear interpolation between quarterly spread targets."""
    if not interp_points:
        return 0.0

    # Before first point
    if target_month <= interp_points[0][0]:
        return interp_points[0][1]

    # After last point — hold the last value
    if target_month >= interp_points[-1][0]:
        return last_spread

    # Find surrounding points and interpolate
    for i in range(len(interp_points) - 1):
        m0, s0 = interp_points[i]
        m1, s1 = interp_points[i + 1]
        if m0 <= target_month <= m1:
            if m1 == m0:
                return s1
            t = (target_month - m0) / (m1 - m0)
            return s0 + t * (s1 - s0)

    return last_spread


def _build_scenario_forecasts(name, actual_data, time_ctx):
    """
    Build scenario-based forecasts for a single commodity.

    Returns a dict of scenario -> {label: price} for all time labels + FY.
    """
    spreads_cfg = SCENARIO_SPREADS.get(name)
    if not spreads_cfg:
        return None

    labels = time_ctx['labels']
    label_types = time_ctx['label_types']
    latest_close = actual_data['latest_close']
    completed = actual_data.get('completed', {})
    current_q_avg = actual_data.get('current_q_avg')

    scenarios = {}

    # ── Actual row: only has values for actual/current_q columns ──
    actual_row = {}
    for i, (label, ltype) in enumerate(zip(labels, label_types)):
        if ltype == 'actual':
            # Completed quarter — use actual average
            q_key = label  # e.g. "Q1"
            actual_row[label] = completed.get(q_key)
        elif ltype == 'current_q':
            # Current quarter estimate
            actual_row[label] = current_q_avg
        else:
            actual_row[label] = None
    actual_row['FY'] = None  # We don't compute FY for the "Actual" row
    scenarios['Actual'] = actual_row

    # ── Scenario rows ──
    for scenario in spreads_cfg.keys():
        row = {}
        monthly_spreads = _interpolate_monthly_spread(name, scenario, time_ctx)

        # Actual/current_q columns — same as actual row
        forecast_vals = []
        for i, (label, ltype) in enumerate(zip(labels, label_types)):
            if ltype == 'actual':
                q_key = label
                val = completed.get(q_key)
                row[label] = val
                if val is not None:
                    forecast_vals.append(val)
            elif ltype == 'current_q':
                row[label] = current_q_avg
                if current_q_avg is not None:
                    forecast_vals.append(current_q_avg)
            elif ltype == 'forecast':
                # Find which forecast month index this is
                fc_idx = sum(1 for lt in label_types[:i] if lt == 'forecast')
                if fc_idx < len(monthly_spreads):
                    spread = monthly_spreads[fc_idx]
                    val = round(latest_close * (1 + spread), 2)
                    row[label] = val
                    forecast_vals.append(val)
                else:
                    row[label] = None

        # FY average: weighted by number of months each value represents
        # Completed quarters: 3 months each, current quarter: ~3 months,
        # forecast months: 1 month each
        if forecast_vals:
            row['FY'] = round(sum(forecast_vals) / len(forecast_vals), 2)
        else:
            row['FY'] = None

        scenarios[scenario] = row

    # ── Weighted average row ──
    weighted = {}
    for label in labels:
        val = 0.0
        has_all = True
        for sc, w in SCENARIO_WEIGHTS.items():
            sc_val = scenarios.get(sc, {}).get(label)
            if sc_val is not None:
                val += w * sc_val
            else:
                has_all = False
                break
        weighted[label] = round(val, 2) if has_all else None

    # FY weighted average
    fy_val = 0.0
    fy_ok = True
    for sc, w in SCENARIO_WEIGHTS.items():
        sc_fy = scenarios.get(sc, {}).get('FY')
        if sc_fy is not None:
            fy_val += w * sc_fy
        else:
            fy_ok = False
            break
    weighted['FY'] = round(fy_val, 2) if fy_ok else None

    scenarios['Weighted Avg'] = weighted

    return scenarios


def _fetch_forecasts():
    """Build complete dynamic forecast dataset."""
    try:
        time_ctx = _get_time_context()
        historical, actuals = _fetch_all_data(time_ctx)

        if not actuals:
            logger.error("No actuals available")
            return None

        # Organize by group
        groups = {}
        for name, (ticker, unit, group) in COMMODITIES.items():
            if name not in actuals:
                continue

            scenarios = _build_scenario_forecasts(name, actuals[name], time_ctx)
            if not scenarios:
                continue

            if group not in groups:
                groups[group] = {'commodities': {}}

            groups[group]['commodities'][name] = {
                'unit': unit,
                'latest_close': actuals[name]['latest_close'],
                'scenarios': scenarios,
                'historical': historical.get(name, []),
            }

        if not groups:
            return None

        # Add group commodity colors
        for group_name, group_data in groups.items():
            group_data['colors'] = GROUP_COMMODITY_COLORS.get(group_name, {})

        now = datetime.utcnow()
        commodities_count = sum(
            len(g['commodities']) for g in groups.values()
        )

        logger.info(
            f"Built dynamic forecasts for {commodities_count} commodities "
            f"across {len(groups)} groups (as of {time_ctx['today']})"
        )

        return {
            'forecast_year': time_ctx['year'],
            'time_context': {
                'today': time_ctx['today'].isoformat(),
                'current_quarter': time_ctx['current_quarter'],
                'labels': time_ctx['labels'],
                'label_types': time_ctx['label_types'],
                'year_end_label': f"FY {time_ctx['year']}",
            },
            'scenario_weights': SCENARIO_WEIGHTS,
            'scenario_labels': SCENARIO_LABELS,
            'scenario_colors': SCENARIO_COLORS,
            'groups': groups,
            'meta': {
                'source': 'ParraMacro Commodities Forecast',
                'data_source': f'yfinance ({HISTORY_YEARS}yr history + YTD {time_ctx["year"]})',
                'method': 'Scenario spread-based forecasts with monthly interpolation',
                'baseline': 'Latest close price',
                'commodities_count': commodities_count,
                'last_updated': now.isoformat(),
            }
        }

    except Exception as e:
        logger.error(f"Forecast fetch failed: {e}")
        return None


# ── Public API ───────────────────────────────────────────────────────────────

def get_forecast_data():
    """Public API: returns cached commodity forecast data."""
    return _cache.get()
