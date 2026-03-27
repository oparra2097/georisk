"""
Commodities Forecast data source.

Dynamic, date-aware forecast engine:
  - Detects today's date to determine completed/current quarters
  - Fetches YTD actual prices from yfinance (live data)
  - Computes current quarter-end estimate from partial data
  - Forecasts next 3 quarters using scenario spread assumptions (rolling)
  - Calculates year-end (FY) weighted average for current calendar year
  - Per-group scenario frameworks (geopolitical, supply/weather, speculative)
  - Thread-safe cache with 24-hour TTL

Groups & Scenarios:
  Oil & Gas — Geopolitical: Base Case 70% | Severe Case 20% | Worst Case 10%
  Agriculture — Supply/Weather: Bear 25% | Base 50% | Bull 25%
  Metals — Speculative/Macro: Bear 25% | Base 50% | Bull 25%
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

# ── Per-Group Scenario Configuration ───────────────────────────────────────
# Each group has its own scenario names, weights, labels, and colors.

GROUP_SCENARIOS = {
    'Oil & Gas': {
        'weights': {
            'Worst Case': 0.10,
            'Severe Case': 0.20,
            'Base Case': 0.70,
        },
        'labels': {
            'Base Case':   'Gradual de-escalation · OPEC+ discipline · Brent reverts to ~$78 by Q4',
            'Severe Case': 'Hormuz closed through year-end · No ceasefire · Brent $110-115 sustained',
            'Worst Case':  'Iran targets critical ME production · Brent >$130 peak · stays $110s',
        },
        'colors': {
            'Actual':       '#94a3b8',
            'Base Case':    '#3b82f6',
            'Severe Case':  '#f59e0b',
            'Worst Case':   '#ef4444',
            'Weighted Avg': '#10b981',
        },
        'scenario_order': ['Actual', 'Base Case', 'Severe Case', 'Worst Case', 'Weighted Avg'],
    },
    'Agriculture': {
        'weights': {
            'Bear': 0.25,
            'Base': 0.50,
            'Bull': 0.25,
        },
        'labels': {
            'Bear':  'Bumper harvest globally · Favourable weather · Ample supply depresses prices',
            'Base':  'Normal seasonal patterns · Trend-line yields · Steady demand',
            'Bull':  'Major drought in key growing regions · Supply shock · Export restrictions',
        },
        'colors': {
            'Actual':       '#94a3b8',
            'Bear':         '#3b82f6',
            'Base':         '#10b981',
            'Bull':         '#ef4444',
            'Weighted Avg': '#f59e0b',
        },
        'scenario_order': ['Actual', 'Bear', 'Base', 'Bull', 'Weighted Avg'],
    },
    'Metals': {
        'weights': {
            'Bear': 0.25,
            'Base': 0.50,
            'Bull': 0.25,
        },
        'labels': {
            'Bear':  'Risk-off environment · Dollar strength · Demand slowdown · De-leveraging',
            'Base':  'Steady macro · Moderate central bank buying · Gradual industrial recovery',
            'Bull':  'Flight to safety · Speculative inflows · Central bank accumulation · Gold $6200+',
        },
        'colors': {
            'Actual':       '#94a3b8',
            'Bear':         '#3b82f6',
            'Base':         '#10b981',
            'Bull':         '#ef4444',
            'Weighted Avg': '#f59e0b',
        },
        'scenario_order': ['Actual', 'Bear', 'Base', 'Bull', 'Weighted Avg'],
    },
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

# ── Scenario Price Targets ──────────────────────────────────────────────────
# Absolute quarterly price targets per scenario.
# Q1 values: None = use live YTD actual (current quarter only);
#            number = used for next-year Q1 forecasts.
# When Q1 is the current quarter the engine auto-fills from live data.
# Scenario NAMES must match the keys in GROUP_SCENARIOS[group]['weights'].

SCENARIO_TARGETS = {
    # ═══════════════════════════════════════════════════════════════════════════
    # OIL & GAS — Geopolitical scenarios
    # Brent: Base → $95-100 Q2, reverts to $75-80 Q4
    # WTI tracks Brent with ~$5-7 discount
    # TTF: Elevated due to Qatar bombing / longer-term production loss
    # ═══════════════════════════════════════════════════════════════════════════
    'WTI Crude': {
        'Base Case':   {'Q1': 68,  'Q2': 95,  'Q3': 83,  'Q4': 72},
        'Severe Case': {'Q1': 90,  'Q2': 108, 'Q3': 104, 'Q4': 94},
        'Worst Case':  {'Q1': 102, 'Q2': 124, 'Q3': 117, 'Q4': 107},
    },
    'Brent Crude': {
        'Base Case':   {'Q1': 74,  'Q2': 100, 'Q3': 87,  'Q4': 78},
        'Severe Case': {'Q1': 96,  'Q2': 113, 'Q3': 110, 'Q4': 99},
        'Worst Case':  {'Q1': 108, 'Q2': 130, 'Q3': 121, 'Q4': 112},
    },
    'Natural Gas (HH)': {
        'Base Case':   {'Q1': 3.80, 'Q2': 3.20, 'Q3': 3.40, 'Q4': 3.60},
        'Severe Case': {'Q1': 4.50, 'Q2': 4.00, 'Q3': 4.40, 'Q4': 4.25},
        'Worst Case':  {'Q1': 5.70, 'Q2': 4.90, 'Q3': 5.80, 'Q4': 5.50},
    },
    'TTF Gas': {
        # Elevated — Qatar production loss, structural supply deficit
        'Base Case':   {'Q1': 68,  'Q2': 60,  'Q3': 64,  'Q4': 66},
        'Severe Case': {'Q1': 84,  'Q2': 76,  'Q3': 87,  'Q4': 81},
        'Worst Case':  {'Q1': 112, 'Q2': 98,  'Q3': 119, 'Q4': 108},
    },
    # ═══════════════════════════════════════════════════════════════════════════
    # AGRICULTURE — Supply / Weather scenarios
    # Bear = bumper harvest, oversupply depresses prices
    # Bull = drought in key regions, supply shock, export restrictions
    # ═══════════════════════════════════════════════════════════════════════════
    'Cocoa': {
        'Bear':  {'Q1': 2650, 'Q2': 2780, 'Q3': 2590, 'Q4': 2690},
        'Base':  {'Q1': 3050, 'Q2': 3065, 'Q3': 3000, 'Q4': 3100},
        'Bull':  {'Q1': 4100, 'Q2': 3475, 'Q3': 3790, 'Q4': 3950},
    },
    'Wheat': {
        'Bear':  {'Q1': 535, 'Q2': 555, 'Q3': 530, 'Q4': 545},
        'Base':  {'Q1': 620, 'Q2': 615, 'Q3': 622, 'Q4': 628},
        'Bull':  {'Q1': 700, 'Q2': 676, 'Q3': 736, 'Q4': 712},
    },
    'Soybeans': {
        'Bear':  {'Q1': 1055, 'Q2': 1090, 'Q3': 1045, 'Q4': 1070},
        'Base':  {'Q1': 1200, 'Q2': 1185, 'Q3': 1195, 'Q4': 1207},
        'Bull':  {'Q1': 1320, 'Q2': 1277, 'Q3': 1370, 'Q4': 1335},
    },
    'Coffee': {
        'Bear':  {'Q1': 265, 'Q2': 277, 'Q3': 259, 'Q4': 271},
        'Base':  {'Q1': 305, 'Q2': 307, 'Q3': 304, 'Q4': 310},
        'Bull':  {'Q1': 400, 'Q2': 346, 'Q3': 376, 'Q4': 391},
    },
    # ═══════════════════════════════════════════════════════════════════════════
    # METALS — Speculative / Macro scenarios
    # Bear = risk-off, dollar strength, demand slowdown
    # Bull = flight to safety, speculative inflows, CB accumulation
    # Gold Base: $5600 year-end, Bull: $6200 year-end
    # ═══════════════════════════════════════════════════════════════════════════
    'Copper': {
        'Bear':  {'Q1': 4.85, 'Q2': 5.03, 'Q3': 4.81, 'Q4': 4.92},
        'Base':  {'Q1': 5.75, 'Q2': 5.63, 'Q3': 5.74, 'Q4': 5.80},
        'Bull':  {'Q1': 6.80, 'Q2': 6.02, 'Q3': 6.45, 'Q4': 6.67},
    },
    'Gold': {
        'Bear':  {'Q1': 4400, 'Q2': 4550, 'Q3': 4380, 'Q4': 4500},
        'Base':  {'Q1': 5700, 'Q2': 5100, 'Q3': 5350, 'Q4': 5600},
        'Bull':  {'Q1': 6400, 'Q2': 5450, 'Q3': 5800, 'Q4': 6200},
    },
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
    """Determine current quarter, completed quarters, and rolling forecast quarters."""
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

    # Rolling forecast: next 4 quarters after current (wraps into next year)
    forecast_quarters = []  # list of (year, quarter_num, display_label)
    for i in range(1, 5):
        fq = current_quarter + i
        fy = year
        if fq > 4:
            fq -= 4
            fy += 1
        # Label: "Q2" if same year, "Q1'27" if next year
        if fy == year:
            label = f'Q{fq}'
        else:
            label = f"Q{fq}'{str(fy)[-2:]}"
        forecast_quarters.append((fy, fq, label))

    # Determine which years are covered → build FY labels
    forecast_years = sorted({fy for fy, _, _ in forecast_quarters})
    all_years = sorted({year} | set(forecast_years))
    next_year = year + 1

    # Build labels array for the API response
    labels = []
    label_types = []

    # Completed quarters in current year (before current)
    for q_label in completed:
        labels.append(q_label)
        label_types.append('actual')

    # Current quarter (with asterisk to indicate estimate)
    labels.append(current_q_label + '*')
    label_types.append('current_q')

    # Next 4 forecast quarters
    for (fy, fq, display_label) in forecast_quarters:
        labels.append(display_label)
        label_types.append('forecast')

    # Year-end labels (one per year covered)
    year_end_labels = [f'FY {y}' for y in all_years]

    return {
        'year': year,
        'next_year': next_year,
        'today': today,
        'quarters': quarters,
        'completed_quarters': completed,
        'current_quarter': current_q_label,
        'current_quarter_num': current_quarter,
        'forecast_quarters': forecast_quarters,
        'labels': labels,
        'label_types': label_types,
        'year_end_labels': year_end_labels,
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


def _build_scenario_forecasts(name, actual_data, time_ctx, group_name):
    """
    Build scenario-based forecasts for a single commodity.

    Uses absolute price targets from SCENARIO_TARGETS (not spreads).
    For actual/current_q columns uses live data; for forecast columns uses
    the fixed model targets.  Q1 targets set to None are auto-filled from
    the current quarter YTD average.

    Returns a dict of scenario -> {label: price} for all time labels + FY.
    """
    targets_cfg = SCENARIO_TARGETS.get(name)
    if not targets_cfg:
        return None

    group_cfg = GROUP_SCENARIOS.get(group_name, {})
    weights = group_cfg.get('weights', {})

    labels = time_ctx['labels']
    label_types = time_ctx['label_types']
    forecast_quarters = time_ctx['forecast_quarters']
    completed = actual_data.get('completed', {})
    current_q_avg = actual_data.get('current_q_avg')
    year = time_ctx['year']
    next_year = time_ctx.get('next_year', year + 1)

    scenarios = {}

    # ── Actual row: only has values for actual/current_q columns ──
    actual_row = {}
    for label, ltype in zip(labels, label_types):
        if ltype == 'actual':
            actual_row[label] = completed.get(label)
        elif ltype == 'current_q':
            actual_row[label] = current_q_avg
        else:
            actual_row[label] = None
    actual_row['FY'] = None
    actual_row['FY2'] = None
    scenarios['Actual'] = actual_row

    # ── Scenario rows (using group-specific scenario names) ──
    for scenario in targets_cfg.keys():
        row = {}
        fy_parts = []    # current year
        fy2_parts = []   # next year

        for i, (label, ltype) in enumerate(zip(labels, label_types)):
            if ltype == 'actual':
                val = completed.get(label)
                row[label] = val
                if val is not None:
                    fy_parts.append(val)

            elif ltype == 'current_q':
                # Use live actual for current quarter
                row[label] = current_q_avg
                if current_q_avg is not None:
                    fy_parts.append(current_q_avg)

            elif ltype == 'forecast':
                fc_idx = sum(1 for lt in label_types[:i] if lt == 'forecast')
                if fc_idx < len(forecast_quarters):
                    fy_q, fq_num, _ = forecast_quarters[fc_idx]
                    # Direct absolute price target lookup
                    target = targets_cfg.get(scenario, {}).get(f'Q{fq_num}')
                    if target is not None:
                        val = round(float(target), 2)
                    else:
                        val = current_q_avg  # fallback for None targets
                    row[label] = val
                    # Bucket into current year or next year FY
                    if val is not None:
                        if fy_q == year:
                            fy_parts.append(val)
                        elif fy_q == next_year:
                            fy2_parts.append(val)
                else:
                    row[label] = None

        row['FY'] = round(sum(fy_parts) / len(fy_parts), 2) if fy_parts else None
        row['FY2'] = round(sum(fy2_parts) / len(fy2_parts), 2) if fy2_parts else None

        scenarios[scenario] = row

    # ── Weighted average row (using group-specific weights) ──
    weighted = {}
    for label in labels:
        val = 0.0
        has_all = True
        for sc, w in weights.items():
            sc_val = scenarios.get(sc, {}).get(label)
            if sc_val is not None:
                val += w * sc_val
            else:
                has_all = False
                break
        weighted[label] = round(val, 2) if has_all else None

    # FY and FY2 weighted averages
    for fy_key in ('FY', 'FY2'):
        fy_val = 0.0
        fy_ok = True
        for sc, w in weights.items():
            sc_fy = scenarios.get(sc, {}).get(fy_key)
            if sc_fy is not None:
                fy_val += w * sc_fy
            else:
                fy_ok = False
                break
        weighted[fy_key] = round(fy_val, 2) if fy_ok else None

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

            scenarios = _build_scenario_forecasts(
                name, actuals[name], time_ctx, group
            )
            if not scenarios:
                continue

            if group not in groups:
                group_cfg = GROUP_SCENARIOS.get(group, {})
                groups[group] = {
                    'commodities': {},
                    'scenario_weights': group_cfg.get('weights', {}),
                    'scenario_labels': group_cfg.get('labels', {}),
                    'scenario_colors': group_cfg.get('colors', {}),
                    'scenario_order': group_cfg.get('scenario_order', []),
                }

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
                'year_end_labels': time_ctx.get('year_end_labels', [f"FY {time_ctx['year']}"]),
            },
            'groups': groups,
            'meta': {
                'source': 'ParraMacro Commodities Forecast',
                'data_source': f'yfinance ({HISTORY_YEARS}yr history + YTD {time_ctx["year"]})',
                'method': 'Scenario spread-based quarterly forecasts',
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
