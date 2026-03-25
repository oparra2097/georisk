"""
Commodities Forecast data source.

Fetches Q1 actual prices from yfinance, applies scenario spread
assumptions to produce Q2-Q4 forecasts, and computes weighted averages.
Thread-safe cache with 24-hour TTL.
"""

import threading
import time
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

CACHE_TTL = 86400   # 24 hours
RETRY_BACKOFF = 3600  # 1 hour after failure

# ── Forecast Configuration ──────────────────────────────────────────────────

FORECAST_YEAR = 2026

QUARTERS = {
    'Q1': (date(2026, 1, 1), date(2026, 3, 31)),
    'Q2': (date(2026, 4, 1), date(2026, 6, 30)),
    'Q3': (date(2026, 7, 1), date(2026, 9, 30)),
    'Q4': (date(2026, 10, 1), date(2026, 12, 31)),
}

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

# Scenario spread assumptions: % change vs Q1 actual per quarter
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
    return {
        'forecast_year': FORECAST_YEAR,
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

def _fetch_q1_actuals():
    """Fetch Q1 average prices from yfinance for all commodities."""
    import yfinance as yf

    q1_start, q1_end = QUARTERS['Q1']
    tickers = list(set(t for t, _, _ in COMMODITIES.values()))

    logger.info(f"Fetching Q1 {FORECAST_YEAR} actuals for {len(tickers)} tickers from yfinance")

    try:
        data = yf.download(
            tickers,
            start=q1_start.isoformat(),
            end=(date(q1_end.year, q1_end.month + 1, 1) if q1_end.month < 12
                 else date(q1_end.year + 1, 1, 1)).isoformat(),
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:
        logger.error(f"yfinance download failed: {e}")
        return {}

    actuals = {}
    for name, (ticker, unit, group) in COMMODITIES.items():
        try:
            if len(tickers) == 1:
                series = data['Close'].dropna()
            else:
                series = data['Close'][ticker].dropna()

            if len(series) == 0:
                logger.warning(f"No Q1 data for {name} ({ticker})")
                continue

            q1_avg = round(float(series.mean()), 2)
            actuals[name] = q1_avg
            logger.info(f"  {name}: Q1 avg = {q1_avg} {unit}")
        except Exception as e:
            logger.warning(f"Failed to get Q1 actual for {name} ({ticker}): {e}")

    return actuals


def _build_scenario_prices(name, q1_actual):
    """Apply scenario spreads to Q1 actual to get Q2-Q4 forecast prices."""
    spreads = SCENARIO_SPREADS.get(name)
    if not spreads:
        return None

    scenarios = {}

    # Actual row
    scenarios['Actual'] = {
        'Q1': q1_actual,
        'Q2': None,
        'Q3': None,
        'Q4': None,
        'FY': q1_actual,
    }

    # Scenario rows
    for scenario, q_spreads in spreads.items():
        prices = {}
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            spread = q_spreads.get(q, 0.0)
            prices[q] = round(q1_actual * (1 + spread), 2)
        vals = [v for v in prices.values() if v is not None]
        prices['FY'] = round(sum(vals) / len(vals), 2) if vals else None
        scenarios[scenario] = prices

    # Weighted average
    weighted = {}
    for q in ['Q1', 'Q2', 'Q3', 'Q4']:
        weighted[q] = round(sum(
            SCENARIO_WEIGHTS[sc] * scenarios[sc][q]
            for sc in SCENARIO_WEIGHTS
        ), 2)
    vals = [v for v in weighted.values() if v is not None]
    weighted['FY'] = round(sum(vals) / len(vals), 2) if vals else None
    scenarios['Weighted Avg'] = weighted

    return scenarios


def _fetch_forecasts():
    """Build complete forecast dataset."""
    try:
        q1_actuals = _fetch_q1_actuals()
        if not q1_actuals:
            logger.error("No Q1 actuals available")
            return None

        # Organize by group
        groups = {}
        for name, (ticker, unit, group) in COMMODITIES.items():
            if name not in q1_actuals:
                continue

            q1 = q1_actuals[name]
            scenarios = _build_scenario_prices(name, q1)
            if not scenarios:
                continue

            if group not in groups:
                groups[group] = {'commodities': {}}

            groups[group]['commodities'][name] = {
                'unit': unit,
                'scenarios': scenarios,
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

        logger.info(f"Built forecasts for {commodities_count} commodities across {len(groups)} groups")

        return {
            'forecast_year': FORECAST_YEAR,
            'scenario_weights': SCENARIO_WEIGHTS,
            'scenario_labels': SCENARIO_LABELS,
            'scenario_colors': SCENARIO_COLORS,
            'groups': groups,
            'meta': {
                'source': 'ParraMacro Commodities Forecast',
                'q1_source': f'yfinance (Jan-Mar {FORECAST_YEAR} daily averages)',
                'method': 'Scenario spread-based forecasts',
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
