"""
Commodities Forecast data source.

Dynamic, date-aware forecast engine:
  - Detects today's date to determine completed/current quarters
  - Fetches YTD actual prices from yfinance (live data)
  - Computes current quarter-end estimate from partial data
  - Forecasts next 4 quarters using absolute price targets (rolling into next year)
  - Calculates FY weighted averages for current and next calendar year
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
            'Base Case':   'Hormuz partially restored · OPEC+ delivers 2027 surplus · Brent slides $73-80 · WTI $70-76',
            'Severe Case': 'Hormuz choked points persist · Shadow fleet attrition · Brent $90-100 · Russia refining losses compound',
            'Worst Case':  'Iran closes Hormuz · Saudi/UAE infrastructure strikes · Brent >$120 then settles $105-115',
        },
        'colors': {
            'Actual':       '#94a3b8',
            'Base Case':    '#3b82f6',
            'Severe Case':  '#f59e0b',
            'Worst Case':   '#ef4444',
        },
        'scenario_order': ['Actual', 'Base Case', 'Severe Case', 'Worst Case'],
    },
    'Agriculture': {
        'weights': {
            'Bear': 0.25,
            'Base': 0.50,
            'Bull': 0.25,
        },
        'labels': {
            'Bear':  'Brazil 26/27 coffee crop record (~70M bags) · US grain stocks rebuild · El Niño mild · Prices normalise lower',
            'Base':  'El Niño peaks Nov 2026-Feb 2027 then fades · Mixed harvests · Brazil supply offsets West Africa shortfall',
            'Bull':  'Very strong El Niño (81% prob per CPC) · W. Africa cocoa 25-30% below · Australia wheat shock · Export restrictions',
        },
        'colors': {
            'Actual':       '#94a3b8',
            'Bear':         '#3b82f6',
            'Base':         '#10b981',
            'Bull':         '#ef4444',
        },
        'scenario_order': ['Actual', 'Bear', 'Base', 'Bull'],
    },
    'Metals': {
        'weights': {
            'Bear': 0.25,
            'Base': 0.50,
            'Bull': 0.25,
        },
        'labels': {
            'Bear':  'Gold cycle tops · Fed stays hawkish longer · Dollar rallies · CB buying slows · Copper demand softens',
            'Base':  'Fed easing resumes · CB gold buying ~1000t/yr · UBS gold path $4.6→5.4K · Copper $6.50/lb · Industrial normalisation',
            'Bull':  'Debasement accelerates · Real rates deep negative · China stimulus revives copper · Silver squeeze on solar demand',
        },
        'colors': {
            'Actual':       '#94a3b8',
            'Bear':         '#3b82f6',
            'Base':         '#10b981',
            'Bull':         '#ef4444',
        },
        'scenario_order': ['Actual', 'Bear', 'Base', 'Bull'],
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
    'Silver':           ('SI=F',  '$/troy oz',  'Metals'),
    'Platinum':         ('PL=F',  '$/troy oz',  'Metals'),
    'Aluminum':         ('ALI=F', '$/MT',       'Metals'),
}

# ── Scenario Price Targets ──────────────────────────────────────────────────
# Absolute quarterly price targets per scenario.
# Scenario NAMES must match the keys in GROUP_SCENARIOS[group]['weights'].
# Q4 → forecast Q+1 (Oct-Dec 2026). Q1/Q2/Q3 → 2027 forecast quarters.
#
# LAST REFRESH: 2026-10-16 — anchored on October-vintage published research:
#   • Goldman Sachs Commodities Weekly (Jul/Sep 2026)
#   • JPMorgan Global Research (Jul cut on gold; Sep refresh on oil)
#   • UBS Global Wealth Management gold path ($4.6K Dec → $5.4K Sep 27)
#   • EIA STEO + NGSA Winter Outlook (Permian supply surge story)
#   • USDA WASDE October 2026 (soybean prod raised, wheat stocks up)
#   • Conab / USDA FAS Brazil 26/27 coffee (record 67-76M bags)
#   • Reuters cocoa poll (NY $6K/t YE); CPC El Niño 81% prob Oct-Dec
#   • Trading Economics / Kitco spot (Oct 15, 2026)

SCENARIO_TARGETS = {
    # ═══════════════════════════════════════════════════════════════════════════
    # OIL & GAS — Hormuz tail-risk fading · 2027 surplus (IEA 3.2-3.7mbd) ·
    # Russia refining losses · Permian gas glut.
    # Goldman Q4 2026: Brent $80, WTI $76. 2027 avg Brent $75, WTI $70.
    # JPM 2026 avg Brent $96 (incl. Hormuz blow-up months), WTI $89. 2027 $75/$70.
    # ═══════════════════════════════════════════════════════════════════════════
    'WTI Crude': {
        # Spot ~$82. Goldman 2027 avg $70. Bear tail: JPM "$30s risk" if OPEC break.
        'Base Case':   {'Q4': 76,  'Q1': 72,  'Q2': 70,  'Q3': 68},
        'Severe Case': {'Q4': 92,  'Q1': 92,  'Q2': 85,  'Q3': 80},
        'Worst Case':  {'Q4': 115, 'Q1': 118, 'Q2': 108, 'Q3': 100},
    },
    'Brent Crude': {
        # Spot ~$86. Goldman Q4 $80 / 2027 avg $75. JPM 2026 $96, 2027 $75.
        'Base Case':   {'Q4': 80,  'Q1': 76,  'Q2': 73,  'Q3': 72},
        'Severe Case': {'Q4': 96,  'Q1': 98,  'Q2': 90,  'Q3': 85},
        'Worst Case':  {'Q4': 120, 'Q1': 123, 'Q2': 112, 'Q3': 105},
    },
    'Natural Gas (HH)': {
        # Spot ~$2.88 Sep. EIA $3.43 avg 2026, $3.28 2027. EIR cut 2027 to $3.25.
        # Permian supply surge > LNG demand growth. Winter forwards $3.43.
        'Base Case':   {'Q4': 3.60, 'Q1': 4.00, 'Q2': 3.20, 'Q3': 3.40},
        'Severe Case': {'Q4': 4.40, 'Q1': 5.00, 'Q2': 3.80, 'Q3': 4.10},
        'Worst Case':  {'Q4': 5.80, 'Q1': 7.00, 'Q2': 4.80, 'Q3': 5.20},
    },
    'TTF Gas': {
        # Spot €81-85 Oct 15. Storage 73% vs 90% norm. Citi bear €56 Q4.
        # Bull range €92 cold-snap. 2026 refill target cut from 90% to 80%.
        'Base Case':   {'Q4': 78,  'Q1': 82,  'Q2': 68,  'Q3': 60},
        'Severe Case': {'Q4': 95,  'Q1': 105, 'Q2': 85,  'Q3': 75},
        'Worst Case':  {'Q4': 130, 'Q1': 150, 'Q2': 110, 'Q3': 95},
    },
    # ═══════════════════════════════════════════════════════════════════════════
    # AGRICULTURE — El Niño peak Nov 2026-Feb 2027 (81% prob per CPC).
    # October WASDE raised US soybean prod (4.56Bbu, +0.3 yield) + wheat stocks → bearish.
    # Record Brazil 26/27 coffee crop 67-76M bags (Conab-Marex range).
    # Ivory Coast 26/27 cocoa 1.77-1.8Mt (−11-18% YoY); Ghana 580-650k.
    # ═══════════════════════════════════════════════════════════════════════════
    'Cocoa': {
        # Spot ~$5,500 Oct. Reuters Jul poll: NY $6,000/t YE. CI/Ghana supply down.
        'Bear':  {'Q4': 4500, 'Q1': 4000, 'Q2': 3800, 'Q3': 3600},
        'Base':  {'Q4': 5500, 'Q1': 5700, 'Q2': 5300, 'Q3': 4800},
        'Bull':  {'Q4': 7000, 'Q1': 7500, 'Q2': 7000, 'Q3': 6300},
    },
    'Wheat': {
        # Dec futures 662-670¢/bu Oct. Oct WASDE raised US stocks (bearish).
        # Bull: Australia -9Mt El Niño + Black Sea.
        'Bear':  {'Q4': 580, 'Q1': 560, 'Q2': 550, 'Q3': 540},
        'Base':  {'Q4': 670, 'Q1': 680, 'Q2': 670, 'Q3': 650},
        'Bull':  {'Q4': 820, 'Q1': 880, 'Q2': 850, 'Q3': 800},
    },
    'Soybeans': {
        # Oct WASDE prod 4.56Bbu +0.3 yield · $11.40 season avg. Futures ~1030-1060¢.
        'Bear':  {'Q4': 1020, 'Q1': 1000, 'Q2': 980,  'Q3': 960},
        'Base':  {'Q4': 1100, 'Q1': 1120, 'Q2': 1140, 'Q3': 1160},
        'Bull':  {'Q4': 1280, 'Q1': 1350, 'Q2': 1370, 'Q3': 1350},
    },
    'Coffee': {
        # Dec arabica 295-308¢/lb Oct. Record Brazil crop 67-76M bags → bearish 2027.
        # Robusta ICE stocks 4-month high → extra downside pressure.
        'Bear':  {'Q4': 250, 'Q1': 240, 'Q2': 230, 'Q3': 220},
        'Base':  {'Q4': 295, 'Q1': 290, 'Q2': 280, 'Q3': 270},
        'Bull':  {'Q4': 380, 'Q1': 410, 'Q2': 400, 'Q3': 370},
    },
    # ═══════════════════════════════════════════════════════════════════════════
    # METALS — Gold reset (JPM cut Q4 to $4,500 from $6,000 on Jul 3) ·
    # UBS path $4.6K Dec → $5.4K Sep 27 · Copper record $6.89 Sep 9 (spot $6.53-6.66).
    # Platinum weak (spot ~$1,600 Oct 15, 34-week low on aluminum).
    # ═══════════════════════════════════════════════════════════════════════════
    'Copper': {
        # Spot $6.53-6.66/lb Oct. Record $6.89 Sep 9. Goldman $10-11k/t range.
        # Structural EV/grid demand + Chile/Peru supply tight.
        'Bear':  {'Q4': 5.80, 'Q1': 5.60, 'Q2': 5.40, 'Q3': 5.20},
        'Base':  {'Q4': 6.50, 'Q1': 6.70, 'Q2': 6.80, 'Q3': 7.00},
        'Bull':  {'Q4': 7.50, 'Q1': 8.00, 'Q2': 8.20, 'Q3': 8.50},
    },
    'Gold': {
        # JPM Q4 2026 $4,500 (July cut from $6,000). Goldman $4,650-4,900.
        # UBS Dec $4,600 → Mar 27 $5,000 → Sep 27 $5,400.
        'Bear':  {'Q4': 4100, 'Q1': 4200, 'Q2': 4300, 'Q3': 4400},
        'Base':  {'Q4': 4650, 'Q1': 4850, 'Q2': 5100, 'Q3': 5300},
        'Bull':  {'Q4': 5200, 'Q1': 5500, 'Q2': 5800, 'Q3': 6000},
    },
    'Silver': {
        # JPM: Q4 2026 $63/oz, 2026 avg $70, 2027 avg $63. Solar demand sustains.
        'Bear':  {'Q4': 48, 'Q1': 50, 'Q2': 52, 'Q3': 54},
        'Base':  {'Q4': 63, 'Q1': 65, 'Q2': 66, 'Q3': 63},
        'Bull':  {'Q4': 80, 'Q1': 85, 'Q2': 90, 'Q3': 95},
    },
    'Platinum': {
        # Spot $1,597-1,629 Oct 15 (fell 8.65% in 4 weeks). JPM $1,800 YE.
        # SA power recovery + weak auto demand drag; H2 economy slow.
        'Bear':  {'Q4': 1450, 'Q1': 1500, 'Q2': 1550, 'Q3': 1580},
        'Base':  {'Q4': 1700, 'Q1': 1800, 'Q2': 1850, 'Q3': 1900},
        'Bull':  {'Q4': 2050, 'Q1': 2150, 'Q2': 2200, 'Q3': 2300},
    },
    'Aluminum': {
        # Spot $2,991/t Oct 15 — 34-week low. Goldman bear $2,350 late-26.
        # TD $3,700 Q4 → $2,900 Q3 27. Yunnan hydro restarts weighing.
        'Bear':  {'Q4': 2600, 'Q1': 2500, 'Q2': 2450, 'Q3': 2400},
        'Base':  {'Q4': 3000, 'Q1': 3100, 'Q2': 3050, 'Q3': 3000},
        'Bull':  {'Q4': 3600, 'Q1': 3800, 'Q2': 3900, 'Q3': 4000},
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
        'Copper':   '#f97316',
        'Gold':     '#eab308',
        'Silver':   '#94a3b8',
        'Platinum': '#a78bfa',
        'Aluminum': '#06b6d4',
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

    # Pre-compute shared timing info — applies to every commodity even when
    # yfinance stalls on its ticker, so the builder can still populate the
    # current-quarter column.
    cq = time_ctx['current_quarter']
    cq_start, cq_end = quarters[cq]
    shared_qtd_elapsed  = max(0, (today - cq_start).days)
    shared_qtd_in_q     = max(1, (cq_end - cq_start).days + 1)

    for name, (ticker, unit, group) in COMMODITIES.items():
        series = _extract_series(data, ticker, len(tickers))

        # Resilient path: if yfinance returned nothing for this ticker
        # (common for newer / thinly traded contracts like ALI=F),
        # keep the commodity in the response with an empty actuals dict
        # so the frontend subview can still render scenario targets +
        # the placeholder targets table. The scenario rows will fall
        # back to hardcoded values.
        if series is None:
            logger.warning(f"No data for {name} ({ticker}) — keeping with empty actuals")
            historical[name] = []
            actuals[name] = {
                'completed': {},
                'latest_close': None,
                'current_q_avg': None,
                'qtd_days_elapsed': shared_qtd_elapsed,
                'qtd_days_in_quarter': shared_qtd_in_q,
            }
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
            logger.warning(f"No YTD data for {name} ({ticker}) — keeping with empty actuals")
            actuals[name] = {
                'completed': {},
                'latest_close': None,
                'current_q_avg': None,
                'qtd_days_elapsed': shared_qtd_elapsed,
                'qtd_days_in_quarter': shared_qtd_in_q,
            }
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

        # Current quarter partial average (raw mean of QTD closes)
        cq_data = ytd_series[cq_start.isoformat():]
        if len(cq_data) > 0:
            result['current_q_avg'] = round(float(cq_data.mean()), 2)

        # Days-elapsed / days-in-quarter — shared across commodities but stored
        # per-commodity so the builder has everything it needs in one dict.
        result['qtd_days_elapsed'] = shared_qtd_elapsed
        result['qtd_days_in_quarter'] = shared_qtd_in_q

        actuals[name] = result
        logger.info(
            f"  {name}: latest={result['latest_close']} {unit}, "
            f"current_q_avg={result.get('current_q_avg', 'N/A')}, "
            f"hist_quarters={len(hist_records)}"
        )

    return historical, actuals


# Mapping from group scenario name → percentile field returned by the
# SARIMAX + GARCH model in commodity_models.get_model_forecast.
# Oil & Gas: 3-tier disruption gradient (higher price = worse for consumers).
# Agriculture / Metals: symmetric Bear/Base/Bull around the median.
_SCENARIO_TO_PERCENTILE = {
    'Oil & Gas':   {'Base Case': 'median', 'Severe Case': 'p90', 'Worst Case': 'p97_5'},
    'Agriculture': {'Bear': 'p2_5', 'Base': 'median', 'Bull': 'p97_5'},
    'Metals':      {'Bear': 'p2_5', 'Base': 'median', 'Bull': 'p97_5'},
}


def _model_targets_for_commodity(name, group_name, forecast_quarters,
                                  qtd_mean=None, days_elapsed=0, days_in_quarter=90):
    """
    Build a dict shaped like SCENARIO_TARGETS[name] populated from the
    SARIMAX + GARCH model. Also returns the model's blended nowcast for
    the current quarter. Returns (None, None) on any failure so the caller
    can fall back to the hardcoded targets.

    Returns:
        (targets_dict, nowcast_value, forward_curve_dict) — any element may
        be None independently. forward_curve_dict is keyed by calendar
        quarter (``'Q3'`` etc.) so the API can plot it alongside the
        scenario rows without further translation.
    """
    try:
        from backend.data_sources import commodity_models
    except Exception as e:
        logger.debug(f'commodity_models import failed ({e}); using hardcoded targets')
        return None, None, None

    scenario_map = _SCENARIO_TO_PERCENTILE.get(group_name)
    if not scenario_map:
        return None, None, None

    try:
        result = commodity_models.get_model_forecast(
            name,
            qtd_mean=qtd_mean,
            days_elapsed=days_elapsed,
            days_in_quarter=days_in_quarter,
        )
    except Exception as e:
        logger.warning(f'{name}: model forecast crashed: {e}')
        return None, None, None, None
    if not result or not result.get('forecast'):
        return None, None, None, None

    fc = result['forecast']  # {'Q+1': {median, p2_5, p10, p90, p97_5, label}, ...}
    nowcast_val = result.get('nowcast')

    # Re-key anchor quarters from Q+i → calendar Q{fq_num} so the frontend
    # can align them with the scenario rows directly.
    def _rekey(raw: dict) -> dict[str, float]:
        out: dict[str, float] = {}
        for i, (_fy, fq_num, _label) in enumerate(forecast_quarters):
            cv = raw.get(f'Q+{i + 1}')
            if cv and 'mean_price' in cv:
                out[f'Q{fq_num}'] = round(float(cv['mean_price']), 2)
        return out

    forward_curve  = _rekey(result.get('forward_curve') or {})
    long_run_trend = _rekey(result.get('long_run_trend') or {})

    # Map forecast index to calendar-quarter key used by SCENARIO_TARGETS
    # (lookup in _build_scenario_forecasts is by `Q{fq_num}` where fq_num ∈ 1..4).
    targets = {scenario: {} for scenario in scenario_map}
    for i, (_fy, fq_num, _label) in enumerate(forecast_quarters):
        bucket = fc.get(f'Q+{i + 1}')
        if not bucket:
            continue
        q_key = f'Q{fq_num}'
        for scenario, percentile in scenario_map.items():
            val = bucket.get(percentile)
            if val is not None:
                targets[scenario][q_key] = round(float(val), 2)

    # If the model produced no usable numbers, signal fallback
    if not any(q for q in targets.values()):
        return None, nowcast_val, forward_curve or None, long_run_trend or None
    return (targets, nowcast_val,
            (forward_curve or None), (long_run_trend or None))


def _build_scenario_forecasts(name, actual_data, time_ctx, group_name):
    """
    Build scenario-based forecasts for a single commodity.

    Tries the SARIMAX + GARCH model (commodity_models.get_model_forecast)
    first; falls back to the hardcoded SCENARIO_TARGETS if the model fails,
    is stale, or does not cover this commodity. For actual / current_q
    columns always uses live data.

    Returns a dict of scenario -> {label: price} for all time labels + FY,
    plus a `_source` marker ('model' or 'hardcoded') used by the meta block.
    """
    targets_cfg, nowcast_val, forward_curve, long_run_trend = _model_targets_for_commodity(
        name, group_name, time_ctx['forecast_quarters'],
        qtd_mean=actual_data.get('current_q_avg'),
        days_elapsed=actual_data.get('qtd_days_elapsed', 0),
        days_in_quarter=actual_data.get('qtd_days_in_quarter', 90),
    )
    source = 'model'
    if not targets_cfg:
        targets_cfg = SCENARIO_TARGETS.get(name)
        source = 'hardcoded'
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
                # Blended nowcast (QTD mean × elapsed_weight + model Q+0 ×
                # remaining_weight) for scenario rows. Falls back to raw
                # QTD mean if the model path didn't yield a nowcast.
                cq_val = nowcast_val if nowcast_val is not None else current_q_avg
                row[label] = round(float(cq_val), 2) if cq_val is not None else None
                if cq_val is not None:
                    fy_parts.append(cq_val)

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

    return scenarios, source, forward_curve, long_run_trend


def _fetch_forecasts():
    """Build complete dynamic forecast dataset."""
    try:
        time_ctx = _get_time_context()
        historical, actuals = _fetch_all_data(time_ctx)

        if not actuals:
            logger.error("No actuals available")
            return None

        # Pull market consensus once (24h-cached inside the tracker).
        try:
            from backend.data_sources import consensus_tracker
            consensus_data = consensus_tracker.get_consensus_data()
        except Exception as e:
            logger.warning(f'consensus fetch failed: {e}')
            consensus_data = {}

        # Organize by group
        groups = {}
        source_counts = {'model': 0, 'hardcoded': 0}
        for name, (ticker, unit, group) in COMMODITIES.items():
            if name not in actuals:
                continue

            built = _build_scenario_forecasts(
                name, actuals[name], time_ctx, group
            )
            if not built:
                continue
            scenarios, forecast_source, forward_curve, long_run_trend = built
            source_counts[forecast_source] = source_counts.get(forecast_source, 0) + 1

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
                'ticker': ticker,
                'unit': unit,
                'latest_close': actuals[name]['latest_close'],
                'scenarios': scenarios,
                'historical': historical.get(name, []),
                'forecast_source': forecast_source,
                'forward_curve': forward_curve,
                'long_run_trend': long_run_trend,
                'consensus': consensus_data.get(name, []),
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
                'method': (
                    'Hybrid SARIMAX(1,0,1) + GARCH(1,1) with 95% CI bootstrap · '
                    '4-quarter rolling forecast · hardcoded scenario targets as fallback'
                ),
                'baseline': 'Live YTD close prices via yfinance',
                'commodities_count': commodities_count,
                'forecast_sources': source_counts,
                'consensus_sources': sorted({
                    entry['source']
                    for entries in consensus_data.values()
                    for entry in entries
                }),
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
