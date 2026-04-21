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

import json
import os
import threading
import time
import logging
import calendar
import copy
from datetime import datetime, date, timedelta

from config import Config

logger = logging.getLogger(__name__)

CACHE_TTL = 86400   # 24 hours
RETRY_BACKOFF = 3600  # 1 hour after failure
HISTORY_YEARS = 10   # years of historical quarterly data

# Seed file ships with the repo; override file is written by the admin UI and
# lives in DATA_DIR so it survives deploys on Render.
_SEED_SCENARIO_FILE = os.path.join(os.path.dirname(__file__), 'scenario_targets.json')
_OVERRIDE_SCENARIO_FILE = os.path.join(Config.DATA_DIR, 'scenario_targets.json')

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
# GROUP_SCENARIOS and SCENARIO_TARGETS are loaded from scenario_targets.json
# (override in DATA_DIR takes precedence over the shipped seed file).
# The admin UI at /auth/admin edits the override file live; see
# load_scenario_config() / save_scenario_config() below.
_config_lock = threading.RLock()
GROUP_SCENARIOS = {}
SCENARIO_TARGETS = {}


def _load_scenario_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _active_scenario_path():
    """Prefer the override file in DATA_DIR if it exists; else the seed file."""
    if os.path.exists(_OVERRIDE_SCENARIO_FILE):
        return _OVERRIDE_SCENARIO_FILE
    return _SEED_SCENARIO_FILE


def load_scenario_config():
    """
    (Re)load GROUP_SCENARIOS + SCENARIO_TARGETS from disk.
    Falls back to the seed file if the override is missing or malformed.
    """
    global GROUP_SCENARIOS, SCENARIO_TARGETS
    path = _active_scenario_path()
    try:
        cfg = _load_scenario_file(path)
    except Exception as e:
        logger.error(f"Failed to load scenario config from {path}: {e}")
        if path != _SEED_SCENARIO_FILE:
            logger.warning("Falling back to seed scenario_targets.json")
            cfg = _load_scenario_file(_SEED_SCENARIO_FILE)
        else:
            raise
    with _config_lock:
        GROUP_SCENARIOS = cfg.get('groups', {})
        SCENARIO_TARGETS = cfg.get('targets', {})
    logger.info(f"Loaded scenario config from {path}")
    return {'groups': GROUP_SCENARIOS, 'targets': SCENARIO_TARGETS, 'source': path}


def get_scenario_config():
    """Return a deep copy of the current scenario config (safe for editing)."""
    with _config_lock:
        return {
            'groups': copy.deepcopy(GROUP_SCENARIOS),
            'targets': copy.deepcopy(SCENARIO_TARGETS),
            'source': _active_scenario_path(),
            'using_override': os.path.exists(_OVERRIDE_SCENARIO_FILE),
        }


def save_scenario_config(new_cfg):
    """
    Persist a new scenario config to the override file, reload, bust cache.
    Validates weights sum to 1.0 per group and every weighted scenario has
    Q1-Q4 numeric targets for every commodity in that group.
    """
    if not isinstance(new_cfg, dict):
        raise ValueError('Config must be a dict')
    if 'groups' not in new_cfg or 'targets' not in new_cfg:
        raise ValueError('Config must contain "groups" and "targets" keys')

    groups = new_cfg['groups']
    targets = new_cfg['targets']
    if not isinstance(groups, dict) or not isinstance(targets, dict):
        raise ValueError('"groups" and "targets" must be objects')

    for group_name, gcfg in groups.items():
        weights = gcfg.get('weights', {})
        if weights:
            total = sum(float(w) for w in weights.values())
            if abs(total - 1.0) > 0.01:
                raise ValueError(
                    f'Group "{group_name}" weights sum to {total:.3f}, must equal 1.0'
                )
        for scenario_name in weights.keys():
            for comm_name, (_ticker, _unit, comm_group) in COMMODITIES.items():
                if comm_group != group_name:
                    continue
                sc_targets = (targets.get(comm_name, {}) or {}).get(scenario_name)
                if sc_targets is None:
                    raise ValueError(
                        f'Missing targets for {comm_name} / {scenario_name}'
                    )
                for q in ('Q1', 'Q2', 'Q3', 'Q4'):
                    if q not in sc_targets:
                        raise ValueError(
                            f'Missing {q} target for {comm_name} / {scenario_name}'
                        )
                    try:
                        float(sc_targets[q])
                    except (TypeError, ValueError):
                        raise ValueError(
                            f'Target for {comm_name} / {scenario_name} / {q} is not numeric'
                        )

    os.makedirs(Config.DATA_DIR, exist_ok=True)
    tmp = _OVERRIDE_SCENARIO_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump({'groups': groups, 'targets': targets}, f, indent=2, ensure_ascii=False)
    os.replace(tmp, _OVERRIDE_SCENARIO_FILE)

    load_scenario_config()
    _cache.clear()
    logger.info(
        f"Saved scenario overrides to {_OVERRIDE_SCENARIO_FILE} and cleared forecast cache"
    )
    return get_scenario_config()


def reset_scenario_config():
    """Delete the override file, revert to the seed, and bust the cache."""
    if os.path.exists(_OVERRIDE_SCENARIO_FILE):
        os.remove(_OVERRIDE_SCENARIO_FILE)
        logger.info(f"Removed scenario override {_OVERRIDE_SCENARIO_FILE}")
    load_scenario_config()
    _cache.clear()
    return get_scenario_config()


# Initial load at import time
load_scenario_config()

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
                'ticker': ticker,
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
                'method': 'Scenario-based absolute price targets · 4-quarter rolling forecast',
                'baseline': 'Live YTD close prices via yfinance',
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
