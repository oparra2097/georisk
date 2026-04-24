"""
Pure normalization helpers: raw time-series → 0-100 risk contribution.
100 = worst (high risk), 0 = best.

Kept dependency-free (stdlib only) so it can be unit-tested without the app.
"""

import math
from typing import Iterable, Optional


def logistic_squash(z: float) -> float:
    """Map a z-score to 0-100 via a logistic. z=0 -> 50; z=+2 -> ~88; z=-2 -> ~12."""
    return 100.0 / (1.0 + math.exp(-z))


def mean(xs: Iterable[float]) -> float:
    xs = list(xs)
    return sum(xs) / len(xs) if xs else 0.0


def stdev(xs: Iterable[float]) -> float:
    xs = list(xs)
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    var = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var)


def zscore(value: float, history: Iterable[float]) -> float:
    """z = (value - mean(history)) / stdev(history). Returns 0.0 if stdev is zero."""
    hist = list(history)
    if len(hist) < 2:
        return 0.0
    m = mean(hist)
    s = stdev(hist)
    if s == 0:
        return 0.0
    return (value - m) / s


def percentile_rank(value: float, history: Iterable[float]) -> float:
    """Rank of `value` within history, on [0, 100]. Higher = larger than more history points."""
    hist = sorted(history)
    if not hist:
        return 50.0
    below = sum(1 for h in hist if h < value)
    equal = sum(1 for h in hist if h == value)
    rank = (below + 0.5 * equal) / len(hist)
    return rank * 100.0


def youth_unemp_risk(level: float, history: list, delta_12m: Optional[float],
                     total_unemp: Optional[float]) -> dict:
    """
    Composite labor risk from youth unemployment inputs.

    Weights inside the sub-score:
      level        50%  → percentile of current level vs country's own 2015+ history
      delta_12m    30%  → z-score of YoY change, logistic-squashed
      gap_vs_total 20%  → z-score of (youth - total), logistic-squashed

    Returns {'value': 0-100, 'drivers': {...}}.
    """
    level_rank = percentile_rank(level, history) if history else 50.0

    if delta_12m is None:
        delta_risk = 50.0
    else:
        deltas = []
        if len(history) >= 13:
            for i in range(12, len(history)):
                deltas.append(history[i] - history[i - 12])
        delta_risk = logistic_squash(zscore(delta_12m, deltas)) if len(deltas) >= 2 else 50.0

    if total_unemp is None:
        gap_risk = 50.0
    else:
        gap = level - total_unemp
        # No per-country gap history yet; use a global prior: mean 5pp, sd 3pp.
        # Source: ILO Global Employment Trends for Youth shows a 2-3x youth-to-adult
        # ratio in most countries, roughly 4-8pp gap. We'll refine with actual panels later.
        gap_risk = logistic_squash((gap - 5.0) / 3.0)

    value = 0.50 * level_rank + 0.30 * delta_risk + 0.20 * gap_risk
    return {
        'value': max(0.0, min(100.0, value)),
        'drivers': {
            'level_pct': round(level, 2),
            'level_pctile_vs_history': round(level_rank, 1),
            'delta_12m_pp': round(delta_12m, 2) if delta_12m is not None else None,
            'delta_12m_risk': round(delta_risk, 1),
            'total_unemp_pct': round(total_unemp, 2) if total_unemp is not None else None,
            'gap_vs_total_pp': round(level - total_unemp, 2) if total_unemp is not None else None,
            'gap_risk': round(gap_risk, 1),
        }
    }
