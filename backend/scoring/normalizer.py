"""
Two-tier composite scoring.

Composite = Base Score (30%) + News Score (70%)

Base Score: World Bank WGI governance + macro fundamentals (0-100)
News Score: Weighted average of 6 EMA-blended indicator scores (0-100)

Score interpretation (absolute):
  0-20  = Very Low Risk
  20-40 = Low Risk
  40-60 = Moderate Risk
  60-80 = High Risk
  80-100 = Critical Risk

A score of 75 always means the same thing regardless of what
other countries are doing. Switzerland stays low, Syria stays high.
"""

from backend.models import IndicatorScore
from config import Config

INDICATOR_WEIGHTS = {
    'political_stability': 0.20,
    'military_conflict': 0.25,
    'economic_sanctions': 0.15,
    'protests_civil_unrest': 0.15,
    'terrorism': 0.15,
    'diplomatic_tensions': 0.10
}


def calculate_news_score(indicators: IndicatorScore) -> float:
    """
    Weighted average of all 6 indicator scores → news score (0-100).
    Military conflict gets highest weight (0.25) as the most impactful factor.
    """
    score = (
        indicators.political_stability * INDICATOR_WEIGHTS['political_stability'] +
        indicators.military_conflict * INDICATOR_WEIGHTS['military_conflict'] +
        indicators.economic_sanctions * INDICATOR_WEIGHTS['economic_sanctions'] +
        indicators.protests_civil_unrest * INDICATOR_WEIGHTS['protests_civil_unrest'] +
        indicators.terrorism * INDICATOR_WEIGHTS['terrorism'] +
        indicators.diplomatic_tensions * INDICATOR_WEIGHTS['diplomatic_tensions']
    )
    return round(max(0.0, min(100.0, score)), 1)


def calculate_composite(base_score: float, news_score: float) -> float:
    """
    Two-tier composite: base_score * 0.3 + news_score * 0.7

    Base score provides a structural floor (governance + macro fundamentals).
    News score provides high-frequency adjustment (GDELT + news APIs).
    """
    composite = (
        base_score * Config.BASE_SCORE_WEIGHT +
        news_score * Config.NEWS_SCORE_WEIGHT
    )
    return round(max(0.0, min(100.0, composite)), 1)


# Legacy compatibility — kept for any code that still calls this
def calculate_composite_score(indicators: IndicatorScore) -> float:
    """Legacy: weighted average of indicators. Use calculate_news_score instead."""
    return calculate_news_score(indicators)


def normalize_scores_absolute(scores_dict):
    """
    With absolute scoring, no normalization is needed.
    Just ensures all scores are properly clamped to 0-100.
    """
    for country_risk in scores_dict.values():
        country_risk.composite_score = round(
            max(0.0, min(100.0, country_risk.composite_score)), 1
        )
    return scores_dict
