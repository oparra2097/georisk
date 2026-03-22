from backend.models import IndicatorScore

INDICATOR_WEIGHTS = {
    'political_stability': 0.20,
    'military_conflict': 0.25,
    'economic_sanctions': 0.15,
    'protests_civil_unrest': 0.15,
    'terrorism': 0.15,
    'diplomatic_tensions': 0.10
}


def calculate_composite_score(indicators: IndicatorScore) -> float:
    """Weighted average of all 6 indicator scores."""
    score = (
        indicators.political_stability * INDICATOR_WEIGHTS['political_stability'] +
        indicators.military_conflict * INDICATOR_WEIGHTS['military_conflict'] +
        indicators.economic_sanctions * INDICATOR_WEIGHTS['economic_sanctions'] +
        indicators.protests_civil_unrest * INDICATOR_WEIGHTS['protests_civil_unrest'] +
        indicators.terrorism * INDICATOR_WEIGHTS['terrorism'] +
        indicators.diplomatic_tensions * INDICATOR_WEIGHTS['diplomatic_tensions']
    )
    return round(max(1.0, min(100.0, score)), 1)


def normalize_scores_percentile(scores_dict):
    """
    Apply percentile normalization across all countries.
    This spreads scores across 1-100 to avoid clustering.
    """
    if not scores_dict:
        return scores_dict

    composites = [s.composite_score for s in scores_dict.values()]
    composites_sorted = sorted(composites)
    n = len(composites_sorted)

    if n <= 1:
        return scores_dict

    for country_risk in scores_dict.values():
        raw = country_risk.composite_score
        count_below = sum(1 for c in composites_sorted if c < raw)
        count_equal = sum(1 for c in composites_sorted if c == raw)
        percentile = ((count_below + 0.5 * count_equal) / n) * 100.0
        country_risk.composite_score = round(max(1.0, min(100.0, percentile)), 1)

    return scores_dict
