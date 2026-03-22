def calculate_indicator_score(indicator_name, gdelt_theme_volume, gdelt_baseline,
                              gdelt_tone, newsapi_signal):
    """
    Calculate a single indicator score (0-100) from GDELT + NewsAPI signals.

    GDELT component (60% weight):
      - volume_ratio = theme_volume / max(baseline, 1)
      - tone_penalty = (5 - tone) / 15  (maps -10..+10 tone to ~0..1 risk)
      - gdelt_raw = volume_ratio * 50 * (1 + tone_penalty)

    NewsAPI component (40% weight):
      - signal_strength from keyword analyzer (0.0 to 1.0)
      - newsapi_raw = signal_strength * 100
    """
    baseline = max(gdelt_baseline, 1)
    volume_ratio = gdelt_theme_volume / baseline

    tone_penalty = (5.0 - gdelt_tone) / 15.0
    tone_penalty = max(0.0, min(1.0, tone_penalty))

    gdelt_raw = min(100.0, volume_ratio * 50.0 * (1.0 + tone_penalty))

    signal_strength = 0.0
    if isinstance(newsapi_signal, dict):
        signal_strength = newsapi_signal.get('signal_strength', 0.0)
    newsapi_raw = signal_strength * 100.0

    score = (gdelt_raw * 0.6) + (newsapi_raw * 0.4)
    return max(0.0, min(100.0, score))


# Global baseline volumes per indicator (approximate medians, will be
# dynamically updated as data comes in)
BASELINE_VOLUMES = {
    'political_stability': 20,
    'military_conflict': 15,
    'economic_sanctions': 5,
    'protests_civil_unrest': 10,
    'terrorism': 5,
    'diplomatic_tensions': 10
}


def get_baseline(indicator_name):
    return BASELINE_VOLUMES.get(indicator_name, 10)
