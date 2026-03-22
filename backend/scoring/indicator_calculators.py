"""
Multi-factor indicator scoring engine.

Each indicator score (0-100) is calculated from 5 factors:

Factor 1: GDELT Event Volume (how many relevant events are happening)
Factor 2: GDELT Tone (how negative is the media coverage)
Factor 3: NLP Keyword Signal (severity-weighted keyword hits with VADER sentiment)
Factor 4: Volume Spike Detection (is today's volume abnormal vs. baseline)
Factor 5: Source Concentration (are multiple independent sources reporting this)

These produce an ABSOLUTE score where:
  0-20  = Very Low Risk (stable, no significant events)
  20-40 = Low Risk (minor tensions, background noise)
  40-60 = Moderate Risk (active situation, worth monitoring)
  60-80 = High Risk (active conflict/crisis)
  80-100 = Critical Risk (severe ongoing crisis, war, collapse)
"""

import math

# ─── FACTOR WEIGHTS ──────────────────────────────────────────────────

FACTOR_WEIGHTS = {
    'volume':         0.25,
    'tone':           0.20,
    'nlp_signal':     0.25,
    'volume_spike':   0.15,
    'source_breadth': 0.15,
}

# ─── BASELINE VOLUMES PER INDICATOR ─────────────────────────────────

BASELINE_VOLUMES = {
    'political_stability': 25,
    'military_conflict': 20,
    'economic_sanctions': 8,
    'protests_civil_unrest': 15,
    'terrorism': 6,
    'diplomatic_tensions': 12
}

# ─── ABSOLUTE SCALING THRESHOLDS ────────────────────────────────────

VOLUME_THRESHOLDS = {
    'political_stability':   {'low': 5, 'moderate': 20, 'high': 60, 'critical': 150},
    'military_conflict':     {'low': 3, 'moderate': 15, 'high': 50, 'critical': 120},
    'economic_sanctions':    {'low': 2, 'moderate': 8,  'high': 25, 'critical': 60},
    'protests_civil_unrest': {'low': 5, 'moderate': 15, 'high': 40, 'critical': 100},
    'terrorism':             {'low': 1, 'moderate': 5,  'high': 15, 'critical': 40},
    'diplomatic_tensions':   {'low': 3, 'moderate': 10, 'high': 30, 'critical': 80}
}


def _volume_to_score(volume, indicator_name):
    """Convert raw article volume to 0-100 using absolute thresholds."""
    thresholds = VOLUME_THRESHOLDS.get(indicator_name, {
        'low': 5, 'moderate': 15, 'high': 50, 'critical': 120
    })

    if volume <= 0:
        return 0.0
    elif volume <= thresholds['low']:
        return (volume / thresholds['low']) * 20.0
    elif volume <= thresholds['moderate']:
        progress = (volume - thresholds['low']) / (thresholds['moderate'] - thresholds['low'])
        return 20.0 + progress * 20.0
    elif volume <= thresholds['high']:
        progress = (volume - thresholds['moderate']) / (thresholds['high'] - thresholds['moderate'])
        return 40.0 + progress * 30.0
    elif volume <= thresholds['critical']:
        progress = (volume - thresholds['high']) / (thresholds['critical'] - thresholds['high'])
        return 70.0 + progress * 20.0
    else:
        overshoot = volume / thresholds['critical']
        return min(100.0, 90.0 + math.log2(overshoot) * 5.0)


def _tone_to_score(avg_tone):
    """
    Convert GDELT tone to risk score.
    Tone -10 (very negative) -> 100. Tone +5 (positive) -> 0.
    """
    if avg_tone >= 5.0:
        return 0.0
    elif avg_tone >= 0.0:
        return (5.0 - avg_tone) / 5.0 * 30.0
    elif avg_tone >= -5.0:
        return 30.0 + (-avg_tone / 5.0) * 45.0
    else:
        return min(100.0, 75.0 + ((-avg_tone - 5.0) / 5.0) * 25.0)


def _spike_score(current_volume, baseline_volume):
    """
    Detect abnormal spikes vs baseline.
    2x baseline -> 30. 5x -> 60. 10x -> 80. 20x+ -> 95.
    """
    if baseline_volume <= 0:
        baseline_volume = 1
    ratio = current_volume / baseline_volume

    if ratio <= 1.0:
        return 0.0
    elif ratio <= 2.0:
        return (ratio - 1.0) * 30.0
    elif ratio <= 5.0:
        return 30.0 + ((ratio - 2.0) / 3.0) * 30.0
    elif ratio <= 10.0:
        return 60.0 + ((ratio - 5.0) / 5.0) * 20.0
    else:
        return min(100.0, 80.0 + math.log2(ratio / 10.0) * 10.0)


def _source_breadth_score(total_articles):
    """
    Score based on how many sources cover this.
    More sources = more significant event.
    """
    if total_articles <= 0:
        return 0.0
    elif total_articles == 1:
        return 10.0
    elif total_articles <= 5:
        return 10.0 + (total_articles - 1) / 4.0 * 30.0
    elif total_articles <= 15:
        return 40.0 + (total_articles - 5) / 10.0 * 25.0
    elif total_articles <= 30:
        return 65.0 + (total_articles - 15) / 15.0 * 20.0
    else:
        return min(100.0, 85.0 + math.log2(total_articles / 30.0) * 5.0)


def calculate_indicator_score(indicator_name, gdelt_theme_volume, gdelt_baseline,
                              gdelt_tone, newsapi_signal):
    """
    Multi-factor indicator score (0-100, absolute scale).

    Combines 5 factors:
      1. Volume score (absolute thresholds per indicator)
      2. Tone score (GDELT media negativity)
      3. NLP signal (keyword + VADER + escalation context)
      4. Volume spike (current vs baseline)
      5. Source breadth (how widely reported)
    """
    # Factor 1: Volume
    volume_score = _volume_to_score(gdelt_theme_volume, indicator_name)

    # Factor 2: Tone
    tone_score = _tone_to_score(gdelt_tone)

    # Factor 3: NLP signal
    signal_strength = 0.0
    if isinstance(newsapi_signal, dict):
        signal_strength = newsapi_signal.get('signal_strength', 0.0)
    nlp_score = signal_strength * 100.0

    # Factor 4: Spike detection
    spike = _spike_score(gdelt_theme_volume, gdelt_baseline)

    # Factor 5: Source breadth
    article_count = 0
    if isinstance(newsapi_signal, dict):
        article_count = newsapi_signal.get('article_count', 0)
    breadth = _source_breadth_score(article_count + gdelt_theme_volume)

    # Weighted combination
    final = (
        volume_score * FACTOR_WEIGHTS['volume'] +
        tone_score * FACTOR_WEIGHTS['tone'] +
        nlp_score * FACTOR_WEIGHTS['nlp_signal'] +
        spike * FACTOR_WEIGHTS['volume_spike'] +
        breadth * FACTOR_WEIGHTS['source_breadth']
    )

    return round(max(0.0, min(100.0, final)), 1)


def get_baseline(indicator_name):
    """Get the baseline volume for an indicator."""
    return BASELINE_VOLUMES.get(indicator_name, 10)
