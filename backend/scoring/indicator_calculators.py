"""
Multi-factor indicator scoring engine.

Each indicator score (0-100) is calculated from 5 factors:

Factor 1: Keyword Theme Volume (how many articles match indicator keywords)
Factor 2: GDELT Tone (how negative is the media coverage)
Factor 3: NLP Keyword Signal (severity-weighted keyword hits with VADER sentiment)
Factor 4: Volume Spike Detection (is today's volume abnormal vs. baseline)
Factor 5: Source Breadth (how widely reported is this topic)

These produce an ABSOLUTE score where:
  0-20  = Very Low Risk (stable, no significant events)
  20-40 = Low Risk (minor tensions, background noise)
  40-60 = Moderate Risk (active situation, worth monitoring)
  60-80 = High Risk (active conflict/crisis)
  80-100 = Critical Risk (severe ongoing crisis, war, collapse)

Theme volumes now come from keyword analysis of article titles
(not separate GDELT API calls), so they represent direct keyword
matches against our dictionaries.
"""

import math

# ─── FACTOR WEIGHTS ──────────────────────────────────────────────────
# NLP signal and tone get highest weights since they are the most
# differentiated signals between countries.

FACTOR_WEIGHTS = {
    'volume':         0.15,
    'tone':           0.25,
    'nlp_signal':     0.30,
    'volume_spike':   0.10,
    'source_breadth': 0.20,
}

# ─── BASELINE VOLUMES PER INDICATOR ─────────────────────────────────
# These are "normal" keyword-matched article counts per indicator
# from 75 GDELT articles. Lowered for earlier spike detection.

BASELINE_VOLUMES = {
    'political_stability': 5,
    'military_conflict': 3,
    'economic_sanctions': 1,
    'protests_civil_unrest': 2,
    'terrorism': 1,
    'diplomatic_tensions': 2
}

# ─── ABSOLUTE SCALING THRESHOLDS ────────────────────────────────────
# Calibrated for keyword-matched article counts from 75 GDELT articles.
# Tuned for high sensitivity — even 2-3 keyword-matched articles is significant.

VOLUME_THRESHOLDS = {
    'political_stability':   {'low': 1, 'moderate': 3,  'high': 8,   'critical': 20},
    'military_conflict':     {'low': 1, 'moderate': 2,  'high': 7,   'critical': 18},
    'economic_sanctions':    {'low': 1, 'moderate': 2,  'high': 5,   'critical': 12},
    'protests_civil_unrest': {'low': 1, 'moderate': 2,  'high': 7,   'critical': 16},
    'terrorism':             {'low': 1, 'moderate': 2,  'high': 5,   'critical': 10},
    'diplomatic_tensions':   {'low': 1, 'moderate': 2,  'high': 6,   'critical': 15}
}


def _volume_to_score(volume, indicator_name):
    """Convert keyword-matched article volume to 0-100."""
    thresholds = VOLUME_THRESHOLDS.get(indicator_name, {
        'low': 2, 'moderate': 6, 'high': 15, 'critical': 35
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
    GDELT tone typically ranges from -8 to +3 for most countries.
    Most news has a slight negative bias (around -2 to -4).

    Tone  +3 or above -> 0 (very positive, no risk)
    Tone   0          -> 25 (neutral, slight baseline risk)
    Tone  -3          -> 50 (moderately negative)
    Tone  -6          -> 75 (very negative)
    Tone -10          -> 100 (extremely negative)
    """
    if avg_tone >= 3.0:
        return 0.0
    elif avg_tone >= 0.0:
        return (3.0 - avg_tone) / 3.0 * 25.0
    elif avg_tone >= -3.0:
        return 25.0 + (-avg_tone / 3.0) * 25.0
    elif avg_tone >= -6.0:
        return 50.0 + ((-avg_tone - 3.0) / 3.0) * 25.0
    else:
        return min(100.0, 75.0 + ((-avg_tone - 6.0) / 4.0) * 25.0)


def _spike_score(current_volume, baseline_volume):
    """
    Detect abnormal spikes vs baseline.
    1.5x baseline -> 15. 2x -> 30. 4x -> 60. 8x -> 80.
    """
    if baseline_volume <= 0:
        baseline_volume = 1
    ratio = current_volume / baseline_volume

    if ratio <= 1.0:
        return 0.0
    elif ratio <= 2.0:
        return (ratio - 1.0) * 30.0
    elif ratio <= 4.0:
        return 30.0 + ((ratio - 2.0) / 2.0) * 30.0
    elif ratio <= 8.0:
        return 60.0 + ((ratio - 4.0) / 4.0) * 20.0
    else:
        return min(100.0, 80.0 + math.log2(ratio / 8.0) * 10.0)


def _source_breadth_score(total_articles):
    """
    Score based on how many articles cover this indicator.
    More coverage = more significant. Tuned for higher sensitivity.
    """
    if total_articles <= 0:
        return 0.0
    elif total_articles <= 1:
        return 18.0
    elif total_articles <= 3:
        return 18.0 + (total_articles - 1) / 2.0 * 22.0
    elif total_articles <= 8:
        return 40.0 + (total_articles - 3) / 5.0 * 30.0
    elif total_articles <= 18:
        return 70.0 + (total_articles - 8) / 10.0 * 20.0
    else:
        return min(100.0, 90.0 + math.log2(total_articles / 18.0) * 5.0)


def calculate_indicator_score(indicator_name, keyword_theme_volume, gdelt_baseline,
                              gdelt_tone, nlp_analysis):
    """
    Multi-factor indicator score (0-100, absolute scale).

    Combines 5 factors:
      1. Volume score (keyword-matched article count)
      2. Tone score (GDELT media negativity)
      3. NLP signal (keyword severity + VADER + escalation context)
      4. Volume spike (current vs baseline)
      5. Source breadth (how many articles match this indicator)
    """
    # Factor 1: Volume from keyword analysis
    volume_score = _volume_to_score(keyword_theme_volume, indicator_name)

    # Factor 2: Tone
    tone_score = _tone_to_score(gdelt_tone)

    # Factor 3: NLP signal
    signal_strength = 0.0
    if isinstance(nlp_analysis, dict):
        signal_strength = nlp_analysis.get('signal_strength', 0.0)
    nlp_score = signal_strength * 100.0

    # Factor 4: Spike detection
    spike = _spike_score(keyword_theme_volume, gdelt_baseline)

    # Factor 5: Source breadth
    article_count = 0
    if isinstance(nlp_analysis, dict):
        article_count = nlp_analysis.get('article_count', 0)
    breadth = _source_breadth_score(article_count)

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
    return BASELINE_VOLUMES.get(indicator_name, 3)
