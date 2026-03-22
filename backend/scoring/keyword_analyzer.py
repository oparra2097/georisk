"""
Multi-factor text analysis engine.

Combines three layers:
1. Keyword matching with severity tiers (what topics are mentioned)
2. VADER sentiment analysis (is the tone negative or positive)
3. Context modifiers (de-escalation phrases reduce score, escalation phrases amplify)

This means "peace talks end the war" scores LOW (de-escalation context)
while "war escalates across border" scores HIGH (escalation context).
"""

import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_vader = SentimentIntensityAnalyzer()

# ─── KEYWORD DICTIONARIES ────────────────────────────────────────────

INDICATOR_KEYWORDS = {
    'political_stability': {
        'high': [
            'coup', 'overthrow', 'assassination', 'regime change', 'martial law',
            'constitutional crisis', 'impeachment', 'state of emergency',
            'political collapse', 'government dissolved', 'power seizure',
            'authoritarian crackdown', 'dictator', 'junta'
        ],
        'medium': [
            'corruption scandal', 'political crisis', 'opposition crackdown',
            'election fraud', 'disputed election', 'cabinet reshuffle',
            'power struggle', 'authoritarian', 'political prisoner',
            'press freedom', 'censorship', 'rigged election', 'political purge'
        ],
        'low': [
            'political tension', 'policy debate', 'reform', 'election',
            'parliament', 'legislation', 'opposition', 'political party',
            'governance', 'rule of law'
        ]
    },
    'military_conflict': {
        'high': [
            'war', 'invasion', 'airstrike', 'bombardment', 'casualties',
            'killed in action', 'military offensive', 'armed conflict',
            'shelling', 'ground offensive', 'air raid', 'missile strike',
            'carpet bombing', 'ethnic cleansing', 'genocide', 'war crimes'
        ],
        'medium': [
            'military buildup', 'troops deployed', 'ceasefire violation',
            'arms deal', 'missile test', 'naval confrontation',
            'border clash', 'skirmish', 'drone strike', 'military escalation',
            'arms race', 'nuclear threat', 'mobilization'
        ],
        'low': [
            'military exercise', 'defense spending', 'arms procurement',
            'military cooperation', 'peacekeeping', 'defense pact',
            'military aid', 'joint exercise'
        ]
    },
    'economic_sanctions': {
        'high': [
            'sanctions imposed', 'trade embargo', 'asset freeze',
            'economic blockade', 'financial sanctions', 'export ban',
            'banking sanctions', 'oil embargo', 'swift ban',
            'economic warfare', 'total embargo'
        ],
        'medium': [
            'sanctions threat', 'trade restrictions', 'tariff war',
            'economic pressure', 'sanctions review', 'trade dispute',
            'currency crisis', 'debt default', 'capital flight',
            'economic isolation', 'import restrictions'
        ],
        'low': [
            'trade negotiations', 'economic agreement', 'sanctions relief',
            'trade talks', 'economic cooperation', 'tariff', 'trade deal'
        ]
    },
    'protests_civil_unrest': {
        'high': [
            'mass protests', 'riots', 'violent clashes', 'civil unrest',
            'uprising', 'revolution', 'looting', 'tear gas',
            'police brutality', 'crackdown on protesters', 'martial law',
            'curfew imposed', 'internet shutdown', 'mass arrests'
        ],
        'medium': [
            'protests', 'demonstrations', 'strikes', 'sit-in',
            'civil disobedience', 'rallies', 'marches', 'blockade',
            'general strike', 'student protests', 'labor unrest'
        ],
        'low': [
            'petition', 'public discontent', 'labor dispute',
            'social movement', 'advocacy', 'walkout', 'vigil'
        ]
    },
    'terrorism': {
        'high': [
            'terrorist attack', 'bombing', 'suicide bomber', 'hostage',
            'mass shooting', 'extremist attack', 'car bomb', 'ied',
            'beheading', 'kidnapping by militants', 'massacre',
            'coordinated attack', 'claimed responsibility'
        ],
        'medium': [
            'terror threat', 'terror plot', 'radicalization',
            'extremist group', 'terror cell', 'militant', 'insurgent',
            'jihadist', 'militia attack', 'recruitment', 'sleeper cell'
        ],
        'low': [
            'counter-terrorism', 'security alert', 'terror warning',
            'deradicalization', 'surveillance', 'intelligence operation'
        ]
    },
    'diplomatic_tensions': {
        'high': [
            'embassy closure', 'diplomat expelled', 'diplomatic breakdown',
            'severed relations', 'recalled ambassador', 'ultimatum',
            'diplomatic crisis', 'war of words', 'declared enemy',
            'persona non grata'
        ],
        'medium': [
            'diplomatic protest', 'summoned ambassador', 'diplomatic row',
            'territorial dispute', 'condemn', 'retaliatory measures',
            'diplomatic incident', 'provocative', 'contested waters'
        ],
        'low': [
            'diplomatic talks', 'negotiations', 'bilateral meeting',
            'summit', 'foreign policy', 'treaty', 'accord'
        ]
    }
}

SEVERITY_WEIGHTS = {
    'high': 1.0,
    'medium': 0.6,
    'low': 0.3
}

# ─── CONTEXT MODIFIERS ───────────────────────────────────────────────
# These phrases change the meaning of keywords.
# "war ends" is de-escalation. "war intensifies" is escalation.

DEESCALATION_PHRASES = [
    'peace talks', 'peace deal', 'peace agreement', 'ceasefire agreed',
    'ceasefire holds', 'truce', 'treaty signed', 'negotiations succeed',
    'conflict resolved', 'tensions ease', 'tensions de-escalate',
    'humanitarian aid', 'reconstruction', 'reconciliation',
    'sanctions lifted', 'sanctions eased', 'embargo lifted',
    'troops withdraw', 'withdrawal', 'demilitarized',
    'democracy restored', 'election success', 'peaceful transition',
    'protests end', 'calm restored', 'stability returns',
    'diplomatic breakthrough', 'relations normalized', 'accord signed'
]

ESCALATION_PHRASES = [
    'escalates', 'intensifies', 'worsens', 'deteriorates', 'spreads',
    'expands', 'surges', 'unprecedented', 'worst in decades',
    'death toll rises', 'casualties mount', 'crisis deepens',
    'declares war', 'state of emergency declared', 'martial law imposed',
    'threatens to', 'ultimatum issued', 'no-fly zone',
    'nuclear option', 'all-out', 'total war', 'ground invasion',
    'mass exodus', 'refugee crisis', 'humanitarian catastrophe',
    'ethnic cleansing', 'systematic', 'coordinated attacks'
]

_compiled_patterns = {}
_deesc_patterns = []
_esc_patterns = []


def _compile_all():
    """Compile all regex patterns once for performance."""
    global _deesc_patterns, _esc_patterns

    if _compiled_patterns:
        return

    for indicator, severity_dict in INDICATOR_KEYWORDS.items():
        _compiled_patterns[indicator] = {}
        for severity, keywords in severity_dict.items():
            patterns = []
            for kw in keywords:
                patterns.append(re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE))
            _compiled_patterns[indicator][severity] = patterns

    _deesc_patterns.clear()
    for phrase in DEESCALATION_PHRASES:
        _deesc_patterns.append(re.compile(r'\b' + re.escape(phrase) + r'\b', re.IGNORECASE))

    _esc_patterns.clear()
    for phrase in ESCALATION_PHRASES:
        _esc_patterns.append(re.compile(r'\b' + re.escape(phrase) + r'\b', re.IGNORECASE))


def get_sentiment(text):
    """
    Use VADER to get sentiment score.
    Returns compound score from -1.0 (most negative) to +1.0 (most positive).
    VADER is specifically tuned for social media and news text.
    """
    if not text:
        return 0.0
    scores = _vader.polarity_scores(text)
    return scores['compound']


def get_context_modifier(text):
    """
    Check for escalation/de-escalation context.
    Returns a multiplier:
      < 1.0 = de-escalation (reduces risk score)
      1.0   = neutral
      > 1.0 = escalation (amplifies risk score)
    """
    _compile_all()
    if not text:
        return 1.0

    deesc_count = sum(1 for p in _deesc_patterns if p.search(text))
    esc_count = sum(1 for p in _esc_patterns if p.search(text))

    if deesc_count > 0 and esc_count == 0:
        return 0.3  # Strong de-escalation -> reduce score by 70%
    elif deesc_count > esc_count:
        return 0.6  # Mostly de-escalation
    elif esc_count > 0 and deesc_count == 0:
        return 1.5  # Pure escalation -> amplify by 50%
    elif esc_count > deesc_count:
        return 1.3  # Mostly escalation

    return 1.0  # Neutral or balanced


def analyze_text(text):
    """
    Full multi-layer analysis of a single text.

    Layer 1: Keyword matching (what risk topics are mentioned)
    Layer 2: VADER sentiment (how negative is the overall tone)
    Layer 3: Context modifier (escalation vs de-escalation)

    Returns per-indicator scores that account for all three layers.
    """
    _compile_all()
    if not text:
        return {}

    # Layer 2: VADER sentiment (-1 to +1, negative = higher risk)
    sentiment = get_sentiment(text)

    # Layer 3: Context modifier
    context_mod = get_context_modifier(text)

    # Convert sentiment to a risk multiplier (0.5 to 1.5)
    # sentiment -1.0 (very negative) -> risk_mult 1.5
    # sentiment  0.0 (neutral)       -> risk_mult 1.0
    # sentiment +1.0 (very positive) -> risk_mult 0.5
    sentiment_mult = 1.0 - (sentiment * 0.5)

    results = {}
    for indicator, severity_patterns in _compiled_patterns.items():
        matches = []
        max_severity = None
        max_weight = 0.0

        for severity in ['high', 'medium', 'low']:
            for pattern in severity_patterns.get(severity, []):
                if pattern.search(text):
                    matches.append(pattern.pattern.replace('\\b', ''))
                    weight = SEVERITY_WEIGHTS[severity]
                    if weight > max_weight:
                        max_weight = weight
                        max_severity = severity

        # Combine all three layers into final score
        if matches:
            raw_score = max_weight
            adjusted_score = raw_score * sentiment_mult * context_mod
            adjusted_score = max(0.0, min(1.0, adjusted_score))
        else:
            adjusted_score = 0.0

        results[indicator] = {
            'matches': matches,
            'severity': max_severity,
            'raw_score': max_weight if matches else 0.0,
            'sentiment': round(sentiment, 3),
            'context_modifier': context_mod,
            'score': round(adjusted_score, 3)
        }

    return results


def analyze_articles(articles):
    """
    Analyze a batch of articles with the full multi-factor pipeline.

    For each article:
      1. Keyword match -> which indicators are relevant
      2. VADER sentiment -> how negative is this article
      3. Context check -> escalation or de-escalation

    Aggregation produces signal_strength per indicator that reflects
    both volume AND severity AND tone AND context.
    """
    if not articles:
        return {ind: {
            'article_count': 0, 'high_count': 0,
            'signal_strength': 0.0, 'avg_sentiment': 0.0
        } for ind in INDICATOR_KEYWORDS}

    aggregated = {ind: {
        'article_count': 0, 'high_count': 0,
        'total_score': 0.0, 'sentiments': []
    } for ind in INDICATOR_KEYWORDS}

    total_articles = len(articles)

    for article in articles:
        text = ''
        if isinstance(article, dict):
            text = (article.get('title', '') or '') + ' ' + (article.get('description', '') or '')
        elif isinstance(article, str):
            text = article

        if not text.strip():
            continue

        result = analyze_text(text)
        for indicator, data in result.items():
            if data['matches']:
                aggregated[indicator]['article_count'] += 1
                aggregated[indicator]['total_score'] += data['score']
                aggregated[indicator]['sentiments'].append(data['sentiment'])
                if data['severity'] == 'high':
                    aggregated[indicator]['high_count'] += 1

    final = {}
    for indicator, data in aggregated.items():
        if data['article_count'] > 0 and total_articles > 0:
            # Signal strength combines:
            # - Coverage: what fraction of articles mention this indicator
            # - Avg adjusted score: severity * sentiment * context averaged
            coverage = min(1.0, data['article_count'] / max(total_articles, 1))
            avg_score = data['total_score'] / data['article_count']

            # High-severity bonus: if many articles are high-severity, amplify
            high_ratio = data['high_count'] / data['article_count']
            high_bonus = 1.0 + (high_ratio * 0.3)

            signal = coverage * avg_score * high_bonus
            avg_sent = sum(data['sentiments']) / len(data['sentiments'])
        else:
            signal = 0.0
            avg_sent = 0.0

        final[indicator] = {
            'article_count': data['article_count'],
            'high_count': data['high_count'],
            'signal_strength': round(min(1.0, signal), 4),
            'avg_sentiment': round(avg_sent, 3)
        }

    return final
