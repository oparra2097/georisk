import re

INDICATOR_KEYWORDS = {
    'political_stability': {
        'high': [
            'coup', 'overthrow', 'assassination', 'regime change', 'martial law',
            'constitutional crisis', 'impeachment', 'state of emergency',
            'political collapse', 'government dissolved'
        ],
        'medium': [
            'corruption scandal', 'political crisis', 'opposition crackdown',
            'election fraud', 'disputed election', 'cabinet reshuffle',
            'power struggle', 'authoritarian', 'political prisoner',
            'press freedom', 'censorship'
        ],
        'low': [
            'political tension', 'policy debate', 'reform', 'election',
            'parliament', 'legislation', 'opposition', 'political party'
        ]
    },
    'military_conflict': {
        'high': [
            'war', 'invasion', 'airstrike', 'bombardment', 'casualties',
            'killed in action', 'military offensive', 'armed conflict',
            'shelling', 'ground offensive', 'air raid', 'missile strike'
        ],
        'medium': [
            'military buildup', 'troops deployed', 'ceasefire violation',
            'arms deal', 'missile test', 'naval confrontation',
            'border clash', 'skirmish', 'drone strike', 'military escalation'
        ],
        'low': [
            'military exercise', 'defense spending', 'arms procurement',
            'military cooperation', 'peacekeeping', 'defense pact'
        ]
    },
    'economic_sanctions': {
        'high': [
            'sanctions imposed', 'trade embargo', 'asset freeze',
            'economic blockade', 'financial sanctions', 'export ban',
            'banking sanctions', 'oil embargo'
        ],
        'medium': [
            'sanctions threat', 'trade restrictions', 'tariff war',
            'economic pressure', 'sanctions review', 'trade dispute',
            'currency crisis', 'debt default'
        ],
        'low': [
            'trade negotiations', 'economic agreement', 'sanctions relief',
            'trade talks', 'economic cooperation', 'tariff'
        ]
    },
    'protests_civil_unrest': {
        'high': [
            'mass protests', 'riots', 'violent clashes', 'civil unrest',
            'uprising', 'revolution', 'looting', 'tear gas',
            'police brutality', 'crackdown on protesters'
        ],
        'medium': [
            'protests', 'demonstrations', 'strikes', 'sit-in',
            'civil disobedience', 'rallies', 'marches', 'blockade',
            'general strike', 'student protests'
        ],
        'low': [
            'petition', 'public discontent', 'labor dispute',
            'social movement', 'advocacy', 'walkout'
        ]
    },
    'terrorism': {
        'high': [
            'terrorist attack', 'bombing', 'suicide bomber', 'hostage',
            'mass shooting', 'extremist attack', 'car bomb', 'ied',
            'beheading', 'kidnapping by militants'
        ],
        'medium': [
            'terror threat', 'terror plot', 'radicalization',
            'extremist group', 'terror cell', 'militant', 'insurgent',
            'jihadist', 'militia attack'
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
            'diplomatic crisis', 'war of words'
        ],
        'medium': [
            'diplomatic protest', 'summoned ambassador', 'diplomatic row',
            'territorial dispute', 'condemn', 'retaliatory measures',
            'diplomatic incident', 'provocative'
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

_compiled_patterns = {}


def _get_patterns():
    """Compile regex patterns once for performance."""
    if _compiled_patterns:
        return _compiled_patterns

    for indicator, severity_dict in INDICATOR_KEYWORDS.items():
        _compiled_patterns[indicator] = {}
        for severity, keywords in severity_dict.items():
            patterns = []
            for kw in keywords:
                patterns.append(re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE))
            _compiled_patterns[indicator][severity] = patterns

    return _compiled_patterns


def analyze_text(text):
    """Analyze a single text against all indicator keyword dictionaries."""
    if not text:
        return {}

    patterns = _get_patterns()
    results = {}

    for indicator, severity_patterns in patterns.items():
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

        results[indicator] = {
            'matches': matches,
            'severity': max_severity,
            'score': max_weight if matches else 0.0
        }

    return results


def analyze_articles(articles):
    """Analyze a batch of articles and return aggregated indicator signals."""
    if not articles:
        return {ind: {'article_count': 0, 'high_count': 0, 'signal_strength': 0.0}
                for ind in INDICATOR_KEYWORDS}

    aggregated = {ind: {'article_count': 0, 'high_count': 0, 'total_weight': 0.0}
                  for ind in INDICATOR_KEYWORDS}

    total_articles = len(articles)

    for article in articles:
        text = ''
        if isinstance(article, dict):
            text = (article.get('title', '') or '') + ' ' + (article.get('description', '') or '')
        elif isinstance(article, str):
            text = article

        result = analyze_text(text)
        for indicator, data in result.items():
            if data['matches']:
                aggregated[indicator]['article_count'] += 1
                aggregated[indicator]['total_weight'] += data['score']
                if data['severity'] == 'high':
                    aggregated[indicator]['high_count'] += 1

    final = {}
    for indicator, data in aggregated.items():
        if data['article_count'] > 0 and total_articles > 0:
            avg_weight = data['total_weight'] / data['article_count']
            coverage = data['article_count'] / total_articles
            signal = coverage * avg_weight
        else:
            signal = 0.0

        final[indicator] = {
            'article_count': data['article_count'],
            'high_count': data['high_count'],
            'signal_strength': min(1.0, signal)
        }

    return final
