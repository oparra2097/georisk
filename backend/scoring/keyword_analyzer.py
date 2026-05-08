"""
Multi-factor text analysis engine.

Combines three layers:
1. Keyword matching with severity tiers (what topics are mentioned)
2. VADER sentiment analysis (is the tone negative or positive)
3. Context modifiers (de-escalation phrases reduce score, escalation phrases amplify)

Keywords use SINGLE-WORD matching for reliability on short GDELT titles.
Multi-word phrases are split into individual words that each contribute.
"""

import re
import math
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_vader = SentimentIntensityAnalyzer()

# ─── KEYWORD DICTIONARIES ────────────────────────────────────────────
# Each keyword is a single word or short phrase.
# Single words use word-boundary matching (\b).
# The analyzer scans title + description (for GDELT, title is duplicated).

INDICATOR_KEYWORDS = {
    'political_stability': {
        'high': [
            'coup', 'overthrow', 'assassination', 'assassinated', 'martial law',
            'impeach', 'impeached', 'impeachment', 'dictator', 'junta',
            'authoritarian', 'crackdown', 'purge', 'despot',
            'state of emergency', 'power grab', 'political crisis',
            'narco state', 'failed state', 'lawlessness', 'anarchy',
            # Regime / autocracy vocabulary
            'regime', 'autocrat', 'autocracy', 'theocracy', 'theocratic',
            'strongman', 'one-party', 'one party rule', 'suppression',
            # Personalist rule signals
            'hardliner', 'hardliners', 'loyalist', 'loyalists',
            # Political crisis signals
            'constitutional crisis', 'government collapse', 'no confidence',
            'cabinet reshuffle', 'caretaker government', 'power vacuum',
            'succession crisis', 'political turmoil', 'leadership crisis',
            'regime change',
            # Election integrity
            'stolen election', 'electoral fraud', 'election rigged',
        ],
        'medium': [
            'corruption', 'scandal', 'fraud', 'rigged', 'disputed',
            'opposition', 'censorship', 'detained', 'arrested',
            'prisoner', 'dissident', 'repression', 'resign',
            # Regime figures (medium — context-dependent)
            'supreme leader', 'ayatollah', 'mullah', 'mullahs',
            'politburo', 'politburo standing committee',
            # Governance dysfunction
            'instability', 'unstable', 'paralysis', 'gridlock',
            'infighting', 'faction', 'factions', 'factional',
            'exile', 'exiled', 'persecution', 'political prisoner',
            'sham trial', 'show trial',
        ],
        'low': [
            'election', 'vote', 'parliament', 'legislation', 'reform',
            'governance', 'political', 'coalition', 'cabinet',
            'minister', 'ministry', 'legislator', 'legislature',
        ]
    },
    'military_conflict': {
        'high': [
            'war', 'invasion', 'airstrike', 'airstrikes', 'bombardment',
            'casualties', 'killed', 'shelling', 'offensive', 'missile',
            'missiles', 'bombing', 'bombed', 'genocide', 'massacre',
            'troops', 'soldiers', 'battlefield', 'combat', 'warfare',
            'air strike', 'ground offensive', 'ethnic cleansing',
            'war crimes', 'drone strike', 'military coup',
        ],
        'medium': [
            'military', 'deploy', 'deployed', 'deployment', 'ceasefire',
            'arms', 'weapons', 'drone', 'drones', 'naval', 'clash',
            'skirmish', 'mobilization', 'nuclear', 'escalation',
            'artillery', 'tank', 'tanks', 'wounded', 'strike',
            'arms deal', 'weapons shipment', 'no fly zone',
            'gang violence', 'armed group', 'armed groups',
            'criminal organization', 'militia',
        ],
        'low': [
            'defense', 'defence', 'army', 'navy', 'peacekeeping',
            'exercise', 'patrol', 'base', 'battalion',
        ]
    },
    'economic_sanctions': {
        'high': [
            'sanctions', 'sanctioned', 'embargo', 'blockade',
            'freeze', 'frozen', 'blacklist', 'blacklisted', 'banned',
            'trade war', 'asset freeze', 'oil embargo', 'financial sanctions',
        ],
        'medium': [
            'tariff', 'tariffs', 'restriction', 'restrictions', 'penalty',
            'default', 'crisis', 'devaluation', 'inflation',
            'currency', 'debt', 'deficit',
            'capital controls', 'debt restructuring',
        ],
        'low': [
            'trade', 'economic', 'export', 'import', 'negotiate',
            'deal', 'agreement',
        ]
    },
    'protests_civil_unrest': {
        'high': [
            'riot', 'riots', 'rioting', 'uprising', 'revolution',
            'looting', 'unrest', 'revolt', 'insurrection', 'curfew',
            'shutdown', 'clashes', 'brutality', 'crackdown',
            'civil unrest', 'mass protest', 'general strike',
            'gang war', 'turf war', 'vigilante', 'lynching', 'mob violence',
        ],
        'medium': [
            'protest', 'protests', 'protesters', 'protesting',
            'demonstration', 'demonstrations', 'strike', 'strikes',
            'rally', 'rallies', 'march', 'marches', 'blockade',
            'arrested', 'teargas', 'detain', 'detained',
        ],
        'low': [
            'petition', 'discontent', 'walkout', 'vigil',
            'movement', 'activist', 'advocacy',
        ]
    },
    'terrorism': {
        'high': [
            'terrorist', 'terrorism', 'bombing', 'bombed', 'bomber',
            'hostage', 'hostages', 'massacre', 'shooting', 'attack',
            'attacked', 'explosion', 'explosive', 'suicide',
            'kidnap', 'kidnapped', 'abducted',
            'car bomb', 'suicide bomber', 'terror attack',
            'cartel', 'cartels', 'narco', 'narcoterrorism',
            'gang', 'gangs', 'trafficking', 'traffickers',
            'paramilitary', 'paramilitaries', 'sicario', 'hitman',
            'extortion', 'beheading', 'dismembered',
            # State-sponsor / proxy-force vocabulary
            'proxy', 'proxies', 'proxy war', 'proxy forces',
            'state-sponsored', 'state sponsor of terror',
            # Named groups commonly covered
            'hezbollah', 'hamas', 'houthi', 'houthis',
            'al-shabaab', 'shabaab', 'boko haram', 'iswap', 'jnim',
            'isis', 'isil', "islamic state", 'daesh', 'al-qaeda', 'al qaeda',
            'irgc', 'quds force', 'taliban', 'ttp', 'pkk',
            # Maritime / shipping attacks
            'tanker seized', 'ship seized', 'vessel attacked',
            'strait of hormuz attack', 'red sea attack', 'ship hijacked',
        ],
        'medium': [
            'militant', 'militants', 'extremist', 'extremism',
            'insurgent', 'insurgency', 'radicalized', 'militia',
            'jihadist', 'armed', 'gunmen', 'threat',
            'organized crime', 'drug lord', 'kingpin', 'smuggling',
            'drug war', 'turf war',
            # Proxy / militant network language
            'axis of resistance', 'iran-backed', 'iranian-backed',
            'russia-backed', 'proxy group', 'affiliated', 'aligned',
            'splinter group', 'rebel group', 'armed wing',
            'radical', 'radicalization', 'sleeper cell',
        ],
        'low': [
            'security', 'surveillance', 'intelligence',
            'alert', 'warning', 'counter-terrorism',
            'watchlist', 'designation', 'blacklist',
        ]
    },
    'diplomatic_tensions': {
        'high': [
            'expelled', 'expel', 'embassy', 'diplomat', 'severed',
            'recalled', 'ultimatum', 'condemn', 'condemned',
            'retaliation', 'retaliatory', 'retaliate', 'retaliates',
            'diplomatic crisis', 'trade dispute', 'border dispute',
            # Active diplomatic breakdown signals
            'talks collapsed', 'talks break down', 'walked out',
            'withdrew from talks', 'suspended negotiations',
            'diplomatic row', 'diplomatic rift', 'diplomatic spat',
        ],
        'medium': [
            'tensions', 'dispute', 'disputed', 'provocative',
            'provocation', 'territorial', 'summoned', 'warned',
            'denounce', 'denounced', 'accuse', 'accused',
            # Failure-to-resolve signals (Iran style coverage)
            'talks stall', 'talks falter', 'stalemate', 'impasse',
            'unresolved', 'rejected', 'refused', 'dismissed',
            'standoff', 'standoffs', 'brinkmanship',
        ],
        'low': [
            'talks', 'negotiations', 'summit', 'treaty',
            'bilateral', 'diplomacy', 'diplomatic',
        ]
    }
}

SEVERITY_WEIGHTS = {
    'high': 1.0,
    'medium': 0.5,
    'low': 0.2
}

# ─── CONTEXT MODIFIERS ───────────────────────────────────────────────

DEESCALATION_WORDS = [
    'peace', 'ceasefire', 'truce', 'treaty', 'agreement', 'resolved',
    'ease', 'eased', 'easing', 'withdraw', 'withdrawal', 'withdrawn',
    'lifted', 'restored', 'reconciliation', 'breakthrough', 'normalized',
    'stabilize', 'stabilized', 'calm', 'end', 'ended', 'ends',
    'humanitarian', 'aid', 'relief', 'reconstruction',
]

ESCALATION_WORDS = [
    'escalate', 'escalates', 'escalation', 'intensify', 'intensifies',
    'worsen', 'worsens', 'deteriorate', 'spreads', 'surge', 'surges',
    'unprecedented', 'worst', 'deadliest', 'crisis', 'emergency',
    'threatens', 'threatened', 'invasion', 'declares', 'declared',
    'catastrophe', 'catastrophic', 'systematic', 'coordinated',
    'imminent', 'alarming',
]

_compiled_keywords = {}
_deesc_patterns = []
_esc_patterns = []
_compiled = False


def _compile_all():
    """Compile all regex patterns once for performance."""
    global _compiled
    if _compiled:
        return

    for indicator, severity_dict in INDICATOR_KEYWORDS.items():
        _compiled_keywords[indicator] = {}
        for severity, keywords in severity_dict.items():
            patterns = []
            for kw in keywords:
                patterns.append(
                    (kw, re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE))
                )
            _compiled_keywords[indicator][severity] = patterns

    for word in DEESCALATION_WORDS:
        _deesc_patterns.append(re.compile(r'\b' + re.escape(word) + r'\b', re.IGNORECASE))

    for word in ESCALATION_WORDS:
        _esc_patterns.append(re.compile(r'\b' + re.escape(word) + r'\b', re.IGNORECASE))

    _compiled = True


def get_sentiment(text):
    """VADER sentiment: -1.0 (most negative) to +1.0 (most positive)."""
    if not text:
        return 0.0
    return _vader.polarity_scores(text)['compound']


def get_context_modifier(text):
    """
    Escalation/de-escalation modifier.
    < 1.0 = de-escalation, 1.0 = neutral, > 1.0 = escalation
    """
    _compile_all()
    if not text:
        return 1.0

    deesc = sum(1 for p in _deesc_patterns if p.search(text))
    esc = sum(1 for p in _esc_patterns if p.search(text))

    if deesc > 0 and esc == 0:
        return 0.4
    elif deesc > esc:
        return 0.6
    elif esc > 0 and deesc == 0:
        return 1.4
    elif esc > deesc:
        return 1.2
    return 1.0


def analyze_text(text):
    """
    Full analysis of a single text.

    Layer 1: Keyword matching (which risk indicators are mentioned)
    Layer 2: VADER sentiment (negative = higher risk)
    Layer 3: Context modifier (escalation vs de-escalation)
    """
    _compile_all()
    if not text:
        return {}

    sentiment = get_sentiment(text)
    context_mod = get_context_modifier(text)

    # sentiment -1.0 -> mult 1.5, 0.0 -> 1.0, +1.0 -> 0.5
    sentiment_mult = 1.0 - (sentiment * 0.5)

    results = {}
    for indicator, severity_patterns in _compiled_keywords.items():
        matches = []
        max_severity = None
        max_weight = 0.0
        total_weight = 0.0

        for severity in ['high', 'medium', 'low']:
            for kw, pattern in severity_patterns.get(severity, []):
                if pattern.search(text):
                    matches.append(kw)
                    weight = SEVERITY_WEIGHTS[severity]
                    total_weight += weight
                    if weight > max_weight:
                        max_weight = weight
                        max_severity = severity

        if matches:
            # Use total_weight capped at 1.0, not just max
            raw_score = min(1.0, total_weight)
            adjusted_score = raw_score * sentiment_mult * context_mod
            adjusted_score = max(0.0, min(1.0, adjusted_score))
        else:
            adjusted_score = 0.0

        results[indicator] = {
            'matches': matches,
            'severity': max_severity,
            'raw_score': min(1.0, total_weight) if matches else 0.0,
            'sentiment': round(sentiment, 3),
            'context_modifier': context_mod,
            'score': round(adjusted_score, 3)
        }

    return results


def analyze_articles(articles):
    """
    Analyze a batch of articles. Returns per-indicator signal strength
    and theme volume (number of articles matching each indicator).
    """
    if not articles:
        return {ind: {
            'article_count': 0, 'high_count': 0,
            'signal_strength': 0.0, 'avg_sentiment': 0.0,
            'theme_volume': 0
        } for ind in INDICATOR_KEYWORDS}

    aggregated = {ind: {
        'article_count': 0, 'high_count': 0,
        'total_score': 0.0, 'sentiments': []
    } for ind in INDICATOR_KEYWORDS}

    processed_articles = 0

    for article in articles:
        text = ''
        if isinstance(article, dict):
            text = (article.get('title', '') or '') + ' ' + (article.get('description', '') or '')
        elif isinstance(article, str):
            text = article

        if not text.strip():
            continue

        processed_articles += 1

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
        if data['article_count'] > 0 and processed_articles > 0:
            article_count = data['article_count']
            avg_score = data['total_score'] / article_count
            high_ratio = data['high_count'] / article_count

            # Absolute count scaling via log2 curve (not coverage ratio).
            # 20 articles matching military keywords is a STRONG signal
            # regardless of whether there were 75 or 200 total articles.
            # 1->0.17, 3->0.33, 5->0.43, 10->0.58, 20->0.72, 40->0.88, 60+->~1.0
            count_signal = min(1.0, math.log2(article_count + 1) / math.log2(65))

            # High-severity boost: all high -> 1.5x, half -> 1.25x, none -> 1.0x
            severity_boost = 1.0 + (high_ratio * 0.5)

            # Coverage bonus: if this topic dominates the news, amplify
            coverage = article_count / max(processed_articles, 1)
            if coverage > 0.4:
                coverage_bonus = 1.15
            elif coverage > 0.25:
                coverage_bonus = 1.08
            else:
                coverage_bonus = 1.0

            signal = min(1.0, count_signal * avg_score * severity_boost * coverage_bonus)
            avg_sent = sum(data['sentiments']) / len(data['sentiments'])
        else:
            signal = 0.0
            avg_sent = 0.0

        final[indicator] = {
            'article_count': data['article_count'],
            'high_count': data['high_count'],
            'signal_strength': round(min(1.0, signal), 4),
            'avg_sentiment': round(avg_sent, 3),
            'theme_volume': data['article_count'],
        }

    return final
