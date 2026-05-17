"""Normalize agency sovereign ratings (S&P / Moody's / Fitch) into a single
broad-rating bucket per country.

Composite rule: map each agency's notch to the Moody's broad scale, then
take the median across whichever agencies are available. Ties round to
the worse rating (more conservative for a credit-lines book).

Reads data/agency_ratings.csv by default; pass `path` to point elsewhere.
"""

import os

import pandas as pd

_SP_TO_BROAD = {
    'AAA': 'Aaa',
    'AA+': 'Aa', 'AA': 'Aa', 'AA-': 'Aa',
    'A+': 'A', 'A': 'A', 'A-': 'A',
    'BBB+': 'Baa', 'BBB': 'Baa', 'BBB-': 'Baa',
    'BB+': 'Ba', 'BB': 'Ba', 'BB-': 'Ba',
    'B+': 'B', 'B': 'B', 'B-': 'B',
    'CCC+': 'Caa-C', 'CCC': 'Caa-C', 'CCC-': 'Caa-C',
    'CC': 'Caa-C', 'C': 'Caa-C', 'SD': 'Caa-C', 'D': 'Caa-C',
}

_MOODYS_TO_BROAD = {
    'Aaa': 'Aaa',
    'Aa1': 'Aa', 'Aa2': 'Aa', 'Aa3': 'Aa',
    'A1': 'A', 'A2': 'A', 'A3': 'A',
    'Baa1': 'Baa', 'Baa2': 'Baa', 'Baa3': 'Baa',
    'Ba1': 'Ba', 'Ba2': 'Ba', 'Ba3': 'Ba',
    'B1': 'B', 'B2': 'B', 'B3': 'B',
    'Caa1': 'Caa-C', 'Caa2': 'Caa-C', 'Caa3': 'Caa-C',
    'Ca': 'Caa-C', 'C': 'Caa-C',
}

_FITCH_TO_BROAD = _SP_TO_BROAD  # Fitch uses the S&P notch grid

_BROAD_ORDER = {'Aaa': 1, 'Aa': 2, 'A': 3, 'Baa': 4, 'Ba': 5, 'B': 6, 'Caa-C': 7}
_ORDER_BROAD = {v: k for k, v in _BROAD_ORDER.items()}

_DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', '..',
    'data', 'agency_ratings.csv',
)


def _to_broad(rating, mapper):
    if not isinstance(rating, str):
        return None
    return mapper.get(rating.strip())


def load_ratings(path=None):
    """Returns DataFrame: iso3, sp, moodys, fitch, broad_rating, broad_rating_order."""
    path = path or _DEFAULT_PATH
    df = pd.read_csv(path, comment='#')
    rows = []
    for _, row in df.iterrows():
        sp = _to_broad(row.get('sp'), _SP_TO_BROAD)
        mo = _to_broad(row.get('moodys'), _MOODYS_TO_BROAD)
        fi = _to_broad(row.get('fitch'), _FITCH_TO_BROAD)
        ranks = sorted(_BROAD_ORDER[r] for r in (sp, mo, fi) if r)
        if not ranks:
            continue
        # Median; for an even count round to the worse rating (higher order int)
        median_rank = ranks[len(ranks) // 2] if len(ranks) % 2 else max(
            ranks[len(ranks) // 2 - 1], ranks[len(ranks) // 2])
        rows.append({
            'iso3': row['iso3'],
            'sp': row.get('sp'),
            'moodys': row.get('moodys'),
            'fitch': row.get('fitch'),
            'broad_rating': _ORDER_BROAD[median_rank],
            'broad_rating_order': median_rank,
        })
    return pd.DataFrame(rows)
