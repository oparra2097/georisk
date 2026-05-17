"""Moody's sovereign rating transition matrix.

Ships with an embedded broad-rating 1-year matrix approximating Moody's
annual "Sovereign Default and Recovery Rates" publication. Override by
dropping a 7-row CSV at data/credit_signal/moodys_transition_matrix.csv -
the loader uses the override automatically when present.

Public API:
    load_matrix(path=None)        -> DataFrame rows=from, cols=to incl 'D'
    nstep_matrix(M, n)            -> DataFrame, n-year cumulative transition
    pd_at_horizon(M, n)           -> Series, cumulative default prob per rating
    rating_to_pd(rating, n, M=None) -> float
"""

import os

import numpy as np
import pandas as pd

BROAD_RATINGS = ['Aaa', 'Aa', 'A', 'Baa', 'Ba', 'B', 'Caa-C']
ALL_STATES = BROAD_RATINGS + ['D']  # D = Default (absorbing)

# Approximation of Moody's published sovereign 1-yr transition rates (broad
# ratings, withdrawn-rating mass renormalized into the rated destinations).
# These are reasonable defaults; refresh annually from the latest Moody's
# "Sovereign Default and Recovery Rates" Exhibit by saving a CSV override.
_EMBEDDED = np.array([
    # Aaa     Aa      A       Baa     Ba      B       Caa-C   D
    [0.9750, 0.0250, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000],  # Aaa
    [0.0120, 0.9430, 0.0440, 0.0010, 0.0000, 0.0000, 0.0000, 0.0000],  # Aa
    [0.0000, 0.0280, 0.9250, 0.0460, 0.0010, 0.0000, 0.0000, 0.0000],  # A
    [0.0000, 0.0030, 0.0540, 0.8920, 0.0460, 0.0040, 0.0010, 0.0000],  # Baa
    [0.0000, 0.0000, 0.0060, 0.0610, 0.8540, 0.0680, 0.0080, 0.0030],  # Ba
    [0.0000, 0.0000, 0.0000, 0.0030, 0.0520, 0.8420, 0.0810, 0.0220],  # B
    [0.0000, 0.0000, 0.0000, 0.0000, 0.0040, 0.0480, 0.7580, 0.1900],  # Caa-C
])

_DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', '..',
    'data', 'credit_signal', 'moodys_transition_matrix.csv',
)


def load_matrix(path=None):
    """Load transition matrix from CSV; fall back to the embedded values."""
    if path is None and os.path.exists(_DEFAULT_PATH):
        path = _DEFAULT_PATH
    if not path:
        return pd.DataFrame(_EMBEDDED, index=BROAD_RATINGS, columns=ALL_STATES)

    df = pd.read_csv(path, index_col=0, comment='#')
    if list(df.index) != BROAD_RATINGS:
        raise ValueError(
            f'Transition-matrix row labels must be {BROAD_RATINGS}; '
            f'got {list(df.index)}'
        )
    if list(df.columns) != ALL_STATES:
        raise ValueError(
            f'Transition-matrix column labels must be {ALL_STATES}; '
            f'got {list(df.columns)}'
        )
    rowsums = df.sum(axis=1).values
    if not np.allclose(rowsums, 1.0, atol=2e-3):
        raise ValueError(
            f'Transition-matrix rows must sum to ~1.0; got {rowsums.tolist()}'
        )
    return df


def _square(M):
    """Square 8x8 stochastic matrix with Default as an absorbing state."""
    full = np.zeros((len(ALL_STATES), len(ALL_STATES)))
    full[: len(BROAD_RATINGS), :] = M.values
    full[-1, -1] = 1.0
    return full


def nstep_matrix(M, n):
    """n-year cumulative transition matrix via matrix power."""
    if n < 1:
        raise ValueError('horizon must be >= 1')
    Pn = np.linalg.matrix_power(_square(M), int(n))
    return pd.DataFrame(
        Pn[: len(BROAD_RATINGS), :], index=BROAD_RATINGS, columns=ALL_STATES
    )


def pd_at_horizon(M, n):
    """Cumulative probability of default within n years, by starting rating."""
    return nstep_matrix(M, n)['D']


def rating_to_pd(rating, horizon, M=None):
    if M is None:
        M = load_matrix()
    pds = pd_at_horizon(M, horizon)
    return float(pds.get(rating, np.nan))
