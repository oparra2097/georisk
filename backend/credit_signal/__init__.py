"""Credit risk signal matrix.

Fuses Moody's sovereign rating transition matrix, current agency ratings,
and commodity-forecast shocks into a country x sector signal map for the
global credit-lines business.

Entry point:
    python scripts/build_credit_signal.py [--horizon 1] [--out outputs/...]
    python -m backend.credit_signal

All inputs are CSVs under data/credit_signal/. The module never makes
network calls at runtime - it is designed to ship and run inside an
air-gapped corporate environment.
"""

__version__ = '0.1.0'

from . import exposures, forecasts, output, ratings, signal, transition_matrix

__all__ = [
    'exposures',
    'forecasts',
    'output',
    'ratings',
    'signal',
    'transition_matrix',
]
