#!/usr/bin/env python3
"""Thin wrapper so analysts can run the signal build from the repo root.

Usage:
    python scripts/build_credit_signal.py
    python scripts/build_credit_signal.py --horizon 3 --forecast-horizon 6m
    python scripts/build_credit_signal.py --out outputs/q2_run/
"""

import os
import sys

# Make `backend` importable when run as a plain script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.credit_signal.cli import main  # noqa: E402

if __name__ == '__main__':
    main()
