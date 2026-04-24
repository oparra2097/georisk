"""
Behavioral-equation specifications for FRB/US-lite.

Each block module (prices, spending, labor, financial) exports one or more
EquationSpec objects via `SPECS`. The top-level `ALL_SPECS` list is the
canonical order the solver iterates over each quarter.

Derived series used by specs (gaps, inflation rates) are added to the
panel by `derive_auxiliary_columns` before estimation or simulation.
"""

import numpy as np

from backend.macro_model.equations import prices, spending, labor, financial

INFLATION_TARGET = 2.0  # % annualized; Fed's stated objective since 2012


def derive_auxiliary_columns(panel):
    """
    Add derived series that appear as regressors in one or more equations.

    Adds:
      unemp_gap   = unemp − nrou
      pi_yoy      = 400 × Δlog(pce_core)         (annualized quarterly %)
      pi_gap      = pi_yoy − INFLATION_TARGET    (gap vs 2% target)
      real_tsy10  = tsy10 − pi_yoy                (ex-post real 10Y yield)
      real_fedfunds = fedfunds − pi_yoy

    Returns a new DataFrame; does not mutate the input.
    """
    out = panel.copy()
    if 'unemp' in out.columns and 'nrou' in out.columns:
        out['unemp_gap'] = out['unemp'] - out['nrou']
    if 'pce_core' in out.columns:
        out['pi_yoy'] = 400.0 * out['pce_core'].diff()
        out['pi_gap'] = out['pi_yoy'] - INFLATION_TARGET
    if 'tsy10' in out.columns and 'pi_yoy' in out.columns:
        out['real_tsy10'] = out['tsy10'] - out['pi_yoy']
    if 'fedfunds' in out.columns and 'pi_yoy' in out.columns:
        out['real_fedfunds'] = out['fedfunds'] - out['pi_yoy']
    return out


ALL_SPECS = [
    *prices.SPECS,
    *spending.SPECS,
    *labor.SPECS,
    *financial.SPECS,
]
