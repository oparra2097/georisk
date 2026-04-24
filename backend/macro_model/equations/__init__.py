"""
Behavioral-equation specifications for FRB/US-lite.

Each block module (prices, spending, labor, financial) exports one or more
EquationSpec objects via `SPECS`. The top-level `ALL_SPECS` list is the
canonical order the solver iterates over.

Derived series used by specs (unemployment gap, etc.) are added to the
panel by `derive_auxiliary_columns`.
"""

from backend.macro_model.equations import prices


def derive_auxiliary_columns(panel):
    """
    Add derived series that appear as regressors in one or more equations.

    Currently:
      unemp_gap = unemp − nrou   (Okun slack; negative when labor market is tight)

    Returns a new DataFrame; does not mutate the input.
    """
    out = panel.copy()
    if 'unemp' in out.columns and 'nrou' in out.columns:
        out['unemp_gap'] = out['unemp'] - out['nrou']
    return out


ALL_SPECS = [
    *prices.SPECS,
]
