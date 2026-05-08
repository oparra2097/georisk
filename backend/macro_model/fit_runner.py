"""
Fit-and-report driver.

Pulls the panel via data.build_panel, derives auxiliary columns, and
iterates over a list of EquationSpecs. Returns {spec.name: EquationFit}
and surfaces per-equation diagnostics. Used both by the CLI smoke tests
and by the Phase G API layer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from backend.macro_model.data import build_panel
from backend.macro_model.equations import ALL_SPECS, derive_auxiliary_columns
from backend.macro_model.estimation import EquationFit, EquationSpec, fit_equation
from backend.macro_model import diagnostics

logger = logging.getLogger(__name__)


@dataclass
class ModelFitReport:
    panel_start: pd.Timestamp
    panel_end:   pd.Timestamp
    n_obs:       int
    fits:        dict[str, EquationFit]

    def to_dict(self):
        return {
            'panel': {
                'start': self.panel_start.date().isoformat(),
                'end':   self.panel_end.date().isoformat(),
                'n_obs': self.n_obs,
            },
            'equations': {name: f.to_dict() for name, f in self.fits.items()},
        }


def fit_all(
    specs: Optional[list[EquationSpec]] = None,
    start: str = '1980-01-01',
    panel: Optional[pd.DataFrame] = None,
) -> ModelFitReport:
    """
    Fit all specs (defaults to ALL_SPECS) against the quarterly panel.

    `panel` can be passed in to bypass the data pipeline (useful in tests).
    """
    specs = specs if specs is not None else list(ALL_SPECS)
    if panel is None:
        panel = build_panel(start=start)
    panel = derive_auxiliary_columns(panel)

    fits: dict[str, EquationFit] = {}
    for spec in specs:
        name = spec.name or spec.dependent
        try:
            fit = fit_equation(panel, spec)
            fits[name] = fit
            logger.info(
                f"fit {name}: lag={fit.chosen_lag}, "
                f"rsq={fit.rsq:.3f}, γ={fit.error_correction_coef():+.3f}, "
                f"DW={fit.durbin_watson:.2f}, N={fit.n_obs}"
            )
            diagnostics.record_fit_ok(
                name=name, rsq=fit.rsq, n_obs=fit.n_obs,
                chosen_lag=fit.chosen_lag, gamma=fit.error_correction_coef(),
            )
        except Exception as e:
            logger.exception(f"fit failed for {name}: {e}")
            diagnostics.record_fit_fail(name, e)

    return ModelFitReport(
        panel_start=panel.index.min(),
        panel_end=panel.index.max(),
        n_obs=len(panel),
        fits=fits,
    )
