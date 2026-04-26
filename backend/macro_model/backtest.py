"""
Historical backtest: train/test split on the quarterly panel, out-of-sample
forecast comparison, per-variable error metrics.

Workflow:

    1. Build panel (1980Q1 → latest)
    2. Split at `train_end` (e.g. '2019-12-31'): train = panel up to here,
       test   = everything after
    3. Fit all 11 equations on the TRAIN sample only
    4. Starting from the last TRAIN quarter's state, simulate forward over
       the test horizon using actual exogenous paths (so we're testing the
       endogenous dynamics, not the exogenous assumptions)
    5. Compare simulated vs actual for every endogenous variable

Metrics returned per variable:
    rmse, mae       — level error over the test window
    rmse_pct        — RMSE / mean(abs(actual)), coarse scale-free comparison
    directional_acc — share of quarters where Δforecast and Δactual share a sign
    n               — number of test quarters

Exogenous variables (gov, oil, row_gdp, nrou, prod, lfpr) are fed in from
the actual test-sample values, so this is a pure test of the endogenous
equations + solver. Pass `flat_exog=True` to carry-forward instead — then
both endogenous and exogenous forecasts are being tested together.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from backend.macro_model.data import build_panel
from backend.macro_model.equations import derive_auxiliary_columns
from backend.macro_model.fit_runner import fit_all
from backend.macro_model.solver import Simulator
from backend.macro_model.variables import BY_CODE, endogenous

logger = logging.getLogger(__name__)


# ── Error metrics ───────────────────────────────────────────────────────

def _level(series: pd.Series, code: str) -> pd.Series:
    """Invert log transforms so RMSE is reported in interpretable units."""
    v = BY_CODE.get(code)
    if v is not None and v.transform == 'log':
        return np.exp(series)
    return series


def _metrics(forecast: pd.Series, actual: pd.Series) -> dict:
    aligned = pd.concat([forecast.rename('f'), actual.rename('a')], axis=1).dropna()
    if len(aligned) < 2:
        return {'rmse': None, 'mae': None, 'rmse_pct': None, 'directional_acc': None, 'n': int(len(aligned))}
    err = aligned['f'] - aligned['a']
    rmse = float(np.sqrt((err ** 2).mean()))
    mae  = float(err.abs().mean())
    denom = float(aligned['a'].abs().mean())
    rmse_pct = rmse / denom if denom > 0 else None

    # Directional accuracy: share of quarters where sign(Δf) == sign(Δa)
    df = aligned['f'].diff().dropna()
    da = aligned['a'].diff().dropna()
    paired = pd.concat([df.rename('df'), da.rename('da')], axis=1).dropna()
    if len(paired) > 0:
        dir_ok = ((paired['df'] > 0) == (paired['da'] > 0)).mean()
    else:
        dir_ok = None

    return {
        'rmse': rmse,
        'mae': mae,
        'rmse_pct': rmse_pct,
        'directional_acc': float(dir_ok) if dir_ok is not None else None,
        'n': int(len(aligned)),
    }


# ── Backtest ────────────────────────────────────────────────────────────

@dataclass
class BacktestResult:
    train_start: pd.Timestamp
    train_end:   pd.Timestamp
    test_start:  pd.Timestamp
    test_end:    pd.Timestamp
    metrics:     dict[str, dict]      # {code: {rmse, mae, ...}}
    forecast:    pd.DataFrame         # forecast levels over test window
    actual:      pd.DataFrame         # actual levels over test window
    flat_exog:   bool

    def to_dict(self) -> dict:
        def ser(df: pd.DataFrame):
            out = []
            for ts, row in df.iterrows():
                rec = {'quarter': ts.date().isoformat()}
                for col, v in row.items():
                    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                        rec[col] = None
                    else:
                        rec[col] = float(v) if v is not None else None
                out.append(rec)
            return out
        return {
            'window': {
                'train_start': self.train_start.date().isoformat(),
                'train_end':   self.train_end.date().isoformat(),
                'test_start':  self.test_start.date().isoformat(),
                'test_end':    self.test_end.date().isoformat(),
                'flat_exog':   self.flat_exog,
            },
            'metrics': {k: {kk: (round(vv, 4) if isinstance(vv, float) else vv)
                            for kk, vv in v.items()}
                        for k, v in self.metrics.items()},
            'forecast': ser(self.forecast),
            'actual':   ser(self.actual),
        }


def run_backtest(
    train_end: str = '2019-12-31',
    test_end:  Optional[str] = None,
    start:     str = '1980-01-01',
    panel:     Optional[pd.DataFrame] = None,
    flat_exog: bool = False,
) -> BacktestResult:
    """
    Fit on panel up to train_end, simulate forward to test_end, compare.

    `panel` can be injected (for tests). Otherwise built via data.build_panel.
    `flat_exog=False` (default): feed actual exogenous paths into the forecast,
    so only the endogenous dynamics are being tested. Set True to carry-forward
    exogenous and test the full forecast system.
    """
    if panel is None:
        panel = build_panel(start=start)
    panel = derive_auxiliary_columns(panel)

    train_end_ts = pd.Timestamp(train_end)
    test_end_ts  = pd.Timestamp(test_end) if test_end else panel.index.max()
    train = panel.loc[:train_end_ts]
    test  = panel.loc[train_end_ts + pd.Timedelta(days=1):test_end_ts]

    if len(train) < 40:
        raise ValueError(f'train sample too short: {len(train)} quarters')
    if len(test) < 1:
        raise ValueError('test sample is empty')

    logger.info(f'backtest: train {train.index[0].date()}→{train.index[-1].date()} ({len(train)} q); '
                f'test {test.index[0].date()}→{test.index[-1].date()} ({len(test)} q)')

    # Fit on training data only
    report = fit_all(panel=train)

    # Build simulator seeded with training data
    sim = Simulator(report.fits, train.copy())

    # Prepare exogenous paths: either actual (oracle exog) or flat
    exog_cols = [c for c in ('gov', 'oil', 'row_gdp', 'nrou', 'prod', 'lfpr')
                 if c in panel.columns]
    horizon = len(test)
    if flat_exog:
        exog_paths = pd.DataFrame(
            {c: np.full(horizon, float(train.iloc[-1][c])) for c in exog_cols},
            index=test.index,
        )
    else:
        exog_paths = test[exog_cols].copy()

    forecast_df = sim.forecast(horizon=horizon, exog_paths=exog_paths)

    # Collect per-variable metrics in interpretable units
    endog_codes = [v.code for v in endogenous() if v.code in panel.columns]
    metrics: dict[str, dict] = {}
    forecast_lvl = pd.DataFrame(index=forecast_df.index)
    actual_lvl   = pd.DataFrame(index=test.index)
    for code in endog_codes:
        f = _level(forecast_df[code], code)
        a = _level(test[code], code)
        forecast_lvl[code] = f
        actual_lvl[code] = a
        metrics[code] = _metrics(f, a)

    return BacktestResult(
        train_start=train.index.min(),
        train_end=train.index.max(),
        test_start=test.index.min(),
        test_end=test.index.max(),
        metrics=metrics,
        forecast=forecast_lvl,
        actual=actual_lvl,
        flat_exog=flat_exog,
    )
