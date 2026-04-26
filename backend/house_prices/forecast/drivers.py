"""
Macro drivers for the HPI forecast model.

Pulls a small set of macro series from FRED, snaps each to a quarterly
end-of-period index, and applies the modelling transform. The HPI side
of the panel is built directly from the existing house_prices.service
summaries / grouped tables — we don't re-download FHFA inside this
module.

Drivers used by the v1 national equation:
    mortgage30   30Y conventional fixed-rate mortgage           (level, %)
    real_income  Real disposable personal income                (log)
    unemp        Civilian unemployment rate                     (level, %)
    fedfunds     Effective federal funds rate                   (level, %)
    cpi          CPI all items                                  (log)

Public:
    Driver registry  (DRIVERS)
    build_panel(start='1980-01-01') -> pd.DataFrame
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from backend.data_sources import fred_client

logger = logging.getLogger(__name__)


Freq = Literal['M', 'Q', 'D']
Transform = Literal['log', 'level']


@dataclass(frozen=True)
class Driver:
    code: str
    fred_id: str
    label: str
    freq: Freq
    transform: Transform
    unit: str = ''


DRIVERS: list[Driver] = [
    Driver('mortgage30', 'MORTGAGE30US', '30Y Fixed Mortgage Rate', 'M', 'level', '%'),
    Driver('real_income', 'DSPIC96',     'Real Disposable Personal Income', 'M', 'log',   'Bn 2017$ SAAR'),
    Driver('unemp',       'UNRATE',      'Civilian Unemployment Rate',       'M', 'level', '%'),
    Driver('fedfunds',    'FEDFUNDS',    'Effective Federal Funds Rate',     'M', 'level', '%'),
    Driver('cpi',         'CPIAUCSL',    'CPI All Items',                    'M', 'log',   'Index'),
]
BY_CODE = {d.code: d for d in DRIVERS}


def _to_quarterly(raw: list[dict], freq: str) -> pd.Series:
    """Snap raw FRED observations to a quarterly end-of-period series.

    For monthly series we average the three months in the quarter; that
    matches the convention used by the macro_model panel builder.
    """
    if not raw:
        return pd.Series(dtype=float)
    df = pd.DataFrame(raw)
    df['date'] = pd.to_datetime(df['date'])
    s = df.set_index('date')['value'].astype(float)
    if freq == 'Q':
        out = s.groupby(s.index.to_period('Q')).last()
    else:
        out = s.groupby(s.index.to_period('Q')).mean()
    out = out.rename_axis('quarter').to_timestamp(how='end')
    out.index = out.index.normalize()
    return out


def _apply_transform(series: pd.Series, transform: str) -> pd.Series:
    if transform == 'log':
        return series.where(series > 0).apply(np.log)
    return series.astype(float)


def _fetch_one(d: Driver, start: str) -> pd.Series:
    raw = fred_client.fetch_series(d.fred_id, start_date=start)
    if not raw:
        logger.warning(f'hpi_forecast.drivers: empty FRED response for {d.code}={d.fred_id}')
        return pd.Series(dtype=float, name=d.code)
    q = _to_quarterly(raw, d.freq)
    q = _apply_transform(q, d.transform).rename(d.code)
    return q


# ── State-specific unemployment ────────────────────────────────────────
#
# FRED publishes an SA monthly unemployment-rate series for every state
# under the consistent code `<STATE>UR` (e.g. CAUR, TXUR, FLUR), starting
# in 1976. State labor markets diverge a lot from the national rate
# (TX in the 1986 oil bust, CA in the 1991 recession, FL post-2008,
# the Sun Belt in 2020); using each state's own unemp instead of the
# national one lifts state-model R² noticeably.

_US_STATE_CODES = (
    'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN',
    'IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH',
    'NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT',
    'VT','VA','WA','WV','WI','WY',
)
STATE_UNEMP_FRED_IDS: dict[str, str] = {c: f'{c}UR' for c in _US_STATE_CODES}


def fetch_state_unemp(state_code: str, start: str = '1980-01-01') -> pd.Series:
    """Quarterly state unemployment rate as a percentage level. Returns
    an empty Series on any FRED failure so the caller can fall back to
    the national series."""
    fred_id = STATE_UNEMP_FRED_IDS.get(state_code.upper())
    if fred_id is None:
        return pd.Series(dtype=float, name='state_unemp')
    try:
        raw = fred_client.fetch_series(fred_id, start_date=start)
    except Exception as e:
        logger.warning(f'hpi_forecast.drivers: state unemp {fred_id} fetch raised: {e}')
        return pd.Series(dtype=float, name='state_unemp')
    if not raw:
        logger.warning(f'hpi_forecast.drivers: state unemp {fred_id} empty')
        return pd.Series(dtype=float, name='state_unemp')
    return _to_quarterly(raw, 'M').rename('state_unemp')


def build_panel(start: str = '1980-01-01') -> pd.DataFrame:
    """Quarterly panel with all macro drivers (no HPI yet — caller joins).

    Trims trailing rows where any driver is NaN, mirroring the macro_model
    pattern: avoids the all-NaN forecast bug when the current quarter's
    monthly aggregates haven't published yet.
    """
    cols = [_fetch_one(d, start) for d in DRIVERS]
    panel = pd.concat(cols, axis=1).sort_index()
    if panel.empty:
        return panel
    panel = panel[panel.index >= pd.Timestamp(start)].dropna(how='all')

    # Trim trailing partial quarters so the last row has every driver filled.
    complete = panel.notna().all(axis=1)
    if complete.any():
        last = complete[complete].index.max()
        trimmed = (panel.index > last).sum()
        if trimmed:
            logger.info(f'hpi_forecast.drivers: trimmed {trimmed} trailing partial rows '
                        f'after {last.date()}')
            panel = panel.loc[:last]
    logger.info(f'hpi_forecast.drivers: panel {panel.shape} '
                f'({panel.index.min().date() if len(panel) else "?"} '
                f'→ {panel.index.max().date() if len(panel) else "?"})')
    return panel
