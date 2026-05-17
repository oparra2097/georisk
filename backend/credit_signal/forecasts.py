"""Commodity forecast loader.

Schema for data/credit_signal/commodity_forecast.csv:
    commodity, current_price, fcst_3m, fcst_6m, fcst_12m, unit, notes

The commodity column should use the same short names that appear in
country_commodity_exposure.csv and sector_commodity_sensitivity.csv
(brent_crude, natural_gas_hh, copper, gold, wheat, ...).

You can populate this from any source: your existing
backend/data_sources/commodities_forecast.py output, a Bloomberg pull,
analyst consensus, or hand-typed scenarios.
"""

import os

import pandas as pd

_DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', '..',
    'data', 'credit_signal', 'commodity_forecast.csv',
)


def load_forecast(path=None):
    return pd.read_csv(path or _DEFAULT_PATH, comment='#')


def forecast_pct_change(df, horizon='12m'):
    """Series: commodity -> forecast % change vs current price."""
    col = f'fcst_{horizon}'
    if col not in df.columns:
        raise KeyError(
            f'forecast column {col!r} missing; have {df.columns.tolist()}'
        )
    pct = (df[col] - df['current_price']) / df['current_price']
    return pd.Series(pct.values, index=df['commodity'])
