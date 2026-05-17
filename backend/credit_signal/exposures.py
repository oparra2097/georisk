"""Loaders for the country/sector exposure CSVs.

All three files live under data/credit_signal/ and ship with seed values
covering ~30 G20+EM sovereigns and ~10 sectors. Replace with your book's
actual exposures before publishing signals to the business.

Schemas:
    country_commodity_exposure.csv
        iso3, country, <commodity_1>, <commodity_2>, ...
        Values are net exports as % of GDP. Positive = net exporter
        (benefits from rising price). Negative = net importer.

    sector_commodity_sensitivity.csv
        sector, commodity, beta
        beta = PD-notch change per 10% commodity price move. Positive beta
        means the sector benefits from rising prices.

    country_sector_weights.csv
        iso3, sector, book_weight_pct
        Relative book exposure to each (country, sector) cell. Drives the
        regional roll-up and lets the matrix be sorted by impact, not just
        signal severity.
"""

import os

import pandas as pd

_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', '..',
    'data', 'credit_signal',
)


def load_country_commodity_exposure(path=None):
    path = path or os.path.join(_DATA_DIR, 'country_commodity_exposure.csv')
    return pd.read_csv(path, comment='#')


def load_sector_commodity_sensitivity(path=None):
    path = path or os.path.join(_DATA_DIR, 'sector_commodity_sensitivity.csv')
    return pd.read_csv(path, comment='#')


def load_country_sector_weights(path=None):
    path = path or os.path.join(_DATA_DIR, 'country_sector_weights.csv')
    return pd.read_csv(path, comment='#')
