"""
Variable registry for the FRB/US-lite US macro model.

Each entry specifies the FRED series ID, raw frequency, the transformation
applied before estimation, and which block the variable belongs to. The
system solves over the transformed series (log-levels for quantities,
percent-levels for rates), and inverts back to FRED units at report time.

Transform legend:
    log     — natural log of the series (stationarity after first-diff)
    level   — use raw values (suitable for rates already in %)
    pct     — already in percent (e.g. rates), no transform

Block legend:
    spending   C, I, NX, GDP identity
    prices     PCE/CPI, wages, expectations
    labor      employment, unemployment, LFPR, productivity
    financial  Fed funds, 10Y, DXY
    foreign    ROW GDP, oil — exogenous in v1
"""

from dataclasses import dataclass
from typing import Literal

Freq = Literal['M', 'Q', 'D']
Transform = Literal['log', 'level', 'pct']
Block = Literal['spending', 'prices', 'labor', 'financial', 'foreign']


@dataclass(frozen=True)
class Variable:
    code: str            # our short name used throughout the model
    fred_id: str         # series ID on FRED
    label: str           # human-readable label
    freq: Freq           # native FRED frequency
    transform: Transform # transformation applied before estimation
    block: Block
    endogenous: bool = True
    unit: str = ''


# Registry: ordered for determinism in the solver and reporting.
# Codes are stable public identifiers; don't rename without a migration.
VARIABLES: list[Variable] = [
    # ── Spending ──────────────────────────────────────────────────────
    Variable('gdp',   'GDPC1',    'Real GDP',                              'Q', 'log',   'spending', unit='Bn 2017$ SAAR'),
    Variable('cons',  'PCECC96',  'Real Personal Consumption Expenditures','Q', 'log',   'spending', unit='Bn 2017$ SAAR'),
    Variable('inv',   'GPDIC1',   'Real Gross Private Domestic Investment','Q', 'log',   'spending', unit='Bn 2017$ SAAR'),
    Variable('exp',   'EXPGSC1',  'Real Exports of Goods and Services',    'Q', 'log',   'spending', unit='Bn 2017$ SAAR'),
    Variable('imp',   'IMPGSC1',  'Real Imports of Goods and Services',    'Q', 'log',   'spending', unit='Bn 2017$ SAAR'),
    Variable('gov',   'GCEC1',    'Real Government Consumption+Invest.',   'Q', 'log',   'spending', endogenous=False, unit='Bn 2017$ SAAR'),

    # ── Prices ────────────────────────────────────────────────────────
    Variable('pce_core', 'PCEPILFE', 'Core PCE Price Index',     'M', 'log',   'prices', unit='2017=100'),
    Variable('cpi',      'CPIAUCSL', 'CPI All Items',            'M', 'log',   'prices', unit='1982-84=100'),
    Variable('wage',     'AHETPI',   'Avg Hourly Earnings (prod)', 'M', 'log', 'prices', unit='$/hr'),

    # ── Labor ─────────────────────────────────────────────────────────
    Variable('unemp',  'UNRATE',   'Unemployment Rate',             'M', 'pct',   'labor', unit='%'),
    Variable('emp',    'PAYEMS',   'Total Nonfarm Payrolls',        'M', 'log',   'labor', unit='Thous. persons'),
    Variable('lfpr',   'CIVPART',  'Labor Force Participation',     'M', 'pct',   'labor', unit='%'),
    Variable('nrou',   'NROU',     'Natural Rate of Unemployment (CBO)', 'Q', 'pct', 'labor', endogenous=False, unit='%'),
    Variable('prod',   'OPHNFB',   'Nonfarm Business Productivity', 'Q', 'log',   'labor', endogenous=False, unit='Index 2017=100'),

    # ── Financial ─────────────────────────────────────────────────────
    Variable('fedfunds', 'FEDFUNDS', 'Effective Federal Funds Rate', 'M', 'pct',   'financial', unit='%'),
    Variable('tsy10',    'DGS10',    '10-Year Treasury Constant Maturity', 'D', 'pct', 'financial', unit='%'),
    Variable('dxy',      'DTWEXBGS', 'Broad USD Index (Nominal)',    'D', 'log',   'financial', unit='Jan 2006=100'),

    # ── Foreign / commodity (exogenous in v1) ─────────────────────────
    Variable('oil',    'DCOILWTICO',         'WTI Crude Oil Spot',     'D', 'log', 'foreign', endogenous=False, unit='$/bbl'),
    Variable('row_gdp','NAEXKP01OEQ661S',    'OECD Total Real GDP',    'Q', 'log', 'foreign', endogenous=False, unit='Index 2015=100'),
]


BY_CODE = {v.code: v for v in VARIABLES}
BY_FRED = {v.fred_id: v for v in VARIABLES}


def get(code: str) -> Variable:
    return BY_CODE[code]


def endogenous() -> list[Variable]:
    return [v for v in VARIABLES if v.endogenous]


def exogenous() -> list[Variable]:
    return [v for v in VARIABLES if not v.endogenous]


def by_block(block: Block) -> list[Variable]:
    return [v for v in VARIABLES if v.block == block]
