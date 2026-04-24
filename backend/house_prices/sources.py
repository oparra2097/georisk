"""
Source catalogue and level registry for the HPI product.

One place to edit if FHFA changes a URL, Zillow rotates a filename, or a
new Case-Shiller city series gets added.
"""

from dataclasses import dataclass
from typing import Literal


Frequency = Literal['monthly', 'quarterly', 'annual']
Level = Literal['national', 'region', 'state', 'msa', 'county', 'zip']


@dataclass(frozen=True)
class Source:
    id: str
    name: str
    publisher: str
    url: str
    license: str
    freq: Frequency
    lag_days: int   # typical reporting lag
    levels: tuple[Level, ...]


SOURCES: list[Source] = [
    Source(
        id='fhfa_master',
        name='FHFA House Price Index (All Transactions)',
        publisher='Federal Housing Finance Agency',
        url='https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_master.csv',
        license='Public domain (US Govt)',
        freq='quarterly',
        lag_days=75,
        levels=('national', 'region', 'state', 'msa'),
    ),
    Source(
        id='fhfa_county',
        name='FHFA County HPI (Developmental)',
        publisher='Federal Housing Finance Agency',
        url='https://www.fhfa.gov/hpi/download/annually_datasets/hpi_at_bdl_county.csv',
        license='Public domain (US Govt)',
        freq='annual',
        lag_days=180,
        levels=('county',),
    ),
    Source(
        id='case_shiller',
        name='S&P/Case-Shiller Home Price Indices',
        publisher='S&P Dow Jones Indices (via FRED)',
        url='https://fred.stlouisfed.org/series/CSUSHPINSA',
        license='FRED — public use with attribution',
        freq='monthly',
        lag_days=60,
        levels=('national', 'msa'),  # 20 metros available
    ),
    Source(
        id='zillow_metro',
        name='Zillow Home Value Index — Metro (ZHVI)',
        publisher='Zillow Research',
        url='https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv',
        license='CC BY 4.0 (attribution required)',
        freq='monthly',
        lag_days=30,
        levels=('msa', 'national'),
    ),
    Source(
        id='zillow_county',
        name='Zillow Home Value Index — County (ZHVI)',
        publisher='Zillow Research',
        url='https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv',
        license='CC BY 4.0 (attribution required)',
        freq='monthly',
        lag_days=30,
        levels=('county',),
    ),
    Source(
        id='zillow_zip',
        name='Zillow Home Value Index — ZIP (ZHVI)',
        publisher='Zillow Research',
        url='https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv',
        license='CC BY 4.0 (attribution required)',
        freq='monthly',
        lag_days=30,
        levels=('zip',),
    ),
]


def for_level(level: Level) -> list[Source]:
    return [s for s in SOURCES if level in s.levels]


# US Census regions (as reported by FHFA)
CENSUS_REGIONS = {
    'NE': {'name': 'Northeast', 'states': ['CT','ME','MA','NH','RI','VT','NJ','NY','PA']},
    'MW': {'name': 'Midwest',   'states': ['IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD']},
    'S':  {'name': 'South',     'states': ['DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX']},
    'W':  {'name': 'West',      'states': ['AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA']},
}


# Case-Shiller 20-city composite members — FRED series IDs.
# Format: {city: (metro_name, fred_series_id_NSA)}
CASE_SHILLER_CITIES: dict[str, tuple[str, str]] = {
    'ATLANTA':     ('Atlanta-Sandy Springs-Alpharetta, GA',       'ATXRNSA'),
    'BOSTON':      ('Boston-Cambridge-Newton, MA-NH',              'BOXRNSA'),
    'CHARLOTTE':   ('Charlotte-Concord-Gastonia, NC-SC',           'CRXRNSA'),
    'CHICAGO':     ('Chicago-Naperville-Elgin, IL-IN-WI',          'CHXRNSA'),
    'CLEVELAND':   ('Cleveland-Elyria, OH',                        'CEXRNSA'),
    'DALLAS':      ('Dallas-Fort Worth-Arlington, TX',             'DAXRNSA'),
    'DENVER':      ('Denver-Aurora-Lakewood, CO',                  'DNXRNSA'),
    'DETROIT':     ('Detroit-Warren-Dearborn, MI',                 'DEXRNSA'),
    'LAS_VEGAS':   ('Las Vegas-Henderson-Paradise, NV',            'LVXRNSA'),
    'LOS_ANGELES': ('Los Angeles-Long Beach-Anaheim, CA',          'LXXRNSA'),
    'MIAMI':       ('Miami-Fort Lauderdale-Pompano Beach, FL',     'MIXRNSA'),
    'MINNEAPOLIS': ('Minneapolis-St. Paul-Bloomington, MN-WI',     'MNXRNSA'),
    'NEW_YORK':    ('New York-Newark-Jersey City, NY-NJ-PA',       'NYXRNSA'),
    'PHOENIX':     ('Phoenix-Mesa-Chandler, AZ',                   'PHXRNSA'),
    'PORTLAND':    ('Portland-Vancouver-Hillsboro, OR-WA',         'POXRNSA'),
    'SAN_DIEGO':   ('San Diego-Chula Vista-Carlsbad, CA',          'SDXRNSA'),
    'SAN_FRANCISCO':('San Francisco-Oakland-Berkeley, CA',         'SFXRNSA'),
    'SEATTLE':     ('Seattle-Tacoma-Bellevue, WA',                 'SEXRNSA'),
    'TAMPA':       ('Tampa-St. Petersburg-Clearwater, FL',         'TPXRNSA'),
    'WASHINGTON':  ('Washington-Arlington-Alexandria, DC-VA-MD-WV','WDXRNSA'),
}
CASE_SHILLER_NATIONAL_FRED = 'CSUSHPINSA'
