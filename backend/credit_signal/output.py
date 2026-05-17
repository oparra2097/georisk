"""Write Excel (multi-sheet) and CSV outputs for the credit signal matrix."""

import os

import pandas as pd

# ISO3 -> business region. Extend as needed for additional country coverage.
REGIONS = {
    'USA': 'North America', 'CAN': 'North America', 'MEX': 'North America',
    'BRA': 'LATAM', 'ARG': 'LATAM', 'COL': 'LATAM', 'CHL': 'LATAM',
    'PER': 'LATAM', 'VEN': 'LATAM',
    'GBR': 'Europe', 'FRA': 'Europe', 'DEU': 'Europe', 'ITA': 'Europe',
    'ESP': 'Europe', 'NLD': 'Europe', 'POL': 'Europe', 'CHE': 'Europe',
    'SWE': 'Europe', 'NOR': 'Europe',
    'RUS': 'CEEMEA', 'TUR': 'CEEMEA', 'SAU': 'CEEMEA', 'ARE': 'CEEMEA',
    'ZAF': 'CEEMEA', 'NGA': 'CEEMEA', 'EGY': 'CEEMEA', 'QAT': 'CEEMEA',
    'KAZ': 'CEEMEA',
    'CHN': 'APAC', 'JPN': 'APAC', 'KOR': 'APAC', 'IND': 'APAC',
    'IDN': 'APAC', 'AUS': 'APAC', 'THA': 'APAC', 'MYS': 'APAC',
    'VNM': 'APAC', 'PHL': 'APAC',
}


def write_outputs(sovereign_df, sector_df, methodology, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    xlsx_path = os.path.join(out_dir, 'credit_signal_matrix.xlsx')
    csv_long_path = os.path.join(out_dir, 'credit_signal_long.csv')
    csv_sov_path = os.path.join(out_dir, 'credit_signal_sovereign.csv')

    sov = sovereign_df.copy()
    sov['region'] = sov['iso3'].map(REGIONS).fillna('Other')
    sov = sov.sort_values(['signal_order', 'adjusted_pd']
                          if 'signal_order' in sov.columns
                          else ['adjusted_pd'],
                          ascending=[False, False] if 'signal_order' in sov.columns
                          else False)
    sov_sorted = sovereign_df.sort_values('adjusted_pd', ascending=False)

    with pd.ExcelWriter(xlsx_path, engine='openpyxl') as w:
        sov_sorted.to_excel(w, sheet_name='Sovereign', index=False)
        if not sector_df.empty:
            sector_sorted = sector_df.sort_values(
                ['sector_pd', 'book_weight_pct'], ascending=[False, False])
            sector_sorted.to_excel(w, sheet_name='Country_x_Sector_Long',
                                   index=False)
            wide_signal = sector_df.pivot_table(
                index=['iso3', 'country'], columns='sector',
                values='signal', aggfunc='first')
            wide_signal.to_excel(w, sheet_name='Signal_Matrix_Wide')
            wide_pd = sector_df.pivot_table(
                index=['iso3', 'country'], columns='sector',
                values='sector_pd', aggfunc='first')
            wide_pd.to_excel(w, sheet_name='PD_Matrix_Wide')
        regional = _regional_rollup(sovereign_df)
        regional.to_excel(w, sheet_name='Regional_Summary', index=False)
        pd.DataFrame(list(methodology.items()),
                     columns=['Item', 'Value']).to_excel(
            w, sheet_name='Methodology', index=False)

    sector_df.to_csv(csv_long_path, index=False)
    sov_sorted.to_csv(csv_sov_path, index=False)
    return {'xlsx': xlsx_path,
            'sovereign_csv': csv_sov_path,
            'matrix_csv': csv_long_path}


def _regional_rollup(sov):
    sov = sov.copy()
    sov['region'] = sov['iso3'].map(REGIONS).fillna('Other')
    grouped = sov.groupby('region').agg(
        countries=('iso3', 'count'),
        median_baseline_pd=('baseline_pd', 'median'),
        median_adjusted_pd=('adjusted_pd', 'median'),
        median_pd_change_pct=('pd_change_pct', 'median'),
        avoid=('signal', lambda s: int((s == 'AVOID').sum())),
        caution=('signal', lambda s: int((s == 'CAUTION').sum())),
        neutral=('signal', lambda s: int((s == 'NEUTRAL').sum())),
        strategic=('signal', lambda s: int((s == 'STRATEGIC').sum())),
    ).reset_index()
    return grouped
