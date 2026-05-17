"""CLI entry point: build the credit signal matrix from CSV inputs."""

import argparse
import sys
from datetime import datetime

from . import exposures, forecasts, output, ratings, signal, transition_matrix


def main(argv=None):
    p = argparse.ArgumentParser(
        description='Build the credit risk signal matrix '
                    '(sovereign + sectors).')
    p.add_argument('--horizon', type=int, default=1, choices=[1, 3, 5],
                   help='PD horizon in years (default: 1)')
    p.add_argument('--forecast-horizon', default='12m',
                   choices=['3m', '6m', '12m'],
                   help='Commodity forecast horizon column to use '
                        '(default: 12m)')
    p.add_argument('--out', default='outputs/credit_signal',
                   help='Output directory (default: outputs/credit_signal)')
    p.add_argument('--ratings', default=None,
                   help='Override agency_ratings.csv path')
    p.add_argument('--matrix', default=None,
                   help="Override Moody's transition matrix CSV path")
    p.add_argument('--exposure', default=None,
                   help='Override country_commodity_exposure.csv path')
    p.add_argument('--sensitivity', default=None,
                   help='Override sector_commodity_sensitivity.csv path')
    p.add_argument('--weights', default=None,
                   help='Override country_sector_weights.csv path')
    p.add_argument('--forecast', default=None,
                   help='Override commodity_forecast.csv path')
    args = p.parse_args(argv)

    log = lambda *a: print(*a, file=sys.stderr)

    log('[1/5] Loading inputs...')
    M = transition_matrix.load_matrix(args.matrix)
    R = ratings.load_ratings(args.ratings)
    E = exposures.load_country_commodity_exposure(args.exposure)
    S = exposures.load_sector_commodity_sensitivity(args.sensitivity)
    W = exposures.load_country_sector_weights(args.weights)
    F = forecasts.load_forecast(args.forecast)
    fpct = forecasts.forecast_pct_change(F, args.forecast_horizon)
    log(f'  ratings: {len(R)} sovereigns')
    log(f'  country commodity-exposure rows: {len(E)}')
    log(f'  sector beta rows: {len(S)} '
        f'(sectors: {S["sector"].nunique()})')
    log(f'  country-sector weight rows: {len(W)}')
    log(f'  commodity forecast: {len(F)} series '
        f'@ horizon {args.forecast_horizon}')

    log('[2/5] Computing sovereign signals...')
    sov = signal.compute_sovereign_signal(
        R, E, fpct, horizon=args.horizon, matrix=M)
    log(f'  scored {len(sov)} sovereigns')

    log('[3/5] Computing country x sector matrix...')
    sector = signal.compute_country_sector_matrix(sov, W, S, fpct)
    log(f'  built {len(sector)} country-sector cells')

    log('[4/5] Writing outputs...')
    methodology = {
        'Generated UTC': datetime.utcnow().isoformat(timespec='seconds'),
        'PD horizon (years)': args.horizon,
        'Forecast horizon column': args.forecast_horizon,
        'Sovereigns scored': len(sov),
        'Country-sector cells': len(sector),
        'Sovereign PD beta (per 1pp GDP shock)': signal.SOVEREIGN_PD_BETA,
        'Sector PD multiplier per notch': signal.SECTOR_PD_NOTCH_MULTIPLIER,
        'PD multiplier floor / ceiling': f'{signal.PD_MULT_FLOOR} / {signal.PD_MULT_CEIL}',
        "Moody's matrix source": args.matrix or 'embedded fallback',
        'Signal thresholds': 'level: top/bottom quintile; '
                             'trajectory: +/-10% PD change vs baseline',
    }
    paths = output.write_outputs(sov, sector, methodology, args.out)
    log('[5/5] Done. Outputs:')
    for k, v in paths.items():
        log(f'  {k}: {v}')

    log('\nSovereign signal preview (top by adjusted PD):')
    preview_cols = ['iso3', 'broad_rating', 'baseline_pd',
                    'adjusted_pd', 'pd_change_pct', 'signal']
    log(sov.sort_values('adjusted_pd', ascending=False)
            [preview_cols].head(15).to_string(index=False))


if __name__ == '__main__':
    main()
