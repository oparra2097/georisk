# Credit Risk Signal Matrix

A country x sector "where to avoid / where the strategy is" signal for the
global credit lines business. Fuses three inputs:

1. **Moody's sovereign 1-year transition matrix** -> baseline PD per
   broad rating.
2. **Agency ratings** (S&P / Moody's / Fitch median, mapped to the
   Moody's broad scale) -> current rating per sovereign.
3. **Commodity forecasts** x **country / sector exposures** -> shock to
   the baseline PD over the forecast horizon.

Output: a per-country sovereign view plus a country x sector matrix with
a 4-way signal label per cell:

- **AVOID** — high cohort PD AND no improvement in trajectory.
- **CAUTION** — high cohort PD with improving trajectory, OR mid PD that
  is deteriorating.
- **NEUTRAL** — mid-pack on both axes.
- **STRATEGIC** — low cohort PD with stable or improving trajectory.

## Quick start (on the work PC, no internet at runtime)

```bash
git pull
pip install -r requirements.txt
python scripts/build_credit_signal.py
# -> outputs/credit_signal/credit_signal_matrix.xlsx
```

Then open `outputs/credit_signal/credit_signal_matrix.xlsx`. Six sheets:

- `Sovereign` — one row per country with baseline & adjusted PD.
- `Country_x_Sector_Long` — long-form 370-row matrix sorted by PD.
- `Signal_Matrix_Wide` — country x sector grid of signal labels.
- `PD_Matrix_Wide` — country x sector grid of adjusted PDs.
- `Regional_Summary` — counts of AVOID/CAUTION/NEUTRAL/STRATEGIC per region.
- `Methodology` — parameters used for this run.

Common variants:

```bash
# 3-year PD horizon, 6-month commodity forecast
python scripts/build_credit_signal.py --horizon 3 --forecast-horizon 6m

# Different output folder
python scripts/build_credit_signal.py --out outputs/q2_strategy_run

# Override any single input CSV
python scripts/build_credit_signal.py --forecast my_commodity_view.csv
```

## Inputs (CSVs under `data/credit_signal/`)

All inputs ship with seed values covering ~37 G20+EM sovereigns and 10
sectors so the build produces a sensible output on the first run.
Replace each file with your house view before publishing.

| File | Schema | What to update |
|------|--------|----------------|
| `moodys_transition_matrix.csv` | 7x8 stochastic, rows=current rating, cols=Aaa..Caa-C,D | Annually, from the latest Moody's "Sovereign Default and Recovery Rates" report |
| `commodity_forecast.csv` | `commodity, current_price, fcst_3m, fcst_6m, fcst_12m, unit, notes` | Each run — pull from your commodities forecast pipeline or analyst consensus |
| `country_commodity_exposure.csv` | `iso3, country, <commodity_1>, ...` (net export % of GDP) | Quarterly, from UNCTAD COMTRADE or World Bank WITS |
| `sector_commodity_sensitivity.csv` | `sector, commodity, beta` (PD-notch change per 10% price move) | Once per calibration cycle; house view |
| `country_sector_weights.csv` | `iso3, sector, book_weight_pct` | From your actual credit-lines exposure breakdown |

The agency rating snapshot is pulled from `data/agency_ratings.csv` (the
file the existing `backend/credit_default/` module already maintains).

## How the math works

Step 1 — sovereign baseline PD from rating:

```
baseline_PD(rating, H) = (M^H)[rating, "D"]
```

`M` is the 1-yr Moody's matrix; raising to the H'th power gives cumulative
default probability over H years.

Step 2 — sovereign commodity shock:

```
shock_pct_gdp = sum_c (net_export_share_pct_gdp[c] * forecast_pct_change[c])
PD_multiplier = clamp(1 - SOVEREIGN_PD_BETA * shock / 100, 0.1, 10)
adjusted_PD = baseline_PD * PD_multiplier
```

Defaults: `SOVEREIGN_PD_BETA = 0.5` (1pp of GDP terms-of-trade improvement
shaves ~0.5% off PD).

Step 3 — sector adjustment on top of sovereign-adjusted PD:

```
notch_shift_sector = -sum_c (beta[sector, c] * forecast_pct_change[c] / 10%)
sector_multiplier  = clamp(1.5 ** notch_shift, 0.1, 10)
sector_PD          = adjusted_sovereign_PD * sector_multiplier
```

Defaults: `SECTOR_PD_NOTCH_MULTIPLIER = 1.5` (each notch of commodity-implied
shift multiplies or divides PD by 1.5).

Step 4 — signal classification (per `signal.classify_signals`):

|                  | Improving (chg <= -10%) | Stable | Deteriorating (chg >= +10%) |
|------------------|-------------------------|--------|------------------------------|
| Top quintile PD  | CAUTION                 | AVOID  | AVOID                        |
| Middle           | NEUTRAL                 | NEUTRAL| CAUTION                      |
| Bottom quintile  | STRATEGIC               | STRATEGIC | NEUTRAL                   |

All thresholds are kwargs on `classify_signals` so you can tune.

## Module layout

```
backend/credit_signal/
  __init__.py
  cli.py               # argparse + build orchestration
  __main__.py          # python -m backend.credit_signal
  transition_matrix.py # Moody's matrix + n-step powers
  ratings.py           # agency rating -> broad rating composite
  exposures.py         # CSV loaders for the three exposure tables
  forecasts.py         # commodity forecast loader
  signal.py            # sovereign + sector signal computation
  output.py            # Excel / CSV writers, regional roll-up
  README.md            # this file

data/credit_signal/
  moodys_transition_matrix.csv      (override; falls back to embedded)
  commodity_forecast.csv            (your forecasts go here)
  country_commodity_exposure.csv    (net export shares)
  sector_commodity_sensitivity.csv  (sector betas)
  country_sector_weights.csv        (your book breakdown)

scripts/
  build_credit_signal.py            # entry-point wrapper
```

## What this does *not* do

- No fitting / calibration. The transition matrix is taken as published;
  the SOVEREIGN_PD_BETA and SECTOR_PD_NOTCH_MULTIPLIER are tuned by hand.
  Calibrating these against historical default events is a follow-on.
- No FX or geopolitical overlay. The existing `backend/credit_default/`
  fitted PD model already accounts for fiscal / external / governance
  fundamentals; the signal here is a *forward-looking commodity overlay*
  on top of a rating-implied baseline, not a replacement.
- No sector-level rating data. Sector PDs inherit from the sovereign
  baseline and shift up/down with commodity moves only. If you want true
  sector ratings (e.g., from Moody's corporate transition matrix), add a
  `sector_baseline_rating.csv` and extend `signal.compute_country_sector_matrix`.
- No live data. By design — runs offline so it works on a locked-down work
  PC. The seed CSVs are illustrative; replace with real exposures before
  publishing signals to the credit business.
