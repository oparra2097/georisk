# Commodity Forecast Methodology

Per-commodity methodology notes for the ParraMacro forecast system.

## How the model works

Every commodity is modeled with a **two-stage hybrid**:

1. **SARIMAX(1,0,1)** on monthly log-returns of the commodity price, with
   exogenous regressors capturing that commodity's primary macro drivers.
2. **GARCH(1,1)** on the SARIMAX residuals so the confidence interval
   widens automatically in volatile regimes instead of assuming constant
   variance.

The forecast is produced by simulating 1,000 future 12-month return paths.
Exogenous drivers are held at their last observed monthly level. For each
simulation we draw innovations from the fitted GARCH conditional
distribution, push them through the SARIMAX state to get log-returns,
compound to prices, and aggregate into four forward quarterly averages.

Outputs per quarter: **median (p50)**, **p2.5**, **p97.5**. These map to
the scenario rows surfaced in the app:

| Group | Worst | Base | Best / upper tail |
|---|---|---|---|
| Metals, Agriculture | p2.5 (Bear) | p50 (Base) | p97.5 (Bull) |
| Oil & Gas | — | p50 (Base Case) | p90 (Severe) → p97.5 (Worst Case) |

Oil & Gas uses the 3-tier disruption gradient (higher price = worse for
consumers); Metals / Ag use symmetric Bear/Base/Bull bands.

## Current-quarter nowcast

For the in-progress quarter we blend QTD realized prices with the model's
Q+0 median:

    nowcast = w · qtd_mean + (1 − w) · model_q0_median
    w = days_elapsed / days_in_quarter

Early in a quarter the model dominates; late in the quarter the QTD
average dominates. The Actual row of the forecast table always shows the
raw QTD mean so operators see both numbers.

## Update cadence

- **Monthly refit**: 1st of each month, 07:00 UTC, via apscheduler job
  `refit_commodity_models` in `backend/scheduler.py`. Fits pickle to
  `backend/cache/commodity_models/` with JSON sidecars.
- **On-demand**: any cached fit older than 35 days is treated as stale
  and refit lazily on next API call.
- **Data refresh**: yfinance daily closes pulled live on forecast request
  (24h cache).
- **Consensus YAML**: `data/consensus.yaml` — operator updates as new
  bank research publishes.

## Consensus benchmarks

Model forecasts are benchmarked against:

- **EIA Short-Term Energy Outlook** (oil, gas) — monthly public CSV.
- **World Bank Commodity Markets Outlook / Pink Sheet** — annual forecasts
  for all commodities.
- **IMF WEO Commodity Outlook** — semi-annual annual averages.
- **Manual YAML** (`data/consensus.yaml`) — Goldman Sachs, J.P. Morgan,
  UBS, Morgan Stanley, Citi etc. pasted in from paywalled research with
  citation.

The per-commodity notes list the consensus sources used and any known
alignment gaps.

## Validation

Walk-forward backtest in `scripts/backtest_commodities.py` reports MAE,
RMSE, MAPE, bias, and 95% CI hit rate per horizon (Q+1 through Q+4).
Well-calibrated bands hit ≈95% of the time; materially less means the CIs
are too tight; materially more means they are too wide. Results written to
`docs/backtest_results.md` on every run.

## Limitations

- Linear regression cannot capture regime shifts (OPEC+ breakup, sanctions
  reversal, new structural demand).
- Several drivers use proxies — e.g. copper price as a leading indicator
  for China manufacturing PMI, seasonal dummies in place of direct rainfall
  anomalies for West Africa. The per-commodity notes flag where.
- 95% CI assumes Gaussian-like residuals; fat tails cause under-coverage
  during commodity shocks (oil, gas, cocoa in particular).
- Current model does not incorporate forward curves, options-implied vol,
  or positioning data.
- TTF gas has a short history in the current regime (post-2022) — expect
  wider CIs and larger errors.

## Files

- `backend/data_sources/commodity_models.py` — model implementation
- `backend/data_sources/commodities_forecast.py` — public API + integration
- `backend/data_sources/consensus_tracker.py` — consensus aggregation
- `data/consensus.yaml` — manual bank consensus
- `scripts/backtest_commodities.py` — walk-forward validation
- `docs/backtest_results.md` — most recent backtest report
