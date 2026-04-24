# Parra Macro Model (FRB/US-lite)

A reduced-form econometric model of the US economy — 11 estimated behavioral
equations plus 2 identities — in the spirit of the Federal Reserve Board's
FRB/US model, substantially simplified for a maintainable v1. This is a
separate product under the Parra Macro brand, served at `/macro-model` and
under the API namespace `/api/macro-model/us/*`.

## Why this exists

The GeoRisk product on this site is a risk scorecard: a weighted composite
of observed indicators. This model is a different class of thing — a real
macro model with equations, endogenous dynamics, and simultaneous solution.
It was built because a scorecard cannot answer questions like _"what
happens to US inflation six quarters after a 100bp Fed hike?"_ — that
requires impulse-response functions, which require equations.

## Architecture at a glance

```
backend/macro_model/
├── variables.py       Variable registry: FRED IDs, transforms, blocks
├── data.py            Quarterly panel builder (monthly/daily → quarterly)
├── estimation.py      Engle-Granger 2-step ECM with AIC lag selection
├── equations/
│   ├── prices.py      Phillips curve + wage equation
│   ├── spending.py    C, I, X, M
│   ├── labor.py       payrolls, unemployment
│   └── financial.py   Fed funds (Taylor rule), 10Y, DXY
├── fit_runner.py      Fits all equations against a panel
├── solver.py          Gauss-Seidel simultaneous solver with SOR damping
├── simulations.py     Baseline forecast, bootstrap fan chart, shock IRFs
├── backtest.py        Train/test split + per-variable OOS error metrics
├── service.py         Cached service facade for the API
└── routes.py          Flask blueprint /api/macro-model/us/*
```

Every module is unit-testable in isolation.

## Model specification

### Variables (19 total; 11 endogenous, 8 exogenous)

| Block      | Variable | FRED | Transform | Endogenous? |
|------------|----------|------|-----------|-------------|
| spending   | `gdp`    | `GDPC1` | log | yes (identity) |
| spending   | `cons`   | `PCECC96` | log | yes |
| spending   | `inv`    | `GPDIC1` | log | yes |
| spending   | `exp`    | `EXPGSC1` | log | yes |
| spending   | `imp`    | `IMPGSC1` | log | yes |
| spending   | `gov`    | `GCEC1` | log | no |
| prices     | `pce_core` | `PCEPILFE` | log | yes |
| prices     | `cpi`    | `CPIAUCSL` | log | yes (identity) |
| prices     | `wage`   | `AHETPI` | log | yes |
| labor      | `unemp`  | `UNRATE` | pct | yes |
| labor      | `emp`    | `PAYEMS` | log | yes |
| labor      | `lfpr`   | `CIVPART` | pct | no |
| labor      | `nrou`   | `NROU`   | pct | no |
| labor      | `prod`   | `OPHNFB` | log | no |
| financial  | `fedfunds` | `FEDFUNDS` | pct | yes |
| financial  | `tsy10`  | `DGS10`  | pct | yes |
| financial  | `dxy`    | `DTWEXBGS` | log | yes |
| foreign    | `oil`    | `DCOILWTICO` | log | no |
| foreign    | `row_gdp` | `NAEXKP01OEQ661S` | log | no |

### Equation form

Engle-Granger two-step error-correction:

```
Stage 1 (long-run):   Y_t = δ_0 + Σ δ_j · X_{j,t} + u_t
Stage 2 (short-run):  ΔY_t = α + γ · u_{t-1}
                           + Σ β_{jk} · ΔX_{j,t-k}           (diff regressors)
                           + Σ δ_{jk} · X_{j,t-k}             (level regressors, e.g. unemp_gap)
                           + ρ_k · ΔY_{t-k}                    (AR terms, optional)
                           + ε_t
```

γ is the error-correction speed. A well-specified equation has `−1 < γ < 0`.
Lag order `k ∈ {1..4}` is selected by AIC.

Equations without a clear cointegrating level relationship (Taylor rule, DXY,
unemployment) are estimated short-run-only — `long_run=[]` in the spec.

### Identities

- **GDP:** `log(gdp) = log(exp(cons) + exp(inv) + exp(gov) + exp(exp) − exp(imp))`
- **CPI:** `cpi_t = cpi_{t-1} + Δlog(pce_core)_t`

### Solver

Damped Gauss-Seidel (SOR, ω=0.5). Each quarter, iterate until max change
across endogenous variables is below tol (default 1e-3) or max_iter (100)
is reached.

## API

Base path: `/api/macro-model/us`

| Method | Path | Purpose |
|---|---|---|
| GET  | `/status` | Build state + fit-error diagnostics |
| GET  | `/variables` | Variable registry |
| GET  | `/fit` | Per-equation diagnostic reports (R², γ, DW, coint-p, AIC) |
| GET  | `/forecast?horizon=20` | Deterministic baseline forecast (levels) |
| GET  | `/fan?horizon=12&n_draws=30` | Bootstrap fan chart (p10/p50/p90) |
| GET  | `/shocks` | Shock catalogue |
| POST | `/shock` | Run shock by id → IRF + paths |
| GET  | `/backtest?train_end=2019-12-31&flat_exog=0` | Historical OOS backtest |
| POST | `/refresh` | Force rebuild (new fit + forecasts) |

## Deployment

The model deploys with the rest of the Parra Macro Flask app. To bring it
online in production:

1. **Set FRED_API_KEY** in the environment. Register at
   https://fred.stlouisfed.org/docs/api/api_key.html — free, instant.
2. **Deploy the branch** `claude/oxford-economics-estimate-2h9Fd` to
   Render/Heroku the same way any other branch is deployed. No new env
   vars or services needed beyond `FRED_API_KEY`.
3. **First request triggers a build** (fetch FRED + fit 11 equations);
   takes ~30-60s. Subsequent requests are cached by the service layer.
4. **Warm cache** (optional): hit `POST /api/macro-model/us/refresh` once
   after deploy so the first real user doesn't wait for the fit.

No database migrations, no new dependencies beyond what's already in
`requirements.txt` (numpy, pandas, statsmodels, scipy, Flask).

### Running the backtest

```
curl 'https://parramacro.com/api/macro-model/us/backtest?train_end=2019-12-31'
```

Returns per-variable RMSE, MAE, and directional accuracy of the out-of-sample
forecast against actual FRED values. `flat_exog=1` carries exogenous forward
instead of feeding actuals — a harder test of the full forecast system.

### Keeping the model fresh

- FRED data updates continuously; the data layer caches for 24h (see
  `backend/data_sources/fred_client.py:CACHE_TTL`). The full fit is cached
  in memory until `/refresh` is called.
- Recommended: add a scheduler job to call `service.refresh()` weekly so
  the model picks up new data without manual intervention.

## Known limitations

1. **Point estimates, not Bayesian.** No shrinkage; OLS can overfit on short
   sub-samples. Eventually replace with Bayesian VAR priors.
2. **Adaptive expectations.** No model-consistent / rational-expectations
   solve. Inflation expectations are captured via lagged inflation; longer-run
   proxies would need SPF or TIPS-derived data.
3. **No fiscal block.** Government spending is exogenous. A fiscal-rule
   equation (G responds to debt/GDP and output gap) is a natural extension.
4. **Foreign block is thin.** ROW GDP is exogenous; there's no foreign
   inflation or rate feedback. Adding a two-country block (US vs ROW) is
   the natural v2 step.
5. **No structural breaks.** Coefficients are stable across the full sample.
   In reality, Volcker disinflation, the 2008 crisis, and COVID likely shift
   relationships. A rolling-window or time-varying-parameter version is a v2
   extension.
6. **Two-step Engle-Granger has attenuation bias on γ.** Moving to one-step
   nonlinear least squares on the ECM would sharpen the equilibrium-adjustment
   coefficient at the cost of implementation complexity.

## Quick smoke test

With Python ≥ 3.10 and the repo's `requirements.txt` installed:

```bash
# Headless: fit all equations on a mocked panel, print diagnostics
python -c "
from backend.macro_model.fit_runner import fit_all
# (requires FRED_API_KEY to actually hit FRED)
report = fit_all()
for name, fit in report.fits.items():
    print(f'{name:<38s} R²={fit.rsq:.3f}  γ={fit.error_correction_coef():+.3f}')
"
```

## Package vs main site

The model is a separate product under the Parra Macro umbrella:

- Separate URL: `/macro-model` (distinct from `/georisk`, `/data`, etc.)
- Separate API namespace: `/api/macro-model/us/*`
- Separate backend package: `backend/macro_model/`
- Separate concerns: no shared state with GeoRisk scoring

The nav link in `templates/base.html` lives next to GeoRisk; if you want it
gated (e.g. insurance users only, or logged-in only), wrap the route in
`@login_required` or the existing `insurance_access_required` decorator in
`app.py`.
