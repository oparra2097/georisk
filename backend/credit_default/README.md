# Sovereign Credit Default Model

A probability-of-default and internal credit rating model for ~190 sovereigns,
served at `/credit-default` and the `/api/credit-default/*` blueprint.

## What it produces

- **Internal rating** on a 1–10 ladder with `+/−` modifiers on the 2–6 grades
  (so 20 distinct notches: `1, 2+, 2, 2−, 3+, …, 6−, 7, 8, 9, 10`). 1 is the
  strongest credit; 10 means in default.
- **Probability of default** at 1, 3 and 5 year horizons, calibrated empirically
  from the realized default frequency by predicted-score bucket.
- **Side-by-side agency rating** (S&P, Moody's, Fitch) for benchmarking — these
  ratings are loaded from `data/agency_ratings.csv` for **display only** and are
  never used as a fitting target.

## Independent variables

Indicators are pulled from public APIs (IMF WEO datamapper, World Bank
WDI/IDS, World Bank WGI), plus the local sovereign-debt overlay:

| Block | Indicator | Source code | Weight |
| --- | --- | --- | --- |
| Public debt (43%) | Gross govt debt / GDP | WEO `GGXWDG_NGDP` | 0.17 |
| Public debt | Interest / revenue | WB `GC.XPN.INTP.RV.ZS` | 0.11 |
| Public debt | Fiscal balance / GDP | WEO `GGXCNL_NGDP` | 0.09 |
| Public debt | Shadow debt gap (estimated − official, pp) | local `sovereign_debt.json` | 0.06 |
| External (29%) | Current account / GDP | WEO `BCA_NGDPD` | 0.08 |
| External | Reserves / imports (months) — *import cover* | WB `FI.RES.TOTL.MO` | 0.08 |
| External | Short-term external debt / reserves | WB `DT.DOD.DSTC.IR.ZS` | 0.07 |
| External | Total external debt / GNI | WB `DT.DOD.DECT.GN.ZS` | 0.06 |
| Real (13%) | Real GDP growth | WEO `NGDP_RPCH` | 0.06 |
| Real | CPI inflation | WEO `PCPIPCH` | 0.04 |
| Real | GDP per capita (PPP) | WEO `PPPPC` | 0.03 |
| Governance (15%) | Rule of Law (WGI) | WB `RL.EST` | 0.04 |
| Governance | Control of Corruption (WGI) | WB `CC.EST` | 0.03 |
| Governance | Government Effectiveness (WGI) | WB `GE.EST` | 0.03 |
| Governance | Regulatory Quality (WGI) | WB `RQ.EST` | 0.02 |
| Governance | Political Stability (WGI) | WB `PV.EST` | 0.02 |
| Governance | Voice & Accountability (WGI) | WB `VA.EST` | 0.01 |

The shadow-debt gap is the only non-API input — it comes from the
`sovereign_debt.json` overlay (your existing pipeline) which already harmonizes
BIS claims, Chinese lending, and IMF official debt to produce an estimated
debt-to-GDP figure.

The scaffold weights above govern the transparent composite score. When a
fitted state file is present, those weights are replaced by the fitted
coefficients (in σ units of the standardized features); the scaffold still
runs in parallel so the dashboard can show both the fitted PM rating and the
transparent composite side by side.

## Dependent variable

The fitting target is **sovereign default events as a binary outcome** —
`defaulted_within_{H}y = 1` if the country starts a default, restructuring, or
sustained arrears spell within `H` years of the observation. Time-to-event
(survival) is also supported by training separate models at `H ∈ {1, 3, 5}` and
exposing a horizon parameter on the dashboard.

The event panel lives at `data/sovereign_defaults.csv`. The seed file mirrors
the [Bank of Canada CRAG](https://www.bankofcanada.ca/?p=120817) schema —
replace it with the full CRAG download for production. Event types currently
counted as a default: `default`, `restructuring`, `arrears`. Paris/London Club
restructurings and IMF programs are in the file but excluded by default
(opt in via `cd_defaults.load_events(include_distress=True)` for target #3 — a
broader distress definition that yields ~3× more positive observations).

The user **explicitly chose targets #1 (binary default) + #2 (hazard, multiple
horizons) only**. Targets #3 (broader distress), #4 (bond-spread-implied PD),
and #5 (Reinhart-Rogoff crisis panel) are documented in the original design
doc but not wired.

## Estimators

Two estimators are available — both consume the same standardized panel:

1. **`fit_logit`** — class-weighted L2-penalized logistic regression
   (`sklearn.linear_model.LogisticRegression`). Coefficients are interpretable
   in σ-units of the standardized features and are written into the fit-state
   JSON so `rating_model.py` can score countries deterministically.

2. **`fit_gbm`** — gradient-boosted classifier
   (`sklearn.ensemble.GradientBoostingClassifier`, 300 trees, depth 3, lr 0.05,
   `subsample=0.8`). Sample weights handle the rare-event imbalance. The state
   file persists permutation-importance feature weights and the empirical PD
   calibration table; rerun `fit_gbm` in-process if you need exact tree-ensemble
   probabilities at serve time.

Both estimators share the same in-sample PD calibration: rank predicted
probabilities, slice into 20 equal-count buckets, report realized default rate
per bucket. This is the empirical-hazard table that replaces the placeholder
PDs in the rating bucket ladder.

## Scaffold fallback

When no fit-state file exists for a horizon, `rating_model.py` falls back to a
**transparent z-score weighted composite**:
- robust z-scores (median / MAD) per indicator across the cross-section, clipped at ±3σ,
- weighted blend across debt (50%) / external (35%) / real (15%) blocks,
- logistic squash to 0–100, bucketed onto the 1–10 ladder,
- bucket-table PDs from S&P / Moody's long-run sovereign default frequencies.

This guarantees the dashboard renders something sensible before the first fit,
and gives an immediate "what indicators contribute and by how much" panel for
every country.

## How to fit

Step 1 — refresh the default-events panel from the Bank of Canada CRAG
database (~1,300 events 1960–present, free, annual update):

```bash
# Pulls the latest CRAG xlsx and writes data/sovereign_defaults.csv
python scripts/fetch_crag_defaults.py

# If the network can't reach bankofcanada.ca, download the spreadsheet
# manually and pass --input.
python scripts/fetch_crag_defaults.py --input ~/Downloads/db-sovereign-defaults-data.xlsx
```

Step 2 — fit the model. Runs against the IMF WEO + World Bank panel
joined to the default events; needs outbound HTTP to imf.org and
api.worldbank.org:

```bash
# Logit, 1-year horizon (default):
python scripts/fit_credit_default.py --estimator logit --horizon 1

# GBM, 3-year horizon:
python scripts/fit_credit_default.py --estimator gbm --horizon 3

# All horizons, both estimators:
python scripts/fit_credit_default.py --estimator both --horizon all
```

State files land in `data/credit_default_fit/fit_state_h{H}.json`. The live
service picks them up automatically on the next dashboard request (the panel
is cached for 6h — hit `POST /api/credit-default/refresh` to force a reload).
A discrete-time hazard view comes for free: train H ∈ {1, 3, 5} and the
dashboard exposes PD at each horizon per country.

## API surface

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/credit-default/dashboard` | Full panel + ratings + summary |
| GET | `/api/credit-default/table` | Compact rows for the table view |
| GET | `/api/credit-default/country/<iso3>` | Drilldown for one country |
| GET | `/api/credit-default/methodology` | Active weights + bucket ladder |
| POST | `/api/credit-default/refresh` | Force re-fetch + re-score |

## Files

```
backend/credit_default/
  __init__.py
  data.py             # IMF/WB indicator harmonizer; exposes get_panel() and get_history_panel()
  defaults.py         # loader for sovereign_defaults.csv → binary labels by horizon
  rating_model.py     # scoring + 1-10 ladder + PD calibration; fit-state aware
  fit.py              # fit_logit / fit_gbm + in-sample PD calibration
  agency_ratings.py   # loader for agency_ratings.csv (display only)
  service.py          # top-level orchestrator + cache
  routes.py           # Flask blueprint
  README.md           # this file

data/
  agency_ratings.csv
  sovereign_defaults.csv
  credit_default_fit/        # fit-state outputs land here

templates/credit_default.html
static/css/credit_default.css
static/js/credit_default.js
scripts/fit_credit_default.py
```

## Site integration (for AIG analyst access)

The page is wired in three places so analysts can find it:

1. `/credit-default` — main dashboard, currently behind
   `@social_or_login_required` (lets social-media crawlers preview the OG
   card; logged-out humans hit the login page).
2. `/models` — listed alongside GeoRisk, US Macro, House Prices, Data Centers.
3. `/` (home page) — `New` chip in the Tools row.

### Cookie-auth API (`/api/credit-default/*`)

Used by the dashboard JS. Endpoints:

- `GET /api/credit-default/dashboard` — full panel + ratings + summary.
- `GET /api/credit-default/table` — compact rows (used by the table view).
- `GET /api/credit-default/country/<iso3>` — drilldown.
- `GET /api/credit-default/methodology` — active weights + bucket ladder.
- `GET /api/credit-default/export` — Excel workbook (3 sheets: ratings,
  indicator panel, methodology).
- `POST /api/credit-default/refresh` — force re-fetch + re-score.

### Bearer-token API (`/api/v1/credit-default/*`)

For programmatic use by analysts who want to pull PDs into spreadsheets,
risk engines, or notebooks without scraping the dashboard. All endpoints
require `Authorization: Bearer pk_live_…` (mint a key at
`/auth/api-keys`). Cookie sessions are deliberately *not* honoured here.

- `GET /api/v1/credit-default/table`
- `GET /api/v1/credit-default/country/<iso3>`
- `GET /api/v1/credit-default/methodology`

### Sharing / OG metadata

Registered in `backend/sharing.py` so links pasted into Slack, LinkedIn,
Substack, etc. render a branded preview with title + description + the
`/og/preview.png?chart=credit-default` image. No additional setup needed.

### Tightening access for analyst-only use

The page is currently marked `Public` (with login). To make it gated like
the US Macro Model (admin-grants access per user), follow the
`macro_access_required` pattern in `app.py`:

1. Add a `has_credit_default_access()` method to the `User` model in
   `backend/auth.py` and a `credit_default_access` boolean column on the
   user table.
2. In `app.py`, wrap the route:
   ```python
   def credit_default_access_required(f):
       return _gated_page(
           lambda u: u.has_credit_default_access(),
           active_page='credit-default',
           reason='Credit Default access is granted by the admin.',
       )(f)

   @app.route('/credit-default')
   @credit_default_access_required
   def credit_default():
       return render_template('credit_default.html', active_page='credit-default')
   ```
3. Wrap the corresponding `/api/credit-default/*` endpoints in
   `backend/credit_default/routes.py` with the same gate (mirroring the
   pattern in `backend/macro_model/routes.py`'s `_macro_gate`).
4. The `/api/v1/credit-default/*` endpoints already require an API key —
   add a `credit_default_access_required` decorator there too if you
   want to restrict by user-tier, not just by valid key.

For now `social_or_login_required` is the right default: AIG users can
self-register, verify their email, and have access immediately while you
gather feedback.

## Next steps

- Replace the seeded `sovereign_defaults.csv` with the full CRAG download
  (~250 events 1960–present) for a meaningful logit fit.
- Add k-fold cross-validation to `fit.py` to report out-of-sample AUC alongside
  the in-sample number.
- Persist the trained GBM model (joblib) so served PDs use the actual tree
  ensemble, not the importance-weighted approximation.
- Wire the GeoRisk geopolitical score as an optional 12th indicator (the user
  flagged this as "eventually" — gated behind a config flag so the macro-only
  default is preserved).
