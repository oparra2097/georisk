# sovereign_debt_pipeline — Upstream Estimation Pipeline (Stub)

This directory is the **intended home** of the upstream pipeline that produces `static/data/sovereign_debt.json`. It currently holds a stub plus documentation. The real pipeline is believed to live at `~/Claude/sovereign_debt/` on the operator's machine and is not under version control — that is the governance gap this stub exists to close.

## Why this matters

The v1.0 pipeline's 50%-of-BIS-claims rule for advanced economies, and the negative-shadow bug for BRA/CAN, both reached production because the estimation code:

1. Was never committed to any repository.
2. Had no CI, no tests, no reviewer.
3. Ran only on a single developer's machine.

The downstream `backend/data_sources/sovereign_debt.py` loader added guardrails in v1.1–v1.2 (AE suppression, negative-shadow clamp, per-country sigma, benchmark reconciliation, Senegal baseline override). Those are safety nets — the root cause fix is to move the estimation pipeline into this directory under CI.

## What should live here

```
sovereign_debt_pipeline/
├── README.md                     # this file
├── pipeline/
│   ├── __init__.py
│   ├── sources/                  # ingestion modules
│   │   ├── imf_weo.py            # IMF WEO general govt debt
│   │   ├── wb_ids.py             # World Bank International Debt Statistics
│   │   ├── bis.py                # BIS consolidated banking statistics (READ only)
│   │   ├── aiddata.py            # AidData Chinese lending
│   │   └── wgi.py                # World Bank Governance Indicators
│   ├── em_model.py               # EM/frontier shadow-debt calculation
│   ├── ae_model.py               # v2.0 AE bottom-up (reads data/ae_contingent_liabilities/)
│   ├── aggregate.py              # merges EM + AE into single output
│   └── validate.py               # CI gate — reconciles against benchmarks.yaml
├── data/
│   └── cache/                    # intermediate parquet
├── tests/
│   ├── test_em_model.py
│   ├── test_ae_model.py
│   └── test_no_regression.py     # snapshot tests of known outputs
├── run_pipeline.py               # end-to-end runner that writes static/data/sovereign_debt.json
└── Makefile                      # `make ingest`, `make estimate`, `make validate`, `make publish`
```

## CI gates that must pass before writing to `static/data/sovereign_debt.json`

1. **No negative shadow** — `estimated_debt_gdp >= official_debt_gdp` for every row.
2. **AE reconciliation** — for every AE, `estimated_debt_gdp` within `benchmarks.yaml` `tol_pp` of the bottom-up stack in `data/ae_contingent_liabilities/`.
3. **EM reconciliation** — for every EM with a populated benchmark, within `tol_pp` of IMF Fiscal Monitor / Article IV figure.
4. **Snapshot test** — no country moves more than 10pp between successive runs without a human-approved override.
5. **Documented version bump** — every change to the output that crosses a reconciliation band must ship with a version tag and a one-line change note in `docs/shadow-debt/METHODOLOGY.md`.

## Migration plan

1. **Copy `~/Claude/sovereign_debt/` into this directory.** Operator-side task; cannot be done from inside Claude Code sessions without access to that filesystem.
2. **Delete the BIS-50% rule** from the AE code path entirely. Replace with the bottom-up loader `backend/data_sources/ae_contingent_liabilities.py` already built in this repo.
3. **Root-cause the BRA / CAN negative-shadow bug.** The downstream clamp is a safety net; the source must be corrected. Current suspicion: whichever code path produces `estimated_debt_gdp` for countries with low BIS exposure and high external-debt-to-GDP is introducing a sign error.
4. **Commit + push on a dedicated branch**, open PR for review, merge only after CI passes.

## Read-only interface with the main app

Until this pipeline is migrated, `backend/data_sources/sovereign_debt.py` reads `static/data/sovereign_debt.json` as a committed artifact. This keeps the app deployable even with the pipeline outside version control — at the cost of making pipeline changes invisible to reviewers. Accept as a short-term compromise; close the gap as migration priority #1.
