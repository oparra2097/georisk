# Shadow Debt Indicator — Methodology

**Current version:** `v1.1-em-guardrails`
**Scope:** Emerging & frontier markets only. Advanced-economy coverage suppressed pending rebuild (see §5).

## 1. Purpose

The Shadow Debt Indicator estimates sovereign debt inclusive of contingent and off-balance-sheet liabilities that do not appear in headline IMF General Government Gross Debt (Maastricht-equivalent) figures. It is intended to flag sovereigns whose actual obligations are materially larger than the published top-line — the Senegal, Mozambique, Zambia, and Ghana cases being the canonical examples.

## 2. Output schema (per country)

| Field | Meaning |
|-------|---------|
| `official_debt_gdp` | IMF general government gross debt (baseline / floor). |
| `estimated_debt_gdp` | Official + estimated shadow component. |
| `debt_gap_pp` | `estimated − official` in percentage points. |
| `confidence_floor_gdp` | Lower bound of estimate (= official by construction). |
| `confidence_ceiling_gdp` | Upper bound of estimate (sigma-based). |
| `sigma` | Per-country noise estimate, fraction of `estimated_debt_gdp`. |
| `risk_tier` | Critical / High / Elevated / Moderate / Low. |
| `benchmark` | Reconciliation result vs external published figure. |
| `upstream_integrity_flag` | Set if downstream guardrails had to clamp the row. |

## 3. Inputs (EM/frontier branch)

- **IMF General Government Debt** — baseline.
- **World Bank International Debt Statistics (DRS)** — external debt stocks, short-term / long-term split.
- **BIS Consolidated Banking Statistics** — cross-border bank claims (read only; used as cross-check, not additive).
- **AidData / Chinese lending** — bilateral opaque exposure.
- **World Bank Worldwide Governance Indicators (WGI)** — six-dimension governance score; feeds sigma and opacity discount.

The shadow component for EM/frontier is driven primarily by the external-debt + bilateral-lending channel: exposure that is known to foreign creditors but under-reported in domestic fiscal accounts. This is the mechanism validated in Horn, Reinhart & Trebesch (2022) "Hidden Defaults."

## 4. Guardrails introduced in v1.1

The v1.0 upstream pipeline shipped several indefensible outputs that were caught in the April 2026 watchlist review. `v1.1-em-guardrails` adds the following downstream protections without depending on an upstream rebuild:

1. **AE suppression.** All 40 IMF WEO advanced economies are stripped from the served output. See §5 for the AE methodology defect that motivated this.
2. **Negative-shadow guard.** Rows where `estimated_debt_gdp < official_debt_gdp` (a mathematical impossibility under the model's own definition) are clamped to `estimated = official, gap = 0` and tagged with `upstream_integrity_flag: negative_shadow_clamped`. The underlying parquet build must still be corrected upstream; this is a safety net, not a fix. Canada and Brazil are the currently-known cases.
3. **Per-country sigma.** v1.0 assigned a flat `sigma=0.35` to AEs and `sigma=0.86` to EMs. v1.1 derives sigma from (a) input completeness across the six required fields and (b) WGI score. Range `[0.15, 1.0]`.
4. **External-benchmark reconciliation.** Every country's `estimated_debt_gdp` is compared to a published extended-debt benchmark (IMF Fiscal Monitor, IMF Article IV, Eurostat supplementary tables). Deviations outside the per-country tolerance are surfaced in `country.benchmark.status = "out_of_band"`.
5. **Pre-publication checklist.** `POST /api/sovereign-debt/preflight/<iso3>` runs the three-gate check (reconciliation, sigma, internal consistency) and returns pass/fail. Any country cited in an external memo should clear this endpoint first.

## 5. Why advanced-economy coverage was removed

The upstream v1.0 pipeline computes AE shadow debt as `0.5 × (BIS consolidated bank claims / GDP × 100)`. Reverse-engineered from the shipped outputs, this rule is exact across DEU, FRA, ITA, ESP, PRT, BEL, AUT, JPN, KOR, AUS, NZL, FIN, SWE, DNK, NOR. It is methodologically wrong for three reasons:

1. **Category error.** BIS consolidated banking statistics measure G-SIB counterparty intermediation — BNP, Deutsche, Crédit Agricole, SocGen running global EUR/USD repo, FX swap, and corporate lending books. These are bank assets or gross exposures, not sovereign obligations. Adding 50% of them to Maastricht debt treats a banking sector balance sheet as a fiscal liability.
2. **Inconsistent application.** The rule was silently switched off for the largest banking hubs (GBR, CHE, IRL, SGP, HKG, NLD, CAN) where it produced obviously absurd results (GBR would carry 58pp of "shadow debt" on this rule; SGP 73pp). The same logic produces wrong answers for DEU and FRA — only slightly less extreme — which were nonetheless served.
3. **Fails reconciliation.** Eurostat's supplementary tables on government interventions (the AAA-grade "extended debt" measure for EU sovereigns) put Germany at ~67–70% and France at ~115–118% of GDP for 2024. The v1.0 model served 82.7% and 138.4% respectively — materially above any defensible extended measure.

### Rebuild plan for AE coverage (v2.0)

AE coverage will return only once rebuilt bottom-up:

- **Per-country contingent-liability stack**, sourced to named Eurostat supplementary tables and national audit-office documents (Bundesrechnungshof, Cour des Comptes, Corte dei Conti).
- **No BIS additive.** If bank-sovereign nexus is desired, it ships as a separate indicator under a different label.
- **Landesbanken / French regional public banks** itemised as a distinct tier, never folded into the primary `estimated_debt_gdp` without explicit toggle.
- **CI reconciliation.** Every AE must land within the Eurostat extended-debt band ±2pp in the benchmark YAML, or deploy is blocked.

## 6. Known limitations (current release)

- **Senegal baseline uncertainty.** The `official_debt_gdp` for Senegal is carried as 128.4%, above the IMF Article IV April 2024 post-revelation figure (~99% for 2024). There is a real risk the baseline already embeds hidden-debt adjustments that are then double-counted in the 63.1pp shadow add. Baseline should be sourced cleanly to a single IMF publication and the shadow add recomputed against it. Tracked.
- **External debt vs BIS claims overlap.** For EMs with large external bank borrowing, external-debt stocks (WB DRS) and BIS consolidated claims partially overlap. No Venn-diagram deduplication is implemented yet. Tracked.
- **Benchmark file is sparsely populated.** `data/benchmarks/shadow_debt_benchmarks.yaml` is seeded with slots for SEN, MOZ, ZMB, GHA, DEU, FRA but most `benchmark_pct_gdp` values are `null` pending human entry from Fiscal Monitor / Article IV PDFs. Until populated, reconciliation reports `missing_benchmark` rather than `ok`.
- **Upstream parquet is outside version control.** The estimation pipeline itself lives at `~/Claude/sovereign_debt/` on the operator's machine. There is no CI, no tests, and no reviewer on upstream changes. Moving it into this repo (or a sibling repo with CI) is the single highest-leverage governance fix.

## 7. Pre-publication gate

Before any shadow-debt figure is cited in a memo, watchlist entry, or client deliverable, the producing analyst must:

1. Call `/api/sovereign-debt/preflight/<iso3>` and attach the response to the draft.
2. Confirm `pass: true`, or document each `reasons[]` entry as an accepted exception.
3. If the country has `status: no_benchmark` or `benchmark_missing_value`, add the benchmark to the YAML first.

This is the minimum governance required to avoid a repeat of the DEU/FRA watchlist episode.

## 8. Version history

| Version | Date | Change |
|---------|------|--------|
| v1.0    | pre-2026-04 | Initial upstream-generated parquet; BIS-50% AE rule; flat sigma. |
| v1.1-em-guardrails | 2026-04-24 | AE suppression; negative-shadow clamp; per-country sigma; benchmark reconciliation; preflight checklist. EM/frontier only. |
| v2.0 (planned) | TBD | Bottom-up AE rebuild; upstream pipeline under version control; benchmark YAML fully populated; AE reconciliation in CI. |
