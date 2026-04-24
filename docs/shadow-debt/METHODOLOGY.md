# Shadow Debt Indicator — Methodology

**Current version:** `v1.3-em-regional-exposure` (served product)
**AE rebuild version:** `ae-v2.0-bottom-up` (separate endpoint, drill-down only)
**EM regional-exposure version:** `em-regional-v1.0-waemu` (skeleton, drill-down only)
**Scope:** Emerging & frontier markets served via the main indicator. Advanced-economy bottom-up stacks available via `/api/ae-contingent-liabilities` (7 countries). Regional-banking exposure (WAEMU titres publics held by Ivorian + pan-African banks) tracked via `/api/em-regional-exposure` — currently skeleton pending live BCEAO/AUT data pulls.

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

## 5b. Regional-banking exposure (v1.3 WAEMU layer)

**Blind spot this closes.** BIS Consolidated Banking Statistics covers 33 reporting jurisdictions — no WAEMU (UEMOA) or CEMAC country is a reporter. Pan-African banks headquartered in Abidjan (SGBCI, BICICI, Ecobank CI, NSIA), Lomé (Ecobank Group, Oragroup), and elsewhere in the union hold material claims on Senegalese and other WAEMU-member sovereigns via the regional XOF-denominated `titres publics` market. These claims are genuine cross-border sovereign exposure but are invisible to BIS and to World Bank IDS (which captures FX-denominated bonds + Paris/non-Paris bilateral + multilateral, not regional-market paper).

**Data stack.** One YAML per country in `data/em_regional_exposure/`:

- `titres_publics_stock_local_bn` — stock of regional public securities issued by this sovereign (AUT bulletin).
- `banking_system_claims_on_govt_local_bn` — WAEMU-wide banking system claims on this sovereign (BCEAO monetary survey "créances sur l'Administration Centrale").
- `cross_border_share` — fraction held by banks in OTHER union members (rarely published explicitly; inferred from Article IV commentary).
- `regional_bank_exposure_usd_bn` — derived: stock × share, in USD.

**CI gate.** Skeleton entries are NOT applied to `estimated_debt_gdp`. An entry passes only when:
1. `regional_bank_exposure_usd_bn` is non-null;
2. `titres_publics_stock_local_bn` is populated from live BCEAO/AUT data (not derived from an aggregate allocation);
3. `cross_border_share_method` is `published` or `inferred_article_iv` (not `order_of_magnitude`);
4. `double_count_check` is documented.

Current state: **WAEMU 8 countries seeded** (SEN, CIV, MLI, BFA, NER, TGO, BEN, GNB); all skeleton, none auto-applied. MLI and NER have `cross_border_share_method: inferred_article_iv` grounded in the 2022 Mali default / 2023 Niger sanctions episodes but remain skeleton because their stock figures are still order-of-magnitude.

**Estimated impact once populated** (order-of-magnitude, not yet applied):
- Senegal: ~10pp of GDP — material shadow adjustment
- Togo: ~20pp of GDP — HIGH-IMPACT (Lomé HQ effect, needs bespoke treatment)
- Benin: ~10pp
- Mali: ~10pp
- Niger: ~9pp
- Burkina Faso: ~9pp
- Côte d'Ivoire: ~5pp (deepest domestic sector)
- Guinea-Bissau: ~4pp

**Future coverage.** CEMAC (6 countries), SADC overflow (MOZ, ZMB, ZWE, AGO → South African + pan-African banks), East Africa (Kenyan bank holdings of regional sovereign debt), frontier Gulf (EGY, JOR Gulf bank exposure). Same schema, same CI gate.

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
| v1.2-em-guardrails-baseline-override | 2026-04-24 | Added baseline overrides (SEN 128.4→99.7, GHA 70.3→82.5, both sourced to IMF WEO); added benchmark_type (floor for EM, symmetric for AE) with shadow_ceiling_pct_gdp upper bound; populated EM benchmarks for SEN/MOZ/ZMB/GHA from IMF WEO Apr 2025 / Oct 2024; floor-semantics test surfaces upstream bugs (GHA stale baseline). |
| ae-v2.0-bottom-up | 2026-04-24 | Bottom-up AE stack for DEU (mid 74%), FRA (mid 118.3%), ITA (mid 140.7%), ESP (mid 103.7%), BEL (mid 105.9%), NLD (mid 44.2%), JPN (mid 238.3%). Exposed via `/api/ae-contingent-liabilities` as drill-down, not folded into main output. Every component cites a source URL; 12-test CI gate enforces band ordering, no-double-counting, defensible-ceiling adherence. |
| v1.3-em-regional-exposure | 2026-04-24 | Added WAEMU regional-banking exposure layer. Closes the "Senegal ↔ Côte d'Ivoire banks" blind spot: BIS consolidated claims do not reach WAEMU (no member is a BIS reporter) and WB IDS covers only FX-denom external debt, missing XOF-denom regional-market titres publics. Skeletons for all 8 WAEMU countries seeded from Agence UMOA-Titres aggregate × country shares. CI gate requires live BCEAO/AUT stock data before auto-apply; MLI and NER have inferred_article_iv share grounding from 2022 sanctions / 2023 coup episodes but are still held back until stock figures are pulled. Served via `/api/em-regional-exposure`. |
| v2.0 (planned) | TBD | Upstream parquet pipeline migrated into `sovereign_debt_pipeline/` under CI; BIS-50% rule removed entirely; all EM benchmarks populated; AE stacks expanded beyond DEU/FRA to ITA, ESP, BEL, NLD, JPN. |

## 9. What agent research populated (2026-04-24)

The v1.2 and ae-v2.0 updates were driven by three research agents whose full output is archived in the April 2026 session transcript. Key extractions:

### EM benchmarks

- **Senegal**: IMF WEO April 2025 post-revelation = 99.7% of GDP; WB IDS external debt = $30.1bn. Used for baseline override.
- **Mozambique**: IMF WEO October 2024 = 91.8%; WB IDS external debt = $14.6bn.
- **Zambia**: IMF WEO October 2024 post-restructuring = 108.7%; WB IDS = $14.2bn.
- **Ghana**: IMF WEO October 2024 = 82.5%; WB IDS = $30.0bn. Used for baseline override (upstream had 70.3% — stale).

### Germany bottom-up (end-2024)

- Maastricht 62.5% (Bundesbank, €2,692bn / €4,305bn GDP).
- Bundeswehr Sondervermögen (€100bn auth, €86.6bn committed) — ALREADY in EDP under ESA 2010.
- Infrastructure/Climate fund (€500bn auth, March 2025) — drawn end-2024 = €0.
- KfW gross liabilities €545.4bn — NOT in EDP (financial corp); central LGD 0.5 adds ~6.3pp.
- Landesbanken aggregate assets €940bn (LBBW/BayernLB/Helaba/NordLB/SaarLB) — no explicit guarantee post-2005; LGD 0.1 adds ~2.2pp.
- Hermes stock ~€400bn at LGD 0.1 adds ~0.9pp.
- ESM/EFSF excluded from default (mutualised, not Germany-specific sovereign risk).
- Defensible range 68-75%; mid 74%. v1.0 model's 82.7% exceeded ceiling by 8pp.

### France bottom-up (end-2024)

- Maastricht 113.0% (INSEE, €3,305bn / €2,925bn GDP).
- CADES €137.9bn, SNCF Réseau €18.9bn, UNEDIC €59bn, ACOSS — ALL already in Maastricht.
- EDF net debt €54.3bn + EDF nuclear provisions €53.8bn — NOT in Maastricht.
- SNCF Group ex-Réseau ~€6bn; Bpifrance guarantees ~€40bn; CDC long-term €149bn (contested).
- Defensible range 115-120%; mid 118.3%. Shadow component over Maastricht is ~5pp. v1.0 model's 138.4% exceeded ceiling by 18pp.

All figures source-cited in `data/benchmarks/shadow_debt_benchmarks.yaml` and `data/ae_contingent_liabilities/{DEU,FRA}.yaml`.
