# EM Sovereign Debt Stack — v2.0 Bottom-Up Schema

Per-country bottom-up sovereign-debt component stacks for emerging and frontier markets, replacing the inherited v1.0 BIS-50% calculation for any EM country with a ready YAML.

## Why this exists

The v1.0 upstream pipeline computed EM shadow debt as `official + 0.5 × (BIS_claims / GDP × 100) + ad-hoc adjustments`. This formula:
- Has no calibrated basis — 50% is an undocumented round number
- Conflates G-SIB cross-border intermediation with sovereign liability
- Captures none of the channels that actually matter for EMs (LGFV, regional banking, SOE guarantees, multi-creditor restructurings)
- Cannot be decomposed — there is no way to ask "where does Mozambique's 137.8% come from"

v2.0 replaces the multiplier with a per-country component stack. Same schema as the AE rebuild (`data/ae_contingent_liabilities/`) but with components calibrated for EM channels: multilateral, bilateral Paris/non-Paris, Chinese policy bank lending, Eurobonds, domestic government securities, regional-market securities (for WAEMU/CEMAC), SOE-guaranteed debt, LGFV-equivalents.

## File schema

```yaml
iso3: <ISO3>
name: <country>
currency: <ISO 4217>
as_of: YYYY-MM-DD
gdp_usd_bn: <float>

imf_general_govt_pct_gdp: <float>
imf_general_govt_source: <citation>
imf_general_govt_url: <URL>
imf_general_govt_date: YYYY-MM-DD

components:
  - id: <short id, e.g. multilateral>
    label: <human label>
    category: <multilateral | bilateral_paris | bilateral_non_paris |
               chinese_policy | eurobond | domestic_secs |
               regional_market | soe_guaranteed | lgfv | central_bank |
               other>
    amount_local_bn: <float>
    amount_usd_bn: <float>
    amount_pct_gdp: <float>
    already_in_general_govt: <bool>
    include_in_extended: <bool>
    loss_given_default: <float 0-1>
    source: <citation>
    source_url: <URL>
    source_date: YYYY-MM-DD
    notes: |
      Why included, why this LGD.

extended_debt_estimate:
  low_pct_gdp: <float>
  mid_pct_gdp: <float>           # SERVED PRIMARY
  high_pct_gdp: <float>
  method: |
    Description of LGD regime for low/mid/high.
  defensible_ceiling_pct_gdp: <float>

context_metrics:
  total_non_financial_sector_credit_pct_gdp: <float>
  total_non_financial_sector_credit_source: <citation>
```

## Calculation rule

```
extended_debt = imf_general_govt_pct_gdp
              + Σ (c.amount_pct_gdp × c.loss_given_default
                   for c in components
                   if c.include_in_extended and not c.already_in_general_govt)
```

`mid_pct_gdp` is what the main Shadow Debt Indicator serves as `estimated_debt_gdp` for any country with a ready stack.

## CI gate

A stack is `ready` only when:
1. `imf_general_govt_pct_gdp` non-null with a source URL
2. `extended_debt_estimate.mid_pct_gdp` non-null
3. `extended_debt_estimate.defensible_ceiling_pct_gdp` non-null
4. At least one `component` has `include_in_extended: true`
5. Every `include_in_extended: true` component has `source_url` populated

Countries without a ready stack fall back to the v1.4-corrected upstream pipeline (AE suppression + baseline overrides + regional exposure + negative-shadow guard).

## Coverage priorities

Tier 1 (must have): SEN, MOZ, ZMB, GHA, CHN, IND, BRA, MEX, TUR, ARG, ZAF
Tier 2: CEMAC, frontier Gulf (EGY, JOR), frontier Asia (PAK, LKA, BGD), EM Europe (RUS, UKR)

## Relationship to other layers

- **`data/em_regional_exposure/`**: regional banking layer (v1.3). Once a country has a ready EM stack with `regional_market` component, the stack value supersedes the regional-exposure layer.
- **`data/benchmarks/`**: benchmark YAML provides floor (Maastricht) and shadow ceiling. Stack's `mid` must sit within those.
- **`data/ae_contingent_liabilities/`**: parallel directory for AEs. Same schema, different country list.
