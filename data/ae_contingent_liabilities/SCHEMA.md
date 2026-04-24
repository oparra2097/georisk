# Advanced-Economy Contingent Liability Stack — Schema

This directory holds bottom-up contingent-liability estimates for advanced economies. Each country has its own YAML file named by ISO3 (e.g. `DEU.yaml`, `FRA.yaml`).

The total estimate is not a single headline number. It is a sum of cited line items, each traceable to a specific publication, so that reviewers can challenge any component independently.

## File schema

```yaml
iso3: <ISO3>
name: <country name>
currency: <ISO 4217, e.g. EUR>
as_of: YYYY-MM-DD
gdp_local_bn: <float>           # nominal GDP in local currency, billions
gdp_usd_bn: <float>             # nominal GDP in USD, billions (for cross-product)
maastricht_debt_pct_gdp: <float>  # headline EDP figure, the baseline
maastricht_debt_source: <str>
maastricht_debt_url: <str>
maastricht_debt_date: YYYY-MM-DD

components:
  - id: <short machine id, e.g. kfw_debt>
    label: <human label, e.g. "KfW gross liabilities">
    category: <special_fund | state_bank | soe_debt | guarantee | pension | other>
    amount_local_bn: <float>           # in local currency
    amount_pct_gdp: <float>            # same value as % of GDP
    already_in_maastricht: <bool>      # TRUE if already in the EDP figure
    include_in_extended: <bool>        # TRUE if we add it to the extended total
    loss_given_default: <float 0-1>    # haircut for guarantees; 1.0 = full debt
    source: <full citation>
    source_url: <URL>
    source_date: YYYY-MM-DD
    notes: |
      Why this is included, what the risk profile is, and any caveats.

extended_debt_estimate:
  low_pct_gdp: <float>
  mid_pct_gdp: <float>
  high_pct_gdp: <float>
  method: |
    Brief description of how low/mid/high were derived — typically by
    varying loss_given_default and which contingent guarantees are
    included.
  defensible_ceiling_pct_gdp: <float>  # anything above this needs extra justification
```

## Extended debt calculation

```
extended_debt = maastricht_debt_pct_gdp
              + sum(c.amount_pct_gdp * c.loss_given_default
                    for c in components
                    if c.include_in_extended and not c.already_in_maastricht)
```

`loss_given_default` applies only to guarantees and similar contingent exposures where full materialisation is unlikely. Direct debt obligations (e.g. drawn special funds, KfW bonded debt if scoped in) use `loss_given_default: 1.0`.

## Categories

- `special_fund` — off-balance-sheet vehicles (Sondervermögen, CADES).
- `state_bank` — 100% state-owned lenders (KfW, Bpifrance).
- `soe_debt` — debt of state-owned enterprises (EDF, SNCF Réseau) where state bears residual risk.
- `guarantee` — explicit or legally implicit guarantees (export credit, deposit insurance, regional public banks).
- `pension` — unfunded pension commitments flagged under national accounts extensions.
- `other` — everything else, including AREVA/Orano legacy, crisis vehicles, etc.

## CI requirements

For a country to be served via the AE branch, its YAML must:
1. Have `maastricht_debt_pct_gdp` sourced to Eurostat (or equivalent).
2. Have `extended_debt_estimate.mid_pct_gdp` within the `benchmarks.yaml` `tol_pp` band.
3. Every component must have `source_url` populated — no undocumented numbers.
4. `defensible_ceiling_pct_gdp` must be set.

Countries that don't meet these requirements are carried as "under construction" and not served through the public API.
