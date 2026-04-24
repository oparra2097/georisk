# EM Regional Banking Exposure — Schema

This directory holds per-country estimates of cross-border bank holdings of EM sovereign debt that **do not appear in BIS consolidated banking statistics or World Bank IDS**. This is the fix for the WAEMU/CEMAC/regional-banking blind spot in the v1.0 model.

## Why this exists

The v1.0 EM pipeline pulled cross-border bank claims from BIS Consolidated Banking Statistics. BIS covers 33 reporting jurisdictions — all major G10 + select EMs. **No WAEMU or CEMAC country is a BIS reporter.** Pan-African banks headquartered in Abidjan (SGBCI, BICICI, Ecobank CI, Oragroup, NSIA, Atlantic) or Lomé (Ecobank Group holdco) hold material claims on WAEMU sovereigns (Senegalese titres publics held by Ivorian banks, etc.). These claims are genuine cross-border sovereign exposure but are **invisible** to the v1.0 model.

Per-country research from BCEAO monetary surveys, Agence UMOA-Titres, BEAC equivalents, SARB, and IMF Article IV debt-composition tables pins this exposure bottom-up.

## File schema

One YAML file per country, named by ISO3 (e.g. `SEN.yaml`).

```yaml
iso3: <ISO3>
name: <country name>
monetary_union: <WAEMU | CEMAC | SADC | EAC | NONE>
currency: <ISO 4217>
as_of: YYYY-MM-DD
gdp_usd_bn: <float>

# Regional public securities issued by this sovereign (domestic + regional
# market combined, since in WAEMU/CEMAC the distinction is artificial).
titres_publics_stock_local_bn: <float>
titres_publics_stock_usd_bn: <float>
titres_publics_source: <str>
titres_publics_url: <str>
titres_publics_date: YYYY-MM-DD

# Total claims of the regional banking system on this sovereign.
# Typically drawn from BCEAO/BEAC monetary survey "créances sur
# l'Administration Centrale" per country.
banking_system_claims_on_govt_local_bn: <float>
banking_system_claims_source: <str>
banking_system_claims_url: <str>
banking_system_claims_date: YYYY-MM-DD

# Estimated fraction of the above held by banks in OTHER countries of
# the same monetary union. Rarely published explicitly — typically
# inferred from IMF Article IV commentary. Mark as estimate if so.
cross_border_share: <float 0-1>
cross_border_share_method: "published | inferred_article_iv | order_of_magnitude"
cross_border_share_source: <str>
cross_border_share_url: <str>

# Derived: stock × share, expressed in USD for cross-country comparison.
regional_bank_exposure_usd_bn: <float>
regional_bank_exposure_pct_gdp: <float>

# Treatment in the shadow-debt aggregate.
include_in_shadow: <bool>           # default true
loss_given_default: <float 0-1>     # default 1.0 (direct sovereign claim, no haircut)
double_count_check: |
  Free-text description of how this avoids double-counting with
  external_debt_usd_bn (WB IDS) or bis_claims_usd_bn (BIS CBS).

notes: |
  Context, source caveats, and known data quality issues.
```

## Layering onto the main pipeline

`backend/data_sources/sovereign_debt.py` reads this directory and, for any served EM country with a YAML file here:

1. Loads `regional_bank_exposure_usd_bn`.
2. Converts to % of GDP using `country.gdp_usd_bn` from the main dataset.
3. Adds the fraction × `loss_given_default` to `estimated_debt_gdp`.
4. Surfaces the adjustment as an explicit line in `country.regional_exposure_adjustment_pp` so it can be inspected / overridden.
5. Adds an entry to `summary.regional_exposure_applied` for auditability.

Countries without a YAML file here are unaffected. Countries marked `include_in_shadow: false` (e.g. asset positions, or exposures already fully captured elsewhere) are tracked but not added.

## Coverage priorities

Tier 1 (must have before next watchlist):
- **WAEMU** — SEN, CIV, MLI, BFA, NER, TGO, BEN, GNB
- **CEMAC** — CMR, TCD, CAF, COG, GAB, GNQ

Tier 2 (add once Tier 1 is stable):
- **SADC overflow** — MOZ, ZMB, ZWE, AGO (exposure to South African + regional banks)
- **East Africa** — KEN, UGA, TZA, RWA, ETH (Kenyan bank holdings)
- **Frontier Gulf** — JOR, EGY (Gulf bank exposure)

## CI requirements

For a country's YAML to be "ready" and have its exposure applied:
1. `regional_bank_exposure_usd_bn` must be non-null.
2. Either `cross_border_share_method: published` or `inferred_article_iv` (not `order_of_magnitude` alone — that requires human sign-off).
3. `double_count_check` must be non-empty.
4. At least one `source_url` must be populated.

Countries that don't meet these are carried as `_status: skeleton` and **not** applied to the shadow calculation.
