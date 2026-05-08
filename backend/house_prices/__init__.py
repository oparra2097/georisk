"""
Parra Macro — US House Price Index product.

Aggregates three authoritative free sources into a unified quarterly panel
covering every geographic level from national → region → state → MSA →
county → ZIP:

  FHFA HPI           Region / State / MSA (quarterly) + County (annual)
  S&P/Case-Shiller   National + 20 cities (monthly, via FRED)
  Zillow ZHVI        Metro / County / ZIP (monthly, public CSVs, CC-BY)

Exposes:
  /house-prices                    admin-gated dashboard
  /api/house-prices/*              admin-gated API

Design mirrors backend/macro_model/ — separate product, own package,
own API namespace, own access gate.
"""
