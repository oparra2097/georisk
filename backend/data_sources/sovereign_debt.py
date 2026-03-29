"""
Sovereign Debt Indicator data source.

Reads pre-computed estimates from the sovereign_debt pipeline's Parquet cache
and serves them as JSON for the frontend. Also provides Excel export.
"""

import os
import json
from pathlib import Path
from datetime import datetime, timedelta

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# Locate the Parquet file from the sovereign_debt pipeline
# Try multiple locations: home dir, relative to georisk, env var
_HOME = Path.home()
_PARQUET_CANDIDATES = [
    _HOME / "Claude" / "sovereign_debt" / "data" / "cache" / "sovereign_debt_estimates.parquet",
    Path(__file__).resolve().parent.parent.parent / "sovereign_debt" / "data" / "cache" / "sovereign_debt_estimates.parquet",
    Path(__file__).resolve().parent.parent.parent.parent / "Claude" / "sovereign_debt" / "data" / "cache" / "sovereign_debt_estimates.parquet",
]
_PARQUET_PATH = next((p for p in _PARQUET_CANDIDATES if p.exists()), _PARQUET_CANDIDATES[0])

# Also check via environment variable
_ENV_PATH = os.environ.get("SOVEREIGN_DEBT_PARQUET")

# Country code lookup (alpha-3 to name) — loaded lazily
_COUNTRY_NAMES = None
_CACHE = {}
_CACHE_TIME = None
_CACHE_TTL = timedelta(hours=6)


def _get_parquet_path():
    """Resolve the Parquet file path."""
    if _ENV_PATH and Path(_ENV_PATH).exists():
        return Path(_ENV_PATH)
    if _PARQUET_PATH.exists():
        return _PARQUET_PATH
    # Try relative to CWD
    cwd_path = Path("sovereign_debt/data/cache/sovereign_debt_estimates.parquet")
    if cwd_path.exists():
        return cwd_path
    return None


def _load_country_names():
    """Load ISO3 → country name mapping from the existing country_codes.json."""
    global _COUNTRY_NAMES
    if _COUNTRY_NAMES is not None:
        return _COUNTRY_NAMES

    _COUNTRY_NAMES = {}
    json_path = Path(__file__).resolve().parent.parent.parent / "static" / "data" / "country_codes.json"
    if json_path.exists():
        with open(json_path) as f:
            for entry in json.load(f):
                alpha3 = entry.get("alpha-3", "")
                name = entry.get("name", "")
                if alpha3 and name:
                    _COUNTRY_NAMES[alpha3] = name

    return _COUNTRY_NAMES


def get_sovereign_debt_data():
    """
    Return sovereign debt indicator data for all countries.

    Returns dict with:
      - countries: {ISO3: {name, region, official_debt_gdp, estimated_debt_gdp,
                           debt_gap_pp, confidence_floor_gdp, confidence_ceiling_gdp,
                           risk_tier, wgi_avg, gdp_usd_bn, ...}}
      - summary: {total_countries, avg_official, avg_estimated, avg_gap,
                  tier_counts: {Critical: N, ...}}
      - updated: ISO timestamp of Parquet file modification
    """
    global _CACHE, _CACHE_TIME

    # Check cache
    if _CACHE_TIME and datetime.now() - _CACHE_TIME < _CACHE_TTL and _CACHE:
        return _CACHE

    if not HAS_PANDAS:
        return {"error": "pandas not installed", "countries": {}, "summary": {}}

    parquet_path = _get_parquet_path()
    if parquet_path is None:
        return {
            "error": "Sovereign debt data not found. Run: python sovereign_debt/run_debt_indicator.py",
            "countries": {},
            "summary": {},
        }

    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        return {"error": str(e), "countries": {}, "summary": {}}

    names = _load_country_names()
    updated = datetime.fromtimestamp(parquet_path.stat().st_mtime).isoformat()

    countries = {}
    for iso3 in df.index:
        row = df.loc[iso3]
        country_name = names.get(iso3, iso3)

        countries[iso3] = {
            "name": country_name,
            "iso3": iso3,
            "region": _safe(row, "region", ""),
            "official_debt_gdp": _round(row, "official_debt_gdp"),
            "estimated_debt_gdp": _round(row, "estimated_debt_gdp"),
            "debt_gap_pp": _round(row, "debt_gap_pp"),
            "confidence_floor_gdp": _round(row, "confidence_floor_gdp"),
            "confidence_ceiling_gdp": _round(row, "confidence_ceiling_gdp"),
            "risk_tier": _safe(row, "risk_tier", ""),
            "wgi_avg": _round(row, "wgi_avg", 2),
            "gdp_usd_bn": _round(row, "gdp_usd_bn"),
            "official_debt_usd_bn": _round(row, "official_debt_usd_bn"),
            "estimated_debt_usd_bn": _round(row, "estimated_debt_usd_bn"),
            "external_debt_usd_bn": _round(row, "external_debt_usd_bn"),
            "bis_claims_usd_bn": _round(row, "bis_claims_usd_bn"),
            "chinese_lending_usd_bn": _round(row, "chinese_lending_usd_bn"),
        }

    # Summary statistics
    tier_counts = {}
    for tier in ["Critical", "High", "Elevated", "Moderate", "Low"]:
        tier_counts[tier] = int((df.get("risk_tier", pd.Series()) == tier).sum())

    summary = {
        "total_countries": len(countries),
        "avg_official": round(df["official_debt_gdp"].mean(), 1) if "official_debt_gdp" in df else None,
        "avg_estimated": round(df["estimated_debt_gdp"].mean(), 1) if "estimated_debt_gdp" in df else None,
        "avg_gap": round(df["debt_gap_pp"].mean(), 1) if "debt_gap_pp" in df else None,
        "tier_counts": tier_counts,
        "updated": updated,
    }

    result = {
        "countries": countries,
        "summary": summary,
        "updated": updated,
    }

    _CACHE = result
    _CACHE_TIME = datetime.now()
    return result


def _safe(row, col, default=""):
    """Safely get a value from a row."""
    try:
        val = row.get(col, default) if hasattr(row, 'get') else getattr(row, col, default)
        if pd.isna(val):
            return default
        return val
    except Exception:
        return default


def _round(row, col, decimals=1):
    """Safely get and round a numeric value."""
    try:
        val = row.get(col) if hasattr(row, 'get') else getattr(row, col, None)
        if val is None or (isinstance(val, float) and (pd.isna(val) or val != val)):
            return None
        return round(float(val), decimals)
    except Exception:
        return None
