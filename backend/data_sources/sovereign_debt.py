"""
Sovereign Debt Indicator data source.

Primary: loads pre-baked JSON from static/data/sovereign_debt.json
         (committed to repo, deployed with the app).
Fallback: reads Parquet from the sovereign_debt pipeline if available.
"""

import os
import json
from pathlib import Path
from datetime import datetime, timedelta

# ── Static JSON (always works — deployed with the app) ───────────────────
_STATIC_JSON = Path(__file__).resolve().parent.parent.parent / "static" / "data" / "sovereign_debt.json"

# ── Parquet fallback (local dev only) ────────────────────────────────────
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

_HOME = Path.home()
_PARQUET_CANDIDATES = [
    _HOME / "Claude" / "sovereign_debt" / "data" / "cache" / "sovereign_debt_estimates.parquet",
    Path(__file__).resolve().parent.parent.parent / "sovereign_debt" / "data" / "cache" / "sovereign_debt_estimates.parquet",
]

# ── Cache ────────────────────────────────────────────────────────────────
_CACHE = {}
_CACHE_TIME = None
_CACHE_TTL = timedelta(hours=6)


def get_sovereign_debt_data():
    """
    Return sovereign debt indicator data for all countries.

    Returns dict with:
      - countries: {ISO3: {...}}
      - summary: {total_countries, avg_official, avg_estimated, avg_gap, tier_counts}
    """
    global _CACHE, _CACHE_TIME

    # Check in-memory cache
    if _CACHE_TIME and datetime.now() - _CACHE_TIME < _CACHE_TTL and _CACHE:
        return _CACHE

    # Strategy 1: Load from static JSON (always available in production)
    if _STATIC_JSON.exists():
        try:
            with open(_STATIC_JSON) as f:
                result = json.load(f)
            if result.get("countries"):
                _CACHE = result
                _CACHE_TIME = datetime.now()
                return result
        except Exception as e:
            pass  # fall through to Parquet

    # Strategy 2: Load from Parquet (local dev with sovereign_debt pipeline)
    result = _try_load_parquet()
    if result:
        _CACHE = result
        _CACHE_TIME = datetime.now()
        return result

    return {
        "error": "Sovereign debt data not found.",
        "countries": {},
        "summary": {},
    }


def _try_load_parquet():
    """Attempt to load from Parquet file (local dev fallback)."""
    if not HAS_PANDAS:
        return None

    parquet_path = None
    env_path = os.environ.get("SOVEREIGN_DEBT_PARQUET")
    if env_path and Path(env_path).exists():
        parquet_path = Path(env_path)
    else:
        for candidate in _PARQUET_CANDIDATES:
            if candidate.exists():
                parquet_path = candidate
                break

    if parquet_path is None:
        return None

    try:
        df = pd.read_parquet(parquet_path)
    except Exception:
        return None

    names = _load_country_names()

    countries = {}
    for iso3 in df.index:
        row = df.loc[iso3]
        countries[iso3] = {
            "name": names.get(iso3, iso3),
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

    tier_counts = {}
    for tier in ["Critical", "High", "Elevated", "Moderate", "Low"]:
        tier_counts[tier] = int((df.get("risk_tier", pd.Series()) == tier).sum())

    return {
        "countries": countries,
        "summary": {
            "total_countries": len(countries),
            "avg_official": round(df["official_debt_gdp"].mean(), 1) if "official_debt_gdp" in df else None,
            "avg_estimated": round(df["estimated_debt_gdp"].mean(), 1) if "estimated_debt_gdp" in df else None,
            "avg_gap": round(df["debt_gap_pp"].mean(), 1) if "debt_gap_pp" in df else None,
            "tier_counts": tier_counts,
        },
    }


def _load_country_names():
    """Load ISO3 → country name mapping."""
    names = {}
    json_path = Path(__file__).resolve().parent.parent.parent / "static" / "data" / "country_codes.json"
    if json_path.exists():
        with open(json_path) as f:
            for entry in json.load(f):
                alpha3 = entry.get("alpha-3", "")
                name = entry.get("name", "")
                if alpha3 and name:
                    names[alpha3] = name
    return names


def _safe(row, col, default=""):
    try:
        val = row.get(col, default) if hasattr(row, 'get') else getattr(row, col, default)
        if pd.isna(val):
            return default
        return val
    except Exception:
        return default


def _round(row, col, decimals=1):
    try:
        val = row.get(col) if hasattr(row, 'get') else getattr(row, col, None)
        if val is None or (isinstance(val, float) and (pd.isna(val) or val != val)):
            return None
        return round(float(val), decimals)
    except Exception:
        return None
