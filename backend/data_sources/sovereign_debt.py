"""
Sovereign Debt Indicator data source.

Primary: loads pre-baked JSON from static/data/sovereign_debt.json
         (committed to repo, deployed with the app).
Fallback: reads Parquet from the sovereign_debt pipeline if available.

Scope note: advanced-economy coverage is suppressed pending methodology
review. The BIS-consolidated-claims adjustment used in the upstream
pipeline miscategorises G-SIB counterparty intermediation as sovereign
liability for AEs, producing indefensible gaps. Only EM/frontier
countries are served until the AE branch is rebuilt bottom-up against
Eurostat supplementary tables and IMF Article IV contingent liabilities.
"""

import os
import json
from pathlib import Path
from datetime import datetime, timedelta

from .benchmarks import reconcile as _reconcile_benchmark
from .em_regional_exposure import apply_regional_exposure as _apply_regional_exposure

METHODOLOGY_VERSION = "v1.4-em-regional-exposure-waemu-sen-graduated"

# IMF WEO Advanced Economies (2024 classification) — suppressed from output
# while AE methodology is under review.
ADVANCED_ECONOMIES_ISO3 = frozenset({
    "AND", "AUS", "AUT", "BEL", "CAN", "CHE", "CYP", "CZE", "DEU", "DNK",
    "ESP", "EST", "FIN", "FRA", "GBR", "GRC", "HKG", "IRL", "ISL", "ISR",
    "ITA", "JPN", "KOR", "LTU", "LUX", "LVA", "MAC", "MLT", "NLD", "NOR",
    "NZL", "PRI", "PRT", "SGP", "SMR", "SVK", "SVN", "SWE", "TWN", "USA",
})

# Baseline overrides — used when upstream official_debt_gdp is known to
# be materially incorrect (e.g. upstream baked in hidden-debt adjustments
# before shadow add-on, causing double-counting). Each entry includes a
# citation to justify the override. Upstream pipeline should ultimately
# be corrected to remove the need for this map.
BASELINE_OVERRIDES = {
    "SEN": {
        "official_debt_gdp": 99.7,
        "source": "IMF WEO April 2025 (post-Cour des Comptes revelation)",
        "upstream_value": 128.4,
        "reason": (
            "Upstream baseline of 128.4% already includes post-2025 "
            "hidden-debt adjustments — adding shadow component on top "
            "would double-count. Override restores clean Maastricht-"
            "equivalent baseline."
        ),
    },
    "GHA": {
        "official_debt_gdp": 82.5,
        "source": "IMF WEO October 2024 database",
        "upstream_value": 70.3,
        "reason": (
            "Upstream baseline of 70.3% is below IMF WEO October 2024 "
            "figure of 82.5% — upstream appears to be using stale "
            "pre-restructuring data. Override restores the current "
            "published general government debt figure."
        ),
    },
}

METHODOLOGY_NOTE = (
    "Advanced economies (IMF WEO classification) are excluded pending "
    "methodology review — bottom-up rebuild available via "
    "/api/ae-contingent-liabilities. Main product covers emerging and "
    "frontier markets; regional-banking exposure (WAEMU titres publics "
    "held by Ivorian and pan-African banks, etc.) is tracked separately "
    "via /api/em-regional-exposure — currently skeleton pending live "
    "BCEAO/AUT data."
)

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
                result = _apply_ae_suppression(result)
                _CACHE = result
                _CACHE_TIME = datetime.now()
                return result
        except Exception as e:
            pass  # fall through to Parquet

    # Strategy 2: Load from Parquet (local dev with sovereign_debt pipeline)
    result = _try_load_parquet()
    if result:
        result = _apply_ae_suppression(result)
        _CACHE = result
        _CACHE_TIME = datetime.now()
        return result

    return {
        "error": "Sovereign debt data not found.",
        "countries": {},
        "summary": {},
    }


def _apply_ae_suppression(result):
    """
    Strip advanced-economy entries, run integrity guards, compute
    per-country sigma, attach benchmark reconciliation, and recompute
    summary on the EM/frontier subset. Keeps upstream data file intact
    so coverage can be re-enabled once the AE methodology is rebuilt.
    """
    countries = result.get("countries") or {}
    filtered = {iso3: c for iso3, c in countries.items()
                if iso3 not in ADVANCED_ECONOMIES_ISO3}

    integrity_flags = []
    for iso3, c in filtered.items():
        _apply_baseline_override(iso3, c, integrity_flags)
        _guard_negative_shadow(iso3, c, integrity_flags)
        _apply_regional_exposure(iso3, c, integrity_flags)
        _compute_sigma(c)
        c["benchmark"] = _reconcile_benchmark(iso3, c.get("estimated_debt_gdp"))

    def _avg(field, decimals=1):
        vals = [c.get(field) for c in filtered.values()
                if c.get(field) is not None]
        return round(sum(vals) / len(vals), decimals) if vals else None

    tier_counts = {}
    for tier in ("Critical", "High", "Elevated", "Moderate", "Low"):
        tier_counts[tier] = sum(1 for c in filtered.values()
                                if c.get("risk_tier") == tier)

    # Reconciliation roll-up
    rec_ok = sum(1 for c in filtered.values()
                 if c.get("benchmark", {}).get("status") == "ok")
    rec_out = sum(1 for c in filtered.values()
                  if c.get("benchmark", {}).get("status") == "out_of_band")
    rec_missing = sum(1 for c in filtered.values()
                      if c.get("benchmark", {}).get("status")
                      in ("no_benchmark", "benchmark_missing_value"))

    # Regional-exposure coverage roll-up
    regional_applied = sum(1 for c in filtered.values()
                           if c.get("regional_exposure_loaded"))
    regional_skeleton = sum(1 for c in filtered.values()
                            if c.get("regional_exposure_reason", "").startswith("skeleton"))

    summary = {
        "total_countries": len(filtered),
        "avg_official": _avg("official_debt_gdp"),
        "avg_estimated": _avg("estimated_debt_gdp"),
        "avg_gap": _avg("debt_gap_pp"),
        "tier_counts": tier_counts,
        "avg_short_term_pct": _avg("short_term_pct"),
        "avg_debt_service_pct": _avg("debt_service_pct_exports"),
        "avg_definition_gap": _avg("definition_gap_pp"),
        "reconciliation": {
            "ok": rec_ok,
            "out_of_band": rec_out,
            "missing_benchmark": rec_missing,
        },
        "regional_exposure": {
            "applied_countries": regional_applied,
            "skeleton_countries": regional_skeleton,
        },
        "integrity_flags": integrity_flags,
    }

    return {
        **result,
        "countries": filtered,
        "summary": summary,
        "methodology_note": METHODOLOGY_NOTE,
        "methodology_version": METHODOLOGY_VERSION,
        "scope": "em_frontier",
    }


def _apply_baseline_override(iso3, c, flags):
    """
    Apply a corrective override to official_debt_gdp when upstream is
    known to be materially incorrect. Recomputes estimated_debt_gdp by
    preserving the original shadow component (estimated − official).
    """
    override = BASELINE_OVERRIDES.get(iso3)
    if not override:
        return
    upstream_official = c.get("official_debt_gdp")
    upstream_estimated = c.get("estimated_debt_gdp")
    new_official = override["official_debt_gdp"]

    if upstream_official is None:
        return

    # Preserve the shadow delta from upstream — but cap it, because in
    # the Senegal case the upstream shadow may itself be miscalibrated.
    original_shadow = 0
    if upstream_estimated is not None:
        original_shadow = max(0.0, upstream_estimated - upstream_official)

    baseline_shift = new_official - upstream_official
    new_estimated = round(new_official + original_shadow, 1)

    c["official_debt_gdp_upstream"] = upstream_official
    c["official_debt_gdp"] = new_official
    c["confidence_floor_gdp"] = new_official
    c["estimated_debt_gdp"] = new_estimated
    c["debt_gap_pp"] = round(original_shadow, 1)

    # Shift the upstream ceiling by the same baseline shift so it continues
    # to bracket the estimate correctly.
    ceiling = c.get("confidence_ceiling_gdp")
    if ceiling is not None:
        c["confidence_ceiling_gdp"] = round(max(new_estimated, ceiling + baseline_shift), 1)

    c["baseline_override_reason"] = override["reason"]
    c["baseline_override_source"] = override["source"]

    flags.append({
        "iso3": iso3,
        "issue": "baseline_override",
        "upstream_official": upstream_official,
        "overridden_official": new_official,
        "source": override["source"],
        "action": (
            f"Baseline reset from {upstream_official}% to {new_official}%; "
            f"shadow component preserved at {round(original_shadow, 1)}pp."
        ),
    })


def _guard_negative_shadow(iso3, c, flags):
    """
    Clamp mathematically-impossible rows where the upstream pipeline
    produced estimated < official. This is a downstream guardrail, not
    a fix — the underlying parquet build still needs to be corrected.
    """
    official = c.get("official_debt_gdp")
    estimated = c.get("estimated_debt_gdp")
    if official is None or estimated is None:
        return
    if estimated < official - 0.05:  # tolerate rounding noise
        flags.append({
            "iso3": iso3,
            "issue": "negative_shadow",
            "official_before": official,
            "estimated_before": estimated,
            "action": "clamped estimated_debt_gdp to official_debt_gdp",
        })
        c["estimated_debt_gdp"] = official
        c["debt_gap_pp"] = 0.0
        c["upstream_integrity_flag"] = "negative_shadow_clamped"
        if c.get("confidence_ceiling_gdp") is not None \
                and c["confidence_ceiling_gdp"] < official:
            c["confidence_ceiling_gdp"] = official


def _compute_sigma(c):
    """
    Per-country sigma derived from input-completeness and governance
    dispersion, replacing the flat 0.35/0.86 placeholder. Range [0.15, 1.0].
    Higher = noisier estimate. Expressed as fraction of estimated_debt_gdp.
    """
    # Start at a base informed by data completeness
    required = [
        "official_debt_gdp", "external_debt_usd_bn", "bis_claims_usd_bn",
        "chinese_lending_usd_bn", "wgi_avg", "gdp_usd_bn",
    ]
    completeness = sum(
        1 for f in required
        if c.get(f) is not None and c.get(f) != 0
    ) / len(required)

    # Governance: WGI in [-2.5, 2.5]; lower = more opaque = more sigma
    wgi = c.get("wgi_avg")
    if wgi is None:
        gov_component = 0.4
    else:
        # Map WGI +2.5 → 0.1, -2.5 → 0.7
        gov_component = max(0.1, min(0.7, 0.4 - 0.12 * float(wgi)))

    # Base sigma: more missing inputs = more noise
    completeness_component = 0.5 * (1 - completeness)

    sigma = round(min(1.0, max(0.15, gov_component + completeness_component)), 2)
    c["sigma"] = sigma


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
