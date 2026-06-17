"""
EM sovereign-debt bottom-up stack loader (v2.0).

Replaces the v1.0 BIS-50% black-box calculation with per-country
itemised components, each sourced and reviewable. Same pattern as the
AE rebuild in backend/data_sources/ae_contingent_liabilities.py but
with EM-specific component categories.

When a country has a ready stack, get_em_stack_estimate() returns its
mid_pct_gdp and the main sovereign_debt pipeline uses that to REPLACE
the upstream estimated_debt_gdp. Countries without a ready stack
continue to use the v1.4-corrected upstream value (AE suppression +
baseline override + negative-shadow guard + regional exposure).
"""

from pathlib import Path
import yaml

_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "em_sovereign_stacks"
)

_CACHE = None


def _load_all():
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    result = {}
    if _DIR.exists():
        for path in sorted(_DIR.glob("*.yaml")):
            if path.name.startswith("_") or path.stem in ("SCHEMA",):
                continue
            try:
                with open(path) as f:
                    entry = yaml.safe_load(f)
            except Exception:
                continue
            if not isinstance(entry, dict):
                continue
            iso3 = (entry.get("iso3") or path.stem).upper()
            result[iso3] = entry
    _CACHE = result
    return result


def _is_ready(entry):
    """CI gate — is this stack complete enough to serve?"""
    if entry.get("imf_general_govt_pct_gdp") is None:
        return False, "imf_general_govt_pct_gdp missing"
    if not entry.get("imf_general_govt_url"):
        return False, "imf_general_govt_url missing"
    est = entry.get("extended_debt_estimate") or {}
    if est.get("mid_pct_gdp") is None:
        return False, "extended_debt_estimate.mid_pct_gdp missing"
    if est.get("defensible_ceiling_pct_gdp") is None:
        return False, "defensible_ceiling_pct_gdp missing"
    components = entry.get("components") or []
    included = [c for c in components if c.get("include_in_extended")]
    if not included:
        return False, "no components marked include_in_extended"
    for c in included:
        if not c.get("source_url"):
            return False, f"component {c.get('id')} missing source_url"
    return True, None


def _compute_extended(entry):
    """Re-derive extended debt from components for drift checking."""
    base = float(entry.get("imf_general_govt_pct_gdp") or 0)
    add = 0.0
    for c in entry.get("components") or []:
        if not c.get("include_in_extended"):
            continue
        if c.get("already_in_general_govt"):
            continue
        amount_pct = float(c.get("amount_pct_gdp") or 0)
        lgd = float(c.get("loss_given_default") or 1.0)
        add += amount_pct * lgd
    return round(base + add, 2)


def get_em_stack_summary():
    """Summary across all EM stacks for /api/em-sovereign-stacks."""
    result = {}
    for iso3, entry in _load_all().items():
        ready, reason = _is_ready(entry)
        est = entry.get("extended_debt_estimate") or {}
        stated = est.get("mid_pct_gdp")
        computed = _compute_extended(entry)
        drift = round(computed - float(stated), 2) if stated is not None else None
        result[iso3] = {
            "ready": ready,
            "reason": reason,
            "name": entry.get("name"),
            "as_of": entry.get("as_of"),
            "imf_general_govt_pct_gdp": entry.get("imf_general_govt_pct_gdp"),
            "extended_debt_stated": stated,
            "extended_debt_computed": computed,
            "drift_pp": drift,
            "extended_low": est.get("low_pct_gdp"),
            "extended_high": est.get("high_pct_gdp"),
            "defensible_ceiling_pct_gdp": est.get("defensible_ceiling_pct_gdp"),
            "components_count": len(entry.get("components") or []),
            "methodology_version": "em-v2.0-bottom-up",
        }
    return result


def get_em_stack_detail(iso3):
    iso3 = iso3.upper()
    entry = _load_all().get(iso3)
    if not entry:
        return None
    ready, reason = _is_ready(entry)
    computed = _compute_extended(entry)
    return {
        **entry,
        "_ready": ready,
        "_reason": reason,
        "_extended_debt_computed": computed,
        "_methodology_version": "em-v2.0-bottom-up",
    }


def apply_em_stack(iso3, country, flags):
    """
    If a ready EM stack exists for this country, REPLACE the upstream
    estimated_debt_gdp with the stack mid figure. This is the v2.0 fix
    for the BIS-50% inheritance — once a country has a stack, it stops
    using the upstream black box.

    Returns True if applied, False otherwise. Mutates country dict in
    place and appends an integrity flag.
    """
    entry = _load_all().get(iso3)
    if not entry:
        country["em_stack_loaded"] = False
        country["em_stack_reason"] = "no_stack"
        return False
    ready, reason = _is_ready(entry)
    if not ready:
        country["em_stack_loaded"] = False
        country["em_stack_reason"] = f"skeleton: {reason}"
        return False

    est = entry.get("extended_debt_estimate") or {}
    new_estimated = float(est.get("mid_pct_gdp"))
    old_estimated = country.get("estimated_debt_gdp")

    country["em_stack_loaded"] = True
    country["em_stack_methodology"] = "v2.0-bottom-up"
    country["em_stack_components_count"] = len(entry.get("components") or [])
    country["em_stack_low"] = est.get("low_pct_gdp")
    country["em_stack_high"] = est.get("high_pct_gdp")
    country["em_stack_defensible_ceiling"] = est.get("defensible_ceiling_pct_gdp")

    if old_estimated is not None:
        country["estimated_debt_gdp_upstream"] = old_estimated

    country["estimated_debt_gdp"] = round(new_estimated, 1)
    if country.get("official_debt_gdp") is not None:
        country["debt_gap_pp"] = round(new_estimated - country["official_debt_gdp"], 1)
    if est.get("defensible_ceiling_pct_gdp") is not None:
        country["confidence_ceiling_gdp"] = round(
            max(new_estimated, float(est["defensible_ceiling_pct_gdp"])), 1
        )

    flags.append({
        "iso3": iso3,
        "issue": "em_stack_applied",
        "upstream_estimate": old_estimated,
        "stack_estimate": new_estimated,
        "components_count": len(entry.get("components") or []),
        "action": (
            f"v2.0 EM stack replaced upstream estimate "
            f"({old_estimated}% → {new_estimated}%) using "
            f"{len(entry.get('components') or [])} sourced components."
        ),
    })
    return True
