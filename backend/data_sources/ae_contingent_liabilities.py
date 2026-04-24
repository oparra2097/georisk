"""
Advanced-economy contingent liability stack loader.

Reads per-country YAML files from data/ae_contingent_liabilities/ and
computes extended debt % of GDP as a bottom-up sum of itemised components.
This is the AE methodology v2.0 replacement for the v1.0 BIS-50% rule.

Only countries whose YAML satisfies the CI requirements in SCHEMA.md are
returned as `ready=True`; the rest are carried but marked `under_construction`.
"""

from pathlib import Path
import yaml

_AE_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "ae_contingent_liabilities"
)

_CACHE = None


def _load_all():
    """Load every ISO3.yaml file in the AE dir. Returns dict keyed by ISO3."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    result = {}
    if _AE_DIR.exists():
        for path in sorted(_AE_DIR.glob("*.yaml")):
            if path.name.startswith("_") or path.stem == "SCHEMA":
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
    """CI gate — is this entry complete enough to serve?"""
    if not entry.get("maastricht_debt_pct_gdp"):
        return False, "maastricht_debt_pct_gdp missing"
    if not entry.get("maastricht_debt_source"):
        return False, "maastricht_debt_source missing"
    est = entry.get("extended_debt_estimate") or {}
    if est.get("mid_pct_gdp") is None:
        return False, "extended_debt_estimate.mid_pct_gdp missing"
    if est.get("defensible_ceiling_pct_gdp") is None:
        return False, "defensible_ceiling_pct_gdp missing"
    for c in entry.get("components") or []:
        if not c.get("source_url"):
            return False, f"component {c.get('id')} missing source_url"
    return True, None


def _compute_extended(entry):
    """
    Re-derive extended debt from components, so the CI can catch drift
    between the stated summary and the itemised sum.
    """
    base = float(entry.get("maastricht_debt_pct_gdp") or 0)
    add = 0.0
    for c in entry.get("components") or []:
        if not c.get("include_in_extended"):
            continue
        if c.get("already_in_maastricht"):
            continue
        amount_pct = float(c.get("amount_pct_gdp") or 0)
        lgd = float(c.get("loss_given_default") or 1.0)
        add += amount_pct * lgd
    return round(base + add, 2)


def get_ae_extended_debt():
    """
    Return a dict keyed by ISO3 with extended-debt summary for AE rebuild.

    Each entry has:
      ready: bool               — passes CI gate
      reason: str | None        — why not ready (if applicable)
      name, currency, as_of
      maastricht_debt_pct_gdp
      extended_debt_computed    — sum from components
      extended_debt_stated      — mid_pct_gdp from the YAML summary
      drift_pp                  — computed − stated; flags YAML inconsistency
      defensible_ceiling_pct_gdp
      components_count
    """
    result = {}
    for iso3, entry in _load_all().items():
        ready, reason = _is_ready(entry)
        est = entry.get("extended_debt_estimate") or {}
        stated = est.get("mid_pct_gdp")
        computed = _compute_extended(entry)
        drift = None
        if stated is not None:
            drift = round(computed - float(stated), 2)

        result[iso3] = {
            "ready": ready,
            "reason": reason,
            "name": entry.get("name"),
            "currency": entry.get("currency"),
            "as_of": entry.get("as_of"),
            "maastricht_debt_pct_gdp": entry.get("maastricht_debt_pct_gdp"),
            "extended_debt_computed": computed,
            "extended_debt_stated": stated,
            "drift_pp": drift,
            "extended_low": est.get("low_pct_gdp"),
            "extended_high": est.get("high_pct_gdp"),
            "defensible_ceiling_pct_gdp": est.get("defensible_ceiling_pct_gdp"),
            "components_count": len(entry.get("components") or []),
            "methodology_version": "ae-v2.0-bottom-up",
        }
    return result


def get_ae_detail(iso3):
    """Return the full component-level YAML for a single country."""
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
        "_methodology_version": "ae-v2.0-bottom-up",
    }
