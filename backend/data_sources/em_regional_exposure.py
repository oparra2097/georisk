"""
EM Regional Banking Exposure loader.

Fills the WAEMU/CEMAC/SADC blind spot in the v1.0 EM shadow-debt
pipeline — cross-border bank holdings of EM sovereign debt that
BIS Consolidated Banking Statistics does not capture because no
WAEMU/CEMAC country is a BIS reporter.

Each YAML in data/em_regional_exposure/ holds one country's estimate
sourced to BCEAO/BEAC monetary surveys, Agence UMOA-Titres, IMF
Article IV reports, and SARB cross-border data. See SCHEMA.md.

Exposed to the main sovereign-debt pipeline via apply_regional_exposure()
which layers the exposure onto estimated_debt_gdp for any EM country
with a ready YAML.
"""

from pathlib import Path
import yaml

_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "em_regional_exposure"
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
    """
    CI gate — is this entry complete enough to apply?

    Requires ALL of:
    - Non-null regional_bank_exposure_usd_bn.
    - Local-currency stock figure populated (not derived from aggregate
      share of a WAEMU/CEMAC total).
    - cross_border_share_method in {published, inferred_article_iv}.
    - double_count_check populated.
    - At least one source URL on some field.

    The local-currency stock requirement is what distinguishes real
    data pulled from BCEAO/AUT from an order-of-magnitude allocation
    of a WAEMU aggregate. Without it, the exposure number cannot be
    said to be source-grounded even if the share methodology is.
    """
    if entry.get("regional_bank_exposure_usd_bn") is None:
        return False, "regional_bank_exposure_usd_bn null"
    if entry.get("titres_publics_stock_local_bn") is None:
        return False, (
            "titres_publics_stock_local_bn null — exposure is derived "
            "from aggregate allocation, not pulled from BCEAO/AUT"
        )
    method = (entry.get("cross_border_share_method") or "").lower()
    if method not in ("published", "inferred_article_iv"):
        return False, f"cross_border_share_method '{method}' is not adequately sourced"
    if not entry.get("double_count_check"):
        return False, "double_count_check is empty"
    sources = [
        entry.get("titres_publics_url"),
        entry.get("banking_system_claims_url"),
        entry.get("cross_border_share_url"),
    ]
    if not any(sources):
        return False, "no source_url present on any field"
    return True, None


def get_regional_exposure_summary():
    """Return dict keyed by ISO3 with summary for every YAML in the dir."""
    result = {}
    for iso3, entry in _load_all().items():
        ready, reason = _is_ready(entry)
        result[iso3] = {
            "ready": ready,
            "reason": reason,
            "name": entry.get("name"),
            "monetary_union": entry.get("monetary_union"),
            "regional_bank_exposure_usd_bn": entry.get("regional_bank_exposure_usd_bn"),
            "regional_bank_exposure_pct_gdp": entry.get("regional_bank_exposure_pct_gdp"),
            "cross_border_share": entry.get("cross_border_share"),
            "cross_border_share_method": entry.get("cross_border_share_method"),
            "include_in_shadow": entry.get("include_in_shadow", True),
            "loss_given_default": entry.get("loss_given_default", 1.0),
            "as_of": entry.get("as_of"),
        }
    return result


def get_regional_detail(iso3):
    """Return full YAML for one country."""
    iso3 = iso3.upper()
    entry = _load_all().get(iso3)
    if not entry:
        return None
    ready, reason = _is_ready(entry)
    return {**entry, "_ready": ready, "_reason": reason}


def apply_regional_exposure(iso3, country, flags):
    """
    Mutate a country dict from the main pipeline by layering regional
    bank exposure onto estimated_debt_gdp. No-op when:
      - No YAML file exists for this ISO3.
      - YAML exists but is not ready (missing fields, weak sourcing).
      - include_in_shadow is false.
      - Country GDP is missing.

    Adds:
      country["regional_exposure_adjustment_pp"] — pp added
      country["regional_exposure_loaded"] — bool
      country["regional_exposure_reason"] — why skipped, if skipped

    Appends a structured entry to `flags` (the integrity_flags list)
    so the adjustment is auditable in the served summary.
    """
    entry = _load_all().get(iso3)
    country["regional_exposure_loaded"] = False
    if not entry:
        return

    ready, reason = _is_ready(entry)
    if not ready:
        country["regional_exposure_loaded"] = False
        country["regional_exposure_reason"] = f"skeleton: {reason}"
        return

    if not entry.get("include_in_shadow", True):
        country["regional_exposure_loaded"] = False
        country["regional_exposure_reason"] = "include_in_shadow: false"
        return

    gdp_usd = country.get("gdp_usd_bn")
    if not gdp_usd:
        country["regional_exposure_loaded"] = False
        country["regional_exposure_reason"] = "main-pipeline gdp_usd_bn missing"
        return

    exposure_usd = float(entry["regional_bank_exposure_usd_bn"])
    lgd = float(entry.get("loss_given_default", 1.0))
    adjustment_pp = round(exposure_usd / float(gdp_usd) * 100.0 * lgd, 2)

    old_estimated = country.get("estimated_debt_gdp") or 0
    new_estimated = round(old_estimated + adjustment_pp, 1)

    country["regional_exposure_loaded"] = True
    country["regional_exposure_adjustment_pp"] = adjustment_pp
    country["regional_exposure_exposure_usd_bn"] = exposure_usd
    country["regional_exposure_lgd"] = lgd
    country["regional_exposure_monetary_union"] = entry.get("monetary_union")
    country["regional_exposure_as_of"] = entry.get("as_of")

    # Update the shadow component.
    country["estimated_debt_gdp"] = new_estimated
    if country.get("official_debt_gdp") is not None:
        country["debt_gap_pp"] = round(
            new_estimated - country["official_debt_gdp"], 1
        )

    # Raise ceiling proportionally so bounds stay consistent.
    if country.get("confidence_ceiling_gdp") is not None:
        country["confidence_ceiling_gdp"] = round(
            max(country["confidence_ceiling_gdp"], new_estimated), 1
        )

    flags.append({
        "iso3": iso3,
        "issue": "regional_exposure_applied",
        "monetary_union": entry.get("monetary_union"),
        "adjustment_pp": adjustment_pp,
        "exposure_usd_bn": exposure_usd,
        "lgd": lgd,
        "action": (
            f"Layered {adjustment_pp}pp of regional-bank exposure "
            f"({entry.get('monetary_union')}) onto estimated_debt_gdp "
            f"(LGD {lgd})."
        ),
    })
