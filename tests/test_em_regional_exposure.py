"""
Tests for the EM regional-banking exposure layer (v1.3).

Ensures the WAEMU/CEMAC/SADC blind-spot layer loads correctly, the CI
gate correctly holds back skeleton entries from applying, and the
layer integrates with the main pipeline without breaking guardrails.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.data_sources.em_regional_exposure import (  # noqa: E402
    get_regional_exposure_summary,
    get_regional_detail,
    _load_all,
    _is_ready,
)
from backend.data_sources.sovereign_debt import get_sovereign_debt_data  # noqa: E402


def test_waemu_skeletons_loaded():
    """All 8 WAEMU countries must have a YAML file."""
    loaded = _load_all()
    for iso3 in ("SEN", "CIV", "MLI", "BFA", "NER", "TGO", "BEN", "GNB"):
        assert iso3 in loaded, f"WAEMU country {iso3} missing YAML"
        assert loaded[iso3].get("monetary_union") == "WAEMU"


def test_skeleton_entries_not_applied_to_shadow():
    """
    All WAEMU entries are currently order_of_magnitude — the CI gate
    must hold them back from being layered into estimated_debt_gdp.
    This fails IF/WHEN someone promotes them to 'published' without
    validating.
    """
    data = get_sovereign_debt_data()
    for iso3 in ("SEN", "CIV", "MLI", "BFA", "NER", "TGO", "BEN", "GNB"):
        c = data["countries"].get(iso3)
        if c is None:
            continue
        assert c.get("regional_exposure_loaded") is False, (
            f"{iso3}: regional exposure was applied despite skeleton status"
        )
        reason = c.get("regional_exposure_reason", "")
        assert reason.startswith("skeleton"), (
            f"{iso3}: expected skeleton reason, got {reason}"
        )


def test_ci_gate_method_enforcement():
    """
    The CI gate must reject cross_border_share_method=order_of_magnitude.
    Only 'published' or 'inferred_article_iv' may ship.
    """
    for iso3, entry in _load_all().items():
        method = (entry.get("cross_border_share_method") or "").lower()
        ready, reason = _is_ready(entry)
        if method == "order_of_magnitude":
            assert not ready, (
                f"{iso3}: order_of_magnitude method incorrectly passed CI gate"
            )


def test_double_count_check_present_on_every_entry():
    """Every YAML must have a non-empty double_count_check."""
    for iso3, entry in _load_all().items():
        assert entry.get("double_count_check"), (
            f"{iso3}: double_count_check is empty"
        )


def test_regional_summary_structure():
    """Public summary returns expected shape."""
    summary = get_regional_exposure_summary()
    assert "SEN" in summary
    sen = summary["SEN"]
    for field in ("ready", "name", "monetary_union",
                  "regional_bank_exposure_usd_bn",
                  "cross_border_share", "cross_border_share_method",
                  "include_in_shadow", "loss_given_default"):
        assert field in sen, f"SEN summary missing {field}"


def test_regional_detail_for_unknown_iso3():
    """get_regional_detail returns None, not raise, for unknown ISO3."""
    assert get_regional_detail("XYZ") is None


def test_integration_surfaces_in_summary():
    """Main pipeline summary must expose regional-exposure roll-up."""
    data = get_sovereign_debt_data()
    re = data["summary"].get("regional_exposure")
    assert re is not None, "summary.regional_exposure missing"
    assert "applied_countries" in re
    assert "skeleton_countries" in re
    # Right now all 8 WAEMU YAMLs are skeleton, so applied should be 0.
    assert re["applied_countries"] == 0, (
        f"expected 0 applied countries (all skeleton), got {re['applied_countries']}"
    )
    assert re["skeleton_countries"] >= 8, (
        f"expected ≥8 skeleton countries (WAEMU), got {re['skeleton_countries']}"
    )


def test_senegal_shows_skeleton_in_country_detail():
    """Served Senegal entry must carry the skeleton status visibly."""
    data = get_sovereign_debt_data()
    sen = data["countries"].get("SEN")
    assert sen is not None
    assert sen.get("regional_exposure_loaded") is False
    assert "skeleton" in (sen.get("regional_exposure_reason") or "")


def test_inferred_article_iv_countries_have_grounded_sources():
    """
    MLI and NER are marked inferred_article_iv via documented sanctions
    episodes. Verify that grounding is in the YAML.
    """
    for iso3 in ("MLI", "NER"):
        entry = _load_all()[iso3]
        method = entry.get("cross_border_share_method", "").lower()
        assert method == "inferred_article_iv", (
            f"{iso3}: expected inferred_article_iv, got {method}"
        )
        src = (entry.get("cross_border_share_source") or "").lower()
        assert "sanction" in src or "arriérés" in src or "default" in src, (
            f"{iso3}: sanctions/arrears evidence not cited in source"
        )
