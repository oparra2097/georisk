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
    Entries still in skeleton state must NOT be layered into
    estimated_debt_gdp. CI gate enforcement. As of v1.4, SEN has
    graduated (stock + inferred_article_iv share). All other WAEMU
    countries remain skeleton pending AUT/BCEAO primary data.
    """
    data = get_sovereign_debt_data()
    still_skeleton = ("CIV", "MLI", "BFA", "NER", "TGO", "BEN", "GNB")
    for iso3 in still_skeleton:
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


def test_senegal_graduated_to_ready():
    """
    SEN should be loaded and applied in v1.4 — stock from UMOA-Titres
    and share from Ecofin/rating-agency 42% Ivorian + other WAEMU.
    If this test fails, SEN has regressed to skeleton.
    """
    data = get_sovereign_debt_data()
    sen = data["countries"].get("SEN")
    assert sen is not None
    assert sen.get("regional_exposure_loaded") is True, (
        "SEN should be ready (stock + inferred_article_iv share); "
        f"reason: {sen.get('regional_exposure_reason')}"
    )
    adj = sen.get("regional_exposure_adjustment_pp")
    assert adj is not None and adj > 5.0, (
        f"SEN regional adjustment suspiciously small: {adj}"
    )
    assert sen.get("regional_exposure_monetary_union") == "WAEMU"


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
    # v1.4: SEN graduated; 7 remain skeleton.
    assert re["applied_countries"] >= 1, (
        f"expected ≥1 applied country (SEN), got {re['applied_countries']}"
    )
    assert re["skeleton_countries"] >= 7, (
        f"expected ≥7 skeleton countries, got {re['skeleton_countries']}"
    )


def test_togo_still_skeleton_despite_grounded_share():
    """
    TGO has inferred_article_iv-grade cross_border_share (Togo First
    data) but null stock — should remain skeleton.
    """
    data = get_sovereign_debt_data()
    tgo = data["countries"].get("TGO")
    if tgo is None:
        return
    assert tgo.get("regional_exposure_loaded") is False
    assert "titres_publics_stock" in (tgo.get("regional_exposure_reason") or "")


def test_inferred_article_iv_countries_have_grounded_sources():
    """
    Countries marked inferred_article_iv must cite IMF primary sources
    in the share_source field. Acceptance markers: 'imf', 'article iv',
    'country report', 'cr ', 'dsa', 'ecofin' (which cites rating-agency
    analysis of primary IMF data), 'togo first' (published
    bank-of-holder breakdown), 'sanction', 'arriérés', 'default'.
    """
    acceptable_markers = (
        "imf", "article iv", "country report", "cr ", "dsa",
        "ecofin", "togo first",
        "sanction", "arriérés", "default",
    )
    for iso3, entry in _load_all().items():
        method = (entry.get("cross_border_share_method") or "").lower()
        if method != "inferred_article_iv":
            continue
        src = (entry.get("cross_border_share_source") or "").lower()
        assert any(m in src for m in acceptable_markers), (
            f"{iso3}: inferred_article_iv grade but source lacks "
            "grounding marker"
        )
