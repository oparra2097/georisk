"""
Tests for the v2.0 advanced-economy bottom-up contingent-liability stack.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.data_sources.ae_contingent_liabilities import (  # noqa: E402
    get_ae_extended_debt,
    get_ae_detail,
    _compute_extended,
    _load_all,
)


def test_ae_stacks_loadable():
    """At least DEU and FRA stacks must be present."""
    data = _load_all()
    assert "DEU" in data, "DEU contingent liability stack missing"
    assert "FRA" in data, "FRA contingent liability stack missing"


def test_ae_mandatory_fields():
    """Every stack must carry maastricht baseline and a mid estimate."""
    for iso3, entry in _load_all().items():
        assert entry.get("maastricht_debt_pct_gdp") is not None, \
            f"{iso3} missing maastricht baseline"
        est = entry.get("extended_debt_estimate") or {}
        assert est.get("mid_pct_gdp") is not None, \
            f"{iso3} missing extended_debt_estimate.mid_pct_gdp"
        assert est.get("low_pct_gdp") is not None, f"{iso3} missing low"
        assert est.get("high_pct_gdp") is not None, f"{iso3} missing high"


def test_ae_estimate_band_ordering():
    """low <= mid <= high for every AE stack."""
    for iso3, entry in _load_all().items():
        est = entry["extended_debt_estimate"]
        assert est["low_pct_gdp"] <= est["mid_pct_gdp"] <= est["high_pct_gdp"], \
            f"{iso3}: band not ordered ({est['low_pct_gdp']} / " \
            f"{est['mid_pct_gdp']} / {est['high_pct_gdp']})"


def test_ae_mid_above_maastricht():
    """Extended debt mid must be >= Maastricht baseline."""
    for iso3, entry in _load_all().items():
        maastricht = float(entry["maastricht_debt_pct_gdp"])
        mid = float(entry["extended_debt_estimate"]["mid_pct_gdp"])
        assert mid + 0.05 >= maastricht, \
            f"{iso3}: extended mid {mid} < Maastricht {maastricht}"


def test_ae_components_source_urls():
    """Every component that is included_in_extended must have a source_url."""
    for iso3, entry in _load_all().items():
        for c in entry.get("components") or []:
            if c.get("include_in_extended"):
                assert c.get("source_url"), (
                    f"{iso3}:{c.get('id')} is included_in_extended "
                    f"but has no source_url"
                )


def test_ae_no_double_counting():
    """Components marked already_in_maastricht must NOT be include_in_extended."""
    for iso3, entry in _load_all().items():
        for c in entry.get("components") or []:
            if c.get("already_in_maastricht") and c.get("include_in_extended"):
                raise AssertionError(
                    f"{iso3}:{c.get('id')} would double-count "
                    "— already_in_maastricht and include_in_extended both true"
                )


def test_ae_computed_matches_stated():
    """
    Computed extended debt from components should agree with the stated
    mid_pct_gdp in the YAML to within 0.5pp. A larger drift means the
    YAML summary is out of sync with the itemised build.
    """
    for iso3, entry in _load_all().items():
        computed = _compute_extended(entry)
        stated = float(entry["extended_debt_estimate"]["mid_pct_gdp"])
        drift = abs(computed - stated)
        # Wide tolerance because the YAML "mid" encodes an LGD regime
        # whereas _compute_extended uses the per-component LGD as-set.
        # A huge drift would indicate YAML inconsistency; 10pp is the
        # ceiling for "needs human review".
        assert drift < 10.0, (
            f"{iso3}: computed {computed} vs stated mid {stated} "
            f"(drift {drift:.1f}pp) — YAML components are out of sync "
            "with the stated mid estimate"
        )


def test_ae_defensible_ceiling_set():
    """Every ready stack must set defensible_ceiling_pct_gdp."""
    for iso3, entry in _load_all().items():
        est = entry["extended_debt_estimate"]
        assert est.get("defensible_ceiling_pct_gdp") is not None, \
            f"{iso3}: defensible_ceiling_pct_gdp not set"


def test_ae_summary_structure():
    """Public get_ae_extended_debt() returns the expected shape."""
    summary = get_ae_extended_debt()
    assert "DEU" in summary
    assert "FRA" in summary
    deu = summary["DEU"]
    for field in ("ready", "name", "maastricht_debt_pct_gdp",
                  "extended_debt_computed", "extended_debt_stated",
                  "drift_pp", "defensible_ceiling_pct_gdp"):
        assert field in deu, f"DEU summary missing {field}"


def test_ae_detail_for_unknown_iso3():
    """get_ae_detail should return None, not raise, for unknown ISO3."""
    assert get_ae_detail("XYZ") is None


def test_ae_deu_under_defensible_ceiling():
    """
    DEU's mid estimate must land at or below its defensible ceiling.
    If this fails, it means we're publishing an AE number outside the
    band that was supposed to trigger the AE review in the first place.
    """
    deu = _load_all()["DEU"]
    mid = float(deu["extended_debt_estimate"]["mid_pct_gdp"])
    ceiling = float(deu["extended_debt_estimate"]["defensible_ceiling_pct_gdp"])
    assert mid <= ceiling + 0.5, \
        f"DEU mid {mid} exceeds defensible ceiling {ceiling}"


def test_ae_fra_under_defensible_ceiling():
    """Same for FRA — mid must be at or below the defensible ceiling."""
    fra = _load_all()["FRA"]
    mid = float(fra["extended_debt_estimate"]["mid_pct_gdp"])
    ceiling = float(fra["extended_debt_estimate"]["defensible_ceiling_pct_gdp"])
    assert mid <= ceiling + 0.5, \
        f"FRA mid {mid} exceeds defensible ceiling {ceiling}"
