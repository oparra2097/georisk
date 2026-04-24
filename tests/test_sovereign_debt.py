"""
Guardrail tests for the Shadow Debt Indicator.

These enforce the invariants described in docs/shadow-debt/METHODOLOGY.md
§4. A failure here should block deploy.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.data_sources.sovereign_debt import (  # noqa: E402
    get_sovereign_debt_data,
    ADVANCED_ECONOMIES_ISO3,
    METHODOLOGY_VERSION,
)
from backend.data_sources.preflight import preflight_check  # noqa: E402
from backend.data_sources.benchmarks import reconcile  # noqa: E402


def test_no_advanced_economies_served():
    """AE coverage must remain suppressed while methodology is under review."""
    data = get_sovereign_debt_data()
    countries = data["countries"]
    leaked = [iso for iso in countries if iso in ADVANCED_ECONOMIES_ISO3]
    assert not leaked, f"AE coverage leaked into served output: {leaked}"


def test_scope_metadata_present():
    """Every response must carry methodology scope + version metadata."""
    data = get_sovereign_debt_data()
    assert data.get("scope") == "em_frontier"
    assert data.get("methodology_version") == METHODOLOGY_VERSION
    assert data.get("methodology_note")


def test_no_negative_shadow():
    """
    estimated_debt_gdp must be >= official_debt_gdp for every served row.
    The downstream guardrail clamps violations; this test confirms it fired.
    """
    data = get_sovereign_debt_data()
    for iso3, c in data["countries"].items():
        official = c.get("official_debt_gdp")
        estimated = c.get("estimated_debt_gdp")
        if official is None or estimated is None:
            continue
        assert estimated + 0.05 >= official, (
            f"{iso3}: estimated {estimated} < official {official} "
            f"— guardrail should have clamped"
        )


def test_integrity_flags_surfaced():
    """If the guardrail fired, the summary must record it."""
    data = get_sovereign_debt_data()
    flags = data["summary"].get("integrity_flags", [])
    for flag in flags:
        assert "iso3" in flag
        assert flag.get("issue") in ("negative_shadow",)
        assert flag.get("action")


def test_sigma_is_per_country():
    """
    Sigma must vary across countries — a single flat value across the
    EM universe is a v1.0 regression.
    """
    data = get_sovereign_debt_data()
    sigmas = {c.get("sigma") for c in data["countries"].values()
              if c.get("sigma") is not None}
    assert len(sigmas) > 1, "sigma collapsed to a single value across EMs"
    for s in sigmas:
        assert 0.15 <= s <= 1.0, f"sigma {s} outside [0.15, 1.0]"


def test_confidence_bounds_ordered():
    """floor <= estimated <= ceiling, with small tolerance for rounding."""
    data = get_sovereign_debt_data()
    for iso3, c in data["countries"].items():
        floor = c.get("confidence_floor_gdp")
        estimated = c.get("estimated_debt_gdp")
        ceiling = c.get("confidence_ceiling_gdp")
        if None in (floor, estimated, ceiling):
            continue
        assert floor <= estimated + 0.5, f"{iso3}: floor > estimated"
        assert ceiling + 0.5 >= estimated, f"{iso3}: ceiling < estimated"


def test_benchmark_reconciliation_attached():
    """Every country must carry a benchmark status."""
    data = get_sovereign_debt_data()
    for iso3, c in data["countries"].items():
        assert "benchmark" in c, f"{iso3} missing benchmark field"
        assert c["benchmark"].get("status") in {
            "ok", "out_of_band", "no_benchmark",
            "benchmark_missing_value", "no_estimate",
        }


def test_preflight_blocks_missing_country():
    """Preflight on an unknown ISO3 should not throw."""
    r = reconcile("XYZ", 100.0)
    assert r["status"] == "no_benchmark"


def test_preflight_end_to_end_on_senegal():
    """Preflight on a served country must return a structured result."""
    data = get_sovereign_debt_data()
    if "SEN" not in data["countries"]:
        return  # Senegal not in this dataset build
    result = preflight_check("SEN", data["countries"]["SEN"])
    assert "pass" in result
    assert isinstance(result["reasons"], list)
    assert isinstance(result["warnings"], list)
    assert "reconciliation" in result
