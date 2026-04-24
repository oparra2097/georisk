"""
Tests for baseline overrides applied when the upstream official_debt_gdp
is known to be materially incorrect.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.data_sources.sovereign_debt import (  # noqa: E402
    get_sovereign_debt_data,
    BASELINE_OVERRIDES,
)


def test_senegal_baseline_overridden():
    """
    Senegal upstream baseline was 128.4%, which is above the IMF WEO
    April 2025 post-revelation figure of 99.7%. The override must fire
    and the served official figure must be 99.7%.
    """
    data = get_sovereign_debt_data()
    sen = data["countries"].get("SEN")
    assert sen is not None, "Senegal missing from served data"
    assert sen["official_debt_gdp"] == 99.7, \
        f"SEN override did not fire; got {sen['official_debt_gdp']}"
    assert sen.get("official_debt_gdp_upstream") == 128.4
    assert sen.get("baseline_override_reason")
    assert sen.get("baseline_override_source")


def test_override_preserves_shadow_component():
    """
    Overriding the baseline must not eliminate the shadow component —
    the delta (estimated − official) should be preserved from upstream.
    """
    data = get_sovereign_debt_data()
    sen = data["countries"]["SEN"]
    # Upstream had estimated 191.5 − official 128.4 = 63.1pp shadow.
    # After override: official 99.7 + 63.1 = 162.8 estimated.
    assert abs(sen["debt_gap_pp"] - 63.1) < 0.15, \
        f"SEN shadow component changed; got {sen['debt_gap_pp']}"
    expected_est = round(99.7 + 63.1, 1)
    assert abs(sen["estimated_debt_gdp"] - expected_est) < 0.5, \
        f"SEN estimated mismatch; got {sen['estimated_debt_gdp']}"


def test_override_logged_in_integrity_flags():
    """Overrides must surface in summary.integrity_flags."""
    data = get_sovereign_debt_data()
    flags = data["summary"]["integrity_flags"]
    sen_flags = [f for f in flags if f.get("iso3") == "SEN"
                 and f.get("issue") == "baseline_override"]
    assert sen_flags, "SEN baseline_override not surfaced in integrity_flags"
    assert sen_flags[0].get("upstream_official") == 128.4
    assert sen_flags[0].get("overridden_official") == 99.7


def test_override_list_documented():
    """
    Every entry in BASELINE_OVERRIDES must carry a source citation.
    Prevents adding undocumented numeric overrides.
    """
    for iso3, override in BASELINE_OVERRIDES.items():
        assert override.get("source"), f"{iso3} override missing source"
        assert override.get("reason"), f"{iso3} override missing reason"
        assert override.get("official_debt_gdp") is not None
        assert override.get("upstream_value") is not None
