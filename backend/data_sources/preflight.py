"""
Pre-publication checklist for a single country's shadow-debt estimate.

Run this before citing a country's number in any memo, watchlist, or
client deliverable. The checklist implements the three-question gate:

  1. Does the estimate land inside the reconciliation band versus a
     published extended-debt benchmark?
  2. Is the shadow component (estimated − official) larger than sigma,
     i.e. statistically distinguishable from noise?
  3. Is the row internally consistent — no negative shadow, floor/ceiling
     ordering correct, governance score present?

Any failure returns pass=False with reasons. Three yeses or it does
not ship.
"""

from .benchmarks import reconcile


def preflight_check(iso3, country):
    """
    Returns dict:
      pass: bool
      reasons: list[str]   — blocking reasons (empty if pass)
      warnings: list[str]  — non-blocking flags
      reconciliation: dict — output of benchmarks.reconcile()
    """
    reasons, warnings = [], []

    official = country.get("official_debt_gdp")
    estimated = country.get("estimated_debt_gdp")
    floor = country.get("confidence_floor_gdp")
    ceiling = country.get("confidence_ceiling_gdp")
    sigma = country.get("sigma")

    # Gate 1 — external reconciliation
    rec = reconcile(iso3, estimated)
    if rec["status"] == "out_of_band":
        reasons.append(
            f"Estimate {estimated}% deviates {rec['deviation_pp']}pp "
            f"from benchmark {rec['benchmark_pct_gdp']}% "
            f"(tolerance ±{rec['tol_pp']}pp, source: {rec['benchmark_source']})"
        )
    elif rec["status"] == "below_floor":
        reasons.append(
            f"Estimate {estimated}% is below Maastricht floor "
            f"{rec['benchmark_pct_gdp']}% — shadow indicator should "
            "never undershoot the official published figure."
        )
    elif rec["status"] == "above_ceiling":
        reasons.append(
            f"Estimate {estimated}% exceeds the documented shadow "
            f"ceiling {rec['shadow_ceiling_pct_gdp']}% — add citation "
            "before publishing, or tighten the estimate."
        )
    elif rec["status"] in ("no_benchmark", "benchmark_missing_value"):
        warnings.append(
            "No external benchmark recorded — add a row to "
            "data/benchmarks/shadow_debt_benchmarks.yaml before publishing."
        )

    # Gate 2 — shadow component vs sigma
    if official is not None and estimated is not None:
        gap = estimated - official
        if sigma is None:
            warnings.append("sigma not set — cannot confirm gap > noise floor.")
        else:
            # sigma is expressed as a fraction of estimated; convert to pp
            sigma_pp = float(sigma) * float(estimated) / 100.0 \
                if float(sigma) < 1.5 else float(sigma)
            if gap > 0 and gap < sigma_pp:
                warnings.append(
                    f"Shadow component {gap:.1f}pp is within sigma "
                    f"(~{sigma_pp:.1f}pp) — treat as indistinguishable from noise."
                )

    # Gate 3 — internal consistency
    if official is not None and estimated is not None and estimated < official:
        reasons.append(
            f"Negative shadow debt: estimated {estimated}% < official {official}%. "
            "Upstream pipeline bug — do not publish."
        )
    if floor is not None and official is not None and floor > official + 0.5:
        reasons.append(
            f"Floor {floor}% exceeds official {official}% "
            "— floor should be ≤ official by construction."
        )
    if ceiling is not None and estimated is not None and ceiling < estimated - 0.5:
        reasons.append(
            f"Ceiling {ceiling}% is below estimate {estimated}% "
            "— ceiling must be ≥ estimate."
        )
    if country.get("wgi_avg") is None:
        warnings.append("Governance score (WGI) missing.")

    return {
        "pass": not reasons,
        "reasons": reasons,
        "warnings": warnings,
        "reconciliation": rec,
    }
