"""
External-benchmark reconciliation for the Shadow Debt Indicator.

Loads published "extended debt" figures from data/benchmarks/
shadow_debt_benchmarks.yaml and compares them against model output.
Used to catch model estimates that drift outside the defensible band
before they ship to a watchlist or memo.

The YAML file is human-maintained — values are pasted in from IMF
Fiscal Monitor, IMF Article IV reports, and Eurostat supplementary
tables. It is NOT auto-generated.
"""

from pathlib import Path
import yaml

_BENCHMARK_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "benchmarks" / "shadow_debt_benchmarks.yaml"
)

_CACHE = None


def load_benchmarks():
    """Load the benchmark YAML. Returns dict keyed by ISO3."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    if not _BENCHMARK_PATH.exists():
        _CACHE = {}
        return _CACHE
    with open(_BENCHMARK_PATH) as f:
        raw = yaml.safe_load(f) or {}
    _CACHE = {iso3: entry for iso3, entry in raw.items() if isinstance(entry, dict)}
    return _CACHE


def reconcile(iso3, estimated_pct_gdp):
    """
    Compare a model estimate against the recorded benchmark.

    benchmark_type (from YAML, default "symmetric"):
      "symmetric" — estimate must land within ±tol_pp of benchmark.
                    Used for AE stacks where benchmark is the bottom-up
                    extended-debt mid estimate.
      "floor"     — estimate must be >= benchmark (with optional
                    shadow_ceiling_pct_gdp as upper bound). Used for
                    EM countries where the benchmark is the Maastricht/
                    WEO figure and any published shadow-debt premium
                    over that floor is the indicator's core output.

    Returns dict with:
      status: "ok" | "below_floor" | "above_ceiling" | "out_of_band" |
              "no_benchmark" | "benchmark_missing_value" | "no_estimate"
      deviation_pp: float or None  (estimate − benchmark)
      tol_pp: float or None
      benchmark_pct_gdp: float or None
      benchmark_type: str
      shadow_ceiling_pct_gdp: float or None
      benchmark_source: str or None
    """
    benchmarks = load_benchmarks()
    entry = benchmarks.get(iso3)
    if not entry:
        return {
            "status": "no_benchmark",
            "deviation_pp": None,
            "tol_pp": None,
            "benchmark_pct_gdp": None,
            "benchmark_type": None,
            "shadow_ceiling_pct_gdp": None,
            "benchmark_source": None,
        }

    bench = entry.get("benchmark_pct_gdp")
    btype = (entry.get("benchmark_type") or "symmetric").lower()
    ceiling = entry.get("shadow_ceiling_pct_gdp")

    base_out = {
        "benchmark_pct_gdp": float(bench) if bench is not None else None,
        "benchmark_type": btype,
        "shadow_ceiling_pct_gdp": float(ceiling) if ceiling is not None else None,
        "benchmark_source": entry.get("benchmark_source"),
        "tol_pp": entry.get("tol_pp"),
    }

    if bench is None:
        return {**base_out, "status": "benchmark_missing_value", "deviation_pp": None}

    if estimated_pct_gdp is None:
        return {**base_out, "status": "no_estimate", "deviation_pp": None}

    est = float(estimated_pct_gdp)
    bench = float(bench)
    dev = round(est - bench, 2)
    base_out["deviation_pp"] = dev

    if btype == "floor":
        # Estimate must sit at or above the Maastricht floor, and below
        # any recorded shadow ceiling. A sensible shadow indicator never
        # falls below the official figure.
        if est < bench - 0.5:
            return {**base_out, "status": "below_floor"}
        if ceiling is not None and est > float(ceiling) + 0.5:
            return {**base_out, "status": "above_ceiling"}
        return {**base_out, "status": "ok"}

    # Symmetric (AE-stack style)
    tol = float(entry.get("tol_pp") or 5.0)
    base_out["tol_pp"] = tol
    return {**base_out, "status": "ok" if abs(dev) <= tol else "out_of_band"}


def reconcile_all(countries):
    """
    Run reconcile() across a dict of countries. Returns a summary:
      { "ok": [iso3,...],
        "out_of_band": [(iso3, deviation, tol),...],
        "below_floor": [iso3,...],
        "above_ceiling": [iso3,...],
        "missing": [iso3,...] }
    """
    ok, out_of_band, below, above, missing = [], [], [], [], []
    for iso3, country in countries.items():
        r = reconcile(iso3, country.get("estimated_debt_gdp"))
        if r["status"] == "ok":
            ok.append(iso3)
        elif r["status"] == "out_of_band":
            out_of_band.append((iso3, r["deviation_pp"], r.get("tol_pp")))
        elif r["status"] == "below_floor":
            below.append(iso3)
        elif r["status"] == "above_ceiling":
            above.append(iso3)
        elif r["status"] in ("no_benchmark", "benchmark_missing_value"):
            missing.append(iso3)
    return {
        "ok": ok,
        "out_of_band": out_of_band,
        "below_floor": below,
        "above_ceiling": above,
        "missing": missing,
    }
