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

    Returns dict with:
      status: "ok" | "out_of_band" | "no_benchmark" | "benchmark_missing_value"
      deviation_pp: float or None  (estimate − benchmark)
      tol_pp: float or None
      benchmark_pct_gdp: float or None
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
            "benchmark_source": None,
        }

    bench = entry.get("benchmark_pct_gdp")
    if bench is None:
        return {
            "status": "benchmark_missing_value",
            "deviation_pp": None,
            "tol_pp": entry.get("tol_pp"),
            "benchmark_pct_gdp": None,
            "benchmark_source": entry.get("benchmark_source"),
        }

    if estimated_pct_gdp is None:
        return {
            "status": "no_estimate",
            "deviation_pp": None,
            "tol_pp": entry.get("tol_pp"),
            "benchmark_pct_gdp": bench,
            "benchmark_source": entry.get("benchmark_source"),
        }

    tol = float(entry.get("tol_pp") or 5.0)
    dev = round(float(estimated_pct_gdp) - float(bench), 2)
    return {
        "status": "ok" if abs(dev) <= tol else "out_of_band",
        "deviation_pp": dev,
        "tol_pp": tol,
        "benchmark_pct_gdp": float(bench),
        "benchmark_source": entry.get("benchmark_source"),
    }


def reconcile_all(countries):
    """
    Run reconcile() across a dict of countries. Returns a summary:
      { "ok": [iso3,...], "out_of_band": [(iso3, deviation, tol),...],
        "missing": [iso3,...] }
    """
    ok, out_of_band, missing = [], [], []
    for iso3, country in countries.items():
        r = reconcile(iso3, country.get("estimated_debt_gdp"))
        if r["status"] == "ok":
            ok.append(iso3)
        elif r["status"] == "out_of_band":
            out_of_band.append((iso3, r["deviation_pp"], r["tol_pp"]))
        elif r["status"] in ("no_benchmark", "benchmark_missing_value"):
            missing.append(iso3)
    return {"ok": ok, "out_of_band": out_of_band, "missing": missing}
