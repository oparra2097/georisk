"""
Parra Macro — FRB/US-lite reduced-form US macro model.

A ~15-equation error-correction model in 5 blocks (spending, prices, labor,
financial, foreign), estimated on quarterly FRED data 1980Q1+. Inspired by
the Federal Reserve Board's FRB/US model, substantially simplified for a
maintainable single-country v1.

Not a scorecard. Each endogenous variable is an estimated behavioral
equation; the system solves simultaneously each quarter via Gauss-Seidel.

Modules:
    variables    Variable registry (FRED IDs, transforms, block assignments)
    data         Quarterly panel builder with per-series transformations
    estimation   ECM-OLS fitter with AIC lag selection + diagnostics (Phase B)
    model        System assembly + simultaneous solver       (Phases D, E)
    shocks       Shock injection + impulse-response engine   (Phase F)
    routes       Flask blueprint                             (Phase G)
"""
