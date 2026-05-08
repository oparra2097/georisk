# Brent Crude Oil — `BZ=F` · $/bbl

North Sea Brent, the global benchmark for seaborne crude. Priced by
Platts against BFOE (Brent/Forties/Oseberg/Ekofisk/Troll) cargoes; used
as the reference for approximately two-thirds of global physical oil.

## Drivers

- **US Dollar Index (DXY, FRED: DTWEXBGS)** — same mechanism as WTI; Brent
  is globally dollar-priced.
- **Geopolitical Risk Index (Caldara & Iacoviello)** — Brent is more
  sensitive to Middle East disruption than WTI because it is the global
  seaborne benchmark; ME export flows move Brent first and WTI only via
  the arb.
- **WTI Crude price (cross-commodity driver, `CL=F`)** — captures the
  physical Brent-WTI arb. Structural shifts in US export capacity and
  Cushing storage show up as a persistent spread change.

## Structural story

Brent leads the global complex: OPEC+ decisions, sanctions on Iranian and
Russian barrels, Red Sea shipping disruption, Chinese import quotas, and
European refinery outages all hit Brent before they feed to WTI through
the atlantic arb. The Brent-WTI spread reflects US shale growth vs. global
seaborne tightness. Post-2022 Russian crude displacement and Urals
discount dynamics reshaped flows, with India and China absorbing Urals
while Europe pivoted to US WTI exports, Angola, Guyana and ME barrels.

Brent's upside tail is driven by combined OPEC+ supply discipline and
Iran / Strait of Hormuz disruption; the downside tail by demand shocks,
particularly Chinese stimulus disappointments.

Scenario mapping identical to WTI (Base/Severe/Worst = p50/p90/p97.5).

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `BZ=F` close.
- Exogenous: DXY log-returns, GPR log-level, WTI log-returns.
- GARCH(1,1) on residuals.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **EIA STEO** — monthly, includes Brent.
- **World Bank Pink Sheet** — Brent annual.
- **Manual YAML**: GS, JPM, UBS quarterly.

## Caveats

- ME tail events understated — see WTI note.
- The Brent-WTI driver is endogenous to global flows; in regimes where
  structural bottlenecks move both benchmarks together, exogeneity
  assumption weakens and residuals can cluster.
- Does not incorporate physical grade differentials (Dated Brent vs
  futures) or time-spread signals from the forward curve.
