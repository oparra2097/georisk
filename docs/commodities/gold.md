# Gold — `GC=F` · $/troy oz

COMEX gold futures, the reference for most financial gold exposure. Key
macro hedge asset; part monetary, part speculative.

## Drivers

- **10Y TIPS real yield (FRED: DFII10)** — the canonical macro anchor.
  Gold pays no yield, so the opportunity cost of holding it is the real
  yield. The correlation is structurally negative on 1-3 year horizons.
- **US Dollar Index (DXY)** — dollar-priced, so a stronger USD makes gold
  more expensive for non-US buyers and typically correlates negatively.
- **Geopolitical Risk Index** — safe-haven flow driver. Episodic, not
  persistent — GPR spikes generate gold rallies but the half-life is
  short.
- **S&P 500 (^GSPC)** — risk-off complement. In correction regimes gold
  decouples from equities; in benign regimes it trades more like a
  commodity.

> Drivers we'd like: World Gold Council central bank purchase data
> (quarterly), ETF holdings (daily), COT positioning. Central bank buying
> has become the dominant marginal demand source in 2023-26, running
> ~1100-1200 tonnes/year — a structural break from the prior regime.

## Structural story

Gold is priced at the intersection of three demand pillars:

- **Monetary / macro** — real rates and USD are the primary signals. The
  2022-24 rate hiking cycle *should* have crushed gold; it didn't, because
  of the second pillar.
- **Central bank reserve diversification** — post-2022 Russian FX
  freeze, EM and non-aligned central banks (China, India, Türkiye, Poland,
  Kazakhstan) stepped up gold purchases as reserve asset de-risking. This
  structural bid is estimated at ~20% of total demand.
- **Speculative / retail** — ETF flows and Chinese retail demand (Shanghai
  premium) add volatility on top. Major upside moves typically coincide
  with ETF inflows, geopolitical shocks, or real yield drops.

The model's exog captures pillars 1 and a noisy read of pillar 3. Pillar
2 is absorbed into the AR component and the drift — this is a
structural-break risk if CB buying pace shifts.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `GC=F` close.
- Exogenous: 10Y TIPS first-difference, DXY log-returns, GPR log-level,
  ^GSPC log-returns.
- GARCH(1,1) on residuals.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual.
- **IMF WEO** — semi-annual.
- **Manual YAML**: GS, JPM, UBS publish year-end and quarterly targets;
  bullion banks publish monthly views.

## Caveats

- **Central bank buying pace is a regime variable** — a policy reversal
  (e.g. reserve dumping to fund fiscal spending) would break the 2023-26
  trend. Not captured by the driver set.
- Real yields can decouple from gold in stagflation regimes (high nominal
  yields + high inflation expectations). Model residuals during Jan-Mar
  2022 were 2+ std dev.
- Does not incorporate options skew, COT positioning, or Shanghai-London
  spread — all of which carry real signal.
