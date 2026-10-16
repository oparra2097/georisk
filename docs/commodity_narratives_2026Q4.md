# Commodity Forecast Narratives — Q4 2026 Presentation

**As of:** October 16, 2026
**Horizon:** Q4 2026 → Q3 2027 (4 forward quarters)
**Scenarios:** Oil & Gas use Base / Severe / Worst Case (consumer-perspective disruption gradient). Agriculture & Metals use Bear / Base / Bull (symmetric 95% CI around median).

---

## Oil & Gas

### WTI Crude — `CL=F` · $/bbl

**Spot (Oct 15):** ~$82

**Base narrative.** Hormuz tensions partially recovered through Q4; US shale growth slowing but still-adding; OPEC+ accelerated unwind pushes 2027 into a 3.0-3.5 mbd surplus (IEA). Goldman holds Q4 Brent $80 / WTI $76 and sees 2027 avg Brent $75 / WTI $70. JPM carries a wider 2026 average ($89 WTI) absorbing the summer Hormuz blow-up months, but converges to $70 by 2027.

**Risks.**
- *Severe:* Hormuz choke-points persist; shadow fleet attrition; Russia refining losses compound; Brent $90-100.
- *Worst:* Iran closes Hormuz; strikes on Saudi/UAE infrastructure; Brent >$120 then settles $105-115.

**Model drivers.** DXY, WCESTUS1 (EIA weekly US crude inventories), OVX (CBOE implied vol), GPR (geopolitical risk index), S&P 500. Forward-curve anchor + VECM-of-WTI-Brent-pair for the long end.

---

### Brent Crude — `BZ=F` · $/bbl

**Spot (Oct 15):** ~$86

**Base narrative.** Same macro frame as WTI with a $4-6 structural premium. Goldman Q4 2026 $80 / 2027 avg $75. JPM: 2026 avg $96 (includes Hormuz tail), 2027 $75. Morgan Stanley holds mid-cycle $75. 3.7 mbd global surplus by H2 2027 per IEA weighs on the back of the curve.

**Risks.**
- *Severe:* $90-100 if Hormuz constraints linger into 2027.
- *Worst:* $120+ Q1 2027 if infrastructure strikes resume; settles $105-115.

**Model drivers.** DXY, WCESTUS1, OVX, GPR, WTI cross-commodity. VECM captures the Brent-WTI spread equilibrium.

---

### Natural Gas (Henry Hub) — `NG=F` · $/MMBtu

**Spot (late Sep):** ~$2.88

**Base narrative.** Permian associated-gas glut outruns LNG feedgas demand growth. EIA September STEO: 2026 avg **$3.43**, 2027 **$3.28**. EIR cut 2027 to $3.25 (from $3.50). NGSA winter 2026-27 forward: $3.43 (−24% YoY). LNG exports grow 1.3 Bcf/d in 2026 and 1.7 Bcf/d in 2027 (Plaquemines, Corpus Christi Stage 3, Golden Pass) but supply addition exceeds.

**Risks.**
- *Severe:* Winter cold snap + Freeport-style LNG outage ($4.40-5.00 Q1 2027).
- *Worst:* Harsh Jan-Feb polar vortex + storage draw > 5-year max ($7+ Q1 2027 spike).

**Model drivers.** DXY, DHHNGSP (HH spot leading signal), WTI cross-commodity, S&P 500.

---

### TTF Gas — `TTF=F` · €/MWh

**Spot (Oct 15):** ~€81-85 — highest since early 2023.

**Base narrative.** EU storage at **73.1% vs 90% seasonal norm** (as of Oct 7). 2026 refill target was cut from 90% to 80%. Hormuz LNG disruption + Norway autumn maintenance + Germany undershooting its mandate have driven the Q3 rally. Citi sits deeply bearish at €56 for Q4 (fundamental vs the market); Goldman at €80+ through winter. Bull-to-bear forecast range €46-€92.

**Risks.**
- *Severe:* Cold winter + Norwegian pipeline outage → €95-105 Q1 2027.
- *Worst:* Cold + Qatar LNG strike + Russian pipeline sabotage → €130-150 Q1 2027.

**Model drivers.** DXY, HH cross-commodity, GPR. (AGSI+ EU storage + Norwegian flows noted as future-work gap.)

---

## Agriculture

### Cocoa — `CC=F` · $/MT

**Spot (early Oct):** ~$5,500 (down from $6,569 Sep 1 peak)

**Base narrative.** Severe West African supply contraction is baked in: Ivory Coast 26/27 at **1.77 Mt** (−11-18% from 2.06 Mt YoY), Ghana at **580k** (down from 675k) per Reuters analyst poll. **CPC gives El Niño 81% probability** for Oct-Dec, with rainfall during Aug-Oct pod-development window the key test. The Ivorian smuggling buffer to Ghana (higher farmgate price) may distort reported production. Reuters poll median NY $6,000/t by YE.

**Risks.**
- *Bear:* Rain lands normally during Oct-Dec, 26/27 harvest surprises upside, prices retrace to $4,000-4,500.
- *Bull:* Extreme El Niño; CI output below 1.6 Mt; swollen shoot virus compounds; $7,500+ Q1 2027.

**Model drivers.** DXY, ENSO ONI, AMO (Atlantic SST → West African monsoon), S&P 500.

---

### Wheat — `ZW=F` · ¢/bu

**Spot (Oct):** Dec futures 662-670¢

**Base narrative.** October WASDE **raised** 2026/27 US wheat ending stocks on higher production + imports → bearish signal. World wheat ending-stocks-to-use at ~27% (comfortable). CBOT prices anchored in the 650-680¢ range as US and Black Sea compete on the export market. Rabobank sees prices softer still; USDA and Goldman at ~$6.70-6.80 base.

**Risks.**
- *Bear:* No disruption + continued stock build → 540-580¢ Q3 2027.
- *Bull:* Australia **−9 Mt** on extreme El Niño (per CPC risk estimate) + Russian export tax + Argentine La Niña drought → 880-920¢ Q1 2027.

**Model drivers.** DXY, ENSO ONI, WASDE US ending stocks (NASS Quick Stats), NAO (N. European climate), GPR (Black Sea), WTI (farm input costs).

---

### Soybeans — `ZS=F` · ¢/bu

**Spot (Oct):** Dec futures ~1030-1060¢

**Base narrative.** October WASDE raised 2026/27 US soybean production to **4.56 Bbu** on a yield of **53.1 bu/ac** (+0.3) — bearish. Season-average price projected at **$11.40/bu** (vs $10.40 2025/26). Brazil 2026/27 looks like a record crop (~155 Mt), with Argentine Pampas rainfall the swing. China demand picture mixed — tariff cycles and trade policy keep import flows volatile.

**Risks.**
- *Bear:* South American record crops land; US carryout further builds → 960-1,020¢.
- *Bull:* Argentine La Niña drought in Nov-Mar window + US-China trade escalation → 1,350-1,450¢ Q1-Q2 2027.

**Model drivers.** DXY, ENSO ONI, WASDE US ending stocks, Wheat cross-commodity, S&P 500.

---

### Coffee — `KC=F` · ¢/lb

**Spot (Oct):** Dec Arabica 295-308¢ (spiked briefly to 308 Oct 7)

**Base narrative.** Record 2026/27 Brazil crop universally forecast: **Conab 67.6 M bags**, **USDA FAS 71.9 M**, Marex 75.9 M, Sucafina 75.4 M, StoneX 75.3 M. Arabica production +34.8% YoY, Robusta −6.6%. ICE Robusta inventories at a **4-month high (4,254 lots)**. Analyst consensus is for prices lower as supply lands. Rabobank sees Arabica easing to 245-285¢ through 2027.

**Risks.**
- *Bear:* Record Brazil crop confirms + Vietnam Robusta recovers → 220-250¢ Q3 2027.
- *Bull:* El Niño delays Minas Gerais flowering (Sep-Oct window critical) + Vietnam typhoon damage → 400-460¢ Q1 2027.

**Model drivers.** DXY, ENSO ONI, AMO (Brazil Cerrado rainfall), S&P 500.

---

## Metals

### Gold — `GC=F` · $/oz

**Spot (Oct 15):** ~$4,500

**Base narrative.** **Massive July reset.** JPMorgan cut Q4 2026 target from $6,000 to **$4,500** (−25%) on July 3. Goldman trimmed to **$4,650-4,900 end-2026**. UBS path: **$4,600 Dec 2026 → $5,000 Mar 2027 → $5,400 Sep 2027**. Drivers of the cut: CB buying pace moderated, Fed rate-cut path clearer, dollar bounce. Still a bullish structural case — just slower trajectory than the Q1-2026 debasement-trade peak forecasts implied.

**Risks.**
- *Bear:* Fed stays hawkish; dollar strengthens; CB buying slows further → $4,100-4,400.
- *Bull:* Debasement accelerates; real rates deep negative; CB buying surges → $5,500-6,000 by Q3 2027.

**Model drivers.** DXY, 10Y TIPS real yield, GPR, S&P 500. Long-run trend anchor (20-year window) pulls toward the structural uptrend.

---

### Silver — `SI=F` · $/oz

**Spot (Oct 15):** ~$60

**Base narrative.** JPMorgan: **Q4 2026 $63/oz**, 2026 avg $70, **2027 avg $63** (declining profile as gold cycle rolls). TD Securities bull case $118 H1 2027 if debasement extends. Silver tracks gold with ~2x volatility; solar/industrial demand provides structural floor.

**Risks.**
- *Bear:* Gold cycle tops + industrial recession → $48-54.
- *Bull:* Debasement + silver supply crunch + squeeze dynamics → $85-95.

**Model drivers.** 10Y TIPS, DXY, Gold cross-commodity, Copper cross-commodity.

---

### Platinum — `PL=F` · $/oz

**Spot (Oct 15):** $1,597-1,629 (fell 8.65% in 4 weeks)

**Base narrative.** **Weakest precious metal.** JPMorgan ~$1,800 year-end target. World Platinum Investment Council sees 26/27 structural deficit narrowing. Headwinds: SA power situation improving (Eskom shedding abating) + auto demand softening + hydrogen-economy capex deployed slower than hoped. Palladium at $1,125 also weak (−13.66% past month) — PGM complex broadly out of favor.

**Risks.**
- *Bear:* SA supply fully recovers + ICE auto decline accelerates → $1,450-1,580.
- *Bull:* Hydrogen capex re-accelerates + Russian supply disruption → $2,100-2,300.

**Model drivers.** DXY, Gold cross-commodity, 10Y TIPS real yield.

---

### Copper — `HG=F` · ¢/lb

**Spot (Oct 15):** 653-666¢/lb (record 689¢ on Sep 9)

**Base narrative.** **Record-high regime continues.** Spot hit $6.89/lb on Sep 9. Goldman holds $10,000-11,000/t range ($4.54-5.00/lb) but market is 25-30% above that. TD Securities above-consensus at $13,000/t 2026 avg ($5.90/lb). JPM at $10,500 2026 avg. Structural EV/grid demand + Chile/Peru supply (strikes, water, ore-grade decline) keep inventories tight.

**Risks.**
- *Bear:* China stimulus disappoints + ex-China demand weakens → $5.20-5.80.
- *Bull:* China stimulus lands + supply crunch worsens → $8.00-8.50 Q3 2027.

**Model drivers.** DXY, S&P 500 (industrial activity proxy), 10Y TIPS real yield.

---

### Aluminum — `ALI=F` · $/MT

**Spot (Oct 15):** $2,991 — **34-week low**

**Base narrative.** **Bearish turn.** Goldman sees drop to $2,350/t by late 2026 (bear). TD Securities Q4 $3,700 / Q3 2027 $2,900 (more balanced). Yunnan hydro capacity restarting as rainy season normalises; EU smelter restarts marginal; China capacity-swap rules add supply. CBAM 2026-27 phase-in creates a green-premium split but underlying LME benchmark trends down.

**Risks.**
- *Bear:* Yunnan fully returns + EU smelters restart + CBAM delays → $2,400-2,500.
- *Bull:* Energy price shock + EU curtailment permanent + China cap tighter → $3,800-4,000.

**Model drivers.** DXY, Copper cross-commodity, HH gas cross-commodity (energy cost proxy).

---

## Methodology — How we build these forecasts

Our pipeline follows the **Baumeister & Kilian (2015)** forecast-combination approach: pooling multiple independent model outputs with inverse-RMSE weights, which the literature has shown consistently beats any single model at 1-3 month horizons.

### 1. Per-commodity SARIMAX + GARCH (statsmodels + arch)

- **Mean equation:** SARIMAX(1,0,1) on monthly log-returns, with exogenous drivers listed per-commodity above (DXY, inventories, climate indices, cross-commodities, etc.). Driver choice follows the Baumeister-Kilian supply/demand decomposition — one driver per fundamental.
- **Variance equation:** GARCH(1,1) on SARIMAX residuals, bootstrapped via arch_model simulation (1000 paths) to produce p2.5/p10/p50/p90/p97.5 bands per horizon.
- **Fit window:** 10y for most commodities; **15-20y for metals** (gold/silver/copper/platinum) so the drift estimate isn't biased by short-term consolidation.

### 2. Forward-curve shrinkage (oil & gas only)

For WTI/Brent/HH/TTF, the model's central tendency is horizon-weight-shrunk toward the futures-curve-implied quarterly price:

| Horizon | Model wt | Curve wt |
|---|---:|---:|
| Q+1 | 70% | 30% |
| Q+2 | 55% | 45% |
| Q+3 | 40% | 60% |
| Q+4 | 25% | 75% |

Pulls the long end toward what the market is actually paying for delivery, hedges against model drift misspecification. CI bands widen to encompass both model + curve.

### 3. Long-run trend anchor (metals only)

For gold, silver, platinum, copper, the model median is additionally shrunk toward a 20-year CAGR trend line. Prevents the SARIMAX drift from pulling against the structural uptrend during a short consolidation.

### 4. VECM cointegrating model (WTI ↔ Brent only)

VECM(p=1, rank=1) on monthly log-prices captures the Brent-WTI spread's mean-reversion. Per Baumeister-Kilian (2015), VECM consistently beats univariate ARIMA at 1-3M crude oil horizons. The marginal forecast per commodity is returned as a separate `vecm` block in the API.

### 5. Scenario shock catalogue

Per-commodity "what-if" levers with elasticities grounded in published research (Kilian for oil, FAO for grains, World Gold Council for gold). The scenario builder lets users apply shocks like "OPEC+ cuts 2 mbpd" or "Argentine La Niña" and see the forecast update live. Shocks compose multiplicatively with geometric per-quarter decay — a one-time OPEC cut bites hardest in Q+1 and attenuates ~15% per quarter.

### 6. Fallback: hardcoded SCENARIO_TARGETS

If any model component fails (yfinance network error, insufficient history, driver missing), the pipeline falls back to the hardcoded SCENARIO_TARGETS in `commodities_forecast.py` — the numbers summarised in this document. These are refreshed monthly against October-vintage published research.

### 7. Consensus benchmark overlay

`data/consensus.yaml` tracks published bank forecasts (Goldman Sachs, JPMorgan, UBS, TD Securities, Morgan Stanley, EIA, USDA WASDE, Conab, Rabobank). Shown as a cross-reference overlay on each commodity's chart.

### Benchmarks we try to beat

Per Baumeister-Kilian and the subsequent literature:

- **No-change (random walk)** benchmark — surprisingly hard to beat for oil at horizons >6M.
- **Futures prices** — curve-implied forecast often outperforms AR models in-sample.
- **Equal-weighted combination** — pooling our SARIMAX + VECM + curve + consensus tends to deliver the single best out-of-sample result in the literature; this is the direction we're building toward (Phase 14 of our roadmap).

### Known gaps (future work)

- **EIA STEO official forecasts** as a 4th combination member for oil/gas (requires EIA Open Data API v2 integration).
- **Kilian index of global real activity** (shipping-cost proxy) as a demand driver.
- **AGSI+ EU gas storage** + Norwegian pipeline flows for TTF (currently thin driver set).
- **NOAA CHIRPS gridded rainfall** for region-specific agri signals.
- **MIDAS mixed-frequency** nowcasting to combine weekly inventory data with monthly forecast cadence.
- **Regime-switching SARIMAX** for crisis vs normal periods.

---

## Sources

**Oil & Gas:**
- [Goldman Sachs — oil forecasts (TheStreet digest)](https://www.thestreet.com/investing/goldman-sachs-doubles-down-oil-price-forecast-2026)
- [JPMorgan Oil Prices Forecast](https://www.jpmorgan.com/insights/global-research/commodities/oil-prices)
- [EIA Henry Hub outlook](https://www.eia.gov/todayinenergy/detail.php?id=67004)
- [Enverus — Henry Hub cut to $3.25](https://www.enverus.com/newsroom/u-s-gas-supply-growth-could-outpace-lng-demand-through-2027/)
- [NGSA Winter 2026-27 Outlook](https://energiesmedia.com/ngsa-natural-gas-winter-forecast)
- [European gas prices > €80/MWh in Sep](https://gmk.center/en/news/european-gas-prices-exceeded-e80-mwh-in-september/)
- [TTF multi-year high + storage deficit](https://www.tftc.io/eu-natural-gas-multi-year-highs-winter-storage-deficit-2026)

**Metals:**
- [JPMorgan cuts Q4 2026 gold target to $4,500 (mining.com)](https://www.mining.com/gold-price-could-reach-5055-by-q4-2026-says-jpmorgan/)
- [Bank gold target convergence near $4,500 (goldsilver.com)](https://goldsilver.com/industry-news/article/gold-price-forecast-2026-2027-key-predictions-from-top-analysts/)
- [JPMorgan Silver Price Forecast](https://www.jpmorgan.com/insights/global-research/commodities/silver-prices)
- [TradingKey metals outlook 2026](https://www.tradingkey.com/analysis/commodities/metal/251382340-gold-silver-copper-xau-xag-prediction-2026-tradingkey)
- [TradingEconomics Copper / Platinum / Aluminum spot](https://tradingeconomics.com/commodity/copper)

**Agriculture:**
- [USDA October 2026 WASDE](https://omb.report/downloads/wasde-report-usda-6a1fffa0)
- [Iowa Farm Bureau — Oct WASDE breakdown](https://www.iowafarmbureau.com/Article/October-WASDE-Boosts-US-Corn-and-Soybean-Production-Estimates)
- [Cocoa Prices Rally on Supply Risks (Webull)](https://www.webull.com/news/15679506573493248)
- [West Africa cocoa supply risks (Procurement Resource)](https://www.procurementresource.com/news-and-articles/cocoa-market-surplus-west-africa-production-risk)
- [USDA Projects Record Brazil 26/27 Coffee Crop (CropGPT)](https://cropgpt.ai/usda-projects-record-brazil-2026-27-coffee-crop-arabica-rebounds-prices-fall)
- [Brazil crop recovery pushes coffee lower (ING Think)](https://think.ing.com/articles/brazil-crop-recovery-to-push-coffee-prices-lower/)

**Methodology:**
- [Baumeister & Kilian — Forecasting the Real Price of Oil (2015)](https://www.researchgate.net/publication/276898765)
- [The Art and Science of Forecasting the Real Price of Oil — BoC Review](https://www.bankofcanada.ca/wp-content/uploads/2014/05/boc-review-spring14-baumeister.pdf)
- [Forecasting Industrial Commodity Prices — World Bank WP](https://documents1.worldbank.org/curated/en/099329511212316381/pdf/IDU07821126108129045030af6304c96e8bbbc1a.pdf)
- [Forecasting Commodity Prices: Benchmarks (MDPI)](https://www.mdpi.com/2571-9394/3/2/27)
- [Quantile VAR for Oil Prices (Baumeister et al. 2026)](https://arxiv.org/html/2604.12927)
