# What Drives Metals Prices — Presentation Appendix

**As of:** October 16, 2026 · **Companion to:** `commodity_narratives_2026Q4.md`

Our 5 metals split into **two very different stories**: precious metals (gold, silver, platinum) are driven primarily by **macro / monetary** forces, industrial metals (copper, aluminum) by **supply-demand fundamentals + China cycle**. Silver bridges both.

| Metal | Dominant driver | Why |
|---|---|---|
| **Gold** | Real yields + CB buying + USD | Pure monetary hedge; no industrial demand floor |
| **Silver** | Gold + solar demand | Half monetary, half industrial |
| **Platinum** | Auto catalyst + SA supply + H₂ | Deep industrial exposure, slower monetary beta |
| **Copper** | China demand + EV/grid capex + mine supply | Industrial bellwether; "Dr Copper" global proxy |
| **Aluminum** | Chinese production policy + power prices | Supply-side (Yunnan hydro), energy-intensive |

---

## Gold — `GC=F` · $/oz

### Supply side (small / stable)

- Mine output ~3,600 t/yr globally — China, Russia, Australia, Canada, US top-5
- Recycling adds ~1,100 t/yr (price-sensitive)
- **Supply is price-inelastic in the near term** — mine lead times 7-10 years. Price moves are almost entirely demand-driven.

### Demand side

- **Central-bank buying** → the structural bid. ~1,100 t/yr in 2024-25, moderating to ~900-1,000 t/yr in 2026 per WGC. **Primary reason** behind the 2022-25 structural uptrend.
- **ETF flows** → the speculative bid. Positive flows amplify rallies; outflows accelerate corrections. SPDR GLD holdings a daily leading indicator.
- **Retail / jewelry** → ~2,100 t/yr. China + India dominate; price-elastic (buyers back off at highs).
- **Industrial** → ~300 t/yr. Negligible.

### Macro drivers (what we model)

1. **10Y TIPS real yield** — the canonical anchor. Gold pays no coupon, so the opportunity cost of holding it is the real yield. **Correlation ~ −0.65** over the past decade. A 50 bp drop in real yields historically moves gold +8-10%.
2. **US Dollar Index (DXY)** — gold is dollar-priced, so a stronger USD makes gold more expensive for non-US buyers. Correlation ~ −0.4.
3. **GPR index (Caldara-Iacoviello)** — captures geopolitical shock premium; episodic, not persistent.
4. **S&P 500 (^GSPC)** — risk-off proxy. In corrections gold decouples from equities; in benign regimes it trades more like a commodity.

### Current catalysts (Oct 2026)

- **JPM cut Q4 target 25%** ($6K → $4.5K) on July 3 → CB buying pace slowing, Fed stance clearer. Reset the whole bank consensus.
- UBS keeps a step-up path: **Dec 2026 $4,600 → Mar 2027 $5,000 → Sep 2027 $5,400**.
- Debasement trade intact but slower than Q1-2026 projections implied.

### Our model's edge

- SARIMAX exog = DXY + 10Y TIPS + GPR + ^GSPC → captures the four forces above
- **20-year fit window** (not 10) because gold has a structural secular trend that a short window misses
- **Long-run trend anchor** shrinks the SARIMAX median toward the 20y CAGR path at long horizons

---

## Silver — `SI=F` · $/oz

### The hybrid problem

- Silver is **~55% industrial / 45% monetary** by current demand share (shifted from 40/60 in the 1980s)
- Volatility ~2x gold; moves amplified in both directions

### Supply side

- Mine output ~26,000 t/yr; **70% is a by-product of base-metals mining** (copper, zinc, lead) → silver supply is price-insensitive to silver, driven by base-metals cycles
- Recycling + above-ground stocks matter — silver can "come out of the vault" at a price

### Demand side

- **Solar PV** → the structural tailwind. PV cells use ~15 g per panel; global deployment grew from 170 GW/yr (2022) to ~600 GW/yr (2026 projected). Silver demand from solar ~200 Moz/yr (~20% of total).
- **Electronics / EVs** — EVs use 25-50 g per vehicle; 5G and data centers add marginal
- **Jewelry / silverware** — ~200 Moz/yr, price-elastic
- **Investment (coins + bars)** — swings sharply with retail sentiment

### Macro drivers (what we model)

1. **10Y TIPS real yield** — same mechanism as gold but ~2x sensitivity (silver amplifies gold moves)
2. **DXY** — dollar inverse correlation
3. **Gold cross-commodity** — gold-silver ratio mean-reverts over 3-5 years (currently ~75, long-run ~65)
4. **Copper cross-commodity** — industrial demand proxy (second half of silver's story)

### Current catalysts (Oct 2026)

- JPM: **Q4 2026 $63/oz**, 2026 avg $70, **2027 avg $63** (declining — gold cycle rolling over)
- TD Securities bull case **$118 H1 2027** if debasement extends
- ICE silver stocks tight — squeeze risk real but low probability

### Our model's edge

- SARIMAX exog = TIPS + DXY + Gold + Copper → captures both halves of silver's duality
- 20-year fit window matches gold

---

## Platinum — `PL=F` · $/oz

### Supply side (highly concentrated)

- **South Africa ~70% of global mine supply** (Anglo American Platinum, Impala, Sibanye-Stillwater, Northam)
- Russia ~15% (Nornickel, affected by sanctions since 2022)
- Zimbabwe + North America fill the rest
- **Eskom power rationing** (SA load-shedding) has driven recurring supply shocks since 2022 — a sustained Stage 6+ event drops output 300-500 koz/yr

### Demand side

- **Autocatalysts ~40%** (primary diesel, some gasoline substitution from palladium). Global ICE auto production matters directly. ICE decline is a slow structural headwind.
- **Jewelry ~25%** (China heavy, price-elastic)
- **Glass manufacturing + petroleum refining ~15%**
- **Hydrogen economy** — long-dated tailwind from PEM electrolyzers + fuel cells (EU, US IRA, Saudi NEOM). Currently ~5% of demand, could reach 15-25% by 2030 if hydrogen deployment hits targets.
- **Investment** — ETF holdings small vs gold/silver

### Macro drivers (what we model)

1. **DXY** — dollar inverse
2. **Gold cross-commodity** — precious-metal complex co-movement; platinum trades at a discount to gold for the first time in decades (ratio ~0.4)
3. **10Y TIPS real yield** — weaker monetary link than gold/silver, but still present via ETF flows

### Current catalysts (Oct 2026)

- Spot $1,597-1,629 (**fell 8.65% in 4 weeks** — weakest precious metal)
- JPM $1,800 YE target → modest recovery expected
- WPIC: 26/27 structural deficit narrowing (SA power recovery weighs)
- Palladium at $1,125 (−13.7% past month) — PGM complex out of favor broadly
- Hydrogen capex timing slower than initial projections

### Our model's edge

- Platinum / palladium complex underweight by most generalists → we track SA supply + gold spread explicitly
- 15-year fit window (shorter than gold/silver) because SA supply regime changed structurally post-2022

---

## Copper — `HG=F` · ¢/lb

### Supply side (bottlenecked)

- **Chile + Peru ~40% of global mine supply**
- Chile faces **declining ore grades** (1.2% → 0.6% over 25 years), water rationing (Atacama drought), labor tension, community protests
- Peru has recurring protests at Las Bambas, Antamina
- **DRC (Katanga)** is the fastest-growing source but increasingly China-controlled
- Smelter capacity constraints (China dominates processing; TC/RC fees ratchet)

### Demand side (bellwether)

- **China ~55% of global consumption** — property construction + manufacturing + grid
- **EV revolution** — EVs use ~4x more copper than ICE vehicles. 70-85 kg per EV vs 20 kg per ICE. BEV penetration moving from 15% to 35% of global sales 2024→2030 → structural demand lift
- **Grid expansion** — wind and solar need 2-5x more copper per MWh than fossil thermal. The electrification capex supercycle
- **AI data centers** — increasingly a swing factor; copper per MW ~2.5 t
- **Inventory cycle** — LME stocks swing 150 kt (tight) to 400+ kt (loose). <100 kt historically precedes price spikes

### Macro drivers (what we model)

1. **DXY** — dollar inverse (medium correlation)
2. **S&P 500** — industrial activity proxy (and copper leads S&P, not the other way)
3. **10Y TIPS real yield** — weaker direct link; lower rates support capex

### Current catalysts (Oct 2026)

- **Spot $6.53-6.66/lb** (record $6.89 Sep 9) — record-high regime
- Goldman $10,000-11,000/t ($4.54-5.00/lb) — materially below spot
- TD Securities **$13,000/t** above-consensus call ($5.90/lb)
- JPM $10,500 2026 avg; slower demand growth 2027
- **China stimulus pending** — swing factor for Q4 2026 / Q1 2027
- Chile Codelco guidance cuts for 2026 (ore-grade decline + maintenance)

### Our model's edge

- 20-year fit window (gold / silver / copper structural metals)
- SARIMAX picks up "China activity" indirectly via S&P 500 (which prices global cycle)
- Long-run trend anchor pulls toward the structural electrification uptrend
- **Known gap**: China Caixin manufacturing PMI would be the single highest-value additional driver — not yet wired (needs separate adapter)

---

## Aluminum — `ALI=F` · $/MT

### Supply side (China-dominated, energy-intensive)

- **China ~60% of global primary aluminum output**; capacity capped by central policy at 45 Mt/yr
- **Yunnan province (hydro-powered) ~25% of China capacity** — faces recurring dry-season power rationing. Dry Yunnan season (Oct-May) typically idles 1-2 Mt of capacity; rainy season restarts
- **Capacity-swap rules** mean new smelters in Gansu/Xinjiang (coal-powered) replace shuttered ones elsewhere
- **European smelters** chronically squeezed by power costs since 2022 (Trimet, Alunorf, Aldel). Multiple curtailed, restart economics require <€80/MWh sustained — not currently available
- **Russian aluminum (Rusal)** carries a persistent discount post-2022

### Demand side

- **Transportation ~27%** (autos — lightweighting for EVs adds ~50 kg Al per vehicle; aerospace)
- **Construction ~25%** (China property cycle sensitive)
- **Packaging ~16%** (stable, non-cyclical floor)
- **Electrical ~12%** (grid wire + transmission infrastructure)
- Green premium — EU CBAM phasing 2026-27 creates a split between low-carbon (hydro-powered) and brown (coal-powered) aluminum. Russian + Chinese coal-based output faces structural discount

### Energy exposure

- **Smelting is 30-40% energy cost** → aluminum price is tightly linked to power prices
- A big enough gas / coal shock changes smelter economics and triggers curtailments

### Macro drivers (what we model)

1. **DXY** — dollar inverse
2. **Copper cross-commodity** — industrial metal complex co-movement
3. **Henry Hub gas cross-commodity** — energy cost proxy

### Current catalysts (Oct 2026)

- **Spot $2,991/t — 34-week low**. Bearish turn.
- Goldman: fall to **$2,350/t late-26** as Yunnan hydro restarts
- TD Securities more balanced: Q4 $3,700 → Q3 2027 $2,900
- Primary driver of weakness: Yunnan rainy-season restart + modest China demand revisions
- Watch: EU CBAM Jan 2027 phase-in (could add $100-200/t premium on EU-stamped low-carbon product)

### Our model's edge

- 10-year fit window (shorter history useful post-2022 energy regime change)
- Energy driver via HH gas cross-commodity is a decent proxy for the broader power-cost story
- **Known gap**: Yunnan hydro state (precipitation + reservoir levels) isn't modeled directly — the single biggest supply swing factor

---

## Common cross-metal patterns

### 1. The real-rate / dollar twin

Every precious metal reacts to **lower real yields** and a **weaker dollar**. These two signals are themselves correlated. Reality check: ~60-70% of 1-month moves in gold and silver can be explained by changes in these two alone.

### 2. The China beta

Copper, aluminum (and silver's industrial half) all have a direct **China manufacturing** sensitivity. China PMI prints are a short-horizon catalyst. We proxy via S&P 500 industrials + the LME base-metals complex.

### 3. The electrification supercycle

Copper + silver + aluminum all benefit from the **EV / grid / data-center electrification** story. Not a short-horizon driver but a structural tailwind that keeps the long-run trend anchor positive for industrial metals.

### 4. The CB / debasement bid

Gold / silver / platinum share the **monetary debasement** story but with different betas: gold most sensitive, platinum least. If this trade accelerates (JPM has it stalling), gold and silver lead and platinum lags.

### 5. The storage / inventory signal

**LME stocks** are the near-term technical signal for copper + aluminum (and nickel + zinc, which we don't track). Watch:
- Copper LME < 100 kt → tight
- Aluminum LME trend — building = bearish, drawing = bullish

Our fallback hardcoded SCENARIO_TARGETS use published bank forecasts anchored to Oct 2026 inventory snapshots.

### 6. Supply shocks are the fat tails

Every metal has a geographic concentration that creates **supply-shock tails**:
- Gold: distributed, low tail risk
- Silver: distributed via base-metals (secondary shock from copper/zinc)
- Platinum: SA power situation — biggest supply-shock exposure
- Copper: Chile strikes / Peru protests / water — moderate tail
- Aluminum: Yunnan hydro — biggest China-side supply shock

These are **not well-captured by SARIMAX** (step-function events). Our scenario shock catalogue is how we let analysts model them manually.

---

## How this fits in our forecast stack

For your presentation, the one-line summary of "what drives metals in our model" is:

> **Precious metals** (Gold / Silver / Platinum): macro-driven SARIMAX exog (real rates + DXY + GPR), shrunk toward a 20-year CAGR trend line to prevent the model from fighting the secular uptrend. Confidence bands via GARCH on residuals.
>
> **Industrial metals** (Copper / Aluminum): SARIMAX exog = DXY + industrial activity proxy (S&P / cross-commodity) + macro rates. Known gap: direct China PMI + LME stocks adapters not yet wired — tracked as future work.
>
> **Scenario shocks** supplement model output: analyst drags sliders for Yunnan hydro, SA power, Chile supply, China stimulus, CB gold buying, and the response plays through published elasticities with geometric per-quarter decay.

---

## Suggested slide structure

1. **Slide 1 — "Metals aren't one asset class"**: the 2-column precious vs industrial table at the top of this doc
2. **Slide 2 — Gold reset**: JPM July cut + UBS path + CB buying pace moderation
3. **Slide 3 — Copper record regime**: $6.89/lb peak Sep 9 + Goldman vs TD bank-forecast split + supply constraints
4. **Slide 4 — Platinum weakest link**: $1,600 spot, SA power recovery, hydrogen capex lag
5. **Slide 5 — Aluminum bearish turn**: 34-week low, Yunnan restart, CBAM phase-in
6. **Slide 6 — What we model vs don't**: methodology stack + the gaps (China PMI, Yunnan hydro, LME stocks)

Numbers to anchor each slide live in `commodity_narratives_2026Q4.md` and `data/consensus.yaml`.
