# Platinum — `PL=F` · $/troy oz

NYMEX platinum futures. Precious metal complex, but unlike gold and
silver, platinum demand is dominated by industrial use — primarily
autocatalysts, with growing hydrogen economy demand.

## Drivers

- **US Dollar Index (DXY)** — dollar-priced, inverse correlation.
- **Gold (`GC=F`, cross-commodity)** — precious metal complex co-
  movement. Gold up typically drags platinum up with a delay.
- **10Y TIPS real yield (FRED: DFII10)** — weaker monetary linkage than
  gold or silver but still present via precious metal ETF flows.

> Drivers we'd like: South African power-rationing schedule (Eskom load
> shedding), global automotive production, platinum-group-metal (PGM)
> basket prices (palladium, rhodium). South Africa supplies ~70% of
> global mined platinum; Eskom rationing has caused recurring output
> shocks since 2022. Auto production is the demand anchor — ~40% of
> primary platinum goes to catalytic converters.

## Structural story

Platinum's supply and demand curves both have idiosyncratic structure:

- **Supply**: ~70% from South Africa (Anglo American Platinum, Impala,
  Sibanye-Stillwater, Northam), ~15% from Russia (Nornickel), with the
  rest from Zimbabwe and North America. SA mines are deep, high-cost,
  and power-hungry — they bear the brunt of Eskom load-shedding. A
  sustained power deficit drops ~300-500 koz/year.
- **Demand**: Light-vehicle autocatalysts (primary diesel), heavy-duty
  diesel autocatalysts (Euro 7, China 6b), jewelry (heavy in China),
  glass manufacture, and the emerging hydrogen sector (PEM electrolyzers
  and fuel cells).
- **Palladium substitution** — 2020-22 saw rapid gasoline autocat
  substitution of Pt for Pd as palladium hit $3000/oz; this unwound in
  2023-24 as palladium crashed. Pt-Pd spread dynamics matter.
- **Hydrogen economy** — long-dated tailwind. PEM electrolyzer build-out
  (EU, US IRA, Saudi NEOM) is nascent but growing. Adds maybe 200-400
  koz/year of demand by 2030.

The downside tail is driven by auto-production recession (structural ICE
decline + EV transition); the upside tail by Eskom load-shedding or
Russian supply disruption.

## Model specification

- SARIMAX(1,0,1) on monthly log-returns of `PL=F` close.
- Exogenous: DXY log-returns, Gold log-returns, 10Y TIPS first-difference.
- GARCH(1,1) on residuals.
- 1,000-path bootstrap, 12-month horizon, 4 quarterly means.

## Consensus benchmarks

- **World Bank Pink Sheet** — annual.
- **WPIC (World Platinum Investment Council)** — quarterly supply/demand.
- **Manual YAML**: Heraeus, Johnson Matthey, GS publish PGM targets.

## Caveats

- **South African power risk** is binary-ish — not captured by continuous
  drivers. A sustained Stage 6+ load-shedding episode would spike
  platinum in a way the model wouldn't anticipate.
- **EV transition dynamics** — long-run demand destruction is slow but
  directional. Model's AR component absorbs it as drift; not as a
  structural break.
- Pt-Pd spread is not in the driver set. Substitution regimes can cause
  platinum to diverge from gold in ways the model cannot explain.
- Thin trading volume (compared to gold) means larger microstructure
  noise on short horizons.
