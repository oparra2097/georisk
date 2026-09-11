# Equity Volatility & Options Model

Methodology note for the random-walk options model at `/options-model`
(`backend/options_model/`). Built for high-volatility single names — the
default is **TSEM** (Tower Semiconductor) — where the daily move is large, the
direction is close to a coin flip, and the tradeable question is how the
*range* is priced.

## The claim this model makes, and the one it does not

It does **not** claim to forecast direction. A random walk cannot, and the
walk-forward test in `backtest.py` is there to demonstrate that rather than
assert it: the directional Brier score is reported against a flat 50% baseline
and against a momentum tilt, on every ticker, every time.

It does claim that **volatility is more forecastable than direction**, and that
the gap between the market's implied volatility and a forecast of realized
volatility is a tradeable quantity. How much more forecastable is measured, not
assumed — the same backtest reports RMSE, R² and a Mincer-Zarnowitz slope for
GARCH against EWMA and trailing realized vol. When those numbers are weak, the
page says so and the narrative stops recommending anything.

## Pipeline

```
prices ──> volatility estimates ──> GARCH(1,1) fit ──> forward vol per expiry
                                          │
                                          ├──> Monte Carlo (4 engines)
                                          │         │
 option chain ──> re-implied vols ────────┤         ├──> direction table
                                          │         ├──> strike edge table
                                          └─────────┴──> ranked structures
```

### 1. Volatility (`vol.py`)

Six realized estimators: close-to-close over six windows, EWMA (λ = 0.94), and
the range family — Parkinson, Garman-Klass, Rogers-Satchell, Yang-Zhang.
**Yang-Zhang is the default reference** because it is the only one that is both
drift-independent and gap-aware, which is the return profile of a foundry name
that trades quietly and then gaps on earnings.

GARCH(1,1) is fitted by **Gaussian quasi-MLE with variance targeting**
(ω = σ̄²(1−α−β)), multi-start over (α, β). Variance targeting removes a
parameter and keeps a short single-name sample out of the near-unit-root corner.
Gaussian QMLE stays consistent under fat tails, so tail thickness is estimated
separately as a Student-t degrees of freedom on the standardized residuals and
used only for simulation.

What GARCH buys over EWMA is the **term structure**: forecast variance reverts
geometrically, `E[σ²_{t+k}] = σ̄² + (α+β)^{k-1}(σ²_{t+1} − σ̄²)`, so a one-week
and a three-month option on the same stock are priced off different vols. The
number used for an expiry is the *average* forward variance over its life, not
today's spot vol.

### 2. Simulation (`random_walk.py`)

Four engines run on identical spot, horizon and drift so the spread between them
is readable as model risk:

| Engine | Vol | Shocks | What it adds |
|---|---|---|---|
| `gbm` | constant | Gaussian | The Black-Scholes benchmark |
| `student_t` | constant | Student-t | Fat tails, same centre |
| `garch_t` | GARCH path | Student-t | Clustering + mean reversion in vol |
| `bootstrap` | empirical | resampled blocks | No distributional assumption at all |

The bootstrap is a **stationary block bootstrap** (Politis-Romano, geometric
block lengths, mean 10 days) so clustering and the real gap shape survive
resampling.

Path extremes use an **exact Brownian-bridge correction**: given two daily
closes, the running max of the bridge between them is sampled in closed form
from `P(max > m) = exp(−2(m−a)(m−b)/v)`. Without it, simulated highs and lows
are biased toward the closes and every touch probability comes out too low.
Validated against the closed-form barrier-hitting probability (MC 0.3912 vs
analytic 0.3904 on a 55%-vol, 42-day test).

**Drift is a choice, and it is explicit.** Under the risk-neutral convention the
log drift is `r − q − σ²/2`: the expected return is `r − q` but the *median* is
dragged below it by half the variance. At 55% vol over 42 days that drag alone
puts P(up) near 46.9%. That is arithmetic, not a forecast, and it is the single
most useful thing a random walk tells an options trader. The other modes are
`zero` (exact coin flip), `historical` (sample mean, shrunk 75% toward zero) and
`custom` (impose a view).

### 3. Time convention

Simulation steps in trading days; Black-Scholes takes a year fraction. These
**must** agree or the model "finds" edge that is only a day-count mismatch.
Everything uses `T = trading_days / 252`, with trading days from a business-day
count and a 252/260 holiday haircut. Calendar DTE is used only for display and
per-calendar-day theta.

### 4. Pricing and the chain (`pricing.py`, `data.py`)

Black-Scholes-Merton prices, greeks in desk units (vega per vol point, theta per
calendar day, rho per bp), and implied vol by **bisection** rather than Newton —
a Newton step blows up on the deep wings where vega is ~0, which is exactly
where a stale quote on a thin chain lives.

Implied vols are **re-solved from the current mid**, not taken from the feed,
which computes them off the last trade. The smile is built from out-of-the-money
quotes only. Every chain row carries its own quality flags (no bid, crossed,
wide spread, no open interest) and untradeable legs never enter the ranking.

### 5. Structures (`strategies.py`)

Fifteen standard structures per expiry, anchored on delta (50/30/25/10) rather
than searched exhaustively — with 40 strikes and 6 expiries, an unconstrained
search finds thousands of "edges" that are quote noise.

Each is scored against the simulated terminal distribution:

- **EV** is a *present value*: the expiry payoff is discounted before the entry
  cost is subtracted. A structure priced exactly at the simulated distribution
  then scores zero, so a non-zero EV is a genuine disagreement with the market
  rather than carry on the premium. Verified: a fair-priced synthetic chain
  scores every structure within Monte Carlo noise of zero.
- **POP, max loss, breakevens and CVaR** stay at expiry value — that is the P&L
  that shows up in the account.
- **Fill sensitivity**: every structure is also priced at the marketable fill
  (buy the ask, sell the bid). On a mid-cap single name the spread is usually
  wider than the edge.
- **Vol stress**: re-scored with the vol forecast wrong in both directions by
  the model's own walk-forward RMSE. Short-vol trades die on the high side,
  long-vol trades on the low side. Only structures surviving both, plus the
  spread, plus a Monte Carlo t-statistic above 2, reach the ranked view.
- **Kelly** is solved numerically for the growth-optimal fraction on the actual
  simulated P&L distribution, not from the coin-flip formula — the only version
  that respects fat tails. It is a ceiling, not a position size.

### 6. Walk-forward validation (`backtest.py`)

Every estimate at date *t* uses only data up to *t*. GARCH refits monthly and
re-filters the conditional variance daily in between (neither uses future
information). Three questions:

1. **Direction** — Brier score vs a 50% coin flip, vs climatology, vs a momentum
   tilt, plus a reliability table.
2. **Volatility** — Mincer-Zarnowitz regression of realized forward vol on
   forecast vol, for all four forecasters. A perfect forecast has intercept 0
   and slope 1; slope well below 1 means the forecast over-extrapolates spikes.
3. **Distribution shape** — coverage of the 50/80/90/95% intervals. An option is
   a bet on a region of the distribution, so a model whose 90% interval only
   contains the outcome 78% of the time is underpricing wings regardless of how
   good its point forecast is.

## Known limitations

- **Earnings are not modelled as scheduled jumps.** A horizon spanning an
  earnings date understates the gap. The page flags when the next earnings date
  falls inside the simulated horizon.
- **American exercise is ignored.** Everything is priced European. For calls on
  a non-dividend payer this is exact; for puts it understates value.
- **No historical implied-vol series**, so the variance risk premium cannot be
  backtested directly — only the realized-vol forecast can.
- **Quotes are delayed and often stale.** Yahoo's chain endpoint is the least
  reliable input; the quality flags mitigate but do not fix this.
- **Positive EV means the model disagrees with the market.** The market is not
  automatically the one that is wrong, and a persistent edge on one wing is more
  likely the skew risk premium — compensation for crash risk — than free money.

## Interfaces

```
GET  /api/options-model/model?ticker=TSEM&horizon=21&engine=garch_t&drift=risk_neutral&paths=20000
GET  /api/options-model/quick        # no chain, no backtest
GET  /api/options-model/backtest     # walk-forward only
GET  /api/options-model/export       # xlsx workbook
POST /api/options-model/refresh?ticker=TSEM
```

Terminal report, same payload:

```bash
python scripts/options_model_report.py TSEM --horizon 21 --engine garch_t
python scripts/options_model_report.py MU --drift custom --mu 0.25 --no-backtest
```

Payloads cache for 10 minutes; the walk-forward test caches for 6 hours since it
is the slow piece and its answer barely moves intraday.
