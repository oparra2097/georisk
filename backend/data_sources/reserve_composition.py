"""
Gold vs US-Treasurys share-of-reserves crossover.

Reproduces the Torsten Sløk / Apollo (originally Crescat / Tavi Costa) chart:
"for the first time since 1996, foreign central banks hold more gold than
US Treasurys." Two lines, each as a % of total global official reserves
(incl. gold):

  - Gold share     = world CB gold value (tonnes × LBMA gold price)
                     ÷ total reserves
  - Treasury share = foreign holdings of US Treasurys (FRED FDHBFIN)
                     ÷ total reserves

World gold tonnage and total reserves are aggregated from our own reserves
data (``get_cofer_data`` — IMF IFS + WGC/PBoC overlay). The gold price
history (GOLDPMGBD228NLBM, LBMA PM fix) and foreign Treasury holdings
(FDHBFIN, quarterly back to 1970) come from FRED.

Note on levels vs crossing: the two lines share one denominator, so the
CROSSOVER timing depends only on (world gold value) vs (foreign Treasury
holdings) — it's independent of how completely we measure total reserves.
The denominator only scales the % levels. So the crossing is robust even
though our reserve coverage (~110 countries) understates the world total a
little (we exclude the IMF/BIS/ECB institutional holdings).
"""

import datetime as _dt
import logging
import threading
import time

logger = logging.getLogger(__name__)

CACHE_TTL = 21600  # 6 hours

TROY_OZ_PER_TONNE = 32150.7466

# FRED series
FRED_GOLD_PM = 'GOLDPMGBD228NLBM'   # LBMA Gold Price PM, USD/oz, daily
FRED_GOLD_AM = 'GOLDAMGBD228NLBM'   # AM fix fallback
# Foreign-OFFICIAL holdings of US Treasurys (central banks) — this is the
# series Sløk's "central banks" framing uses, and the one that actually
# crosses (~$3.8T vs world gold). Total-foreign FDHBFIN (~$8.5T, incl.
# private) is far larger than world gold value, so it never crosses; we
# keep it only as a fallback if the official series is unavailable.
FRED_FOREIGN_OFFICIAL_UST = 'BOGZ1FL263061130Q'  # foreign official, quarterly
FRED_FOREIGN_UST = 'FDHBFIN'                      # total foreign (fallback)


class _CrossoverCache:
    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._ts = 0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._ts) < CACHE_TTL:
                return self._data
        data = _build_crossover()
        if data and not data.get('meta', {}).get('error'):
            with self._lock:
                self._data = data
                self._ts = time.time()
        return data

    def clear(self):
        with self._lock:
            self._data = None
            self._ts = 0


_cache = _CrossoverCache()


def _empty(error):
    return {'periods': [], 'gold_share': [], 'treasury_share': [],
            'meta': {'error': error}}


def _fred_by_month(series_id, start='2000-01-01'):
    """Fetch a FRED series and collapse to {`YYYY-MM`: last_value_in_month}."""
    from backend.data_sources.fred_client import fetch_series
    obs = fetch_series(series_id, start_date=start)
    by_month = {}
    for o in obs:
        d = o.get('date', '')
        v = o.get('value')
        if len(d) >= 7 and v is not None:
            by_month[d[:7]] = float(v)  # asc order → last write wins = month-end
    return by_month


def _ffill_monthly(by_month, periods):
    """Forward-fill a sparse {period: value} map across the ordered periods
    (e.g. quarterly FDHBFIN → monthly). Returns a list aligned to periods."""
    out = []
    last = None
    for p in periods:
        if p in by_month:
            last = by_month[p]
        out.append(last)
    return out


def _world_series_from_cofer():
    """Sum world gold tonnage and total reserves per period from /api/cofer.

    Returns (periods, world_gold_tonnes[], total_reserves_usd_b[]).
    """
    from backend.data_sources.imf_cofer import get_cofer_data
    d = get_cofer_data() or {}
    periods = d.get('years') or []
    countries = d.get('countries') or []
    n = len(periods)
    if not n or not countries:
        return [], [], []

    def _ffill(arr):
        """Carry each country's last reported value forward to the end.

        A central bank's gold/reserves don't disappear just because it
        reported late, so the WORLD aggregate must not collapse at the
        right edge where only a few countries have posted the newest
        month. Leading Nones (before a country's first report) stay None.
        """
        out = []
        last = None
        for i in range(n):
            v = arr[i] if i < len(arr) else None
            if v is not None:
                last = float(v)
            out.append(last)
        return out

    gold_t = [0.0] * n
    gold_have = [False] * n
    total_b = [0.0] * n
    total_have = [False] * n
    for c in countries:
        gt = _ffill(c.get('gold_tonnes') or [])
        tr = _ffill(c.get('total_reserves') or [])
        for i in range(n):
            if gt[i] is not None:
                gold_t[i] += gt[i]
                gold_have[i] = True
            if tr[i] is not None:
                total_b[i] += tr[i]
                total_have[i] = True

    world_gold = [gold_t[i] if gold_have[i] else None for i in range(n)]
    world_total = [total_b[i] if total_have[i] else None for i in range(n)]
    return periods, world_gold, world_total


def _gold_price_by_month():
    """Monthly gold price ``{YYYY-MM: usd_per_oz}``.

    Primary: yfinance GC=F (the same source as our live spot — works on
    Render). Fallback: FRED LBMA. Returns ``(by_month, source)``.
    """
    try:
        from backend.data_sources.market_data import get_gold_monthly_history
        ymonth = get_gold_monthly_history()
        if ymonth:
            return ymonth, 'yfinance GC=F'
    except Exception as e:
        logger.warning('yfinance gold history failed: %s', e)
    fred = _fred_by_month(FRED_GOLD_PM) or _fred_by_month(FRED_GOLD_AM)
    if fred:
        return fred, 'FRED LBMA'
    return {}, None


def _build_crossover():
    """Assemble the gold-vs-Treasury share series."""
    try:
        periods, world_gold_t, world_total_b = _world_series_from_cofer()
        if not periods:
            return _empty('No reserves data available to aggregate')

        gold_px, gold_src = _gold_price_by_month()
        # Foreign-OFFICIAL Treasury holdings (Sløk's "central banks" line);
        # fall back to total-foreign only if the official series is empty.
        ust = _fred_by_month(FRED_FOREIGN_OFFICIAL_UST)
        ust_src = 'FDHBFIN-official (BOGZ1FL263061130Q)'
        if not ust:
            ust = _fred_by_month(FRED_FOREIGN_UST)
            ust_src = 'FDHBFIN total-foreign (fallback)'
        if not gold_px or not ust:
            # Be specific so the failure is unambiguous (gold price comes
            # from yfinance, NOT FRED — only the Treasury series needs the
            # FRED key).
            from backend.data_sources.fred_client import _get_api_key
            key_ok = bool(_get_api_key())
            reasons = []
            if not gold_px:
                reasons.append('gold price unavailable (yfinance GC=F + FRED LBMA both empty)')
            if not ust:
                reasons.append(
                    'foreign Treasury holdings empty — '
                    + ('FRED key detected but the series returned no rows (rate-limit or series issue)'
                       if key_ok else 'no FRED_API_KEY detected in the environment')
                )
            return _empty('; '.join(reasons))

        # Gold price forward-filled to every month; Treasury (quarterly)
        # forward-filled to every month.
        gold_px_m = _ffill_monthly(gold_px, periods)
        ust_m = _ffill_monthly(ust, periods)

        # FDHBFIN scale: FRED reports it in millions of USD (≈ 7,000,000 for
        # ~$7T). Normalize to USD billions to match total_reserves ($B).
        ust_max = max((v for v in ust_m if v is not None), default=0)
        ust_scale = 1e-3 if ust_max > 1e5 else 1.0  # millions→billions, else already $B

        gold_share = []
        treasury_share = []
        for i, p in enumerate(periods):
            gt = world_gold_t[i]
            tot = world_total_b[i]
            px = gold_px_m[i]
            tre = ust_m[i]
            gs = None
            ts = None
            if gt and tot and px and tot > 0:
                gold_val_b = gt * TROY_OZ_PER_TONNE * px / 1e9
                gs = round(gold_val_b / tot * 100, 2)
            if tre is not None and tot and tot > 0:
                tre_b = tre * ust_scale
                ts = round(tre_b / tot * 100, 2)
            gold_share.append(gs)
            treasury_share.append(ts)

        # Find the most recent crossover (gold share rising above treasury).
        crossover_period = None
        for i in range(len(periods) - 1, 0, -1):
            g, t = gold_share[i], treasury_share[i]
            gp, tp = gold_share[i - 1], treasury_share[i - 1]
            if None in (g, t, gp, tp):
                continue
            if g >= t and gp < tp:
                crossover_period = periods[i]
                break

        latest_g = next((g for g in reversed(gold_share) if g is not None), None)
        latest_t = next((t for t in reversed(treasury_share) if t is not None), None)

        return {
            'periods': periods,
            'gold_share': gold_share,
            'treasury_share': treasury_share,
            'meta': {
                'title': 'Gold has overtaken US Treasurys in central-bank reserves',
                'source': 'World Gold Council · IMF · U.S. Treasury (FRED, foreign official) · gold price (Yahoo/GC=F)',
                'gold_price_source': gold_src,
                'treasury_source': ust_src,
                'gold_label': 'Gold',
                'treasury_label': 'US Treasurys (foreign-held)',
                'crossover_period': crossover_period,
                'latest_gold_share': latest_g,
                'latest_treasury_share': latest_t,
                'note': ('Each line is a share of total official reserves '
                         '(incl. gold). Crossover timing is denominator-'
                         'independent; levels are approximate (coverage '
                         'excludes IMF/BIS/ECB institutional gold).'),
            },
        }
    except Exception as e:
        logger.error('gold-treasury crossover build failed: %s', e)
        return _empty(f'{type(e).__name__}: {e}')


def get_gold_treasury_crossover():
    """Public API: cached crossover series."""
    return _cache.get()


def refresh_crossover():
    _cache.clear()
    return _cache.get()


def diagnose_crossover():
    """Force a rebuild and surface key checkpoints for verification."""
    data = _build_crossover()
    with _cache._lock:
        if data and not data.get('meta', {}).get('error'):
            _cache._data = data
            _cache._ts = time.time()
    periods = data.get('periods', [])
    gs = data.get('gold_share', [])
    ts = data.get('treasury_share', [])

    def at(period):
        if period in periods:
            i = periods.index(period)
            return {'gold': gs[i], 'treasury': ts[i]}
        return None

    samples = {}
    for p in ('2015-12', '2020-12', '2024-12', '2025-09', '2025-12',
              periods[-1] if periods else ''):
        if p:
            samples[p] = at(p)

    # Source-availability checkpoints so a failure is unambiguous.
    inputs = {}
    try:
        from backend.data_sources.fred_client import _get_api_key
        inputs['fred_key_detected'] = bool(_get_api_key())
    except Exception as e:
        inputs['fred_key_detected'] = f'check failed: {e}'
    try:
        gp, gsrc = _gold_price_by_month()
        inputs['gold_price_obs'] = len(gp)
        inputs['gold_price_source'] = gsrc
    except Exception as e:
        inputs['gold_price_obs'] = f'error: {e}'
    try:
        inputs['ust_official_obs'] = len(_fred_by_month(FRED_FOREIGN_OFFICIAL_UST))
        inputs['ust_total_obs'] = len(_fred_by_month(FRED_FOREIGN_UST))
    except Exception as e:
        inputs['ust_obs'] = f'error: {e}'
    # World-aggregate sanity at the latest period (should be ~33-36k t, not
    # a collapsed few-thousand from sparse right-edge coverage).
    try:
        per, wg, wt = _world_series_from_cofer()
        inputs['world_gold_tonnes_latest'] = round(wg[-1], 0) if wg and wg[-1] else None
        inputs['world_reserves_b_latest'] = round(wt[-1], 0) if wt and wt[-1] else None
    except Exception as e:
        inputs['world_aggregate'] = f'error: {e}'

    return {
        'meta': data.get('meta', {}),
        'period_range': f'{periods[0]} to {periods[-1]}' if periods else '',
        'point_count': len(periods),
        'inputs': inputs,
        'samples': samples,
        'today': _dt.date.today().isoformat(),
    }
