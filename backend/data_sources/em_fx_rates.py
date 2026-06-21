"""
EM FX & Rates tracker.

Builds a single payload for the EM FX & Rates dashboard:

  * FX monitor   — spot + 1d/1w/1m/YTD/1y moves for the major EM currencies
                   (vs USD), plus the DXY dollar index and the MSCI-EM equity
                   proxy (EEM). All from Yahoo Finance.
  * Rates        — the US Treasury curve (3M→30Y), EM bond-ETF proxies
                   (EMB hard-currency, EMLC local-currency) and per-country
                   10Y government bond yields from FRED (OECD), where available.
  * Drivers      — trailing beta/correlation of each EM currency to the dollar
                   (DXY) and to oil (Brent), the global risk/commodity backdrop
                   (VIX, Brent, gold), and an auto-generated narrative.

Everything except the FRED yields comes from yfinance with a single
``period='1y', interval='1d'`` history call per ticker, from which all the
windows, sparklines and correlations are derived. Thread-safe cache, 15-min
TTL — FX spot drifts intraday but a quarter-hour is plenty for a macro
dashboard and keeps us well clear of Yahoo's rate limits.
"""

import threading
import time
import logging
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

# Lazy import — yfinance and numpy are heavy.
yf = None
np = None


def _ensure_libs():
    global yf, np
    if yf is None:
        import yfinance as _yf
        yf = _yf
    if np is None:
        import numpy as _np
        np = _np


# ── Instrument universe ───────────────────────────────────────────────────

# EM currencies. `pair` is the Yahoo USD/CCY cross (a higher pair = a WEAKER
# local currency). `flag` is just a display nicety. `bloc` groups them for the
# UI. Order here is the display order.
CURRENCIES = [
    {'code': 'TRY', 'pair': 'USDTRY=X', 'name': 'Turkish Lira',      'flag': '🇹🇷', 'bloc': 'EMEA'},
    {'code': 'ZAR', 'pair': 'USDZAR=X', 'name': 'South African Rand', 'flag': '🇿🇦', 'bloc': 'EMEA'},
    {'code': 'PLN', 'pair': 'USDPLN=X', 'name': 'Polish Zloty',      'flag': '🇵🇱', 'bloc': 'EMEA'},
    {'code': 'BRL', 'pair': 'USDBRL=X', 'name': 'Brazilian Real',    'flag': '🇧🇷', 'bloc': 'LatAm'},
    {'code': 'MXN', 'pair': 'USDMXN=X', 'name': 'Mexican Peso',      'flag': '🇲🇽', 'bloc': 'LatAm'},
    {'code': 'COP', 'pair': 'USDCOP=X', 'name': 'Colombian Peso',    'flag': '🇨🇴', 'bloc': 'LatAm'},
    {'code': 'CLP', 'pair': 'USDCLP=X', 'name': 'Chilean Peso',      'flag': '🇨🇱', 'bloc': 'LatAm'},
    {'code': 'CNY', 'pair': 'USDCNY=X', 'name': 'Chinese Yuan',      'flag': '🇨🇳', 'bloc': 'Asia'},
    {'code': 'INR', 'pair': 'USDINR=X', 'name': 'Indian Rupee',      'flag': '🇮🇳', 'bloc': 'Asia'},
    {'code': 'IDR', 'pair': 'USDIDR=X', 'name': 'Indonesian Rupiah', 'flag': '🇮🇩', 'bloc': 'Asia'},
    {'code': 'KRW', 'pair': 'USDKRW=X', 'name': 'South Korean Won',  'flag': '🇰🇷', 'bloc': 'Asia'},
]

# Benchmarks shown alongside the FX grid.
DXY_TICKER = 'DX-Y.NYB'      # ICE US Dollar Index
EM_EQUITY_TICKER = 'EEM'     # iShares MSCI Emerging Markets ETF

# US Treasury curve (Yahoo yield indices are quoted in %, ×1 i.e. 4.25 = 4.25%).
US_CURVE = [
    {'tenor': '3M',  'sym': '^IRX'},
    {'tenor': '5Y',  'sym': '^FVX'},
    {'tenor': '10Y', 'sym': '^TNX'},
    {'tenor': '30Y', 'sym': '^TYX'},
]

# EM bond ETF proxies — a market-based read on EM rates (price up = yields down).
EM_BOND_ETFS = [
    {'code': 'EMB',  'sym': 'EMB',  'name': 'EM USD Sovereign (EMBI)', 'desc': 'iShares JPM USD EM Bond'},
    {'code': 'EMLC', 'sym': 'EMLC', 'name': 'EM Local-Currency Govt',  'desc': 'VanEck JPM EM Local Currency'},
]

# Global risk / commodity backdrop.
VIX_TICKER = '^VIX'
BRENT_TICKER = 'BZ=F'
GOLD_TICKER = 'GC=F'

# FRED OECD 10Y government bond yields, monthly (level in %). Coverage varies;
# any series that returns no rows is simply omitted from the rates table.
FRED_10Y = {
    'TRY': 'IRLTLT01TRM156N',
    'ZAR': 'IRLTLT01ZAM156N',
    'PLN': 'IRLTLT01PLM156N',
    'MXN': 'IRLTLT01MXM156N',
    'COP': 'IRLTLT01COM156N',
    'CLP': 'IRLTLT01CLM156N',
    'IDR': 'IRLTLT01IDM156N',
    'KRW': 'IRLTLT01KRM156N',
    'INR': 'IRLTLT01INM156N',
}

CACHE_TTL = 900  # 15 minutes
CORR_WINDOW = 90  # trailing observations for beta/correlation


# ── Thread-safe cache ─────────────────────────────────────────────────────

class _Cache:
    def __init__(self):
        self._lock = threading.RLock()
        self._data = None
        self._ts = 0.0

    def get(self):
        with self._lock:
            if self._data and (time.time() - self._ts) < CACHE_TTL:
                return self._data
        data = _build_payload()
        with self._lock:
            # Only overwrite a good payload — if a transient fetch failure
            # produced an empty grid, keep serving the last good one.
            if data.get('fx') or self._data is None:
                self._data = data
                self._ts = time.time()
            return self._data

    def clear(self):
        with self._lock:
            self._data = None
            self._ts = 0.0


_cache = _Cache()


# ── yfinance helpers ──────────────────────────────────────────────────────

def _history(ticker):
    """Return a list of {'date': date, 'value': float} daily closes for ~1y,
    sorted ascending. Empty list on any failure."""
    _ensure_libs()
    try:
        hist = yf.Ticker(ticker).history(period='1y', interval='1d')
        if hist is None or hist.empty:
            return []
        out = []
        for idx, val in zip(hist.index, hist['Close'].tolist()):
            if val is None or val != val:  # skip NaN
                continue
            out.append({'date': idx.date(), 'value': float(val)})
        return out
    except Exception as e:
        logger.warning(f"EM-FX history fetch failed for {ticker}: {e}")
        return []


def _value_on_or_before(series, target):
    """Last close on or before `target` date. None if series starts after it."""
    chosen = None
    for pt in series:
        if pt['date'] <= target:
            chosen = pt['value']
        else:
            break
    return chosen


def _pct(now, then):
    if now is None or then is None or then == 0:
        return None
    return round((now / then - 1.0) * 100.0, 2)


def _windows_for_pair(series):
    """Compute currency-vs-USD % moves over standard windows from a USD/CCY
    pair series. A pair *fall* = currency *appreciation*, so we invert: the
    currency return is (then_pair / now_pair − 1)."""
    if not series:
        return {}, None, None
    last = series[-1]
    now = last['value']
    today = last['date']

    def ccy_move(days=None, ytd=False):
        if ytd:
            target = date(today.year - 1, 12, 31)
        else:
            target = today - timedelta(days=days)
        then = _value_on_or_before(series, target)
        if then is None or now == 0:
            return None
        # invert pair move → currency-vs-USD move
        return round((then / now - 1.0) * 100.0, 2)

    moves = {
        'd1':  ccy_move(1),
        'w1':  ccy_move(7),
        'm1':  ccy_move(30),
        'm3':  ccy_move(91),
        'ytd': ccy_move(ytd=True),
        'y1':  ccy_move(365),
    }
    return moves, now, today.isoformat()


def _windows_for_level(series, bp=False):
    """% (or basis-point, if bp=True) moves for a plain level series
    (DXY, ETF price, yields). For yields bp=True returns the raw level change
    ×100 in basis points."""
    if not series:
        return {}, None, None
    last = series[-1]
    now = last['value']
    today = last['date']

    def move(days=None, ytd=False):
        target = date(today.year - 1, 12, 31) if ytd else today - timedelta(days=days)
        then = _value_on_or_before(series, target)
        if then is None:
            return None
        if bp:
            return round((now - then) * 100.0, 0)  # %-pts → bp
        return _pct(now, then)

    moves = {
        'd1':  move(1),
        'w1':  move(7),
        'm1':  move(30),
        'm3':  move(91),
        'ytd': move(ytd=True),
        'y1':  move(365),
    }
    return moves, now, today.isoformat()


def _sparkline_ccy(series, points=120):
    """Currency-strength sparkline normalized to 100 at the window start, so a
    rising line = an appreciating currency (pair inverted)."""
    if not series:
        return []
    slice_ = series[-points:]
    base = slice_[0]['value']
    if not base:
        return []
    # currency strength index = 100 * base_pair / pair  (pair down → index up)
    return [round(100.0 * base / p['value'], 2) for p in slice_ if p['value']]


def _sparkline_level(series, points=120):
    if not series:
        return []
    slice_ = series[-points:]
    return [round(p['value'], 4) for p in slice_]


def _daily_returns(series):
    """List of (date, simple_return) from a level series."""
    out = []
    for i in range(1, len(series)):
        prev = series[i - 1]['value']
        cur = series[i]['value']
        if prev:
            out.append((series[i]['date'], cur / prev - 1.0))
    return out


def _align(a, b):
    """Align two (date, value) lists on common dates → (arr_a, arr_b)."""
    bd = {d: v for d, v in b}
    xs, ys = [], []
    for d, v in a:
        if d in bd:
            xs.append(v)
            ys.append(bd[d])
    return xs, ys


def _fit_factor(ccy_returns, factor_returns):
    """Regress currency returns on a factor's returns over the trailing window.
    Returns (beta, corr, r2) rounded, or (None, None, None)."""
    _ensure_libs()
    xs, ys = _align(ccy_returns, factor_returns)  # xs=ccy, ys=factor
    if len(xs) < 20:
        return None, None, None
    x = np.array(xs[-CORR_WINDOW:])
    y = np.array(ys[-CORR_WINDOW:])
    if x.std() == 0 or y.std() == 0:
        return None, None, None
    try:
        beta = float(np.polyfit(y, x, 1)[0])      # ccy = a + beta*factor
        corr = float(np.corrcoef(x, y)[0, 1])
        return round(beta, 2), round(corr, 2), round(corr * corr, 2)
    except Exception:
        return None, None, None


def _beta_corr(ccy_returns, factor_returns):
    beta, corr, _ = _fit_factor(ccy_returns, factor_returns)
    return beta, corr


def _ann_vol_pct(ccy_returns, n=CORR_WINDOW):
    """Annualized FX volatility (%), from the trailing `n` daily returns."""
    _ensure_libs()
    r = [v for _, v in ccy_returns[-n:]]
    if len(r) < 15:
        return None
    arr = np.array(r)
    return round(float(arr.std() * (252 ** 0.5) * 100.0), 1)


def _skew_kurt(ccy_returns):
    """Sample skewness and excess kurtosis of daily returns. Negative skew +
    fat tails = the carry-crash signature (you're implicitly short gamma)."""
    _ensure_libs()
    r = np.array([v for _, v in ccy_returns])
    if r.size < 30:
        return None, None
    mu, sd = r.mean(), r.std()
    if sd == 0:
        return None, None
    z = (r - mu) / sd
    skew = float((z ** 3).mean())
    exkurt = float((z ** 4).mean() - 3.0)
    return round(skew, 2), round(exkurt, 1)


def _yield_changes(level_series):
    """Daily first-difference of a yield level series → (date, Δ in %-points)."""
    out = []
    for i in range(1, len(level_series)):
        out.append((level_series[i]['date'],
                    level_series[i]['value'] - level_series[i - 1]['value']))
    return out


def _empirical_duration(etf_returns, dyield):
    """Effective duration (years) of a bond ETF, from regressing its daily
    returns on daily US-10Y yield changes. price_return ≈ −Dur · Δy(decimal),
    so with Δy in %-points: Dur = −beta · 100. Returns (duration, r2)."""
    _ensure_libs()
    xs, ys = _align(etf_returns, dyield)  # xs=etf ret, ys=Δyield(%pts)
    if len(xs) < 30:
        return None, None
    x = np.array(xs)
    y = np.array(ys)
    if y.std() == 0:
        return None, None
    try:
        beta = float(np.polyfit(y, x, 1)[0])
        corr = float(np.corrcoef(x, y)[0, 1])
        dur = round(-beta * 100.0, 1)
        return dur, round(corr * corr, 2)
    except Exception:
        return None, None


# ── Payload assembly ──────────────────────────────────────────────────────

def _build_payload():
    _ensure_libs()

    # Fetch every ticker's 1y daily history once.
    fx_series = {c['code']: _history(c['pair']) for c in CURRENCIES}
    dxy_series = _history(DXY_TICKER)
    eem_series = _history(EM_EQUITY_TICKER)
    brent_series = _history(BRENT_TICKER)
    vix_series = _history(VIX_TICKER)
    gold_series = _history(GOLD_TICKER)
    curve_series = {r['sym']: _history(r['sym']) for r in US_CURVE}
    etf_series = {e['sym']: _history(e['sym']) for e in EM_BOND_ETFS}

    # Factor returns for the drivers section.
    dxy_ret = _daily_returns(dxy_series)
    brent_ret = _daily_returns(brent_series)

    # ── FX grid ──
    fx = []
    for c in CURRENCIES:
        series = fx_series[c['code']]
        moves, spot, asof = _windows_for_pair(series)
        # currency daily returns = inverse of pair returns
        pair_ret = _daily_returns(series)
        ccy_ret = [(d, -r) for d, r in pair_ret]
        dxy_beta, dxy_corr, dxy_r2 = _fit_factor(ccy_ret, dxy_ret)
        oil_beta, oil_corr = _beta_corr(ccy_ret, brent_ret)

        # Convexity (gamma) proxies from realized returns — no options data, so
        # these are realized, not implied: a vol-compression ratio (coiled vs
        # expanding) and the skew/kurtosis carry-crash signature.
        vol_1m = _ann_vol_pct(ccy_ret, 21)
        vol_3m = _ann_vol_pct(ccy_ret, 63)
        vol_1y = _ann_vol_pct(ccy_ret, 252)
        compression = round(vol_1m / vol_1y, 2) if (vol_1m and vol_1y) else None
        skew, exkurt = _skew_kurt(ccy_ret)

        fx.append({
            'code': c['code'], 'name': c['name'], 'flag': c['flag'],
            'bloc': c['bloc'], 'pair': c['pair'],
            'spot': round(spot, 4) if spot is not None else None,
            'asof': asof,
            'chg': moves,
            'ann_vol_pct': _ann_vol_pct(ccy_ret),
            'spark': _sparkline_ccy(series),
            'drivers': {
                'dxy_beta': dxy_beta, 'dxy_corr': dxy_corr, 'dxy_r2': dxy_r2,
                'oil_beta': oil_beta, 'oil_corr': oil_corr,
            },
            'convexity': {
                'vol_1m': vol_1m, 'vol_3m': vol_3m, 'vol_1y': vol_1y,
                'compression': compression, 'skew': skew, 'exkurt': exkurt,
            },
        })

    # ── Benchmarks ──
    dxy_moves, dxy_now, dxy_asof = _windows_for_level(dxy_series)
    eem_moves, eem_now, eem_asof = _windows_for_level(eem_series)
    benchmarks = {
        'dxy': {'name': 'US Dollar Index (DXY)', 'level': round(dxy_now, 2) if dxy_now else None,
                'asof': dxy_asof, 'chg': dxy_moves, 'spark': _sparkline_level(dxy_series)},
        'em_equity': {'name': 'MSCI EM Equity (EEM)', 'level': round(eem_now, 2) if eem_now else None,
                      'asof': eem_asof, 'chg': eem_moves, 'spark': _sparkline_level(eem_series)},
    }

    # ── Rates: US curve ──
    us_curve = []
    for r in US_CURVE:
        s = curve_series[r['sym']]
        moves, level, asof = _windows_for_level(s, bp=True)
        us_curve.append({
            'tenor': r['tenor'], 'sym': r['sym'],
            'yield': round(level, 2) if level is not None else None,
            'asof': asof,
            'chg_bp': moves,   # basis-point changes
            'spark': _sparkline_level(s),
        })
    # 10Y–3M slope, if both legs present.
    slope = None
    y10 = next((c['yield'] for c in us_curve if c['tenor'] == '10Y'), None)
    y3m = next((c['yield'] for c in us_curve if c['tenor'] == '3M'), None)
    if y10 is not None and y3m is not None:
        slope = round(y10 - y3m, 2)

    # ── Rates: EM bond ETFs ──
    em_bond_etfs = []
    for e in EM_BOND_ETFS:
        s = etf_series[e['sym']]
        moves, level, asof = _windows_for_level(s)
        em_bond_etfs.append({
            'code': e['code'], 'name': e['name'], 'desc': e['desc'],
            'price': round(level, 2) if level is not None else None,
            'asof': asof, 'chg': moves, 'spark': _sparkline_level(s),
        })

    # Empirical effective duration of the EM bond proxies, from regressing
    # their daily returns on daily US-10Y yield changes.
    us10_dy = _yield_changes(curve_series.get('^TNX', []))
    for e in em_bond_etfs:
        ret = _daily_returns(etf_series.get(e['code'], []))
        dur, r2 = _empirical_duration(ret, us10_dy)
        e['duration'] = dur
        e['duration_r2'] = r2

    # ── Rates: per-country FRED 10Y yields ──
    em_10y, has_fred = _fred_10y_table()

    # ── Drivers / backdrop ──
    vix_moves, vix_now, _ = _windows_for_level(vix_series)
    brent_moves, brent_now, _ = _windows_for_level(brent_series)
    gold_moves, gold_now, _ = _windows_for_level(gold_series)
    backdrop = {
        'vix':   {'name': 'VIX', 'level': round(vix_now, 2) if vix_now else None, 'chg': vix_moves},
        'brent': {'name': 'Brent Crude', 'level': round(brent_now, 2) if brent_now else None, 'chg': brent_moves},
        'gold':  {'name': 'Gold', 'level': round(gold_now, 2) if gold_now else None, 'chg': gold_moves},
        'narrative': _build_narrative(fx, benchmarks, backdrop_levels={
            'dxy': dxy_moves, 'vix': vix_now, 'vix_chg': vix_moves, 'brent': brent_moves,
        }),
    }

    # ── Opportunity engines ──
    signals = _build_signals(fx, y10, em_10y, dxy_moves)            # FX beta/carry/value
    duration = _build_duration(em_10y, em_bond_etfs, has_fred)      # rates beta
    convexity = _build_convexity(fx)                                # gamma proxies

    return {
        'meta': {
            'updated_at': datetime.utcnow().isoformat() + 'Z',
            'source': 'Yahoo Finance (FX, DXY, curve, ETFs) · FRED/OECD (EM 10Y yields)',
            'corr_window_days': CORR_WINDOW,
            'has_fred': has_fred,
        },
        'fx': fx,
        'benchmarks': benchmarks,
        'rates': {
            'us_curve': us_curve,
            'us_slope_10y_3m': slope,
            'em_bond_etfs': em_bond_etfs,
            'em_10y': em_10y,
        },
        'signals': signals,
        'duration': duration,
        'convexity': convexity,
        'backdrop': backdrop,
    }


def _build_duration(em_10y_rows, em_bond_etfs, has_fred):
    """Duration (rates-beta) opportunities. For each country with a FRED 10Y
    yield we combine the *level* (carry cushion) with the recent *trend* in
    yields: falling yields = capital gains for a long-duration (receiver)
    position, on top of the carry. Ranked by a simple high-yield + falling-
    yield score. The ETF block reports the empirical effective duration of the
    EM bond proxies — the actual price sensitivity to a 100bp rate move."""
    rows = []
    for r in (em_10y_rows or []):
        level = r.get('yield')
        d3 = r.get('chg_3m_bp')
        d12 = r.get('chg_12m_bp')
        # Stance from the 3m trend in yields.
        if d3 is not None and d3 <= -15:
            stance = 'Long duration (receiver)'
            note = f'Yields down {abs(int(d3))}bp/3m with a {level:.1f}% carry cushion — falling rates add capital gains to carry.'
        elif d3 is not None and d3 >= 25:
            stance = 'Pay / short duration'
            note = f'Yields up {int(d3)}bp/3m — rising-rate drag; better received later or paid now.'
        elif level is not None and level >= 8:
            stance = 'High carry, range-bound'
            note = f'{level:.1f}% yield with little trend — carry without a clear duration tailwind.'
        else:
            stance = 'Neutral'
            note = 'No standout level or trend in the 10Y right now.'
        rows.append({
            'code': r.get('code'), 'name': r.get('name'),
            'yield': level, 'chg_3m_bp': d3, 'chg_12m_bp': d12,
            'asof': r.get('asof'), 'stance': stance, 'note': note,
        })

    # Score: high level + falling yields (negative 3m change is good).
    lvl_z = _zscores([r['yield'] for r in rows])
    trd_z = _zscores([(-(r['chg_3m_bp']) if r['chg_3m_bp'] is not None else None) for r in rows])
    for i, r in enumerate(rows):
        parts = [z for z in (lvl_z[i], trd_z[i]) if z is not None]
        r['score'] = round(sum(parts) / len(parts), 2) if parts else None
    rows.sort(key=lambda r: (r['score'] if r['score'] is not None else -1e9), reverse=True)
    for i, r in enumerate(rows, 1):
        r['rank'] = i

    return {
        'rows': rows,
        'etfs': [{'code': e['code'], 'name': e['name'], 'duration': e.get('duration'),
                  'r2': e.get('duration_r2')} for e in (em_bond_etfs or [])],
        'has_fred': has_fred,
        'method': (
            'Effective duration = −β·100 from regressing each EM bond proxy\'s daily '
            'return on daily US-10Y yield changes (price move per 100bp). Country stance '
            'reads the 3-month trend in the FRED/OECD 10Y: falling yields favour receiving '
            '(long duration), rising yields favour paying.'
        ),
    }


def _build_convexity(fx):
    """Convexity (gamma) opportunities from *realized* return distributions —
    we have no options data, so this is realized, not implied vol/greeks.

      • Vol compression = 1m realized vol ÷ 1y realized vol. Well below 1 = a
        coiled, low-vol regime → a long-gamma / breakout setup (cheap optionality
        if you could buy it). Well above 1 = vol already expanding.
      • Skew + excess kurtosis = the carry-crash signature. Strongly negative
        skew with fat tails means small steady gains punctuated by large drops —
        you are implicitly SHORT gamma holding the carry (e.g. managed pegs).
    """
    rows = []
    for f in fx:
        cx = f.get('convexity', {})
        comp = cx.get('compression')
        skew = cx.get('skew')
        exk = cx.get('exkurt')

        if skew is not None and skew <= -0.6 and (exk is None or exk >= 1):
            label = 'Short gamma — negative convexity'
            note = f'Skew {skew:+.2f}, fat tails — carry-crash risk; you are implicitly short vol holding this.'
        elif comp is not None and comp <= 0.75:
            label = 'Long gamma — coiled (vol compressed)'
            note = f'1m vol is {comp:.2f}× its 1y average — quiet regime, breakout/optionality setup.'
        elif comp is not None and comp >= 1.3:
            label = 'Vol expanding'
            note = f'1m vol is {comp:.2f}× its 1y average — already moving; momentum over mean-reversion.'
        else:
            label = 'Balanced'
            note = 'No vol-compression or tail-skew dislocation right now.'

        rows.append({
            'code': f['code'], 'name': f['name'], 'flag': f['flag'], 'bloc': f['bloc'],
            'vol_1m': cx.get('vol_1m'), 'vol_1y': cx.get('vol_1y'),
            'compression': comp, 'skew': skew, 'exkurt': exk,
            'label': label, 'note': note,
        })

    # Most "interesting" first: lowest compression (most coiled) and most
    # negative skew bubble up; balanced names sink.
    def sort_key(r):
        priority = 0
        if r['label'].startswith('Short gamma'):
            priority = 3
        elif r['label'].startswith('Long gamma'):
            priority = 2
        elif r['label'].startswith('Vol expanding'):
            priority = 1
        return (priority, -(r['compression'] or 99))
    rows.sort(key=sort_key, reverse=True)

    return {
        'rows': rows,
        'method': (
            'Realized (not implied) convexity proxies. Vol compression = 1m ÷ 1y realized '
            'vol: <0.75 = coiled long-gamma setup, >1.3 = expanding. Skew/kurtosis flag the '
            'carry-crash tail (short-gamma). True option greeks need an implied-vol surface, '
            'which is paid data.'
        ),
    }


def _zscores(values):
    """Cross-sectional z-scores for a list (None preserved). Returns dict
    index→z."""
    _ensure_libs()
    idx = [i for i, v in enumerate(values) if v is not None]
    if len(idx) < 3:
        return {i: None for i in range(len(values))}
    arr = np.array([values[i] for i in idx], dtype=float)
    mu, sd = arr.mean(), arr.std()
    out = {i: None for i in range(len(values))}
    if sd == 0:
        return out
    for i in idx:
        out[i] = round(float((values[i] - mu) / sd), 2)
    return out


def _build_signals(fx, us_10y, em_10y_rows, dxy_moves):
    """Turn the raw FX/rates trends into a ranked, non-visual opportunity
    screen. For each currency we decompose the recent move into:

      • Dollar beta + idiosyncratic alpha — predict the 1m move from DXY using
        the fitted beta; the residual (actual − predicted) is the part the
        dollar does NOT explain. Big negative residual = the currency lagged
        what the dollar alone implies → mean-reversion long candidate.
      • Carry — local 10Y minus US 10Y (the excess yield you earn holding it).
      • Carry-to-vol — carry divided by annualized FX vol, the classic
        risk-adjusted carry-trade attractiveness metric.
      • Momentum — trailing 3-month trend vs USD.

    A transparent composite (cross-sectional z of carry-to-vol + momentum)
    ranks the cross-section; a rules-based label explains each name.
    """
    dxy_m1 = (dxy_moves or {}).get('m1')
    yld = {r['code']: r['yield'] for r in (em_10y_rows or [])}

    rows = []
    for c in fx:
        d = c.get('drivers', {})
        ch = c.get('chg', {})
        beta = d.get('dxy_beta')
        em10 = yld.get(c['code'])
        vol = c.get('ann_vol_pct')
        m1 = ch.get('m1')
        m3 = ch.get('m3')

        carry_bp = round((em10 - us_10y) * 100.0, 0) if (em10 is not None and us_10y is not None) else None
        carry_pct = (carry_bp / 100.0) if carry_bp is not None else None
        # carry-to-vol: excess yield (%) per unit of annualized FX vol (%).
        carry_to_vol = round(carry_pct / vol, 2) if (carry_pct is not None and vol) else None

        # Dollar-adjusted residual over the past month.
        resid = None
        predicted = None
        if beta is not None and m1 is not None and dxy_m1 is not None:
            predicted = round(beta * dxy_m1, 2)
            resid = round(m1 - predicted, 2)

        rows.append({
            'code': c['code'], 'name': c['name'], 'flag': c['flag'], 'bloc': c['bloc'],
            'beta_dxy': beta, 'r2': d.get('dxy_r2'),
            'ann_vol_pct': vol,
            'carry_bp': carry_bp,
            'carry_to_vol': carry_to_vol,
            'mom_3m': m3, 'mom_1m': m1,
            'dxy_implied_1m': predicted,
            'residual_1m': resid,
            'oil_corr': d.get('oil_corr'),
        })

    # Cross-sectional composite: risk-adjusted carry + momentum.
    cv_z = _zscores([r['carry_to_vol'] for r in rows])
    mom_z = _zscores([r['mom_3m'] for r in rows])
    res_vals = [r['residual_1m'] for r in rows]
    res_z = _zscores(res_vals)

    for i, r in enumerate(rows):
        parts = [z for z in (cv_z[i], mom_z[i]) if z is not None]
        r['score'] = round(sum(parts) / len(parts), 2) if parts else None
        r['_cv_z'] = cv_z[i]
        r['_mom_z'] = mom_z[i]
        r['_res_z'] = res_z[i]

    # Rules-based labels. Thresholds are deliberately simple/transparent.
    for r in rows:
        label, rationale = _signal_label(r, dxy_m1)
        r['signal'] = label
        r['rationale'] = rationale
        # strip internal z helpers from the payload
        for k in ('_cv_z', '_mom_z', '_res_z'):
            r.pop(k, None)

    # Rank by composite (None last), then by carry-to-vol.
    rows.sort(key=lambda r: (
        r['score'] if r['score'] is not None else -1e9,
        r['carry_to_vol'] if r['carry_to_vol'] is not None else -1e9,
    ), reverse=True)
    for i, r in enumerate(rows, 1):
        r['rank'] = i

    return {
        'us_10y': us_10y,
        'rows': rows,
        'method': (
            'Composite = cross-sectional z(carry-to-vol) + z(3m momentum). '
            'Residual_1m = actual 1m move − (DXY beta × DXY 1m move): the part '
            'of the move the dollar does not explain. Carry = local 10Y − US 10Y — '
            'the forward/NDF-implied carry under covered interest parity (we proxy it '
            'with the rate differential because forward points and the cross-currency '
            'basis are paid data).'
        ),
    }


def _signal_label(r, dxy_m1):
    """Map a signal row to a (label, rationale). Order = priority."""
    cv = r.get('carry_to_vol')
    mom = r.get('mom_3m')
    resid = r.get('residual_1m')
    beta = r.get('beta_dxy')
    carry_bp = r.get('carry_bp')

    # 1) Carry + momentum aligned — the cleanest long.
    if cv is not None and mom is not None and cv >= 0.3 and mom > 0:
        return ('Carry + momentum (long)',
                f'Risk-adjusted carry {cv:.2f} with +{mom:.1f}% 3m trend — both factors point the same way.')

    # 2) Idiosyncratic weakness vs the dollar → mean-reversion long.
    if resid is not None and resid <= -2.5 and (carry_bp is None or carry_bp > 0):
        carry_txt = f', carry {int(carry_bp)}bp' if carry_bp is not None else ''
        return ('Value vs USD-beta (mean-reversion long)',
                f'Lagged its dollar beta by {resid:.1f}% over 1m{carry_txt} — cheap vs what the dollar explains.')

    # 3) Idiosyncratic strength → rich / fade.
    if resid is not None and resid >= 2.5:
        return ('Idiosyncratic strength (rich)',
                f'Outran its dollar beta by +{resid:.1f}% over 1m — richer than the dollar alone justifies.')

    # 4) High dollar-beta with the dollar rolling over → beta long.
    if beta is not None and abs(beta) >= 0.6 and dxy_m1 is not None and dxy_m1 < -0.3:
        return ('High dollar-beta (long on USD rollover)',
                f'Beta {beta:.2f} to DXY and the dollar is down {abs(dxy_m1):.1f}% over 1m — high-beta upside.')

    # 5) High beta with the dollar rising → vulnerable.
    if beta is not None and abs(beta) >= 0.6 and dxy_m1 is not None and dxy_m1 > 0.3:
        return ('High dollar-beta (vulnerable to USD)',
                f'Beta {beta:.2f} to DXY and the dollar is up {dxy_m1:.1f}% over 1m — high-beta downside.')

    return ('Neutral', 'No standout carry, momentum or dollar-beta dislocation right now.')


def _fred_10y_table():
    """Per-country 10Y govt yield from FRED OECD. Returns (rows, has_fred).
    `has_fred` is False when the key is missing, so the UI can explain the
    empty column rather than implying the data simply doesn't exist."""
    from backend.data_sources.fred_client import fetch_series, _get_api_key
    has_fred = bool(_get_api_key())
    rows = []
    if not has_fred:
        return rows, False
    name_by_code = {c['code']: c['name'] for c in CURRENCIES}
    for code, series_id in FRED_10Y.items():
        data = fetch_series(series_id)
        if not data:
            continue
        latest = data[-1]
        level = latest['value']
        latest_dt = latest['date']

        def yield_n_months_ago(n):
            from datetime import datetime as _dt
            cutoff = _dt.strptime(latest_dt, '%Y-%m-%d')
            target = (cutoff.year * 12 + cutoff.month - 1) - n
            ty, tm = divmod(target, 12)
            tag = f"{ty:04d}-{tm + 1:02d}"
            best = None
            for pt in data:
                if pt['date'][:7] <= tag:
                    best = pt['value']
                else:
                    break
            return best

        m3 = yield_n_months_ago(3)
        m12 = yield_n_months_ago(12)
        rows.append({
            'code': code, 'name': name_by_code.get(code, code),
            'yield': round(level, 2),
            'asof': latest_dt,
            'chg_3m_bp': round((level - m3) * 100, 0) if m3 is not None else None,
            'chg_12m_bp': round((level - m12) * 100, 0) if m12 is not None else None,
        })
    rows.sort(key=lambda r: r['yield'], reverse=True)
    return rows, True


def _build_narrative(fx, benchmarks, backdrop_levels):
    """A short, data-driven read of what's moving EM FX right now: dollar
    direction, risk sentiment, and which currencies are most dollar- vs
    oil-sensitive. Plain heuristics over the computed figures — no opinions."""
    parts = []

    # Dollar direction (1m DXY).
    dxy_m1 = (backdrop_levels.get('dxy') or {}).get('m1')
    if dxy_m1 is not None:
        if dxy_m1 >= 1.0:
            parts.append(f"The dollar (DXY) is up {dxy_m1:.1f}% over the past month — a headwind for EM FX broadly.")
        elif dxy_m1 <= -1.0:
            parts.append(f"The dollar (DXY) is down {abs(dxy_m1):.1f}% over the past month — a tailwind for EM FX broadly.")
        else:
            parts.append(f"The dollar (DXY) is roughly flat over the past month ({dxy_m1:+.1f}%), leaving EM FX driven by local stories.")

    # Risk sentiment via VIX.
    vix = backdrop_levels.get('vix')
    if vix is not None:
        if vix >= 25:
            parts.append(f"Volatility is elevated (VIX {vix:.0f}), which typically pressures high-beta EM currencies.")
        elif vix <= 15:
            parts.append(f"Volatility is subdued (VIX {vix:.0f}), supportive of carry into EM.")
        else:
            parts.append(f"Volatility is moderate (VIX {vix:.0f}).")

    # Best / worst EM currency over 1m.
    ranked = [c for c in fx if (c['chg'] or {}).get('m1') is not None]
    if ranked:
        best = max(ranked, key=lambda c: c['chg']['m1'])
        worst = min(ranked, key=lambda c: c['chg']['m1'])
        parts.append(
            f"Over the past month the {best['name']} leads ({best['chg']['m1']:+.1f}% vs USD) "
            f"while the {worst['name']} lags ({worst['chg']['m1']:+.1f}%)."
        )

    # Most dollar-sensitive vs most oil-sensitive.
    with_dxy = [c for c in fx if c['drivers'].get('dxy_corr') is not None]
    if with_dxy:
        most_usd = max(with_dxy, key=lambda c: abs(c['drivers']['dxy_corr']))
        parts.append(
            f"{most_usd['name']} is currently the most dollar-sensitive "
            f"(corr {most_usd['drivers']['dxy_corr']:+.2f} to DXY)."
        )
    with_oil = [c for c in fx if c['drivers'].get('oil_corr') is not None]
    if with_oil:
        most_oil = max(with_oil, key=lambda c: c['drivers']['oil_corr'])
        if abs(most_oil['drivers']['oil_corr']) >= 0.2:
            parts.append(
                f"{most_oil['name']} is the most oil-linked "
                f"(corr {most_oil['drivers']['oil_corr']:+.2f} to Brent)."
            )

    return ' '.join(parts)


# ── Public API ────────────────────────────────────────────────────────────

def get_em_fx_rates():
    """Cached EM FX & rates payload."""
    return _cache.get()


def clear_cache():
    _cache.clear()
    try:
        from backend.data_sources.fred_client import clear_cache as clear_fred
        clear_fred()
    except Exception:
        pass
