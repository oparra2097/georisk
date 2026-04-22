"""
Hybrid statistical model stack for commodity price forecasting.

Methodology
-----------
For each commodity we fit a two-stage hybrid:

  1. SARIMAX(1,0,1) on monthly log-returns of the commodity price, with
     exogenous regressors capturing that commodity's primary macro drivers
     (e.g. DXY and 10Y real yield for gold; oil for TTF gas; gold for silver).

  2. GARCH(1,1) on the SARIMAX residuals, to capture volatility clustering.
     This makes the posterior confidence band widen automatically during
     turbulent regimes (2022 gas crisis, 2024-26 cocoa spike).

Forecast generation
-------------------
We simulate 1000 future 12-month return paths. Exogenous drivers are held
at their last observed monthly level (a naive random walk on levels). For
each simulation we draw innovations from the fitted GARCH conditional
distribution, push them through the SARIMAX state to get log-returns,
compound to prices, and aggregate into four forward quarterly averages.

Outputs per commodity are {Q+1, Q+2, Q+3, Q+4} with median (p50), lower
95% (p2.5), upper 95% (p97.5). These map to the Base / Worst / Best case
scenario rows in the existing forecast API.

Nowcast
-------
Current-quarter estimate blends QTD actuals with the model's Q+0 median:
    nowcast = w * qtd_mean + (1 - w) * model_q0_median
    w = days_elapsed / days_in_quarter

Refit cadence
-------------
Fits are cached to disk and considered stale after 35 days. The scheduler
in backend/scheduler.py triggers monthly refits.
"""

from __future__ import annotations

import os
import json
import pickle
import logging
import calendar
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    from arch import arch_model
    _STATS_OK = True
except ImportError:
    _STATS_OK = False

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────

HISTORY_YEARS = 10
BOOTSTRAP_DRAWS = 1000
FORECAST_MONTHS = 12       # 4 quarters × 3 months
STALE_AFTER_DAYS = 35

CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'cache',
    'commodity_models',
)

# yfinance tickers matching backend/data_sources/commodities_forecast.py
TICKERS = {
    'WTI Crude':        'CL=F',
    'Brent Crude':      'BZ=F',
    'Natural Gas (HH)': 'NG=F',
    'TTF Gas':          'TTF=F',
    'Cocoa':            'CC=F',
    'Wheat':            'ZW=F',
    'Soybeans':         'ZS=F',
    'Coffee':           'KC=F',
    'Copper':           'HG=F',
    'Gold':             'GC=F',
    'Silver':           'SI=F',
    'Platinum':         'PL=F',
    'Aluminum':         'ALI=F',
}

# Per-commodity driver configuration. Each driver is one of:
#   ('fred',   series_id)             FRED time series (monthly)
#   ('yf',     ticker)                yfinance close (any freq, resampled)
#   ('gpr',    None)                  Geopolitical Risk Index
#   ('comm',   commodity_name)        another commodity's price
#
# Proxies are used where a true driver has no reliable free API:
#   - OPEC spare capacity → oil price volatility (handled via GARCH)
#   - US crude inventory → absent; rely on SARIMAX AR component
#   - West Africa rainfall → seasonal dummy inside SARIMAX (period=12)
#   - China PMI → copper price itself as leading indicator
#   - LME stocks → absent; rely on AR component
#
DRIVERS: dict[str, list[tuple[str, str]]] = {
    'WTI Crude':        [('fred', 'DTWEXBGS'), ('gpr', ''), ('yf', '^GSPC')],
    'Brent Crude':      [('fred', 'DTWEXBGS'), ('gpr', ''), ('comm', 'WTI Crude')],
    'Natural Gas (HH)': [('fred', 'DTWEXBGS'), ('comm', 'WTI Crude'), ('yf', '^GSPC')],
    'TTF Gas':          [('fred', 'DTWEXBGS'), ('comm', 'Natural Gas (HH)'), ('gpr', '')],
    'Gold':             [('fred', 'DFII10'), ('fred', 'DTWEXBGS'), ('gpr', ''), ('yf', '^GSPC')],
    'Silver':           [('fred', 'DFII10'), ('fred', 'DTWEXBGS'), ('comm', 'Gold'), ('comm', 'Copper')],
    'Platinum':         [('fred', 'DTWEXBGS'), ('comm', 'Gold'), ('fred', 'DFII10')],
    'Copper':           [('fred', 'DTWEXBGS'), ('yf', '^GSPC'), ('fred', 'DFII10')],
    'Aluminum':         [('fred', 'DTWEXBGS'), ('comm', 'Copper'), ('comm', 'Natural Gas (HH)')],
    'Cocoa':            [('fred', 'DTWEXBGS'), ('yf', '^GSPC')],
    'Wheat':            [('fred', 'DTWEXBGS'), ('gpr', ''), ('comm', 'WTI Crude')],
    'Soybeans':         [('fred', 'DTWEXBGS'), ('comm', 'Wheat'), ('yf', '^GSPC')],
    'Coffee':           [('fred', 'DTWEXBGS'), ('yf', '^GSPC')],
}

# How each driver enters the SARIMAX design matrix
DRIVER_TRANSFORM = {
    'fred_DTWEXBGS': 'logret',   # dollar index — returns
    'fred_DFII10':   'diff',     # real yield level — first difference
    'fred_NAPM':     'diff',     # ISM — first difference
    'gpr_':          'loglevel', # GPR index — log of level
    'yf_^GSPC':      'logret',   # equities — returns
    'comm_':         'logret',   # other commodity price — returns
}


# ── Driver fetchers ────────────────────────────────────────────────────────

class DriverFetcher:
    """Pulls monthly driver series, caches in-memory for a build."""

    def __init__(self):
        self._cache: dict[str, pd.Series] = {}

    def fetch(self, kind: str, key: str, start: date) -> Optional[pd.Series]:
        cache_key = f'{kind}:{key}'
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            if kind == 'fred':
                series = self._fetch_fred(key, start)
            elif kind == 'yf':
                series = self._fetch_yf(key, start)
            elif kind == 'gpr':
                series = self._fetch_gpr(start)
            elif kind == 'comm':
                series = self._fetch_yf(TICKERS[key], start)
            else:
                logger.warning(f'Unknown driver kind: {kind}')
                return None
        except Exception as e:
            logger.warning(f'Driver fetch {kind}:{key} failed: {e}')
            series = None

        self._cache[cache_key] = series
        return series

    @staticmethod
    def _fetch_yf(ticker: str, start: date) -> Optional[pd.Series]:
        if yf is None:
            return None
        data = yf.download(
            ticker,
            start=start.isoformat(),
            end=(date.today() + timedelta(days=1)).isoformat(),
            auto_adjust=True,
            progress=False,
        )
        if data is None or data.empty:
            return None
        close = data['Close'] if 'Close' in data.columns else data.iloc[:, 0]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = close.dropna()
        if close.empty:
            return None
        monthly = close.resample('ME').mean()
        monthly.index = monthly.index.to_period('M').to_timestamp('M')
        return monthly

    @staticmethod
    def _fetch_fred(series_id: str, start: date) -> Optional[pd.Series]:
        try:
            from backend.data_sources import fred_client
        except Exception:
            return None
        obs = fred_client.fetch_series(
            series_id,
            start_date=start.isoformat(),
            end_date=date.today().isoformat(),
        )
        if not obs:
            return None
        df = pd.DataFrame(obs)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()
        s = pd.to_numeric(df['value'], errors='coerce').dropna()
        monthly = s.resample('ME').mean()
        monthly.index = monthly.index.to_period('M').to_timestamp('M')
        return monthly

    @staticmethod
    def _fetch_gpr(start: date) -> Optional[pd.Series]:
        try:
            from backend.data_sources import gpr_index
            data = gpr_index.fetch_gpr_data()
        except Exception:
            return None
        if not data:
            return None
        # gpr_index stores monthly world GPR under 'world' or similar;
        # fall back to the first available global series.
        series = None
        if isinstance(data, dict):
            for key in ('world', 'GPR', 'global', 'aggregate'):
                if key in data:
                    series = data[key]
                    break
            if series is None:
                # Average all country series as an aggregate proxy
                try:
                    df = pd.DataFrame(data)
                    series = df.mean(axis=1)
                except Exception:
                    return None
        if series is None:
            return None
        try:
            s = pd.Series(series)
            if not isinstance(s.index, pd.DatetimeIndex):
                s.index = pd.to_datetime(s.index)
            s = s.sort_index()
            monthly = s.resample('ME').mean()
            monthly.index = monthly.index.to_period('M').to_timestamp('M')
            return monthly.loc[monthly.index >= pd.Timestamp(start)]
        except Exception:
            return None


def _transform(series: pd.Series, kind: str) -> pd.Series:
    if kind == 'logret':
        return np.log(series.replace(0, np.nan)).diff()
    if kind == 'diff':
        return series.diff()
    if kind == 'loglevel':
        s = series.replace(0, np.nan)
        return np.log(s)
    return series


# ── CommodityModel ────────────────────────────────────────────────────────

class CommodityModel:
    """SARIMAX(1,0,1) + GARCH(1,1) hybrid with 95% CI bootstrap."""

    def __init__(self, name: str):
        if name not in TICKERS:
            raise ValueError(f'Unknown commodity: {name}')
        self.name = name
        self.ticker = TICKERS[name]
        self.driver_spec = DRIVERS.get(name, [])

        self.price_monthly: Optional[pd.Series] = None
        self.last_price: Optional[float] = None
        self.exog_monthly: Optional[pd.DataFrame] = None
        self.sarimax_res = None
        self.garch_res = None
        self.residuals: Optional[pd.Series] = None
        self.n_obs: Optional[int] = None
        self.rmse: Optional[float] = None
        self.fit_at: Optional[datetime] = None
        self.fit_error: Optional[str] = None

    # ── fit ────────────────────────────────────────────────────────────

    def fit(self, fetcher: Optional[DriverFetcher] = None) -> bool:
        if not _STATS_OK:
            self.fit_error = 'statsmodels/arch not installed'
            return False
        if yf is None:
            self.fit_error = 'yfinance not installed'
            return False

        fetcher = fetcher or DriverFetcher()
        start = date.today() - timedelta(days=365 * HISTORY_YEARS)

        price = DriverFetcher._fetch_yf(self.ticker, start)
        if price is None or len(price) < 36:
            self.fit_error = f'Insufficient price history ({0 if price is None else len(price)} months)'
            return False
        self.price_monthly = price
        self.last_price = float(price.iloc[-1])

        y = _transform(price, 'logret').dropna()
        exog = self._build_exog(fetcher, start)
        if exog is not None:
            idx = y.index.intersection(exog.index)
            y = y.loc[idx]
            exog = exog.loc[idx]

        if len(y) < 36:
            self.fit_error = f'Insufficient post-align history ({len(y)} months)'
            return False

        try:
            sarimax = SARIMAX(
                y,
                exog=exog,
                order=(1, 0, 1),
                seasonal_order=(0, 0, 0, 0),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            self.sarimax_res = sarimax.fit(disp=False, maxiter=200)
        except Exception as e:
            logger.warning(f'SARIMAX fit failed for {self.name}: {e}. Retrying without exog.')
            try:
                sarimax = SARIMAX(
                    y, order=(1, 0, 1),
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                )
                self.sarimax_res = sarimax.fit(disp=False, maxiter=200)
                exog = None
            except Exception as e2:
                self.fit_error = f'SARIMAX failed: {e2}'
                return False

        self.exog_monthly = exog
        self.residuals = pd.Series(self.sarimax_res.resid, index=y.index).dropna()
        self.n_obs = int(len(y))
        self.rmse = float(np.sqrt(np.mean(self.residuals ** 2)))

        try:
            garch = arch_model(
                self.residuals * 100,  # scale for numerical stability
                mean='Zero', vol='Garch', p=1, q=1, dist='normal',
                rescale=False,
            )
            self.garch_res = garch.fit(disp='off', show_warning=False)
        except Exception as e:
            logger.warning(f'GARCH fit failed for {self.name}: {e}. Using empirical residuals.')
            self.garch_res = None

        self.fit_at = datetime.utcnow()
        self.fit_error = None
        logger.info(
            f'Fit {self.name}: n={self.n_obs}, rmse={self.rmse:.4f}, '
            f'exog={list(exog.columns) if exog is not None else "none"}, '
            f'garch={"yes" if self.garch_res is not None else "no"}'
        )
        return True

    def _build_exog(self, fetcher: DriverFetcher, start: date) -> Optional[pd.DataFrame]:
        cols: dict[str, pd.Series] = {}
        for kind, key in self.driver_spec:
            raw = fetcher.fetch(kind, key, start)
            if raw is None or len(raw) < 24:
                continue
            transform_key = f'{kind}_{key}' if kind not in ('comm',) else 'comm_'
            transform = DRIVER_TRANSFORM.get(transform_key, 'logret')
            transformed = _transform(raw, transform).dropna()
            if len(transformed) < 24:
                continue
            col_name = f'{kind}:{key}' if key else kind
            cols[col_name] = transformed
        if not cols:
            return None
        df = pd.DataFrame(cols).dropna()
        return df if len(df) >= 24 else None

    # ── forecast ────────────────────────────────────────────────────────

    def forecast(self, h: int = 4, draws: int = BOOTSTRAP_DRAWS) -> dict:
        """Forecast h forward quarterly averages with 95% CIs."""
        if self.sarimax_res is None or self.price_monthly is None:
            return {}

        rng = np.random.default_rng(seed=42)
        n_months = h * 3

        # Deterministic mean forecast (exog held at last value)
        exog_future = None
        if self.exog_monthly is not None and len(self.exog_monthly) > 0:
            last = self.exog_monthly.iloc[-1]
            exog_future = pd.DataFrame(
                np.tile(last.values, (n_months, 1)),
                columns=self.exog_monthly.columns,
            )

        try:
            mean_fc = self.sarimax_res.get_forecast(steps=n_months, exog=exog_future)
            mean_returns = np.asarray(mean_fc.predicted_mean)
        except Exception as e:
            logger.warning(f'Forecast mean failed for {self.name}: {e}')
            return {}

        # Innovation draws for each simulation: GARCH-conditional if available,
        # otherwise bootstrap from empirical residuals.
        if self.garch_res is not None:
            try:
                sims = self.garch_res.forecast(
                    horizon=n_months, reindex=False, method='simulation',
                    simulations=draws,
                )
                innov = sims.simulations.values[0] / 100.0  # back to return scale
            except Exception as e:
                logger.warning(f'GARCH simulate fell back to empirical: {e}')
                innov = rng.choice(self.residuals.values, size=(draws, n_months), replace=True)
        else:
            innov = rng.choice(self.residuals.values, size=(draws, n_months), replace=True)

        # Combine: each sim = mean_returns + innovation path
        sim_returns = mean_returns[np.newaxis, :] + innov  # (draws, n_months)
        sim_log_prices = np.cumsum(sim_returns, axis=1) + np.log(self.last_price)
        sim_prices = np.exp(sim_log_prices)

        # Aggregate into quarterly averages
        result = {}
        today = date.today()
        current_q = (today.month - 1) // 3 + 1
        next_q_num = current_q + 1
        next_q_year = today.year
        if next_q_num > 4:
            next_q_num -= 4
            next_q_year += 1

        for q_idx in range(h):
            cols = slice(q_idx * 3, (q_idx + 1) * 3)
            q_avg = sim_prices[:, cols].mean(axis=1)
            label = self._q_label(next_q_num + q_idx, next_q_year)
            result[f'Q+{q_idx + 1}'] = {
                'label': label,
                'median': float(np.median(q_avg)),
                'p2_5':   float(np.percentile(q_avg, 2.5)),
                'p10':    float(np.percentile(q_avg, 10)),
                'p90':    float(np.percentile(q_avg, 90)),
                'p97_5':  float(np.percentile(q_avg, 97.5)),
            }
        return result

    @staticmethod
    def _q_label(q_num: int, year: int) -> str:
        while q_num > 4:
            q_num -= 4
            year += 1
        return f'Q{q_num} {year}'

    # ── nowcast ─────────────────────────────────────────────────────────

    def nowcast(self, qtd_mean: Optional[float], days_elapsed: int, days_in_quarter: int) -> Optional[float]:
        if self.sarimax_res is None or self.price_monthly is None or qtd_mean is None:
            return qtd_mean
        w = max(0.0, min(1.0, days_elapsed / max(1, days_in_quarter)))
        # Model's 1-month-ahead mean forecast as a proxy for the current quarter
        try:
            exog_future = None
            if self.exog_monthly is not None and len(self.exog_monthly) > 0:
                exog_future = pd.DataFrame(
                    [self.exog_monthly.iloc[-1].values],
                    columns=self.exog_monthly.columns,
                )
            mean_fc = self.sarimax_res.get_forecast(steps=1, exog=exog_future)
            mean_return = float(mean_fc.predicted_mean.iloc[0])
            model_q0 = float(self.last_price) * float(np.exp(mean_return))
        except Exception:
            model_q0 = float(self.last_price)
        return w * qtd_mean + (1.0 - w) * model_q0

    # ── serialization ───────────────────────────────────────────────────

    def summary(self) -> dict:
        return {
            'name': self.name,
            'ticker': self.ticker,
            'drivers': [f'{k}:{v}' if v else k for k, v in self.driver_spec],
            'n_obs': self.n_obs,
            'rmse': self.rmse,
            'fit_at': self.fit_at.isoformat() if self.fit_at else None,
            'fit_error': self.fit_error,
            'last_price': self.last_price,
            'garch': self.garch_res is not None,
        }

    def save(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, path: str) -> Optional['CommodityModel']:
        try:
            with open(path, 'rb') as f:
                obj = pickle.load(f)
            return obj if isinstance(obj, cls) else None
        except Exception:
            return None


# ── Storage layer ──────────────────────────────────────────────────────────

def _safe_name(name: str) -> str:
    return name.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')


def _cache_path(name: str, cache_dir: str = CACHE_DIR) -> str:
    return os.path.join(cache_dir, f'{_safe_name(name)}.pkl')


def _sidecar_path(name: str, cache_dir: str = CACHE_DIR) -> str:
    return os.path.join(cache_dir, f'{_safe_name(name)}.json')


def _manifest_path(cache_dir: str = CACHE_DIR) -> str:
    return os.path.join(cache_dir, 'manifest.json')


def _write_sidecar(model: 'CommodityModel', cache_dir: str = CACHE_DIR) -> None:
    """Write a human-readable JSON summary + latest forecast alongside the pickle."""
    try:
        payload = {
            'summary': model.summary(),
            'forecast': model.forecast(h=4),
            'written_at': datetime.utcnow().isoformat(),
        }
        os.makedirs(cache_dir, exist_ok=True)
        with open(_sidecar_path(model.name, cache_dir), 'w') as f:
            json.dump(payload, f, indent=2, default=str)
    except Exception as e:
        logger.warning(f'{model.name}: sidecar write failed: {e}')


def _update_manifest(summaries: dict, cache_dir: str = CACHE_DIR) -> None:
    """Maintain a single manifest.json enumerating all cached fits."""
    try:
        os.makedirs(cache_dir, exist_ok=True)
        manifest = {
            'updated_at': datetime.utcnow().isoformat(),
            'stale_after_days': STALE_AFTER_DAYS,
            'cache_dir': cache_dir,
            'models': summaries,
        }
        with open(_manifest_path(cache_dir), 'w') as f:
            json.dump(manifest, f, indent=2, default=str)
    except Exception as e:
        logger.warning(f'manifest write failed: {e}')


def load_cached(name: str, cache_dir: str = CACHE_DIR) -> Optional[CommodityModel]:
    path = _cache_path(name, cache_dir)
    if not os.path.exists(path):
        return None
    model = CommodityModel.load(path)
    if model is None or model.fit_at is None:
        return None
    age = datetime.utcnow() - model.fit_at
    if age > timedelta(days=STALE_AFTER_DAYS):
        logger.info(f'{name}: cached fit is {age.days}d old (> {STALE_AFTER_DAYS}d), treating as stale')
        return None
    return model


def fit_and_cache(name: str, cache_dir: str = CACHE_DIR) -> Optional[CommodityModel]:
    model = CommodityModel(name)
    if not model.fit():
        logger.warning(f'{name}: fit failed ({model.fit_error})')
        return None
    try:
        model.save(_cache_path(name, cache_dir))
        _write_sidecar(model, cache_dir)
    except Exception as e:
        logger.warning(f'{name}: save failed: {e}')
    return model


def get_or_fit(name: str) -> Optional[CommodityModel]:
    return load_cached(name) or fit_and_cache(name)


def list_cached(cache_dir: str = CACHE_DIR) -> list[dict]:
    """Inspect what's currently on disk without deserializing the pickles."""
    if not os.path.isdir(cache_dir):
        return []
    out = []
    for fname in sorted(os.listdir(cache_dir)):
        if not fname.endswith('.json') or fname == 'manifest.json':
            continue
        try:
            with open(os.path.join(cache_dir, fname)) as f:
                out.append(json.load(f).get('summary', {}))
        except Exception:
            continue
    return out


def refit_all(cache_dir: str = CACHE_DIR) -> dict[str, dict]:
    """Monthly scheduler entry point. Refit every known commodity."""
    os.makedirs(cache_dir, exist_ok=True)
    summaries: dict[str, dict] = {}
    fetcher = DriverFetcher()  # share across all fits
    for name in TICKERS:
        try:
            model = CommodityModel(name)
            ok = model.fit(fetcher=fetcher)
            if ok:
                model.save(_cache_path(name, cache_dir))
                _write_sidecar(model, cache_dir)
            summaries[name] = model.summary()
        except Exception as e:
            logger.error(f'{name}: refit crashed: {e}')
            summaries[name] = {'name': name, 'fit_error': str(e)}
    _update_manifest(summaries, cache_dir)
    return summaries


def get_model_forecast(
    name: str,
    qtd_mean: Optional[float] = None,
    days_elapsed: int = 0,
    days_in_quarter: int = 90,
) -> Optional[dict]:
    """Public entry used by commodities_forecast.py integration."""
    model = get_or_fit(name)
    if model is None:
        return None
    forecast = model.forecast(h=4)
    if not forecast:
        return None
    nowcast_val = model.nowcast(qtd_mean, days_elapsed, days_in_quarter) if qtd_mean is not None else None
    return {
        'forecast': forecast,
        'nowcast': nowcast_val,
        'summary': model.summary(),
    }


# ── Smoke test ─────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')
    for commodity in ('Gold', 'WTI Crude'):
        print(f'\n=== {commodity} ===')
        model = CommodityModel(commodity)
        if model.fit():
            print('summary:', model.summary())
            print('forecast:', model.forecast(h=4))
        else:
            print('fit failed:', model.fit_error)
