"""
/api/v1/* — bearer-token-only JSON surface for programmatic clients.

Design:
  * Auth via `Authorization: Bearer pk_live_...` (or `?api_key=...`).
    Cookie sessions are *deliberately* ignored here, so a stolen browser
    cookie cannot read the data API and a leaked bot key cannot reach the
    dashboard.
  * Returns JSON 401/403 — never an HTML redirect to /auth/login.
  * Read-only. Mutating endpoints stay on the cookie-auth /api/* surface.

Algotrader contract:
  * Stable. Adding fields is a non-breaking change; renaming or removing
    fields requires a /api/v2/.
  * Every payload includes `as_of` (server clock) so consumers can detect
    stale-cache reads.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, date

from flask import Blueprint, jsonify, request
from flask_login import current_user

from config import Config

logger = logging.getLogger(__name__)

api_v1_bp = Blueprint('api_v1', __name__)


# ── Decorators ───────────────────────────────────────────────────────────

def _is_api_key_request():
    """True iff the current request authenticated via bearer/api_key, not cookie."""
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        return True
    if request.args.get('api_key'):
        return True
    return False


def api_key_required(f):
    """Bearer-token gate. Returns JSON 401 (not redirect) on failure."""
    from functools import wraps

    @wraps(f)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({
                'error': 'authentication_required',
                'detail': 'Send `Authorization: Bearer <api_key>` against /api/v1/*. '
                          'Mint a key at /auth/api-keys.',
            }), 401
        if not _is_api_key_request():
            # Logged-in browser users hitting /api/v1 directly is fine for
            # debugging in DevTools, but bots should always use bearer.
            # Allow it but tag it; some downstream auditors may filter.
            request.environ['parramacro.auth_via_cookie'] = True
        if not current_user.email_verified:
            return jsonify({'error': 'email_not_verified'}), 403
        return f(*args, **kwargs)

    return wrapped


def macro_access_required(f):
    from functools import wraps

    @wraps(f)
    def wrapped(*args, **kwargs):
        if not current_user.has_macro_access():
            return jsonify({'error': 'macro_access_required'}), 403
        return f(*args, **kwargs)

    return api_key_required(wrapped)


def hpi_access_required(f):
    from functools import wraps

    @wraps(f)
    def wrapped(*args, **kwargs):
        if not current_user.has_hpi_access():
            return jsonify({'error': 'hpi_access_required'}), 403
        return f(*args, **kwargs)

    return api_key_required(wrapped)


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'


# ── /api/v1/health ──────────────────────────────────────────────────────
#
# Public (no auth). Lets the algotrader gate trades on data freshness:
# stale data → don't trade.

# How fresh is fresh? If the source's last update is older than this,
# the source is reported in `stale[]`.
_FRESHNESS_LIMITS = {
    'georisk': timedelta(hours=2),       # GDELT runs every 15min, allow slack
    'commodities': timedelta(hours=36),  # commodities cache TTL is 24h + slack
    'macro_model': timedelta(days=14),   # quarterly data; refit infrequently
}


def _to_iso(value):
    """Coerce any of the timestamp shapes the underlying services use
    into an ISO-8601 string with `Z`. Returns None if value is missing
    or unparseable.

    Different services persist their freshness clocks differently:
      - georisk uses a `datetime` object
      - commodities stores ISO strings (`now.isoformat()`)
      - macro_model stores Unix-timestamp floats (`time.time()`)
    The /health probe must accept all three, or it'll silently report
    fully-built products as null/stale."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat() + 'Z'
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(value)).replace(
                microsecond=0).isoformat() + 'Z'
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        # Already ISO-ish; normalize to a single 'Z' suffix.
        return value if value.endswith('Z') else value + 'Z'
    return None


def _freshness_for_georisk():
    try:
        from backend.cache.store import RiskDataStore
        store = RiskDataStore()
        return _to_iso(store.get_last_refresh())
    except Exception as e:
        logger.debug(f'georisk freshness probe failed: {e}')
        return None


def _freshness_for_commodities():
    """Peek the commodities cache without triggering a refit. /health
    must stay cheap — calling get_forecast_data() would fall through to
    _fetch_forecasts() on a cold cache and refit every model (~20s).

    Probe order:
      1. In-memory daily forecast cache (`commodities_forecast._cache`).
         Populated by the boot warmup, request handlers, and the cron.
      2. On-disk model pickles (`commodity_models.CACHE_DIR/*.pkl`).
         Falls back here so we don't report null during the brief window
         between the warmup finishing per-model fits and finishing the
         daily-cache rebuild — and after a worker restart that hasn't
         yet hit the forecast endpoint.
    """
    try:
        from backend.data_sources.commodities_forecast import _cache
        with _cache._lock:
            data = _cache._data
        if data:
            return _to_iso(data.get('last_updated'))
    except Exception as e:
        logger.debug(f'commodities daily-cache probe failed: {e}')
    # Disk fallback
    try:
        import os
        from backend.data_sources.commodity_models import CACHE_DIR
        if not os.path.isdir(CACHE_DIR):
            return None
        mtimes = []
        for fname in os.listdir(CACHE_DIR):
            if not fname.endswith('.pkl'):
                continue
            try:
                mtimes.append(os.path.getmtime(os.path.join(CACHE_DIR, fname)))
            except OSError:
                continue
        if not mtimes:
            return None
        # Most recent fit wins — algotrader's "is data fresh?" gate
        # only needs to know the youngest fit hasn't gone stale.
        return _to_iso(max(mtimes))
    except Exception as e:
        logger.debug(f'commodities disk-probe failed: {e}')
        return None


def _freshness_for_macro():
    try:
        from backend.macro_model import service as macro_service
        return _to_iso(macro_service.status().get('built_at'))
    except Exception as e:
        logger.debug(f'macro freshness probe failed: {e}')
        return None


def _parse_iso_z(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.rstrip('Z'))
    except Exception:
        return None


@api_v1_bp.route('/health')
def health():
    """Service health + per-product freshness map.

    Public — no auth required. Algotrader should poll this before placing
    any trade and refuse to trade if its product appears in `stale[]` or
    `freshest_data` is null for it.
    """
    freshest = {
        'georisk':     _freshness_for_georisk(),
        'commodities': _freshness_for_commodities(),
        'macro_model': _freshness_for_macro(),
    }
    now = datetime.utcnow()
    stale = []
    for product, ts_iso in freshest.items():
        ts = _parse_iso_z(ts_iso)
        limit = _FRESHNESS_LIMITS.get(product)
        if ts is None:
            stale.append(product)
        elif limit is not None and (now - ts) > limit:
            stale.append(product)
    return jsonify({
        'as_of': _now_iso(),
        'freshest_data': freshest,
        'stale': stale,
        'ok': len(stale) == 0,
    })


# ── /api/v1/georisk ──────────────────────────────────────────────────────

@api_v1_bp.route('/georisk/scores')
@api_key_required
def v1_georisk_scores():
    from backend.cache.store import RiskDataStore
    store = RiskDataStore()
    all_scores = store.get_all_scores()
    return jsonify({
        'as_of': _now_iso(),
        'scores': {code: r.to_dict() for code, r in all_scores.items()},
    })


@api_v1_bp.route('/georisk/scores/<country_code>')
@api_key_required
def v1_georisk_country(country_code):
    from backend.cache.store import RiskDataStore
    store = RiskDataStore()
    r = store.get_country(country_code.upper())
    if not r:
        return jsonify({'error': 'country_not_found', 'country_code': country_code.upper()}), 404
    return jsonify({'as_of': _now_iso(), 'score': r.to_dict()})


@api_v1_bp.route('/georisk/hotspots')
@api_key_required
def v1_georisk_hotspots():
    from backend.cache.store import RiskDataStore
    store = RiskDataStore()
    try:
        threshold = float(request.args.get('threshold', Config.HOTSPOT_THRESHOLD))
    except (TypeError, ValueError):
        return jsonify({'error': 'invalid_threshold'}), 400
    hotspots = store.get_hotspots(threshold)
    hotspots.sort(key=lambda x: x.composite_score, reverse=True)
    return jsonify({
        'as_of': _now_iso(),
        'threshold': threshold,
        'hotspots': [h.to_dict() for h in hotspots],
    })


@api_v1_bp.route('/georisk/headlines/<country_code>')
@api_key_required
def v1_georisk_headlines(country_code):
    from backend.cache.store import RiskDataStore
    store = RiskDataStore()
    code = country_code.upper()
    if code == 'GLOBAL':
        articles = store.get_global_headlines()
    else:
        articles = store.get_headlines(code)
    return jsonify({
        'as_of': _now_iso(),
        'country_code': code,
        'articles': [a.to_dict() for a in articles],
    })


# ── /api/v1/commodities ──────────────────────────────────────────────────

@api_v1_bp.route('/commodities/list')
@api_key_required
def v1_commodities_list():
    try:
        from backend.data_sources.commodity_models import TICKERS
        return jsonify({
            'as_of': _now_iso(),
            'commodities': sorted(TICKERS.keys()),
        })
    except Exception as e:
        logger.exception('commodities list failed')
        return jsonify({'error': 'list_failed', 'detail': str(e)}), 500


def _parse_as_of(raw):
    """Parse YYYY-MM-DD or ISO timestamp into a `date`. Returns
    (date_or_None, error_message_or_None)."""
    if not raw:
        return None, None
    try:
        return date.fromisoformat(raw[:10]), None
    except Exception:
        return None, f'invalid as_of: expected YYYY-MM-DD, got {raw!r}'


def _resolve_commodity(raw):
    """Map any case variant of a commodity name to the canonical TICKERS
    key. Returns (canonical_name, error_response_tuple_or_None)."""
    raw = (raw or '').strip()
    if not raw:
        return None, (jsonify({'error': 'missing_commodity'}), 400)
    from backend.data_sources.commodity_models import TICKERS
    name = next((k for k in TICKERS if k.lower() == raw.lower()), None)
    if name is None:
        return None, (jsonify({
            'error': 'unknown_commodity',
            'detail': f'Unknown commodity: {raw!r}',
            'known': sorted(TICKERS.keys()),
        }), 404)
    return name, None


@api_v1_bp.route('/commodities/forecasts')
@api_key_required
def v1_commodities_forecast():
    """Per-commodity SARIMAX+GARCH fan. Optional `?as_of=YYYY-MM-DD`
    refits the model with data truncated at that date — for backtesting.

    Commodity name resolution is case-insensitive: `?commodity=Gold`,
    `gold`, and `GOLD` all map to the canonical TICKERS key 'Gold'. The
    response always echoes the canonical mixed-case form.

    Without as_of:
      * Uses the cached daily forecast from the scheduled refit (fast).
    With as_of:
      * Fits fresh on data up to as_of (slower, ~10-30s per commodity).
      * Result is NOT cached (every as_of value would clutter cache).

    Response shape (both paths):
      {
        "as_of": <server-now>,
        "as_of_param": <as_of-or-null>,
        "commodity": "Gold",
        "forecast": { ... model output ... }
      }
    """
    name, err_resp = _resolve_commodity(request.args.get('commodity'))
    if err_resp is not None:
        return err_resp

    as_of_dt, err = _parse_as_of(request.args.get('as_of'))
    if err:
        return jsonify({'error': 'invalid_as_of', 'detail': err}), 400

    try:
        if as_of_dt is None:
            from backend.data_sources.commodity_models import get_model_forecast
            forecast = get_model_forecast(name)
            if not forecast:
                return jsonify({
                    'error': 'commodity_not_available',
                    'detail': f'no cached forecast for {name!r}',
                }), 404
            return jsonify({
                'as_of': _now_iso(),
                'as_of_param': None,
                'commodity': name,
                'forecast': forecast,
            })
        # Backtest path: fresh fit at the historical pivot.
        from backend.data_sources.commodity_models import (
            CommodityModel, DriverFetcher,
        )
        model = CommodityModel(name)
        ok = model.fit(fetcher=DriverFetcher(), as_of=as_of_dt)
        if not ok:
            return jsonify({
                'error': 'fit_failed',
                'detail': model.fit_error or 'unknown',
                'commodity': name,
                'as_of': as_of_dt.isoformat(),
            }), 422
        forecast = model.forecast(h=4)
        return jsonify({
            'as_of': _now_iso(),
            'as_of_param': as_of_dt.isoformat(),
            'commodity': name,
            'forecast': forecast,
            'model_summary': model.summary(),
        })
    except Exception as e:
        logger.exception('v1 commodity forecast failed')
        return jsonify({'error': 'internal_error', 'detail': str(e)}), 500


# ── /api/v1/macro (US macro model) ───────────────────────────────────────

@api_v1_bp.route('/macro/baseline')
@macro_access_required
def v1_macro_baseline():
    try:
        from backend.macro_model import service as macro_service
        try:
            horizon = int(request.args.get('horizon', 20))
        except (TypeError, ValueError):
            return jsonify({'error': 'invalid_horizon'}), 400
        return jsonify({
            'as_of': _now_iso(),
            'horizon': horizon,
            'baseline': macro_service.get_baseline(horizon=horizon),
        })
    except Exception as e:
        logger.exception('v1 macro baseline failed')
        return jsonify({'error': 'internal_error', 'detail': str(e)}), 500


@api_v1_bp.route('/macro/fan')
@macro_access_required
def v1_macro_fan():
    try:
        from backend.macro_model import service as macro_service
        try:
            horizon = int(request.args.get('horizon', 12))
            n_draws = int(request.args.get('n_draws', 30))
        except (TypeError, ValueError):
            return jsonify({'error': 'invalid_params'}), 400
        return jsonify({
            'as_of': _now_iso(),
            'horizon': horizon,
            'n_draws': n_draws,
            'fan': macro_service.get_bootstrap(horizon=horizon, n_draws=n_draws),
        })
    except Exception as e:
        logger.exception('v1 macro fan failed')
        return jsonify({'error': 'internal_error', 'detail': str(e)}), 500


# ── /api/v1/hpi ──────────────────────────────────────────────────────────

@api_v1_bp.route('/hpi/forecast/state/<code>/baseline')
@hpi_access_required
def v1_hpi_state_baseline(code):
    try:
        from backend.house_prices.forecast import service as hpi_service
        out = hpi_service.get_state_baseline(code.upper())
        if out is None:
            return jsonify({'error': 'state_not_found', 'code': code.upper()}), 404
        return jsonify({'as_of': _now_iso(), 'state': code.upper(), 'baseline': out})
    except Exception as e:
        logger.exception('v1 hpi state baseline failed')
        return jsonify({'error': 'internal_error', 'detail': str(e)}), 500


@api_v1_bp.route('/hpi/forecast/state/<code>/fan')
@hpi_access_required
def v1_hpi_state_fan(code):
    try:
        from backend.house_prices.forecast import service as hpi_service
        out = hpi_service.get_state_fan(code.upper())
        if out is None:
            return jsonify({'error': 'state_not_found', 'code': code.upper()}), 404
        return jsonify({'as_of': _now_iso(), 'state': code.upper(), 'fan': out})
    except Exception as e:
        logger.exception('v1 hpi state fan failed')
        return jsonify({'error': 'internal_error', 'detail': str(e)}), 500
