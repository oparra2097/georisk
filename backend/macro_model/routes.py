"""
Flask blueprint for /api/macro-model/us/*.

Endpoints:
  GET  /api/macro-model/us/status       Build status + error diagnostics
  GET  /api/macro-model/us/variables    Full variable registry
  GET  /api/macro-model/us/fit          Per-equation diagnostic reports
  GET  /api/macro-model/us/forecast     Baseline 20q forecast (levels)
  GET  /api/macro-model/us/fan          Bootstrap fan chart (p10/p50/p90)
  GET  /api/macro-model/us/shocks       Catalogue of declared shocks
  POST /api/macro-model/us/shock        Run one shock by id -> IRF + paths
  POST /api/macro-model/us/refresh      Force rebuild (new fit + forecasts)
"""

import logging
from functools import wraps

from flask import Blueprint, jsonify, request
from flask_login import current_user

from backend.macro_model import service

logger = logging.getLogger(__name__)

macro_model_bp = Blueprint('macro_model', __name__)


def _macro_gate(f):
    """Require authenticated, email-verified, macro-access-granted user.

    Kept local to this blueprint (rather than importing from app.py) to avoid
    a circular import. Mirrors app.macro_access_required but always returns JSON.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'Authentication required', 'login_url': '/auth/login'}), 401
        if not current_user.email_verified:
            return jsonify({'error': 'Please verify your email first.'}), 403
        if not current_user.has_macro_access():
            return jsonify({'error': 'US Macro Model access is granted by the admin.'}), 403
        return f(*args, **kwargs)
    return decorated


def _building_detail():
    s = service.status()
    if s.get('building'):
        msg = 'model building in background — retry in 30-60 seconds'
    elif s.get('fit_error'):
        msg = 'fit failed: ' + str(s['fit_error'])
    else:
        # First touch — ensure_built inside service methods already kicked off
        # a background build, so this message matches that state.
        msg = 'model build queued — retry in 30-60 seconds'
    return msg, s


@macro_model_bp.route('/status')
@_macro_gate
def get_status():
    return jsonify(service.status())


@macro_model_bp.route('/diagnostics')
@_macro_gate
def get_diagnostics():
    """Per-series fetch status + per-equation fit status + last build error."""
    return jsonify({'status': service.status(), 'diagnostics': service.get_diagnostics()})


@macro_model_bp.route('/debug')
@_macro_gate
def get_debug():
    """One-stop diagnostic dump: env, pickle, last log lines, status, diagnostics.

    Hit this from a browser when the dashboard is empty and there's nowhere
    else to look. No secrets are returned (FRED key is reported as a boolean,
    not a value).
    """
    import os, time
    from config import Config
    from backend.log_capture import snapshot
    from backend.macro_model.service import _PERSIST_PATH

    fred_set = bool(getattr(Config, 'FRED_API_KEY', '') or os.environ.get('FRED_API_KEY', ''))

    pickle_info = {'path': _PERSIST_PATH, 'exists': os.path.exists(_PERSIST_PATH)}
    if pickle_info['exists']:
        st = os.stat(_PERSIST_PATH)
        pickle_info.update({
            'size_mb': round(st.st_size / 1024 / 1024, 2),
            'age_seconds': round(time.time() - st.st_mtime, 1),
        })

    return jsonify({
        'product': 'macro-model',
        'env': {
            'FRED_API_KEY_set': fred_set,
            'DATA_DIR': Config.DATA_DIR,
            'data_dir_exists': os.path.isdir(Config.DATA_DIR),
            'data_dir_writable': os.access(Config.DATA_DIR, os.W_OK) if os.path.isdir(Config.DATA_DIR) else False,
        },
        'pickle': pickle_info,
        'status': service.status(),
        'diagnostics': service.get_diagnostics(),
        'logs_macro_model': snapshot('macro_model')[-100:],
        'logs_fred': snapshot('fred')[-30:],
        'logs_warmup': snapshot('warmup')[-30:],
    })


@macro_model_bp.route('/variables')
@_macro_gate
def get_variables():
    return jsonify({'variables': service.get_variables()})


@macro_model_bp.route('/fit')
@_macro_gate
def get_fit():
    report = service.get_fit_report()
    if report is None:
        msg, s = _building_detail()
        return jsonify({'error': msg, 'status': s}), 503
    return jsonify(report)


@macro_model_bp.route('/forecast')
@_macro_gate
def get_forecast():
    horizon = max(1, min(40, int(request.args.get('horizon', 20))))
    records = service.get_baseline(horizon=horizon)
    if records is None:
        msg, s = _building_detail()
        return jsonify({'error': msg, 'status': s}), 503
    return jsonify({'horizon': horizon, 'path': records})


@macro_model_bp.route('/fan')
@_macro_gate
def get_fan():
    horizon = max(1, min(24, int(request.args.get('horizon', 12))))
    n_draws = max(5, min(100, int(request.args.get('n_draws', 30))))
    result = service.get_bootstrap(horizon=horizon, n_draws=n_draws)
    if result is None:
        msg, s = _building_detail()
        return jsonify({'error': msg, 'status': s}), 503
    return jsonify({'horizon': horizon, 'n_draws': n_draws, 'bands': result})


@macro_model_bp.route('/shocks')
@_macro_gate
def get_shocks():
    return jsonify({'shocks': service.get_shock_catalogue()})


@macro_model_bp.route('/shock', methods=['POST'])
@_macro_gate
def post_shock():
    body = request.get_json(silent=True) or {}
    shock_id = body.get('id')
    if not shock_id:
        return jsonify({'error': "'id' is required"}), 400
    horizon = max(1, min(40, int(body.get('horizon', 20))))
    try:
        result = service.run_shock_api(shock_id, horizon=horizon)
    except KeyError as e:
        return jsonify({'error': f'unknown shock: {e}'}), 404
    if result is None:
        msg, s = _building_detail()
        return jsonify({'error': msg, 'status': s}), 503
    return jsonify(result)


@macro_model_bp.route('/refresh', methods=['POST'])
@_macro_gate
def post_refresh():
    service.refresh()
    return jsonify(service.status())


@macro_model_bp.route('/backtest')
@_macro_gate
def get_backtest():
    """
    GET /backtest?train_end=2019-12-31&flat_exog=0

    Fits on data up to train_end, simulates out-of-sample to the panel end,
    and returns per-variable RMSE / MAE / directional accuracy plus the
    forecast-vs-actual paths. `flat_exog=1` carries exogenous forward
    instead of feeding actual values; use that to stress-test the full
    forecast system (both endogenous AND exogenous expectations).
    """
    train_end = request.args.get('train_end', '2019-12-31')
    flat_exog = request.args.get('flat_exog', '0').lower() in ('1', 'true', 'yes')
    result = service.get_backtest(train_end=train_end, flat_exog=flat_exog)
    if result is None:
        msg, s = _building_detail()
        return jsonify({'error': msg, 'status': s}), 503
    if isinstance(result, dict) and result.get('error'):
        return jsonify(result), 500
    return jsonify(result)
