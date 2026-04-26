"""
HPI forecast Flask blueprint at /api/house-prices/forecast/*.

Reuses the same access-gating policy as the rest of /api/house-prices/*:
authenticated, email-verified, admin-granted has_hpi_access.

Endpoints:
  GET  /status             Fit status + last error
  GET  /baseline?h=8       Deterministic 8q baseline forecast
  GET  /fan?h=8&n=200      Residual-bootstrap fan (p10/p50/p90)
  GET  /fit                Full per-equation diagnostic report
  GET  /shocks             Shock catalogue
  POST /shock              Run one shock by id; returns baseline + shocked + IRF
  POST /refresh            Force rebuild
"""

from __future__ import annotations

import logging
from functools import wraps

from flask import Blueprint, jsonify, request
from flask_login import current_user

from backend.house_prices.forecast import service

logger = logging.getLogger(__name__)

hpi_forecast_bp = Blueprint('hpi_forecast', __name__)


def _hpi_gate(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'Authentication required', 'login_url': '/auth/login'}), 401
        if not current_user.email_verified:
            return jsonify({'error': 'Please verify your email first.'}), 403
        has_access = getattr(current_user, 'has_hpi_access', lambda: False)()
        if not has_access:
            return jsonify({'error': 'US House Prices access is granted by the admin.'}), 403
        return f(*args, **kwargs)
    return decorated


def _building_msg() -> tuple[str, dict]:
    s = service.status()
    if s.get('building'):
        return 'forecast model building in background — retry in 15-30 seconds', s
    if s.get('fit_error'):
        return 'fit failed: ' + str(s['fit_error']), s
    return 'forecast build queued — retry shortly', s


@hpi_forecast_bp.route('/status')
@_hpi_gate
def get_status():
    return jsonify(service.status())


@hpi_forecast_bp.route('/baseline')
@_hpi_gate
def get_baseline():
    h = max(1, min(24, int(request.args.get('h', 8))))
    records = service.get_baseline(horizon=h)
    if records is None:
        msg, s = _building_msg()
        return jsonify({'error': msg, 'status': s}), 503
    return jsonify({'horizon': h, 'path': records})


@hpi_forecast_bp.route('/fan')
@_hpi_gate
def get_fan():
    h = max(1, min(20, int(request.args.get('h', 8))))
    n = max(20, min(500, int(request.args.get('n', 200))))
    records = service.get_fan(horizon=h, n_draws=n)
    if records is None:
        msg, s = _building_msg()
        return jsonify({'error': msg, 'status': s}), 503
    return jsonify({'horizon': h, 'n_draws': n, 'bands': records})


@hpi_forecast_bp.route('/fit')
@_hpi_gate
def get_fit():
    report = service.get_fit_report()
    if report is None:
        msg, s = _building_msg()
        return jsonify({'error': msg, 'status': s}), 503
    return jsonify(report)


@hpi_forecast_bp.route('/shocks')
@_hpi_gate
def get_shocks():
    return jsonify({'shocks': service.get_shock_list()})


@hpi_forecast_bp.route('/shock', methods=['POST'])
@_hpi_gate
def post_shock():
    body = request.get_json(silent=True) or {}
    shock_id = body.get('id')
    if not shock_id:
        return jsonify({'error': "'id' is required"}), 400
    h = max(1, min(20, int(body.get('h', 8))))
    try:
        result = service.run_shock(shock_id, horizon=h)
    except KeyError as e:
        return jsonify({'error': f'unknown shock: {e}'}), 404
    if result is None:
        msg, s = _building_msg()
        return jsonify({'error': msg, 'status': s}), 503
    return jsonify(result)


@hpi_forecast_bp.route('/refresh', methods=['POST'])
@_hpi_gate
def post_refresh():
    service.refresh()
    return jsonify(service.status())
