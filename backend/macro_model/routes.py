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

from flask import Blueprint, jsonify, request

from backend.macro_model import service

logger = logging.getLogger(__name__)

macro_model_bp = Blueprint('macro_model', __name__)


@macro_model_bp.route('/status')
def get_status():
    return jsonify(service.status())


@macro_model_bp.route('/variables')
def get_variables():
    return jsonify({'variables': service.get_variables()})


@macro_model_bp.route('/fit')
def get_fit():
    report = service.get_fit_report()
    if report is None:
        status = service.status()
        return jsonify({'error': 'model not built', 'detail': status}), 503
    return jsonify(report)


@macro_model_bp.route('/forecast')
def get_forecast():
    horizon = max(1, min(40, int(request.args.get('horizon', 20))))
    records = service.get_baseline(horizon=horizon)
    if records is None:
        return jsonify({'error': 'model not built'}), 503
    return jsonify({'horizon': horizon, 'path': records})


@macro_model_bp.route('/fan')
def get_fan():
    horizon = max(1, min(24, int(request.args.get('horizon', 12))))
    n_draws = max(5, min(100, int(request.args.get('n_draws', 30))))
    result = service.get_bootstrap(horizon=horizon, n_draws=n_draws)
    if result is None:
        return jsonify({'error': 'model not built'}), 503
    return jsonify({'horizon': horizon, 'n_draws': n_draws, 'bands': result})


@macro_model_bp.route('/shocks')
def get_shocks():
    return jsonify({'shocks': service.get_shock_catalogue()})


@macro_model_bp.route('/shock', methods=['POST'])
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
        return jsonify({'error': 'model not built'}), 503
    return jsonify(result)


@macro_model_bp.route('/refresh', methods=['POST'])
def post_refresh():
    service.refresh()
    return jsonify(service.status())
