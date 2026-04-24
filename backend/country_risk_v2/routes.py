"""
Flask blueprint for /api/country-risk/*.

Endpoints (Phase 1: first three are live; /shocks and /scenario are Phase 3):

  GET  /api/country-risk/countries            — coverage list
  GET  /api/country-risk/                     — score every supported country
  GET  /api/country-risk/<code>               — score one country
  GET  /api/country-risk/<code>/drivers       — raw sub-indicator values
  GET  /api/country-risk/shocks               — shock catalogue       [Phase 3]
  POST /api/country-risk/scenario             — apply shocks          [Phase 3]
"""

from flask import Blueprint, jsonify, request

from backend.country_risk_v2 import service
from backend.country_risk_v2.country_configs import is_supported

country_risk_v2_bp = Blueprint('country_risk_v2', __name__)


@country_risk_v2_bp.route('/countries')
def list_countries():
    return jsonify({'countries': service.get_supported_countries()})


@country_risk_v2_bp.route('/', strict_slashes=False)
def all_scores():
    risks = service.score_all()
    return jsonify({'countries': [r.to_dict() for r in risks]})


@country_risk_v2_bp.route('/<country_code>')
def single_score(country_code):
    code = country_code.upper()
    if not is_supported(code):
        return jsonify({'error': f'country {code} not yet supported'}), 404

    risk = service.score_country(code)
    if risk is None:
        return jsonify({'error': f'no data for {code} (may be a phase not yet wired)'}), 503
    return jsonify(risk.to_dict())


@country_risk_v2_bp.route('/<country_code>/drivers')
def country_drivers(country_code):
    code = country_code.upper()
    if not is_supported(code):
        return jsonify({'error': f'country {code} not yet supported'}), 404

    risk = service.score_country(code)
    if risk is None:
        return jsonify({'error': f'no data for {code}'}), 503

    return jsonify({
        'country_code': code,
        'country_name': risk.country_name,
        'structural': risk.structural.to_dict() if risk.structural else None,
        'macro': risk.macro.to_dict() if risk.macro else None,
        'labor': risk.labor.to_dict() if risk.labor else None,
        'data_asof': risk.data_asof,
    })
