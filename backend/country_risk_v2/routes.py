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

from backend.country_risk_v2 import service, shocks
from backend.country_risk_v2.country_configs import is_supported
from backend.country_risk_v2.models import ShockSpec

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
    # 'EA' is a special alias for the euro-area aggregate; accepted even though
    # it's not in PRIORITY_ORDER.
    if not (is_supported(code) or code == 'EA'):
        return jsonify({'error': f'country {code} not yet supported'}), 404

    scope = request.args.get('scope', 'eu27').lower()
    risk = service.score_country(code, scope=scope)
    if risk is None:
        return jsonify({'error': f'no data for {code} (may be a phase not yet wired)'}), 503
    return jsonify(risk.to_dict())


@country_risk_v2_bp.route('/<country_code>/drivers')
def country_drivers(country_code):
    code = country_code.upper()
    if not (is_supported(code) or code == 'EA'):
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


@country_risk_v2_bp.route('/shocks')
def list_shocks():
    return jsonify({
        'shocks': shocks.get_catalogue(),
        'supported_countries': shocks.supported_countries(),
    })


@country_risk_v2_bp.route('/scenario', methods=['POST'])
def run_scenario():
    """
    Body: {"country": "US", "shocks": [{"id": "oil_plus_20", "magnitude": 0.2}, ...]}

    `magnitude` is optional; if omitted or 0, the shock's default_magnitude is used.
    """
    body = request.get_json(silent=True) or {}
    country = str(body.get('country', '')).upper()
    if not country:
        return jsonify({'error': "'country' is required"}), 400

    if not (is_supported(country) or country == 'EA'):
        return jsonify({'error': f'country {country} not yet supported'}), 404

    raw_shocks = body.get('shocks') or []
    if not isinstance(raw_shocks, list) or not raw_shocks:
        return jsonify({'error': "'shocks' must be a non-empty list"}), 400

    try:
        shock_specs = [ShockSpec.from_dict(s) for s in raw_shocks]
    except (KeyError, TypeError, ValueError) as e:
        return jsonify({'error': f'invalid shock spec: {e}'}), 400

    base_risk = service.score_country(country)
    if base_risk is None:
        return jsonify({'error': f'no base score for {country}'}), 503

    result = shocks.apply_scenario(country, base_risk.composite, shock_specs)
    return jsonify(result.to_dict())
