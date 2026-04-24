"""
Flask blueprint for /api/house-prices/*.

All endpoints are gated the same way as /api/macro-model/us/* — verified,
admin-granted users only. Uses a separate access flag on the User model:
has_hpi_access (admin email auto-granted).

Endpoints:
  GET  /api/house-prices/status           Build state
  GET  /api/house-prices/sources          Source catalogue (FHFA / CS / Zillow)
  GET  /api/house-prices/summary          National + top regions (dashboard hero)
  GET  /api/house-prices/level/<level>    All entities at a level, sorted by YoY
  GET  /api/house-prices/entity/<level>/<code>   Single entity + history
  GET  /api/house-prices/history/<level>/<code>  Just the time-series
  POST /api/house-prices/refresh          Force rebuild
"""

from __future__ import annotations

import logging
from functools import wraps

from flask import Blueprint, jsonify, request
from flask_login import current_user

from backend.house_prices import service

logger = logging.getLogger(__name__)

house_prices_bp = Blueprint('house_prices', __name__)

_VALID_LEVELS = {'national', 'region', 'state', 'msa', 'county', 'zip'}


def _hpi_gate(f):
    """Require authenticated, verified, HPI-access-granted user. JSON responses."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'Authentication required', 'login_url': '/auth/login'}), 401
        if not current_user.email_verified:
            return jsonify({'error': 'Please verify your email first.'}), 403
        # has_hpi_access is added in Phase 6 (auth.py); guard safely.
        has_access = getattr(current_user, 'has_hpi_access', lambda: False)()
        if not has_access:
            return jsonify({'error': 'US House Prices access is granted by the admin.'}), 403
        return f(*args, **kwargs)
    return decorated


def _build_state_msg():
    s = service.status()
    if s.get('building'):
        return 'data sources downloading in background — retry in 30-90 seconds', s
    if s.get('build_error'):
        return 'build failed: ' + str(s['build_error']), s
    if not s.get('built'):
        return 'data not yet loaded — kicked off background build, retry shortly', s
    return None, s


@house_prices_bp.route('/status')
@_hpi_gate
def get_status():
    return jsonify(service.status())


@house_prices_bp.route('/diagnostics')
@_hpi_gate
def get_diagnostics():
    """Per-source fetch status + build state. The dashboard polls this
    while building so the user sees what's happening."""
    return jsonify({'status': service.status(), 'diagnostics': service.get_diagnostics()})


@house_prices_bp.route('/sources')
@_hpi_gate
def get_sources():
    return jsonify({'sources': service.get_sources()})


@house_prices_bp.route('/summary')
@_hpi_gate
def get_summary():
    return jsonify(service.get_summary())


@house_prices_bp.route('/level/<level>')
@_hpi_gate
def get_level(level):
    level = level.lower()
    if level not in _VALID_LEVELS:
        return jsonify({'error': f'invalid level: {level}'}), 400
    s = service.status()
    if not s.get('built'):
        # Trigger a background build via ensure_built() (called inside service.get_level)
        # but signal that we're not yet ready, so the dashboard can show "building" UX.
        msg, st = _build_state_msg()
        return jsonify({'level': level, 'entities': service.get_level(level),
                        'pending': True, 'message': msg, 'status': st})
    return jsonify({'level': level, 'entities': service.get_level(level)})


@house_prices_bp.route('/entity/<level>/<path:code>')
@_hpi_gate
def get_entity(level, code):
    level = level.lower()
    if level not in _VALID_LEVELS:
        return jsonify({'error': f'invalid level: {level}'}), 400
    entity = service.get_entity(level, code)
    if entity is None:
        return jsonify({'error': f'no data for {level}/{code}'}), 404
    return jsonify(entity)


@house_prices_bp.route('/history/<level>/<path:code>')
@_hpi_gate
def get_history(level, code):
    level = level.lower()
    if level not in _VALID_LEVELS:
        return jsonify({'error': f'invalid level: {level}'}), 400
    min_year = int(request.args.get('min_year', '2000'))
    hist = service.get_history(level, code, min_year=min_year)
    if hist is None:
        return jsonify({'error': f'no history for {level}/{code}'}), 404
    return jsonify({'level': level, 'code': code, 'history': hist})


@house_prices_bp.route('/refresh', methods=['POST'])
@_hpi_gate
def post_refresh():
    include_zip = str(request.args.get('zip', '0')).lower() in ('1', 'true', 'yes')
    service.refresh(include_zillow_zip=include_zip)
    return jsonify(service.status())
