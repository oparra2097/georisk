"""Flask blueprint for /api/data-centers/*."""

from __future__ import annotations

from functools import wraps

from flask import Blueprint, jsonify, request
from flask_login import current_user

from backend.data_centers import service
from backend.auth import ADMIN_EMAIL

data_centers_bp = Blueprint('data_centers', __name__)


def _require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'authentication required'}), 401
        if (current_user.email or '').lower() != ADMIN_EMAIL:
            return jsonify({'error': 'admin only'}), 403
        return f(*args, **kwargs)
    return decorated


@data_centers_bp.route('/status')
def get_status():
    return jsonify(service.status())


@data_centers_bp.route('/markets')
def get_markets():
    tier = request.args.get('tier')
    if tier and tier.lower() not in {'primary', 'secondary', 'emerging'}:
        return jsonify({'error': 'tier must be primary|secondary|emerging'}), 400
    return jsonify({'markets': service.get_markets(tier)})


def _scenario_args():
    baseline = (request.args.get('baseline') or service.DEFAULT_SCENARIO).lower()
    stresses = request.args.get('stresses') or ''
    return baseline, stresses


@data_centers_bp.route('/summary')
def get_summary():
    baseline, stresses = _scenario_args()
    return jsonify(service.get_summary(baseline=baseline, stresses=stresses))


@data_centers_bp.route('/facilities')
def get_facilities():
    status_q = request.args.get('status')
    funding_q = request.args.get('funding_type')
    market_q = request.args.get('market')
    tenant_q = request.args.get('tenant')
    developer_q = request.args.get('developer')
    baseline, stresses = _scenario_args()
    if status_q and status_q.lower() not in {'built', 'under_construction', 'planned'}:
        return jsonify({'error': 'status must be built|under_construction|planned'}), 400
    if funding_q and funding_q.lower() not in service.FUNDING_TYPES:
        return jsonify({'error': f'funding_type must be one of {list(service.FUNDING_TYPES)}'}), 400
    return jsonify({
        'facilities': service.get_facilities(
            status_q, funding_q, market_q, tenant_q, developer_q,
            baseline=baseline, stresses=stresses,
        ),
        'funding_types': service.FUNDING_TYPES,
    })


@data_centers_bp.route('/refresh', methods=['POST'])
def refresh():
    data = service.build(force=True)
    return jsonify({
        'built': data.get('built', False),
        'build_error': data.get('build_error'),
        'market_count': len(data.get('markets', [])),
        'facility_count': len(data.get('facilities', [])),
    })


# ── Admin endpoints ──────────────────────────────────────────────────────

@data_centers_bp.route('/admin/upload-markets', methods=['POST'])
@_require_admin
def upload_markets():
    from backend.data_centers import admin
    f = request.files.get('file')
    if not f:
        return jsonify({'ok': False, 'error': 'no file uploaded (form field "file")'}), 400
    raw = f.read()
    if len(raw) > 5 * 1024 * 1024:
        return jsonify({'ok': False, 'error': 'file too large (max 5MB)'}), 413
    return jsonify(admin.upload_csv('markets', raw))


@data_centers_bp.route('/admin/upload-facilities', methods=['POST'])
@_require_admin
def upload_facilities():
    from backend.data_centers import admin
    f = request.files.get('file')
    if not f:
        return jsonify({'ok': False, 'error': 'no file uploaded (form field "file")'}), 400
    raw = f.read()
    if len(raw) > 5 * 1024 * 1024:
        return jsonify({'ok': False, 'error': 'file too large (max 5MB)'}), 413
    return jsonify(admin.upload_csv('facilities', raw))


@data_centers_bp.route('/admin/drift', methods=['GET'])
@_require_admin
def drift_signals():
    from backend.data_centers import drift
    return jsonify(drift.load_signals())


@data_centers_bp.route('/admin/drift/scan', methods=['POST'])
@_require_admin
def drift_scan():
    from backend.data_centers import drift
    return jsonify(drift.scan())
