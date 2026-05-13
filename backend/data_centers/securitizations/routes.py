"""Flask blueprint for /api/securitizations/* — DC ABS / CMBS deal data."""

from __future__ import annotations

from functools import wraps

from flask import Blueprint, jsonify, request
from flask_login import current_user

from backend.data_centers.securitizations import service
from backend.auth import ADMIN_EMAIL

securitizations_bp = Blueprint('securitizations', __name__)


def _require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'authentication required'}), 401
        if (current_user.email or '').lower() != ADMIN_EMAIL:
            return jsonify({'error': 'admin only'}), 403
        return f(*args, **kwargs)
    return decorated


@securitizations_bp.route('/status')
def get_status():
    return jsonify(service.status())


@securitizations_bp.route('/summary')
def get_summary():
    return jsonify(service.get_summary())


@securitizations_bp.route('/deals')
def list_deals():
    return jsonify({'deals': service.get_deals()})


@securitizations_bp.route('/deal/<deal_id>')
def get_deal(deal_id):
    d = service.get_deal(deal_id)
    if not d:
        return jsonify({'error': 'not found'}), 404
    return jsonify(d)


@securitizations_bp.route('/refresh', methods=['POST'])
def refresh():
    data = service.build(force=True)
    return jsonify({
        'built': data.get('built', False),
        'build_error': data.get('build_error'),
        'deal_count': len(data.get('deals', [])),
    })


# ── Admin: EDGAR ABS pull ─────────────────────────────────────────────

@securitizations_bp.route('/admin/edgar/pull', methods=['POST'])
@_require_admin
def edgar_pull():
    from backend.data_centers.securitizations import edgar_abs
    return jsonify(edgar_abs.pull_all())


@securitizations_bp.route('/admin/kbra/parse', methods=['POST'])
@_require_admin
def kbra_parse():
    """Accepts JSON {url: ...} of a KBRA pre-sale PDF and returns parsed fields."""
    from backend.data_centers.securitizations import kbra_parser
    body = request.get_json(silent=True) or {}
    url = (body.get('url') or '').strip()
    if not url:
        return jsonify({'ok': False, 'error': 'missing url'}), 400
    return jsonify(kbra_parser.parse_url(url))
