"""Flask blueprint for /api/private-credit/* — DC private-credit ledger."""

from __future__ import annotations

from functools import wraps

from flask import Blueprint, jsonify
from flask_login import current_user

from backend.data_centers.private_credit import service
from backend.auth import ADMIN_EMAIL

private_credit_bp = Blueprint('private_credit', __name__)


def _require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'authentication required'}), 401
        if (current_user.email or '').lower() != ADMIN_EMAIL:
            return jsonify({'error': 'admin only'}), 403
        return f(*args, **kwargs)
    return decorated


@private_credit_bp.route('/status')
def get_status():
    return jsonify(service.status())


@private_credit_bp.route('/summary')
def get_summary():
    return jsonify(service.get_summary())


@private_credit_bp.route('/rows')
def list_rows():
    return jsonify({'rows': service.get_rows()})


@private_credit_bp.route('/admin/bdc/pull', methods=['POST'])
@_require_admin
def bdc_pull():
    """Scrape EDGAR for the latest 10-Q from each BDC, match against the
    operator dictionary, persist results to data/_bdc_cache.json, and
    refresh the cache."""
    from backend.data_centers.private_credit import edgar_bdc
    result = edgar_bdc.pull_all()
    if result.get('ok'):
        service.write_bdc_cache(result)
    return jsonify(result)
