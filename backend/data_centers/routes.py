"""Flask blueprint for /api/data-centers/*."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from backend.data_centers import service

data_centers_bp = Blueprint('data_centers', __name__)


@data_centers_bp.route('/status')
def get_status():
    return jsonify(service.status())


@data_centers_bp.route('/markets')
def get_markets():
    tier = request.args.get('tier')
    if tier and tier.lower() not in {'primary', 'secondary', 'emerging'}:
        return jsonify({'error': 'tier must be primary|secondary|emerging'}), 400
    return jsonify({'markets': service.get_markets(tier)})


@data_centers_bp.route('/summary')
def get_summary():
    return jsonify(service.get_summary())


@data_centers_bp.route('/refresh', methods=['POST'])
def refresh():
    data = service.build(force=True)
    return jsonify({
        'built': data.get('built', False),
        'build_error': data.get('build_error'),
        'market_count': len(data.get('markets', [])),
    })
