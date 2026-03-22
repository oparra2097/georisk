from flask import Blueprint, jsonify
from backend.cache.store import store
from backend.data_sources.market_data import get_market_data
from config import Config

api_bp = Blueprint('api', __name__)


@api_bp.route('/scores')
def get_all_scores():
    """Return risk scores for all countries."""
    all_scores = store.get_all_scores()
    result = {}
    for code, risk in all_scores.items():
        result[code] = risk.to_dict()
    return jsonify(result)


@api_bp.route('/scores/<country_code>')
def get_country_score(country_code):
    """Return detailed score for a single country."""
    code = country_code.upper()
    risk = store.get_country(code)
    if not risk:
        return jsonify({'error': 'Country not found'}), 404
    return jsonify(risk.to_dict())


@api_bp.route('/headlines/<country_code>')
def get_country_headlines(country_code):
    """Return recent headlines for a country."""
    code = country_code.upper()
    if code == 'GLOBAL':
        articles = store.get_global_headlines()
    else:
        articles = store.get_headlines(code)
    return jsonify({
        'articles': [a.to_dict() for a in articles]
    })


@api_bp.route('/headlines/global')
def get_global_headlines():
    """Return global geopolitical headlines."""
    articles = store.get_global_headlines()
    return jsonify({
        'articles': [a.to_dict() for a in articles]
    })


@api_bp.route('/hotspots')
def get_hotspots():
    """Return countries with risk score above threshold."""
    threshold = Config.HOTSPOT_THRESHOLD
    hotspots = store.get_hotspots(threshold)
    hotspots.sort(key=lambda x: x.composite_score, reverse=True)
    return jsonify({
        'hotspots': [h.to_dict() for h in hotspots]
    })


@api_bp.route('/markets')
def get_markets():
    """Return live market data (cached 5 minutes)."""
    data = get_market_data()
    return jsonify(data)


@api_bp.route('/status')
def get_status():
    """Return system health info."""
    last_refresh = store.get_last_refresh()
    return jsonify({
        'status': 'ok',
        'last_refresh': last_refresh.isoformat() if last_refresh else None,
        'countries_tracked': store.country_count(),
        'hotspot_count': len(store.get_hotspots(Config.HOTSPOT_THRESHOLD))
    })
