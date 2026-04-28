import os
from functools import wraps
from flask import Flask, render_template, redirect, url_for, request, jsonify
from flask_cors import CORS
from flask_login import LoginManager, current_user, login_required
from config import Config

# Install the in-memory log ring buffer BEFORE anything else logs anything,
# so the /debug endpoints can show the full warmup history.
from backend.log_capture import install as _install_log_capture
_install_log_capture()


def _gated_page(check_access, active_page: str, reason: str):
    """Build a Flask decorator that gates a page on a User method.

    `check_access` is a callable (user) -> bool. `reason` is shown on the
    access-denied page when the user is verified but not granted.
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Authentication required', 'login_url': '/auth/login'}), 401
                return redirect(url_for('auth.login', next=request.url))
            if not current_user.email_verified:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Please verify your email first.'}), 403
                return render_template('access_denied.html',
                                       reason='Verify your email to continue.',
                                       active_page=active_page), 403
            if not check_access(current_user):
                if request.path.startswith('/api/'):
                    return jsonify({'error': f'{active_page} access is granted by the admin.'}), 403
                return render_template('access_denied.html',
                                       reason=reason,
                                       active_page=active_page), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


def macro_access_required(f):
    return _gated_page(
        lambda u: u.has_macro_access(),
        active_page='macro-model',
        reason='Macro Model access is granted by the admin. Contact support to request access.',
    )(f)


def hpi_access_required(f):
    return _gated_page(
        lambda u: u.has_hpi_access(),
        active_page='house-prices',
        reason='US House Prices access is granted by the admin. Contact support to request access.',
    )(f)


from backend.routes import api_bp
from backend.economist import economist_bp
from backend.auth import auth_bp, user_loader, request_loader, init_auth_db, ADMIN_EMAIL
from backend.api_v1 import api_v1_bp
from backend.scheduler import init_scheduler
from backend.sharing import sharing_bp, meta_for_path, is_social_crawler
from backend.macro_model.routes import macro_model_bp
from backend.house_prices.routes import house_prices_bp
from backend.house_prices.forecast.routes import hpi_forecast_bp
from backend.data_centers.routes import data_centers_bp


def social_or_login_required(view):
    """Like @login_required, but lets social-media crawlers (LinkedIn, X,
    Slack, Substack, etc.) fetch the page unauthenticated so they can read
    the OG meta tags. Human visitors without a session still get redirected
    to the login screen.
    """
    @wraps(view)
    def wrapper(*args, **kwargs):
        if is_social_crawler(request.headers.get('User-Agent', '')):
            return view(*args, **kwargs)
        return login_required(view)(*args, **kwargs)
    return wrapper


def create_app():
    app = Flask(__name__,
                static_folder='static',
                template_folder='templates')
    app.config.from_object(Config)
    CORS(app, origins=[os.environ.get('CORS_ORIGIN', '*')])

    # ── Flask-Login ──────────────────────────────────────────────────────
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.user_loader(user_loader)
    # Bearer-token auth for /api/v1/* (and any other API-key consumer).
    # Returning None here falls through to cookie-session auth.
    login_manager.request_loader(request_loader)

    @login_manager.unauthorized_handler
    def unauthorized():
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Authentication required', 'login_url': '/auth/login'}), 401
        return redirect(url_for('auth.login', next=request.url))

    # Inject admin_email into all templates
    @app.context_processor
    def inject_admin():
        return {'admin_email': ADMIN_EMAIL}

    # Inject Open Graph metadata for the current URL so every template
    # renders a shareable preview (LinkedIn, X, Substack, Slack, etc.)
    @app.context_processor
    def inject_og():
        return meta_for_path(request.path)

    # ── Blueprints ───────────────────────────────────────────────────────
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(api_v1_bp, url_prefix='/api/v1')
    app.register_blueprint(economist_bp, url_prefix='/api')
    app.register_blueprint(macro_model_bp, url_prefix='/api/macro-model/us')
    app.register_blueprint(house_prices_bp, url_prefix='/api/house-prices')
    app.register_blueprint(hpi_forecast_bp, url_prefix='/api/house-prices/forecast')
    app.register_blueprint(data_centers_bp, url_prefix='/api/data-centers')
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(sharing_bp)  # /og/* routes (public, no auth)

    # ── Routes ───────────────────────────────────────────────────────────
    @app.route('/')
    def home():
        return render_template('home.html', active_page='home')

    @app.route('/georisk')
    @social_or_login_required
    def georisk():
        return render_template('georisk.html', active_page='georisk')

    @app.route('/models')
    def models():
        return render_template('models.html', active_page='models')

    @app.route('/about')
    def about():
        return render_template('about.html', active_page='about')

    @app.route('/data')
    @app.route('/data/<path:subpath>')
    @social_or_login_required
    def data(subpath=None):
        has_insurance = current_user.has_insurance_access() if current_user.is_authenticated else False
        return render_template('data.html', active_page='data',
                               is_authenticated=current_user.is_authenticated,
                               has_insurance_access=has_insurance)

    @app.route('/research')
    def research():
        return render_template('research.html', active_page='research')

    @app.route('/economist')
    @login_required
    def economist():
        return render_template('economist.html', active_page='economist')

    @app.route('/macro-model')
    @macro_access_required
    def macro_model():
        return render_template('macro_model.html', active_page='macro-model')

    @app.route('/house-prices')
    @hpi_access_required
    def house_prices():
        return render_template('house_prices.html', active_page='house-prices')

    @app.route('/data-centers')
    @social_or_login_required
    def data_centers():
        return render_template('data_centers.html', active_page='data-centers')

    # ── Init ─────────────────────────────────────────────────────────────
    init_auth_db()
    init_scheduler(app)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=5000)
