import os
from functools import wraps
from flask import Flask, render_template, redirect, url_for, request, jsonify
from flask_cors import CORS
from flask_login import LoginManager, current_user, login_required
from config import Config


def macro_access_required(f):
    """Decorator: requires verified email + admin-granted Macro Model access.

    Returns HTML for page routes (via Flask-Login's unauthorized handler on the
    login step; 403 page here for the access-denied step), and JSON for /api/*.
    """
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
                                   active_page='macro-model'), 403
        if not current_user.has_macro_access():
            if request.path.startswith('/api/'):
                return jsonify({
                    'error': 'US Macro Model access is granted by the admin. Contact support.',
                }), 403
            return render_template('access_denied.html',
                                   reason='Macro Model access is granted by the admin. Contact support to request access.',
                                   active_page='macro-model'), 403
        return f(*args, **kwargs)
    return decorated
from backend.routes import api_bp
from backend.economist import economist_bp
from backend.auth import auth_bp, user_loader, init_auth_db, ADMIN_EMAIL
from backend.scheduler import init_scheduler
from backend.macro_model.routes import macro_model_bp


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

    @login_manager.unauthorized_handler
    def unauthorized():
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Authentication required', 'login_url': '/auth/login'}), 401
        return redirect(url_for('auth.login', next=request.url))

    # Inject admin_email into all templates
    @app.context_processor
    def inject_admin():
        return {'admin_email': ADMIN_EMAIL}

    # ── Blueprints ───────────────────────────────────────────────────────
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(economist_bp, url_prefix='/api')
    app.register_blueprint(macro_model_bp, url_prefix='/api/macro-model/us')
    app.register_blueprint(auth_bp, url_prefix='/auth')

    # ── Routes ───────────────────────────────────────────────────────────
    @app.route('/')
    def home():
        return render_template('home.html', active_page='home')

    @app.route('/georisk')
    @login_required
    def georisk():
        return render_template('georisk.html', active_page='georisk')

    @app.route('/about')
    def about():
        return render_template('about.html', active_page='about')

    @app.route('/data')
    @app.route('/data/<path:subpath>')
    @login_required
    def data(subpath=None):
        has_insurance = current_user.has_insurance_access() if current_user.is_authenticated else False
        return render_template('data.html', active_page='data',
                               is_authenticated=True,
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

    # ── Init ─────────────────────────────────────────────────────────────
    init_auth_db()
    init_scheduler(app)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=5000)
