import os
from functools import wraps
from flask import Flask, render_template, redirect, url_for, request, jsonify
from flask_cors import CORS
from flask_login import LoginManager, current_user, login_required
from config import Config
from backend.routes import api_bp
from backend.economist import economist_bp
from backend.auth import auth_bp, user_loader, init_auth_db, ADMIN_EMAIL
from backend.scheduler import init_scheduler
from backend.sharing import sharing_bp, meta_for_path, is_social_crawler


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
    app.register_blueprint(economist_bp, url_prefix='/api')
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

    # ── Init ─────────────────────────────────────────────────────────────
    init_auth_db()
    init_scheduler(app)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=5000)
