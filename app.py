import os
from flask import Flask, render_template, redirect, url_for, request, jsonify
from flask_cors import CORS
from flask_login import LoginManager, current_user, login_required
from config import Config
from backend.routes import api_bp
from backend.economist import economist_bp
from backend.auth import auth_bp, user_loader, init_auth_db, ADMIN_EMAIL
from backend.scheduler import init_scheduler


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
    def data(subpath=None):
        is_auth = current_user.is_authenticated if current_user else False
        return render_template('data.html', active_page='data', is_authenticated=is_auth)

    @app.route('/research')
    def research():
        return render_template('research.html', active_page='research')

    @app.route('/economist')
    def economist():
        return render_template('economist.html', active_page='economist')

    # ── Init ─────────────────────────────────────────────────────────────
    init_auth_db()
    init_scheduler(app)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=5000)
