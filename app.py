from flask import Flask, render_template
from flask_cors import CORS
from config import Config
from backend.routes import api_bp
from backend.scheduler import init_scheduler


def create_app():
    app = Flask(__name__,
                static_folder='static',
                template_folder='templates')
    app.config.from_object(Config)
    CORS(app)
    app.register_blueprint(api_bp, url_prefix='/api')

    @app.route('/')
    def index():
        return render_template('index.html')

    init_scheduler(app)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=5000)
