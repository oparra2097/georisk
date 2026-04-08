"""
Authentication system using Flask-Login + SQLite.

User model backed by data/users.db (separate from georisk.db).
Password hashing via werkzeug.security.
"""

import sqlite3
import logging
from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required
from werkzeug.security import generate_password_hash, check_password_hash
from config import Config

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__, template_folder='../templates')

# ── Database ─────────────────────────────────────────────────────────────

_DB_PATH = Config.DATA_DIR + '/users.db'


def _get_db():
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


def init_auth_db():
    """Create users table if it doesn't exist."""
    import os
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    conn = _get_db()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Auth DB initialized")


# ── User model ───────────────────────────────────────────────────────────

class User(UserMixin):
    def __init__(self, id, email, password_hash):
        self.id = id
        self.email = email
        self.password_hash = password_hash

    @staticmethod
    def get_by_id(user_id):
        conn = _get_db()
        row = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
        conn.close()
        if row:
            return User(row['id'], row['email'], row['password_hash'])
        return None

    @staticmethod
    def get_by_email(email):
        conn = _get_db()
        row = conn.execute('SELECT * FROM users WHERE email = ?', (email,)).fetchone()
        conn.close()
        if row:
            return User(row['id'], row['email'], row['password_hash'])
        return None

    @staticmethod
    def create(email, password):
        pw_hash = generate_password_hash(password)
        conn = _get_db()
        try:
            conn.execute('INSERT INTO users (email, password_hash) VALUES (?, ?)', (email, pw_hash))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


def user_loader(user_id):
    """Flask-Login user_loader callback."""
    return User.get_by_id(int(user_id))


# ── Routes ───────────────────────────────────────────────────────────────

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')

        user = User.get_by_email(email)
        if user and user.check_password(password):
            login_user(user, remember=True)
            next_page = request.args.get('next', '/data')
            return redirect(next_page)

        flash('Invalid email or password.', 'error')

    return render_template('login.html', active_page='data')


@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        confirm = request.form.get('confirm', '')

        if not email or not password:
            flash('Email and password are required.', 'error')
        elif password != confirm:
            flash('Passwords do not match.', 'error')
        elif len(password) < 8:
            flash('Password must be at least 8 characters.', 'error')
        elif User.create(email, password):
            flash('Account created. Please log in.', 'success')
            return redirect(url_for('auth.login'))
        else:
            flash('An account with that email already exists.', 'error')

    return render_template('register.html', active_page='data')


@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect('/')
