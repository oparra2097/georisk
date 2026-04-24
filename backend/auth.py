"""
Authentication system using Flask-Login + SQLite.

Two-tier access:
  - Anyone can register and log in (public site)
  - Insurance inflation data requires verified @aig.com email OR admin grant

Email verification via Gmail SMTP (App Password).
Admin dashboard at /auth/admin for user management.
"""

import os
import secrets
import smtplib
import sqlite3
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from config import Config

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__, template_folder='../templates')

# ── Database ─────────────────────────────────────────────────────────────

_DB_PATH = Config.DATA_DIR + '/users.db'

ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', '').strip().lower()


def _get_db():
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


def _is_admin():
    return current_user.is_authenticated and current_user.email == ADMIN_EMAIL


def init_auth_db():
    """Create/migrate users table."""
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    conn = _get_db()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            email_verified BOOLEAN DEFAULT 0,
            verification_token TEXT,
            insurance_access BOOLEAN DEFAULT 0,
            macro_access BOOLEAN DEFAULT 0,
            last_login TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Migrate existing tables
    for col, definition in [
        ('email_verified', 'BOOLEAN DEFAULT 0'),
        ('verification_token', 'TEXT'),
        ('insurance_access', 'BOOLEAN DEFAULT 0'),
        ('macro_access', 'BOOLEAN DEFAULT 0'),
        ('last_login', 'TIMESTAMP'),
        ('reset_token', 'TEXT'),
    ]:
        try:
            conn.execute(f'ALTER TABLE users ADD COLUMN {col} {definition}')
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()
    logger.info("Auth DB initialized")


# ── User model ───────────────────────────────────────────────────────────

class User(UserMixin):
    def __init__(self, id, email, password_hash, email_verified=False,
                 insurance_access=False, macro_access=False,
                 last_login=None, created_at=None):
        self.id = id
        self.email = email
        self.password_hash = password_hash
        self.email_verified = bool(email_verified)
        self.insurance_access = bool(insurance_access)
        self.macro_access = bool(macro_access)
        self.last_login = last_login
        self.created_at = created_at

    @staticmethod
    def _from_row(row):
        if not row:
            return None
        # Columns added by migration may not exist on older rows — guard with .keys()
        keys = row.keys() if hasattr(row, 'keys') else []
        return User(
            row['id'], row['email'], row['password_hash'],
            row['email_verified'], row['insurance_access'],
            row['macro_access'] if 'macro_access' in keys else False,
            row['last_login'] if 'last_login' in keys else None,
            row['created_at'] if 'created_at' in keys else None,
        )

    @staticmethod
    def get_by_id(user_id):
        conn = _get_db()
        row = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
        conn.close()
        return User._from_row(row)

    @staticmethod
    def get_by_email(email):
        conn = _get_db()
        row = conn.execute('SELECT * FROM users WHERE email = ?', (email,)).fetchone()
        conn.close()
        return User._from_row(row)

    @staticmethod
    def get_all():
        conn = _get_db()
        rows = conn.execute('SELECT * FROM users ORDER BY created_at DESC').fetchall()
        conn.close()
        return [User._from_row(r) for r in rows]

    @staticmethod
    def create(email, password):
        pw_hash = generate_password_hash(password)
        token = secrets.token_urlsafe(32)
        conn = _get_db()
        try:
            conn.execute(
                'INSERT INTO users (email, password_hash, verification_token) VALUES (?, ?, ?)',
                (email, pw_hash, token)
            )
            conn.commit()
            return token
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()

    @staticmethod
    def verify_token(token):
        conn = _get_db()
        row = conn.execute('SELECT * FROM users WHERE verification_token = ?', (token,)).fetchone()
        if row:
            conn.execute('UPDATE users SET email_verified = 1, verification_token = NULL WHERE id = ?', (row['id'],))
            conn.commit()
            conn.close()
            return row['email']
        conn.close()
        return None

    @staticmethod
    def set_verified(user_id, verified=True):
        conn = _get_db()
        conn.execute('UPDATE users SET email_verified = ? WHERE id = ?', (1 if verified else 0, user_id))
        conn.commit()
        conn.close()

    @staticmethod
    def set_insurance_access(user_id, access=True):
        conn = _get_db()
        conn.execute('UPDATE users SET insurance_access = ? WHERE id = ?', (1 if access else 0, user_id))
        conn.commit()
        conn.close()

    @staticmethod
    def set_macro_access(user_id, access=True):
        conn = _get_db()
        conn.execute('UPDATE users SET macro_access = ? WHERE id = ?', (1 if access else 0, user_id))
        conn.commit()
        conn.close()

    @staticmethod
    def grant_access(email):
        conn = _get_db()
        conn.execute('UPDATE users SET insurance_access = 1 WHERE email = ?', (email,))
        conn.commit()
        affected = conn.total_changes
        conn.close()
        return affected > 0

    @staticmethod
    def revoke_access(email):
        conn = _get_db()
        conn.execute('UPDATE users SET insurance_access = 0 WHERE email = ?', (email,))
        conn.commit()
        conn.close()

    @staticmethod
    def delete_by_id(user_id):
        conn = _get_db()
        conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
        conn.commit()
        conn.close()

    @staticmethod
    def update_last_login(user_id):
        conn = _get_db()
        conn.execute('UPDATE users SET last_login = ? WHERE id = ?', (datetime.utcnow().isoformat(), user_id))
        conn.commit()
        conn.close()

    @staticmethod
    def get_verification_token(user_id):
        """Get or regenerate verification token for resending."""
        conn = _get_db()
        row = conn.execute('SELECT verification_token, email FROM users WHERE id = ?', (user_id,)).fetchone()
        if not row:
            conn.close()
            return None, None
        token = row['verification_token']
        if not token:
            token = secrets.token_urlsafe(32)
            conn.execute('UPDATE users SET verification_token = ?, email_verified = 0 WHERE id = ?', (token, user_id))
            conn.commit()
        conn.close()
        return row['email'], token

    @staticmethod
    def set_reset_token(email):
        """Generate and store a password reset token. Returns token or None."""
        conn = _get_db()
        row = conn.execute('SELECT id FROM users WHERE email = ?', (email,)).fetchone()
        if not row:
            conn.close()
            return None
        token = secrets.token_urlsafe(32)
        conn.execute('UPDATE users SET reset_token = ? WHERE id = ?', (token, row['id']))
        conn.commit()
        conn.close()
        return token

    @staticmethod
    def reset_password(token, new_password):
        """Reset password using token. Returns email or None."""
        conn = _get_db()
        row = conn.execute('SELECT id, email FROM users WHERE reset_token = ?', (token,)).fetchone()
        if not row:
            conn.close()
            return None
        pw_hash = generate_password_hash(new_password)
        conn.execute('UPDATE users SET password_hash = ?, reset_token = NULL WHERE id = ?', (pw_hash, row['id']))
        conn.commit()
        conn.close()
        return row['email']

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def has_insurance_access(self):
        if not self.email_verified:
            return False
        if self.email.endswith('@aig.com'):
            return True
        return bool(self.insurance_access)

    def has_macro_access(self):
        """Access to the US Macro Model product. Admin and whitelisted users only."""
        if not self.email_verified:
            return False
        if self.email == ADMIN_EMAIL:
            return True
        return bool(self.macro_access)


def user_loader(user_id):
    return User.get_by_id(int(user_id))


# ── Email verification ───────────────────────────────────────────────────

def _send_verification_email(email, token):
    """Send verification email via Gmail SMTP."""
    smtp_email = Config.SMTP_EMAIL
    smtp_password = Config.SMTP_PASSWORD

    if not smtp_email or not smtp_password:
        logger.warning("SMTP not configured — skipping verification email")
        return False

    base_url = os.environ.get('BASE_URL', 'https://parramacro.com')
    verify_url = f'{base_url}/auth/verify/{token}'

    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Verify your Parra Macro account'
    msg['From'] = f'Parra Macro <{smtp_email}>'
    msg['To'] = email

    text = f"Welcome to Parra Macro.\n\nPlease verify your email:\n{verify_url}\n\nIf you did not create this account, ignore this email."

    html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 500px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #1e293b;">Welcome to Parra Macro</h2>
        <p style="color: #475569;">Please verify your email to activate your account:</p>
        <a href="{verify_url}" style="display: inline-block; padding: 12px 24px; background: #3b82f6; color: white; text-decoration: none; border-radius: 6px; font-weight: 600;">Verify Email</a>
        <p style="color: #94a3b8; font-size: 13px; margin-top: 24px;">If you did not create this account, you can safely ignore this email.</p>
    </div>
    """

    msg.attach(MIMEText(text, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    try:
        with smtplib.SMTP_SSL(Config.SMTP_SERVER, Config.SMTP_PORT) as server:
            server.login(smtp_email, smtp_password)
            server.sendmail(smtp_email, email, msg.as_string())
        logger.info(f"Verification email sent to {email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send verification email to {email}: {e}")
        return False


def _send_reset_email(email, token):
    """Send password reset email via Gmail SMTP."""
    smtp_email = Config.SMTP_EMAIL
    smtp_password = Config.SMTP_PASSWORD

    if not smtp_email or not smtp_password:
        logger.warning("SMTP not configured — skipping reset email")
        return False

    base_url = os.environ.get('BASE_URL', 'https://parramacro.com')
    reset_url = f'{base_url}/auth/reset-password/{token}'

    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Reset your Parra Macro password'
    msg['From'] = f'Parra Macro <{smtp_email}>'
    msg['To'] = email

    text = f"Reset your password by clicking this link:\n{reset_url}\n\nIf you did not request this, ignore this email."

    html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 500px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #1e293b;">Reset Your Password</h2>
        <p style="color: #475569;">Click the button below to set a new password:</p>
        <a href="{reset_url}" style="display: inline-block; padding: 12px 24px; background: #3b82f6; color: white; text-decoration: none; border-radius: 6px; font-weight: 600;">Reset Password</a>
        <p style="color: #94a3b8; font-size: 13px; margin-top: 24px;">If you did not request a password reset, you can safely ignore this email.</p>
    </div>
    """

    msg.attach(MIMEText(text, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    try:
        with smtplib.SMTP_SSL(Config.SMTP_SERVER, Config.SMTP_PORT) as server:
            server.login(smtp_email, smtp_password)
            server.sendmail(smtp_email, email, msg.as_string())
        logger.info(f"Password reset email sent to {email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send reset email to {email}: {e}")
        return False


# ── Routes ───────────────────────────────────────────────────────────────

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    try:
        if request.method == 'POST':
            email = request.form.get('email', '').strip().lower()
            password = request.form.get('password', '')

            user = User.get_by_email(email)
            if user and user.check_password(password):
                login_user(user, remember=True)
                User.update_last_login(user.id)
                if not user.email_verified:
                    flash('Please check your email and verify your account.', 'error')
                next_page = request.args.get('next', '/data')
                return redirect(next_page)

            flash('Invalid email or password.', 'error')

        return render_template('login.html', active_page='data')
    except Exception as e:
        logger.error(f"Login error: {e}", exc_info=True)
        return f"Login error: {e}", 500


@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    try:
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
            else:
                token = User.create(email, password)
                if token:
                    _send_verification_email(email, token)
                    flash('Account created! Check your email for a verification link.', 'success')
                    return redirect(url_for('auth.login'))
                else:
                    flash('An account with that email already exists.', 'error')

        return render_template('register.html', active_page='data')
    except Exception as e:
        logger.error(f"Register error: {e}", exc_info=True)
        return f"Register error: {e}", 500


@auth_bp.route('/verify/<token>')
def verify_email(token):
    email = User.verify_token(token)
    if email:
        return render_template('verify_success.html', email=email, active_page='data')
    else:
        flash('Invalid or expired verification link.', 'error')
        return redirect(url_for('auth.login'))


@auth_bp.route('/resend-verification', methods=['GET', 'POST'])
def resend_verification():
    """Self-service resend verification email."""
    try:
        if request.method == 'POST':
            email = request.form.get('email', '').strip().lower()
            if not email:
                flash('Please enter your email address.', 'error')
            else:
                user = User.get_by_email(email)
                if user and not user.email_verified:
                    _, token = User.get_verification_token(user.id)
                    if token:
                        _send_verification_email(email, token)
                # Always show success to prevent email enumeration
                flash('If an unverified account exists with that email, a verification link has been sent.', 'success')
                return redirect(url_for('auth.login'))

        return render_template('resend_verification.html', active_page='data')
    except Exception as e:
        logger.error(f"Resend verification error: {e}", exc_info=True)
        return f"Error: {e}", 500


@auth_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """Request a password reset link."""
    try:
        if request.method == 'POST':
            email = request.form.get('email', '').strip().lower()
            if not email:
                flash('Please enter your email address.', 'error')
            else:
                token = User.set_reset_token(email)
                if token:
                    _send_reset_email(email, token)
                # Always show success to prevent email enumeration
                flash('If an account exists with that email, a password reset link has been sent.', 'success')
                return redirect(url_for('auth.login'))

        return render_template('forgot_password.html', active_page='data')
    except Exception as e:
        logger.error(f"Forgot password error: {e}", exc_info=True)
        return f"Error: {e}", 500


@auth_bp.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    """Reset password using token from email."""
    try:
        if request.method == 'POST':
            password = request.form.get('password', '')
            confirm = request.form.get('confirm', '')

            if not password:
                flash('Password is required.', 'error')
            elif password != confirm:
                flash('Passwords do not match.', 'error')
            elif len(password) < 8:
                flash('Password must be at least 8 characters.', 'error')
            else:
                email = User.reset_password(token, password)
                if email:
                    flash('Password reset successfully. Please log in.', 'success')
                    return redirect(url_for('auth.login'))
                else:
                    flash('Invalid or expired reset link.', 'error')
                    return redirect(url_for('auth.forgot_password'))

        return render_template('reset_password.html', token=token, active_page='data')
    except Exception as e:
        logger.error(f"Reset password error: {e}", exc_info=True)
        return f"Error: {e}", 500


# ── Admin Dashboard ──────────────────────────────────────────────────────

@auth_bp.route('/admin')
@login_required
def admin_dashboard():
    if not _is_admin():
        return 'Unauthorized', 403
    users = User.get_all()
    stats = {
        'total': len(users),
        'verified': sum(1 for u in users if u.email_verified),
        'insurance': sum(1 for u in users if u.insurance_access or u.email.endswith('@aig.com')),
        'macro': sum(1 for u in users if u.has_macro_access()),
        'aig': sum(1 for u in users if u.email.endswith('@aig.com')),
    }
    return render_template('admin.html', users=users, stats=stats, active_page='data')


@auth_bp.route('/admin/toggle-access/<int:user_id>', methods=['POST'])
@login_required
def admin_toggle_access(user_id):
    if not _is_admin():
        return jsonify({'error': 'Unauthorized'}), 403
    user = User.get_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    User.set_insurance_access(user_id, not user.insurance_access)
    flash(f'Insurance access {"granted to" if not user.insurance_access else "revoked for"} {user.email}', 'success')
    return redirect(url_for('auth.admin_dashboard'))


@auth_bp.route('/admin/toggle-macro/<int:user_id>', methods=['POST'])
@login_required
def admin_toggle_macro(user_id):
    if not _is_admin():
        return jsonify({'error': 'Unauthorized'}), 403
    user = User.get_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    User.set_macro_access(user_id, not user.macro_access)
    flash(f'Macro Model access {"granted to" if not user.macro_access else "revoked for"} {user.email}', 'success')
    return redirect(url_for('auth.admin_dashboard'))


@auth_bp.route('/admin/toggle-verify/<int:user_id>', methods=['POST'])
@login_required
def admin_toggle_verify(user_id):
    if not _is_admin():
        return jsonify({'error': 'Unauthorized'}), 403
    user = User.get_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    User.set_verified(user_id, not user.email_verified)
    flash(f'Email {"verified" if not user.email_verified else "unverified"} for {user.email}', 'success')
    return redirect(url_for('auth.admin_dashboard'))


@auth_bp.route('/admin/delete/<int:user_id>', methods=['POST'])
@login_required
def admin_delete_user(user_id):
    if not _is_admin():
        return jsonify({'error': 'Unauthorized'}), 403
    user = User.get_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    if user.email == ADMIN_EMAIL:
        flash('Cannot delete admin account.', 'error')
        return redirect(url_for('auth.admin_dashboard'))
    User.delete_by_id(user_id)
    flash(f'Deleted user {user.email}', 'success')
    return redirect(url_for('auth.admin_dashboard'))


@auth_bp.route('/admin/resend/<int:user_id>', methods=['POST'])
@login_required
def admin_resend_verification(user_id):
    if not _is_admin():
        return jsonify({'error': 'Unauthorized'}), 403
    email, token = User.get_verification_token(user_id)
    if email and token:
        _send_verification_email(email, token)
        flash(f'Verification email resent to {email}', 'success')
    else:
        flash('User not found', 'error')
    return redirect(url_for('auth.admin_dashboard'))


@auth_bp.route('/admin/send-reset/<int:user_id>', methods=['POST'])
@login_required
def admin_send_reset(user_id):
    """Send password reset email to a user from admin dashboard."""
    if not _is_admin():
        return jsonify({'error': 'Unauthorized'}), 403
    user = User.get_by_id(user_id)
    if not user:
        flash('User not found', 'error')
        return redirect(url_for('auth.admin_dashboard'))
    token = User.set_reset_token(user.email)
    if token:
        _send_reset_email(user.email, token)
        flash(f'Password reset email sent to {user.email}', 'success')
    return redirect(url_for('auth.admin_dashboard'))


# Legacy URL-based grant/revoke (kept for backward compatibility)
@auth_bp.route('/admin/grant/<path:email>')
@login_required
def admin_grant_access(email):
    if not _is_admin():
        return 'Unauthorized', 403
    email = email.strip().lower()
    if User.grant_access(email):
        return f'Insurance access granted to {email}'
    return f'User {email} not found', 404


@auth_bp.route('/admin/revoke/<path:email>')
@login_required
def admin_revoke_access(email):
    if not _is_admin():
        return 'Unauthorized', 403
    email = email.strip().lower()
    User.revoke_access(email)
    return f'Insurance access revoked for {email}'


@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect('/')
