"""Session handling for the rent portal.

Two audiences, two gates:

* **Residents** get their own cookie-session key (`rent_tenant_id`). They are
  intentionally *not* Flask-Login users — the macro site's user table is for
  research subscribers, and mixing a tenant roster into it would let a rent
  login inherit research entitlements.
* **Managers** are Flask-Login users whose email is the site admin or appears
  in RENT_MANAGERS (comma-separated env var).
"""

from __future__ import annotations

import os
from functools import wraps

from flask import jsonify, redirect, request, session, url_for
from flask_login import current_user

from backend.rent import models

TENANT_SESSION_KEY = 'rent_tenant_id'


def manager_emails() -> set[str]:
    raw = os.environ.get('RENT_MANAGERS', '')
    emails = {e.strip().lower() for e in raw.split(',') if e.strip()}
    admin = (os.environ.get('ADMIN_EMAIL') or '').strip().lower()
    if admin:
        emails.add(admin)
    return emails


def is_manager() -> bool:
    return bool(current_user and current_user.is_authenticated
                and (current_user.email or '').strip().lower() in manager_emails())


# ── Tenant session ───────────────────────────────────────────────────────

def login_tenant(tenant_id: int) -> None:
    session[TENANT_SESSION_KEY] = int(tenant_id)
    session.permanent = True
    models.touch_login(tenant_id)


def logout_tenant() -> None:
    session.pop(TENANT_SESSION_KEY, None)


def current_tenant() -> dict | None:
    tenant_id = session.get(TENANT_SESSION_KEY)
    if not tenant_id:
        return None
    tenant = models.get_tenant(int(tenant_id))
    if not tenant or not tenant['active']:
        session.pop(TENANT_SESSION_KEY, None)
        return None
    return tenant


def _unauthorized(login_endpoint: str):
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Authentication required',
                        'login_url': url_for(login_endpoint)}), 401
    return redirect(url_for(login_endpoint, next=request.url))


def tenant_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if current_tenant() is None:
            return _unauthorized('rent_pages.login')
        return view(*args, **kwargs)
    return wrapper


def manager_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if not is_manager():
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Manager access required'}), 403
            if not (current_user and current_user.is_authenticated):
                return redirect(url_for('auth.login', next=request.url))
            return redirect(url_for('rent_pages.no_access'))
        return view(*args, **kwargs)
    return wrapper


def lease_belongs_to_tenant(lease: dict, tenant: dict) -> bool:
    return bool(lease and tenant and lease['tenant_id'] == tenant['id'])
