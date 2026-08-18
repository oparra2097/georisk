"""Data access for the rent portal.

Thin functions over sqlite3 that return plain dicts, so the route layer can
`jsonify` them directly. Nothing here formats money for display — that is the
template/JS layer's job; everything in and out is integer cents.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta

from werkzeug.security import check_password_hash, generate_password_hash

from backend.rent.db import get_db

INVITE_TTL_HOURS = 72


def _dict(row):
    return dict(row) if row is not None else None


def _dicts(rows):
    return [dict(r) for r in rows]


# ── Properties ───────────────────────────────────────────────────────────

def list_properties(include_stats: bool = True) -> list[dict]:
    conn = get_db()
    try:
        props = _dicts(conn.execute(
            'SELECT * FROM properties ORDER BY name').fetchall())
        if not include_stats:
            return props
        for prop in props:
            stats = conn.execute("""
                SELECT COUNT(*) AS units,
                       SUM(CASE WHEN status = 'occupied' THEN 1 ELSE 0 END) AS occupied,
                       SUM(CASE WHEN status = 'vacant'   THEN 1 ELSE 0 END) AS vacant,
                       SUM(CASE WHEN status = 'offline'  THEN 1 ELSE 0 END) AS offline
                FROM units WHERE property_id = ?""", (prop['id'],)).fetchone()
            prop['unit_count'] = stats['units'] or 0
            prop['occupied'] = stats['occupied'] or 0
            prop['vacant'] = stats['vacant'] or 0
            prop['offline'] = stats['offline'] or 0
            scheduled = conn.execute("""
                SELECT COALESCE(SUM(l.rent_cents), 0) AS rent
                FROM leases l JOIN units u ON u.id = l.unit_id
                WHERE u.property_id = ? AND l.status = 'active'""",
                (prop['id'],)).fetchone()
            prop['scheduled_rent_cents'] = scheduled['rent'] or 0
        return props
    finally:
        conn.close()


def get_property(prop_id: int) -> dict | None:
    conn = get_db()
    try:
        return _dict(conn.execute(
            'SELECT * FROM properties WHERE id = ?', (prop_id,)).fetchone())
    finally:
        conn.close()


def get_property_by_slug(slug: str) -> dict | None:
    conn = get_db()
    try:
        return _dict(conn.execute(
            'SELECT * FROM properties WHERE slug = ?', (slug,)).fetchone())
    finally:
        conn.close()


def create_property(slug: str, name: str, **fields) -> int:
    conn = get_db()
    try:
        cur = conn.execute(
            'INSERT INTO properties (slug, name, address_line1, city, state, '
            'postal_code, amenities, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (slug, name, fields.get('address_line1', ''), fields.get('city', ''),
             fields.get('state', ''), fields.get('postal_code', ''),
             fields.get('amenities', ''), fields.get('notes', '')))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_property(prop_id: int, **fields) -> None:
    allowed = ('name', 'address_line1', 'city', 'state', 'postal_code',
               'amenities', 'notes', 'active')
    _update('properties', prop_id, allowed, fields)


# ── Units ────────────────────────────────────────────────────────────────

def list_units(property_id: int | None = None, status: str | None = None) -> list[dict]:
    sql = """
        SELECT u.*, p.name AS property_name, p.slug AS property_slug,
               l.id AS lease_id, l.rent_cents AS lease_rent_cents,
               t.full_name AS tenant_name, t.email AS tenant_email
        FROM units u
        JOIN properties p ON p.id = u.property_id
        LEFT JOIN leases l  ON l.unit_id = u.id AND l.status = 'active'
        LEFT JOIN tenants t ON t.id = l.tenant_id
    """
    where, params = [], []
    if property_id:
        where.append('u.property_id = ?')
        params.append(property_id)
    if status:
        where.append('u.status = ?')
        params.append(status)
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    # Numeric-aware ordering so 101 < 1010 < 102 does not happen.
    sql += ' ORDER BY p.name, LENGTH(u.label), u.label'
    conn = get_db()
    try:
        return _dicts(conn.execute(sql, params).fetchall())
    finally:
        conn.close()


def get_unit(unit_id: int) -> dict | None:
    conn = get_db()
    try:
        return _dict(conn.execute("""
            SELECT u.*, p.name AS property_name, p.slug AS property_slug,
                   p.amenities AS property_amenities
            FROM units u JOIN properties p ON p.id = u.property_id
            WHERE u.id = ?""", (unit_id,)).fetchone())
    finally:
        conn.close()


def create_unit(property_id: int, label: str, bedrooms: int = 2,
                bathrooms: float = 2.0, sqft: int | None = None,
                market_rent_cents: int = 0, status: str = 'vacant') -> int:
    conn = get_db()
    try:
        cur = conn.execute(
            'INSERT INTO units (property_id, label, bedrooms, bathrooms, sqft, '
            'market_rent_cents, status) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (property_id, label, bedrooms, bathrooms, sqft, market_rent_cents, status))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_unit(unit_id: int, **fields) -> None:
    allowed = ('label', 'bedrooms', 'bathrooms', 'sqft', 'market_rent_cents',
               'status', 'notes')
    _update('units', unit_id, allowed, fields)


def set_unit_status(unit_id: int, status: str) -> None:
    update_unit(unit_id, status=status)


# ── Tenants ──────────────────────────────────────────────────────────────

def list_tenants() -> list[dict]:
    conn = get_db()
    try:
        return _dicts(conn.execute("""
            SELECT t.id, t.email, t.full_name, t.phone, t.active, t.last_login,
                   t.created_at, (t.password_hash IS NOT NULL) AS activated,
                   l.id AS lease_id, u.label AS unit_label, p.name AS property_name
            FROM tenants t
            LEFT JOIN leases l     ON l.tenant_id = t.id AND l.status = 'active'
            LEFT JOIN units u      ON u.id = l.unit_id
            LEFT JOIN properties p ON p.id = u.property_id
            ORDER BY t.full_name""").fetchall())
    finally:
        conn.close()


def get_tenant(tenant_id: int) -> dict | None:
    conn = get_db()
    try:
        return _dict(conn.execute(
            'SELECT * FROM tenants WHERE id = ?', (tenant_id,)).fetchone())
    finally:
        conn.close()


def get_tenant_by_email(email: str) -> dict | None:
    conn = get_db()
    try:
        return _dict(conn.execute(
            'SELECT * FROM tenants WHERE email = ?',
            (email.strip().lower(),)).fetchone())
    finally:
        conn.close()


def create_tenant(email: str, full_name: str, phone: str = '') -> dict:
    """Create a tenant in the invited (not yet activated) state."""
    conn = get_db()
    try:
        cur = conn.execute(
            'INSERT INTO tenants (email, full_name, phone) VALUES (?, ?, ?)',
            (email.strip().lower(), full_name.strip(), phone.strip()))
        conn.commit()
        tenant_id = cur.lastrowid
    finally:
        conn.close()
    token = issue_invite(tenant_id)
    tenant = get_tenant(tenant_id)
    tenant['invite_token'] = token
    return tenant


def update_tenant(tenant_id: int, **fields) -> None:
    allowed = ('full_name', 'phone', 'email', 'active')
    _update('tenants', tenant_id, allowed, fields)


def issue_invite(tenant_id: int) -> str:
    """Mint a single-use activation / password-reset token."""
    token = secrets.token_urlsafe(32)
    expires = (datetime.utcnow() + timedelta(hours=INVITE_TTL_HOURS)).isoformat()
    conn = get_db()
    try:
        conn.execute('UPDATE tenants SET invite_token = ?, token_expires = ? '
                     'WHERE id = ?', (token, expires, tenant_id))
        conn.commit()
    finally:
        conn.close()
    return token


def tenant_by_invite(token: str) -> dict | None:
    if not token:
        return None
    conn = get_db()
    try:
        row = _dict(conn.execute(
            'SELECT * FROM tenants WHERE invite_token = ?', (token,)).fetchone())
    finally:
        conn.close()
    if not row:
        return None
    try:
        if datetime.fromisoformat(row['token_expires']) < datetime.utcnow():
            return None
    except (TypeError, ValueError):
        return None
    return row


def set_tenant_password(tenant_id: int, password: str) -> None:
    conn = get_db()
    try:
        conn.execute(
            'UPDATE tenants SET password_hash = ?, invite_token = NULL, '
            'token_expires = NULL WHERE id = ?',
            (generate_password_hash(password), tenant_id))
        conn.commit()
    finally:
        conn.close()


def verify_tenant_password(tenant: dict, password: str) -> bool:
    return bool(tenant and tenant.get('password_hash')
                and check_password_hash(tenant['password_hash'], password))


def touch_login(tenant_id: int) -> None:
    conn = get_db()
    try:
        conn.execute('UPDATE tenants SET last_login = CURRENT_TIMESTAMP '
                     'WHERE id = ?', (tenant_id,))
        conn.commit()
    finally:
        conn.close()


# ── Leases ───────────────────────────────────────────────────────────────

LEASE_SELECT = """
    SELECT l.*, u.label AS unit_label, u.bedrooms, u.bathrooms, u.sqft,
           p.id AS property_id, p.name AS property_name, p.slug AS property_slug,
           p.amenities AS property_amenities,
           t.full_name AS tenant_name, t.email AS tenant_email, t.phone AS tenant_phone
    FROM leases l
    JOIN units u       ON u.id = l.unit_id
    JOIN properties p  ON p.id = u.property_id
    JOIN tenants t     ON t.id = l.tenant_id
"""


def list_leases(status: str | None = 'active', property_id: int | None = None) -> list[dict]:
    sql = LEASE_SELECT
    where, params = [], []
    if status:
        where.append('l.status = ?')
        params.append(status)
    if property_id:
        where.append('p.id = ?')
        params.append(property_id)
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    sql += ' ORDER BY p.name, LENGTH(u.label), u.label'
    conn = get_db()
    try:
        return _dicts(conn.execute(sql, params).fetchall())
    finally:
        conn.close()


def get_lease(lease_id: int) -> dict | None:
    conn = get_db()
    try:
        return _dict(conn.execute(
            LEASE_SELECT + ' WHERE l.id = ?', (lease_id,)).fetchone())
    finally:
        conn.close()


def leases_for_tenant(tenant_id: int, status: str | None = None) -> list[dict]:
    sql = LEASE_SELECT + ' WHERE l.tenant_id = ?'
    params = [tenant_id]
    if status:
        sql += ' AND l.status = ?'
        params.append(status)
    sql += ' ORDER BY l.start_date DESC'
    conn = get_db()
    try:
        return _dicts(conn.execute(sql, params).fetchall())
    finally:
        conn.close()


def active_lease_for_tenant(tenant_id: int) -> dict | None:
    leases = leases_for_tenant(tenant_id, status='active')
    return leases[0] if leases else None


def create_lease(unit_id: int, tenant_id: int, start_date: str, rent_cents: int,
                 **fields) -> int:
    conn = get_db()
    try:
        cur = conn.execute("""
            INSERT INTO leases (unit_id, tenant_id, start_date, end_date, rent_cents,
                                deposit_cents, due_day, grace_days, late_fee_cents,
                                late_fee_daily_cents, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (unit_id, tenant_id, start_date, fields.get('end_date'), rent_cents,
             fields.get('deposit_cents', 0), fields.get('due_day', 1),
             fields.get('grace_days', 5), fields.get('late_fee_cents', 5000),
             fields.get('late_fee_daily_cents', 0), fields.get('status', 'active')))
        lease_id = cur.lastrowid
        if fields.get('status', 'active') == 'active':
            conn.execute("UPDATE units SET status = 'occupied' WHERE id = ?", (unit_id,))
        conn.commit()
        return lease_id
    finally:
        conn.close()


def update_lease(lease_id: int, **fields) -> None:
    allowed = ('start_date', 'end_date', 'rent_cents', 'deposit_cents', 'due_day',
               'grace_days', 'late_fee_cents', 'late_fee_daily_cents', 'status')
    _update('leases', lease_id, allowed, fields)


def end_lease(lease_id: int, end_date: str) -> None:
    conn = get_db()
    try:
        row = conn.execute('SELECT unit_id FROM leases WHERE id = ?',
                           (lease_id,)).fetchone()
        conn.execute("UPDATE leases SET status = 'ended', end_date = ? WHERE id = ?",
                     (end_date, lease_id))
        if row:
            conn.execute("UPDATE units SET status = 'vacant' WHERE id = ?",
                         (row['unit_id'],))
        conn.commit()
    finally:
        conn.close()


# ── Ledger: charges + payments ───────────────────────────────────────────

def add_charge(lease_id: int, kind: str, amount_cents: int, due_date: str,
               period: str | None = None, description: str = '') -> int | None:
    """Insert a charge. Returns None if an identical recurring charge exists."""
    conn = get_db()
    try:
        # The uniqueness index is partial (recurring charges only), so the
        # conflict target has to repeat its WHERE clause. One-off charges
        # (period IS NULL) fall outside the index and always insert.
        cur = conn.execute(
            'INSERT INTO charges (lease_id, kind, period, description, amount_cents, '
            'due_date) VALUES (?, ?, ?, ?, ?, ?) '
            'ON CONFLICT (lease_id, kind, period) WHERE period IS NOT NULL DO NOTHING',
            (lease_id, kind, period, description, amount_cents, due_date))
        conn.commit()
        return cur.lastrowid if cur.rowcount else None
    finally:
        conn.close()


def list_charges(lease_id: int) -> list[dict]:
    conn = get_db()
    try:
        return _dicts(conn.execute(
            'SELECT * FROM charges WHERE lease_id = ? ORDER BY due_date, id',
            (lease_id,)).fetchall())
    finally:
        conn.close()


def delete_charge(charge_id: int) -> None:
    conn = get_db()
    try:
        conn.execute('DELETE FROM charges WHERE id = ?', (charge_id,))
        conn.commit()
    finally:
        conn.close()


def create_payment(lease_id: int, tenant_id: int, amount_cents: int,
                   method: str = 'card', status: str = 'pending',
                   fee_cents: int = 0, stripe_session_id: str | None = None,
                   memo: str = '') -> int:
    conn = get_db()
    try:
        cur = conn.execute("""
            INSERT INTO payments (lease_id, tenant_id, amount_cents, fee_cents, method,
                                  status, stripe_session_id, memo, paid_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (lease_id, tenant_id, amount_cents, fee_cents, method, status,
             stripe_session_id, memo,
             datetime.utcnow().isoformat(timespec='seconds') if status == 'succeeded' else None))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_payment(payment_id: int, **fields) -> None:
    allowed = ('status', 'amount_cents', 'fee_cents', 'method', 'stripe_payment_intent',
               'receipt_url', 'failure_reason', 'paid_at', 'memo')
    _update('payments', payment_id, allowed, fields)


def mark_payment_succeeded(payment_id: int, **fields) -> None:
    fields.setdefault('paid_at', datetime.utcnow().isoformat(timespec='seconds'))
    fields['status'] = 'succeeded'
    update_payment(payment_id, **fields)


def get_payment(payment_id: int) -> dict | None:
    conn = get_db()
    try:
        return _dict(conn.execute(
            'SELECT * FROM payments WHERE id = ?', (payment_id,)).fetchone())
    finally:
        conn.close()


def get_payment_by_session(session_id: str) -> dict | None:
    conn = get_db()
    try:
        return _dict(conn.execute(
            'SELECT * FROM payments WHERE stripe_session_id = ?',
            (session_id,)).fetchone())
    finally:
        conn.close()


def list_payments(lease_id: int | None = None, limit: int = 200) -> list[dict]:
    sql = """
        SELECT pay.*, t.full_name AS tenant_name, t.email AS tenant_email,
               u.label AS unit_label, p.name AS property_name
        FROM payments pay
        JOIN tenants t     ON t.id = pay.tenant_id
        JOIN leases l      ON l.id = pay.lease_id
        JOIN units u       ON u.id = l.unit_id
        JOIN properties p  ON p.id = u.property_id
    """
    params = []
    if lease_id:
        sql += ' WHERE pay.lease_id = ?'
        params.append(lease_id)
    sql += ' ORDER BY COALESCE(pay.paid_at, pay.created_at) DESC, pay.id DESC LIMIT ?'
    params.append(limit)
    conn = get_db()
    try:
        return _dicts(conn.execute(sql, params).fetchall())
    finally:
        conn.close()


def event_seen(event_id: str, event_type: str) -> bool:
    """Record a Stripe event id; returns True if it had already been handled."""
    conn = get_db()
    try:
        cur = conn.execute(
            'INSERT INTO stripe_events (id, type) VALUES (?, ?) '
            'ON CONFLICT(id) DO NOTHING', (event_id, event_type))
        conn.commit()
        return cur.rowcount == 0
    finally:
        conn.close()


# ── internal ─────────────────────────────────────────────────────────────

def _update(table: str, row_id: int, allowed: tuple, fields: dict) -> None:
    sets, params = [], []
    for key, value in fields.items():
        if key in allowed:
            sets.append(f'{key} = ?')
            params.append(value)
    if not sets:
        return
    params.append(row_id)
    conn = get_db()
    try:
        conn.execute(f'UPDATE {table} SET {", ".join(sets)} WHERE id = ?', params)
        conn.commit()
    finally:
        conn.close()
