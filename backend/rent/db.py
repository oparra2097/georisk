"""SQLite schema + connection handling for the rent portal.

Everything lives in `rent.db` alongside the other databases in DATA_DIR so it
rides the same Render persistent disk. Money is stored in integer cents —
never floats — and dates are ISO-8601 strings (`YYYY-MM-DD`).
"""

from __future__ import annotations

import logging
import os
import sqlite3

from config import Config

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(Config.DATA_DIR, 'rent.db')

SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    slug          TEXT UNIQUE NOT NULL,
    name          TEXT NOT NULL,
    address_line1 TEXT DEFAULT '',
    city          TEXT DEFAULT '',
    state         TEXT DEFAULT '',
    postal_code   TEXT DEFAULT '',
    amenities     TEXT DEFAULT '',      -- comma-separated, shown to tenants
    notes         TEXT DEFAULT '',
    active        INTEGER DEFAULT 1,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS units (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id       INTEGER NOT NULL,
    label             TEXT NOT NULL,     -- "101", "B-204", ...
    bedrooms          INTEGER NOT NULL DEFAULT 2,
    bathrooms         REAL NOT NULL DEFAULT 2,
    sqft              INTEGER,
    market_rent_cents INTEGER NOT NULL DEFAULT 0,
    status            TEXT NOT NULL DEFAULT 'vacant',  -- vacant|occupied|offline
    notes             TEXT DEFAULT '',
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (property_id, label),
    FOREIGN KEY (property_id) REFERENCES properties(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tenants (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    email         TEXT UNIQUE NOT NULL,
    full_name     TEXT NOT NULL,
    phone         TEXT DEFAULT '',
    password_hash TEXT,                  -- NULL until the invite is accepted
    invite_token  TEXT,
    token_expires TEXT,
    active        INTEGER DEFAULT 1,
    last_login    TIMESTAMP,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS leases (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    unit_id              INTEGER NOT NULL,
    tenant_id            INTEGER NOT NULL,
    start_date           TEXT NOT NULL,
    end_date             TEXT,
    rent_cents           INTEGER NOT NULL,
    deposit_cents        INTEGER NOT NULL DEFAULT 0,
    due_day              INTEGER NOT NULL DEFAULT 1,   -- day of month rent is due
    grace_days           INTEGER NOT NULL DEFAULT 5,
    late_fee_cents       INTEGER NOT NULL DEFAULT 5000,
    late_fee_daily_cents INTEGER NOT NULL DEFAULT 0,   -- optional per-diem add-on
    status               TEXT NOT NULL DEFAULT 'active',  -- active|ended|pending
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (unit_id)   REFERENCES units(id)   ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_leases_tenant ON leases (tenant_id);
CREATE INDEX IF NOT EXISTS idx_leases_unit   ON leases (unit_id);

-- One row per amount owed. `period` is 'YYYY-MM' for recurring items and NULL
-- for one-offs; the UNIQUE index makes monthly generation idempotent.
CREATE TABLE IF NOT EXISTS charges (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id     INTEGER NOT NULL,
    kind         TEXT NOT NULL,          -- rent|late_fee|deposit|utility|other|credit
    period       TEXT,
    description  TEXT DEFAULT '',
    amount_cents INTEGER NOT NULL,       -- negative for credits/concessions
    due_date     TEXT NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (lease_id) REFERENCES leases(id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_charges_recurring
    ON charges (lease_id, kind, period) WHERE period IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_charges_lease ON charges (lease_id);

CREATE TABLE IF NOT EXISTS payments (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id             INTEGER NOT NULL,
    tenant_id            INTEGER NOT NULL,
    amount_cents         INTEGER NOT NULL,   -- credited to the ledger
    fee_cents            INTEGER NOT NULL DEFAULT 0,  -- convenience fee, not credited
    method               TEXT NOT NULL DEFAULT 'card', -- card|us_bank_account|check|cash|other
    status               TEXT NOT NULL DEFAULT 'pending', -- pending|processing|succeeded|failed|refunded
    stripe_session_id    TEXT UNIQUE,
    stripe_payment_intent TEXT,
    receipt_url          TEXT,
    memo                 TEXT DEFAULT '',
    failure_reason       TEXT DEFAULT '',
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    paid_at              TIMESTAMP,
    FOREIGN KEY (lease_id)  REFERENCES leases(id)   ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)  ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_payments_lease ON payments (lease_id);
CREATE INDEX IF NOT EXISTS idx_payments_intent ON payments (stripe_payment_intent);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Raw Stripe events, so a replayed webhook is a no-op.
CREATE TABLE IF NOT EXISTS stripe_events (
    id           TEXT PRIMARY KEY,
    type         TEXT NOT NULL,
    received_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

DEFAULT_SETTINGS = {
    # Convenience fee passed through to the tenant on card payments, in basis
    # points of the payment amount, plus a flat cent amount. ACH is free by
    # default because it costs the owner ~$0.80 capped.
    'card_fee_bps': '300',
    'card_fee_flat_cents': '30',
    'ach_fee_bps': '0',
    'ach_fee_flat_cents': '0',
    'allow_card': '1',
    'allow_ach': '1',
    'allow_partial_payments': '1',
    'currency': 'usd',
    'statement_descriptor': 'RENT',
    'support_email': '',
    'support_phone': '',
}


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.row_factory = sqlite3.Row
    return conn


def init_db(seed: bool = True) -> None:
    """Create the schema, backfill defaults, and (optionally) seed properties."""
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    conn = get_db()
    try:
        conn.executescript(SCHEMA)
        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                'INSERT INTO settings (key, value) VALUES (?, ?) '
                'ON CONFLICT(key) DO NOTHING', (key, value))
        conn.commit()
    finally:
        conn.close()

    if seed:
        from backend.rent.seed import seed_portfolio
        seed_portfolio()
    logger.info("Rent DB initialized at %s", DB_PATH)


def get_setting(key: str, default: str = '') -> str:
    conn = get_db()
    try:
        row = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
    finally:
        conn.close()
    return row['value'] if row else default


def get_settings() -> dict:
    conn = get_db()
    try:
        rows = conn.execute('SELECT key, value FROM settings').fetchall()
    finally:
        conn.close()
    out = dict(DEFAULT_SETTINGS)
    out.update({r['key']: r['value'] for r in rows})
    return out


def set_setting(key: str, value: str) -> None:
    conn = get_db()
    try:
        conn.execute(
            'INSERT INTO settings (key, value) VALUES (?, ?) '
            'ON CONFLICT(key) DO UPDATE SET value = excluded.value', (key, str(value)))
        conn.commit()
    finally:
        conn.close()


def get_int_setting(key: str, default: int = 0) -> int:
    try:
        return int(float(get_setting(key, str(default))))
    except (TypeError, ValueError):
        return default
