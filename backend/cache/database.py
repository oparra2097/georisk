"""
SQLite-based persistent storage for GeoRisk daily scores.

Replaces history.json with an indexed database that supports:
- Efficient country + date range queries
- 365-day retention (vs 90-day JSON cap)
- WAL mode for concurrent reads during scheduler writes
- Anomaly detection (score change flagging)

DB file lives at data/georisk.db alongside existing scores.json.
No new pip dependency — sqlite3 is in Python stdlib.
"""

import sqlite3
import os
import json
import logging
import threading
from datetime import date, timedelta, datetime
from config import Config

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(Config.DATA_DIR, 'georisk.db')
_conn = None
_write_lock = threading.Lock()


def get_connection():
    """Get or create a module-level SQLite connection (WAL mode, thread-safe)."""
    global _conn
    if _conn is None:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.row_factory = sqlite3.Row
    return _conn


def init_db():
    """Create tables and indexes if they don't exist."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            country_code TEXT NOT NULL,
            composite_score REAL NOT NULL,
            base_score REAL NOT NULL DEFAULT 0.0,
            news_score REAL NOT NULL DEFAULT 0.0,
            political_stability REAL DEFAULT 0.0,
            military_conflict REAL DEFAULT 0.0,
            economic_sanctions REAL DEFAULT 0.0,
            protests_civil_unrest REAL DEFAULT 0.0,
            terrorism REAL DEFAULT 0.0,
            diplomatic_tensions REAL DEFAULT 0.0,
            avg_tone REAL DEFAULT 0.0,
            headline_count INTEGER DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(date, country_code)
        );

        CREATE INDEX IF NOT EXISTS idx_daily_scores_country
            ON daily_scores(country_code);
        CREATE INDEX IF NOT EXISTS idx_daily_scores_date
            ON daily_scores(date);
        CREATE INDEX IF NOT EXISTS idx_daily_scores_country_date
            ON daily_scores(country_code, date);

        CREATE TABLE IF NOT EXISTS score_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            country_code TEXT NOT NULL,
            event_type TEXT NOT NULL,
            old_score REAL,
            new_score REAL,
            delta REAL,
            details TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_events_country
            ON score_events(country_code);
        CREATE INDEX IF NOT EXISTS idx_events_date
            ON score_events(date);
    """)
    conn.commit()
    logger.info(f"SQLite database initialized at {DB_PATH}")


# ── Write Operations ────────────────────────────────────────────────────


def save_daily_scores(store):
    """
    Save today's scores for all countries.
    Called after each GDELT refresh cycle (replaces JSON snapshot).
    Uses INSERT OR REPLACE — last write of the day wins.
    """
    conn = get_connection()
    today = date.today().isoformat()
    all_scores = store.get_all_scores()

    rows = []
    for code, risk in all_scores.items():
        ind = risk.indicators
        rows.append((
            today, code,
            round(risk.composite_score, 1),
            round(risk.base_score, 1),
            round(risk.news_score, 1),
            round(ind.political_stability, 1),
            round(ind.military_conflict, 1),
            round(ind.economic_sanctions, 1),
            round(ind.protests_civil_unrest, 1),
            round(ind.terrorism, 1),
            round(ind.diplomatic_tensions, 1),
            round(risk.avg_tone, 2),
            risk.headline_count,
        ))

    with _write_lock:
        conn.executemany("""
            INSERT OR REPLACE INTO daily_scores
            (date, country_code, composite_score, base_score, news_score,
             political_stability, military_conflict, economic_sanctions,
             protests_civil_unrest, terrorism, diplomatic_tensions,
             avg_tone, headline_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()

    logger.info(f"Daily scores saved to SQLite: {len(rows)} countries for {today}")


def cleanup_old_scores(max_days=None):
    """Delete scores older than max_days (default: HISTORY_RETENTION_DAYS)."""
    if max_days is None:
        max_days = getattr(Config, 'HISTORY_RETENTION_DAYS', 365)
    conn = get_connection()
    cutoff = (date.today() - timedelta(days=max_days)).isoformat()
    with _write_lock:
        cursor = conn.execute("DELETE FROM daily_scores WHERE date < ?", (cutoff,))
        deleted = cursor.rowcount
        # Also clean old events
        conn.execute("DELETE FROM score_events WHERE date < ?", (cutoff,))
        conn.commit()
    if deleted > 0:
        logger.info(f"Cleaned up {deleted} old score rows (before {cutoff})")


# ── Read Operations ─────────────────────────────────────────────────────


def get_country_history(country_code, days=90):
    """
    Return time series for a single country.
    Returns list of dicts: [{date, composite_score, base_score, news_score, ...}]
    """
    conn = get_connection()
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    cursor = conn.execute("""
        SELECT date, composite_score, base_score, news_score,
               political_stability, military_conflict, economic_sanctions,
               protests_civil_unrest, terrorism, diplomatic_tensions,
               avg_tone, headline_count
        FROM daily_scores
        WHERE country_code = ? AND date >= ?
        ORDER BY date ASC
    """, (country_code.upper(), cutoff))
    return [dict(row) for row in cursor.fetchall()]


def get_all_history(days=90):
    """
    Return overview history: {date_str: {country_code: composite_score}}.
    Used by the /api/history endpoint for the full map overview.
    """
    conn = get_connection()
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    cursor = conn.execute("""
        SELECT date, country_code, composite_score
        FROM daily_scores
        WHERE date >= ?
        ORDER BY date ASC
    """, (cutoff,))

    result = {}
    for row in cursor.fetchall():
        d = row['date']
        if d not in result:
            result[d] = {}
        result[d][row['country_code']] = row['composite_score']
    return result


def get_history_dates():
    """Return sorted list of all dates that have score data."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT DISTINCT date FROM daily_scores ORDER BY date ASC"
    )
    return [row['date'] for row in cursor.fetchall()]


def get_score_count():
    """Return total number of daily score rows."""
    conn = get_connection()
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM daily_scores")
    row = cursor.fetchone()
    return row['cnt'] if row else 0


# ── Anomaly Detection ───────────────────────────────────────────────────


def detect_anomalies(threshold_delta=10.0):
    """
    Compare today's scores with yesterday's.
    Flag countries with composite score change > threshold_delta.
    Returns list of anomaly dicts and stores them in score_events.
    """
    conn = get_connection()
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    cursor = conn.execute("""
        SELECT t.country_code,
               t.composite_score AS today_score,
               y.composite_score AS yesterday_score,
               (t.composite_score - y.composite_score) AS delta
        FROM daily_scores t
        JOIN daily_scores y ON t.country_code = y.country_code
        WHERE t.date = ? AND y.date = ?
        AND ABS(t.composite_score - y.composite_score) > ?
        ORDER BY ABS(t.composite_score - y.composite_score) DESC
    """, (today, yesterday, threshold_delta))

    anomalies = []
    event_rows = []
    for row in cursor.fetchall():
        event_type = 'spike' if row['delta'] > 0 else 'drop'
        anomalies.append({
            'country_code': row['country_code'],
            'event_type': event_type,
            'old_score': row['yesterday_score'],
            'new_score': row['today_score'],
            'delta': round(row['delta'], 1),
        })
        event_rows.append((
            today, row['country_code'], event_type,
            row['yesterday_score'], row['today_score'],
            round(row['delta'], 1)
        ))

    if event_rows:
        with _write_lock:
            conn.executemany("""
                INSERT OR IGNORE INTO score_events
                (date, country_code, event_type, old_score, new_score, delta)
                VALUES (?, ?, ?, ?, ?, ?)
            """, event_rows)
            conn.commit()

    return anomalies


def get_recent_events(days=7, country_code=None):
    """Return recent score events (anomalies)."""
    conn = get_connection()
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    if country_code:
        cursor = conn.execute("""
            SELECT * FROM score_events
            WHERE date >= ? AND country_code = ?
            ORDER BY date DESC, ABS(delta) DESC
        """, (cutoff, country_code.upper()))
    else:
        cursor = conn.execute("""
            SELECT * FROM score_events
            WHERE date >= ?
            ORDER BY date DESC, ABS(delta) DESC
            LIMIT 100
        """, (cutoff,))

    return [dict(row) for row in cursor.fetchall()]


# ── Migration ───────────────────────────────────────────────────────────


def migrate_from_json():
    """
    One-time migration of history.json into SQLite.
    Idempotent — uses INSERT OR IGNORE so re-running is safe.
    Returns number of rows imported.
    """
    history_file = Config.HISTORY_FILE
    if not os.path.exists(history_file):
        logger.info("No history.json found — nothing to migrate.")
        return 0

    try:
        with open(history_file, 'r') as f:
            history = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read history.json for migration: {e}")
        return 0

    if not history:
        return 0

    conn = get_connection()
    rows = []
    for date_str, snapshot in history.items():
        for code, data in snapshot.items():
            indicators = data.get('indicators', {})
            rows.append((
                date_str, code,
                data.get('composite', 0),
                data.get('base_score', 0),
                data.get('news_score', 0),
                indicators.get('political_stability', 0),
                indicators.get('military_conflict', 0),
                indicators.get('economic_sanctions', 0),
                indicators.get('protests_civil_unrest', 0),
                indicators.get('terrorism', 0),
                indicators.get('diplomatic_tensions', 0),
                data.get('avg_tone', 0),
                0,  # headline_count not in history.json
            ))

    with _write_lock:
        conn.executemany("""
            INSERT OR IGNORE INTO daily_scores
            (date, country_code, composite_score, base_score, news_score,
             political_stability, military_conflict, economic_sanctions,
             protests_civil_unrest, terrorism, diplomatic_tensions,
             avg_tone, headline_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()

    logger.info(f"Migrated {len(rows)} rows from history.json into SQLite")
    return len(rows)
