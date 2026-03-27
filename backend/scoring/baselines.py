"""
Country-specific baselines derived from historical data.

Instead of using global baselines (same for all countries),
compute the typical indicator score for each country based on
its own 30-day rolling average. This means a spike in
Switzerland (normally quiet) is detected at much lower volume
than a spike in Syria (normally active).

Falls back to global defaults for countries without enough history.
Recomputed once per day, cached in-memory.
"""

import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)

# Global fallback baselines (unchanged from indicator_calculators.py)
GLOBAL_BASELINES = {
    'political_stability': 5,
    'military_conflict': 3,
    'economic_sanctions': 1,
    'protests_civil_unrest': 2,
    'terrorism': 1,
    'diplomatic_tensions': 2,
}

# In-memory cache: {country_code: {indicator: baseline_value}}
_country_baselines = {}
_last_computed = None


def compute_country_baselines():
    """
    Compute per-country baseline scores from the last 30 days of SQLite data.
    The baseline for each indicator is the average of that indicator's
    score over the past 30 days. Requires at least 14 days of data.
    """
    global _country_baselines, _last_computed

    try:
        from backend.cache.database import get_connection
    except Exception:
        logger.warning("Cannot compute baselines: database not available")
        return {}

    conn = get_connection()
    cutoff = (date.today() - timedelta(days=30)).isoformat()

    try:
        cursor = conn.execute("""
            SELECT country_code,
                   AVG(political_stability) as avg_ps,
                   AVG(military_conflict) as avg_mc,
                   AVG(economic_sanctions) as avg_es,
                   AVG(protests_civil_unrest) as avg_pcu,
                   AVG(terrorism) as avg_t,
                   AVG(diplomatic_tensions) as avg_dt,
                   COUNT(*) as day_count
            FROM daily_scores
            WHERE date >= ?
            GROUP BY country_code
            HAVING day_count >= 14
        """, (cutoff,))

        baselines = {}
        for row in cursor.fetchall():
            code = row['country_code']
            baselines[code] = {
                'political_stability': row['avg_ps'] or 0,
                'military_conflict': row['avg_mc'] or 0,
                'economic_sanctions': row['avg_es'] or 0,
                'protests_civil_unrest': row['avg_pcu'] or 0,
                'terrorism': row['avg_t'] or 0,
                'diplomatic_tensions': row['avg_dt'] or 0,
            }

        _country_baselines = baselines
        _last_computed = date.today()

        if baselines:
            logger.info(f"Computed country-specific baselines for {len(baselines)} countries")

        return baselines

    except Exception as e:
        logger.warning(f"Failed to compute country baselines: {e}")
        return {}


def get_country_baseline(country_code, indicator_name):
    """
    Get baseline for a specific country + indicator.
    Falls back to global baseline if country has insufficient history.
    """
    global _last_computed

    # Recompute once per day
    if _last_computed != date.today():
        compute_country_baselines()

    country_data = _country_baselines.get(country_code)
    if country_data and indicator_name in country_data:
        # Use the country-specific average as the "normal" level
        # Minimum baseline of 1 to avoid division by zero in spike detection
        return max(1, round(country_data[indicator_name]))

    return GLOBAL_BASELINES.get(indicator_name, 3)
