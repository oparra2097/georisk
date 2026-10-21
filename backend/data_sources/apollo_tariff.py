"""
Apollo / Torsten Slok effective US tariff rate (realized / collected).

Reproduces the chart from Apollo's "Daily Spark" (Dr. Torsten Slok):
    "US effective tariff rate = Calculated Duty / Total Goods Imports × 100"

Numerator  = monthly US Customs Duty receipts (US Treasury Fiscal Data,
             Monthly Treasury Statement, Table 4). Open API, no key.
Denominator = monthly US Imports of Goods, Balance-of-Payments basis
             (FRED series ``BOPGIMP``, millions of USD, NSA).

Returns a monthly series complementary to the Yale Budget Lab policy
series — Yale shows the statutory rate set by policy; this shows what
Customs actually collected (reflects exemptions, substitution, timing).

Auto-updates monthly as the underlying US Treasury & FRED series are
revised, so no manual data entry is required.

Cache: 12 hours (data refreshes monthly, no need to re-fetch often).
"""

import logging
import threading
import time
from datetime import datetime

import requests

from backend.data_sources import fred_client

logger = logging.getLogger(__name__)

CACHE_TTL = 12 * 3600

TREASURY_MTS_URL = (
    'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/'
    'accounting/mts/mts_table_4'
)

# FRED: Imports of Goods, Balance of Payments Basis — monthly NSA millions
FRED_IMPORTS_SERIES = 'BOPGIMP'


# ── Cache (in-memory; 12h TTL on a derived series is fine) ──

_lock = threading.RLock()
_cached_data = None
_cached_at = 0


def _fetch_customs_duties():
    """
    Return monthly US customs duty receipts as {'YYYY-MM-01': dollars}.

    US Treasury Fiscal Data MTS Table 4 (Receipts of the US Government).
    Rows include multiple ``classification_desc`` values; we filter for
    the "Customs Duties" line.
    """
    out = {}
    params = {
        'filter': 'classification_desc:eq:Customs Duties',
        'fields': 'record_date,current_month_rcpt_outly_amt,classification_desc,parent_id',
        'page[size]': '10000',
        'sort': 'record_date',
        'format': 'json',
    }
    try:
        resp = requests.get(TREASURY_MTS_URL, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logger.warning(f'Treasury MTS fetch failed: {e}')
        return out

    for rec in payload.get('data', []):
        rec_date = (rec.get('record_date') or '')[:7]
        if not rec_date:
            continue
        try:
            amt = float(rec.get('current_month_rcpt_outly_amt'))
        except (TypeError, ValueError):
            continue
        key = f'{rec_date}-01'
        # Multiple rows per month exist (receipt-type subtotals in MTS).
        # Prefer the largest magnitude per month to catch the headline total.
        if key not in out or amt > out[key]:
            out[key] = amt
    return out


def _fetch_goods_imports():
    """
    Return monthly US goods imports (BOP basis) as {'YYYY-MM-01': dollars}.

    FRED series BOPGIMP is monthly NSA, millions of USD. Converted to
    plain dollars here so numerator and denominator share units.
    """
    out = {}
    try:
        observations = fred_client.fetch_series(FRED_IMPORTS_SERIES)
    except Exception as e:
        logger.warning(f'FRED {FRED_IMPORTS_SERIES} fetch failed: {e}')
        return out

    for obs in observations:
        date = obs.get('date')
        val = obs.get('value')
        if not date or val is None:
            continue
        key = date[:7] + '-01'
        out[key] = float(val) * 1_000_000  # millions → dollars
    return out


def _build_series():
    """Compute monthly effective tariff rate = duties / imports × 100."""
    duties = _fetch_customs_duties()
    imports = _fetch_goods_imports()
    if not duties or not imports:
        logger.warning(
            'Apollo tariff: duties=%d months, imports=%d months — insufficient',
            len(duties), len(imports),
        )
        return None

    points = []
    for date_key, imp_dollars in sorted(imports.items()):
        duty_dollars = duties.get(date_key)
        if duty_dollars is None or imp_dollars <= 0:
            continue
        rate = (duty_dollars / imp_dollars) * 100.0
        points.append({
            'date': date_key,
            'value': round(rate, 2),
            'source': 'US Treasury MTS + FRED BOPGIMP (live)',
        })

    if not points:
        return None

    latest = points[-1]
    peak = max(points, key=lambda p: p['value'])
    return {
        'source': 'US Treasury Fiscal Data + FRED BOPGIMP',
        'source_url': 'https://www.apolloacademy.com/the-daily-spark/',
        'source_detail': 'Monthly Calculated Duty ÷ Total Goods Imports × 100',
        'title': 'US Effective Tariff Rate (Collected)',
        'subtitle': 'Customs duties actually collected, monthly',
        'unit': 'Percent of goods imports',
        'frequency': 'Monthly',
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        'latest_date': latest['date'],
        'latest_value': latest['value'],
        'peak_date': peak['date'],
        'peak_value': peak['value'],
        'methodology': (
            'Monthly US customs duty receipts (US Treasury Monthly Treasury '
            'Statement, Table 4) divided by monthly US goods imports on a '
            'Balance of Payments basis (FRED series BOPGIMP). Expressed in '
            'percent. Matches the methodology published by Apollo Chief '
            'Economist Dr. Torsten Slok in "The Daily Spark" (Sources: US '
            'Census Bureau, Macrobond, Apollo Chief Economist). Auto-updates '
            'monthly as both underlying series are revised.'
        ),
        'points': points,
    }


def get_apollo_tariff_data():
    """Public API: returns cached Apollo/Slok effective tariff series."""
    global _cached_data, _cached_at
    with _lock:
        if _cached_data and (time.time() - _cached_at) < CACHE_TTL:
            return _cached_data
    data = _build_series()
    if data:
        with _lock:
            _cached_data = data
            _cached_at = time.time()
    return data or {'points': [], 'error': 'Apollo tariff data unavailable'}


def refresh():
    """Force refresh — used by diagnostic endpoints."""
    global _cached_data, _cached_at
    with _lock:
        _cached_data = None
        _cached_at = 0
    return get_apollo_tariff_data()
