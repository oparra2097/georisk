"""
Geopolitical Risk (GPR) Index by Iacoviello (2022).

Monthly country-level index for 44 countries, published by the Federal Reserve.
The academic standard benchmark for geopolitical risk measurement.
Downloaded from https://www.matteoiacoviello.com/gpr.htm

Used as a calibration signal within the base score calculation
alongside WGI governance scores and macro indicators.

Cached locally for 30 days. No API key needed — public data.
File is legacy .xls (CDFV2) format — requires xlrd library.
"""

import os
import json
import math
import logging
import requests
from datetime import datetime
from config import Config

RETRY_BACKOFF = 3600  # 1 hour cooldown after a failed download

logger = logging.getLogger(__name__)

# Monthly country-level data (Excel format)
GPR_DATA_URL = 'https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls'

GPR_CACHE_FILE = os.path.join(Config.DATA_DIR, 'gpr_cache.json')
GPR_CACHE_DAYS = 30

# GPR column names → ISO alpha-2 codes (44 countries)
GPR_COUNTRIES = {
    'GPRC_ARG': 'AR', 'GPRC_AUS': 'AU', 'GPRC_BRA': 'BR',
    'GPRC_CAN': 'CA', 'GPRC_CHN': 'CN', 'GPRC_COL': 'CO',
    'GPRC_EGY': 'EG', 'GPRC_FRA': 'FR', 'GPRC_DEU': 'DE',
    'GPRC_IND': 'IN', 'GPRC_IDN': 'ID', 'GPRC_ISR': 'IL',
    'GPRC_ITA': 'IT', 'GPRC_JPN': 'JP', 'GPRC_KOR': 'KR',
    'GPRC_MEX': 'MX', 'GPRC_MYS': 'MY', 'GPRC_NGA': 'NG',
    'GPRC_PAK': 'PK', 'GPRC_PHL': 'PH', 'GPRC_POL': 'PL',
    'GPRC_RUS': 'RU', 'GPRC_SAU': 'SA', 'GPRC_ZAF': 'ZA',
    'GPRC_ESP': 'ES', 'GPRC_SWE': 'SE', 'GPRC_THA': 'TH',
    'GPRC_TUR': 'TR', 'GPRC_GBR': 'GB', 'GPRC_USA': 'US',
    'GPRC_UKR': 'UA', 'GPRC_VEN': 'VE', 'GPRC_IRN': 'IR',
    'GPRC_IRQ': 'IQ', 'GPRC_NOR': 'NO', 'GPRC_NLD': 'NL',
    'GPRC_CHE': 'CH', 'GPRC_CHL': 'CL', 'GPRC_PER': 'PE',
    'GPRC_COD': 'CD', 'GPRC_TUN': 'TN', 'GPRC_GRC': 'GR',
    'GPRC_BGD': 'BD', 'GPRC_HUN': 'HU',
}

# In-memory cache
_gpr_scores = None
_gpr_fetched_at = None
_gpr_last_failure = None  # Prevents retry storms on download failure


def _load_cache():
    """Load GPR scores from disk cache if fresh enough."""
    if not os.path.exists(GPR_CACHE_FILE):
        return None
    try:
        with open(GPR_CACHE_FILE, 'r') as f:
            cache = json.load(f)
        fetched = datetime.fromisoformat(cache.get('fetched_at', '2000-01-01'))
        if (datetime.utcnow() - fetched).days < GPR_CACHE_DAYS:
            return cache.get('scores', {})
    except Exception:
        pass
    return None


def _save_cache(scores):
    """Save GPR scores to disk cache."""
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        cache = {
            'fetched_at': datetime.utcnow().isoformat(),
            'scores': scores,
        }
        with open(GPR_CACHE_FILE, 'w') as f:
            json.dump(cache, f, separators=(',', ':'))
    except Exception as e:
        logger.warning(f"Failed to save GPR cache: {e}")


def _normalize_gpr(raw_value):
    """
    Normalize raw GPR index value to 0-100 scale.

    Country-level GPRC values typically range from ~0.3 (calm) to ~20+ (crisis).
    The global GPR index uses a different scale (~50-500).

    Uses log-based scaling that works for both ranges:
      GPRC  0.5 →  ~5    (very low risk)
      GPRC  1.0 → ~15    (low)
      GPRC  2.0 → ~25    (moderate)
      GPRC  5.0 → ~40    (elevated)
      GPRC 10.0 → ~55    (high)
      GPRC 20.0 → ~70    (severe)
      GPR 100+  → ~85+   (extreme - global index)
    """
    if raw_value is None or raw_value <= 0:
        return 0.0
    log_val = math.log2(max(raw_value, 0.1))
    # Map log2(0.5)≈-1 to ~5, log2(20)≈4.3 to ~70
    norm = max(0, min(100, (log_val + 1.5) * 12.5))
    return round(norm, 1)


def fetch_gpr_data():
    """
    Download and parse GPR index data. Returns {iso2: normalized_score}.
    Uses disk cache (30 days), then memory cache.
    The source file is legacy .xls format — parsed with xlrd.
    Includes a 1-hour backoff after download failures to prevent retry storms.
    """
    global _gpr_scores, _gpr_fetched_at, _gpr_last_failure

    # Check memory cache
    if _gpr_scores is not None and _gpr_fetched_at is not None:
        if (datetime.utcnow() - _gpr_fetched_at).days < GPR_CACHE_DAYS:
            return _gpr_scores

    # Check disk cache
    cached = _load_cache()
    if cached:
        _gpr_scores = cached
        _gpr_fetched_at = datetime.utcnow()
        return cached

    # Backoff: don't retry within 1 hour of a failure
    if _gpr_last_failure is not None:
        elapsed = (datetime.utcnow() - _gpr_last_failure).total_seconds()
        if elapsed < RETRY_BACKOFF:
            return _gpr_scores or {}

    # Download fresh data
    try:
        import xlrd

        resp = requests.get(GPR_DATA_URL, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"GPR download failed: HTTP {resp.status_code}")
            _gpr_last_failure = datetime.utcnow()
            return _gpr_scores or {}

        # Parse legacy .xls with xlrd
        wb = xlrd.open_workbook(file_contents=resp.content)
        ws = wb.sheet_by_index(0)

        if ws.nrows < 2:
            logger.warning("GPR Excel has no data rows")
            _gpr_last_failure = datetime.utcnow()
            return _gpr_scores or {}

        # Headers from first row
        headers = [ws.cell_value(0, c) for c in range(ws.ncols)]

        # Last row = most recent month
        last_row_idx = ws.nrows - 1

        # Extract and normalize scores
        scores = {}
        for i, header in enumerate(headers):
            if header and header in GPR_COUNTRIES:
                raw = ws.cell_value(last_row_idx, i)
                if raw is not None and raw != '':
                    try:
                        iso2 = GPR_COUNTRIES[header]
                        scores[iso2] = _normalize_gpr(float(raw))
                    except (ValueError, TypeError):
                        pass

        if scores:
            _gpr_scores = scores
            _gpr_fetched_at = datetime.utcnow()
            _gpr_last_failure = None
            _save_cache(scores)
            logger.info(f"GPR Index loaded: {len(scores)} countries")
        else:
            logger.warning("GPR parsing produced no scores")
            _gpr_last_failure = datetime.utcnow()

        return scores

    except ImportError:
        logger.warning("xlrd not installed — GPR Index unavailable (pip install xlrd)")
        _gpr_last_failure = datetime.utcnow()
        return _gpr_scores or {}
    except Exception as e:
        logger.error(f"Failed to fetch GPR data: {e}")
        _gpr_last_failure = datetime.utcnow()
        return _gpr_scores or {}


def get_gpr_score(country_code):
    """
    Get normalized GPR score (0-100) for a country.
    Returns None if country not covered by GPR (44 countries only).
    """
    scores = fetch_gpr_data()
    return scores.get(country_code.upper())
