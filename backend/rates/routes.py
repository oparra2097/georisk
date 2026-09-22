"""
Flask blueprint for /api/us-rates/*.

Three endpoints back the ``/us-rates`` page and the ``/trades`` board's
US rates section:

  GET /api/us-rates/curve      — raw fed funds futures curve (12-15
                                 monthly ZQ contracts + FRED DFF spot).
  GET /api/us-rates/fedwatch   — per-FOMC-meeting probability grid
                                 (5 buckets: -50/-25/hold/+25/+50).
  GET /api/us-rates/edge       — market post-rate vs. macro model
                                 fedfunds path, sorted by |edge_bp|.

Endpoints are unguarded — market data + the model comparison are the
observable outputs of the free tier. The ``/us-rates`` PAGE route
(defined in ``app.py``) is gated with ``macro_access_required`` since
opening the underlying model itself requires access.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from flask import Blueprint, jsonify, request

from backend.data_sources.rates_futures import get_fed_funds_curve
from backend.data_sources import fred_client
from backend.rates.fedwatch import compute_fedwatch
from backend.rates.fomc_calendar import get_upcoming_meetings
from backend.rates import em_rates_service, em_config

logger = logging.getLogger(__name__)

us_rates_bp = Blueprint('us_rates', __name__)
em_rates_bp = Blueprint('em_rates', __name__)


from backend.data_sources._cache import cached


@cached(namespace='us_target_mid', ttl=6 * 3600, disk=True)
def _get_current_target_mid() -> float:
    """Current fed funds target range midpoint in %.

    Priority:
      1. ``FED_TARGET_MID_OVERRIDE`` env var (or ``Config`` attribute)
         — manual escape hatch. Set this whenever the Fed moves and
         the FRED path stops matching reality. Overrides FRED.
      2. FRED ``DFEDTARU`` + ``DFEDTARL`` / 2 (official target range).
      3. FRED ``DFF`` (effective rate — usually within 10bp of midpoint).
      4. Hardcoded default ``3.875`` (mid of 3.75-4.00 post-Sept 2026
         cut). Refresh this the next time the Fed moves.

    Cached 6h on disk so ``/fedwatch`` and ``/edge`` don't triple-hit
    FRED per request.
    """
    import os

    # 1. Manual override
    override = os.environ.get('FED_TARGET_MID_OVERRIDE', '').strip()
    if not override:
        try:
            from config import Config
            override = str(getattr(Config, 'FED_TARGET_MID_OVERRIDE', '') or '').strip()
        except Exception:  # noqa: BLE001
            pass
    if override:
        try:
            return float(override)
        except (TypeError, ValueError):
            logger.warning(f'FED_TARGET_MID_OVERRIDE={override!r} not a float; ignoring')

    # 2. FRED official target range
    try:
        _, up = fred_client.fetch_latest_value('DFEDTARU')
        _, lo = fred_client.fetch_latest_value('DFEDTARL')
        if up is not None and lo is not None:
            return (float(up) + float(lo)) / 2.0
    except Exception as e:  # noqa: BLE001
        logger.debug(f'DFEDTARU/DFEDTARL lookup failed: {e}')

    # 3. FRED effective rate
    try:
        _, dff = fred_client.fetch_latest_value('DFF')
        if dff is not None:
            return float(dff)
    except Exception as e:  # noqa: BLE001
        logger.debug(f'DFF lookup failed: {e}')

    # 4. Hardcoded fallback — mid of the current 3.75-4.00 range.
    # BUMP THIS EACH TIME THE FED MOVES if you can't rely on FRED being
    # reachable + fresh. Also settable via FED_TARGET_MID_OVERRIDE.
    return 3.875


def _interpolate_p50(model_bands: list[dict], meeting_iso: str) -> Optional[float]:
    """Linear interpolation of the model's p50 fedfunds path to a
    specific meeting date. ``model_bands`` is quarter-end records from
    ``backend.macro_model.service.get_bootstrap()['fedfunds']``.

    Falls off the nearest end if the meeting is outside the model's
    horizon; returns None if the surrounding p50 values are missing.
    """
    if not model_bands:
        return None
    try:
        m_date = date.fromisoformat(meeting_iso)
    except (TypeError, ValueError):
        return None

    quarters = []
    for rec in model_bands:
        q = rec.get('quarter')
        if not q:
            continue
        try:
            quarters.append((date.fromisoformat(q), rec.get('p50')))
        except (TypeError, ValueError):
            continue
    if not quarters:
        return None

    first_q, first_p50 = quarters[0]
    if m_date <= first_q:
        return float(first_p50) if first_p50 is not None else None
    last_q, last_p50 = quarters[-1]
    if m_date >= last_q:
        return float(last_p50) if last_p50 is not None else None

    for i in range(len(quarters) - 1):
        q0, p0 = quarters[i]
        q1, p1 = quarters[i + 1]
        if q0 <= m_date <= q1:
            span_days = (q1 - q0).days
            if span_days <= 0 or p0 is None or p1 is None:
                return float(p0) if p0 is not None else None
            w = (m_date - q0).days / span_days
            return float((1.0 - w) * float(p0) + w * float(p1))
    return None


@us_rates_bp.route('/curve')
def curve():
    """Fed funds futures curve — passthrough to the cached fetcher."""
    try:
        horizon = int(request.args.get('horizon_months', 15))
    except (TypeError, ValueError):
        horizon = 15
    horizon = max(6, min(24, horizon))
    return jsonify(get_fed_funds_curve(horizon_months=horizon))


@us_rates_bp.route('/fedwatch')
def fedwatch():
    """Per-FOMC-meeting hike/hold/cut probability grid."""
    try:
        n = int(request.args.get('n', 8))
    except (TypeError, ValueError):
        n = 8
    n = max(1, min(16, n))

    curve_payload = get_fed_funds_curve()
    target_mid = _get_current_target_mid()
    meetings = get_upcoming_meetings(n=n)
    rows = compute_fedwatch(
        curve_payload.get('curve') or [],
        current_target_mid=target_mid,
        meetings=meetings,
    )
    return jsonify({
        'target_mid': target_mid,
        'grid': rows,
        'anchor_date': curve_payload.get('anchor_date'),
        'spot': curve_payload.get('spot'),
        'source': curve_payload.get('source'),
    })


@us_rates_bp.route('/edge')
def edge():
    """Market vs. macro-model divergence, per FOMC meeting.

    For each of the next 8 meetings, compare the market-implied
    post-meeting rate (from the FedWatch decomposition) to the macro
    model's fedfunds p50 interpolated to that meeting's date. Sort by
    |edge_bp|, return the top N.

    Positive ``edge_bp`` → model expects higher rates than the market;
    the market is pricing too dovish, so SHORT DURATION is the trade.
    Negative → market prices too hawkish, LONG DURATION.
    """
    try:
        top_n = int(request.args.get('top_n', 3))
    except (TypeError, ValueError):
        top_n = 3
    top_n = max(1, min(10, top_n))

    curve_payload = get_fed_funds_curve()
    target_mid = _get_current_target_mid()
    meetings = get_upcoming_meetings(n=8)
    fw_rows = compute_fedwatch(
        curve_payload.get('curve') or [],
        current_target_mid=target_mid,
        meetings=meetings,
    )

    # Pull the model fedfunds fan (quarterly, 8q ahead). The macro model
    # sits behind an ensure_built() lazy-load — first call may return
    # None while the model is still fitting.
    model_bands = None
    error = None
    try:
        from backend.macro_model import service
        bands = service.get_bootstrap(horizon=8, n_draws=30)
        if bands and 'fedfunds' in bands:
            model_bands = bands['fedfunds']
        else:
            error = 'macro model still building — retry in 30-60s'
    except Exception as e:  # noqa: BLE001
        error = f'macro model unavailable: {e}'

    edges: list[dict] = []
    if model_bands:
        for row in fw_rows:
            if row.get('post_rate') is None:
                continue
            model_pct = _interpolate_p50(model_bands, row['meeting_date'])
            if model_pct is None:
                continue
            edge_bp = (model_pct - float(row['post_rate'])) * 100.0
            edges.append({
                'meeting_date': row['meeting_date'],
                'market_post_rate': row['post_rate'],
                'model_post_rate': round(model_pct, 4),
                'edge_bp': round(edge_bp, 2),
                'direction': 'SHORT DURATION' if edge_bp > 0 else 'LONG DURATION',
                'delta_bp_market': row.get('delta_bp'),
            })
        edges.sort(key=lambda e: abs(e['edge_bp']), reverse=True)

    return jsonify({
        'top_n': top_n,
        'edges': edges[:top_n],
        'anchor_date': curve_payload.get('anchor_date'),
        'target_mid': target_mid,
        'error': error,
    })


# ── EM rates blueprint (mounted at /api/em-rates in app.py) ─────────────

@em_rates_bp.route('/panel')
def em_panel():
    """LATAM + Asia top 6 direction grid.

    Returns the full 12-country panel: current policy rate, real rate,
    CPI YoY, next meeting date, Taylor-rule direction call, and 10Y
    yield + deltas. See ``em_rates_service.get_panel`` for the payload
    shape.
    """
    return jsonify(em_rates_service.get_panel())


@em_rates_bp.route('/country/<iso3>')
def em_country(iso3: str):
    """Full detail for one country — history vectors + fit diagnostics.

    Used by the drill-down chart on ``/em-rates`` and (later) by a
    ``/em-rates/<iso3>`` deep-link page.
    """
    detail = em_rates_service.get_country_detail(iso3.upper())
    if detail is None:
        return jsonify({'error': f'unknown country {iso3}'}), 404
    return jsonify(detail)


@em_rates_bp.route('/methodology')
def em_methodology():
    """Taylor rule spec + per-country target CPI and neutral rate table.
    Static — no fetches. Refresh yearly by editing ``em_config.py``."""
    rows = []
    for iso3, cfg in em_config.COUNTRIES.items():
        rows.append({
            'iso3': iso3,
            'name': cfg['name'],
            'region': em_config.region_of(iso3),
            'cb_name': cfg['cb_name'],
            'rate_name': cfg['rate_name'],
            'target_cpi': cfg.get('target_cpi'),
            'neutral_r': cfg.get('neutral_r'),
            'note': cfg.get('note'),
            'strategist_expected': cfg.get('strategist_expected'),
        })
    return jsonify({
        'countries': rows,
        'rule': {
            'form': 'r_t = ρ·r_{t-1} + (1-ρ)·[α + β_π·(π−π*) + β_y·y_gap] + ε',
            'guardrails': {
                'rho_bounds': [0.5, 0.98],
                'beta_pi_min': 0.5,
                'beta_y_min': 0.0,
                'min_obs': 60,
            },
            'notes': [
                'One inertial Taylor rule fit per country on 15-20y monthly panel.',
                'β_π enforces the Taylor principle: > 0.5 or the term is dropped.',
                'β_y ≥ 0 or dropped. y_gap is currently a zero placeholder — '
                'output-gap proxy pending IMF IFS AIP_IX ingest.',
                'Fits cache 30 days on disk under Config.DATA_DIR/em_rates_fit_cache/.',
                'Direction bucketing: >75bp = ±50, >25bp = ±25, else HOLD.',
            ],
        },
    })
