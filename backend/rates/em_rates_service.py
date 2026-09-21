"""
EM rates service — orchestrates per-country fetch, fit, and evaluate.

For each of the 12 countries in ``backend.rates.em_config`` this
module:

1. Fetches monthly policy-rate history + monthly CPI YoY via
   ``em_policy_rates.py``.
2. Fits a per-country inertial Taylor rule via ``taylor_rule.fit``.
3. Fetches the current 10Y yield + 3M / 12M deltas via
   ``em_yields.get_yield``.
4. Looks up the next scheduled central-bank meeting from
   ``em_meetings.get_next_meeting``.
5. Runs ``taylor_rule.evaluate`` against the current rate + CPI to
   produce the direction call.

The panel assembly caches for 6 hours via ``@cached`` — inputs
(monthly CPI, monthly rate, weekly yield) don't move faster than that.
Fit artifacts cache for 30 days on disk under
``Config.DATA_DIR/em_rates_fit_cache/<iso3>.json``.
"""

from __future__ import annotations

import logging
from typing import Optional

from backend.data_sources._cache import cached
from backend.data_sources import em_policy_rates, em_yields
from backend.rates import em_config, em_meetings, taylor_rule

logger = logging.getLogger(__name__)


@cached(namespace='em_rates_fit', ttl=30 * 86400, disk=True)
def get_fit(iso3: str) -> dict:
    """Fit (or return cached fit) for one country.

    30-day TTL because policy-rate dynamics change slowly and the
    monthly panel adds one obs at a time; refitting daily has no
    payoff.
    """
    iso3 = iso3.upper()
    cfg = em_config.get_country(iso3)
    if not cfg:
        return {'fit_error': f'unknown country {iso3}'}

    rate_hist = em_policy_rates.get_policy_rate_history(iso3)
    cpi_hist = em_policy_rates.get_cpi_history(iso3)
    return taylor_rule.fit(rate_hist, cpi_hist, cfg.get('target_cpi'))


@cached(namespace='em_rates_panel', ttl=6 * 3600, disk=True)
def get_panel() -> dict:
    """Full 12-country grid backing ``/api/em-rates/panel``.

    Structure::

        {
          'countries': [
            {
              'iso3': 'BRA', 'name': 'Brazil', 'region': 'LATAM',
              'cb_name': 'BCB', 'rate_name': 'SELIC target',
              'current_rate': 10.75, 'current_rate_date': '2026-09-01',
              'real_rate': 5.75,        # current − cpi_yoy
              'cpi_yoy': 5.0, 'target_cpi': 3.0,
              'next_meeting': '2026-11-05',
              'model': {'implied_rate': 10.9, 'gap_bp': 15,
                        'direction': 'HOLD', 'magnitude_bp': 0,
                        'confidence': 'LOW', ...},
              'yield_10y': 11.82,
              'delta_3m_bp': 34, 'delta_12m_bp': -128,
              'yield_direction': 'HIGHER',
              'fit_ok': True, 'r_squared': 0.92,
            },
            ...
          ],
          'summary': {
            'n_hikes': 3, 'n_holds': 6, 'n_cuts': 3,
            'panel_avg_rate': 6.8, 'panel_avg_cpi_yoy': 4.2,
          },
        }
    """
    import time

    rows: list[dict] = []
    for iso3 in em_config.ALL_ISO3:
        cfg = em_config.get_country(iso3)
        if not cfg:
            continue
        row = _country_row(iso3, cfg)
        rows.append(row)

    # Panel summary counts (only rows with a valid direction).
    dir_counts = {'HIKE': 0, 'HOLD': 0, 'CUT': 0}
    rates: list[float] = []
    cpis: list[float] = []
    for r in rows:
        d = (r.get('model') or {}).get('direction')
        if d in dir_counts:
            dir_counts[d] += 1
        if r.get('current_rate') is not None:
            rates.append(r['current_rate'])
        if r.get('cpi_yoy') is not None:
            cpis.append(r['cpi_yoy'])
    summary = {
        'n_hikes': dir_counts['HIKE'],
        'n_holds': dir_counts['HOLD'],
        'n_cuts':  dir_counts['CUT'],
        'panel_avg_rate':    round(sum(rates) / len(rates), 3) if rates else None,
        'panel_avg_cpi_yoy': round(sum(cpis) / len(cpis), 3) if cpis else None,
    }
    return {
        'countries': rows,
        'summary': summary,
        'last_updated': time.time(),
    }


def _country_row(iso3: str, cfg: dict) -> dict:
    """Assemble one row of the panel. Every sub-fetch is fail-soft
    (returns partial data plus an ``errors`` list rather than raising)."""
    errors: list[str] = []

    rate_hist = em_policy_rates.get_policy_rate_history(iso3)
    cpi_hist = em_policy_rates.get_cpi_history(iso3)
    current_rate = rate_hist[-1]['value'] if rate_hist else None
    current_rate_date = rate_hist[-1]['date'] if rate_hist else None
    cpi_yoy = cpi_hist[-1]['value'] if cpi_hist else None
    real_rate = (current_rate - cpi_yoy) if (current_rate is not None and cpi_yoy is not None) else None

    if not rate_hist:
        errors.append('no policy rate history')
    if not cpi_hist:
        errors.append('no CPI history')

    # Fit + evaluate
    fit_res = get_fit(iso3)
    if fit_res.get('fit_error'):
        errors.append(f"fit: {fit_res['fit_error']}")
    model = taylor_rule.evaluate(
        fit_res, current_rate, cpi_yoy, cfg.get('target_cpi'),
    )

    # 10Y yield
    yld = em_yields.get_yield(iso3)
    if yld.get('error'):
        errors.append(f"10y: {yld['error']}")

    # Yield direction — derived from policy call + 3M yield move.
    yield_direction = _yield_direction(
        yld.get('delta_3m_bp'), model.get('direction'),
    )

    # Next meeting
    next_meeting = em_meetings.get_next_meeting(iso3)

    return {
        'iso3': iso3,
        'name': cfg['name'],
        'iso2': cfg['iso2'],
        'ccy': cfg['ccy'],
        'region': em_config.region_of(iso3),
        'cb_name': cfg['cb_name'],
        'rate_name': cfg['rate_name'],
        'current_rate': round(current_rate, 3) if current_rate is not None else None,
        'current_rate_date': current_rate_date,
        'real_rate': round(real_rate, 3) if real_rate is not None else None,
        'cpi_yoy': round(cpi_yoy, 3) if cpi_yoy is not None else None,
        'target_cpi': cfg.get('target_cpi'),
        'next_meeting': next_meeting.isoformat() if next_meeting else None,
        'model': model,
        'yield_10y': yld.get('current'),
        'yield_date': yld.get('current_date'),
        'delta_3m_bp': yld.get('delta_3m_bp'),
        'delta_12m_bp': yld.get('delta_12m_bp'),
        'yield_direction': yield_direction,
        'fit_ok': fit_res.get('fit_error') is None,
        'r_squared': fit_res.get('r_squared'),
        'errors': errors,
    }


def _yield_direction(delta_3m_bp: Optional[float],
                     policy_call: Optional[str]) -> str:
    """Combined-signal 10Y direction call. Not model-fitted — it's
    a heuristic transmitted from the policy-rate call plus the 3M
    yield move."""
    if delta_3m_bp is None or policy_call is None:
        return 'UNKNOWN'
    if abs(delta_3m_bp) < 30 and policy_call == 'HOLD':
        return 'SIDEWAYS'
    if delta_3m_bp > 30 and policy_call == 'HIKE':
        return 'HIGHER'
    if delta_3m_bp < -30 and policy_call == 'CUT':
        return 'LOWER'
    return 'MIXED'


def get_country_detail(iso3: str) -> Optional[dict]:
    """Full detail for one country: history, fit, methodology."""
    iso3 = iso3.upper()
    cfg = em_config.get_country(iso3)
    if not cfg:
        return None
    rate_hist = em_policy_rates.get_policy_rate_history(iso3)
    cpi_hist = em_policy_rates.get_cpi_history(iso3)
    fit_res = get_fit(iso3)
    yld = em_yields.get_yield(iso3)
    return {
        'iso3': iso3,
        'name': cfg['name'],
        'region': em_config.region_of(iso3),
        'config': {k: v for k, v in cfg.items() if k not in ('note',)},
        'policy_rate_history': rate_hist[-260:],  # ~20 years monthly
        'cpi_yoy_history': cpi_hist[-260:],
        'yield_history': (yld.get('history') or [])[-260:],
        'yield_current': yld.get('current'),
        'yield_deltas': {
            'delta_3m_bp': yld.get('delta_3m_bp'),
            'delta_12m_bp': yld.get('delta_12m_bp'),
        },
        'fit': fit_res,
    }
