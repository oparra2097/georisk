"""
Top-level orchestrator for the Credit Default model.

Pulls the harmonized indicator panel, runs the rating model on it,
overlays the agency-rating snapshot, and caches the result for the
Flask routes layer.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, List, Optional

from backend.credit_default import agency_ratings
from backend.credit_default import data as cd_data
from backend.credit_default import defaults as cd_defaults
from backend.credit_default import fit as cd_fit
from backend.credit_default import rating_model


_cache: Dict[str, object] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 6 * 3600


def _enrich_with_agencies(scored: Dict) -> Dict:
    agencies = agency_ratings.get_agency_ratings()
    countries = scored.get('countries') or {}
    for iso3, c in countries.items():
        a = agencies.get(iso3)
        if not a:
            continue
        c['agency'] = a

        # Compute notch deltas (model rating vs each agency). The rating
        # block now carries its own pm_numeric (1..20) so we map agency
        # letters onto the same scale via the SP/Moody's equivalents.
        model_sp_equiv = (c.get('rating') or {}).get('sp_equiv')
        model_num = agency_ratings.to_numeric(model_sp_equiv)
        deltas = {}
        for k in ('sp', 'moodys', 'fitch'):
            num = a.get(f'{k}_num')
            if model_num is not None and num is not None:
                deltas[k] = model_num - num  # positive = model is harsher
        c['rating']['notch_delta'] = deltas
    return scored


def get_dashboard(force_refresh: bool = False) -> Dict:
    with _cache_lock:
        cached = _cache.get('dashboard')
        cached_ts = _cache.get('dashboard_ts', 0)
    if cached and not force_refresh and (time.time() - cached_ts) < _CACHE_TTL:
        return cached

    panel = cd_data.get_panel(force_refresh=force_refresh)
    scored = rating_model.score_panel(panel)
    enriched = _enrich_with_agencies(scored)

    summary = _summarize(enriched)
    enriched['summary'] = summary

    with _cache_lock:
        _cache['dashboard'] = enriched
        _cache['dashboard_ts'] = time.time()
    return enriched


def get_country(iso3: str) -> Optional[Dict]:
    iso3 = (iso3 or '').upper()
    if len(iso3) != 3:
        return None
    dash = get_dashboard()
    return (dash.get('countries') or {}).get(iso3)


def get_country_history(iso3: str, horizon_years: int = 1) -> Optional[Dict]:
    """Re-score one country's macro panel year-by-year using the current
    fit_state, alongside CRAG default events. Used by the dashboard's
    drilldown chart to back-test whether the model would have flagged
    the country before each historical default (e.g. Ghana 2022).
    """
    iso3 = (iso3 or '').upper()
    if len(iso3) != 3 or not cd_data._is_sovereign_iso(iso3):
        return None

    cache_key = f'history_{iso3}_h{horizon_years}'
    with _cache_lock:
        cached = _cache.get(cache_key)
        cached_ts = _cache.get(f'{cache_key}_ts', 0)
    if cached and (time.time() - cached_ts) < _CACHE_TTL:
        return cached

    fit_state = cd_fit.load_state(horizon_years)
    if not fit_state or not fit_state.get('coefficients'):
        return None
    coefs = fit_state.get('coefficients') or {}
    intercept = float(fit_state.get('intercept') or 0.0)
    scaler = fit_state.get('scaler') or {}
    medians = fit_state.get('medians') or {}
    pd_cal = fit_state.get('pd_calibration') or []
    rb = fit_state.get('rating_buckets') or {}
    cal_buckets = rb.get('buckets') if isinstance(rb, dict) else rb

    panel_df = cd_data.get_history_panel()
    if panel_df is None or panel_df.empty:
        return None
    sub = panel_df[panel_df['iso3'] == iso3].copy()
    if sub.empty:
        return None
    sub = sub.sort_values('year')

    history = []
    for _, row in sub.iterrows():
        z = intercept
        for feat, coef in coefs.items():
            if not coef:
                continue
            raw = row.get(feat)
            try:
                raw_f = float(raw)
                if raw_f != raw_f:  # NaN
                    raw_f = None
            except (TypeError, ValueError):
                raw_f = None
            if raw_f is None:
                raw_f = medians.get(feat)
            if raw_f is None:
                continue
            sc = scaler.get(feat) or {}
            mean = float(sc.get('mean', 0.0))
            std = float(sc.get('std', 1.0)) or 1.0
            zf = (raw_f - mean) / std
            if zf > rating_model.Z_CLIP:
                zf = rating_model.Z_CLIP
            elif zf < -rating_model.Z_CLIP:
                zf = -rating_model.Z_CLIP
            z += float(coef) * zf
        try:
            import math
            proba = 1.0 / (1.0 + math.exp(-z))
        except OverflowError:
            proba = 0.0 if z < 0 else 1.0
        if pd_cal:
            for bkt in pd_cal:
                if proba <= bkt['score_hi']:
                    model_pd = float(bkt['pd_empirical'])
                    break
            else:
                model_pd = float(pd_cal[-1]['pd_empirical'])
        else:
            model_pd = proba
        score = 100.0 * model_pd
        rating = rating_model._letter_and_pd(
            score, defaulted=False, calibrated_buckets=cal_buckets,
        )
        history.append({
            'year': int(row['year']),
            'model_pd': round(model_pd, 5),
            'model_score': round(score, 3),
            'pm_notch': rating['pm_notch'],
            'pm_numeric': rating['pm_numeric'],
            'sp_equiv': rating['sp_equiv'],
        })

    events = [
        {'start_year': ev['start_year'],
         'end_year': ev.get('end_year'),
         'event_type': ev['event_type'],
         'instrument': ev.get('instrument', '')}
        for ev in cd_defaults.load_events(include_distress=True)
        if ev['iso3'] == iso3
    ]
    events.sort(key=lambda e: e['start_year'])

    agencies = agency_ratings.get_agency_ratings()
    agency = agencies.get(iso3) or {}

    dash_country = (get_dashboard().get('countries') or {}).get(iso3) or {}
    out = {
        'iso3': iso3,
        'name': dash_country.get('name', iso3),
        'region': dash_country.get('region', ''),
        'horizon_years': horizon_years,
        'history': history,
        'default_events': events,
        'agency': {
            'sp': agency.get('sp'),
            'moodys': agency.get('moodys'),
            'fitch': agency.get('fitch'),
            'sp_num': agency.get('sp_num'),
            'consensus_num': agency.get('consensus_num'),
            'as_of': agency.get('as_of'),
        },
    }

    with _cache_lock:
        _cache[cache_key] = out
        _cache[f'{cache_key}_ts'] = time.time()
    return out


def get_table_rows() -> List[Dict]:
    """Compact list-of-dicts suitable for the dashboard table.

    Mirrors the Tellimer screenshot columns: country, region, PD 1y,
    PD 3y, PD 5y, model rating, agency consensus, shadow-debt gap.
    """
    dash = get_dashboard()
    rows: List[Dict] = []
    for iso3, c in (dash.get('countries') or {}).items():
        rating = c.get('rating') or {}
        composite = rating.get('composite') or {}
        agency = c.get('agency') or {}
        shadow = c.get('shadow_debt') or {}
        rows.append({
            'iso3': iso3,
            'name': c.get('name'),
            'region': c.get('region'),
            # Headline rating = fitted model output (or scaffold fallback)
            'score': rating.get('score'),
            'pm_notch': rating.get('pm_notch'),
            'sp_equiv': rating.get('sp_equiv'),
            'moodys_equiv': rating.get('moodys_equiv'),
            'source': rating.get('source'),
            'pd_1y': rating.get('pd_1y'),
            'pd_3y': rating.get('pd_3y'),
            'pd_5y': rating.get('pd_5y'),
            'defaulted': rating.get('defaulted', False),
            'is_investment_grade': rating.get('is_investment_grade'),
            # Reference composite score (separate from the fitted model)
            'composite_score': composite.get('score'),
            'composite_pm_notch': composite.get('pm_notch'),
            'composite_pd_1y': composite.get('pd_1y'),
            # Agency comparison
            'agency_sp': agency.get('sp'),
            'agency_moodys': agency.get('moodys'),
            'agency_fitch': agency.get('fitch'),
            'agency_consensus_num': agency.get('consensus_num'),
            'notch_delta_sp': (rating.get('notch_delta') or {}).get('sp'),
            # Shadow debt overlay
            'shadow_debt_gap_pp': shadow.get('debt_gap_pp'),
            'risk_tier': shadow.get('risk_tier'),
        })
    rows.sort(key=lambda r: (r['pd_1y'] is None, -(r['pd_1y'] or 0)))
    return rows


def _summarize(scored: Dict) -> Dict:
    countries = scored.get('countries') or {}
    pds: List[float] = []
    tier_counts: Dict[str, int] = {}
    in_default = 0
    coverage_total = 0
    coverage_count = 0
    for c in countries.values():
        rating = c.get('rating') or {}
        if rating.get('defaulted'):
            in_default += 1
        pd1 = rating.get('pd_1y')
        if pd1 is not None:
            pds.append(pd1)
        letter = rating.get('sp_letter')
        if letter:
            bucket = (
                'IG' if letter and letter[0:2] in ('AA', 'A+') or letter in ('AAA', 'A', 'A-', 'BBB+', 'BBB', 'BBB-')
                else 'HY'
            )
            tier_counts[bucket] = tier_counts.get(bucket, 0) + 1
        cov = rating.get('coverage')
        if cov is not None:
            coverage_total += cov
            coverage_count += 1
    avg_pd1 = round(sum(pds) / len(pds), 4) if pds else None
    avg_coverage = round(coverage_total / coverage_count, 2) if coverage_count else None
    return {
        'country_count': len(countries),
        'avg_pd_1y': avg_pd1,
        'in_default': in_default,
        'tier_counts': tier_counts,
        'avg_indicator_coverage': avg_coverage,
    }
