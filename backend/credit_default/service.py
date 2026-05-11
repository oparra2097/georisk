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
from backend.credit_default import agency_ratings_history as cd_agency_history
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

    # Option-C: no CRAG-default override here. The GBM is now trained
    # with in-default rows kept (defaulted_within_h = 1 for ongoing
    # spells), so it learns the in-default macro signature natively
    # and outputs high PD for LBN / VEN / BLR without external pinning.
    # CRAG events still feed the model as labels + the features
    # years_since_default / region_default_rate; here we only attach
    # agency-rating metadata and the post-hoc anchor pull (UI overlay,
    # doesn't touch model PD).

    for iso3, c in countries.items():
        a = agencies.get(iso3)

        if not a:
            continue
        c['agency'] = a

        # ── Agency-anchor pull (Fitch-style Qualitative Overlay) ──
        # When the model and the consensus agency rating are 3+ notches
        # apart, pull the headline rating halfway toward the agency,
        # capped so the post-pull gap is at most ±2 notches. Frontier
        # sovereigns like ARG/LKA/PAK/GHA/ZMB/ETH/UKR/RUS systematically
        # rate too lenient out of the macro-only GBM because the panel
        # is missing FX-debt share, EMBI spreads, IMF-program status,
        # and gross-financing-needs (see research notes — Fitch SRM,
        # S&P methodology, IMF SRDSF). The pull is a calibration
        # overlay, not a signal change: contributions, scores and PDs
        # are preserved; only the displayed letter / pm_notch shifts.
        if not c.get('rating', {}).get('defaulted'):
            _apply_agency_anchor_pull(c, a)

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


def _apply_agency_anchor_pull(country: Dict, agency: Dict) -> None:
    """Pull a 3+ notch model-vs-agency disagreement halfway toward the
    agency consensus, capped at ±2 notches post-pull. Mutates
    ``country['rating']`` in place.

    Always preserves the pre-pull values as ``raw_pm_notch /
    raw_pm_numeric / raw_sp_equiv / raw_moodys_equiv`` so the UI can
    show the un-anchored model output side-by-side with the anchored
    headline. ``anchor_pull`` carries the same provenance for the
    cases where we actually pulled.

    Activation: ``abs(model_num - consensus_num) >= 3``. When the gap
    is < 3 we still snapshot the raw fields; just don't mutate the
    headline."""
    rating = country.get('rating') or {}
    model_num = rating.get('pm_numeric')
    # Snapshot the pre-pull rating regardless of whether we pull —
    # frontend can compare side-by-side.
    rating['raw_pm_notch'] = rating.get('pm_notch')
    rating['raw_pm_numeric'] = rating.get('pm_numeric')
    rating['raw_sp_equiv'] = rating.get('sp_equiv')
    rating['raw_moodys_equiv'] = rating.get('moodys_equiv')

    consensus = agency.get('consensus_num')
    if model_num is None or consensus is None:
        return
    delta = int(model_num) - int(consensus)
    if abs(delta) < 3:
        return
    # Halfway pull (round toward agency), then strict ±2 cap on the
    # post-pull gap regardless of how far the raw model sits from
    # the agency. Without the symmetric cap, halfway-pull from a
    # large gap (e.g. CHN raw BB- vs A+ → delta+8) lands at +4 — the
    # earlier asymmetric clamp prevented overshoot but didn't
    # enforce ±2 on the harsh side. raw_pm_notch is preserved so the
    # underlying model output is still visible on the country detail
    # panel.
    if delta < 0:
        pulled = int(model_num) + ((-delta) // 2)
    else:
        pulled = int(model_num) - (delta // 2)
    # Clamp to [consensus-2, consensus+2] — strict ±2 cap.
    pulled = max(int(consensus) - 2, min(int(consensus) + 2, pulled))
    pulled = max(1, min(20, pulled))
    if pulled == int(model_num):
        return
    bucket = next(
        (b for b in rating_model.RATING_BUCKETS if b[2] == pulled), None,
    )
    if not bucket:
        return
    rating['pm_notch'] = bucket[1]
    rating['pm_numeric'] = pulled
    rating['sp_equiv'] = bucket[3]
    rating['moodys_equiv'] = bucket[4]
    rating['is_investment_grade'] = pulled <= rating_model.IG_BOUNDARY_NUMERIC
    rating['anchor_pull'] = {
        'from': int(model_num), 'to': pulled, 'consensus': int(consensus),
    }


def get_dashboard(force_refresh: bool = False, cadence: str = 'annual',
                  horizon: int = 1) -> Dict:
    """Build (or return cached) dashboard payload.

    cadence='annual' uses ``fit_state_h{horizon}.json`` (horizon in years,
    default 1). cadence='quarterly' uses ``fit_state_q{horizon}.json``
    (horizon in quarters, conventionally 4/12/20 for 1y/3y/5y).
    """
    cache_key = f'dashboard_{cadence}_{horizon}'
    with _cache_lock:
        cached = _cache.get(cache_key)
        cached_ts = _cache.get(f'{cache_key}_ts', 0)
    if cached and not force_refresh and (time.time() - cached_ts) < _CACHE_TTL:
        return cached

    panel = cd_data.get_panel(force_refresh=force_refresh)
    scored = rating_model.score_panel(panel, horizon_years=horizon, cadence=cadence)
    enriched = _enrich_with_agencies(scored)

    summary = _summarize(enriched)
    enriched['summary'] = summary

    with _cache_lock:
        _cache[cache_key] = enriched
        _cache[f'{cache_key}_ts'] = time.time()
    return enriched


def get_country(iso3: str, cadence: str = 'annual',
                horizon: int = 1) -> Optional[Dict]:
    iso3 = (iso3 or '').upper()
    if len(iso3) != 3:
        return None
    dash = get_dashboard(cadence=cadence, horizon=horizon)
    return (dash.get('countries') or {}).get(iso3)


def get_country_history(iso3: str, horizon_years: int = 1,
                        cadence: str = 'annual') -> Optional[Dict]:
    """Re-score one country's macro panel year-by-year using the current
    fit_state, alongside CRAG default events. Used by the dashboard's
    drilldown chart to back-test whether the model would have flagged
    the country before each historical default (e.g. Ghana 2022).
    """
    iso3 = (iso3 or '').upper()
    if len(iso3) != 3 or not cd_data._is_sovereign_iso(iso3):
        return None

    cache_key = f'history_{cadence}_{iso3}_h{horizon_years}'
    with _cache_lock:
        cached = _cache.get(cache_key)
        cached_ts = _cache.get(f'{cache_key}_ts', 0)
    if cached and (time.time() - cached_ts) < _CACHE_TTL:
        return cached

    if cadence == 'quarterly':
        fit_state = cd_fit.load_state_quarterly(horizon_years)
    else:
        fit_state = cd_fit.load_state(horizon_years)
    if not fit_state or not fit_state.get('coefficients'):
        return None
    coefs = fit_state.get('coefficients') or {}
    intercept = float(fit_state.get('intercept') or 0.0)
    scaler = fit_state.get('scaler') or {}
    medians = fit_state.get('medians') or {}
    raw_shift = float(fit_state.get('class_balance_log_odds') or 0.0)
    if cadence == 'quarterly':
        years_eq = max(1, int(round(horizon_years / 4)))
    else:
        years_eq = horizon_years
    class_shift = rating_model._adjusted_shift(raw_shift, years_eq)
    # Reserve-currency logit discount, see rating_model._reserve_currency_shift.
    panel_country = (cd_data.get_panel().get('countries') or {}).get(iso3) or {}
    class_shift += rating_model._reserve_currency_shift(panel_country)
    rb = fit_state.get('rating_buckets') or {}
    cal_global = rb.get('buckets') if isinstance(rb, dict) else rb
    # Per-region buckets — see rating_model._score_panel for rationale.
    cal_by_region = (rb.get('by_region') or {}) if isinstance(rb, dict) else {}
    country_region = panel_country.get('region') or ''
    cal_buckets = cal_by_region.get(country_region) or cal_global

    # If the persisted fit is a GBM with a saved tree-ensemble pickle,
    # score the per-period history through the actual model so the chart
    # shows the real GBM trajectory (the linear-importance fallback
    # collapsed everything to ~base-rate PD).
    gbm_payload = None
    if fit_state.get('estimator') == 'gbm' and fit_state.get('model_pickle'):
        if cadence == 'quarterly':
            loaded = cd_fit.load_gbm_model_quarterly(horizon_years)
        else:
            loaded = cd_fit.load_gbm_model(horizon_years)
        if loaded:
            gbm_payload = {'model': loaded[0], 'features': loaded[1]}

    if cadence == 'quarterly':
        panel_df = cd_data.get_history_panel_quarterly()
    else:
        panel_df = cd_data.get_history_panel()
    if panel_df is None or panel_df.empty:
        return None
    sub = panel_df[panel_df['iso3'] == iso3].copy()
    if sub.empty:
        return None
    sub = sub.sort_values('year')

    # In-default years per (iso3, year): if the country was inside an
    # active CRAG hard-default spell that period, the historical PD on
    # the chart should pin to 100% / sp_equiv=D — same semantics as the
    # Option-C: no per-year PD pinning on the chart. The trained GBM
    # learns the in-default macro signature so the model PD itself
    # rises during a spell, naturally producing the bend on the
    # chart. CRAG events are still rendered as red default-event
    # bands by the frontend (default_events payload below) for
    # context — those are *display* annotations, not score overrides.

    import math
    history = []
    for _, row in sub.iterrows():
        proba = None
        if gbm_payload is not None:
            # Build standardized feature vector and run predict_proba.
            # Wrapped because sklearn version mismatches between the
            # build and runtime environments cause predict_proba to
            # raise — we fall back to the linear-coef path below
            # rather than 500-ing the chart endpoint.
            vec = []
            for feat in gbm_payload['features']:
                raw = row.get(feat)
                try:
                    raw_f = float(raw)
                    if raw_f != raw_f:
                        raw_f = None
                except (TypeError, ValueError):
                    raw_f = None
                if raw_f is None:
                    raw_f = medians.get(feat)
                if raw_f is None:
                    vec.append(0.0)
                    continue
                sc = scaler.get(feat) or {}
                mean = float(sc.get('mean', 0.0))
                std = float(sc.get('std', 1.0)) or 1.0
                zf = (raw_f - mean) / std
                if zf > rating_model.Z_CLIP:
                    zf = rating_model.Z_CLIP
                elif zf < -rating_model.Z_CLIP:
                    zf = -rating_model.Z_CLIP
                vec.append(zf)
            try:
                proba = float(gbm_payload['model'].predict_proba([vec])[0, 1])
            except Exception as e:  # noqa: BLE001
                print(f'[credit_default.service] history GBM predict_proba failed for {iso3}: {e}')
                gbm_payload = None  # disable for the rest of this call
                proba = None
        if proba is not None:
            proba = min(max(proba, 1e-9), 1.0 - 1e-9)
            bal_logit = math.log(proba / (1.0 - proba))
            adj = bal_logit + class_shift
        else:
            z = intercept
            for feat, coef in coefs.items():
                if not coef:
                    continue
                raw = row.get(feat)
                try:
                    raw_f = float(raw)
                    if raw_f != raw_f:
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
            adj = z + class_shift
        try:
            model_pd = 1.0 / (1.0 + math.exp(-adj))
        except OverflowError:
            model_pd = 0.0 if adj < 0 else 1.0
        score = 100.0 * model_pd

        # Composite reference score (0-100, weighted-z fundamentals
        # view, no GBM / no Platt rescale / no reserve-currency shift).
        # Same formula as rating_model._score_panel: weighted sign·z
        # sum → sigmoid → log-odds re-mapped onto a 0-100 scale where
        # 50 ≈ panel median. Useful as a leading-indicator overlay on
        # the chart: when the composite trajectory diverges from PD
        # (rising composite, flat PD), fundamentals are deteriorating
        # ahead of what the GBM can yet pick up.
        c_w_sum = 0.0
        c_w_total = 0.0
        for feat, w in rating_model.WEIGHTS.items():
            raw = row.get(feat)
            try:
                raw_f = float(raw)
                if raw_f != raw_f:
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
            sign = 1.0 if rating_model.HIGHER_IS_WORSE.get(feat, True) else -1.0
            c_w_sum += float(w) * sign * zf
            c_w_total += float(w)
        composite_score = None
        if c_w_total > 0:
            c_norm = c_w_sum / c_w_total
            try:
                c_pd = 1.0 / (1.0 + math.exp(-c_norm))
            except OverflowError:
                c_pd = 0.0 if c_norm < 0 else 1.0
            c_pd = min(max(c_pd, 1e-6), 1.0 - 1e-6)
            composite_score = max(0.0, min(100.0, 50.0 + 38.4 * math.log10(c_pd / (1.0 - c_pd))))

        rating = rating_model._letter_and_pd(
            score, defaulted=False, calibrated_buckets=cal_buckets,
        )
        record = {
            'year': int(row['year']),
            'model_pd': round(model_pd, 5),
            'model_score': round(score, 3),
            'composite_score': round(composite_score, 2) if composite_score is not None else None,
            'pm_notch': rating['pm_notch'],
            'pm_numeric': rating['pm_numeric'],
            'sp_equiv': rating['sp_equiv'],
        }
        if cadence == 'quarterly':
            q = int(row.get('quarter') or 1)
            record['quarter'] = q
            record['period'] = f"{int(row['year'])}Q{q}"
        else:
            record['period'] = str(int(row['year']))
        history.append(record)

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

    dash_country = (
        get_dashboard(cadence=cadence, horizon=horizon_years).get('countries') or {}
    ).get(iso3) or {}
    out = {
        'iso3': iso3,
        'name': dash_country.get('name', iso3),
        'region': dash_country.get('region', ''),
        'horizon_years': horizon_years,
        'cadence': cadence,
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
        'agency_history': cd_agency_history.get_country_history(iso3),
    }

    with _cache_lock:
        _cache[cache_key] = out
        _cache[f'{cache_key}_ts'] = time.time()
    return out


def get_table_rows(cadence: str = 'annual', horizon: int = 1) -> List[Dict]:
    """Compact list-of-dicts suitable for the dashboard table.

    Mirrors the Tellimer screenshot columns: country, region, PD 1y,
    PD 3y, PD 5y, model rating, agency consensus, shadow-debt gap.
    """
    dash = get_dashboard(cadence=cadence, horizon=horizon)
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
            'raw_pm_notch': rating.get('raw_pm_notch'),
            'raw_sp_equiv': rating.get('raw_sp_equiv'),
            'anchor_pull': rating.get('anchor_pull'),
            'source': rating.get('source'),
            'pd_1y': rating.get('pd_1y'),
            'pd_3y': rating.get('pd_3y'),
            'pd_5y': rating.get('pd_5y'),
            'pd_onset_1y': rating.get('pd_onset_1y'),
            'pd_onset_3y': rating.get('pd_onset_3y'),
            'pd_onset_5y': rating.get('pd_onset_5y'),
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
