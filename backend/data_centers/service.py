"""
US Data Center market service.

Loads the seed market table (data/datacenter_markets.csv), computes risk
metrics, and serves them to the /api/data-centers/* blueprint.

Risk framing
------------
- inventory_mw            operational/built capacity
- under_construction_mw   active builds (typically 12–24 months out)
- planned_mw              announced but not yet under construction
- preleased_pct           % of UC capacity already pre-leased to a tenant
- vacancy_pct             current operational vacancy

Derived per market:
  pipeline_mw             UC + planned
  pipeline_ratio          pipeline_mw / inventory_mw  (expansion intensity)
  speculative_uc_mw       UC * (1 - preleased)  (unleased near-term supply)
  spec_ratio              speculative_uc_mw / inventory_mw  (overbuild risk)
  inventory_share         market inventory / national inventory
  pipeline_share          market pipeline  / national pipeline

CBRE tier definitions (per CBRE Global Data Center Trends, primary/secondary
plus emerging markets the firm tracks separately):
  primary    7 largest established markets
  secondary  10 mature but smaller markets
  emerging   recently-active build-out markets
"""

from __future__ import annotations

import csv
import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_CACHE: dict[str, Any] = {'built': False, 'markets': [], 'national': {}, 'facilities': []}

_CSV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data',
    'datacenter_markets.csv',
)
_FACILITIES_CSV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data',
    'datacenter_facilities.csv',
)

# Funding-source taxonomy.  Top-level buckets so the map can color cleanly.
FUNDING_TYPES = {
    'hyperscaler':      'Hyperscaler self-fund',
    'reit':             'Public REIT',
    'infra_fund':       'Infra fund / PE',
    'sovereign_jv':     'Sovereign / strategic JV',
    'specialty_pe':     'Specialty / venture',
    'public_specialty': 'Public specialty',
}

VALID_STATUSES = {'built', 'under_construction', 'planned'}

# ── Tenant normalization ─────────────────────────────────────────────────
# The CSV stores free-text tenants. Bucket multi-tenant strings and TBD
# values so concentration rollups read cleanly.
_MULTI_TENANT_TOKENS = {
    'multiple', 'multiple hyperscalers', 'multiple enterprise',
    'colo', 'multi-tenant', 'multi tenant', 'mixed',
}
_UNLEASED_TOKENS = {'tbd', 'unknown', 'n/a', '', 'none'}


def _normalize_tenant(raw: str) -> str:
    s = (raw or '').strip()
    low = s.lower()
    if low in _UNLEASED_TOKENS:
        return 'Unleased'
    if low in _MULTI_TENANT_TOKENS or low.startswith('multiple'):
        return 'Colo (multi-tenant)'
    return s


# ── Stranded-asset risk model ─────────────────────────────────────────────
#
# Drivers, weighted into a 0–100 composite score per facility:
#   1. Tenant concentration   single-named tenant > unleased > colo
#   2. Tenant credit quality  capacity-burning tenants riskier than IG mega-caps
#   3. Speculative build      unleased UC/planned riskier than locked & built
#   4. Funding-source         high-leverage PE > sovereign > REIT > hyperscaler
#                              (who absorbs the writedown)
#   5. Geographic correlation facilities in metros with high spec_ratio share
#                              correlated demand-bust risk
#
# `at_risk_mw` is an illustrative expected-writedown estimate: facility MW
# times a default-probability matrix on (tenant credit tier, status). It is
# the point estimate for a "demand normalizes" (not crash, not boom) scenario
# and is rolled up by funding source so investors can size their exposure.

TENANT_CREDIT_TIER = {
    'Microsoft': 'investment_grade',
    'Meta':      'investment_grade',
    'Google':    'investment_grade',
    'Amazon':    'investment_grade',
    'Apple':     'investment_grade',
    'Oracle':    'investment_grade',
    'OpenAI':    'high_growth_unprofitable',
    'xAI':       'venture_backed',
    'Colo (multi-tenant)': 'diversified',
    'Unleased':            'speculative',
}
TENANT_TIER_LABEL = {
    'investment_grade':         'Investment grade',
    'high_growth_unprofitable': 'High-growth, unprofitable',
    'venture_backed':           'Venture-backed',
    'diversified':              'Diversified colo',
    'speculative':              'Speculative (unleased)',
    'unknown':                  'Unknown',
}

_CREDIT_RISK = {  # 0–20
    'investment_grade':         3,
    'high_growth_unprofitable': 14,
    'venture_backed':           20,
    'diversified':              5,
    'speculative':              17,
    'unknown':                  10,
}

_FUNDING_RESILIENCE = {  # 0–10  (higher = investor more exposed)
    'hyperscaler':       1,
    'reit':              3,
    'infra_fund':        8,
    'sovereign_jv':      5,
    'specialty_pe':     10,
    'public_specialty':  4,
}

# Tenant balance-sheet stretch: how exposed is the tenant's reported
# capacity-build to its own free-cash-flow generation?  Manually-set
# scores grounded in published 2024–2025 capex guidance and FCF runs.
# Higher = thinner cushion if AI demand disappoints.
#
# fcf_b   ≈ trailing FCF (USD billions)
# capex_b ≈ committed/guided AI-related capex (USD billions, annualized)
# stretch ≈ 0–10 score (manually weighted)
TENANT_FCF_HEADROOM = {
    'Microsoft': {'fcf_b':  74, 'capex_b':  85, 'stretch': 1.5},
    'Google':    {'fcf_b':  73, 'capex_b':  75, 'stretch': 2.0},
    'Meta':      {'fcf_b':  52, 'capex_b':  65, 'stretch': 3.0},
    'Amazon':    {'fcf_b':  35, 'capex_b': 100, 'stretch': 4.0},
    'Apple':     {'fcf_b': 110, 'capex_b':   8, 'stretch': 0.5},
    'Oracle':    {'fcf_b':  15, 'capex_b':  35, 'stretch': 7.5},
    'OpenAI':    {'fcf_b':  -5, 'capex_b':  50, 'stretch': 9.0},
    'xAI':       {'fcf_b':  -3, 'capex_b':  15, 'stretch':10.0},
}
_DEFAULT_STRETCH = 5.0  # for Colo / Unleased / unknown tenants

# ── Implied credit profile per tenant ─────────────────────────────────────
# Annual default probability is illustrative. Spread = annual_pd × LGD × 1e4
# with LGD ≈ 60% (40% recovery on senior unsecured / lease obligations).
# Hyperscalers anchor at AAA-equivalent; OpenAI / xAI use private-equity
# implied ratings.  Colo blends across enterprise tenants.
TENANT_CREDIT_PROFILE = {
    'Microsoft': {'rating': 'AAA',  'annual_pd': 0.0005, 'spread_bps':   3},
    'Apple':     {'rating': 'AAA',  'annual_pd': 0.0005, 'spread_bps':   3},
    'Google':    {'rating': 'AA+',  'annual_pd': 0.0008, 'spread_bps':   5},
    'Amazon':    {'rating': 'AA',   'annual_pd': 0.0020, 'spread_bps':  12},
    'Meta':      {'rating': 'A+',   'annual_pd': 0.0025, 'spread_bps':  15},
    'Oracle':    {'rating': 'BBB+', 'annual_pd': 0.0050, 'spread_bps':  30},
    'OpenAI':    {'rating': 'B-',   'annual_pd': 0.0500, 'spread_bps': 300},
    'xAI':       {'rating': 'CCC',  'annual_pd': 0.1200, 'spread_bps': 720},
    'Colo (multi-tenant)': {'rating': 'BB blended', 'annual_pd': 0.0150, 'spread_bps': 90},
}
_DEFAULT_CREDIT = {'rating': 'NR', 'annual_pd': None, 'spread_bps': None}


def _credit_profile(tenant_norm: str) -> dict:
    return TENANT_CREDIT_PROFILE.get(tenant_norm, _DEFAULT_CREDIT)


# Three demand-environment scenarios.  Each maps (tenant tier, status) →
# probability that the facility is written down.  The point estimate
# reported in the summary KPI is moderate; the slider re-runs the rollup
# under mild and severe matrices.
_SCENARIOS = {
    'mild': {  # AI capex normalizes — modest air comes out
        'label': 'Mild — capex normalizes',
        'investment_grade':         {'built': 0.00, 'under_construction': 0.01, 'planned': 0.03},
        'high_growth_unprofitable': {'built': 0.04, 'under_construction': 0.15, 'planned': 0.30},
        'venture_backed':           {'built': 0.10, 'under_construction': 0.25, 'planned': 0.40},
        'diversified':              {'built': 0.00, 'under_construction': 0.05, 'planned': 0.10},
        'speculative':              {'built': 0.08, 'under_construction': 0.30, 'planned': 0.50},
        'unknown':                  {'built': 0.02, 'under_construction': 0.10, 'planned': 0.20},
    },
    'moderate': {  # demand neutralizes — base case
        'label': 'Moderate — demand neutralizes',
        'investment_grade':         {'built': 0.00, 'under_construction': 0.03, 'planned': 0.08},
        'high_growth_unprofitable': {'built': 0.10, 'under_construction': 0.30, 'planned': 0.55},
        'venture_backed':           {'built': 0.20, 'under_construction': 0.50, 'planned': 0.70},
        'diversified':              {'built': 0.02, 'under_construction': 0.10, 'planned': 0.20},
        'speculative':              {'built': 0.15, 'under_construction': 0.55, 'planned': 0.80},
        'unknown':                  {'built': 0.05, 'under_construction': 0.20, 'planned': 0.40},
    },
    'severe': {  # AI demand crash — broad unwind
        'label': 'Severe — AI demand crash',
        'investment_grade':         {'built': 0.03, 'under_construction': 0.10, 'planned': 0.20},
        'high_growth_unprofitable': {'built': 0.25, 'under_construction': 0.60, 'planned': 0.80},
        'venture_backed':           {'built': 0.40, 'under_construction': 0.75, 'planned': 0.90},
        'diversified':              {'built': 0.08, 'under_construction': 0.25, 'planned': 0.40},
        'speculative':              {'built': 0.30, 'under_construction': 0.80, 'planned': 0.95},
        'unknown':                  {'built': 0.15, 'under_construction': 0.40, 'planned': 0.60},
    },
}
SCENARIO_KEYS = ('mild', 'moderate', 'severe')
DEFAULT_SCENARIO = 'moderate'

# Texas-grid markets used by the ERCOT stress predicate.
_ERCOT_MARKETS = {'Dallas-Fort Worth', 'Houston', 'Austin-San Antonio'}

# Named stress tests overlay a per-facility modifier on top of the moderate
# baseline.  Each entry has a `predicate` (which facilities are affected),
# and either a `multiplier` or an additive `add` applied to the baseline
# default probability.  Ordering preserves UI presentation order.
_STRESS_TESTS = {
    'stress_openai': {
        'label':       'Stress · OpenAI revenue plan misses 50%',
        'description': 'OpenAI revenue ramp halves; Stargate-anchored facilities at-risk × 1.8.',
        'base':        'moderate',
        'predicate':   lambda f: f.get('tenant_norm') == 'OpenAI',
        'multiplier':  1.8,
    },
    'stress_hyperscaler_pause': {
        'label':       'Stress · Hyperscaler capex pause (2Q)',
        'description': 'Investment-grade tenants pause spec UC for 2 quarters; UC/planned at-risk × 1.6.',
        'base':        'moderate',
        'predicate':   lambda f: f.get('tenant_credit_tier') == 'investment_grade'
                                  and f.get('status') != 'built',
        'multiplier':  1.6,
    },
    'stress_ercot': {
        'label':       'Stress · ERCOT power crisis',
        'description': 'Texas grid capacity crunch; ERCOT-region facilities + 0.15 default prob.',
        'base':        'moderate',
        'predicate':   lambda f: f.get('market') in _ERCOT_MARKETS,
        'add':         0.15,
    },
}
STRESS_KEYS = tuple(_STRESS_TESTS.keys())
ALL_SCENARIO_KEYS = SCENARIO_KEYS + STRESS_KEYS


def _writedown_prob(facility: dict, scenario_key: str) -> float:
    """Probability of writedown for the given facility under the given scenario."""
    tier = facility['tenant_credit_tier']
    status = facility['status']
    if scenario_key in _SCENARIOS:
        return _SCENARIOS[scenario_key][tier][status]
    if scenario_key in _STRESS_TESTS:
        st = _STRESS_TESTS[scenario_key]
        base = _SCENARIOS[st['base']][tier][status]
        if st['predicate'](facility):
            if 'multiplier' in st:
                base = min(0.95, base * st['multiplier'])
            elif 'add' in st:
                base = min(0.95, base + st['add'])
        return base
    return _SCENARIOS[DEFAULT_SCENARIO][tier][status]


def _credit_tier(tenant_norm: str) -> str:
    return TENANT_CREDIT_TIER.get(tenant_norm, 'unknown')


def _facility_risk(f: dict, market_spec_ratio: float) -> dict:
    tn = f['tenant_norm']
    tier = _credit_tier(tn)

    # 1. Tenant concentration (0-30)
    if tn == 'Colo (multi-tenant)':
        c_concentration = 5
    elif tn == 'Unleased':
        c_concentration = 22
    else:
        c_concentration = 30

    # 2. Tenant credit quality (0-20)
    c_credit = _CREDIT_RISK[tier]

    # 3. Speculative build (0-15) — locked-in built capacity scores 0
    is_locked = tier in ('investment_grade', 'diversified')
    if f['status'] == 'planned':
        c_spec = 8 if is_locked else 15
    elif f['status'] == 'under_construction':
        c_spec = 4 if is_locked else 12
    else:
        c_spec = 0

    # 4. Funding-source / capital structure (0-10)
    c_funding = _FUNDING_RESILIENCE.get(f['funding_type'], 5)

    # 5. Geographic correlation (0-10) — facility's metro spec_ratio scaled
    c_geo = max(0.0, min(10.0, (market_spec_ratio or 0.0) * 25.0))

    # 6. Tenant balance-sheet stretch (0-15) — committed AI capex vs. tenant FCF
    headroom = TENANT_FCF_HEADROOM.get(tn, {})
    stretch = headroom.get('stretch', _DEFAULT_STRETCH)
    c_stretch = round(stretch * 1.5, 1)  # 0–10 stretch → 0–15 component

    score = round(min(100.0, c_concentration + c_credit + c_spec + c_funding + c_geo + c_stretch), 1)

    # Tenant credit profile (rating + PD + spread) — used in tooltip.
    credit = _credit_profile(tn)

    # Per-scenario at-risk MW (covers demand environments AND named stress tests).
    f_with_tier = {**f, 'tenant_credit_tier': tier}
    prob_by_scen = {sk: _writedown_prob(f_with_tier, sk) for sk in ALL_SCENARIO_KEYS}
    at_risk_by_scenario = {sk: round(f['mw'] * p, 1) for sk, p in prob_by_scen.items()}

    return {
        'tenant_credit_tier': tier,
        'tenant_credit_label': TENANT_TIER_LABEL[tier],
        'tenant_rating':      credit['rating'],
        'tenant_annual_pd':   credit['annual_pd'],
        'tenant_spread_bps':  credit['spread_bps'],
        'stranded_risk': score,
        'risk_drivers': {
            'tenant_concentration':   c_concentration,
            'tenant_credit':          c_credit,
            'speculative_build':      c_spec,
            'funding_resilience':     c_funding,
            'geographic_correlation': round(c_geo, 1),
            'tenant_stretch':         c_stretch,
        },
        'tenant_fcf_b':   headroom.get('fcf_b'),
        'tenant_capex_b': headroom.get('capex_b'),
        'writedown_prob_by_scenario': prob_by_scen,
        'at_risk_mw_by_scenario': at_risk_by_scenario,
        # Legacy: keep moderate scenario fields for back-compat
        'writedown_prob': _SCENARIOS[DEFAULT_SCENARIO][tier][f['status']],
        'at_risk_mw': at_risk_by_scenario[DEFAULT_SCENARIO],
    }

# Risk thresholds — used both for color scaling and for flagging "watch" markets.
# spec_ratio > 0.35  ≈ unleased near-term supply > 35% of installed base.
# pipeline_ratio > 1.0 ≈ announced+UC capacity exceeds existing inventory.
SPEC_RATIO_HOT = 0.35
PIPELINE_RATIO_HOT = 1.0


def _to_float(s: str, default: float = 0.0) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def _load_csv() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(_CSV_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            rows.append({
                'market': r['market'],
                'tier': r['tier'].lower(),
                'lat': _to_float(r['lat']),
                'lon': _to_float(r['lon']),
                'inventory_mw': _to_float(r['inventory_mw']),
                'under_construction_mw': _to_float(r['under_construction_mw']),
                'planned_mw': _to_float(r['planned_mw']),
                'preleased_pct': _to_float(r['preleased_pct']),
                'vacancy_pct': _to_float(r['vacancy_pct']),
                'power_note': r.get('power_note', ''),
            })
    return rows


def _load_facilities_csv() -> list[dict[str, Any]]:
    if not os.path.exists(_FACILITIES_CSV_PATH):
        return []
    rows: list[dict[str, Any]] = []
    with open(_FACILITIES_CSV_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            status = (r.get('status') or '').strip().lower()
            funding = (r.get('funding_type') or '').strip().lower()
            rows.append({
                'name': r['name'],
                'market': r['market'],
                'lat': _to_float(r['lat']),
                'lon': _to_float(r['lon']),
                'status': status if status in VALID_STATUSES else 'planned',
                'mw': _to_float(r.get('mw', 0)),
                'operator': r.get('operator', ''),
                'developer': r.get('developer', ''),
                'funding_type': funding if funding in FUNDING_TYPES else 'infra_fund',
                'funding_detail': r.get('funding_detail', ''),
                'tenant': r.get('tenant', ''),
                'tenant_norm': _normalize_tenant(r.get('tenant', '')),
                'announced_year': r.get('announced_year', ''),
                'target_online': r.get('target_online', ''),
                'notes': r.get('notes', ''),
            })
    return rows


def _enrich(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_inventory = sum(r['inventory_mw'] for r in rows) or 1.0
    total_uc = sum(r['under_construction_mw'] for r in rows)
    total_planned = sum(r['planned_mw'] for r in rows)
    total_pipeline = total_uc + total_planned or 1.0

    enriched = []
    for r in rows:
        inv = r['inventory_mw']
        uc = r['under_construction_mw']
        planned = r['planned_mw']
        prelease = r['preleased_pct'] / 100.0

        pipeline = uc + planned
        spec_uc = uc * (1.0 - prelease)
        pipeline_ratio = pipeline / inv if inv > 0 else 0.0
        spec_ratio = spec_uc / inv if inv > 0 else 0.0

        # Composite watch score 0–100. Weights chosen so a market that is
        # both highly speculative AND adding lots of pipeline lands in red.
        watch = min(100.0, 60.0 * (spec_ratio / SPEC_RATIO_HOT) +
                            40.0 * (pipeline_ratio / PIPELINE_RATIO_HOT))

        enriched.append({
            **r,
            'pipeline_mw': round(pipeline, 1),
            'pipeline_ratio': round(pipeline_ratio, 3),
            'speculative_uc_mw': round(spec_uc, 1),
            'spec_ratio': round(spec_ratio, 3),
            'inventory_share': round(inv / total_inventory, 4),
            'pipeline_share': round(pipeline / total_pipeline, 4),
            'watch_score': round(watch, 1),
        })

    enriched.sort(key=lambda x: x['watch_score'], reverse=True)

    return {
        'markets': enriched,
        'national': {
            'inventory_mw': round(total_inventory, 1),
            'under_construction_mw': round(total_uc, 1),
            'planned_mw': round(total_planned, 1),
            'pipeline_mw': round(total_uc + total_planned, 1),
            'pipeline_ratio': round((total_uc + total_planned) / total_inventory, 3),
            'market_count': len(rows),
        },
    }


def build(force: bool = False) -> dict[str, Any]:
    with _LOCK:
        if _CACHE['built'] and not force:
            return _CACHE
        try:
            rows = _load_csv()
            data = _enrich(rows)
            _CACHE['markets'] = data['markets']
            _CACHE['national'] = data['national']

            # Build a market->spec_ratio lookup so we can compute the
            # geographic-correlation component of facility risk.
            spec_by_market = {m['market']: m['spec_ratio'] for m in data['markets']}

            facilities = _load_facilities_csv()
            for f in facilities:
                f.update(_facility_risk(f, spec_by_market.get(f['market'], 0.0)))

            _CACHE['facilities'] = facilities
            _CACHE['built'] = True
            _CACHE['build_error'] = None
        except Exception as e:
            logger.exception('data_centers build failed')
            _CACHE['build_error'] = str(e)
            _CACHE['built'] = False
        return _CACHE


def status() -> dict[str, Any]:
    return {
        'built': _CACHE.get('built', False),
        'build_error': _CACHE.get('build_error'),
        'market_count': len(_CACHE.get('markets', [])),
        'facility_count': len(_CACHE.get('facilities', [])),
        'funding_types': FUNDING_TYPES,
        'thresholds': {
            'spec_ratio_hot': SPEC_RATIO_HOT,
            'pipeline_ratio_hot': PIPELINE_RATIO_HOT,
        },
    }


def get_facilities(
    status: str | None = None,
    funding_type: str | None = None,
    market: str | None = None,
    tenant: str | None = None,
    developer: str | None = None,
) -> list[dict[str, Any]]:
    if not _CACHE.get('built'):
        build()
    fac = _CACHE.get('facilities', [])
    if status:
        fac = [f for f in fac if f['status'] == status.lower()]
    if funding_type:
        fac = [f for f in fac if f['funding_type'] == funding_type.lower()]
    if market:
        fac = [f for f in fac if f['market'] == market]
    if tenant:
        fac = [f for f in fac if f['tenant_norm'] == tenant]
    if developer:
        fac = [f for f in fac if f['developer'] == developer]
    return fac


def get_markets(tier: str | None = None) -> list[dict[str, Any]]:
    if not _CACHE.get('built'):
        build()
    markets = _CACHE.get('markets', [])
    if tier:
        markets = [m for m in markets if m['tier'] == tier.lower()]
    return markets


def get_summary() -> dict[str, Any]:
    if not _CACHE.get('built'):
        build()
    markets = _CACHE.get('markets', [])
    nat = _CACHE.get('national', {})

    by_tier: dict[str, dict[str, float]] = {}
    for m in markets:
        t = m['tier']
        b = by_tier.setdefault(t, {
            'tier': t, 'count': 0,
            'inventory_mw': 0.0, 'under_construction_mw': 0.0,
            'planned_mw': 0.0, 'speculative_uc_mw': 0.0,
        })
        b['count'] += 1
        b['inventory_mw'] += m['inventory_mw']
        b['under_construction_mw'] += m['under_construction_mw']
        b['planned_mw'] += m['planned_mw']
        b['speculative_uc_mw'] += m['speculative_uc_mw']

    for b in by_tier.values():
        inv = b['inventory_mw'] or 1.0
        b['pipeline_mw'] = round(b['under_construction_mw'] + b['planned_mw'], 1)
        b['pipeline_ratio'] = round(b['pipeline_mw'] / inv, 3)
        b['spec_ratio'] = round(b['speculative_uc_mw'] / inv, 3)
        for k in ('inventory_mw', 'under_construction_mw', 'planned_mw', 'speculative_uc_mw'):
            b[k] = round(b[k], 1)

    top_overbuild = sorted(markets, key=lambda x: x['spec_ratio'], reverse=True)[:5]
    top_concentration = sorted(markets, key=lambda x: x['inventory_share'], reverse=True)[:5]
    top_pipeline_share = sorted(markets, key=lambda x: x['pipeline_share'], reverse=True)[:5]

    # ─── Facility-level roll-ups ────────────────────────────────────────
    facilities = _CACHE.get('facilities', [])
    by_funding: dict[str, dict[str, Any]] = {}
    by_developer: dict[str, dict[str, Any]] = {}
    by_tenant: dict[str, dict[str, Any]] = {}
    fac_total_mw = sum(f['mw'] for f in facilities) or 1.0
    total_at_risk_by_scenario = {
        s: sum(f.get('at_risk_mw_by_scenario', {}).get(s, 0.0) for f in facilities)
        for s in ALL_SCENARIO_KEYS
    }
    total_at_risk_mw = total_at_risk_by_scenario[DEFAULT_SCENARIO]

    def _zero_scen(): return {s: 0.0 for s in ALL_SCENARIO_KEYS}

    for f in facilities:
        f_at_risk = f.get('at_risk_mw_by_scenario', {})
        ft = f['funding_type']
        b = by_funding.setdefault(ft, {
            'funding_type': ft,
            'label': FUNDING_TYPES.get(ft, ft),
            'count': 0, 'mw': 0.0,
            'built_mw': 0.0, 'uc_mw': 0.0, 'planned_mw': 0.0,
            'at_risk_mw': 0.0,
            'at_risk_mw_by_scenario': _zero_scen(),
        })
        b['count'] += 1
        b['mw'] += f['mw']
        b['at_risk_mw'] += f.get('at_risk_mw', 0.0)
        for s in ALL_SCENARIO_KEYS:
            b['at_risk_mw_by_scenario'][s] += f_at_risk.get(s, 0.0)
        if f['status'] == 'built':              b['built_mw']  += f['mw']
        elif f['status'] == 'under_construction': b['uc_mw']     += f['mw']
        else:                                   b['planned_mw'] += f['mw']

        dev = f['developer'] or 'Unknown'
        d = by_developer.setdefault(dev, {
            'developer': dev, 'count': 0, 'mw': 0.0,
            'funding_type': f['funding_type'],
        })
        d['count'] += 1
        d['mw'] += f['mw']

        tn = f['tenant_norm'] or 'Unleased'
        t = by_tenant.setdefault(tn, {
            'tenant': tn, 'count': 0, 'mw': 0.0,
            'built_mw': 0.0, 'uc_mw': 0.0, 'planned_mw': 0.0,
            'at_risk_mw': 0.0,
            'at_risk_mw_by_scenario': _zero_scen(),
            'tenant_credit_tier': f.get('tenant_credit_tier', 'unknown'),
            'tenant_fcf_b': f.get('tenant_fcf_b'),
            'tenant_capex_b': f.get('tenant_capex_b'),
            'tenant_rating': f.get('tenant_rating'),
            'tenant_annual_pd': f.get('tenant_annual_pd'),
            'tenant_spread_bps': f.get('tenant_spread_bps'),
        })
        t['count'] += 1
        t['mw'] += f['mw']
        t['at_risk_mw'] += f.get('at_risk_mw', 0.0)
        for s in ALL_SCENARIO_KEYS:
            t['at_risk_mw_by_scenario'][s] += f_at_risk.get(s, 0.0)
        if f['status'] == 'built':              t['built_mw']  += f['mw']
        elif f['status'] == 'under_construction': t['uc_mw']     += f['mw']
        else:                                   t['planned_mw'] += f['mw']

    for b in by_funding.values():
        b['share'] = round(b['mw'] / fac_total_mw, 4)
        b['at_risk_share'] = round(b['at_risk_mw'] / b['mw'], 3) if b['mw'] else 0.0
        b['at_risk_share_by_scenario'] = {
            s: round(b['at_risk_mw_by_scenario'][s] / b['mw'], 3) if b['mw'] else 0.0
            for s in ALL_SCENARIO_KEYS
        }
        for s in ALL_SCENARIO_KEYS:
            b['at_risk_mw_by_scenario'][s] = round(b['at_risk_mw_by_scenario'][s], 1)
        for k in ('mw', 'built_mw', 'uc_mw', 'planned_mw', 'at_risk_mw'):
            b[k] = round(b[k], 1)

    for d in by_developer.values():
        d['share'] = round(d['mw'] / fac_total_mw, 4)
        d['mw'] = round(d['mw'], 1)

    for t in by_tenant.values():
        t['share'] = round(t['mw'] / fac_total_mw, 4)
        t['at_risk_share'] = round(t['at_risk_mw'] / t['mw'], 3) if t['mw'] else 0.0
        t['at_risk_share_by_scenario'] = {
            s: round(t['at_risk_mw_by_scenario'][s] / t['mw'], 3) if t['mw'] else 0.0
            for s in ALL_SCENARIO_KEYS
        }
        for s in ALL_SCENARIO_KEYS:
            t['at_risk_mw_by_scenario'][s] = round(t['at_risk_mw_by_scenario'][s], 1)
        for k in ('mw', 'built_mw', 'uc_mw', 'planned_mw', 'at_risk_mw'):
            t[k] = round(t[k], 1)

    # Top stranded-risk facilities (sorted by mw-weighted score, since a small
    # very-risky building matters less than a large moderately-risky one).
    top_risk = sorted(
        facilities,
        key=lambda x: x.get('stranded_risk', 0) * x.get('mw', 0),
        reverse=True,
    )[:8]

    return {
        'national': nat,
        'by_tier': sorted(by_tier.values(), key=lambda x: -x['inventory_mw']),
        'top_overbuild': top_overbuild,
        'top_concentration': top_concentration,
        'top_pipeline_share': top_pipeline_share,
        'facility_total_mw': round(fac_total_mw, 1),
        'expected_writedown_mw': round(total_at_risk_mw, 1),
        'expected_writedown_share': round(total_at_risk_mw / fac_total_mw, 3) if fac_total_mw else 0.0,
        'expected_writedown_mw_by_scenario': {
            s: round(total_at_risk_by_scenario[s], 1) for s in ALL_SCENARIO_KEYS
        },
        'expected_writedown_share_by_scenario': {
            s: round(total_at_risk_by_scenario[s] / fac_total_mw, 3) if fac_total_mw else 0.0
            for s in ALL_SCENARIO_KEYS
        },
        'by_funding': sorted(by_funding.values(), key=lambda x: -x['at_risk_mw']),
        'top_developers': sorted(by_developer.values(), key=lambda x: -x['mw'])[:12],
        'by_tenant': sorted(by_tenant.values(), key=lambda x: -x['mw']),
        'top_stranded_risk': top_risk,
        'tenant_tier_labels': TENANT_TIER_LABEL,
        'scenarios': {
            **{s: _SCENARIOS[s]['label'] for s in SCENARIO_KEYS},
            **{s: _STRESS_TESTS[s]['label'] for s in STRESS_KEYS},
        },
        'scenario_groups': {
            'demand_environment': list(SCENARIO_KEYS),
            'stress_tests': list(STRESS_KEYS),
        },
        'scenario_descriptions': {
            **{s: '' for s in SCENARIO_KEYS},
            **{s: _STRESS_TESTS[s]['description'] for s in STRESS_KEYS},
        },
        'default_scenario': DEFAULT_SCENARIO,
    }
