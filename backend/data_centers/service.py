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
_CACHE: dict[str, Any] = {'built': False, 'markets': [], 'national': {}}

_CSV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data',
    'datacenter_markets.csv',
)

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
        'thresholds': {
            'spec_ratio_hot': SPEC_RATIO_HOT,
            'pipeline_ratio_hot': PIPELINE_RATIO_HOT,
        },
    }


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

    return {
        'national': nat,
        'by_tier': sorted(by_tier.values(), key=lambda x: -x['inventory_mw']),
        'top_overbuild': top_overbuild,
        'top_concentration': top_concentration,
        'top_pipeline_share': top_pipeline_share,
    }
