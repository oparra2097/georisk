"""
Shock scenario engine.

Loads `shocks_config.json` and exposes:
  - get_catalogue()      → shock list for UI
  - apply_scenario()     → delta_risk per shock + combined shocked score

v1 mechanics: delta_risk_points = elasticity * magnitude, per shock, summed.
Shocks are assumed independent (no cross-terms) in v1 — a fair approximation
for small-to-moderate moves. A future enhancement can chain via
`backend.data_sources.commodity_models.apply_shocks` so a DXY or supply shock
propagates through the commodity forecast into country risk.
"""

import json
import logging
import os
import threading
from datetime import datetime
from typing import Dict, List, Optional

from backend.country_risk_v2.models import ScenarioResult, ShockSpec

logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'shocks_config.json')
_lock = threading.RLock()
_config: Optional[dict] = None


def _load():
    global _config
    with _lock:
        if _config is None:
            with open(_CONFIG_PATH, 'r') as f:
                _config = json.load(f)
        return _config


def get_catalogue() -> List[dict]:
    """Public-facing shock catalogue (no elasticities)."""
    cfg = _load()
    return [
        {k: v for k, v in s.items() if not k.startswith('_')}
        for s in cfg.get('shocks', [])
    ]


def _get_shock_spec(shock_id: str) -> Optional[dict]:
    for s in _load().get('shocks', []):
        if s['id'] == shock_id:
            return s
    return None


def _get_elasticity(country_code: str, variable: str) -> Optional[float]:
    row = _load().get('elasticities', {}).get(country_code.upper())
    if row is None:
        return None
    return row.get(variable)


def supported_countries() -> List[str]:
    return sorted(_load().get('elasticities', {}).keys())


def apply_scenario(country_code: str, base_score: float, shocks: List[ShockSpec]) -> ScenarioResult:
    """
    Compute shocked_score = base_score + sum(elasticity_i * magnitude_i), clamped [0, 100].

    Unknown shock IDs are recorded with delta=0 and elasticity=None so callers
    can surface the error in the UI without the whole call failing.
    """
    code = country_code.upper()
    contributions = []
    total_delta = 0.0

    for shock in shocks:
        spec = _get_shock_spec(shock.id)
        if spec is None:
            contributions.append({
                'shock_id': shock.id,
                'magnitude': shock.magnitude,
                'elasticity': None,
                'delta': 0.0,
                'error': 'unknown shock id',
            })
            continue

        variable = spec['variable']
        # If caller sent magnitude=0 and there's a default, use the default.
        magnitude = shock.magnitude if shock.magnitude != 0 else spec.get('default_magnitude', 0.0)
        elasticity = _get_elasticity(code, variable)

        if elasticity is None:
            contributions.append({
                'shock_id': shock.id,
                'variable': variable,
                'magnitude': magnitude,
                'elasticity': None,
                'delta': 0.0,
                'error': f'no elasticity configured for {code}/{variable}',
            })
            continue

        delta = elasticity * magnitude
        contributions.append({
            'shock_id': shock.id,
            'variable': variable,
            'magnitude': magnitude,
            'unit': spec.get('unit'),
            'elasticity': elasticity,
            'delta': round(delta, 2),
        })
        total_delta += delta

    shocked = max(0.0, min(100.0, base_score + total_delta))

    return ScenarioResult(
        country_code=code,
        base_score=base_score,
        shocked_score=shocked,
        contributions=contributions,
        transmission_detail={
            'model': 'linear_independent_v1',
            'note': _load().get('_note', ''),
        },
        asof=datetime.utcnow(),
    )


def reload_config():
    """Force-reload the JSON (useful after editing elasticities)."""
    global _config
    with _lock:
        _config = None
    return _load()
