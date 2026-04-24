"""
Dataclasses for Country Risk v2.

`to_dict()` output shape is kept parallel to backend/models.py:CountryRisk so
the existing frontend indicator-card components can render v2 payloads with
minimal changes.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional, Dict, Any


@dataclass
class SubScore:
    """A single 0-100 sub-score with its drivers."""
    name: str
    value: float
    weight: float
    drivers: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        return {
            'name': self.name,
            'value': round(self.value, 1),
            'weight': self.weight,
            'drivers': self.drivers,
        }


@dataclass
class CountryRiskV2:
    country_code: str
    country_name: str
    composite: float = 0.0
    structural: SubScore = None
    macro: SubScore = None
    labor: SubScore = None
    confidence: str = 'high'        # high | medium | low
    is_aggregate: bool = False
    members_included: List[str] = field(default_factory=list)
    data_asof: Dict[str, Optional[str]] = field(default_factory=dict)
    updated_at: Optional[datetime] = None

    def to_dict(self):
        return {
            'country_code': self.country_code,
            'country_name': self.country_name,
            'composite': round(self.composite, 1),
            'sub_scores': {
                'structural': self.structural.to_dict() if self.structural else None,
                'macro': self.macro.to_dict() if self.macro else None,
                'labor': self.labor.to_dict() if self.labor else None,
            },
            'confidence': self.confidence,
            'is_aggregate': self.is_aggregate,
            'members_included': self.members_included,
            'data_asof': self.data_asof,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }


@dataclass
class ShockSpec:
    """A single shock in a scenario request."""
    id: str
    magnitude: float

    @classmethod
    def from_dict(cls, d):
        return cls(id=d['id'], magnitude=float(d.get('magnitude', 0.0)))


@dataclass
class ScenarioResult:
    country_code: str
    base_score: float
    shocked_score: float
    contributions: List[Dict[str, Any]] = field(default_factory=list)
    transmission_detail: Dict[str, Any] = field(default_factory=dict)
    asof: Optional[datetime] = None

    def to_dict(self):
        return {
            'country_code': self.country_code,
            'base_score': round(self.base_score, 1),
            'shocked_score': round(self.shocked_score, 1),
            'delta': round(self.shocked_score - self.base_score, 1),
            'contributions': self.contributions,
            'transmission_detail': self.transmission_detail,
            'asof': self.asof.isoformat() if self.asof else None,
        }
