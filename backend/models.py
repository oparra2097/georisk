from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional


@dataclass
class IndicatorScore:
    political_stability: float = 0.0
    military_conflict: float = 0.0
    economic_sanctions: float = 0.0
    protests_civil_unrest: float = 0.0
    terrorism: float = 0.0
    diplomatic_tensions: float = 0.0

    def to_dict(self):
        return asdict(self)


@dataclass
class CountryRisk:
    country_code: str
    country_name: str
    composite_score: float = 0.0
    indicators: IndicatorScore = field(default_factory=IndicatorScore)
    headline_count: int = 0
    gdelt_event_count: int = 0
    avg_tone: float = 0.0
    updated_at: Optional[datetime] = None
    trend: List[float] = field(default_factory=list)

    def to_dict(self):
        return {
            'country_code': self.country_code,
            'country_name': self.country_name,
            'composite': round(self.composite_score, 1),
            'indicators': self.indicators.to_dict(),
            'headline_count': self.headline_count,
            'gdelt_event_count': self.gdelt_event_count,
            'avg_tone': round(self.avg_tone, 2),
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'trend': [round(t, 1) for t in self.trend]
        }


@dataclass
class NewsArticle:
    title: str
    description: str
    url: str
    source: str
    published_at: str
    country_code: str = ''
    matched_indicators: List[str] = field(default_factory=list)

    def to_dict(self):
        return {
            'title': self.title,
            'description': self.description,
            'url': self.url,
            'source': self.source,
            'publishedAt': self.published_at,
            'country_code': self.country_code,
            'matched_indicators': self.matched_indicators
        }
