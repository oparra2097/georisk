import threading
from datetime import datetime
from typing import Dict, List, Optional
from backend.models import CountryRisk, NewsArticle


class RiskDataStore:
    """Thread-safe in-memory store for all risk data."""

    _instance = None
    _lock_cls = threading.Lock()

    def __new__(cls):
        with cls._lock_cls:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._lock = threading.RLock()
        self._scores: Dict[str, CountryRisk] = {}
        self._headlines: Dict[str, List[NewsArticle]] = {}
        self._global_headlines: List[NewsArticle] = []
        self._last_refresh: Optional[datetime] = None
        self._initialized = True

    def update_country(self, country_code: str, risk: CountryRisk):
        with self._lock:
            self._scores[country_code] = risk

    def get_country(self, country_code: str) -> Optional[CountryRisk]:
        with self._lock:
            return self._scores.get(country_code)

    def get_all_scores(self) -> Dict[str, CountryRisk]:
        with self._lock:
            return dict(self._scores)

    def get_hotspots(self, threshold: int = 70) -> List[CountryRisk]:
        with self._lock:
            return [r for r in self._scores.values()
                    if r.composite_score >= threshold]

    def update_headlines(self, country_code: str, articles: List[NewsArticle]):
        with self._lock:
            self._headlines[country_code] = articles

    def get_headlines(self, country_code: str) -> List[NewsArticle]:
        with self._lock:
            return self._headlines.get(country_code, [])

    def set_global_headlines(self, articles: List[NewsArticle]):
        with self._lock:
            self._global_headlines = articles

    def get_global_headlines(self) -> List[NewsArticle]:
        with self._lock:
            return list(self._global_headlines)

    def set_last_refresh(self, dt: datetime):
        with self._lock:
            self._last_refresh = dt

    def get_last_refresh(self) -> Optional[datetime]:
        with self._lock:
            return self._last_refresh

    def country_count(self) -> int:
        with self._lock:
            return len(self._scores)


store = RiskDataStore()
