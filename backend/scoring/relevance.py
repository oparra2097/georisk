"""
Country-relevance filter for articles.

Every article returned by GDELT or a paid news API is passed through
`filter_articles_for_country(articles, alpha2)` before keyword analysis.
An article is kept when:

  * the target country (name or a known alias / capital / city / leader /
    org) appears in the TITLE,
    OR
  * it appears at least twice in title+description AND it is mentioned
    strictly more than any other country's aliases in the same text
    (i.e. the article is primarily about this country).

Ambiguous tokens (e.g. "Sudan" inside a South-Sudan query) are handled by
the `exclude_unless` map in country_aliases.json: a hit on the token is
ignored unless one of the required context words also appears in the text.

A title-hash dedupe pass collapses wire-service republications.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'static', 'data', 'country_aliases.json',
)

_load_lock = threading.Lock()
_aliases_raw: Optional[Dict[str, dict]] = None
_alias_regex: Dict[str, re.Pattern] = {}
_exclude_unless: Dict[str, Dict[str, List[str]]] = {}


def _compile_regex(aliases: List[str]) -> Optional[re.Pattern]:
    if not aliases:
        return None
    # Longest first so multi-word aliases beat single-word substrings
    parts = sorted({a for a in aliases if a and len(a) >= 3},
                   key=lambda s: -len(s))
    escaped = [re.escape(p) for p in parts]
    # \b works well for ASCII; for tokens containing punctuation (e.g. "U.S.")
    # we fall back to lookaround boundaries.
    pattern = r'(?<![A-Za-z0-9])(?:' + '|'.join(escaped) + r')(?![A-Za-z0-9])'
    return re.compile(pattern, re.IGNORECASE)


def _ensure_loaded() -> None:
    global _aliases_raw
    if _aliases_raw is not None:
        return
    with _load_lock:
        if _aliases_raw is not None:
            return
        try:
            with open(_DATA_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            logger.error('country_aliases.json not found at %s — filter '
                         'will pass everything through', _DATA_PATH)
            _aliases_raw = {}
            return
        for alpha2, entry in data.items():
            aliases = entry.get('aliases', [])
            rx = _compile_regex(aliases)
            if rx is not None:
                _alias_regex[alpha2] = rx
            if entry.get('exclude_unless'):
                _exclude_unless[alpha2] = entry['exclude_unless']
        _aliases_raw = data
        logger.info('Loaded relevance aliases for %d countries', len(data))


def _article_text(article) -> Tuple[str, str]:
    if isinstance(article, dict):
        title = (article.get('title') or '').strip()
        desc = (article.get('description') or '').strip()
        return title, desc
    if isinstance(article, str):
        return article, ''
    return '', ''


def _count_matches(rx: Optional[re.Pattern], text: str) -> int:
    if rx is None or not text:
        return 0
    return len(rx.findall(text))


def _apply_exclude_unless(alpha2: str, title: str, desc: str,
                          match_count: int) -> int:
    """If the only matches are ambiguous tokens whose required context is
    missing, zero out the match count.
    """
    rules = _exclude_unless.get(alpha2)
    if not rules or match_count == 0:
        return match_count
    full = f'{title}\n{desc}'.lower()
    # For each ambiguous token, require at least one context word
    for token, required in rules.items():
        token_rx = re.compile(
            r'(?<![A-Za-z0-9])' + re.escape(token) + r'(?![A-Za-z0-9])',
            re.IGNORECASE,
        )
        if not token_rx.search(full):
            continue
        if any(re.search(
                r'(?<![A-Za-z0-9])' + re.escape(ctx) + r'(?![A-Za-z0-9])',
                full, re.IGNORECASE) for ctx in required):
            continue
        # Context missing — drop the ambiguous matches by subtracting them
        match_count -= len(token_rx.findall(full))
    return max(0, match_count)


def is_relevant(article, alpha2: str) -> Tuple[bool, str]:
    """Return (keep?, reason) for a single article under moderate strictness."""
    _ensure_loaded()
    rx = _alias_regex.get(alpha2)
    if rx is None:
        # Unknown country — don't filter
        return True, 'no-aliases-known'
    title, desc = _article_text(article)
    if not title and not desc:
        return False, 'empty'

    title_hits = _apply_exclude_unless(alpha2, title, '',
                                       _count_matches(rx, title))
    if title_hits > 0:
        return True, 'title-hit'

    body_hits = _apply_exclude_unless(alpha2, title, desc,
                                      _count_matches(rx, f'{title} {desc}'))
    if body_hits < 2:
        return False, 'no-title-hit-and-body-hits<2'

    # Dominance check: target must out-mention any single other country
    own_rank = body_hits
    best_other = 0
    full = f'{title} {desc}'
    for other_a2, other_rx in _alias_regex.items():
        if other_a2 == alpha2:
            continue
        hits = _apply_exclude_unless(other_a2, title, desc,
                                     _count_matches(other_rx, full))
        if hits > best_other:
            best_other = hits
            if best_other >= own_rank:
                return False, f'out-mentioned-by-{other_a2}'
    return True, 'body-dominant'


def _dedupe_key(title: str) -> str:
    # Normalize: lowercase, strip punctuation, collapse whitespace
    key = re.sub(r'[^a-z0-9 ]+', '', title.lower())
    return re.sub(r'\s+', ' ', key).strip()


def filter_articles_for_country(articles, alpha2: str) -> List[dict]:
    """Filter + dedupe articles. Returns a new list preserving order."""
    _ensure_loaded()
    out = []
    seen_keys = set()
    dropped_irrelevant = 0
    dropped_dupe = 0
    for art in articles or []:
        title, _desc = _article_text(art)
        key = _dedupe_key(title) if title else ''
        if key and key in seen_keys:
            dropped_dupe += 1
            continue
        keep, _reason = is_relevant(art, alpha2)
        if not keep:
            dropped_irrelevant += 1
            continue
        if key:
            seen_keys.add(key)
        out.append(art)
    if articles:
        logger.debug(
            '%s relevance filter: kept=%d dropped_irrelevant=%d dropped_dupe=%d',
            alpha2, len(out), dropped_irrelevant, dropped_dupe,
        )
    return out
