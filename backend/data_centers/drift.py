"""
Drift watcher: scan public news / press feeds for MW announcements that may
contradict our cached facility / market figures.

Best-effort. Output is a list of "signals" the admin can review — not a
ground-truth feed. We:

  1. Fetch a curated list of public URLs (Microsoft Source, DCD news,
     Data Center Frontier, Crusoe newsroom).
  2. Pull plain text from the HTML.
  3. Regex out "X MW" mentions.
  4. Match each mention's surrounding text against known facility names
     and market names.
  5. Persist signals to data/datacenter_drift.json.

The admin endpoint surfaces the latest signals; the daily scheduler keeps
them fresh.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import re
from typing import Any

import requests

from backend.data_centers import service

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data',
)
SIGNALS_PATH = os.path.join(DATA_DIR, 'datacenter_drift.json')

# Curated public URLs to monitor. Prefer RSS / Atom feeds where available
# (more stable, lighter, less bot-blocked); fall back to HTML index pages.
MONITOR_URLS = [
    'https://www.datacenterfrontier.com/rss.xml',
    'https://www.datacenterdynamics.com/en/feed/',
    'https://news.microsoft.com/source/feed/',
    'https://about.fb.com/news/category/data-centers/feed/',
    'https://www.crusoe.ai/resources/newsroom',
]

USER_AGENT = (
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/124.0.0.0 Safari/537.36'
)

# "<number> MW" / "<number> GW" with reasonable bounds (10–10000 MW after
# any GW→MW conversion). Allow single-digit numbers since "2 GW" is common.
_MW_RE = re.compile(r'(?<!\d)(\d{1,5}(?:[.,]\d+)?)\s*(MW|megawatt|GW|gigawatt)', re.IGNORECASE)
# Very rough HTML-to-text strip.
_TAG_RE = re.compile(r'<[^>]+>')
_WHITESPACE_RE = re.compile(r'\s+')


def _strip_html(html: str) -> str:
    text = _TAG_RE.sub(' ', html)
    return _WHITESPACE_RE.sub(' ', text).strip()


def _extract_mw_mentions(text: str) -> list[dict]:
    """Find each MW/GW number plus a small text window for context."""
    mentions = []
    seen = set()
    for m in _MW_RE.finditer(text):
        raw = m.group(1).replace(',', '')
        try:
            v = float(raw)
        except ValueError:
            continue
        is_gw = m.group(2).lower().startswith('g')
        mw = v * 1000 if is_gw else v
        if not (10 <= mw <= 10000):
            continue
        key = (round(mw, 0), m.start() // 50)
        if key in seen:
            continue
        seen.add(key)
        start = max(0, m.start() - 120)
        end = min(len(text), m.end() + 120)
        mentions.append({
            'mw': round(mw, 1),
            'unit': 'GW' if is_gw else 'MW',
            'context': text[start:end].strip(),
        })
    return mentions


def _matches(text: str, names: list[str]) -> list[str]:
    low = text.lower()
    out = []
    for n in names:
        # Use the first word of the facility name as a proxy + the full name.
        # E.g., "xAI Colossus Memphis" → match "xai colossus" or "memphis".
        first = n.split()[0].lower()
        if len(first) >= 3 and first in low and first not in {'the', 'data', 'and', 'for', 'new'}:
            out.append(n)
            continue
        if n.lower() in low:
            out.append(n)
    return out


def _facility_names() -> list[str]:
    if not service._CACHE.get('built'):
        service.build()
    return [f['name'] for f in service._CACHE.get('facilities', [])]


def _market_names() -> list[str]:
    if not service._CACHE.get('built'):
        service.build()
    return [m['market'] for m in service._CACHE.get('markets', [])]


def _fetch(url: str) -> tuple[str | None, str | None]:
    try:
        r = requests.get(url, timeout=15, headers={'User-Agent': USER_AGENT})
        r.raise_for_status()
        return r.text, None
    except Exception as e:
        return None, str(e)


def _flag_drift(mention: dict, matched_facilities: list[str]) -> dict | None:
    """If the mention's MW disagrees with our cached value for a matched
    facility by >20%, mark it as drift. Returns the drift entry or None."""
    if not matched_facilities:
        return None
    facs = service._CACHE.get('facilities', [])
    by_name = {f['name']: f for f in facs}
    for n in matched_facilities:
        f = by_name.get(n)
        if not f or not f.get('mw'):
            continue
        cached = float(f['mw'])
        observed = float(mention['mw'])
        delta = observed - cached
        if cached and abs(delta) / cached >= 0.2:
            return {
                'name': n,
                'cached_mw': cached,
                'observed_mw': observed,
                'delta_mw': round(delta, 1),
                'delta_pct': round(100 * delta / cached, 1),
            }
    return None


def scan() -> dict:
    """Run a single scan pass; return a summary and persist signals."""
    facility_names = _facility_names()
    market_names = _market_names()
    out: dict[str, Any] = {
        'scanned_at': _dt.datetime.utcnow().isoformat() + 'Z',
        'urls_scanned': 0,
        'urls_failed': [],
        'signals': [],
        'drift_flags': [],
    }
    for url in MONITOR_URLS:
        html, err = _fetch(url)
        if err:
            out['urls_failed'].append({'url': url, 'error': err})
            continue
        out['urls_scanned'] += 1
        text = _strip_html(html or '')
        mentions = _extract_mw_mentions(text)
        for m in mentions:
            ctx = m['context']
            matched_fac = _matches(ctx, facility_names)
            matched_mkt = _matches(ctx, market_names)
            if not matched_fac and not matched_mkt:
                continue
            entry = {
                'url': url,
                'mw': m['mw'],
                'unit': m['unit'],
                'context': ctx,
                'matched_facilities': matched_fac,
                'matched_markets': matched_mkt,
            }
            out['signals'].append(entry)
            df = _flag_drift(m, matched_fac)
            if df:
                df['url'] = url
                df['context'] = ctx
                out['drift_flags'].append(df)

    # Cap to keep file size reasonable.
    out['signals'] = out['signals'][:60]
    out['drift_flags'] = out['drift_flags'][:30]

    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(SIGNALS_PATH, 'w', encoding='utf-8') as f:
            json.dump(out, f, indent=2)
    except Exception as e:
        logger.warning(f'failed to persist drift signals: {e}')
    return out


def load_signals() -> dict:
    """Read the most recent drift scan output."""
    if not os.path.exists(SIGNALS_PATH):
        return {'scanned_at': None, 'signals': [], 'drift_flags': [], 'urls_scanned': 0}
    try:
        with open(SIGNALS_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f'failed to load drift signals: {e}')
        return {'scanned_at': None, 'signals': [], 'drift_flags': [], 'urls_scanned': 0}
