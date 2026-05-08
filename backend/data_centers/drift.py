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

# SEC EDGAR fair-access policy requires identifying ourselves.
SEC_USER_AGENT = (
    'ParraMacro Research data-centers admin@parramacro.com '
    '(SEC fair-access; identify and contact)'
)
SEC_HEADERS = {'User-Agent': SEC_USER_AGENT, 'Accept': 'application/json,text/html;q=0.9'}

# Hyperscaler CIKs to scan for recent 8-K filings.  Material capex
# announcements (e.g. "we're expanding our X campus by $Y billion") often
# land here before the press release fully circulates.
HYPERSCALER_CIKS = {
    'MSFT':  '0000789019',
    'META':  '0001326801',
    'GOOGL': '0001652044',
    'AMZN':  '0001018724',
    'AAPL':  '0000320193',
    'ORCL':  '0001341439',
}
SEC_8K_LOOKBACK_DAYS = 30

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


def _scan_sec_8k(facility_names: list[str], market_names: list[str]) -> dict:
    """Scan recent 8-K filings from each hyperscaler CIK for MW / data center
    mentions. Returns {signals, urls_scanned, urls_failed} same shape as the
    RSS scan so results can be merged."""
    signals = []
    urls_scanned = 0
    urls_failed = []
    cutoff = _dt.datetime.utcnow().date() - _dt.timedelta(days=SEC_8K_LOOKBACK_DAYS)

    for ticker, cik in HYPERSCALER_CIKS.items():
        cik_padded = cik.lstrip('0').zfill(10)
        sub_url = f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
        try:
            r = requests.get(sub_url, headers=SEC_HEADERS, timeout=20)
            if r.status_code != 200:
                urls_failed.append({'url': sub_url, 'error': f'HTTP {r.status_code}'})
                continue
            j = r.json()
        except Exception as e:
            urls_failed.append({'url': sub_url, 'error': str(e)})
            continue
        urls_scanned += 1

        recent = j.get('filings', {}).get('recent', {})
        forms = recent.get('form', [])
        # Iterate recent 8-Ks within the lookback window.
        for i, form in enumerate(forms):
            if form != '8-K':
                continue
            filed = recent['filingDate'][i]
            try:
                filed_dt = _dt.date.fromisoformat(filed)
            except ValueError:
                continue
            if filed_dt < cutoff:
                continue
            accession = recent['accessionNumber'][i]
            primary   = recent['primaryDocument'][i]
            int_cik   = str(int(cik))
            no_dashes = accession.replace('-', '')
            doc_url   = f'https://www.sec.gov/Archives/edgar/data/{int_cik}/{no_dashes}/{primary}'
            try:
                d = requests.get(doc_url, headers=SEC_HEADERS, timeout=30)
                if d.status_code != 200:
                    urls_failed.append({'url': doc_url, 'error': f'HTTP {d.status_code}'})
                    continue
                text = _strip_html(d.content.decode('utf-8', errors='ignore'))
            except Exception as e:
                urls_failed.append({'url': doc_url, 'error': str(e)})
                continue
            urls_scanned += 1

            # Only emit a signal if the 8-K text contains data-center
            # context AND a numeric MW/GW reference.  Pure governance /
            # earnings 8-Ks are skipped.
            low = text.lower()
            if not any(k in low for k in ('data center', 'datacenter', 'data-center',
                                            'ai infrastructure', 'cloud capacity',
                                            'compute capacity')):
                continue
            mentions = _extract_mw_mentions(text)
            for m in mentions:
                ctx = m['context']
                matched_fac = _matches(ctx, facility_names)
                matched_mkt = _matches(ctx, market_names)
                signals.append({
                    'url':    doc_url,
                    'ticker': ticker,
                    'filed':  filed,
                    'mw':     m['mw'],
                    'unit':   m['unit'],
                    'context': ctx,
                    'matched_facilities': matched_fac,
                    'matched_markets':    matched_mkt,
                    'is_8k': True,
                })
    return {'signals': signals, 'urls_scanned': urls_scanned, 'urls_failed': urls_failed}


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

    # Layer in SEC 8-K scan (hyperscaler material event filings).
    sec = _scan_sec_8k(facility_names, market_names)
    out['urls_scanned'] += sec['urls_scanned']
    out['urls_failed'].extend(sec['urls_failed'])
    for s in sec['signals']:
        # If the 8-K matches one of our cached facilities, run the same
        # ≥20% drift check we apply to RSS signals.
        if s['matched_facilities']:
            df = _flag_drift({'mw': s['mw']}, s['matched_facilities'])
            if df:
                df['url'] = s['url']
                df['context'] = s['context']
                df['source'] = f'SEC 8-K · {s["ticker"]} · {s["filed"]}'
                out['drift_flags'].append(df)
        out['signals'].append(s)

    # Cap to keep file size reasonable.
    out['signals'] = out['signals'][:80]
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
