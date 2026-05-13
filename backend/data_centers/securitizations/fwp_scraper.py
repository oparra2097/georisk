"""
EDGAR Form FWP / 424B5 scraper for senior-tranche CUSIPs.

Each securitization issuer (Vantage Data Centers Issuer LLC, Aligned
Data Centers Issuer LLC, etc.) has a CIK on EDGAR.  When a series is
priced, the issuer files Form FWP (free-writing prospectus) which
contains the tranche table with CUSIPs.  Form 424B5 (prospectus
supplement) is the post-pricing equivalent.

Strategy:
  1. For each deal with an edgar_cik, fetch the issuer's submissions
     JSON and list FWP / 424B5 / 424B2 filings.
  2. Match each filing to the deal by series tag in the document (e.g.,
     "Series 2023-1") — most issuer CIKs cover all series under one
     master trust, so we have to disambiguate by series.
  3. Fetch the matching filing's primary document, regex-extract the
     CUSIP table (9-char alphanumeric, with Class A-2 / Senior Notes
     context).
  4. Persist results to data/_fwp_cusip_cache.json keyed by deal_id.

Honest limit: many DC ABS deals are 144A/Reg S private placements where
the FWP is filed but the CUSIP table may be in a non-machine-readable
exhibit.  We fall back gracefully and record "no CUSIP found in FWP"
rather than guessing.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import requests

logger = logging.getLogger(__name__)

USER_AGENT = (
    'ParraMacro Research data-centers admin@parramacro.com '
    '(SEC fair-access; identify and contact)'
)
HEADERS = {'User-Agent': USER_AGENT, 'Accept': 'application/json,text/html;q=0.9'}

_CUSIP_CACHE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    'data',
    '_fwp_cusip_cache.json',
)

# A CUSIP is 9 characters: 8 alphanumeric + 1 check digit (alphanumeric).
# Excludes ambiguous letters (I, O) per CUSIP rules; we accept anything
# alphanumeric and let the surrounding context filter false positives.
_CUSIP_RE = re.compile(r'\b([0-9A-Z]{8}[0-9A-Z])\b')

# Senior tranche labels we care about.  Order matters — most specific first.
_SENIOR_LABELS = [
    r'Class\s*A-2',
    r'Class\s*A-1FCF',          # Stack 2023-1 style: insured AAA tranche
    r'Senior\s*Notes',
    r'Class\s*A',                # fallback
]

# Pattern that finds every class label in an FWP tranche table.
# Matches: "Class A-1", "Class A-1FCF", "Class A-2", "Class B", "Class C", etc.
_CLASS_LABEL_RE = re.compile(
    r'Class\s+([A-D](?:-[12](?:FCF)?)?)\b', re.IGNORECASE,
)
_RATING_INLINE_RE = re.compile(
    r'\b(AAA|AA[+-]?|A[+-]?|BBB[+-]?|BB[+-]?|B[+-]?|CCC[+-]?|NR)\b'
)
_COUPON_RE = re.compile(r'(\d{1,2}\.\d{1,4})\s*%')
_SIZE_RE   = re.compile(r'\$\s*([\d,]+(?:\.\d+)?)\s*(?:million|MM|M\b)', re.IGNORECASE)


def _get(url: str, timeout: int = 30) -> tuple[Any, str | None]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None, f'HTTP {r.status_code}'
        return r, None
    except Exception as e:
        return None, str(e)


def list_filings(cik: str, forms: tuple[str, ...] = ('FWP', '424B5', '424B2')) -> dict:
    """Return all recent filings for a CIK matching the given form types."""
    cik_padded = cik.lstrip('0').zfill(10)
    url = f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
    r, err = _get(url)
    if err:
        return {'ok': False, 'error': err}
    try:
        j = r.json()
    except Exception as e:
        return {'ok': False, 'error': f'parse JSON: {e}'}
    recent = j.get('filings', {}).get('recent', {})
    matches = []
    for i, form in enumerate(recent.get('form', [])):
        if form not in forms:
            continue
        accession = recent['accessionNumber'][i]
        no_dashes = accession.replace('-', '')
        int_cik = str(int(cik))
        matches.append({
            'form':       form,
            'accession':  accession,
            'filed_date': recent['filingDate'][i],
            'doc_url':    f"https://www.sec.gov/Archives/edgar/data/{int_cik}/{no_dashes}/{recent['primaryDocument'][i]}",
        })
    return {'ok': True, 'cik': cik, 'company': j.get('name'), 'filings': matches}


def _extract_series_tag(deal_name: str) -> str | None:
    """Pull the series tag (e.g., '2023-1', '2024-2 / 2024-3') from a
    deal name so we can match it against filings."""
    m = re.search(r'Series\s*([0-9]{4}-[0-9A-Z]+(?:\s*/\s*[0-9]{4}-[0-9A-Z]+)?)', deal_name, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'\b(20\d{2}-[0-9A-Z]+)\b', deal_name)
    return m.group(1) if m else None


def find_senior_cusip(html: str) -> dict:
    """Locate the senior-tranche CUSIP in an FWP / 424B5 document."""
    # Strip tags, collapse whitespace.
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()

    for label_pat in _SENIOR_LABELS:
        for m in re.finditer(label_pat, text, re.IGNORECASE):
            # Look in a ~400-char window after the label for a CUSIP.
            window = text[m.end(): m.end() + 400]
            c = _CUSIP_RE.search(window)
            if c:
                return {
                    'ok':           True,
                    'cusip':        c.group(1),
                    'label_hit':    m.group(0),
                    'context':      text[max(0, m.start() - 40): m.end() + 80],
                }
    return {'ok': False, 'reason': 'no senior-label CUSIP found'}


def parse_tranche_table(html: str) -> list[dict]:
    """Extract every tranche we can find in an FWP / 424B5 document.

    Each tranche row contains a class label (Class A-1, Class A-2, Class
    A-1FCF, Class B, Class C, ...), and within a small window after the
    label we expect to find a CUSIP, a rating (AAA/AA/A/BBB/BB/B/...), a
    coupon (e.g., '4.50%'), and a tranche size ('$50 million').  The FWP
    geometry varies, so we capture each field independently from a 600-
    char window after the class label and let the admin review."""
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()

    tranches: list[dict] = []
    seen_classes: set[str] = set()
    for m in _CLASS_LABEL_RE.finditer(text):
        cls = m.group(1).upper().replace(' ', '')
        if cls in seen_classes:
            continue
        seen_classes.add(cls)

        window = text[m.end(): m.end() + 600]
        cusip = _CUSIP_RE.search(window)
        rating = _RATING_INLINE_RE.search(window)
        coupon = _COUPON_RE.search(window)
        size   = _SIZE_RE.search(window)

        # Skip rows that don't look like real tranche-table rows
        # (no CUSIP AND no rating AND no coupon → probably just a class
        # reference in prose).
        if not (cusip or rating or coupon):
            continue

        tranches.append({
            'class':   f'Class {cls}',
            'cusip':   cusip.group(1) if cusip else None,
            'rating':  rating.group(1) if rating else None,
            'coupon':  float(coupon.group(1)) if coupon else None,
            'size_usd_m': (
                round(float(size.group(1).replace(',', '')), 1)
                if size else None
            ),
            'context': text[max(0, m.start() - 20): m.end() + 200],
        })
    return tranches


def pull_for_deal(deal: dict) -> dict:
    cik = (deal.get('edgar_cik') or '').strip()
    if not cik:
        return {'ok': False, 'deal_id': deal.get('deal_id'),
                'error': 'no edgar_cik on this deal'}
    listing = list_filings(cik)
    if not listing.get('ok'):
        return {'ok': False, 'deal_id': deal.get('deal_id'),
                'cik': cik, 'error': listing.get('error')}

    series = _extract_series_tag(deal.get('deal_name', ''))
    # Find filings that mention this series tag in their document.
    candidates = listing.get('filings', [])
    if not candidates:
        return {'ok': False, 'deal_id': deal.get('deal_id'),
                'cik': cik, 'error': 'no FWP/424B filings found'}

    best = None
    best_tranches: list[dict] = []
    for f in candidates[:30]:  # check most-recent 30 max
        r, err = _get(f['doc_url'], timeout=45)
        if err:
            continue
        html = r.content.decode('utf-8', errors='ignore')
        if series and series.lower() not in html.lower():
            continue
        found = find_senior_cusip(html)
        if found.get('ok'):
            best = {**f, **found, 'series_matched': bool(series)}
            best_tranches = parse_tranche_table(html)
            break
    if not best:
        return {'ok': False, 'deal_id': deal.get('deal_id'),
                'cik': cik, 'series': series,
                'error': f'no CUSIP in {len(candidates)} candidate filings'}
    return {
        'ok':        True,
        'deal_id':   deal.get('deal_id'),
        'cik':       cik,
        'series':    series,
        'cusip':     best['cusip'],
        'form':      best['form'],
        'filed_date': best['filed_date'],
        'doc_url':   best['doc_url'],
        'label_hit': best['label_hit'],
        'context':   best['context'][:300],
        'tranches':  best_tranches,
    }


def pull_all() -> dict:
    """Pull senior-tranche CUSIPs for every deal with an edgar_cik tag."""
    from backend.data_centers.securitizations import service
    deals = service.get_deals()
    results = []
    cache = _load_cache()
    for d in deals:
        if not d.get('edgar_cik'):
            continue
        # Skip if we already have a verified CUSIP cached and the
        # deal's CIK hasn't changed.
        cached = cache.get(d['deal_id'])
        if cached and cached.get('cusip') and cached.get('cik') == d['edgar_cik']:
            results.append({**cached, 'from_cache': True})
            continue
        r = pull_for_deal(d)
        if r.get('ok'):
            cache[d['deal_id']] = r
        results.append(r)
    _save_cache(cache)
    return {
        'ok':       any(r.get('ok') for r in results),
        'attempted': len(results),
        'resolved':  sum(1 for r in results if r.get('ok')),
        'results':   results,
    }


def _load_cache() -> dict:
    if not os.path.exists(_CUSIP_CACHE):
        return {}
    try:
        with open(_CUSIP_CACHE, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cache(cache: dict) -> None:
    os.makedirs(os.path.dirname(_CUSIP_CACHE), exist_ok=True)
    with open(_CUSIP_CACHE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2, sort_keys=True)


def get_cached_cusip(deal_id: str) -> str | None:
    """Return the cached senior CUSIP for a deal if one was previously
    resolved successfully. Called by service.py at load time."""
    cache = _load_cache()
    e = cache.get(deal_id) or {}
    return e.get('cusip') if e.get('ok') else None


def get_cached_tranches(deal_id: str) -> list[dict]:
    """Return the cached capital-stack tranches for a deal, or []."""
    cache = _load_cache()
    e = cache.get(deal_id) or {}
    return e.get('tranches') or [] if e.get('ok') else []
