"""
SEC EDGAR ABS-EE (asset-level data) ingestor.

Public US ABS deals file periodic Form ABS-EE and Form 10-D distribution
reports. We pull the issuer's latest filings, parse the distribution
report to get current balance / delinquency / payments, and update the
corresponding row in data/datacenter_abs_deals.csv.

The issuer CIKs are the per-deal trust CIKs (e.g., Vantage Data Centers
Issuer LLC has its own CIK). They're stored in the CSV's `notes` field
keyed as "edgar_cik=XXX" so the admin can populate them progressively.

SEC fair-access policy: real User-Agent required.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import requests

logger = logging.getLogger(__name__)

USER_AGENT = (
    'ParraMacro Research data-centers admin@parramacro.com '
    '(SEC fair-access; identify and contact)'
)
HEADERS = {'User-Agent': USER_AGENT, 'Accept': 'application/json,text/html;q=0.9'}

_CIK_RE = re.compile(r'edgar_cik\s*=\s*(\d+)', re.IGNORECASE)


def _extract_cik(notes: str) -> str | None:
    m = _CIK_RE.search(notes or '')
    return m.group(1) if m else None


def _get(url: str, timeout: int = 20) -> tuple[Any, str | None]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None, f'HTTP {r.status_code}'
        return r, None
    except Exception as e:
        return None, str(e)


def latest_10d(cik: str) -> dict:
    """Find the most recent Form 10-D (distribution report) for an issuer CIK."""
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
    forms = recent.get('form', [])
    for i, form in enumerate(forms):
        if form == '10-D':
            accession = recent['accessionNumber'][i]
            primary = recent['primaryDocument'][i]
            filed = recent['filingDate'][i]
            int_cik = str(int(cik))
            no_dashes = accession.replace('-', '')
            doc_url = f'https://www.sec.gov/Archives/edgar/data/{int_cik}/{no_dashes}/{primary}'
            return {'ok': True, 'cik': cik, 'company': j.get('name'),
                    'accession': accession, 'filed_date': filed, 'doc_url': doc_url}
    return {'ok': False, 'error': 'no 10-D found in recent filings'}


# Heuristic regexes for the distribution report. ABS-EE format is XML but
# the 10-D HTML cover page typically restates the headline figures.
_BALANCE_RE = re.compile(
    r'(?:current|outstanding|aggregate)\s*(?:note|certificate|principal)?\s*balance[^$0-9]*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:million)?',
    re.IGNORECASE,
)
_DELINQ_RE = re.compile(
    r'(?:60|90)\s*\+?\s*days?\s*delinquen[ct]y?[^0-9]*(\d+(?:\.\d+)?)\s*%',
    re.IGNORECASE,
)


def parse_distribution(html: str) -> dict:
    """Pull headline numbers from a 10-D distribution report HTML.
    Best-effort regex; returns what it can find."""
    out: dict[str, Any] = {}
    # Strip tags for regex over plain text
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()

    m = _BALANCE_RE.search(text)
    if m:
        try:
            bal = float(m.group(1).replace(',', ''))
            # Heuristic: if the matched number is < 100,000 assume it's in
            # $M already; else assume raw dollars and convert.
            out['current_balance_usd_m'] = round(bal if bal < 100000 else bal / 1e6, 1)
        except ValueError:
            pass

    m = _DELINQ_RE.search(text)
    if m:
        try:
            out['delinquency_pct'] = round(float(m.group(1)), 2)
        except ValueError:
            pass

    out['text_excerpt'] = text[:500]
    return out


def pull_for_deal(deal: dict) -> dict:
    cik = _extract_cik(deal.get('notes', ''))
    if not cik:
        return {'ok': False, 'error': 'no edgar_cik in deal notes', 'deal_id': deal.get('deal_id')}
    meta = latest_10d(cik)
    if not meta.get('ok'):
        return {'ok': False, 'error': meta.get('error'), 'deal_id': deal.get('deal_id'), 'cik': cik}
    r, err = _get(meta['doc_url'], timeout=30)
    if err:
        return {'ok': False, 'error': f'fetch 10-D: {err}', 'deal_id': deal.get('deal_id'), 'meta': meta}
    try:
        html = r.content.decode('utf-8', errors='ignore')
    except Exception as e:
        return {'ok': False, 'error': f'decode: {e}', 'deal_id': deal.get('deal_id')}
    parsed = parse_distribution(html)
    return {
        'ok': True,
        'deal_id':       deal.get('deal_id'),
        'cik':           cik,
        'filed_date':    meta['filed_date'],
        'doc_url':       meta['doc_url'],
        'parsed':        parsed,
    }


def pull_all() -> dict:
    """Pull latest 10-D for every deal that has an edgar_cik tag in its notes."""
    from backend.data_centers.securitizations import service
    deals = service.get_deals()
    results = []
    for d in deals:
        if not _extract_cik(d.get('notes', '')):
            continue
        results.append(pull_for_deal(d))
    return {
        'ok': any(r.get('ok') for r in results),
        'pulled': len(results),
        'results': results,
    }
