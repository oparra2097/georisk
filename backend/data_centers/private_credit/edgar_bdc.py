"""
EDGAR Business Development Company (BDC) Schedule-of-Investments scraper.

Public BDCs (Blackstone BCRED, Blue Owl OBDC, FS KKR Capital, Ares Capital,
Apollo Debt Solutions, etc.) file quarterly 10-Q and annual 10-K reports
with EDGAR.  Each filing contains a Consolidated Schedule of Investments
listing every portfolio loan: borrower name, industry, security type
(first-lien / second-lien / mezz), coupon, maturity, principal /
amortised cost / fair value.

This module pulls the latest 10-Q for each BDC and matches portfolio
companies against the curated data-center operator dictionary
(data/dc_private_credit_operators.csv).  Matches are exposed as
private-credit "rows" alongside the manually-curated parent-level
financings in dc_private_credit.csv.

Limits:
  - Schedules are large HTML tables; row geometry varies between BDCs.
    The parser uses a "find the operator name, walk forward for the
    next ~6 dollar figures" heuristic that's correct most of the time
    but should be admin-reviewed before treating as ground truth.
  - Form PF (the rest of the private-credit universe) is NOT public.
    This scraper covers only the publicly-traded BDC slice.
"""

from __future__ import annotations

import csv
import io
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

# ── BDC universe ──────────────────────────────────────────────────────
# (CIK, ticker, sponsor, label) — covers the largest BDCs by AUM that
# are most likely to hold DC operator paper.
BDCS = [
    ('0001736035', 'BXSL',  'Blackstone',   'Blackstone Secured Lending Fund'),
    ('0001803498', 'BCRED', 'Blackstone',   'Blackstone Private Credit Fund'),
    ('0001655888', 'OBDC',  'Blue Owl',     'Blue Owl Capital Corp (OBDC)'),
    ('0001422183', 'FSK',   'KKR',          'FS KKR Capital Corp'),
    ('0001287750', 'ARCC',  'Ares',         'Ares Capital Corp'),
    ('0001845815', 'ADS',   'Apollo',       'Apollo Debt Solutions BDC'),
    ('0001476765', 'GBDC',  'Golub',        'Golub Capital BDC Inc'),
]

_OPERATORS_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    'data',
    'dc_private_credit_operators.csv',
)


def _load_operator_patterns() -> list[dict[str, Any]]:
    """Load the operator-name dictionary used to grep BDC schedules."""
    if not os.path.exists(_OPERATORS_CSV):
        return []
    out = []
    with open(_OPERATORS_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            patterns = [p.strip() for p in (r.get('match_patterns') or '').split(';') if p.strip()]
            if not patterns:
                continue
            # Compile each pattern as a word-boundary regex; case-insensitive.
            compiled = [re.compile(r'\b' + re.escape(p) + r'\b', re.IGNORECASE) for p in patterns]
            out.append({
                'operator_id':    r.get('operator_id', '').strip(),
                'canonical_name': r.get('canonical_name', '').strip(),
                'role':           r.get('role', 'operator').strip(),
                'patterns':       compiled,
            })
    return out


def _get(url: str, timeout: int = 30) -> tuple[Any, str | None]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None, f'HTTP {r.status_code}'
        return r, None
    except Exception as e:
        return None, str(e)


def latest_periodic_filing(cik: str) -> dict:
    """Find the most recent 10-Q or 10-K for a BDC issuer CIK."""
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
        if form in ('10-Q', '10-K'):
            accession = recent['accessionNumber'][i]
            primary = recent['primaryDocument'][i]
            filed = recent['filingDate'][i]
            int_cik = str(int(cik))
            no_dashes = accession.replace('-', '')
            doc_url = f'https://www.sec.gov/Archives/edgar/data/{int_cik}/{no_dashes}/{primary}'
            return {'ok': True, 'cik': cik, 'company': j.get('name'),
                    'form': form, 'accession': accession, 'filed_date': filed,
                    'doc_url': doc_url}
    return {'ok': False, 'error': 'no 10-Q or 10-K in recent filings'}


# Pull the schedule-of-investments slice from a 10-Q HTML document.
_SOI_START_RE = re.compile(
    r'(?:Consolidated\s+)?Schedule\s+of\s+Investments', re.IGNORECASE)
_SOI_END_RE = re.compile(
    r'(?:Notes?\s+to\s+(?:the\s+)?(?:Consolidated\s+)?(?:Financial\s+Statements|Schedule\s+of\s+Investments)'
    r'|Total\s+Investments\b)',
    re.IGNORECASE,
)

# Dollar amount: "$12,345" or "12,345" — captured for column extraction.
_DOLLAR_RE = re.compile(r'\$?\s*\(?([\d,]{1,3}(?:,\d{3})*(?:\.\d+)?)\)?')


def _strip_tags(html: str) -> str:
    """Strip HTML tags but preserve table-cell delimiters as ' | '."""
    s = re.sub(r'</td\s*>|</tr\s*>', ' | ', html, flags=re.IGNORECASE)
    s = re.sub(r'<[^>]+>', ' ', s)
    # Collapse whitespace runs but keep | separators readable.
    s = re.sub(r'[ \t\xa0]+', ' ', s)
    s = re.sub(r' \| \s*\| ', ' | ', s)
    return s


def extract_soi_slice(text: str) -> str:
    """Return only the Schedule-of-Investments portion of the filing."""
    start = _SOI_START_RE.search(text)
    if not start:
        return text  # fall back to full doc
    sliced = text[start.start():]
    end = _SOI_END_RE.search(sliced, pos=200)  # skip the heading line itself
    if end:
        sliced = sliced[:end.start()]
    return sliced


def _find_amounts_after(text: str, idx: int, window: int = 600) -> list[float]:
    """Walk forward from `idx` looking for dollar columns. BDC schedules
    typically format rows as: name | industry | type | rate | maturity |
    principal | cost | fair_value. We collect plausible $-figures."""
    snippet = text[idx:idx + window]
    amounts = []
    for m in _DOLLAR_RE.finditer(snippet):
        raw = m.group(1).replace(',', '')
        try:
            v = float(raw)
        except ValueError:
            continue
        # Filter out obviously-non-loan numbers: percentages (small),
        # share counts (very large with no thousands), years.
        if 0.1 <= v <= 5_000_000:  # in thousands of dollars
            amounts.append(v)
        if len(amounts) >= 6:
            break
    return amounts


def scan_filing(doc_url: str, operators: list[dict] | None = None) -> dict:
    """Pull a BDC 10-Q/10-K and find DC-operator hits in its schedule."""
    if operators is None:
        operators = _load_operator_patterns()
    r, err = _get(doc_url, timeout=60)
    if err:
        return {'ok': False, 'error': err, 'doc_url': doc_url}
    try:
        html = r.content.decode('utf-8', errors='ignore')
    except Exception as e:
        return {'ok': False, 'error': f'decode: {e}', 'doc_url': doc_url}

    plain = _strip_tags(html)
    soi = extract_soi_slice(plain)

    hits = []
    for op in operators:
        for pat in op['patterns']:
            for m in pat.finditer(soi):
                idx = m.start()
                context = soi[max(0, idx - 40): idx + 300]
                amounts = _find_amounts_after(soi, m.end(), window=600)
                # Heuristic: BDC schedules report in thousands. The last two
                # amounts are typically (cost, fair_value). Use fair value
                # if present, else cost, else principal.
                fair_value_kusd = amounts[-1] if amounts else 0.0
                cost_kusd       = amounts[-2] if len(amounts) >= 2 else fair_value_kusd
                principal_kusd  = amounts[0] if amounts else 0.0
                hits.append({
                    'operator_id':    op['operator_id'],
                    'canonical_name': op['canonical_name'],
                    'matched_text':   m.group(0),
                    'context':        context.strip(),
                    'amounts_kusd':   amounts,
                    'principal_usd_m':  round(principal_kusd / 1000.0, 2),
                    'cost_usd_m':       round(cost_kusd / 1000.0, 2),
                    'fair_value_usd_m': round(fair_value_kusd / 1000.0, 2),
                })
                # One hit per pattern per filing is enough.
                break
            else:
                continue
            break

    return {
        'ok': True,
        'doc_url': doc_url,
        'soi_length': len(soi),
        'operator_dictionary_size': len(operators),
        'hits': hits,
    }


def pull_all() -> dict:
    """Pull latest periodic filing for every BDC and scan for DC operator hits."""
    operators = _load_operator_patterns()
    by_bdc = []
    all_hits = []
    for cik, ticker, sponsor, label in BDCS:
        meta = latest_periodic_filing(cik)
        if not meta.get('ok'):
            by_bdc.append({
                'cik': cik, 'ticker': ticker, 'sponsor': sponsor, 'label': label,
                'error': meta.get('error'),
            })
            continue
        scan = scan_filing(meta['doc_url'], operators=operators)
        for h in scan.get('hits', []):
            h.update({
                'lender':       label,
                'lender_short': ticker,
                'sponsor':      sponsor,
                'filed_date':   meta['filed_date'],
                'form':         meta['form'],
                'doc_url':      meta['doc_url'],
            })
            all_hits.append(h)
        by_bdc.append({
            'cik':         cik,
            'ticker':      ticker,
            'sponsor':     sponsor,
            'label':       label,
            'form':        meta['form'],
            'filed_date':  meta['filed_date'],
            'doc_url':     meta['doc_url'],
            'hit_count':   len(scan.get('hits', [])),
            'scan_error':  scan.get('error'),
        })
    return {'ok': True, 'bdcs': by_bdc, 'hits': all_hits, 'hit_count': len(all_hits)}
