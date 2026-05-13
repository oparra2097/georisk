"""
KBRA pre-sale PDF parser for data center ABS deals.

KBRA publishes most of its data center ABS pre-sale reports free at
www.kbra.com. Each report contains structured tables we want:
  - Collateral facility list (name, MW, location)
  - Tenant exposure (top tenants, share of revenue)
  - Lease maturity schedule
  - Stress-test cash flow tables

This is the harder source — KBRA's PDFs vary in formatting and the
tables can be image-rendered. We use pypdf for text extraction first,
fall back to surface fields we can confidently regex out, and emit a
reviewable structured object for the admin to confirm before merging
into the deals CSV.
"""

from __future__ import annotations

import io
import logging
import re
from typing import Any

import requests

logger = logging.getLogger(__name__)

USER_AGENT = (
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/124.0.0.0 Safari/537.36'
)

# Patterns frequently seen in KBRA DC ABS pre-sale reports.
_SIZE_RE  = re.compile(r'\$\s*([\d,]+(?:\.\d+)?)\s*million', re.IGNORECASE)
_RATING_RE = re.compile(r'(AAA|AA[+-]?|A[+-]?|BBB[+-]?|BB[+-]?|B[+-]?|CCC[+-]?)\b', re.IGNORECASE)
_MW_RE    = re.compile(r'(\d{1,4}(?:\.\d+)?)\s*MW', re.IGNORECASE)
_WAL_RE   = re.compile(r'weighted\s*average\s*(?:remaining\s*)?lease\s*(?:term|life)[^0-9]*(\d+(?:\.\d+)?)\s*(?:years?|yrs?)',
                         re.IGNORECASE)
_TENANT_HINT = re.compile(
    r'(?:top|largest)\s*tenants?[^.]{0,200}',
    re.IGNORECASE,
)
_FACILITY_HINT = re.compile(
    r'collateral\s*(?:properties|portfolio|facilities)[^.]{0,400}',
    re.IGNORECASE,
)


def _fetch_pdf(url: str) -> tuple[bytes | None, str | None]:
    try:
        r = requests.get(url, timeout=60,
                         headers={'User-Agent': USER_AGENT,
                                  'Accept': 'application/pdf,*/*;q=0.9'})
        if r.status_code != 200:
            return None, f'HTTP {r.status_code}'
        return r.content, None
    except Exception as e:
        return None, str(e)


def _extract_text(pdf_bytes: bytes) -> tuple[str, str | None]:
    try:
        from pypdf import PdfReader
    except ImportError:
        try:
            from PyPDF2 import PdfReader  # type: ignore
        except ImportError:
            return '', 'pypdf / PyPDF2 not installed'
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages[:20]:  # cap at first 20 pages
            try:
                parts.append(page.extract_text() or '')
            except Exception:
                continue
        return '\n'.join(parts), None
    except Exception as e:
        return '', str(e)


def parse_presale(text: str) -> dict:
    """Surface fields we can confidently regex from a KBRA pre-sale text."""
    out: dict[str, Any] = {}

    m = _SIZE_RE.search(text)
    if m:
        try:
            out['total_size_usd_m'] = round(float(m.group(1).replace(',', '')), 1)
        except ValueError:
            pass

    # Pull the first plausible senior-tranche rating after the word "Senior"
    # or "Class A" if present; else just the first hit.
    rating_window = text
    for keyword in ('Senior Notes', 'Class A', 'Senior Tranche'):
        idx = text.lower().find(keyword.lower())
        if idx >= 0:
            rating_window = text[idx:idx + 800]
            break
    m = _RATING_RE.search(rating_window)
    if m:
        out['rating_senior'] = m.group(1).upper()
        out['rater'] = 'KBRA'

    mws = [float(x.group(1)) for x in _MW_RE.finditer(text)]
    if mws:
        out['mw_mentions'] = mws[:20]
        # Heuristic: take the sum of the top-N MW mentions as a rough
        # collateral total. The admin verifies before merge.
        out['collateral_mw_estimate'] = round(sum(sorted(mws, reverse=True)[:8]), 1)

    m = _WAL_RE.search(text)
    if m:
        try:
            out['wal_years'] = round(float(m.group(1)), 1)
        except ValueError:
            pass

    # Soft surfaces for the admin to inspect
    t = _TENANT_HINT.search(text)
    if t:
        out['tenant_snippet'] = t.group(0)[:400]

    f = _FACILITY_HINT.search(text)
    if f:
        out['facility_snippet'] = f.group(0)[:600]

    out['text_length'] = len(text)
    return out


def parse_url(url: str) -> dict:
    """Fetch a KBRA pre-sale PDF and return parsed fields."""
    raw, err = _fetch_pdf(url)
    if err:
        return {'ok': False, 'error': err, 'url': url}
    text, err = _extract_text(raw)
    if err:
        return {'ok': False, 'error': err, 'url': url}
    parsed = parse_presale(text)
    return {'ok': True, 'url': url, 'parsed': parsed}
