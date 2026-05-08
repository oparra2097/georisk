"""
SEC EDGAR REIT property-table parser.

Pulls the latest 10-K filing for each public data center REIT, locates the
"Properties" section, and emits the underlying property table rows as a
reviewable preview (not auto-merged into the production CSV).

The admin UI surfaces a "Pull SEC EDGAR" button; the parsed rows are
downloadable as CSV for the operator to clean and re-upload via the
existing /admin/upload-facilities endpoint.

SEC's fair-access policy: identify ourselves with a real User-Agent.
See https://www.sec.gov/os/accessing-edgar-data.

Note: 10-K HTML formatting changes year to year. This parser uses
heuristic table detection (header keywords + row count) and is best-effort.
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

# Public data center REITs we cover. CIKs are the SEC central index keys —
# stable identifiers that don't change with ticker / corporate actions.
REIT_CIKS = {
    'EQIX': '0001101239',  # Equinix
    'DLR':  '0001297996',  # Digital Realty Trust
    'IRM':  '0001020569',  # Iron Mountain
}
REIT_LABELS = {
    'EQIX': 'Equinix',
    'DLR':  'Digital Realty Trust',
    'IRM':  'Iron Mountain',
}


def _get(url: str, timeout: int = 20) -> tuple[Any, str | None]:
    """Wrapper around requests.get returning (response_or_none, error_message)."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None, f'HTTP {r.status_code}'
        return r, None
    except Exception as e:
        return None, str(e)


def latest_10k(cik: str) -> dict:
    """Fetch the latest 10-K filing metadata for a CIK.

    Returns {ok, accession, doc_url, filed_date, error}.
    """
    cik_padded = cik.lstrip('0').zfill(10)
    url = f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
    r, err = _get(url)
    if err:
        return {'ok': False, 'error': f'{url}: {err}'}
    try:
        j = r.json()
    except Exception as e:
        return {'ok': False, 'error': f'parse JSON: {e}'}
    recent = j.get('filings', {}).get('recent', {})
    forms = recent.get('form', [])
    for i, form in enumerate(forms):
        if form == '10-K':
            accession = recent['accessionNumber'][i]
            primary = recent['primaryDocument'][i]
            filed_date = recent['filingDate'][i]
            int_cik = str(int(cik))
            no_dashes = accession.replace('-', '')
            doc_url = (
                f'https://www.sec.gov/Archives/edgar/data/{int_cik}/{no_dashes}/{primary}'
            )
            index_url = (
                f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany'
                f'&CIK={cik}&type=10-K&dateb=&owner=include&count=10'
            )
            return {
                'ok': True,
                'cik': cik,
                'company_name': j.get('name'),
                'accession': accession,
                'filed_date': filed_date,
                'doc_url': doc_url,
                'index_url': index_url,
            }
    return {'ok': False, 'error': 'no 10-K found in recent filings'}


# ── HTML parsing helpers ────────────────────────────────────────────────

_TAG = re.compile(r'<[^>]+>')
_WS  = re.compile(r'\s+')
_TABLE_RE = re.compile(r'<table\b[^>]*>(.*?)</table>', re.IGNORECASE | re.DOTALL)
_TR_RE    = re.compile(r'<tr\b[^>]*>(.*?)</tr>',     re.IGNORECASE | re.DOTALL)
_CELL_RE  = re.compile(r'<t[hd]\b[^>]*>(.*?)</t[hd]>', re.IGNORECASE | re.DOTALL)

# Header keywords that suggest a property table (case-insensitive substring).
_PROPERTY_HINTS = (
    'location', 'property', 'site', 'ibx', 'data center', 'facility',
    'metro area', 'city', 'square feet', 'sq. ft', 'sqft', 'capacity',
)


def _strip(s: str) -> str:
    return _WS.sub(' ', _TAG.sub(' ', s)).strip()


def _row_cells(tr_html: str) -> list[str]:
    return [_strip(m.group(1)) for m in _CELL_RE.finditer(tr_html)]


def _table_looks_like_properties(table_html: str) -> tuple[bool, list[str]]:
    """Heuristic: a property table has a header row mentioning location-ish
    keywords and many data rows."""
    rows = list(_TR_RE.finditer(table_html))
    if len(rows) < 6:
        return False, []
    header_cells = _row_cells(rows[0].group(1))
    header_text = ' '.join(header_cells).lower()
    hits = sum(1 for k in _PROPERTY_HINTS if k in header_text)
    if hits < 2:
        return False, []
    return True, header_cells


def parse_property_tables(html: str, ticker: str) -> list[dict]:
    """Return a list of candidate property tables, each with:
       {ticker, header, rows[], n_rows}
    The caller decides which table(s) to keep.
    """
    out: list[dict] = []
    for tm in _TABLE_RE.finditer(html):
        thtml = tm.group(1)
        ok, header = _table_looks_like_properties(thtml)
        if not ok:
            continue
        rows = []
        tr_iter = list(_TR_RE.finditer(thtml))
        for tr in tr_iter[1:]:  # skip header row
            cells = _row_cells(tr.group(1))
            if not cells or all(not c for c in cells):
                continue
            rows.append(cells)
        if len(rows) >= 6:
            out.append({
                'ticker': ticker,
                'header': header,
                'n_rows': len(rows),
                'rows': rows[:200],  # cap for sanity
            })
    return out


def pull(ticker: str) -> dict:
    """Pull the latest 10-K for one ticker and parse property tables."""
    cik = REIT_CIKS.get(ticker)
    if not cik:
        return {'ok': False, 'error': f'unknown ticker: {ticker}'}
    meta = latest_10k(cik)
    if not meta.get('ok'):
        return {'ok': False, 'error': meta.get('error'), 'ticker': ticker}
    r, err = _get(meta['doc_url'], timeout=30)
    if err:
        return {'ok': False, 'error': f'fetch 10-K: {err}', 'ticker': ticker, 'meta': meta}
    try:
        html = r.content.decode('utf-8', errors='ignore')
    except Exception as e:
        return {'ok': False, 'error': f'decode: {e}', 'ticker': ticker, 'meta': meta}
    tables = parse_property_tables(html, ticker)
    return {
        'ok': True,
        'ticker': ticker,
        'company': REIT_LABELS.get(ticker, ticker),
        'cik': cik,
        'filed_date': meta['filed_date'],
        'accession': meta['accession'],
        'doc_url': meta['doc_url'],
        'index_url': meta['index_url'],
        'tables_found': len(tables),
        'total_rows': sum(t['n_rows'] for t in tables),
        'tables': tables,
    }


def pull_all() -> dict:
    """Pull the latest 10-K for every covered REIT."""
    results = {ticker: pull(ticker) for ticker in REIT_CIKS}
    summary = {
        'ok': any(v.get('ok') for v in results.values()),
        'reits': results,
        'total_tables': sum(v.get('tables_found', 0) for v in results.values()),
        'total_rows': sum(v.get('total_rows', 0) for v in results.values()),
    }
    if summary['ok']:
        try:
            from backend.data_centers import freshness
            freshness.record_pull('sec_edgar', {
                'reits_ok': [t for t, v in results.items() if v.get('ok')],
                'total_rows': summary['total_rows'],
            })
        except Exception:
            pass
    return summary


def to_csv(parsed: dict) -> str:
    """Flatten a `pull_all` result into a CSV the admin can download.

    Columns: ticker, company, filed_date, accession, doc_url, table_index,
             header_cell_1..N, row_cell_1..N
    """
    import csv as _csv
    import io as _io

    buf = _io.StringIO()
    # Determine max width across all tables so columns line up.
    max_cells = 1
    for r in parsed.get('reits', {}).values():
        for t in r.get('tables', []) or []:
            if t.get('rows'):
                max_cells = max(max_cells, max(len(row) for row in t['rows']))
            if t.get('header'):
                max_cells = max(max_cells, len(t['header']))

    w = _csv.writer(buf)
    base_cols = ['ticker', 'company', 'filed_date', 'accession', 'doc_url',
                 'table_index', 'row_kind']
    cell_cols = [f'col_{i+1}' for i in range(max_cells)]
    w.writerow(base_cols + cell_cols)

    for r in parsed.get('reits', {}).values():
        if not r.get('ok'):
            continue
        for ti, t in enumerate(r.get('tables', []) or []):
            common = [r['ticker'], r['company'], r.get('filed_date', ''),
                      r.get('accession', ''), r.get('doc_url', ''), ti]
            # Header
            h = (t.get('header') or [])[:max_cells]
            w.writerow(common + ['header'] + h + [''] * (max_cells - len(h)))
            for row in t.get('rows', []) or []:
                row = row[:max_cells]
                w.writerow(common + ['row'] + row + [''] * (max_cells - len(row)))
    return buf.getvalue()
