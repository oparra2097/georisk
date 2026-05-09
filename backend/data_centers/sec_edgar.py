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


# ── Auto-merge into facilities CSV ──────────────────────────────────────
#
# Map raw 10-K property rows into the facilities-CSV schema and merge
# into data/datacenter_facilities.csv. Skip rows whose name already
# exists (case-insensitive) so manually-curated entries are preserved;
# add new rows with confidence='low' since 10-K parsing is heuristic.

# REIT-owned data center cities → CBRE metro. Covers the high-traffic
# locations seen in EQIX, DLR, IRM 10-K property tables. Unmapped cities
# bucket to 'Other' and the row is skipped.
CITY_TO_METRO = {
    # Northern Virginia
    ('VA', 'ashburn'):     ('Northern Virginia', 39.0438, -77.4874),
    ('VA', 'sterling'):    ('Northern Virginia', 39.0062, -77.4286),
    ('VA', 'manassas'):    ('Northern Virginia', 38.7509, -77.4753),
    ('VA', 'reston'):      ('Northern Virginia', 38.9586, -77.3570),
    ('VA', 'herndon'):     ('Northern Virginia', 38.9695, -77.3861),
    ('VA', 'chantilly'):   ('Northern Virginia', 38.8943, -77.4311),
    ('VA', 'culpeper'):    ('Northern Virginia', 38.4734, -77.9966),
    # NY / NJ
    ('NJ', 'secaucus'):    ('New York-New Jersey', 40.7895, -74.0566),
    ('NJ', 'newark'):      ('New York-New Jersey', 40.7357, -74.1724),
    ('NJ', 'carlstadt'):   ('New York-New Jersey', 40.8395, -74.0918),
    ('NJ', 'piscataway'):  ('New York-New Jersey', 40.5546, -74.4651),
    ('NJ', 'parsippany'):  ('New York-New Jersey', 40.8579, -74.4259),
    ('NJ', 'jersey city'): ('New York-New Jersey', 40.7178, -74.0431),
    ('NJ', 'weehawken'):   ('New York-New Jersey', 40.7700, -74.0233),
    ('NY', 'new york'):    ('New York-New Jersey', 40.7128, -74.0060),
    ('NY', 'manhattan'):   ('New York-New Jersey', 40.7831, -73.9712),
    # Chicago
    ('IL', 'elk grove village'): ('Chicago', 42.0039, -87.9703),
    ('IL', 'chicago'):           ('Chicago', 41.8781, -87.6298),
    ('IL', 'aurora'):            ('Chicago', 41.7606, -88.3201),
    ('IL', 'franklin park'):     ('Chicago', 41.9358, -87.8645),
    ('IL', 'wood dale'):         ('Chicago', 41.9636, -87.9784),
    # Atlanta
    ('GA', 'atlanta'):       ('Atlanta', 33.7490, -84.3880),
    ('GA', 'lithia springs'): ('Atlanta', 33.7901, -84.6413),
    ('GA', 'douglasville'):  ('Atlanta', 33.7515, -84.7477),
    ('GA', 'alpharetta'):    ('Atlanta', 34.0754, -84.2941),
    # DFW / Houston / Austin
    ('TX', 'dallas'):       ('Dallas-Fort Worth', 32.7767, -96.7970),
    ('TX', 'plano'):        ('Dallas-Fort Worth', 33.0198, -96.6989),
    ('TX', 'richardson'):   ('Dallas-Fort Worth', 32.9483, -96.7299),
    ('TX', 'irving'):       ('Dallas-Fort Worth', 32.8140, -96.9489),
    ('TX', 'fort worth'):   ('Dallas-Fort Worth', 32.7555, -97.3308),
    ('TX', 'red oak'):      ('Dallas-Fort Worth', 32.5188, -96.8050),
    ('TX', 'houston'):      ('Houston', 29.7604, -95.3698),
    ('TX', 'austin'):       ('Austin-San Antonio', 30.2672, -97.7431),
    ('TX', 'san antonio'):  ('Austin-San Antonio', 29.4241, -98.4936),
    ('TX', 'abilene'):      ('Dallas-Fort Worth', 32.4487, -99.7331),
    # Phoenix
    ('AZ', 'phoenix'):  ('Phoenix', 33.4484, -112.0740),
    ('AZ', 'mesa'):     ('Phoenix', 33.4152, -111.8315),
    ('AZ', 'chandler'): ('Phoenix', 33.3062, -111.8413),
    ('AZ', 'tempe'):    ('Phoenix', 33.4255, -111.9400),
    ('AZ', 'scottsdale'): ('Phoenix', 33.4942, -111.9261),
    # Silicon Valley
    ('CA', 'san jose'):    ('Silicon Valley', 37.3382, -121.8863),
    ('CA', 'santa clara'): ('Silicon Valley', 37.3541, -121.9552),
    ('CA', 'sunnyvale'):   ('Silicon Valley', 37.3688, -122.0363),
    ('CA', 'el segundo'):  ('Silicon Valley', 33.9170, -118.4019),
    ('CA', 'palo alto'):   ('Silicon Valley', 37.4419, -122.1430),
    ('CA', 'fremont'):     ('Silicon Valley', 37.5485, -121.9886),
    # Hillsboro-Portland
    ('OR', 'hillsboro'):  ('Hillsboro-Portland', 45.5230, -122.9890),
    ('OR', 'beaverton'):  ('Hillsboro-Portland', 45.4871, -122.8037),
    ('OR', 'portland'):   ('Hillsboro-Portland', 45.5152, -122.6784),
    # Seattle
    ('WA', 'seattle'):  ('Seattle', 47.6062, -122.3321),
    ('WA', 'redmond'):  ('Seattle', 47.6740, -122.1215),
    ('WA', 'tukwila'):  ('Seattle', 47.4759, -122.2602),
    ('WA', 'quincy'):   ('Seattle', 47.2350, -119.8525),
    # Misc primary
    ('NV', 'las vegas'): ('Reno-Las Vegas', 36.1699, -115.1398),
    ('NV', 'reno'):      ('Reno-Las Vegas', 39.5296, -119.8138),
    ('UT', 'salt lake city'): ('Salt Lake City', 40.7608, -111.8910),
    ('MA', 'boston'):    ('Boston', 42.3601, -71.0589),
    ('MA', 'somerville'): ('Boston', 42.3876, -71.0995),
    ('CO', 'denver'):    ('Denver', 39.7392, -104.9903),
    ('NC', 'charlotte'): ('Charlotte', 35.2271, -80.8431),
    ('MN', 'minneapolis'): ('Minneapolis', 44.9778, -93.2650),
    ('MN', 'eagan'):     ('Minneapolis', 44.8041, -93.1669),
}

# Watts per square foot for the MW estimate when only sqft is disclosed.
# 100–200 W/sqft is typical for modern enterprise colo (AI campuses run
# higher but those aren't usually in REIT 10-Ks).
WATTS_PER_SQFT = 0.000150  # MW per sqft (i.e. 150 W/sqft)
KW_PER_CABINET = 5.0       # rough default for "X cabinets" disclosures

_NUM_RE = re.compile(r'(\d[\d,]*\.?\d*)')


def _to_int(s: str | int | float) -> int:
    if s is None:
        return 0
    if isinstance(s, (int, float)):
        return int(s)
    m = _NUM_RE.search(str(s))
    if not m:
        return 0
    try:
        return int(float(m.group(1).replace(',', '')))
    except ValueError:
        return 0


def _to_float(s: str | int | float) -> float:
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    m = _NUM_RE.search(str(s))
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(',', ''))
    except ValueError:
        return 0.0


def _col_index(header: list[str], keywords: tuple[str, ...]) -> int | None:
    """First column whose header text contains any of the keywords (case-insensitive)."""
    for i, h in enumerate(header):
        low = (h or '').lower()
        for k in keywords:
            if k in low:
                return i
    return None


def _split_city_state(loc: str) -> tuple[str, str]:
    """Try to split 'City, ST' or 'City, State' or 'City' from a location string."""
    if not loc:
        return '', ''
    parts = [p.strip() for p in loc.split(',')]
    if len(parts) >= 2:
        city = parts[0].lower()
        st_raw = parts[-1].strip().upper()
        # Last token often "USA" — fall back to second-to-last
        if st_raw == 'USA' and len(parts) >= 3:
            st_raw = parts[-2].strip().upper()
        return city, st_raw[:2]
    return parts[0].lower(), ''


def _map_table_to_facilities(ticker: str, company: str, doc_url: str,
                              table: dict) -> list[dict]:
    """Convert one raw 10-K Properties table into a list of facilities-schema
    rows. Drops rows that don't map to a known US metro."""
    header = table.get('header') or []
    rows = table.get('rows') or []

    name_idx     = _col_index(header, ('ibx', 'data center', 'property', 'site name', 'facility', 'name'))
    location_idx = _col_index(header, ('metro area', 'location', 'city'))
    state_idx    = _col_index(header, ('state', 'st'))
    sqft_idx     = _col_index(header, ('square feet', 'sq. ft', 'sqft', 'sf'))
    mw_idx       = _col_index(header, ('mw', 'critical load', 'critical it', 'power capacity'))
    cab_idx      = _col_index(header, ('cabinet', 'racks'))

    out = []
    for row in rows:
        if not row:
            continue
        # Pull cells defensively
        def cell(i): return row[i].strip() if i is not None and i < len(row) else ''

        raw_name = cell(name_idx)
        loc      = cell(location_idx)
        st_cell  = cell(state_idx)
        sqft     = _to_int(cell(sqft_idx))
        mw_disc  = _to_float(cell(mw_idx))
        cabinets = _to_int(cell(cab_idx))

        if not raw_name and not loc:
            continue

        # Geocode: prefer explicit state column, else parse from location.
        if st_cell and len(st_cell) <= 4:
            city, st = (loc or '').strip().lower(), st_cell.upper()[:2]
        else:
            city, st = _split_city_state(loc)

        meta = CITY_TO_METRO.get((st, city))
        if not meta:
            continue  # skip foreign / unmapped cities
        market, lat, lon = meta

        # MW: prefer disclosed; else estimate from sqft / cabinets.
        if mw_disc > 0:
            mw, conf = mw_disc, 'medium'
        elif sqft > 0:
            mw, conf = round(sqft * WATTS_PER_SQFT, 1), 'low'
        elif cabinets > 0:
            mw, conf = round(cabinets * KW_PER_CABINET / 1000.0, 1), 'low'
        else:
            mw, conf = 0.0, 'low'
        if mw <= 0:
            continue

        # Synthetic facility name: "<Company> <name> <city>"
        bits = [company, raw_name, city.title()]
        name = ' '.join(b for b in bits if b)

        out.append({
            'name':           name,
            'market':         market,
            'lat':            lat,
            'lon':            lon,
            'status':         'built',
            'mw':             mw,
            'operator':       company,
            'developer':      company,
            'funding_type':   'reit',
            'funding_detail': f'{company} (10-K filing {table.get("filed_date", "")})',
            'tenant':         'Multiple',
            'announced_year': '',
            'target_online':  '',
            'notes':          f'auto-merged from SEC 10-K · sqft={sqft or "?"} · cabinets={cabinets or "?"}',
            'source_url':     doc_url,
            'confidence':     conf,
        })
    return out


def to_facility_rows(parsed: dict) -> list[dict]:
    """Flatten a `pull_all` result into facilities-schema rows."""
    out = []
    for ticker, rep in (parsed.get('reits') or {}).items():
        if not rep.get('ok'):
            continue
        company = rep.get('company') or REIT_LABELS.get(ticker, ticker)
        for t in rep.get('tables') or []:
            t = {**t, 'filed_date': rep.get('filed_date', '')}
            out.extend(_map_table_to_facilities(ticker, company, rep.get('doc_url', ''), t))
    # Dedup within the new pull (operator + city + sqft can produce dupes
    # across multiple property tables in the same 10-K).
    seen = set()
    deduped = []
    for r in out:
        key = r['name'].lower().strip()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped


def merge_into_facilities() -> dict:
    """Pull every REIT 10-K, map to facilities schema, and append new rows
    to data/datacenter_facilities.csv. Existing rows (matched by name,
    case-insensitive) are preserved so manually-curated entries are not
    overwritten. Atomic swap with .bak; cache rebuilt on success."""
    import csv as _csv
    import os as _os
    import shutil as _shutil

    parsed = pull_all()
    new_rows = to_facility_rows(parsed)

    DATA_DIR = _os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))),
        'data',
    )
    target = _os.path.join(DATA_DIR, 'datacenter_facilities.csv')

    # Read existing rows; preserve order.
    existing: list[dict] = []
    existing_names: set[str] = set()
    fieldnames: list[str] = []
    if _os.path.exists(target):
        with open(target, newline='', encoding='utf-8') as f:
            reader = _csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            for r in reader:
                existing.append(r)
                existing_names.add((r.get('name') or '').strip().lower())

    if not fieldnames:
        # First-time write: derive fieldnames from a new row.
        if not new_rows:
            return {'ok': False, 'error': 'no rows to merge and no existing CSV'}
        fieldnames = list(new_rows[0].keys())

    # Append only rows that don't already exist by name.
    added = []
    skipped = []
    for r in new_rows:
        if (r['name'] or '').strip().lower() in existing_names:
            skipped.append(r['name'])
            continue
        # Pad row to existing schema; ignore extra keys.
        full = {k: r.get(k, '') for k in fieldnames}
        existing.append(full)
        existing_names.add((r['name'] or '').strip().lower())
        added.append(r)

    # Write atomically: tmp → backup current → rename.
    tmp = target + '.tmp'
    bak = target + '.bak'
    try:
        with open(tmp, 'w', newline='', encoding='utf-8') as f:
            w = _csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in existing:
                w.writerow(r)
        if _os.path.exists(target):
            _shutil.copy2(target, bak)
        _os.replace(tmp, target)
    except Exception as e:
        return {'ok': False, 'error': f'file swap failed: {e}'}

    # Refresh the service cache so the dashboard reflects the new data.
    from backend.data_centers import service
    service.build(force=True)

    return {
        'ok': True,
        'pulled':  len(new_rows),
        'added':   len(added),
        'skipped': len(skipped),
        'preview_added':   [{'name': r['name'], 'market': r['market'], 'mw': r['mw'],
                              'confidence': r['confidence']} for r in added[:20]],
        'preview_skipped': skipped[:20],
        'reit_status': {
            t: {k: v for k, v in (parsed.get('reits') or {}).get(t, {}).items()
                 if k in ('ok', 'error', 'filed_date', 'tables_found', 'total_rows')}
            for t in REIT_CIKS
        },
    }
