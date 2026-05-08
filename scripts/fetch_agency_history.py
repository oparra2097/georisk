#!/usr/bin/env python3
"""
Backfill data/agency_ratings_history.csv by scraping
countryeconomy.com/ratings/<country-slug> for sovereign rating action
histories.

Usage::

    # Backfill ALL sovereigns currently in the credit-default panel
    python scripts/fetch_agency_history.py

    # Backfill specific countries by ISO3
    python scripts/fetch_agency_history.py --iso3 ROU AGO PHL

The script is idempotent: it deduplicates against rows already in the
CSV (matched by iso3 + as_of) and appends only new actions. It also
filters out "no-change" rows where neither rating nor outlook moved
relative to the previous action by the same agency for that country.

Source attribution: countryeconomy.com is a free, public site; data
ultimately originates from S&P, Moody's, and Fitch press releases. We
respect a 1.5 s delay between requests to avoid stressing their server.
"""

from __future__ import annotations

import argparse
import csv
import html
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Allow running as `python scripts/fetch_agency_history.py` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


CSV_PATH = Path(__file__).resolve().parent.parent / 'data' / 'agency_ratings_history.csv'
USER_AGENT = 'parra-macro-credit-default/1.0 (+https://parramacro.com)'
REQUEST_DELAY_S = 1.5


# ── ISO3 → countryeconomy.com slug ──────────────────────────────────────
# countryeconomy.com slugs are lowercase, hyphenated names. Most match
# straightforwardly from the country name, but a few quirks (special
# chars, alternate spellings) are worth pinning explicitly.

_SLUG_OVERRIDES: Dict[str, str] = {
    'AGO': 'angola', 'ARG': 'argentina', 'BRA': 'brazil', 'CHL': 'chile',
    'COL': 'colombia', 'EGY': 'egypt', 'GHA': 'ghana', 'IND': 'india',
    'IDN': 'indonesia', 'KAZ': 'kazakhstan', 'KEN': 'kenya', 'LBN': 'lebanon',
    'LKA': 'sri-lanka', 'MAR': 'morocco', 'MEX': 'mexico', 'NGA': 'nigeria',
    'PAK': 'pakistan', 'PER': 'peru', 'PHL': 'philippines', 'ROU': 'romania',
    'RUS': 'russia', 'TUR': 'turkey', 'UKR': 'ukraine', 'URY': 'uruguay',
    'VEN': 'venezuela', 'VNM': 'vietnam', 'ZAF': 'south-africa',
    'USA': 'usa', 'GBR': 'uk', 'DEU': 'germany', 'FRA': 'france',
    'ITA': 'italy', 'ESP': 'spain', 'PRT': 'portugal', 'GRC': 'greece',
    'IRL': 'ireland', 'NLD': 'netherlands', 'BEL': 'belgium', 'AUT': 'austria',
    'POL': 'poland', 'CZE': 'czech-republic', 'HUN': 'hungary', 'BGR': 'bulgaria',
    'HRV': 'croatia', 'SVK': 'slovakia', 'SVN': 'slovenia', 'EST': 'estonia',
    'LVA': 'latvia', 'LTU': 'lithuania', 'CYP': 'cyprus', 'MLT': 'malta',
    'JPN': 'japan', 'KOR': 'south-korea', 'CHN': 'china', 'HKG': 'hong-kong',
    'SGP': 'singapore', 'AUS': 'australia', 'NZL': 'new-zealand',
    'CAN': 'canada', 'CHE': 'switzerland', 'NOR': 'norway', 'SWE': 'sweden',
    'DNK': 'denmark', 'FIN': 'finland', 'ISL': 'iceland',
    'SAU': 'saudi-arabia', 'ARE': 'united-arab-emirates', 'QAT': 'qatar',
    'BHR': 'bahrain', 'KWT': 'kuwait', 'OMN': 'oman', 'JOR': 'jordan',
    'ISR': 'israel', 'TUN': 'tunisia', 'DZA': 'algeria',
    'CIV': 'ivory-coast', 'SEN': 'senegal', 'CMR': 'cameroon', 'ETH': 'ethiopia',
    'TZA': 'tanzania', 'UGA': 'uganda', 'RWA': 'rwanda', 'BFA': 'burkina-faso',
    'BEN': 'benin', 'TGO': 'togo', 'BDI': 'burundi', 'CPV': 'cape-verde',
    'GAB': 'gabon', 'NAM': 'namibia', 'BWA': 'botswana', 'MOZ': 'mozambique',
    'ZMB': 'zambia', 'ZWE': 'zimbabwe', 'MUS': 'mauritius', 'SYC': 'seychelles',
    'TTO': 'trinidad-and-tobago', 'JAM': 'jamaica', 'DOM': 'dominican-republic',
    'CRI': 'costa-rica', 'PAN': 'panama', 'GTM': 'guatemala', 'HND': 'honduras',
    'SLV': 'el-salvador', 'NIC': 'nicaragua', 'ECU': 'ecuador', 'BOL': 'bolivia',
    'PRY': 'paraguay', 'BHS': 'bahamas', 'BRB': 'barbados',
    'BLZ': 'belize', 'SUR': 'suriname', 'GUY': 'guyana',
    'KHM': 'cambodia', 'MMR': 'myanmar', 'BGD': 'bangladesh', 'NPL': 'nepal',
    'MNG': 'mongolia', 'AZE': 'azerbaijan', 'GEO': 'georgia', 'ARM': 'armenia',
    'BLR': 'belarus', 'MDA': 'moldova', 'MKD': 'north-macedonia',
    'SRB': 'serbia', 'BIH': 'bosnia-and-herzegovina', 'ALB': 'albania',
    'XKX': 'kosovo', 'MNE': 'montenegro',
}


# ── Light HTML parser ───────────────────────────────────────────────────


_TR_RE = re.compile(r'<tr[^>]*>(.*?)</tr>', re.S | re.I)
_TD_RE = re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', re.S | re.I)
_TAG_RE = re.compile(r'<[^>]+>')


def _strip_tags(s: str) -> str:
    return html.unescape(_TAG_RE.sub('', s)).strip()


def _http_get(url: str, timeout: int = 30) -> Optional[str]:
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode('utf-8', errors='replace')
    except (urllib.error.URLError, TimeoutError) as e:
        print(f'  [http] {url}: {e}', file=sys.stderr)
        return None


def _parse_country_page(body: str, agency_hint: str) -> List[Dict]:
    """Extract every <tr> with at least 3 cells (date, rating, outlook)
    from the page. ``agency_hint`` lets us tag rows when the page splits
    the three agencies across separate tables under heading sections.
    """
    rows: List[Dict] = []
    for tr in _TR_RE.findall(body):
        cells = [_strip_tags(c) for c in _TD_RE.findall(tr)]
        if len(cells) < 2:
            continue
        # Heuristic: valid action rows start with a date in YYYY-MM-DD
        # or DD/MM/YYYY format.
        date = cells[0]
        m = re.match(r'(\d{4})[-/](\d{2})[-/](\d{2})', date)
        if not m:
            m = re.match(r'(\d{2})/(\d{2})/(\d{4})', date)
            if m:
                date = f'{m.group(3)}-{m.group(2)}-{m.group(1)}'
            else:
                continue
        else:
            date = f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
        # Layout varies: typically [date, rating, outlook] or
        # [date, agency_logo, rating, outlook].
        rating_idx = 1 if len(cells) <= 3 else 2
        rating = cells[rating_idx] if rating_idx < len(cells) else ''
        outlook = cells[rating_idx + 1] if rating_idx + 1 < len(cells) else ''
        if not rating or rating.lower() in ('rating', '-', '—'):
            continue
        rows.append({
            'agency': agency_hint,
            'date': date,
            'rating': rating,
            'outlook': outlook if outlook.lower() not in ('outlook', '-', '—', 'n/a') else '',
        })
    return rows


def fetch_country_history(slug: str) -> Dict[str, List[Dict]]:
    """Fetch a single country page and return
    ``{'sp': [...], 'moodys': [...], 'fitch': [...]}`` lists of
    actions, sorted oldest → newest."""
    body = _http_get(f'https://countryeconomy.com/ratings/{slug}')
    if not body:
        return {'sp': [], 'moodys': [], 'fitch': []}
    # The page has one table per agency under H2/H3 sections. We split
    # on those headers and parse each chunk with the matching tag.
    chunks = re.split(
        r'<h\d[^>]*>\s*(S&amp;P|Moody|Fitch)[^<]*</h\d>', body, flags=re.I,
    )
    out = {'sp': [], 'moodys': [], 'fitch': []}
    for i in range(1, len(chunks), 2):
        header = chunks[i].lower()
        body_part = chunks[i + 1] if i + 1 < len(chunks) else ''
        if 's&amp;p' in header or 's&p' in header:
            out['sp'].extend(_parse_country_page(body_part, 'sp'))
        elif 'moody' in header:
            out['moodys'].extend(_parse_country_page(body_part, 'moodys'))
        elif 'fitch' in header:
            out['fitch'].extend(_parse_country_page(body_part, 'fitch'))
    for k in out:
        out[k].sort(key=lambda r: r['date'])
    return out


def dedupe_changes(actions: List[Dict]) -> List[Dict]:
    """Drop rows where (rating, outlook) is unchanged from the prior
    action — those are confirmation reviews that add no signal."""
    out: List[Dict] = []
    last: Tuple[str, str] = ('', '')
    for r in actions:
        key = (r['rating'], r['outlook'])
        if key != last:
            out.append(r)
            last = key
    return out


# ── CSV merge ───────────────────────────────────────────────────────────


_CSV_HEADER = (
    'iso3,as_of,sp,moodys,fitch,sp_outlook,moodys_outlook,fitch_outlook'
)


def _load_existing() -> set:
    """Return the set of (iso3, as_of) tuples already in the CSV so we
    don't duplicate rows on re-run."""
    seen = set()
    if not CSV_PATH.exists():
        return seen
    with open(CSV_PATH, encoding='utf-8') as f:
        cleaned = (ln for ln in f if ln.strip() and not ln.lstrip().startswith('#'))
        reader = csv.DictReader(cleaned)
        for row in reader:
            iso = (row.get('iso3') or '').strip().upper()
            asof = (row.get('as_of') or '').strip()
            if iso and asof:
                seen.add((iso, asof))
    return seen


def append_country(iso3: str, history: Dict[str, List[Dict]]) -> int:
    """Append new rows for one country. Returns the count appended."""
    existing = _load_existing()
    rows_to_write: List[Dict] = []
    for agency, actions in history.items():
        for r in dedupe_changes(actions):
            key = (iso3.upper(), r['date'])
            if key in existing:
                continue
            existing.add(key)
            row = {
                'iso3': iso3.upper(),
                'as_of': r['date'],
                'sp': r['rating'] if agency == 'sp' else '',
                'moodys': r['rating'] if agency == 'moodys' else '',
                'fitch': r['rating'] if agency == 'fitch' else '',
                'sp_outlook': r['outlook'] if agency == 'sp' else '',
                'moodys_outlook': r['outlook'] if agency == 'moodys' else '',
                'fitch_outlook': r['outlook'] if agency == 'fitch' else '',
            }
            rows_to_write.append(row)

    if not rows_to_write:
        return 0
    rows_to_write.sort(key=lambda r: r['as_of'])

    write_header = not CSV_PATH.exists()
    with open(CSV_PATH, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=_CSV_HEADER.split(','), extrasaction='ignore',
        )
        if write_header:
            f.write(f'# Auto-appended by scripts/fetch_agency_history.py\n')
            writer.writeheader()
        f.write(f'# ── {iso3.upper()} (auto, {time.strftime("%Y-%m-%d")}) ──\n')
        for row in rows_to_write:
            writer.writerow(row)
    return len(rows_to_write)


# ── Main ────────────────────────────────────────────────────────────────


def _resolve_iso3_universe(args_iso3: List[str]) -> List[str]:
    if args_iso3:
        return [s.upper() for s in args_iso3]
    # Default: every sovereign currently in the credit-default panel.
    try:
        from backend.credit_default import service as cd_service
    except ImportError as e:
        raise SystemExit(f'cannot load panel: {e}')
    panel = cd_service.get_dashboard()
    return sorted((panel.get('countries') or {}).keys())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--iso3', nargs='*', default=None,
        help='ISO-3 codes to backfill (default: every sovereign in the panel).',
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='Stop after N countries (useful for a dry run).',
    )
    args = parser.parse_args()

    iso_list = _resolve_iso3_universe(args.iso3 or [])
    if args.limit:
        iso_list = iso_list[:args.limit]

    print(f'[fetch_agency_history] {len(iso_list)} sovereigns to process')
    total_added = 0
    skipped = 0
    for i, iso3 in enumerate(iso_list, 1):
        slug = _SLUG_OVERRIDES.get(iso3)
        if not slug:
            skipped += 1
            print(f'  [{i:>3}/{len(iso_list)}] {iso3}: no slug — skip')
            continue
        print(f'  [{i:>3}/{len(iso_list)}] {iso3} → {slug}', end=' ', flush=True)
        history = fetch_country_history(slug)
        n = append_country(iso3, history)
        total_added += n
        s_n = sum(len(v) for v in history.values())
        print(f'(actions {s_n}, appended {n})')
        time.sleep(REQUEST_DELAY_S)
    print(f'\n[fetch_agency_history] done. Appended {total_added} new rows. '
          f'Skipped (no slug): {skipped}.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
