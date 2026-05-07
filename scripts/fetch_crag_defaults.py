"""
Fetch the Bank of Canada Credit Rating Assessment Group (CRAG) database
of sovereign default events and write it into
``data/sovereign_defaults.csv`` in the schema this project expects.

The CRAG file is the authoritative public panel of sovereign default
events: ~1,300 country-year-instrument observations 1960–present,
covering bond defaults, bank-loan restructurings, Paris Club and London
Club agreements, and arrears. Updated annually (typically June). Free.

Usage::

    python scripts/fetch_crag_defaults.py
    # By default writes to data/sovereign_defaults.csv (overwrites).
    # --dry-run to inspect counts without writing.

If your network can't reach bankofcanada.ca, download the spreadsheet
manually and pass ``--input /path/to/file.xlsx``.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import urllib.request
from pathlib import Path
from typing import List, Optional

# Allow running as `python scripts/fetch_crag_defaults.py` from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Direct download link for the CRAG dataset. The Bank of Canada hosts it
# at a stable path under their economic-research microsite. The link
# resolves to the latest annual update.
CRAG_URL = (
    'https://www.bankofcanada.ca/wp-content/uploads/'
    '2014/03/db-sovereign-defaults-data.xlsx'
)

# CRAG event-type column → our event_type bucket.
EVENT_TYPE_MAP = {
    'External Sovereign Bond':       'default',
    'Bond':                          'default',
    'Bonds':                         'default',
    'Foreign Currency Bonds':        'default',
    'Foreign Currency Bank Loans':   'restructuring',
    'Bank Loans':                    'restructuring',
    'Bank Debt':                     'restructuring',
    'Local Currency Bank Loans':     'restructuring',
    'Local Currency Public Debt':    'restructuring',
    'Local Currency Debt':           'restructuring',
    'Paris Club':                    'paris_club',
    'London Club':                   'london_club',
    'Arrears':                       'arrears',
}

OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent / 'data' / 'sovereign_defaults.csv'
)


def download_crag(url: str = CRAG_URL) -> bytes:
    """Download the CRAG xlsx; raise if the network call fails."""
    req = urllib.request.Request(
        url, headers={'User-Agent': 'parra-macro/1.0 (+https://parramacro.com)'}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def parse_crag_xlsx(blob: bytes) -> List[dict]:
    """Parse the CRAG xlsx blob into normalized event rows.

    The CRAG sheet is a wide country-year matrix (one row per country,
    one column per year) where each cell carries the instrument type if
    a default is active. We flatten it to one row per (iso3, year-range,
    instrument) tuple.
    """
    try:
        from openpyxl import load_workbook
    except ImportError:
        raise RuntimeError('openpyxl required: pip install openpyxl')

    try:
        from backend.data_sources.country_codes import get_iso3_for_name
    except ImportError:
        # Fallback: minimal name → ISO3 mapping. The CRAG sheet uses
        # English country names. We rely on the project's existing
        # country_codes helper when available.
        get_iso3_for_name = None

    wb = load_workbook(io.BytesIO(blob), data_only=True, read_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 2:
        raise RuntimeError('CRAG sheet appears empty')

    # First row is the header (column 0 = country; cols 1..N = years).
    header = rows[0]
    years = []
    for v in header[1:]:
        try:
            years.append(int(v))
        except (TypeError, ValueError):
            years.append(None)

    out: List[dict] = []
    for r in rows[1:]:
        country_name = r[0]
        if not country_name or not isinstance(country_name, str):
            continue
        iso3 = _name_to_iso3(country_name, get_iso3_for_name)
        if not iso3:
            continue
        # Walk each year column, grouping consecutive non-null cells by
        # instrument type into single (start, end) events.
        run_start = None
        run_type = None
        for i, val in enumerate(r[1:]):
            year = years[i] if i < len(years) else None
            if val and isinstance(val, str) and val.strip():
                inst = val.strip()
                if run_start is None:
                    run_start = year
                    run_type = inst
                elif inst != run_type:
                    if run_start is not None and run_type:
                        out.append(_event(iso3, run_start, year - 1, run_type))
                    run_start = year
                    run_type = inst
            else:
                if run_start is not None and run_type:
                    end_year = year - 1 if year else run_start
                    out.append(_event(iso3, run_start, end_year, run_type))
                run_start = None
                run_type = None
        # Flush trailing run (default still active in the latest year).
        if run_start is not None and run_type:
            out.append(_event(iso3, run_start, None, run_type))
    return out


def _event(iso3: str, start: int, end: Optional[int], crag_type: str) -> dict:
    et = EVENT_TYPE_MAP.get(crag_type, 'default')
    inst = (
        'paris_club' if et == 'paris_club' else
        'bank_loan' if et == 'restructuring' else
        'external_bond'
    )
    return {
        'iso3': iso3, 'start_year': start, 'end_year': end,
        'event_type': et, 'instrument': inst,
        'source': 'CRAG', 'notes': f'CRAG raw type: {crag_type}',
    }


_FALLBACK_NAME_MAP = {
    # Common CRAG spellings → ISO3.
    'United States': 'USA', 'United States of America': 'USA',
    'United Kingdom': 'GBR', 'Russia': 'RUS', 'Russian Federation': 'RUS',
    'South Korea': 'KOR', 'Korea, Rep.': 'KOR',
    'North Korea': 'PRK', 'Korea, Dem. Peoples Rep.': 'PRK',
    'Iran, Islamic Rep.': 'IRN', 'Iran': 'IRN',
    'Egypt, Arab Rep.': 'EGY', 'Egypt': 'EGY',
    'Venezuela, RB': 'VEN', 'Venezuela': 'VEN',
    'Yemen, Rep.': 'YEM', 'Yemen': 'YEM',
    'Cote d Ivoire': 'CIV', "Cote d'Ivoire": 'CIV', 'Ivory Coast': 'CIV',
    'Congo, Rep.': 'COG', 'Congo, Dem. Rep.': 'COD',
    'Czech Republic': 'CZE', 'Slovak Republic': 'SVK',
    'Macedonia, FYR': 'MKD', 'North Macedonia': 'MKD',
    'Serbia and Montenegro': 'SRB', 'Yugoslavia': 'SRB',
    'Bahamas, The': 'BHS', 'Gambia, The': 'GMB',
    'Lao PDR': 'LAO', 'Vietnam': 'VNM', 'Viet Nam': 'VNM',
    'Syrian Arab Republic': 'SYR', 'Syria': 'SYR',
    'Sao Tome and Principe': 'STP',
}


def _name_to_iso3(name: str, helper) -> Optional[str]:
    if helper is not None:
        try:
            iso3 = helper(name)
            if iso3:
                return iso3
        except Exception:
            pass
    return _FALLBACK_NAME_MAP.get(name.strip())


def write_csv(events: List[dict], path: Path) -> None:
    fieldnames = ['iso3', 'start_year', 'end_year', 'event_type',
                  'instrument', 'source', 'notes']
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        f.write(
            '# Sovereign default / restructuring events from the Bank of Canada CRAG\n'
            '# database. Auto-generated by scripts/fetch_crag_defaults.py.\n'
            '# Schema matches what backend/credit_default/defaults.py expects.\n'
        )
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ev in events:
            row = {k: (ev.get(k) if ev.get(k) is not None else '') for k in fieldnames}
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='Local CRAG xlsx path (skip network).')
    parser.add_argument('--output', default=str(OUTPUT_PATH),
                        help='Where to write the harmonized CSV.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse + summarize without writing.')
    args = parser.parse_args()

    if args.input:
        with open(args.input, 'rb') as f:
            blob = f.read()
        print(f'[crag] using local file: {args.input}')
    else:
        try:
            print(f'[crag] downloading {CRAG_URL} …')
            blob = download_crag()
        except Exception as e:
            print(f'[crag] download failed: {e}')
            print('[crag] re-run with --input <path> after manual download.')
            return 2

    events = parse_crag_xlsx(blob)
    print(f'[crag] parsed {len(events)} events across {len(set(e["iso3"] for e in events))} countries')

    type_counts = {}
    for e in events:
        type_counts[e['event_type']] = type_counts.get(e['event_type'], 0) + 1
    print(f'[crag] type breakdown: {type_counts}')

    if args.dry_run:
        print('[crag] dry-run — not writing.')
        return 0

    out = Path(args.output)
    write_csv(events, out)
    print(f'[crag] wrote {out}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
