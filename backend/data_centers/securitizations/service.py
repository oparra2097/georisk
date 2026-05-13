"""
Data Center ABS / CMBS / structured-debt service.

Loads the curated seed dataset of public US data center securitization
deals (data/datacenter_abs_deals.csv), computes roll-ups by sponsor /
rating / vintage / tenant, and cross-references each deal's collateral
against the facilities CSV so a deal click can show the buildings, MW,
and stranded-risk scores.

The seed CSV is updated by:
  - manual curation                                    via /admin upload
  - SEC EDGAR ABS-EE periodic asset-level filings      (edgar_abs.py)
  - KBRA / DBRS pre-sale PDF parsing for new deals     (kbra_parser.py)
"""

from __future__ import annotations

import csv
import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_CACHE: dict[str, Any] = {'built': False, 'deals': []}

_CSV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    'data',
    'datacenter_abs_deals.csv',
)

VALID_TYPES = {'abs', 'single_asset_cmbs', 'hybrid', 'term_loan', 'unsecured'}
TYPE_LABELS = {
    'abs':                'Pure ABS',
    'single_asset_cmbs':  'Single-asset CMBS',
    'hybrid':             'Hybrid ABS/CMBS',
    'term_loan':          'Securitized term loan',
    'unsecured':          'Unsecured corporate',
}

VALID_TENANT_TYPES = {'hyperscale', 'colocation', 'wholesale', 'mixed', 'hpc_ai'}
TENANT_TYPE_LABELS = {
    'hyperscale': 'Hyperscale',
    'colocation': 'Retail colocation',
    'wholesale':  'Wholesale BTS',
    'mixed':      'Mixed',
    'hpc_ai':     'HPC / AI',
}

# Facility form-factor — what the BUILDING is, distinct from who occupies it.
VALID_DC_TYPES = {
    'retail_colo',         # many small enterprise tenants, interconnection-heavy
    'wholesale',           # multi-tenant wholesale colo (larger suites)
    'hyperscale_bts',      # single-tenant build-to-suit dedicated facilities
    'hyperscale_campus',   # multi-building campus for one or more hyperscalers
    'ai_campus',           # purpose-built AI / GPU training campus
    'mixed',
}
DC_TYPE_LABELS = {
    'retail_colo':       'Retail colo',
    'wholesale':         'Wholesale',
    'hyperscale_bts':    'Hyperscale BTS',
    'hyperscale_campus': 'Hyperscale campus',
    'ai_campus':         'AI campus',
    'mixed':             'Mixed facility',
}


def _to_float(s, default=0.0):
    try: return float(str(s).replace(',', '').strip())
    except (TypeError, ValueError, AttributeError): return default


def _to_int(s, default=0):
    try: return int(float(str(s).replace(',', '').strip()))
    except (TypeError, ValueError, AttributeError): return default


def _split_list(s: str) -> list[str]:
    """Split semicolon-delimited list (with comma fallback for resilience)."""
    s = (s or '').strip()
    if not s:
        return []
    if ';' in s:
        return [x.strip() for x in s.split(';') if x.strip()]
    return [x.strip() for x in s.split(',') if x.strip()]


def _load_csv() -> list[dict[str, Any]]:
    if not os.path.exists(_CSV_PATH):
        return []
    rows: list[dict[str, Any]] = []
    with open(_CSV_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            dtype = (r.get('deal_type') or '').strip().lower()
            if dtype not in VALID_TYPES:
                dtype = 'abs'
            ttype = (r.get('tenant_type') or '').strip().lower()
            if ttype not in VALID_TENANT_TYPES:
                ttype = ''
            fctype = (r.get('datacenter_type') or '').strip().lower()
            if fctype not in VALID_DC_TYPES:
                fctype = ''
            rows.append({
                'deal_id':      r.get('deal_id', '').strip(),
                'deal_name':    r.get('deal_name', '').strip(),
                'sponsor':      r.get('sponsor', '').strip(),
                'deal_type':    dtype,
                'deal_type_label': TYPE_LABELS.get(dtype, dtype),
                'tenant_type':       ttype,
                'tenant_type_label': TENANT_TYPE_LABELS.get(ttype, ''),
                'datacenter_type':       fctype,
                'datacenter_type_label': DC_TYPE_LABELS.get(fctype, ''),
                'cusip_senior': r.get('cusip_senior', '').strip(),
                'issue_date':   r.get('issue_date', '').strip(),
                'vintage':      (r.get('issue_date') or '')[:4],
                'total_size_usd_m':     _to_float(r.get('total_size_usd_m')),
                'rating_senior':        r.get('rating_senior', '').strip(),
                'rater':                r.get('rater', '').strip(),
                'collateral_facilities':   _split_list(r.get('collateral_facilities', '')),
                'collateral_facility_count': _to_int(r.get('collateral_facility_count')),
                'collateral_mw_built':  _to_float(r.get('collateral_mw_built')),
                'collateral_mw_uc':     _to_float(r.get('collateral_mw_uc')),
                'top_tenants':          _split_list(r.get('top_tenants', '')),
                'top_tenant_share_pct': _to_float(r.get('top_tenant_share_pct')),
                'wal_years':            _to_float(r.get('wal_years')),
                'final_maturity':       r.get('final_maturity', '').strip(),
                'current_balance_usd_m': _to_float(r.get('current_balance_usd_m')),
                'status':               (r.get('status') or 'active').strip().lower(),
                'source_url':           r.get('source_url', '').strip(),
                'confidence':           (r.get('confidence') or 'medium').strip().lower(),
                'notes':                r.get('notes', '').strip(),
            })
    # Drop placeholder rows that have no real data.
    rows = [r for r in rows if r['deal_id'] and r['deal_id'] != 'placeholder_seed']
    return rows


def _cross_reference_facilities(deals: list[dict]) -> None:
    """Attach a facility_matches[] list to each deal — facility entries
    from the facilities CSV whose name overlaps with the deal's
    collateral_facilities list.  Allows the UI to deep-link to facility-
    level risk scores."""
    from backend.data_centers import service as facility_service
    if not facility_service._CACHE.get('built'):
        facility_service.build()
    fac_index = facility_service._CACHE.get('facilities', [])

    def _match(name: str) -> list[dict]:
        low = name.lower()
        hits = []
        for f in fac_index:
            fname = (f.get('name') or '').lower()
            # Loose match: either side contains the other as a substring,
            # or share a distinctive token (>=4 chars).
            if low in fname or fname in low:
                hits.append(f); continue
            toks_a = {t for t in low.split() if len(t) >= 4}
            toks_b = {t for t in fname.split() if len(t) >= 4}
            if toks_a & toks_b:
                hits.append(f)
        return hits

    for d in deals:
        matches = []
        for c in d['collateral_facilities']:
            for f in _match(c):
                if f['name'] not in {m['name'] for m in matches}:
                    matches.append({
                        'name':        f['name'],
                        'market':      f['market'],
                        'mw':          f['mw'],
                        'status':      f['status'],
                        'tenant_norm': f.get('tenant_norm', ''),
                        'stranded_risk':   f.get('stranded_risk'),
                        'at_risk_mw':      f.get('at_risk_mw'),
                    })
        d['facility_matches'] = matches


def build(force: bool = False) -> dict[str, Any]:
    with _LOCK:
        if _CACHE['built'] and not force:
            return _CACHE
        try:
            deals = _load_csv()
            _cross_reference_facilities(deals)
            _CACHE['deals'] = deals
            _CACHE['built'] = True
            _CACHE['build_error'] = None
        except Exception as e:
            logger.exception('securitizations build failed')
            _CACHE['build_error'] = str(e)
            _CACHE['built'] = False
        return _CACHE


def status() -> dict[str, Any]:
    return {
        'built': _CACHE.get('built', False),
        'build_error': _CACHE.get('build_error'),
        'deal_count': len(_CACHE.get('deals', [])),
        'deal_types': TYPE_LABELS,
    }


def get_deals() -> list[dict]:
    if not _CACHE.get('built'):
        build()
    return _CACHE.get('deals', [])


def get_summary() -> dict[str, Any]:
    """Roll-ups: by sponsor / by rater / by vintage / by tenant + totals."""
    deals = get_deals()
    if not deals:
        return {'deals': [], 'totals': {}, 'by_sponsor': [], 'by_vintage': [],
                'by_rater': [], 'by_tenant': [], 'by_type': []}

    total_size = sum(d['total_size_usd_m'] for d in deals)
    total_balance = sum(d['current_balance_usd_m'] for d in deals)
    active = [d for d in deals if d['status'] == 'active']

    def _bucket(key_fn, label='key'):
        out: dict[str, dict[str, Any]] = {}
        for d in deals:
            for k in (key_fn(d) if isinstance(key_fn(d), list) else [key_fn(d)]):
                if not k: continue
                b = out.setdefault(k, {label: k, 'deals': 0,
                                        'size_usd_m': 0.0, 'balance_usd_m': 0.0,
                                        'mw_built': 0.0, 'mw_uc': 0.0})
                b['deals']         += 1
                b['size_usd_m']    += d['total_size_usd_m']
                b['balance_usd_m'] += d['current_balance_usd_m']
                b['mw_built']      += d['collateral_mw_built']
                b['mw_uc']         += d['collateral_mw_uc']
        for v in out.values():
            for k in ('size_usd_m', 'balance_usd_m', 'mw_built', 'mw_uc'):
                v[k] = round(v[k], 1)
        return sorted(out.values(), key=lambda x: -x['size_usd_m'])

    by_sponsor = _bucket(lambda d: d['sponsor'], 'sponsor')
    by_vintage = _bucket(lambda d: d['vintage'], 'vintage')
    by_rater   = _bucket(lambda d: d['rater'], 'rater')
    by_tenant  = _bucket(lambda d: d['top_tenants'], 'tenant')
    by_type    = _bucket(lambda d: d['deal_type_label'], 'deal_type')

    return {
        'deals': sorted(deals, key=lambda d: -d['total_size_usd_m']),
        'totals': {
            'deal_count':         len(deals),
            'active_count':       len(active),
            'total_size_usd_m':   round(total_size, 1),
            'total_balance_usd_m': round(total_balance, 1),
            'mw_built_collateral': round(sum(d['collateral_mw_built'] for d in deals), 1),
            'mw_uc_collateral':    round(sum(d['collateral_mw_uc'] for d in deals), 1),
        },
        'by_sponsor': by_sponsor,
        'by_vintage': sorted(by_vintage, key=lambda x: x['vintage']),
        'by_rater':   by_rater,
        'by_tenant':  by_tenant,
        'by_type':    by_type,
    }


def get_deal(deal_id: str) -> dict | None:
    for d in get_deals():
        if d['deal_id'] == deal_id:
            return d
    return None
