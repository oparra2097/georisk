"""
Private-credit ledger for data-center exposure.

Combines two data streams:
  1. Manually-curated rows in data/dc_private_credit.csv (parent-level
     LBO / take-private / equity-recap financings, where most of the
     KKR / Blackstone / Brookfield / Stonepeak risk actually lives).
  2. BDC schedule-of-investments hits scraped from EDGAR
     (edgar_bdc.py).  These get cached to data/_bdc_cache.json after
     an admin pull so the page renders fast.

Why both: BDCs only capture the publicly-traded BDC slice
(~20-30% of private credit by AUM). Form PF, where the bulk of
private-credit fund disclosure goes, is non-public. We surface what's
findable, label each row by source-tier, and remind the user of the
gap in the UI.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_CACHE: dict[str, Any] = {'built': False}

_BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
_CURATED_CSV = os.path.join(_BASE, 'data', 'dc_private_credit.csv')
_BDC_CACHE   = os.path.join(_BASE, 'data', '_bdc_cache.json')


def _to_float(s, default=0.0):
    try: return float(str(s).replace(',', '').strip())
    except (TypeError, ValueError, AttributeError): return default


def _to_int(s, default=0):
    try: return int(float(str(s).replace(',', '').strip()))
    except (TypeError, ValueError, AttributeError): return default


VALID_DEAL_TYPES = {
    'acquisition_financing', 'acquisition_equity',
    'equity_majority',       'equity_recap', 'equity',
    'term_loan',             'revolver', 'mezz', 'second_lien', 'first_lien',
    'preferred',             'bridge',
}
LENDER_TYPE_LABELS = {
    'pe_infra':       'PE / Infrastructure fund',
    'private_credit': 'Private credit / BDC',
    'bank':           'Bank syndicate',
    'public':         'Public bondholders',
    'other':          'Other',
}


def _load_curated() -> list[dict]:
    if not os.path.exists(_CURATED_CSV):
        return []
    rows = []
    with open(_CURATED_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            rows.append({
                'entry_id':          r.get('entry_id', '').strip(),
                'lender':            r.get('lender', '').strip(),
                'lender_type':       (r.get('lender_type') or 'other').strip(),
                'lender_type_label': LENDER_TYPE_LABELS.get(
                    (r.get('lender_type') or 'other').strip(), 'Other'),
                'borrower':          r.get('borrower', '').strip(),
                'borrower_type':     r.get('borrower_type', '').strip(),
                'deal_type':         r.get('deal_type', '').strip(),
                'commitment_usd_m':  _to_float(r.get('commitment_usd_m')),
                'outstanding_usd_m': _to_float(r.get('outstanding_usd_m')),
                'coupon':            r.get('coupon', '').strip(),
                'maturity_year':     r.get('maturity_year', '').strip(),
                'collateral':        r.get('collateral', '').strip(),
                'source_tier':       (r.get('source_tier') or 'curated').strip(),
                'source_url':        r.get('source_url', '').strip(),
                'confidence':        (r.get('confidence') or 'medium').strip(),
                'notes':             r.get('notes', '').strip(),
            })
    return rows


def _load_bdc_cache() -> list[dict]:
    """Load most-recent BDC scrape result, if cached. Each cache row is
    converted to the same shape as a curated row so they merge cleanly."""
    if not os.path.exists(_BDC_CACHE):
        return []
    try:
        with open(_BDC_CACHE, encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.warning('failed to load BDC cache: %s', e)
        return []
    rows = []
    for h in data.get('hits', []):
        rows.append({
            'entry_id':          f"bdc::{h.get('lender_short','?')}::{h.get('operator_id','?')}::{h.get('filed_date','?')}",
            'lender':            h.get('lender', ''),
            'lender_type':       'private_credit',
            'lender_type_label': LENDER_TYPE_LABELS['private_credit'],
            'borrower':          h.get('canonical_name', ''),
            'borrower_type':     'operator',
            'deal_type':         'first_lien',  # most BDC DC paper is 1L; admin can correct
            'commitment_usd_m':  h.get('principal_usd_m', 0.0),
            'outstanding_usd_m': h.get('fair_value_usd_m', 0.0),
            'coupon':            '',
            'maturity_year':     '',
            'collateral':        '',
            'source_tier':       'bdc_filing',
            'source_url':        h.get('doc_url', ''),
            'confidence':        'medium',
            'notes':             f"Matched '{h.get('matched_text','')}' in {h.get('form','?')} filed {h.get('filed_date','?')}",
        })
    return rows


def build(force: bool = False) -> dict[str, Any]:
    with _LOCK:
        if _CACHE.get('built') and not force:
            return _CACHE
        try:
            curated = _load_curated()
            bdc = _load_bdc_cache()
            _CACHE['curated'] = curated
            _CACHE['bdc']     = bdc
            _CACHE['rows']    = curated + bdc
            _CACHE['built']   = True
            _CACHE['build_error'] = None
        except Exception as e:
            logger.exception('private_credit build failed')
            _CACHE['build_error'] = str(e)
            _CACHE['built'] = False
        return _CACHE


def status() -> dict[str, Any]:
    return {
        'built':              _CACHE.get('built', False),
        'build_error':        _CACHE.get('build_error'),
        'curated_count':      len(_CACHE.get('curated', [])),
        'bdc_count':          len(_CACHE.get('bdc', [])),
        'bdc_cache_present':  os.path.exists(_BDC_CACHE),
        'bdc_cache_mtime':    (os.path.getmtime(_BDC_CACHE)
                               if os.path.exists(_BDC_CACHE) else None),
    }


def get_rows() -> list[dict]:
    if not _CACHE.get('built'):
        build()
    return _CACHE.get('rows', [])


def get_summary() -> dict[str, Any]:
    rows = get_rows()
    if not rows:
        return {'rows': [], 'totals': {}, 'by_lender': [], 'by_borrower': [],
                'by_source_tier': []}

    total_commitment  = sum(r['commitment_usd_m'] for r in rows)
    total_outstanding = sum(r['outstanding_usd_m'] for r in rows)

    def _bucket(key_fn, label):
        out: dict[str, dict] = {}
        for r in rows:
            k = key_fn(r)
            if not k: continue
            b = out.setdefault(k, {label: k, 'rows': 0,
                                    'commitment_usd_m': 0.0,
                                    'outstanding_usd_m': 0.0})
            b['rows']              += 1
            b['commitment_usd_m']  += r['commitment_usd_m']
            b['outstanding_usd_m'] += r['outstanding_usd_m']
        for v in out.values():
            v['commitment_usd_m']  = round(v['commitment_usd_m'], 1)
            v['outstanding_usd_m'] = round(v['outstanding_usd_m'], 1)
        return sorted(out.values(), key=lambda x: -x['commitment_usd_m'])

    return {
        'rows': sorted(rows, key=lambda r: -r['commitment_usd_m']),
        'totals': {
            'row_count':              len(rows),
            'curated_count':          len(_CACHE.get('curated', [])),
            'bdc_count':              len(_CACHE.get('bdc', [])),
            'total_commitment_usd_m':  round(total_commitment, 1),
            'total_outstanding_usd_m': round(total_outstanding, 1),
        },
        'by_lender':      _bucket(lambda r: r['lender'], 'lender'),
        'by_borrower':    _bucket(lambda r: r['borrower'], 'borrower'),
        'by_source_tier': _bucket(lambda r: r['source_tier'], 'source_tier'),
    }


def write_bdc_cache(payload: dict) -> None:
    """Persist a fresh BDC pull to disk for fast page reloads."""
    os.makedirs(os.path.dirname(_BDC_CACHE), exist_ok=True)
    with open(_BDC_CACHE, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    build(force=True)
