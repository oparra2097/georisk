"""Rent ledger: monthly charge generation, late fees, balances, statements.

The ledger is append-only. A tenant's balance is simply

    sum(charges) - sum(payments credited)

where a payment is credited once it reaches `succeeded`. ACH payments sit in
`processing` for a few business days; those are reported separately as
"pending" so the tenant sees the payment landed without the balance moving
twice.
"""

from __future__ import annotations

import calendar
import logging
from datetime import date, datetime, timedelta

from backend.rent.db import get_db
from backend.rent import models

logger = logging.getLogger(__name__)

CREDITED_STATUSES = ('succeeded',)
PENDING_STATUSES = ('pending', 'processing')


# ── Period helpers ───────────────────────────────────────────────────────

def today() -> date:
    return date.today()


def period_of(day: date | None = None) -> str:
    day = day or today()
    return f'{day.year:04d}-{day.month:02d}'


def parse_period(period: str) -> tuple[int, int]:
    year, month = period.split('-')
    return int(year), int(month)


def period_bounds(period: str) -> tuple[date, date]:
    year, month = parse_period(period)
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


def next_period(period: str) -> str:
    year, month = parse_period(period)
    return f'{year + 1:04d}-01' if month == 12 else f'{year:04d}-{month + 1:02d}'


def due_date_for(period: str, due_day: int) -> date:
    """Rent due date for a period, clamped to the length of the month."""
    year, month = parse_period(period)
    last = calendar.monthrange(year, month)[1]
    return date(year, month, min(max(int(due_day or 1), 1), last))


def _as_date(value) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


# ── Charge generation ────────────────────────────────────────────────────

def prorated_rent_cents(rent_cents: int, period: str, start: date | None,
                        end: date | None) -> int:
    """Rent owed for `period`, prorated by days if the lease starts or ends
    partway through the month. Daily rate = monthly rent / days in month.
    """
    first, last = period_bounds(period)
    span_start = max(first, start) if start else first
    span_end = min(last, end) if end else last
    if span_end < span_start:
        return 0
    days_in_month = (last - first).days + 1
    days_owed = (span_end - span_start).days + 1
    if days_owed >= days_in_month:
        return int(rent_cents)
    return int(round(rent_cents * days_owed / days_in_month))


def generate_charges_for_period(period: str | None = None) -> dict:
    """Post the rent charge for every active lease for `period` (default: the
    current month). Idempotent — re-running posts nothing new.
    """
    period = period or period_of()
    first, last = period_bounds(period)
    created, skipped, total_cents = 0, 0, 0

    for lease in models.list_leases(status='active'):
        start = _as_date(lease['start_date'])
        end = _as_date(lease['end_date'])
        if start and start > last:
            skipped += 1
            continue
        if end and end < first:
            skipped += 1
            continue
        amount = prorated_rent_cents(lease['rent_cents'], period, start, end)
        if amount <= 0:
            skipped += 1
            continue
        prorated = amount != lease['rent_cents']
        description = f'Rent {period}' + (' (prorated)' if prorated else '')
        charge_id = models.add_charge(
            lease_id=lease['id'], kind='rent', amount_cents=amount,
            due_date=due_date_for(period, lease['due_day']).isoformat(),
            period=period, description=description)
        if charge_id:
            created += 1
            total_cents += amount
        else:
            skipped += 1

    logger.info("Rent charges for %s: %d posted, %d skipped ($%.2f)",
                period, created, skipped, total_cents / 100)
    return {'period': period, 'created': created, 'skipped': skipped,
            'total_cents': total_cents}


def apply_late_fees(as_of: date | None = None) -> dict:
    """Post one late fee per lease per overdue rent period.

    A period is late when today is past due date + grace days and the rent for
    that period is still not covered by credited payments.
    """
    as_of = as_of or today()
    assessed, total_cents = 0, 0

    for lease in models.list_leases(status='active'):
        if not lease['late_fee_cents'] and not lease['late_fee_daily_cents']:
            continue
        ledger = lease_ledger(lease['id'])
        for period, owed in _unpaid_rent_periods(ledger).items():
            due = due_date_for(period, lease['due_day'])
            late_after = due + timedelta(days=int(lease['grace_days'] or 0))
            if as_of <= late_after or owed <= 0:
                continue
            days_late = (as_of - late_after).days
            amount = int(lease['late_fee_cents'] or 0)
            amount += int(lease['late_fee_daily_cents'] or 0) * max(days_late, 0)
            if amount <= 0:
                continue
            charge_id = models.add_charge(
                lease_id=lease['id'], kind='late_fee', amount_cents=amount,
                due_date=as_of.isoformat(), period=period,
                description=f'Late fee — rent {period}')
            if charge_id:
                assessed += 1
                total_cents += amount

    logger.info("Late fees: %d assessed ($%.2f)", assessed, total_cents / 100)
    return {'assessed': assessed, 'total_cents': total_cents}


def _unpaid_rent_periods(ledger: dict) -> dict:
    """Map period -> cents still owed, applying credited payments oldest-first."""
    owed = {}
    for charge in ledger['charges']:
        if charge['kind'] in ('rent', 'late_fee') and charge['period']:
            owed[charge['period']] = owed.get(charge['period'], 0) + charge['amount_cents']
    remaining = ledger['paid_cents']
    for period in sorted(owed):
        applied = min(remaining, owed[period])
        owed[period] -= applied
        remaining -= applied
        if remaining <= 0:
            break
    return owed


# ── Balances + statements ────────────────────────────────────────────────

def lease_ledger(lease_id: int) -> dict:
    """Charges, payments, and the derived balance for one lease."""
    conn = get_db()
    try:
        charges = [dict(r) for r in conn.execute(
            'SELECT * FROM charges WHERE lease_id = ? ORDER BY due_date, id',
            (lease_id,)).fetchall()]
        payments = [dict(r) for r in conn.execute(
            'SELECT * FROM payments WHERE lease_id = ? '
            'ORDER BY COALESCE(paid_at, created_at), id', (lease_id,)).fetchall()]
    finally:
        conn.close()

    charged = sum(c['amount_cents'] for c in charges)
    paid = sum(p['amount_cents'] for p in payments if p['status'] in CREDITED_STATUSES)
    pending = sum(p['amount_cents'] for p in payments if p['status'] in PENDING_STATUSES)
    return {
        'lease_id': lease_id,
        'charges': charges,
        'payments': payments,
        'charged_cents': charged,
        'paid_cents': paid,
        'pending_cents': pending,
        'balance_cents': charged - paid,
        'balance_after_pending_cents': charged - paid - pending,
    }


def amount_due(lease_id: int, as_of: date | None = None) -> int:
    """Cents owed right now — charges whose due date has arrived, less credits."""
    as_of = as_of or today()
    ledger = lease_ledger(lease_id)
    due_now = sum(c['amount_cents'] for c in ledger['charges']
                  if (_as_date(c['due_date']) or as_of) <= as_of)
    return max(due_now - ledger['paid_cents'] - ledger['pending_cents'], 0)


def statement(lease_id: int) -> dict:
    """A merged, chronologically ordered ledger with a running balance."""
    ledger = lease_ledger(lease_id)
    rows = []
    for charge in ledger['charges']:
        rows.append({
            'date': charge['due_date'],
            'type': charge['kind'],
            'description': charge['description'] or charge['kind'].replace('_', ' ').title(),
            'amount_cents': charge['amount_cents'],
            'status': 'charged',
        })
    for payment in ledger['payments']:
        if payment['status'] == 'failed':
            continue
        stamp = (payment['paid_at'] or payment['created_at'] or '')[:10]
        rows.append({
            'date': stamp,
            'type': 'payment',
            'description': _payment_label(payment),
            'amount_cents': -payment['amount_cents'],
            'status': payment['status'],
            'payment_id': payment['id'],
            'receipt_url': payment['receipt_url'],
        })
    rows.sort(key=lambda r: (r['date'] or '', r['type'] != 'payment'))

    running = 0
    for row in rows:
        if row['status'] in ('charged',) or row['status'] in CREDITED_STATUSES:
            running += row['amount_cents']
        row['running_balance_cents'] = running

    return {
        'rows': rows,
        'charged_cents': ledger['charged_cents'],
        'paid_cents': ledger['paid_cents'],
        'pending_cents': ledger['pending_cents'],
        'balance_cents': ledger['balance_cents'],
    }


def _payment_label(payment: dict) -> str:
    method = {
        'card': 'Card payment',
        'us_bank_account': 'Bank transfer (ACH)',
        'check': 'Check',
        'cash': 'Cash',
    }.get(payment['method'], 'Payment')
    if payment['status'] == 'processing':
        return f'{method} — processing'
    if payment['status'] == 'refunded':
        return f'{method} — refunded'
    return method


def lease_summary(lease: dict, as_of: date | None = None) -> dict:
    """Everything the tenant dashboard needs about one lease."""
    as_of = as_of or today()
    ledger = lease_ledger(lease['id'])
    period = period_of(as_of)
    due = due_date_for(period, lease['due_day'])
    if due < as_of:
        nxt = next_period(period)
        next_due = due_date_for(nxt, lease['due_day'])
    else:
        next_due = due
    balance = ledger['balance_cents']
    past_due = amount_due(lease['id'], as_of)
    late_after = due + timedelta(days=int(lease['grace_days'] or 0))
    return {
        'lease': lease,
        'balance_cents': balance,
        'pending_cents': ledger['pending_cents'],
        'amount_due_cents': past_due,
        'current_period': period,
        'next_due_date': next_due.isoformat(),
        'is_late': past_due > 0 and as_of > late_after,
        'days_until_due': (next_due - as_of).days,
    }


# ── Portfolio-level reporting ────────────────────────────────────────────

def portfolio_summary(as_of: date | None = None) -> dict:
    as_of = as_of or today()
    period = period_of(as_of)
    properties = models.list_properties()
    leases = models.list_leases(status='active')

    billed = collected = outstanding = 0
    delinquent = []
    for lease in leases:
        ledger = lease_ledger(lease['id'])
        period_charges = sum(c['amount_cents'] for c in ledger['charges']
                             if c['period'] == period)
        billed += period_charges
        collected += ledger['paid_cents']
        outstanding += max(ledger['balance_cents'], 0)
        if ledger['balance_cents'] > 0:
            due = due_date_for(period, lease['due_day'])
            if as_of > due + timedelta(days=int(lease['grace_days'] or 0)):
                delinquent.append({
                    'lease_id': lease['id'],
                    'tenant_name': lease['tenant_name'],
                    'tenant_email': lease['tenant_email'],
                    'property_name': lease['property_name'],
                    'unit_label': lease['unit_label'],
                    'balance_cents': ledger['balance_cents'],
                })
    delinquent.sort(key=lambda d: -d['balance_cents'])

    units = sum(p['unit_count'] for p in properties)
    occupied = sum(p['occupied'] for p in properties)
    scheduled = sum(p['scheduled_rent_cents'] for p in properties)
    collected_this_period = _collected_in_period(period)

    return {
        'as_of': as_of.isoformat(),
        'period': period,
        'properties': properties,
        'unit_count': units,
        'occupied_units': occupied,
        'vacant_units': units - occupied,
        'occupancy_pct': round(100 * occupied / units, 1) if units else 0.0,
        'scheduled_rent_cents': scheduled,
        'billed_this_period_cents': billed,
        'collected_this_period_cents': collected_this_period,
        'collection_rate_pct': round(100 * collected_this_period / billed, 1) if billed else 0.0,
        'outstanding_cents': outstanding,
        'delinquent': delinquent,
        'active_leases': len(leases),
    }


def _collected_in_period(period: str) -> int:
    first, last = period_bounds(period)
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT COALESCE(SUM(amount_cents), 0) AS total FROM payments
            WHERE status = 'succeeded' AND DATE(COALESCE(paid_at, created_at))
                  BETWEEN ? AND ?""", (first.isoformat(), last.isoformat())).fetchone()
        return row['total'] or 0
    finally:
        conn.close()


def run_monthly_close(as_of: date | None = None) -> dict:
    """Scheduler entry point: post this month's rent, then assess late fees."""
    as_of = as_of or today()
    charges = generate_charges_for_period(period_of(as_of))
    fees = apply_late_fees(as_of)
    return {'charges': charges, 'late_fees': fees}
