"""JSON API for the rent portal — mounted at /api/rent.

Resident endpoints are scoped to the signed-in tenant's own leases; every
manager endpoint is behind `@manager_required`. The Stripe webhook is the one
unauthenticated route and is protected by signature verification instead.
"""

from __future__ import annotations

import logging
from datetime import date

from flask import Blueprint, jsonify, request, url_for

from backend.rent import billing, models, stripe_gateway
from backend.rent.auth import (current_tenant, is_manager, lease_belongs_to_tenant,
                               manager_required, tenant_required)
from backend.rent.db import DEFAULT_SETTINGS, get_settings, set_setting

logger = logging.getLogger(__name__)

rent_api_bp = Blueprint('rent_api', __name__)


def _body() -> dict:
    return request.get_json(silent=True) or {}


def _cents(data: dict, key: str = 'amount_cents') -> int:
    """Accept either integer cents or a dollar string ("1,250.00")."""
    if key in data and data[key] is not None:
        return int(data[key])
    raw = data.get(key.replace('_cents', ''))
    if raw is None or raw == '':
        return 0
    if isinstance(raw, (int, float)):
        return int(round(float(raw) * 100))
    cleaned = str(raw).replace('$', '').replace(',', '').strip()
    return int(round(float(cleaned) * 100)) if cleaned else 0


def _today() -> date:
    return date.today()


# ── Resident endpoints ───────────────────────────────────────────────────

@rent_api_bp.route('/me')
@tenant_required
def me():
    tenant = current_tenant()
    leases = models.leases_for_tenant(tenant['id'])
    active = next((l for l in leases if l['status'] == 'active'), None)
    payload = {
        'tenant': {k: tenant[k] for k in ('id', 'email', 'full_name', 'phone')},
        'leases': leases,
        'lease': active,
        'summary': billing.lease_summary(active) if active else None,
        'statement': billing.statement(active['id']) if active else None,
        'payment_methods': stripe_gateway.quote(
            max(billing.amount_due(active['id']), 0) if active else 0),
        'stripe_ready': stripe_gateway.available(),
        'settings': _public_settings(),
    }
    return jsonify(payload)


@rent_api_bp.route('/quote')
@tenant_required
def payment_quote():
    try:
        amount_cents = int(request.args.get('amount_cents', 0))
    except (TypeError, ValueError):
        amount_cents = 0
    return jsonify({'amount_cents': amount_cents,
                    'methods': stripe_gateway.quote(max(amount_cents, 0))})


@rent_api_bp.route('/checkout', methods=['POST'])
@tenant_required
def checkout():
    tenant = current_tenant()
    data = _body()
    lease = (models.get_lease(int(data['lease_id'])) if data.get('lease_id')
             else models.active_lease_for_tenant(tenant['id']))
    if not lease or not lease_belongs_to_tenant(lease, tenant):
        return jsonify({'error': 'No active lease found for this account.'}), 404

    amount_cents = _cents(data) or billing.amount_due(lease['id'])
    if amount_cents <= 0:
        return jsonify({'error': 'Nothing is currently due on this lease.'}), 400

    balance = billing.lease_ledger(lease['id'])['balance_cents']
    if amount_cents > max(balance, 0) and not data.get('allow_overpay'):
        return jsonify({'error': 'Amount is more than the balance on the account.',
                        'balance_cents': balance}), 400
    if amount_cents < balance and get_settings().get('allow_partial_payments') != '1':
        return jsonify({'error': 'Partial payments are not accepted. '
                                 'Please pay the full balance.'}), 400

    method = data.get('method', stripe_gateway.METHOD_CARD)
    if not stripe_gateway.available():
        return jsonify({'error': 'Online payments are not connected yet. '
                                 'Contact the office to arrange payment.'}), 503
    try:
        session_info = stripe_gateway.create_checkout_session(
            lease=lease, tenant=tenant, amount_cents=amount_cents, method=method,
            success_url=url_for('rent_pages.receipt', _external=True)
            + '?session_id={CHECKOUT_SESSION_ID}',
            cancel_url=url_for('rent_pages.pay', _external=True) + '?canceled=1')
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except stripe_gateway.StripeNotConfigured as exc:
        return jsonify({'error': str(exc)}), 503
    except Exception:
        return jsonify({'error': 'Could not start the payment. Please try again.'}), 502
    return jsonify(session_info)


@rent_api_bp.route('/session/<session_id>')
@tenant_required
def session_status(session_id):
    tenant = current_tenant()
    payment = models.get_payment_by_session(session_id)
    if payment and payment['tenant_id'] != tenant['id']:
        return jsonify({'error': 'Not found'}), 404
    stripe_gateway.reconcile_session(session_id)
    payment = models.get_payment_by_session(session_id)
    if not payment:
        return jsonify({'error': 'Not found'}), 404
    lease = models.get_lease(payment['lease_id'])
    return jsonify({'payment': payment,
                    'lease': lease,
                    'summary': billing.lease_summary(lease) if lease else None})


# ── Stripe webhook (unauthenticated; signature-verified) ─────────────────

@rent_api_bp.route('/stripe/webhook', methods=['POST'])
def stripe_webhook():
    signature = request.headers.get('Stripe-Signature', '')
    try:
        event = stripe_gateway.verify_webhook(request.get_data(), signature)
    except stripe_gateway.StripeNotConfigured as exc:
        logger.error("Stripe webhook received but not configured: %s", exc)
        return jsonify({'error': 'not configured'}), 503
    except Exception as exc:
        logger.warning("Rejected Stripe webhook: %s", exc)
        return jsonify({'error': 'invalid signature'}), 400
    try:
        result = stripe_gateway.handle_event(event)
    except Exception:
        logger.exception("Failed handling Stripe event")
        return jsonify({'error': 'handler error'}), 500
    return jsonify(result)


# ── Manager: portfolio + properties + units ──────────────────────────────

@rent_api_bp.route('/portfolio')
@manager_required
def portfolio():
    return jsonify(billing.portfolio_summary())


@rent_api_bp.route('/properties', methods=['GET', 'POST'])
@manager_required
def properties():
    if request.method == 'GET':
        return jsonify({'properties': models.list_properties()})
    data = _body()
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Property name is required.'}), 400
    slug = (data.get('slug') or name.lower().replace(' ', '-')).strip()
    if models.get_property_by_slug(slug):
        return jsonify({'error': f'A property with slug "{slug}" already exists.'}), 409
    fields = {k: v for k, v in data.items() if k not in ('name', 'slug')}
    prop_id = models.create_property(slug, name, **fields)
    return jsonify({'property': models.get_property(prop_id)}), 201


@rent_api_bp.route('/properties/<int:prop_id>', methods=['PATCH'])
@manager_required
def patch_property(prop_id):
    models.update_property(prop_id, **_body())
    return jsonify({'property': models.get_property(prop_id)})


@rent_api_bp.route('/units', methods=['GET', 'POST'])
@manager_required
def units():
    if request.method == 'GET':
        prop_id = request.args.get('property_id', type=int)
        return jsonify({'units': models.list_units(property_id=prop_id,
                                                   status=request.args.get('status'))})
    data = _body()
    prop_id = data.get('property_id')
    if not prop_id:
        return jsonify({'error': 'property_id is required.'}), 400

    rows = data.get('units')
    if rows is None:
        rows = [data]
    created, errors = [], []
    for row in rows:
        label = str(row.get('label', '')).strip()
        if not label:
            errors.append('A unit label is required.')
            continue
        try:
            unit_id = models.create_unit(
                property_id=int(prop_id), label=label,
                bedrooms=int(row.get('bedrooms', 2)),
                bathrooms=float(row.get('bathrooms', 2)),
                sqft=int(row['sqft']) if row.get('sqft') else None,
                market_rent_cents=_cents(row, 'market_rent_cents'),
                status=row.get('status', 'vacant'))
            created.append(unit_id)
        except Exception as exc:
            errors.append(f'{label}: {exc}')
    return jsonify({'created': len(created), 'errors': errors}), 201 if created else 400


@rent_api_bp.route('/units/<int:unit_id>', methods=['PATCH'])
@manager_required
def patch_unit(unit_id):
    data = _body()
    if 'market_rent' in data or 'market_rent_cents' in data:
        data['market_rent_cents'] = _cents(data, 'market_rent_cents')
    models.update_unit(unit_id, **data)
    return jsonify({'unit': models.get_unit(unit_id)})


# ── Manager: tenants + leases ────────────────────────────────────────────

@rent_api_bp.route('/tenants', methods=['GET', 'POST'])
@manager_required
def tenants():
    if request.method == 'GET':
        return jsonify({'tenants': models.list_tenants()})
    data = _body()
    email = (data.get('email') or '').strip().lower()
    name = (data.get('full_name') or '').strip()
    if not email or not name:
        return jsonify({'error': 'Name and email are required.'}), 400
    if models.get_tenant_by_email(email):
        return jsonify({'error': 'A resident with that email already exists.'}), 409
    tenant = models.create_tenant(email, name, data.get('phone', ''))
    invite_url = url_for('rent_pages.activate', token=tenant['invite_token'],
                         _external=True)
    return jsonify({'tenant': {k: tenant[k] for k in ('id', 'email', 'full_name')},
                    'invite_url': invite_url}), 201


@rent_api_bp.route('/tenants/<int:tenant_id>', methods=['PATCH'])
@manager_required
def patch_tenant(tenant_id):
    models.update_tenant(tenant_id, **_body())
    tenant = models.get_tenant(tenant_id)
    return jsonify({'tenant': {k: tenant[k] for k in ('id', 'email', 'full_name',
                                                      'phone', 'active')}})


@rent_api_bp.route('/tenants/<int:tenant_id>/invite', methods=['POST'])
@manager_required
def resend_invite(tenant_id):
    from backend.rent import mailer
    tenant = models.get_tenant(tenant_id)
    if not tenant:
        return jsonify({'error': 'Resident not found.'}), 404
    token = models.issue_invite(tenant_id)
    invite_url = url_for('rent_pages.activate', token=token, _external=True)
    lease = models.active_lease_for_tenant(tenant_id)
    sent = mailer.send_invite(
        tenant['email'], tenant['full_name'],
        lease['unit_label'] if lease else '—',
        lease['property_name'] if lease else 'your community', invite_url)
    return jsonify({'invite_url': invite_url, 'emailed': sent})


@rent_api_bp.route('/leases', methods=['GET', 'POST'])
@manager_required
def leases():
    if request.method == 'GET':
        return jsonify({'leases': models.list_leases(
            status=request.args.get('status', 'active') or None,
            property_id=request.args.get('property_id', type=int))})
    data = _body()
    required = ('unit_id', 'tenant_id', 'start_date')
    missing = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({'error': f'Missing: {", ".join(missing)}'}), 400
    rent_cents = _cents(data, 'rent_cents')
    if rent_cents <= 0:
        return jsonify({'error': 'Rent must be greater than zero.'}), 400
    existing = [l for l in models.list_leases(status='active')
                if l['unit_id'] == int(data['unit_id'])]
    if existing:
        return jsonify({'error': 'That unit already has an active lease.'}), 409
    lease_id = models.create_lease(
        unit_id=int(data['unit_id']), tenant_id=int(data['tenant_id']),
        start_date=data['start_date'], rent_cents=rent_cents,
        end_date=data.get('end_date'),
        deposit_cents=_cents(data, 'deposit_cents'),
        due_day=int(data.get('due_day', 1)),
        grace_days=int(data.get('grace_days', 5)),
        late_fee_cents=_cents(data, 'late_fee_cents') or 5000,
        late_fee_daily_cents=_cents(data, 'late_fee_daily_cents'))

    if data.get('post_deposit') and _cents(data, 'deposit_cents') > 0:
        models.add_charge(lease_id, 'deposit', _cents(data, 'deposit_cents'),
                          data['start_date'], period=None,
                          description='Security deposit')
    if data.get('post_first_month'):
        billing.generate_charges_for_period(billing.period_of(_today()))
    return jsonify({'lease': models.get_lease(lease_id)}), 201


@rent_api_bp.route('/leases/<int:lease_id>', methods=['PATCH'])
@manager_required
def patch_lease(lease_id):
    data = _body()
    for key in ('rent_cents', 'deposit_cents', 'late_fee_cents', 'late_fee_daily_cents'):
        if key in data or key.replace('_cents', '') in data:
            data[key] = _cents(data, key)
    models.update_lease(lease_id, **data)
    return jsonify({'lease': models.get_lease(lease_id)})


@rent_api_bp.route('/leases/<int:lease_id>/end', methods=['POST'])
@manager_required
def close_lease(lease_id):
    end_date = _body().get('end_date') or _today().isoformat()
    models.end_lease(lease_id, end_date)
    return jsonify({'lease': models.get_lease(lease_id)})


@rent_api_bp.route('/leases/<int:lease_id>/ledger')
@manager_required
def lease_ledger(lease_id):
    lease = models.get_lease(lease_id)
    if not lease:
        return jsonify({'error': 'Lease not found.'}), 404
    return jsonify({'lease': lease,
                    'statement': billing.statement(lease_id),
                    'summary': billing.lease_summary(lease)})


# ── Manager: ledger operations ───────────────────────────────────────────

@rent_api_bp.route('/charges', methods=['POST'])
@manager_required
def add_charge():
    data = _body()
    lease_id = data.get('lease_id')
    amount_cents = _cents(data)
    if not lease_id or amount_cents == 0:
        return jsonify({'error': 'lease_id and a non-zero amount are required.'}), 400
    charge_id = models.add_charge(
        lease_id=int(lease_id), kind=data.get('kind', 'other'),
        amount_cents=amount_cents,
        due_date=data.get('due_date') or _today().isoformat(),
        period=data.get('period'), description=data.get('description', ''))
    if charge_id is None:
        return jsonify({'error': 'That recurring charge is already posted.'}), 409
    return jsonify({'charge_id': charge_id}), 201


@rent_api_bp.route('/charges/<int:charge_id>', methods=['DELETE'])
@manager_required
def remove_charge(charge_id):
    models.delete_charge(charge_id)
    return jsonify({'deleted': charge_id})


@rent_api_bp.route('/payments', methods=['GET'])
@manager_required
def payments():
    return jsonify({'payments': models.list_payments(
        lease_id=request.args.get('lease_id', type=int),
        limit=request.args.get('limit', default=200, type=int))})


@rent_api_bp.route('/payments/manual', methods=['POST'])
@manager_required
def record_manual_payment():
    """Log a check, money order, or cash payment taken at the office."""
    data = _body()
    lease = models.get_lease(int(data.get('lease_id', 0)))
    amount_cents = _cents(data)
    if not lease or amount_cents <= 0:
        return jsonify({'error': 'A lease and a positive amount are required.'}), 400
    method = data.get('method', 'check')
    if method not in ('check', 'cash', 'other'):
        return jsonify({'error': 'Manual payments must be check, cash, or other.'}), 400
    payment_id = models.create_payment(
        lease_id=lease['id'], tenant_id=lease['tenant_id'], amount_cents=amount_cents,
        method=method, status='succeeded', memo=data.get('memo', ''))
    return jsonify({'payment': models.get_payment(payment_id)}), 201


@rent_api_bp.route('/billing/run', methods=['POST'])
@manager_required
def run_billing():
    data = _body()
    period = data.get('period') or billing.period_of(_today())
    result = {'charges': billing.generate_charges_for_period(period)}
    if data.get('late_fees', True):
        result['late_fees'] = billing.apply_late_fees(_today())
    return jsonify(result)


# ── Manager: settings + Stripe status ────────────────────────────────────

def _public_settings() -> dict:
    settings = get_settings()
    return {k: settings.get(k) for k in
            ('allow_card', 'allow_ach', 'allow_partial_payments', 'currency',
             'card_fee_bps', 'card_fee_flat_cents', 'ach_fee_bps',
             'ach_fee_flat_cents', 'support_email', 'support_phone')}


@rent_api_bp.route('/settings', methods=['GET', 'PATCH'])
def settings():
    if request.method == 'GET':
        if is_manager():
            return jsonify({'settings': get_settings(), 'stripe': stripe_gateway.status()})
        return jsonify({'settings': _public_settings()})
    if not is_manager():
        return jsonify({'error': 'Manager access required'}), 403
    updates = _body()
    unknown = [k for k in updates if k not in DEFAULT_SETTINGS]
    if unknown:
        return jsonify({'error': f'Unknown setting(s): {", ".join(unknown)}'}), 400
    for key, value in updates.items():
        set_setting(key, value)
    return jsonify({'settings': get_settings()})


@rent_api_bp.route('/stripe/status')
@manager_required
def stripe_status():
    status = stripe_gateway.status()
    status['webhook_url'] = url_for('rent_api.stripe_webhook', _external=True)
    return jsonify(status)
