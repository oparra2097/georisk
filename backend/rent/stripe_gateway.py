"""Stripe Checkout integration for rent collection.

Design notes
------------
* One Stripe Checkout Session per payment attempt. The session is created
  server-side with a `payments` row already inserted in `pending`, so a
  payment can always be traced back to a lease even if the tenant closes the
  tab mid-flow.
* Card and ACH are separate sessions, not one session with two options: the
  convenience fee differs per method and Checkout cannot vary a line item by
  the method the tenant ends up picking.
* Webhooks are the source of truth. `reconcile_session()` exists as a
  belt-and-braces fallback for the return-from-Checkout page so a receipt is
  correct even before the webhook lands (or if webhooks are not configured
  yet).
* The module never raises on a missing Stripe SDK or key — `available()`
  reports the truth and the UI tells the manager what to configure. Nothing
  here fabricates a successful payment.
"""

from __future__ import annotations

import logging
import os

from backend.rent import models
from backend.rent.db import get_int_setting, get_setting

logger = logging.getLogger(__name__)

try:  # The SDK is optional at import time so the site boots without it.
    import stripe as _stripe
except ImportError:  # pragma: no cover - depends on deployment
    _stripe = None

METHOD_CARD = 'card'
METHOD_ACH = 'us_bank_account'
SUPPORTED_METHODS = (METHOD_CARD, METHOD_ACH)


class StripeNotConfigured(RuntimeError):
    pass


def _secret_key() -> str:
    return (os.environ.get('STRIPE_SECRET_KEY') or '').strip()


def publishable_key() -> str:
    return (os.environ.get('STRIPE_PUBLISHABLE_KEY') or '').strip()


def webhook_secret() -> str:
    return (os.environ.get('STRIPE_WEBHOOK_SECRET') or '').strip()


def available() -> bool:
    return bool(_stripe is not None and _secret_key())


def mode() -> str:
    key = _secret_key()
    if key.startswith('sk_live_'):
        return 'live'
    if key.startswith('sk_test_'):
        return 'test'
    return 'unconfigured'


def status() -> dict:
    """What the manager portal shows on the Stripe settings card."""
    return {
        'sdk_installed': _stripe is not None,
        'secret_key_set': bool(_secret_key()),
        'publishable_key_set': bool(publishable_key()),
        'webhook_secret_set': bool(webhook_secret()),
        'mode': mode(),
        'ready': available(),
    }


def _client():
    if not available():
        raise StripeNotConfigured(
            'Stripe is not configured. Install the `stripe` package and set '
            'STRIPE_SECRET_KEY (plus STRIPE_WEBHOOK_SECRET) in the environment.')
    _stripe.api_key = _secret_key()
    return _stripe


# ── Fees ─────────────────────────────────────────────────────────────────

def method_enabled(method: str) -> bool:
    if method == METHOD_CARD:
        return get_int_setting('allow_card', 1) == 1
    if method == METHOD_ACH:
        return get_int_setting('allow_ach', 1) == 1
    return False


def fee_for(amount_cents: int, method: str) -> int:
    """Convenience fee passed through to the tenant, in cents."""
    if method == METHOD_CARD:
        bps = get_int_setting('card_fee_bps', 300)
        flat = get_int_setting('card_fee_flat_cents', 30)
    elif method == METHOD_ACH:
        bps = get_int_setting('ach_fee_bps', 0)
        flat = get_int_setting('ach_fee_flat_cents', 0)
    else:
        return 0
    if bps <= 0 and flat <= 0:
        return 0
    return int(round(amount_cents * bps / 10000)) + int(flat)


def quote(amount_cents: int) -> dict:
    """Per-method fee quote for the tenant's pay screen."""
    out = {}
    for method in SUPPORTED_METHODS:
        if not method_enabled(method):
            continue
        fee = fee_for(amount_cents, method)
        out[method] = {
            'method': method,
            'fee_cents': fee,
            'total_cents': amount_cents + fee,
        }
    return out


# ── Checkout ─────────────────────────────────────────────────────────────

def create_checkout_session(lease: dict, tenant: dict, amount_cents: int,
                            method: str, success_url: str, cancel_url: str) -> dict:
    """Insert a pending payment and open a Stripe Checkout Session for it."""
    if method not in SUPPORTED_METHODS:
        raise ValueError(f'Unsupported payment method: {method}')
    if not method_enabled(method):
        raise ValueError(f'{method} payments are turned off for this portfolio.')
    if amount_cents <= 0:
        raise ValueError('Payment amount must be greater than zero.')

    stripe = _client()
    currency = get_setting('currency', 'usd')
    fee_cents = fee_for(amount_cents, method)

    payment_id = models.create_payment(
        lease_id=lease['id'], tenant_id=tenant['id'], amount_cents=amount_cents,
        method=method, status='pending', fee_cents=fee_cents)

    label = f"Rent — {lease['property_name']} #{lease['unit_label']}"
    line_items = [{
        'price_data': {
            'currency': currency,
            'unit_amount': amount_cents,
            'product_data': {'name': label,
                             'description': f"Tenant: {tenant['full_name']}"},
        },
        'quantity': 1,
    }]
    if fee_cents > 0:
        fee_label = ('Card processing fee' if method == METHOD_CARD
                     else 'Bank transfer fee')
        line_items.append({
            'price_data': {
                'currency': currency,
                'unit_amount': fee_cents,
                'product_data': {'name': fee_label},
            },
            'quantity': 1,
        })

    metadata = {
        'portal': 'rent',
        'payment_id': str(payment_id),
        'lease_id': str(lease['id']),
        'tenant_id': str(tenant['id']),
        'unit': f"{lease['property_name']} #{lease['unit_label']}",
        'rent_cents': str(amount_cents),
        'fee_cents': str(fee_cents),
    }

    try:
        session = stripe.checkout.Session.create(
            mode='payment',
            payment_method_types=[method],
            line_items=line_items,
            customer_email=tenant['email'],
            client_reference_id=f"lease-{lease['id']}",
            success_url=success_url,
            cancel_url=cancel_url,
            metadata=metadata,
            payment_intent_data={
                'metadata': metadata,
                'statement_descriptor_suffix': get_setting('statement_descriptor', 'RENT')[:22],
            },
        )
    except Exception as exc:
        models.update_payment(payment_id, status='failed', failure_reason=str(exc)[:400])
        logger.exception("Stripe checkout session failed for lease %s", lease['id'])
        raise

    models.update_payment(payment_id, memo=f'checkout {session.id}')
    _attach_session_id(payment_id, session.id)
    return {'payment_id': payment_id, 'session_id': session.id, 'url': session.url,
            'amount_cents': amount_cents, 'fee_cents': fee_cents}


def _attach_session_id(payment_id: int, session_id: str) -> None:
    from backend.rent.db import get_db
    conn = get_db()
    try:
        conn.execute('UPDATE payments SET stripe_session_id = ? WHERE id = ?',
                     (session_id, payment_id))
        conn.commit()
    finally:
        conn.close()


# ── Webhooks + reconciliation ────────────────────────────────────────────

def _field(obj, key, default=None):
    """Read a field from either a plain dict or a StripeObject.

    stripe-python dropped the dict base class in v12, so `.get()` is not
    available on live event objects even though replayed JSON fixtures are
    plain dicts. This reads both shapes.
    """
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    try:
        value = getattr(obj, key)
    except (AttributeError, KeyError):
        return default
    return default if value is None else value


def verify_webhook(payload: bytes, signature: str):
    stripe = _client()
    secret = webhook_secret()
    if not secret:
        raise StripeNotConfigured('STRIPE_WEBHOOK_SECRET is not set.')
    return stripe.Webhook.construct_event(payload, signature, secret)


def handle_event(event) -> dict:
    """Apply a verified Stripe event to the ledger. Safe to replay."""
    event_id = _field(event, 'id')
    event_type = _field(event, 'type')
    data = _field(_field(event, 'data'), 'object')

    if event_id and models.event_seen(event_id, event_type or ''):
        return {'handled': False, 'reason': 'duplicate', 'type': event_type}

    if event_type == 'checkout.session.completed':
        return _apply_session(data)
    if event_type == 'checkout.session.async_payment_succeeded':
        return _apply_session(data, force_status='succeeded')
    if event_type == 'checkout.session.async_payment_failed':
        return _apply_session(data, force_status='failed')
    if event_type == 'charge.refunded':
        return _apply_refund(data)
    return {'handled': False, 'reason': 'ignored', 'type': event_type}


def _payment_for_session(session) -> dict | None:
    session_id = _field(session, 'id')
    payment = models.get_payment_by_session(session_id) if session_id else None
    if payment:
        return payment
    payment_id = _field(_field(session, 'metadata'), 'payment_id')
    if payment_id:
        return models.get_payment(int(payment_id))
    return None


def _apply_session(session, force_status: str | None = None) -> dict:
    payment = _payment_for_session(session)
    if not payment:
        logger.warning("Stripe session %s has no matching payment row",
                       _field(session, 'id'))
        return {'handled': False, 'reason': 'unknown_payment'}

    payment_status = _field(session, 'payment_status')  # paid | unpaid | no_payment_required
    intent = _field(session, 'payment_intent')
    if not isinstance(intent, str):
        intent = _field(intent, 'id')

    status = force_status
    if status is None:
        status = 'succeeded' if payment_status == 'paid' else 'processing'

    fields = {'status': status, 'stripe_payment_intent': intent or None}
    if status == 'succeeded':
        fields['receipt_url'] = _receipt_url(intent)
        models.mark_payment_succeeded(payment['id'], **fields)
    else:
        if status == 'failed':
            fields['failure_reason'] = 'Stripe reported the payment failed.'
        models.update_payment(payment['id'], **fields)

    logger.info("Stripe session %s -> payment %s is %s",
                _field(session, 'id'), payment['id'], status)
    return {'handled': True, 'payment_id': payment['id'], 'status': status}


def _apply_refund(charge) -> dict:
    intent = _field(charge, 'payment_intent')
    if not isinstance(intent, str):
        intent = _field(intent, 'id')
    if not intent:
        return {'handled': False, 'reason': 'no_intent'}
    from backend.rent.db import get_db
    conn = get_db()
    try:
        row = conn.execute('SELECT id FROM payments WHERE stripe_payment_intent = ?',
                           (intent,)).fetchone()
    finally:
        conn.close()
    if not row:
        return {'handled': False, 'reason': 'unknown_payment'}
    models.update_payment(row['id'], status='refunded')
    return {'handled': True, 'payment_id': row['id'], 'status': 'refunded'}


def _receipt_url(intent_id: str | None) -> str:
    if not intent_id or not available():
        return ''
    try:
        stripe = _client()
        intent = stripe.PaymentIntent.retrieve(intent_id, expand=['latest_charge'])
        return _field(_field(intent, 'latest_charge'), 'receipt_url', '') or ''
    except Exception:  # receipts are a nicety, never worth failing a payment
        logger.debug("Could not fetch receipt for %s", intent_id, exc_info=True)
    return ''


def reconcile_session(session_id: str) -> dict | None:
    """Pull a session from Stripe and apply it — used on the return page so the
    receipt is right even if the webhook has not arrived yet.
    """
    if not available() or not session_id:
        return None
    try:
        stripe = _client()
        session = stripe.checkout.Session.retrieve(session_id)
    except Exception:
        logger.warning("Could not retrieve Stripe session %s", session_id, exc_info=True)
        return None
    result = _apply_session(session)
    payment = models.get_payment_by_session(session_id)
    if payment:
        result['payment'] = payment
    return result
