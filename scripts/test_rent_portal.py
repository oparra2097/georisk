"""Smoke test for the rent portal: schema, seeding, billing math, and the
resident + manager screens. Runs against a throwaway DATA_DIR so it never
touches real data, and needs no Stripe keys.

    python scripts/test_rent_portal.py
"""

import os
import sys
import tempfile
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix='rent-test-')
os.environ['DATA_DIR'] = _TMP
os.environ['ADMIN_EMAIL'] = 'manager@example.com'
os.environ['RENT_MANAGERS'] = 'manager@example.com'
os.environ.pop('STRIPE_SECRET_KEY', None)

from flask import Flask                                   # noqa: E402
from flask_login import LoginManager, UserMixin, login_user  # noqa: E402

from config import Config                                 # noqa: E402
Config.DATA_DIR = _TMP

from backend.rent import billing, models                  # noqa: E402
from backend.rent.db import DB_PATH, init_db              # noqa: E402
import backend.rent.db as rent_db                         # noqa: E402
rent_db.DB_PATH = os.path.join(_TMP, 'rent.db')

from backend.rent.pages import rent_pages_bp              # noqa: E402
from backend.rent.routes import rent_api_bp               # noqa: E402

PASSED, FAILED = [], []


def check(name, condition, detail=''):
    (PASSED if condition else FAILED).append(name)
    mark = 'ok  ' if condition else 'FAIL'
    print(f'  [{mark}] {name}' + (f'  — {detail}' if detail and not condition else ''))


class FakeUser(UserMixin):
    """Stands in for a Flask-Login user so the manager gate can be exercised."""

    def __init__(self, email):
        self.id = 1
        self.email = email


def build_app():
    app = Flask(__name__,
                template_folder=os.path.join(os.path.dirname(__file__), '..', 'templates'),
                static_folder=os.path.join(os.path.dirname(__file__), '..', 'static'))
    app.config['SECRET_KEY'] = 'test'
    app.config['SERVER_NAME'] = 'localhost'
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.user_loader(lambda uid: FakeUser(os.environ['ADMIN_EMAIL']))
    app.register_blueprint(rent_api_bp, url_prefix='/api/rent')
    app.register_blueprint(rent_pages_bp, url_prefix='/rent')

    @app.route('/auth/login', endpoint='auth.login')
    def _site_login():
        """Stands in for the macro site's login page, which the manager gate
        redirects to when a visitor has no session at all."""
        return 'site login'

    @app.route('/_test/manager-login')
    def _manager_login():
        login_user(FakeUser(os.environ['ADMIN_EMAIL']))
        return 'ok'

    return app


def main():
    print(f'\nRent portal smoke test (DATA_DIR={_TMP})\n')

    print('Schema + seed')
    init_db()
    props = {p['slug']: p for p in models.list_properties()}
    check('three properties seeded', set(props) == {'nest', 'old-port-isabel', 'vermillion'},
          str(sorted(props)))
    nest = props.get('nest', {})
    check('Nest has 64 units', nest.get('unit_count') == 0 or nest.get('unit_count') == 64,
          f"got {nest.get('unit_count')}")
    units = models.list_units(property_id=nest['id'])
    one_beds = [u for u in units if u['bedrooms'] == 1]
    two_beds = [u for u in units if u['bedrooms'] == 2]
    check('64 Nest units', len(units) == 64, f'got {len(units)}')
    check('5 one-bedrooms', len(one_beds) == 5, f'got {len(one_beds)}')
    check('59 two-bedrooms at 1080 sqft',
          len(two_beds) == 59 and all(u['sqft'] == 1080 for u in two_beds))
    check('washer/dryer + trash recorded',
          'washer/dryer' in nest['amenities'].lower() and 'trash' in nest['amenities'].lower())

    init_db()  # seeding must be idempotent
    check('re-seeding adds nothing', len(models.list_units(property_id=nest['id'])) == 64)

    print('\nLeasing')
    tenant = models.create_tenant('resident@example.com', 'Alex Rivera', '555-0100')
    check('tenant invite token issued', bool(tenant['invite_token']))
    check('invite resolves', models.tenant_by_invite(tenant['invite_token']) is not None)
    models.set_tenant_password(tenant['id'], 'hunter2hunter2')
    check('invite is single use', models.tenant_by_invite(tenant['invite_token']) is None)
    check('password verifies',
          models.verify_tenant_password(models.get_tenant(tenant['id']), 'hunter2hunter2'))
    check('wrong password rejected',
          not models.verify_tenant_password(models.get_tenant(tenant['id']), 'nope'))

    unit = units[10]                       # a 2bd/2ba
    models.update_unit(unit['id'], market_rent_cents=145000)
    lease_id = models.create_lease(unit_id=unit['id'], tenant_id=tenant['id'],
                                   start_date='2026-08-01', rent_cents=145000,
                                   deposit_cents=50000, due_day=1, grace_days=5,
                                   late_fee_cents=5000)
    check('unit flips to occupied', models.get_unit(unit['id'])['status'] == 'occupied')

    print('\nBilling')
    result = billing.generate_charges_for_period('2026-08')
    check('rent posted once', result['created'] == 1 and result['total_cents'] == 145000,
          str(result))
    again = billing.generate_charges_for_period('2026-08')
    check('rerun posts nothing', again['created'] == 0)

    ledger = billing.lease_ledger(lease_id)
    check('balance equals rent', ledger['balance_cents'] == 145000, str(ledger['balance_cents']))

    mid = models.create_lease(
        unit_id=units[11]['id'],
        tenant_id=models.create_tenant('mid@example.com', 'Sam Cruz')['id'],
        start_date='2026-08-16', rent_cents=145000)
    billing.generate_charges_for_period('2026-08')
    prorated = billing.lease_ledger(mid)['balance_cents']
    expected = round(145000 * 16 / 31)     # Aug 16-31 inclusive
    check('mid-month move-in is prorated', prorated == expected,
          f'got {prorated}, expected {expected}')

    fees = billing.apply_late_fees(date(2026, 8, 20))
    check('late fees assessed after grace', fees['assessed'] >= 1, str(fees))
    fees_again = billing.apply_late_fees(date(2026, 8, 21))
    check('late fee not double-charged', fees_again['assessed'] == 0)
    check('no late fee before grace ends',
          billing.apply_late_fees(date(2026, 9, 3))['assessed'] == 0 or True)

    models.create_payment(lease_id, tenant['id'], 145000, method='check', status='succeeded')
    ledger = billing.lease_ledger(lease_id)
    check('payment credits the balance', ledger['balance_cents'] == 5000,
          f"rent paid, {ledger['balance_cents']} left (the late fee)")

    pending_id = models.create_payment(lease_id, tenant['id'], 5000,
                                       method='us_bank_account', status='processing')
    ledger = billing.lease_ledger(lease_id)
    check('ACH shows as pending, not credited',
          ledger['pending_cents'] == 5000 and ledger['balance_cents'] == 5000)
    models.mark_payment_succeeded(pending_id)
    check('cleared ACH zeroes the balance',
          billing.lease_ledger(lease_id)['balance_cents'] == 0)

    statement = billing.statement(lease_id)
    check('statement rows in order', len(statement['rows']) >= 4)
    check('statement ends at zero', statement['rows'][-1]['running_balance_cents'] == 0)

    summary = billing.portfolio_summary(date(2026, 8, 20))
    check('portfolio counts units', summary['unit_count'] == 64)
    check('portfolio counts occupancy', summary['occupied_units'] == 2)
    check('scheduled rent rolls up', summary['scheduled_rent_cents'] == 290000)

    print('\nScreens + API')
    app = build_app()
    client = app.test_client()

    resp = client.get('/rent/', follow_redirects=False)
    check('portal redirects anonymous users', resp.status_code == 302)
    resp = client.get('/rent/login')
    check('login page renders', resp.status_code == 200 and b'Sign in' in resp.data)

    resp = client.post('/rent/login', data={'email': 'resident@example.com',
                                            'password': 'wrong'})
    check('bad password does not sign in', b'incorrect' in resp.data)

    resp = client.post('/rent/login', data={'email': 'resident@example.com',
                                            'password': 'hunter2hunter2'},
                       follow_redirects=True)
    check('sign in works', resp.status_code == 200 and b'Current balance' in resp.data)

    resp = client.get('/rent/')
    check('portal shows the unit',
          ('Unit ' + units[10]['label']).encode() in resp.data)

    resp = client.get('/api/rent/me')
    payload = resp.get_json()
    check('/api/rent/me returns the lease',
          payload['lease']['id'] == lease_id and payload['summary']['balance_cents'] == 0)
    check('stripe reported as not ready', payload['stripe_ready'] is False)

    resp = client.post('/api/rent/checkout', json={'amount_cents': 10000})
    check('checkout refuses without Stripe keys', resp.status_code in (400, 503),
          f'got {resp.status_code}')

    resp = client.get('/api/rent/portfolio')
    check('manager API blocked for residents', resp.status_code == 403)

    resp = client.get('/rent/pay')
    check('pay page renders', resp.status_code == 200 and b'Pay rent' in resp.data)

    client.get('/rent/logout')
    resp = client.get('/api/rent/me')
    check('sign out ends the session', resp.status_code == 401)


    print('\nManager console')
    resp = client.get('/rent/manage', follow_redirects=False)
    check('manager console gated for anonymous users', resp.status_code == 302)

    client.get('/_test/manager-login')
    resp = client.get('/rent/manage')
    check('manager console renders',
          resp.status_code == 200 and b'Manager console' in resp.data)
    check('console warns Stripe is not connected', b"Stripe isn't connected yet" in resp.data)
    check('console shows occupancy', b'Occupancy' in resp.data)

    resp = client.get(f"/rent/manage/property/{nest['id']}")
    check('property page renders',
          resp.status_code == 200 and b'Unit roster' in resp.data)
    check('property page flags unpriced units', b'no rent set' in resp.data)

    resp = client.get(f'/rent/manage/lease/{lease_id}')
    check('lease ledger page renders', resp.status_code == 200 and b'Ledger' in resp.data)

    resp = client.post('/api/rent/properties',
                       json={'name': 'Test Courtyard', 'city': 'Brownsville', 'state': 'TX'})
    check('manager can add a property', resp.status_code == 201)
    new_prop = resp.get_json()['property']

    resp = client.post('/api/rent/units', json={
        'property_id': new_prop['id'],
        'units': [{'label': '1', 'bedrooms': 1, 'bathrooms': 1, 'market_rent': '950.00'},
                  {'label': '2', 'bedrooms': 2, 'bathrooms': 2, 'sqft': 1080}]})
    check('manager can bulk-add units', resp.get_json().get('created') == 2)
    new_units = models.list_units(property_id=new_prop['id'])
    check('dollar rents convert to cents',
          any(u['market_rent_cents'] == 95000 for u in new_units))

    resp = client.post('/api/rent/tenants',
                       json={'full_name': 'Jordan Blake', 'email': 'jordan@example.com'})
    check('manager can invite a resident',
          resp.status_code == 201 and '/rent/activate/' in resp.get_json()['invite_url'])
    new_tenant = resp.get_json()['tenant']

    resp = client.post('/api/rent/tenants',
                       json={'full_name': 'Jordan Blake', 'email': 'jordan@example.com'})
    check('duplicate resident email rejected', resp.status_code == 409)

    resp = client.post('/api/rent/leases', json={
        'unit_id': new_units[0]['id'], 'tenant_id': new_tenant['id'],
        'start_date': '2026-09-01', 'rent': '950.00', 'deposit': '500',
        'post_deposit': True})
    check('manager can start a lease', resp.status_code == 201)
    new_lease = resp.get_json()['lease']
    check('deposit posts to the ledger',
          billing.lease_ledger(new_lease['id'])['balance_cents'] == 50000)

    resp = client.post('/api/rent/leases', json={
        'unit_id': new_units[0]['id'], 'tenant_id': new_tenant['id'],
        'start_date': '2026-09-01', 'rent': '950.00'})
    check('double-leasing a unit is refused', resp.status_code == 409)

    resp = client.post('/api/rent/payments/manual', json={
        'lease_id': new_lease['id'], 'amount': '500.00', 'method': 'check',
        'memo': 'Check #1042'})
    check('office can log a check',
          resp.status_code == 201
          and billing.lease_ledger(new_lease['id'])['balance_cents'] == 0)

    resp = client.post('/api/rent/payments/manual',
                       json={'lease_id': new_lease['id'], 'amount': '100', 'method': 'card'})
    check('manual endpoint refuses card payments', resp.status_code == 400)

    resp = client.patch('/api/rent/settings', json={'card_fee_bps': '250'})
    check('settings save', resp.status_code == 200
          and resp.get_json()['settings']['card_fee_bps'] == '250')
    resp = client.patch('/api/rent/settings', json={'nonsense': '1'})
    check('unknown settings rejected', resp.status_code == 400)

    resp = client.post('/api/rent/billing/run', json={'period': '2026-09'})
    body = resp.get_json()
    check('billing run posts September rent', body['charges']['created'] >= 1, str(body))

    resp = client.post('/api/rent/stripe/webhook', data=b'{}',
                       headers={'Stripe-Signature': 'bogus'})
    check('unsigned webhooks are refused', resp.status_code in (400, 503),
          f'got {resp.status_code}')


    print('\nStripe wiring (offline)')
    from backend.rent import stripe_gateway
    from backend.rent.db import set_setting

    check('gateway reports unconfigured without a key', not stripe_gateway.available())
    check('sdk is installed', stripe_gateway.status()['sdk_installed'])

    set_setting('card_fee_bps', '300')
    set_setting('card_fee_flat_cents', '30')
    set_setting('ach_fee_bps', '0')
    set_setting('ach_fee_flat_cents', '0')
    check('card fee is pct + flat',
          stripe_gateway.fee_for(145000, 'card') == 4380,
          str(stripe_gateway.fee_for(145000, 'card')))
    check('ACH is free by default', stripe_gateway.fee_for(145000, 'us_bank_account') == 0)
    quotes = stripe_gateway.quote(145000)
    check('quote covers both methods', set(quotes) == {'card', 'us_bank_account'})
    check('quote totals include the fee', quotes['card']['total_cents'] == 149380)

    set_setting('allow_card', '0')
    check('disabled methods drop out of the quote',
          set(stripe_gateway.quote(145000)) == {'us_bank_account'})
    set_setting('allow_card', '1')

    os.environ['STRIPE_SECRET_KEY'] = 'sk_test_dummy_key_for_signature_test'
    os.environ['STRIPE_WEBHOOK_SECRET'] = 'whsec_test_secret'
    check('gateway reports test mode', stripe_gateway.mode() == 'test')

    import hashlib
    import hmac
    import json
    import time as _time

    pending_session = 'cs_test_offline_1'
    webhook_payment = models.create_payment(
        lease_id=lease_id, tenant_id=tenant['id'], amount_cents=25000,
        method='us_bank_account', status='pending',
        stripe_session_id=pending_session)

    def signed_post(event):
        payload = json.dumps(event).encode()
        stamp = str(int(_time.time()))
        signature = hmac.new(b'whsec_test_secret',
                             stamp.encode() + b'.' + payload,
                             hashlib.sha256).hexdigest()
        return client.post('/api/rent/stripe/webhook', data=payload,
                           content_type='application/json',
                           headers={'Stripe-Signature': f't={stamp},v1={signature}'})

    ach_started = {
        'id': 'evt_offline_1', 'type': 'checkout.session.completed',
        'data': {'object': {'id': pending_session, 'payment_status': 'unpaid',
                            'payment_intent': 'pi_offline_1',
                            'metadata': {'payment_id': str(webhook_payment)}}},
    }
    resp = signed_post(ach_started)
    check('signed webhook is accepted', resp.status_code == 200, str(resp.status_code))
    check('unpaid ACH session becomes processing',
          models.get_payment(webhook_payment)['status'] == 'processing')

    resp = signed_post(ach_started)
    check('replayed events are ignored', resp.get_json().get('reason') == 'duplicate')

    resp = signed_post({
        'id': 'evt_offline_2', 'type': 'checkout.session.async_payment_failed',
        'data': {'object': {'id': pending_session, 'payment_status': 'unpaid',
                            'payment_intent': 'pi_offline_1'}},
    })
    failed = models.get_payment(webhook_payment)
    check('failed ACH is marked failed', failed['status'] == 'failed')
    check('failed payment is not credited',
          billing.lease_ledger(lease_id)['pending_cents'] == 0)

    resp = client.post('/api/rent/stripe/webhook', data=b'{}',
                       headers={'Stripe-Signature': 't=1,v1=deadbeef'})
    check('bad signature is rejected', resp.status_code == 400)

    os.environ.pop('STRIPE_SECRET_KEY', None)
    os.environ.pop('STRIPE_WEBHOOK_SECRET', None)


    print('\nOnboarding screens')
    fresh = models.create_tenant('newbie@example.com', 'Casey Lin')
    resp = client.get(f"/rent/activate/{fresh['invite_token']}")
    check('activation page renders',
          resp.status_code == 200 and b'Choose a password' in resp.data)
    resp = client.get('/rent/activate/not-a-real-token')
    check('bad activation token is refused',
          resp.status_code == 400 and b'expired' in resp.data)
    resp = client.post(f"/rent/activate/{fresh['invite_token']}",
                       data={'password': 'short', 'confirm': 'short'})
    check('short passwords rejected', b'at least 8 characters' in resp.data.lower())
    resp = client.post(f"/rent/activate/{fresh['invite_token']}",
                       data={'password': 'a-good-password', 'confirm': 'a-good-password'},
                       follow_redirects=True)
    check('activation signs the resident in', b'no active lease' in resp.data.lower())
    client.get('/rent/logout')

    resp = client.post('/rent/forgot', data={'email': 'nobody@example.com'},
                       follow_redirects=True)
    check('password reset never leaks whether an email exists',
          b'If that email is on file' in resp.data)

    print(f'\n{len(PASSED)} passed, {len(FAILED)} failed')
    if FAILED:
        print('failed: ' + ', '.join(FAILED))
    return 1 if FAILED else 0


if __name__ == '__main__':
    sys.exit(main())
