"""HTML pages for the rent portal — mounted at /rent.

Resident-facing screens are plain server-rendered forms so that paying rent
works on an old phone with flaky data; the manager console is server-rendered
too and calls the JSON API for its write actions.
"""

from __future__ import annotations

import logging
from datetime import date

from flask import (Blueprint, flash, redirect, render_template, request,
                   url_for)

from backend.rent import billing, mailer, models, stripe_gateway
from backend.rent.auth import (current_tenant, is_manager, login_tenant,
                               logout_tenant, manager_required, tenant_required)
from backend.rent.db import get_settings

logger = logging.getLogger(__name__)

rent_pages_bp = Blueprint('rent_pages', __name__,
                          template_folder='../../templates')

MIN_PASSWORD_LEN = 8


def money(cents) -> str:
    cents = int(cents or 0)
    sign = '-' if cents < 0 else ''
    return f'{sign}${abs(cents) / 100:,.2f}'


@rent_pages_bp.app_template_filter('money')
def money_filter(cents):
    return money(cents)


def _ctx(**kwargs) -> dict:
    kwargs.setdefault('settings', get_settings())
    kwargs.setdefault('tenant', current_tenant())
    kwargs.setdefault('is_manager', is_manager())
    return kwargs


# ── Resident screens ─────────────────────────────────────────────────────

@rent_pages_bp.route('/')
def portal():
    tenant = current_tenant()
    if not tenant:
        return redirect(url_for('rent_pages.login'))
    leases = models.leases_for_tenant(tenant['id'])
    active = next((l for l in leases if l['status'] == 'active'), None)
    summary = billing.lease_summary(active) if active else None
    statement = billing.statement(active['id']) if active else None
    return render_template('rent/portal.html', **_ctx(
        active_lease=active, leases=leases, summary=summary, statement=statement,
        stripe_ready=stripe_gateway.available()))


@rent_pages_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_tenant():
        return redirect(url_for('rent_pages.portal'))
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''
        tenant = models.get_tenant_by_email(email)
        if tenant and not tenant['active']:
            flash('That account is no longer active. Contact the office.', 'error')
        elif models.verify_tenant_password(tenant, password):
            login_tenant(tenant['id'])
            nxt = request.args.get('next') or request.form.get('next')
            return redirect(nxt if nxt and nxt.startswith('/rent')
                            else url_for('rent_pages.portal'))
        elif tenant and not tenant['password_hash']:
            flash('Your account has not been set up yet — check your email for '
                  'the setup link, or ask the office to resend it.', 'error')
        else:
            flash('Email or password is incorrect.', 'error')
    return render_template('rent/login.html', **_ctx(next=request.args.get('next', '')))


@rent_pages_bp.route('/logout')
def logout():
    logout_tenant()
    flash('You have been signed out.', 'ok')
    return redirect(url_for('rent_pages.login'))


@rent_pages_bp.route('/activate/<token>', methods=['GET', 'POST'])
def activate(token):
    tenant = models.tenant_by_invite(token)
    if not tenant:
        return render_template('rent/set_password.html', **_ctx(
            token=token, invalid=True)), 400
    if request.method == 'POST':
        password = request.form.get('password') or ''
        confirm = request.form.get('confirm') or ''
        if len(password) < MIN_PASSWORD_LEN:
            flash(f'Use at least {MIN_PASSWORD_LEN} characters.', 'error')
        elif password != confirm:
            flash('The two passwords do not match.', 'error')
        else:
            models.set_tenant_password(tenant['id'], password)
            login_tenant(tenant['id'])
            flash('Your password is set. Welcome!', 'ok')
            return redirect(url_for('rent_pages.portal'))
    return render_template('rent/set_password.html', **_ctx(
        token=token, invalid=False, resident=tenant))


@rent_pages_bp.route('/forgot', methods=['GET', 'POST'])
def forgot():
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        tenant = models.get_tenant_by_email(email)
        if tenant and tenant['active']:
            token = models.issue_invite(tenant['id'])
            mailer.send_reset(tenant['email'], tenant['full_name'],
                              url_for('rent_pages.activate', token=token, _external=True))
        # Always the same response — never reveal whether an email is on file.
        flash('If that email is on file, a reset link is on its way.', 'ok')
        return redirect(url_for('rent_pages.login'))
    return render_template('rent/forgot.html', **_ctx())


@rent_pages_bp.route('/pay')
@tenant_required
def pay():
    tenant = current_tenant()
    lease = models.active_lease_for_tenant(tenant['id'])
    if not lease:
        flash('There is no active lease on your account.', 'error')
        return redirect(url_for('rent_pages.portal'))
    summary = billing.lease_summary(lease)
    default_cents = max(summary['amount_due_cents'], 0) or max(summary['balance_cents'], 0)
    return render_template('rent/pay.html', **_ctx(
        lease=lease, summary=summary, default_cents=default_cents,
        quotes=stripe_gateway.quote(default_cents),
        stripe_ready=stripe_gateway.available(),
        canceled=request.args.get('canceled') == '1'))


@rent_pages_bp.route('/receipt')
@tenant_required
def receipt():
    tenant = current_tenant()
    session_id = request.args.get('session_id', '')
    payment = None
    if session_id:
        stripe_gateway.reconcile_session(session_id)
        payment = models.get_payment_by_session(session_id)
        if payment and payment['tenant_id'] != tenant['id']:
            payment = None
    lease = models.get_lease(payment['lease_id']) if payment else None
    if payment and payment['status'] == 'succeeded' and lease:
        mailer.send_receipt(
            tenant['email'], tenant['full_name'], money(payment['amount_cents']),
            f"{lease['property_name']} #{lease['unit_label']}",
            (payment['paid_at'] or date.today().isoformat())[:10],
            url_for('rent_pages.portal', _external=True))
    return render_template('rent/receipt.html', **_ctx(
        payment=payment, lease=lease,
        summary=billing.lease_summary(lease) if lease else None))


# ── Manager screens ──────────────────────────────────────────────────────

@rent_pages_bp.route('/manage')
@manager_required
def manage():
    summary = billing.portfolio_summary()
    return render_template('rent/manage.html', **_ctx(
        summary=summary,
        leases=models.list_leases(status='active'),
        tenants=models.list_tenants(),
        vacant_units=models.list_units(status='vacant'),
        payments=models.list_payments(limit=25),
        stripe=stripe_gateway.status(),
        webhook_url=url_for('rent_api.stripe_webhook', _external=True),
        smtp_ready=mailer.smtp_configured()))


@rent_pages_bp.route('/manage/property/<int:prop_id>')
@manager_required
def manage_property(prop_id):
    prop = models.get_property(prop_id)
    if not prop:
        flash('That property does not exist.', 'error')
        return redirect(url_for('rent_pages.manage'))
    units = models.list_units(property_id=prop_id)
    ledgers = {}
    for unit in units:
        if unit['lease_id']:
            ledgers[unit['lease_id']] = billing.lease_ledger(unit['lease_id'])
    return render_template('rent/property.html', **_ctx(
        property=prop, units=units, ledgers=ledgers,
        tenants=models.list_tenants()))


@rent_pages_bp.route('/manage/lease/<int:lease_id>')
@manager_required
def manage_lease(lease_id):
    lease = models.get_lease(lease_id)
    if not lease:
        flash('That lease does not exist.', 'error')
        return redirect(url_for('rent_pages.manage'))
    return render_template('rent/lease.html', **_ctx(
        lease=lease, statement=billing.statement(lease_id),
        summary=billing.lease_summary(lease)))


@rent_pages_bp.route('/no-access')
def no_access():
    return render_template('rent/no_access.html', **_ctx()), 403
