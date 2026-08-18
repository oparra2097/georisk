"""Transactional email for the rent portal.

Reuses the site's existing Gmail SMTP credentials. Every send is best-effort:
if SMTP is not configured the function returns False and the caller shows the
manager a copyable link instead, so onboarding never hard-depends on email.
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import Config

logger = logging.getLogger(__name__)

FROM_NAME = 'Rent Portal'


def smtp_configured() -> bool:
    return bool(Config.SMTP_EMAIL and Config.SMTP_PASSWORD)


def send(to_email: str, subject: str, html: str, text: str = '') -> bool:
    if not smtp_configured():
        logger.info("SMTP not configured — skipping email to %s (%s)", to_email, subject)
        return False
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'{FROM_NAME} <{Config.SMTP_EMAIL}>'
    msg['To'] = to_email
    msg.attach(MIMEText(text or _strip(html), 'plain'))
    msg.attach(MIMEText(html, 'html'))
    try:
        with smtplib.SMTP_SSL(Config.SMTP_SERVER, Config.SMTP_PORT) as server:
            server.login(Config.SMTP_EMAIL, Config.SMTP_PASSWORD)
            server.send_message(msg)
        return True
    except Exception:
        logger.exception("Failed to send rent-portal email to %s", to_email)
        return False


def _strip(html: str) -> str:
    import re
    return re.sub(r'<[^>]+>', '', html).strip()


def _shell(title: str, body: str, cta_label: str = '', cta_url: str = '') -> str:
    button = ''
    if cta_url:
        button = (f'<p style="margin:28px 0"><a href="{cta_url}" '
                  'style="background:#3b82f6;color:#fff;padding:12px 22px;'
                  'border-radius:6px;text-decoration:none;font-weight:600">'
                  f'{cta_label}</a></p>'
                  f'<p style="font-size:12px;color:#6b7280">Or paste this link into your '
                  f'browser:<br>{cta_url}</p>')
    return f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;
                max-width:560px;margin:0 auto;color:#111827">
      <h2 style="margin:0 0 16px">{title}</h2>
      {body}
      {button}
      <hr style="border:none;border-top:1px solid #e5e7eb;margin:28px 0">
      <p style="font-size:12px;color:#6b7280">Resident portal &middot; questions?
      Just reply to this email.</p>
    </div>"""


def send_invite(email: str, full_name: str, unit_label: str, property_name: str,
                activate_url: str) -> bool:
    body = (f'<p>Hi {full_name},</p>'
            f'<p>Your resident account for <strong>{property_name} #{unit_label}</strong> '
            'is ready. Set a password to view your balance and pay rent online.</p>'
            '<p>The link is good for 72 hours.</p>')
    return send(email, f'Set up your {property_name} resident portal',
                _shell('Welcome home', body, 'Set your password', activate_url))


def send_reset(email: str, full_name: str, reset_url: str) -> bool:
    body = (f'<p>Hi {full_name},</p>'
            '<p>Use the link below to choose a new password. It expires in 72 hours. '
            'If you did not request this, you can ignore this email.</p>')
    return send(email, 'Reset your resident portal password',
                _shell('Password reset', body, 'Choose a new password', reset_url))


def send_receipt(email: str, full_name: str, amount_str: str, unit: str,
                 paid_on: str, portal_url: str) -> bool:
    body = (f'<p>Hi {full_name},</p>'
            f'<p>We received <strong>{amount_str}</strong> for <strong>{unit}</strong> '
            f'on {paid_on}. Thank you!</p>')
    return send(email, f'Payment received — {amount_str}',
                _shell('Payment received', body, 'View your statement', portal_url))
