"""Rent payment portal.

A self-contained module that runs the leasing + rent-collection side of the
apartment portfolio (Old Port Isabel, Vermillion, Nest). It keeps its own
SQLite database (`rent.db` in DATA_DIR) and its own tenant sessions so that
nothing here is coupled to the macro-research side of the site.

Layout:
    db.py              schema, connection, migrations, seed data
    models.py          data access for properties/units/tenants/leases/ledger
    billing.py         monthly rent charges, late fees, balances, proration
    stripe_gateway.py  Stripe Checkout + webhook reconciliation
    auth.py            tenant sessions, invites, password resets, manager gate
    routes.py          /api/rent/* JSON API
    pages.py           /rent/* HTML pages
    mailer.py          transactional email over the site's SMTP settings
"""
