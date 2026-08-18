# Rent portal

Rent collection for the apartment portfolio — **Nest**, **Old Port Isabel**,
and **Vermillion**. Residents sign in, see their ledger, and pay by card or
bank transfer through Stripe Checkout. The office runs leasing, billing, and
collections from a manager console.

It runs inside the existing Flask app but is otherwise self-contained: its own
SQLite database (`rent.db` in `DATA_DIR`), its own sessions, its own
stylesheet. Nothing in it reads or writes the macro-research tables.

## URLs

| Path | Who | What |
| --- | --- | --- |
| `/rent` | resident | Balance, lease terms, full statement |
| `/rent/pay` | resident | Amount + method, then Stripe Checkout |
| `/rent/receipt?session_id=…` | resident | Return page from Checkout |
| `/rent/login`, `/rent/forgot`, `/rent/activate/<token>` | resident | Sign-in and onboarding |
| `/rent/manage` | manager | Portfolio KPIs, leases, residents, payments, fee settings |
| `/rent/manage/property/<id>` | manager | Unit roster, inline rent pricing, bulk unit add |
| `/rent/manage/lease/<id>` | manager | Ledger, manual charges, offline payments |
| `/api/rent/*` | both | JSON API (resident routes scoped to the caller's own lease) |
| `/api/rent/stripe/webhook` | Stripe | Signature-verified webhook endpoint |

## Configuration

Environment variables:

| Variable | Purpose |
| --- | --- |
| `STRIPE_SECRET_KEY` | Required for online payments (`sk_test_…` / `sk_live_…`) |
| `STRIPE_PUBLISHABLE_KEY` | Reported in the console for reference |
| `STRIPE_WEBHOOK_SECRET` | Required to accept webhooks (`whsec_…`) |
| `RENT_MANAGERS` | Comma-separated emails allowed into `/rent/manage`. `ADMIN_EMAIL` is always allowed |
| `SMTP_EMAIL` / `SMTP_PASSWORD` | Reused from the main site for invites, resets, receipts. Without them the console shows invite links to copy by hand |

Stripe dashboard setup:

1. Turn on **US bank account (ACH Direct Debit)** under Settings → Payment methods.
2. Add a webhook pointing at `https://<host>/api/rent/stripe/webhook` for:
   `checkout.session.completed`, `checkout.session.async_payment_succeeded`,
   `checkout.session.async_payment_failed`, `charge.refunded`.
3. Copy the signing secret into `STRIPE_WEBHOOK_SECRET`.

Fee pass-through, partial payments, and support contact details are edited in
the console (stored in the `settings` table), not in code. Defaults: card 3.00%
+ $0.30 charged to the resident, ACH free, partial payments allowed.

## Day-to-day flow

1. **Price the units** — `/rent/manage/property/<id>`. Units seed at $0 and
   cannot be leased until priced.
2. **Add the resident** — console → *Add resident*. They get a 72-hour link to
   set a password.
3. **Start the lease** — console → *New lease*. Set rent, deposit, due day,
   grace days, late fee. Optionally post the deposit and the first (prorated)
   month right away.
4. **Rent posts itself** — the scheduler posts every active lease's rent on the
   1st at 08:00 UTC and sweeps for late fees daily at 09:00 UTC. *Post rent +
   late fees* in the console does the same thing on demand; both are idempotent.
5. **Residents pay** — card posts immediately; ACH sits in `processing` for
   3–5 business days and credits the ledger when Stripe says it cleared.
6. **Offline payments** — record checks and cash on the lease ledger page.

## Money rules

* All amounts are integer cents. Nothing is stored as a float.
* Balance = `sum(charges) − sum(payments where status = 'succeeded')`.
  `processing` ACH payments are reported separately so a resident sees the
  payment landed without the balance moving twice.
* Convenience fees are a separate Checkout line item and are **not** credited
  to the ledger — a $1,450 rent payment on a card charges $1,493.80 and credits
  $1,450.
* Rent prorates by day when a lease starts or ends mid-month
  (`monthly rent ÷ days in month × days occupied`).
* Late fees post at most once per lease per period, only after the due date
  plus the grace period, and only while that period is still unpaid.
* Charge rows are unique on `(lease_id, kind, period)`, so re-running billing
  never double-charges.
* Stripe event ids are recorded, so a replayed webhook is a no-op.

## Seeding

`init_db()` runs on app start and seeds the three properties plus the Nest
roster: 64 units on four floors, units 101–105 as 1bd/1ba and the remaining 59
as 2bd/2ba at ~1,080 sqft, with in-unit washer/dryer and trash included. Old
Port Isabel and Vermillion are created without units — add their rosters in the
console (the *Add units* dialog expands ranges like `101-116, 201-216`).

Seeding never overwrites later edits, so pricing and unit changes survive
restarts and redeploys.

## Tests

```
python scripts/test_rent_portal.py
```

Runs against a throwaway `DATA_DIR` and needs no Stripe keys: schema, seeding,
proration, late fees, ACH state transitions, webhook signature handling and
replay, every resident and manager screen, and the API's authorization gates.

## Notes / not yet built

* **Autopay** (saved payment method + scheduled charge) is not implemented.
  Residents pay on demand; Stripe subscriptions or off-session PaymentIntents
  would be the way to add it.
* Rents seed at $0 on purpose — the unit mix was known when this was built, the
  rent roll was not.
* Residents are deliberately separate from the site's research accounts; a rent
  login grants no access to macro data.
