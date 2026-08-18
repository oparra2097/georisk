/* Resident pay screen: keeps the fee quote and total in step with the amount
   the resident types, then hands off to Stripe Checkout. */
(function () {
    'use strict';

    const form = document.getElementById('pay-form');
    if (!form) return;

    const amountInput = document.getElementById('amount');
    const totalEl = document.getElementById('total-amount');
    const errorEl = document.getElementById('pay-error');
    const submitBtn = document.getElementById('pay-submit');
    const balanceCents = parseInt(form.dataset.balance || '0', 10);
    const allowPartial = form.dataset.partial === '1';

    let quotes = {};
    let quoteTimer = null;

    const fmt = (cents) =>
        (cents < 0 ? '-' : '') + '$' + (Math.abs(cents) / 100).toLocaleString('en-US', {
            minimumFractionDigits: 2, maximumFractionDigits: 2,
        });

    function amountCents() {
        const raw = (amountInput.value || '').replace(/[$,\s]/g, '');
        const value = parseFloat(raw);
        return Number.isFinite(value) ? Math.round(value * 100) : 0;
    }

    function selectedMethod() {
        const checked = form.querySelector('input[name="method"]:checked');
        return checked ? checked.value : 'card';
    }

    function showError(message) {
        if (!message) {
            errorEl.classList.add('hidden');
            errorEl.textContent = '';
            return;
        }
        errorEl.textContent = message;
        errorEl.classList.remove('hidden');
    }

    function validate() {
        const cents = amountCents();
        if (cents <= 0) return 'Enter an amount greater than zero.';
        if (cents > balanceCents && balanceCents > 0) {
            return 'That is more than the ' + fmt(balanceCents) + ' balance on your account.';
        }
        if (!allowPartial && balanceCents > 0 && cents < balanceCents) {
            return 'The full balance of ' + fmt(balanceCents) + ' is required.';
        }
        return '';
    }

    function render() {
        const cents = amountCents();
        const problem = validate();
        showError(problem);
        submitBtn.disabled = Boolean(problem);

        Object.keys(quotes).forEach((method) => {
            const cell = form.querySelector('[data-fee-for="' + method + '"]');
            if (!cell) return;
            const fee = quotes[method].fee_cents;
            cell.innerHTML = fee > 0
                ? '+' + fmt(fee) + ' fee'
                : '<span class="free">No fee</span>';
        });

        const quote = quotes[selectedMethod()];
        totalEl.textContent = quote ? fmt(quote.total_cents) : fmt(cents);
    }

    function refreshQuotes() {
        const cents = amountCents();
        if (cents <= 0) { render(); return; }
        fetch('/api/rent/quote?amount_cents=' + cents, { credentials: 'same-origin' })
            .then((res) => res.json())
            .then((data) => { quotes = data.methods || {}; render(); })
            .catch(() => render());
    }

    amountInput.addEventListener('input', () => {
        render();
        clearTimeout(quoteTimer);
        quoteTimer = setTimeout(refreshQuotes, 250);
    });
    form.addEventListener('change', (event) => {
        if (event.target.name === 'method') render();
    });

    form.addEventListener('submit', (event) => {
        event.preventDefault();
        const problem = validate();
        if (problem) { showError(problem); return; }

        submitBtn.disabled = true;
        submitBtn.textContent = 'Opening secure checkout…';
        showError('');

        fetch('/api/rent/checkout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ amount_cents: amountCents(), method: selectedMethod() }),
        })
            .then((res) => res.json().then((body) => ({ ok: res.ok, body })))
            .then(({ ok, body }) => {
                if (!ok || !body.url) throw new Error(body.error || 'Could not start the payment.');
                window.location.href = body.url;
            })
            .catch((err) => {
                showError(err.message);
                submitBtn.disabled = false;
                submitBtn.textContent = 'Continue to secure checkout';
            });
    });

    refreshQuotes();
}());
