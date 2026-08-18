/* Manager console: dialog-driven writes against /api/rent/*.
   Every form posts JSON, reports the result in the banner at the top of the
   page, and reloads so the server-rendered numbers stay authoritative. */
(function () {
    'use strict';

    const banner = document.getElementById('manage-result');

    function report(message, kind) {
        if (!banner) { window.alert(message); return; }
        banner.textContent = message;
        banner.className = 'banner banner-' + (kind || 'ok');
        banner.classList.remove('hidden');
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    function post(url, payload, method) {
        return fetch(url, {
            method: method || 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify(payload),
        }).then((res) => res.json().then((body) => {
            if (!res.ok) throw new Error(body.error || 'Request failed (' + res.status + ')');
            return body;
        }));
    }

    function formPayload(form) {
        const payload = {};
        new FormData(form).forEach((value, key) => {
            if (value !== '') payload[key] = value;
        });
        form.querySelectorAll('input[type="checkbox"]').forEach((box) => {
            payload[box.name] = box.checked;
        });
        return payload;
    }

    function openDialog(id) {
        const dlg = document.getElementById(id);
        if (dlg && typeof dlg.showModal === 'function') dlg.showModal();
    }

    /* "101-116, 201" -> one unit row per label, sharing the dialog's mix. */
    function expandUnitLabels(payload) {
        const raw = payload.labels || '';
        delete payload.labels;
        const labels = [];
        raw.split(',').forEach((chunk) => {
            const part = chunk.trim();
            if (!part) return;
            const range = part.match(/^(\d+)\s*-\s*(\d+)$/);
            if (!range) { labels.push(part); return; }
            const width = range[1].length;
            for (let n = parseInt(range[1], 10); n <= parseInt(range[2], 10); n += 1) {
                labels.push(String(n).padStart(width, '0'));
            }
        });
        payload.units = labels.map((label) => ({
            label: label,
            bedrooms: payload.bedrooms,
            bathrooms: payload.bathrooms,
            sqft: payload.sqft,
            market_rent: payload.market_rent,
        }));
    }

    /* Dialog forms ---------------------------------------------------- */
    document.querySelectorAll('dialog.rent-dialog form').forEach((form) => {
        form.addEventListener('submit', (event) => {
            const submitter = event.submitter;
            if (submitter && submitter.value === 'cancel') return;   // let dialog close
            event.preventDefault();
            if (!form.reportValidity()) return;

            const endpoint = form.dataset.endpoint;
            const payload = formPayload(form);
            if (endpoint === '/api/rent/units') expandUnitLabels(payload);
            post(endpoint, payload)
                .then((body) => {
                    form.closest('dialog').close();
                    if (body.invite_url) {
                        window.prompt('Send this setup link to the resident:', body.invite_url);
                    }
                    window.location.reload();
                })
                .catch((err) => report(err.message, 'error'));
        });
    });

    /* Toolbar actions ------------------------------------------------- */
    document.addEventListener('click', (event) => {
        const target = event.target.closest('[data-action]');
        if (!target) return;
        const action = target.dataset.action;

        if (action === 'add-resident') { openDialog('dlg-resident'); return; }
        if (action === 'add-property') { openDialog('dlg-property'); return; }
        if (action === 'add-lease') { openDialog('dlg-lease'); return; }
        if (action === 'add-units') { openDialog('dlg-units'); return; }
        if (action === 'add-charge') { openDialog('dlg-charge'); return; }
        if (action === 'record-payment') { openDialog('dlg-manual'); return; }

        if (action === 'run-billing') {
            if (!window.confirm('Post this month’s rent charges and assess late fees?')) return;
            target.disabled = true;
            post('/api/rent/billing/run', {})
                .then((body) => {
                    report('Posted ' + body.charges.created + ' rent charge(s) for '
                        + body.charges.period + '; assessed '
                        + (body.late_fees ? body.late_fees.assessed : 0) + ' late fee(s).', 'ok');
                    setTimeout(() => window.location.reload(), 1200);
                })
                .catch((err) => { report(err.message, 'error'); target.disabled = false; });
            return;
        }

        if (action === 'invite') {
            post('/api/rent/tenants/' + target.dataset.tenant + '/invite', {})
                .then((body) => {
                    if (body.emailed) report('Setup link emailed.', 'ok');
                    else window.prompt('Email is not configured — send this link:', body.invite_url);
                })
                .catch((err) => report(err.message, 'error'));
            return;
        }

        if (action === 'end-lease') {
            if (!window.confirm('End this lease and mark the unit vacant?')) return;
            post('/api/rent/leases/' + target.dataset.lease + '/end', {})
                .then(() => window.location.reload())
                .catch((err) => report(err.message, 'error'));
            return;
        }

        if (action === 'delete-charge') {
            if (!window.confirm('Remove this charge from the ledger?')) return;
            post('/api/rent/charges/' + target.dataset.charge, {}, 'DELETE')
                .then(() => window.location.reload())
                .catch((err) => report(err.message, 'error'));
        }
    });

    /* Inline unit rent editing ---------------------------------------- */
    document.querySelectorAll('[data-unit-rent]').forEach((input) => {
        input.addEventListener('change', () => {
            post('/api/rent/units/' + input.dataset.unitRent,
                { market_rent: input.value }, 'PATCH')
                .then(() => report('Rent updated for unit ' + input.dataset.unitLabel + '.', 'ok'))
                .catch((err) => report(err.message, 'error'));
        });
    });

    /* Settings -------------------------------------------------------- */
    const settingsForm = document.getElementById('settings-form');
    if (settingsForm) {
        settingsForm.addEventListener('submit', (event) => {
            event.preventDefault();
            post('/api/rent/settings', formPayload(settingsForm), 'PATCH')
                .then(() => report('Payment settings saved.', 'ok'))
                .catch((err) => report(err.message, 'error'));
        });
    }

    /* Prefill lease rent from the unit's market rent ------------------ */
    const unitSelect = document.getElementById('lease-unit');
    const rentInput = document.getElementById('lease-rent');
    if (unitSelect && rentInput) {
        unitSelect.addEventListener('change', () => {
            const option = unitSelect.selectedOptions[0];
            const cents = option ? parseInt(option.dataset.rent || '0', 10) : 0;
            if (cents > 0) rentInput.value = (cents / 100).toFixed(2);
        });
    }
}());
