/* Data Center Securitizations — interactive view */

const SecuritizationsPage = {
  summary: null,
  selectedDealId: null,
  sponsorFilter: null,

  async init() {
    try {
      const r = await fetch('/api/securitizations/summary');
      this.summary = await r.json();
    } catch (e) {
      console.error('summary fetch failed:', e);
      return;
    }
    this.renderKPIs();
    this.renderDealsTable();
    this.renderBySponsor();
    this.renderByTenant();
    this.renderByVintage();
    // Auto-select first deal so the side panel isn't empty.
    if (this.summary.deals?.length) this.selectDeal(this.summary.deals[0].deal_id);
  },

  fmtNum(v) { return v == null ? '—' : Math.round(v).toLocaleString(); },

  ratingClass(r) { return 'rating-' + (r || '').replace(/\+/g, 'plus').replace(/-/g, 'minus') || 'NR'; },

  renderKPIs() {
    const t = this.summary?.totals || {};
    document.getElementById('kpi-deals').textContent = t.deal_count ?? '—';
    document.getElementById('kpi-deals-sub').textContent =
      (t.active_count ? `${t.active_count} active` : '');
    document.getElementById('kpi-issuance').textContent  = this.fmtNum(t.total_size_usd_m);
    document.getElementById('kpi-balance').textContent   = this.fmtNum(t.total_balance_usd_m);
    document.getElementById('kpi-mw-built').textContent  = this.fmtNum(t.mw_built_collateral);
    document.getElementById('kpi-mw-uc').textContent     = this.fmtNum(t.mw_uc_collateral);
  },

  riskColor(score) {
    if (score == null) return '#d1d5db';
    if (score >= 75) return '#dc2626';
    if (score >= 50) return '#f59e0b';
    if (score >= 25) return '#facc15';
    return '#10b981';
  },

  _matchesFilter(d) {
    if (!this.sponsorFilter) return true;
    const f = this.sponsorFilter.toLowerCase();
    const sp = (d.sponsor || '').toLowerCase();
    const cf = (d.collateral_facilities || []).join(' ').toLowerCase();
    return sp.includes(f) || cf.includes(f) || f.includes(sp);
  },

  renderDealsTable() {
    const tbody = document.querySelector('#deals-table tbody');
    let deals = this.summary?.deals || [];
    if (this.sponsorFilter) deals = deals.filter(d => this._matchesFilter(d));
    if (!deals.length) {
      const msg = this.sponsorFilter
        ? `no deals match "${this.sponsorFilter}"`
        : 'no deals loaded — seed pending verification';
      tbody.innerHTML = `<tr><td colspan="13" class="loading">${msg}</td></tr>`;
      return;
    }
    tbody.innerHTML = deals.map(d => {
      const collMw = (d.collateral_mw_built || 0) + (d.collateral_mw_uc || 0);
      const tpillCls = d.tenant_type || 'empty';
      const tpillLbl = d.tenant_type_label || '—';
      const fpillCls = d.datacenter_type || 'empty';
      const fpillLbl = d.datacenter_type_label || '—';
      const cusipTitle = d.cusip_source === 'edgar_fwp_cache' ? 'pulled from EDGAR FWP' : '';
      const cusip = d.cusip_senior
        ? `<span class="cusip-cell" title="${cusipTitle}">${d.cusip_senior}</span>`
        : `<span class="cusip-cell" style="color:#d1d5db;" title="pending EDGAR FWP lookup">pending</span>`;
      const sr = d.stranded_risk_avg;
      const riskCell = sr == null
        ? '<span style="color:#d1d5db;">—</span>'
        : `<span style="display:inline-block;padding:1px 5px;border-radius:3px;background:${this.riskColor(sr)}20;color:${this.riskColor(sr)};font-weight:600;">${sr.toFixed(0)}</span>`;
      return `
        <tr data-deal-id="${d.deal_id}">
          <td><span class="conf-dot conf-${d.confidence || 'medium'}"></span>${d.deal_name}</td>
          <td>${d.sponsor}</td>
          <td><span class="pill ${d.deal_type}">${d.deal_type_label}</span></td>
          <td><span class="tpill ${tpillCls}">${tpillLbl}</span></td>
          <td><span class="fpill ${fpillCls}">${fpillLbl}</span></td>
          <td>${d.issue_date || '—'}</td>
          <td class="num">${this.fmtNum(d.total_size_usd_m)}</td>
          <td class="num">${this.fmtNum(d.current_balance_usd_m)}</td>
          <td><span class="rating ${this.ratingClass(d.rating_senior)}">${d.rating_senior || 'NR'}</span>
              <span style="color:#9ca3af;font-size:10px;">${d.rater || ''}</span></td>
          <td>${cusip}</td>
          <td class="num">${this.fmtNum(collMw)}</td>
          <td class="num">${riskCell}</td>
          <td class="num">${this.fmtNum(d.at_risk_mw_total)}</td>
        </tr>`;
    }).join('');
    tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => this.selectDeal(tr.dataset.dealId));
    });
  },

  selectDeal(deal_id) {
    this.selectedDealId = deal_id;
    document.querySelectorAll('#deals-table tbody tr').forEach(tr =>
      tr.classList.toggle('selected', tr.dataset.dealId === deal_id));
    const d = (this.summary?.deals || []).find(x => x.deal_id === deal_id);
    const panel = document.getElementById('deal-panel');
    if (!d) { panel.innerHTML = '<div class="loading">deal not found</div>'; return; }

    const facChips = (d.collateral_facilities || []).map(name => {
      const matched = (d.facility_matches || []).find(m =>
        m.name.toLowerCase().includes(name.toLowerCase()) ||
        name.toLowerCase().includes(m.name.toLowerCase()));
      const cls = matched ? 'fac-chip matched' : 'fac-chip';
      const ttl = matched
        ? `${matched.market} · ${Math.round(matched.mw)} MW · risk ${(matched.stranded_risk||0).toFixed(0)}/100`
        : 'no facility match in dashboard';
      return `<span class="${cls}" title="${ttl}">${name}</span>`;
    }).join('');

    const tenantChips = (d.top_tenants || []).map(t =>
      `<span class="fac-chip">${t}</span>`).join('');

    panel.innerHTML = `
      <div style="font-weight:600;font-size:13px;margin-bottom:8px;">
        ${d.deal_name}
        <span class="pill ${d.deal_type}" style="margin-left:6px;">${d.deal_type_label}</span>
      </div>
      <dl>
        <dt>Sponsor</dt><dd>${d.sponsor}</dd>
        <dt>Issued</dt><dd>${d.issue_date || '—'}</dd>
        <dt>Final maturity</dt><dd>${d.final_maturity || '—'}</dd>
        <dt>Size</dt><dd>$${this.fmtNum(d.total_size_usd_m)} M</dd>
        <dt>Balance</dt><dd>$${this.fmtNum(d.current_balance_usd_m)} M</dd>
        <dt>Senior rating</dt><dd><span class="rating ${this.ratingClass(d.rating_senior)}">${d.rating_senior || 'NR'}</span> ${d.rater || ''}</dd>
        <dt>WAL</dt><dd>${(d.wal_years || 0).toFixed(1)} yrs</dd>
        <dt>Status</dt><dd>${d.status}</dd>
      </dl>
      <section>
        <h4>Collateral facilities (${d.collateral_facility_count || (d.collateral_facilities || []).length})</h4>
        <div>${facChips || '<span style="color:#9ca3af;">not disclosed in seed</span>'}</div>
        <div style="font-size:10px;color:#9ca3af;margin-top:4px;">
          Built: ${this.fmtNum(d.collateral_mw_built)} MW · UC: ${this.fmtNum(d.collateral_mw_uc)} MW
        </div>
      </section>
      ${(d.tranches && d.tranches.length) ? `
      <section>
        <h4>Capital stack <span style="font-size:9px;color:#9ca3af;font-weight:400;text-transform:none;letter-spacing:0;">(from EDGAR FWP)</span></h4>
        <table style="width:100%;font-size:11px;border-collapse:collapse;">
          <thead>
            <tr style="border-bottom:1px solid #e5e7eb;">
              <th style="text-align:left;padding:3px 4px;color:#6b7280;font-weight:500;">Class</th>
              <th style="text-align:left;padding:3px 4px;color:#6b7280;font-weight:500;">CUSIP</th>
              <th style="text-align:left;padding:3px 4px;color:#6b7280;font-weight:500;">Rating</th>
              <th style="text-align:right;padding:3px 4px;color:#6b7280;font-weight:500;">Cpn %</th>
              <th style="text-align:right;padding:3px 4px;color:#6b7280;font-weight:500;">Size $M</th>
            </tr>
          </thead>
          <tbody>
            ${d.tranches.map(t => `
              <tr style="border-bottom:1px solid #f3f4f6;">
                <td style="padding:3px 4px;">${t.class}</td>
                <td style="padding:3px 4px;font-family:ui-monospace,monospace;">${t.cusip || '—'}</td>
                <td style="padding:3px 4px;">${t.rating ? `<span class="rating ${this.ratingClass(t.rating)}">${t.rating}</span>` : '—'}</td>
                <td style="padding:3px 4px;text-align:right;">${t.coupon != null ? t.coupon.toFixed(2) : '—'}</td>
                <td style="padding:3px 4px;text-align:right;">${t.size_usd_m != null ? this.fmtNum(t.size_usd_m) : '—'}</td>
              </tr>`).join('')}
          </tbody>
        </table>
      </section>` : ''}
      ${d.stranded_risk_avg != null ? `
      <section>
        <h4>Stranded-risk rollup (matched facilities)</h4>
        <div style="display:flex;gap:12px;align-items:center;font-size:12px;">
          <div>
            <span style="display:inline-block;padding:3px 9px;border-radius:4px;background:${this.riskColor(d.stranded_risk_avg)}22;color:${this.riskColor(d.stranded_risk_avg)};font-weight:700;font-size:14px;">
              ${d.stranded_risk_avg.toFixed(0)}/100
            </span>
          </div>
          <div style="color:#6b7280;font-size:11px;line-height:1.5;">
            MW-weighted avg across ${(d.facility_matches || []).length} matched facilities<br>
            ${this.fmtNum(d.at_risk_mw_total)} at-risk MW · ${this.fmtNum(d.matched_mw_total)} total matched MW
          </div>
        </div>
      </section>` : ''}
      <section>
        <h4>Top tenants ${d.top_tenant_share_pct ? `(top one ${d.top_tenant_share_pct.toFixed(0)}%)` : ''}</h4>
        <div>${tenantChips || '<span style="color:#9ca3af;">not disclosed in seed</span>'}</div>
      </section>
      ${d.notes ? `<section><h4>Notes</h4><div style="font-size:11px;color:#374151;">${d.notes}</div></section>` : ''}
      ${d.source_url ? `<section><h4>Source</h4>
        <a href="${d.source_url}" target="_blank" rel="noopener">${new URL(d.source_url).hostname.replace(/^www\./, '')} ↗</a>
      </section>` : ''}
    `;
  },

  _renderBucket(selector, rows, keyField) {
    const tbody = document.querySelector(selector + ' tbody');
    if (!rows?.length) { tbody.innerHTML = `<tr><td colspan="4" class="loading">no data</td></tr>`; return; }
    tbody.innerHTML = rows.map(r => {
      const extra = r.mw_built != null
        ? `<td class="num">${this.fmtNum((r.mw_built || 0) + (r.mw_uc || 0))}</td>`
        : '';
      return `<tr>
        <td>${r[keyField]}</td>
        <td class="num">${r.deals}</td>
        <td class="num">${this.fmtNum(r.size_usd_m)}</td>
        ${extra}
      </tr>`;
    }).join('');
  },

  renderBySponsor() { this._renderBucket('#by-sponsor-table', this.summary?.by_sponsor, 'sponsor'); },
  renderByTenant()  { this._renderBucket('#by-tenant-table',  this.summary?.by_tenant,  'tenant'); },
  renderByVintage() {
    const rows = (this.summary?.by_vintage || []).map(r => ({ ...r, mw_built: r.mw_built, mw_uc: r.mw_uc }));
    this._renderBucket('#by-vintage-table', rows, 'vintage');
  },

  _bucketBy(field, label) {
    const deals = this.summary?.deals || [];
    const m = new Map();
    deals.forEach(d => {
      const k = d[field] || '—';
      const b = m.get(k) || { [label]: k, deals: 0, size_usd_m: 0 };
      b.deals++;
      b.size_usd_m += d.total_size_usd_m || 0;
      m.set(k, b);
    });
    return [...m.values()].sort((a, b) => b.size_usd_m - a.size_usd_m)
      .map(r => ({ ...r, size_usd_m: Math.round(r.size_usd_m) }));
  },
  renderByTenantType() {
    this._renderBucket('#by-tenant-type-table', this._bucketBy('tenant_type_label', 'tenant_type'), 'tenant_type');
  },
  renderByDcType() {
    this._renderBucket('#by-dc-type-table', this._bucketBy('datacenter_type_label', 'datacenter_type'), 'datacenter_type');
  },

  filterBySponsor(name) {
    this.sponsorFilter = name;
    const badge = document.getElementById('deals-filter-badge');
    const nameEl = document.getElementById('deals-filter-name');
    if (name) {
      badge.style.display = '';
      nameEl.textContent = name;
    } else {
      badge.style.display = 'none';
    }
    this.renderDealsTable();
    // Scroll the deals table into view so the user sees the filter took effect.
    document.querySelector('#deals-table')?.scrollIntoView({behavior: 'smooth', block: 'start'});
  },

  async loadPrivateCredit() {
    try {
      const r = await fetch('/api/private-credit/summary');
      if (!r.ok) return;
      const j = await r.json();
      const t = j.totals || {};
      const set = (id, v) => document.getElementById(id).textContent = (v == null ? '—' : Math.round(v).toLocaleString());
      set('pc-kpi-rows',       t.row_count);
      set('pc-kpi-commitment', t.total_commitment_usd_m);
      set('pc-kpi-curated',    t.curated_count);
      set('pc-kpi-bdc',        t.bdc_count);

      const tbody = document.querySelector('#pc-table tbody');
      const rows = j.rows || [];
      if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="loading">no rows loaded</td></tr>';
        return;
      }
      tbody.innerHTML = rows.map(r => {
        const tierColor = r.source_tier === 'bdc_filing' ? '#10b981' : '#f59e0b';
        const sourceLabel = r.source_url
          ? `<a href="${r.source_url}" target="_blank" rel="noopener" style="color:#2563eb;text-decoration:none;">${r.source_tier}↗</a>`
          : r.source_tier;
        const borrowerKey = (r.borrower || '').split(/[\s,/]/)[0];
        return `<tr>
          <td><span class="conf-dot conf-${r.confidence||'medium'}"></span>${r.lender}
              <span style="color:#9ca3af;font-size:10px;">· ${r.lender_type_label || ''}</span></td>
          <td><a href="#" data-borrower="${borrowerKey}" class="pc-borrower-link"
                 style="color:#7c3aed;text-decoration:none;border-bottom:1px dotted #c4b5fd;"
                 title="Show securitized deals also collateralized by ${r.borrower}">${r.borrower}</a></td>
          <td style="font-size:11px;color:#374151;">${(r.deal_type||'').replace(/_/g,' ')}</td>
          <td class="num">${this.fmtNum(r.commitment_usd_m)}</td>
          <td class="num">${this.fmtNum(r.outstanding_usd_m)}</td>
          <td><span style="color:${tierColor};">●</span> ${sourceLabel}</td>
          <td style="color:#6b7280;font-size:11px;">${r.maturity_year || '—'}</td>
        </tr>`;
      }).join('');
      tbody.querySelectorAll('a.pc-borrower-link').forEach(a => {
        a.addEventListener('click', (e) => {
          e.preventDefault();
          this.filterBySponsor(a.dataset.borrower);
        });
      });
    } catch (e) { console.error('private-credit fetch failed:', e); }
  },
};

// Hook the new renderers into init.
const __origInit = SecuritizationsPage.init.bind(SecuritizationsPage);
SecuritizationsPage.init = async function() {
  await __origInit();
  this.renderByTenantType();
  this.renderByDcType();
  this.loadPrivateCredit();
  document.getElementById('deals-filter-clear')?.addEventListener('click', () => this.filterBySponsor(null));
  this.wireAdmin();
};

// ── Admin pipeline: resolve CIKs, then pull CUSIPs + capital stacks ──
SecuritizationsPage.wireAdmin = function() {
  const block = document.getElementById('sec-admin-block');
  if (!block) return;
  const status = document.getElementById('admin-status');
  const results = document.getElementById('cik-results');
  const setStatus = (msg) => { if (status) status.textContent = msg; };

  document.getElementById('btn-cik-resolve')?.addEventListener('click', async () => {
    setStatus('resolving CIKs…');
    results.style.display = '';
    results.innerHTML = '<div style="color:#6b7280;">querying EDGAR full-text search for each missing deal…</div>';

    // Build a list of issuer-name guesses from deals missing edgar_cik.
    const missing = (this.summary?.deals || []).filter(d => !d.edgar_cik);
    if (!missing.length) {
      results.innerHTML = '<div style="color:#10b981;">all 29 deals already have CIKs populated.</div>';
      setStatus(''); return;
    }
    const issuers = missing.map(d => ({
      deal_id: d.deal_id,
      // Strip series-number suffix; keep just the issuer LLC name.
      issuer:  d.deal_name.replace(/,?\s*Series\s+\S.*/i, '').trim(),
    }));
    try {
      const r = await fetch('/api/securitizations/admin/cik/lookup', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ issuers: issuers.map(i => i.issuer) }),
      });
      const j = await r.json();
      if (!j.ok) { setStatus(`error: ${j.error || r.status}`); return; }

      results.innerHTML = j.results.map((res, i) => {
        const meta = issuers[i];
        if (!res.ok) {
          return `<div style="padding:6px 0;border-bottom:1px solid #fde68a;">
            <strong style="color:#374151;">${meta.deal_id}</strong>
            <span style="color:#9ca3af;">(${meta.issuer})</span>
            <span style="color:#dc2626;font-size:10px;">— ${res.error}</span>
          </div>`;
        }
        if (!res.candidates?.length) {
          return `<div style="padding:6px 0;border-bottom:1px solid #fde68a;">
            <strong style="color:#374151;">${meta.deal_id}</strong>
            <span style="color:#9ca3af;">(${meta.issuer})</span>
            <span style="color:#dc2626;font-size:10px;">— no EDGAR hits</span>
          </div>`;
        }
        const candList = res.candidates.slice(0, 5).map(c => `
          <button type="button" data-cik="${c.cik}" class="cik-copy-btn"
            style="margin:2px 4px 2px 0;padding:3px 8px;border:1px solid #d1d5db;background:#fff;border-radius:3px;cursor:pointer;font-family:ui-monospace,monospace;font-size:11px;"
            title="${c.name} · forms: ${(c.form_hits||[]).join(', ')}">
            ${c.cik} <span style="color:#9ca3af;">${c.name}</span>
          </button>`).join('');
        return `<div style="padding:6px 0;border-bottom:1px solid #fde68a;">
          <strong style="color:#374151;">${meta.deal_id}</strong>
          <span style="color:#9ca3af;font-size:10px;">(${meta.issuer})</span>
          <div style="margin-top:4px;">${candList}</div>
        </div>`;
      }).join('');
      results.querySelectorAll('.cik-copy-btn').forEach(b =>
        b.addEventListener('click', () => {
          navigator.clipboard?.writeText(b.dataset.cik);
          b.style.background = '#d1fae5';
          setStatus(`copied ${b.dataset.cik} — paste into edgar_cik column for the row above`);
        }));
      setStatus(`${j.results.filter(r => r.ok && r.candidates?.length).length}/${issuers.length} resolved with candidates`);
    } catch (e) {
      setStatus(`error: ${e.message}`);
    }
  });

  document.getElementById('btn-cusip-pull')?.addEventListener('click', async () => {
    setStatus('pulling FWP filings from EDGAR…');
    try {
      const r = await fetch('/api/securitizations/admin/cusip/pull', {method: 'POST'});
      const j = await r.json();
      setStatus(`resolved ${j.resolved}/${j.attempted} CUSIPs — refreshing…`);
      // Refresh the page-level summary so new CUSIPs and tranches show up.
      await fetch('/api/securitizations/refresh', {method: 'POST'});
      const s = await fetch('/api/securitizations/summary');
      this.summary = await s.json();
      this.renderDealsTable();
      if (this.selectedDealId) this.selectDeal(this.selectedDealId);
      setStatus(`pulled ${j.resolved}/${j.attempted} · page refreshed`);
    } catch (e) {
      setStatus(`error: ${e.message}`);
    }
  });
};
