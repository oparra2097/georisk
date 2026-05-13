/* Data Center Securitizations — interactive view */

const SecuritizationsPage = {
  summary: null,
  selectedDealId: null,

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

  renderDealsTable() {
    const tbody = document.querySelector('#deals-table tbody');
    const deals = this.summary?.deals || [];
    if (!deals.length) {
      tbody.innerHTML = '<tr><td colspan="11" class="loading">no deals loaded — seed pending verification</td></tr>';
      return;
    }
    tbody.innerHTML = deals.map(d => {
      const collMw = (d.collateral_mw_built || 0) + (d.collateral_mw_uc || 0);
      const tpillCls = d.tenant_type || 'empty';
      const tpillLbl = d.tenant_type_label || '—';
      const fpillCls = d.datacenter_type || 'empty';
      const fpillLbl = d.datacenter_type_label || '—';
      const cusip = d.cusip_senior
        ? `<span class="cusip-cell">${d.cusip_senior}</span>`
        : `<span class="cusip-cell" style="color:#d1d5db;" title="pending EDGAR FWP lookup">pending</span>`;
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
        return `<tr>
          <td><span class="conf-dot conf-${r.confidence||'medium'}"></span>${r.lender}
              <span style="color:#9ca3af;font-size:10px;">· ${r.lender_type_label || ''}</span></td>
          <td>${r.borrower}</td>
          <td style="font-size:11px;color:#374151;">${(r.deal_type||'').replace(/_/g,' ')}</td>
          <td class="num">${this.fmtNum(r.commitment_usd_m)}</td>
          <td class="num">${this.fmtNum(r.outstanding_usd_m)}</td>
          <td><span style="color:${tierColor};">●</span> ${sourceLabel}</td>
          <td style="color:#6b7280;font-size:11px;">${r.maturity_year || '—'}</td>
        </tr>`;
      }).join('');
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
};
