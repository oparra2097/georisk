/* US Data Center Risk Map — D3 v7, Albers USA. */

const TIER_COLOR = {
  primary:   '#1d4ed8',
  secondary: '#0e7490',
  emerging:  '#b45309',
};

const FUNDING_COLOR = {
  hyperscaler:      '#6366f1',
  reit:             '#059669',
  infra_fund:       '#d97706',
  sovereign_jv:     '#be123c',
  specialty_pe:     '#0891b2',
  public_specialty: '#7c3aed',
};

// Tenant brand-ish palette. Falls back to gray for any unmapped tenant.
const TENANT_COLOR = {
  'Microsoft':            '#0078d4',
  'Meta':                 '#0668e1',
  'Google':               '#ea4335',
  'Amazon':               '#ff9900',
  'OpenAI':               '#10a37f',
  'xAI':                  '#1f2937',
  'Apple':                '#6b7280',
  'Oracle':               '#c74634',
  'Colo (multi-tenant)':  '#94a3b8',
  'Unleased':             '#fbbf24',
};
const TENANT_FALLBACK = '#9ca3af';

let _developerColor = null;  // built lazily once we know the developer set

const METRIC_LABEL = {
  spec_ratio:      'Overbuild risk',
  pipeline_ratio:  'Pipeline intensity',
  inventory_share: 'Concentration',
};

const DataCenterMap = {
  svg: null,
  g: null,
  bubbleLayer: null,
  facilityLayer: null,
  width: 960,
  height: 560,
  projection: null,
  pathGenerator: null,
  markets: [],
  facilities: [],
  fundingTypes: {},
  national: null,
  summary: null,
  tooltipEl: null,
  mode: 'markets',           // 'markets' | 'facilities'
  baseline: 'moderate',      // 'mild' | 'moderate' | 'severe'
  stresses: new Set(),       // any subset of stress_* keys
  scenario: 'moderate',      // legacy: derived label, kept for tooltip text
  colorBy: 'funding',        // 'funding' | 'tenant' | 'developer' | 'risk'
  activeTier: 'all',
  activeMetric: 'spec_ratio',
  activeStatus: 'all',
  activeFunding: 'all',
  activeTenant: 'all',
  activeDeveloper: 'all',
  selectedMarket: null,

  async init() {
    this.tooltipEl = document.getElementById('dc-tooltip');
    this.svg = d3.select('#dc-map-host')
      .append('svg')
      .attr('viewBox', `0 0 ${this.width} ${this.height}`)
      .attr('preserveAspectRatio', 'xMidYMid meet');

    this.svg.append('rect')
      .attr('width', this.width).attr('height', this.height)
      .attr('fill', '#f9fafb');

    this.g = this.svg.append('g');
    this.projection = d3.geoAlbersUsa()
      .scale(1180)
      .translate([this.width / 2, this.height / 2 - 10]);
    this.pathGenerator = d3.geoPath().projection(this.projection);

    this.bubbleLayer = this.svg.append('g').attr('class', 'bubble-layer');
    this.facilityLayer = this.svg.append('g').attr('class', 'facility-layer').style('display', 'none');

    this.bindToolbar();
    this.parseScenarioFromURL();

    await Promise.all([this.loadStates(), this.loadData()]);
    this.render();
  },

  async loadStates() {
    try {
      const us = await d3.json('https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json');
      const states = topojson.feature(us, us.objects.states);
      const nation = topojson.mesh(us, us.objects.states, (a, b) => a !== b);

      this.g.selectAll('path.state')
        .data(states.features)
        .join('path')
        .attr('class', 'state')
        .attr('d', this.pathGenerator);

      this.g.append('path')
        .datum(nation)
        .attr('class', 'state-outline')
        .attr('d', this.pathGenerator);
    } catch (e) {
      console.error('Failed to load US atlas:', e);
    }
  },

  scenarioQuery() {
    const qs = new URLSearchParams();
    if (this.baseline && this.baseline !== 'moderate') qs.set('baseline', this.baseline);
    if (this.stresses.size) qs.set('stresses', Array.from(this.stresses).join(','));
    return qs.toString();
  },

  parseScenarioFromURL() {
    const qs = new URLSearchParams(window.location.search);
    const b = (qs.get('baseline') || '').toLowerCase();
    if (['mild', 'moderate', 'severe'].includes(b)) this.baseline = b;
    const s = qs.get('stresses') || '';
    this.stresses = new Set(
      s.split(',').map(x => x.trim()).filter(Boolean).filter(k =>
        ['stress_openai', 'stress_hyperscaler_pause', 'stress_ercot'].includes(k))
    );
    // Reflect in toolbar visual state
    document.querySelectorAll('.dc-tab[data-baseline]').forEach(b =>
      b.classList.toggle('active', b.dataset.baseline === this.baseline));
    document.querySelectorAll('.dc-tab[data-stress]').forEach(b =>
      b.classList.toggle('active', this.stresses.has(b.dataset.stress)));
  },

  pushURLState() {
    const qs = this.scenarioQuery();
    const url = window.location.pathname + (qs ? '?' + qs : '') + window.location.hash;
    window.history.replaceState(null, '', url);
  },

  scenarioLabel() {
    const baselineLbl = { mild: 'Mild', moderate: 'Moderate', severe: 'Severe' }[this.baseline] || 'Moderate';
    const stressLbls = {
      stress_openai: 'OpenAI -50%',
      stress_hyperscaler_pause: 'capex pause',
      stress_ercot: 'ERCOT crisis',
    };
    const parts = Array.from(this.stresses).map(k => stressLbls[k] || k);
    return parts.length ? `${baselineLbl} + ${parts.join(' + ')}` : baselineLbl;
  },

  async loadData() {
    try {
      const qs = this.scenarioQuery();
      const sfx = qs ? '?' + qs : '';
      const [m, s, f] = await Promise.all([
        fetch('/api/data-centers/markets').then(r => r.json()),
        fetch('/api/data-centers/summary' + sfx).then(r => r.json()),
        fetch('/api/data-centers/facilities' + sfx).then(r => r.json()),
      ]);
      this.markets = m.markets || [];
      this.summary = s || {};
      this.national = s.national || {};
      this.facilities = f.facilities || [];
      this.fundingTypes = f.funding_types || {};
    } catch (e) {
      console.error('Failed to load data center data:', e);
    }
  },

  async onScenarioChange() {
    // Refetch summary + facilities under the new (baseline, stresses) and
    // re-render dependent panels. Markets data is scenario-independent.
    this.pushURLState();
    try {
      const qs = this.scenarioQuery();
      const sfx = qs ? '?' + qs : '';
      const [s, f] = await Promise.all([
        fetch('/api/data-centers/summary' + sfx).then(r => r.json()),
        fetch('/api/data-centers/facilities' + sfx).then(r => r.json()),
      ]);
      this.summary = s || {};
      this.facilities = f.facilities || [];
      this.applyScenario();
    } catch (e) {
      console.error('Failed to refetch under new scenario:', e);
    }
  },

  bindToolbar() {
    document.querySelectorAll('.dc-tab[data-mode]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.dc-tab[data-mode]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.setMode(btn.dataset.mode);
      });
    });
    document.querySelectorAll('.dc-tab[data-baseline]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.dc-tab[data-baseline]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.baseline = btn.dataset.baseline;
        this.onScenarioChange();
      });
    });
    document.querySelectorAll('.dc-tab[data-stress]').forEach(btn => {
      btn.addEventListener('click', () => {
        const key = btn.dataset.stress;
        if (this.stresses.has(key)) { this.stresses.delete(key); btn.classList.remove('active'); }
        else                        { this.stresses.add(key);    btn.classList.add('active'); }
        this.onScenarioChange();
      });
    });
    const stressClear = document.getElementById('stress-clear');
    if (stressClear) stressClear.addEventListener('click', () => {
      this.stresses.clear();
      document.querySelectorAll('.dc-tab[data-stress]').forEach(b => b.classList.remove('active'));
      this.onScenarioChange();
    });
    const share = document.getElementById('share-scenario');
    if (share) share.addEventListener('click', e => {
      e.preventDefault();
      navigator.clipboard.writeText(window.location.href).then(() => {
        share.textContent = 'Link copied ✓';
        setTimeout(() => share.textContent = 'Copy share link', 1500);
      });
    });
    document.querySelectorAll('.dc-tab[data-tier]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.dc-tab[data-tier]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.activeTier = btn.dataset.tier;
        this.renderBubbles();
      });
    });
    document.querySelectorAll('.dc-tab[data-metric]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.dc-tab[data-metric]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.activeMetric = btn.dataset.metric;
        this.renderBubbles();
      });
    });
    document.querySelectorAll('.dc-tab[data-status]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.dc-tab[data-status]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.activeStatus = btn.dataset.status;
        this.renderFacilities();
      });
    });
    document.querySelectorAll('.dc-tab[data-colorby]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.dc-tab[data-colorby]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.colorBy = btn.dataset.colorby;
        this.renderFacilityLegend();
        this.renderFacilities();
      });
    });
    const fundSel = document.getElementById('funding-filter');
    if (fundSel) fundSel.addEventListener('change', () => {
      this.activeFunding = fundSel.value;
      this.renderFacilities();
    });
    const tenSel = document.getElementById('tenant-filter');
    if (tenSel) tenSel.addEventListener('change', () => {
      this.activeTenant = tenSel.value;
      this.renderFacilities();
    });
    const devSel = document.getElementById('developer-filter');
    if (devSel) devSel.addEventListener('change', () => {
      this.activeDeveloper = devSel.value;
      this.renderFacilities();
    });
    const reset = document.getElementById('reset-filters');
    if (reset) reset.addEventListener('click', () => this.resetFacilityFilters());
  },

  // Read the right scenario value off any rollup row.
  // Active scenario values are injected server-side on each row.
  scenarioAtRiskMw(row)    { return row?.at_risk_mw    || 0; },
  scenarioAtRiskShare(row) { return row?.at_risk_share || 0; },

  applyScenario() {
    const help = document.getElementById('scenario-help');
    if (help) {
      const lbl = this.scenarioLabel();
      const stressDescs = Array.from(this.stresses)
        .map(k => this.summary?.scenario_descriptions?.[k]).filter(Boolean);
      help.textContent = stressDescs.length
        ? `${lbl} · ${stressDescs[0]}` + (stressDescs.length > 1 ? ` (+${stressDescs.length - 1} more)` : '')
        : lbl;
    }
    this.renderKPIs();
    this.renderFundingTable();
    this.renderStrandedTable();
    this.renderTenantTable();
    if (this.mode === 'facilities') this.renderFacilities();
  },

  resetFacilityFilters() {
    this.activeFunding = 'all';
    this.activeTenant = 'all';
    this.activeDeveloper = 'all';
    const ids = ['funding-filter', 'tenant-filter', 'developer-filter'];
    ids.forEach(id => { const el = document.getElementById(id); if (el) el.value = 'all'; });
    this.renderFacilities();
  },

  setMode(mode) {
    this.mode = mode;
    const isFacilities = mode === 'facilities';
    document.getElementById('markets-toolbar').style.display = isFacilities ? 'none' : '';
    document.getElementById('facilities-toolbar').style.display = isFacilities ? '' : 'none';
    const filtersBar = document.getElementById('facilities-toolbar-filters');
    if (filtersBar) filtersBar.style.display = isFacilities ? '' : 'none';
    document.getElementById('legend-markets').style.display = isFacilities ? 'none' : '';
    document.getElementById('legend-facilities').style.display = isFacilities ? '' : 'none';
    document.getElementById('mode-help').textContent = isFacilities
      ? `Marker color = ${this.colorBy} · shape = build status · radius ∝ √MW`
      : 'Bubble area ∝ pipeline MW · color = selected metric';
    this.bubbleLayer.style('display', isFacilities ? 'none' : '');
    this.facilityLayer.style('display', isFacilities ? '' : 'none');
    if (isFacilities) {
      this.renderFacilityLegend();
      this.renderFacilities();
    } else {
      this.renderBubbles();
    }
  },

  // ─── Render ────────────────────────────────────────────────────────────
  render() {
    this.buildDeveloperColorScale();
    this.renderKPIs();
    this.renderTierTable();
    this.renderTopTables();
    this.renderFundingTable();
    this.renderStrandedTable();
    this.renderTenantTable();
    this.renderDeveloperTable();
    this.populateFundingFilter();
    this.populateTenantFilter();
    this.populateDeveloperFilter();
    this.renderFacilityLegend();
    this.renderBubbles();
    this.renderFacilities();
    this.applyScenario();  // sets scenario-help text + scenario KPIs
  },

  buildDeveloperColorScale() {
    const devs = Array.from(new Set(this.facilities.map(f => f.developer))).filter(Boolean).sort();
    const palette = (d3.schemeTableau10 || []).concat(d3.schemeSet3 || []);
    _developerColor = d3.scaleOrdinal().domain(devs).range(palette);
  },

  populateFundingFilter() {
    const sel = document.getElementById('funding-filter');
    if (!sel) return;
    const types = this.fundingTypes || {};
    const opts = ['<option value="all">All funding sources</option>']
      .concat(Object.entries(types).map(([k, v]) => `<option value="${k}">${v}</option>`));
    sel.innerHTML = opts.join('');
  },

  populateTenantFilter() {
    const sel = document.getElementById('tenant-filter');
    if (!sel) return;
    const tenants = Array.from(new Set(this.facilities.map(f => f.tenant_norm || 'Unleased')))
      .sort((a, b) => a.localeCompare(b));
    sel.innerHTML = ['<option value="all">All tenants</option>']
      .concat(tenants.map(t => `<option value="${t}">${t}</option>`)).join('');
  },

  populateDeveloperFilter() {
    const sel = document.getElementById('developer-filter');
    if (!sel) return;
    const devs = Array.from(new Set(this.facilities.map(f => f.developer))).filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    sel.innerHTML = ['<option value="all">All developers</option>']
      .concat(devs.map(d => `<option value="${d}">${d}</option>`)).join('');
  },

  _switchToFacilitiesMode() {
    document.querySelectorAll('.dc-tab[data-mode]').forEach(b =>
      b.classList.toggle('active', b.dataset.mode === 'facilities'));
    this.setMode('facilities');
  },

  renderFundingTable() {
    const tbody = document.querySelector('#funding-table tbody');
    const rows = this.summary?.by_funding || [];
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="4" class="loading">no data</td></tr>'; return; }
    const riskColor = pct => {
      if (pct < 5) return '#10b981';
      if (pct < 15) return '#fde047';
      if (pct < 30) return '#f97316';
      return '#dc2626';
    };
    tbody.innerHTML = rows.map(r => {
      const at_risk = this.scenarioAtRiskMw(r);
      const at_share = this.scenarioAtRiskShare(r);
      const pct = at_share * 100;
      return `
        <tr data-funding="${r.funding_type}">
          <td>
            <span class="swatch" style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${FUNDING_COLOR[r.funding_type] || '#9ca3af'};margin-right:6px;vertical-align:middle;"></span>
            ${r.label}
          </td>
          <td class="num">${Math.round(r.mw).toLocaleString()}</td>
          <td class="num">${Math.round(at_risk).toLocaleString()}</td>
          <td class="num" style="color:${riskColor(pct)};font-weight:600;">${pct.toFixed(1)}%</td>
        </tr>`;
    }).join('');
    tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        this._switchToFacilitiesMode();
        this.activeFunding = tr.dataset.funding;
        this.activeTenant = 'all';
        this.activeDeveloper = 'all';
        const f = document.getElementById('funding-filter');     if (f) f.value = this.activeFunding;
        const t = document.getElementById('tenant-filter');      if (t) t.value = 'all';
        const d = document.getElementById('developer-filter');   if (d) d.value = 'all';
        this.renderFacilities();
      });
    });
  },

  renderStrandedTable() {
    const tbody = document.querySelector('#stranded-table tbody');
    const rows = this.summary?.top_stranded_risk || [];
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="4" class="loading">no data</td></tr>'; return; }
    const scale = this.riskScale();
    tbody.innerHTML = rows.map(r => {
      const c = scale(r.stranded_risk || 0);
      const shortName = r.name.length > 36 ? r.name.slice(0, 34) + '…' : r.name;
      const at_risk = r.at_risk_mw || 0;
      return `
        <tr title="${r.name}\nTenant: ${r.tenant_norm} (${r.tenant_credit_label})\nFunding: ${r.funding_detail || r.funding_type}\nStatus: ${r.status}">
          <td>${shortName}</td>
          <td class="num">
            <span style="display:inline-block;width:30px;height:5px;background:#f3f4f6;border-radius:3px;vertical-align:middle;margin-right:4px;">
              <span style="display:block;height:100%;border-radius:3px;width:${(r.stranded_risk||0).toFixed(0)}%;background:${c};"></span>
            </span>
            ${(r.stranded_risk || 0).toFixed(0)}
          </td>
          <td class="num">${Math.round(r.mw).toLocaleString()}</td>
          <td class="num" style="color:#991b1b;font-weight:600;">${Math.round(at_risk).toLocaleString()}</td>
        </tr>`;
    }).join('');
  },

  renderTenantTable() {
    const tbody = document.querySelector('#tenant-table tbody');
    const rows = this.summary?.by_tenant || [];
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="4" class="loading">no data</td></tr>'; return; }
    tbody.innerHTML = rows.map(r => {
      const color = TENANT_COLOR[r.tenant] || TENANT_FALLBACK;
      const at_risk = this.scenarioAtRiskMw(r);
      const tip = [];
      if (r.tenant_capex_b != null && r.tenant_fcf_b != null)
        tip.push(`~$${r.tenant_capex_b}B 2025 AI capex on ~$${r.tenant_fcf_b}B FCF`);
      if (r.tenant_rating && r.tenant_spread_bps != null)
        tip.push(`${r.tenant_rating} · ~${r.tenant_spread_bps} bps spread (illustrative)`);
      const tipAttr = tip.length ? ` title="${tip.join(' · ')}"` : '';
      const ratingCell = r.tenant_rating
        ? `<span style="font-size:10px;color:#6b7280;">${r.tenant_rating}</span>
           <span style="font-size:10px;color:#9ca3af;margin-left:4px;">${r.tenant_spread_bps != null ? r.tenant_spread_bps + 'bps' : ''}</span>`
        : `<span style="color:#cbd5e1;font-size:10px;">—</span>`;
      return `
        <tr data-tenant="${r.tenant}"${tipAttr}>
          <td>
            <span class="swatch" style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${color};margin-right:6px;vertical-align:middle;"></span>
            ${r.tenant}
            <div>${ratingCell}</div>
          </td>
          <td class="num">${r.count}</td>
          <td class="num">${Math.round(r.mw).toLocaleString()}</td>
          <td class="num" style="color:${at_risk > 0 ? '#991b1b' : '#9ca3af'};">${Math.round(at_risk).toLocaleString()}</td>
        </tr>`;
    }).join('');
    tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        this._switchToFacilitiesMode();
        this.activeTenant = tr.dataset.tenant;
        this.activeFunding = 'all';
        this.activeDeveloper = 'all';
        const f = document.getElementById('funding-filter');     if (f) f.value = 'all';
        const t = document.getElementById('tenant-filter');      if (t) t.value = this.activeTenant;
        const d = document.getElementById('developer-filter');   if (d) d.value = 'all';
        // Auto-switch color encoding to tenant for clarity
        document.querySelectorAll('.dc-tab[data-colorby]').forEach(b =>
          b.classList.toggle('active', b.dataset.colorby === 'tenant'));
        this.colorBy = 'tenant';
        this.renderFacilityLegend();
        this.renderFacilities();
      });
    });
  },

  renderDeveloperTable() {
    const tbody = document.querySelector('#developer-table tbody');
    const rows = this.summary?.top_developers || [];
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="4" class="loading">no data</td></tr>'; return; }
    tbody.innerHTML = rows.map(r => {
      const devColor = (_developerColor && _developerColor(r.developer)) || '#9ca3af';
      return `
        <tr data-developer="${r.developer}">
          <td>${r.developer}</td>
          <td>
            <span class="swatch" style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${devColor};vertical-align:middle;"></span>
          </td>
          <td class="num">${Math.round(r.mw).toLocaleString()}</td>
          <td class="num">${(r.share * 100).toFixed(1)}%</td>
        </tr>`;
    }).join('');
    tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        this._switchToFacilitiesMode();
        this.activeDeveloper = tr.dataset.developer;
        this.activeFunding = 'all';
        this.activeTenant = 'all';
        const f = document.getElementById('funding-filter');     if (f) f.value = 'all';
        const t = document.getElementById('tenant-filter');      if (t) t.value = 'all';
        const d = document.getElementById('developer-filter');   if (d) d.value = this.activeDeveloper;
        document.querySelectorAll('.dc-tab[data-colorby]').forEach(b =>
          b.classList.toggle('active', b.dataset.colorby === 'developer'));
        this.colorBy = 'developer';
        this.renderFacilityLegend();
        this.renderFacilities();
      });
    });
  },

  renderKPIs() {
    const n = this.national || {};
    const fmt = v => v == null ? '—' : Math.round(v).toLocaleString();
    document.getElementById('kpi-inventory').textContent = fmt(n.inventory_mw);
    document.getElementById('kpi-uc').textContent = fmt(n.under_construction_mw);
    document.getElementById('kpi-planned').textContent = fmt(n.planned_mw);
    document.getElementById('kpi-markets').textContent = (n.market_count || 0) + ' markets';
    if (n.pipeline_ratio != null) {
      document.getElementById('kpi-pipeline-ratio').textContent = (n.pipeline_ratio).toFixed(2) + '×';
      document.getElementById('kpi-pipeline-sub').textContent =
        n.pipeline_ratio > 1 ? 'pipeline exceeds installed base' : 'pipeline below installed base';
    }
    const ew  = this.summary?.expected_writedown_mw;
    const ewS = this.summary?.expected_writedown_share;
    const wEl = document.getElementById('kpi-writedown');
    const sEl = document.getElementById('kpi-writedown-sub');
    if (wEl && ew != null) {
      wEl.textContent = Math.round(ew).toLocaleString();
      sEl.textContent = `${(ewS * 100).toFixed(1)}% of named MW · ${this.scenarioLabel()}`;
    }
  },

  renderTierTable() {
    const tbody = document.querySelector('#tier-table tbody');
    const rows = (this.summary?.by_tier || []);
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="7" class="loading">no data</td></tr>'; return; }
    tbody.innerHTML = rows.map(r => `
      <tr>
        <td><span class="pill ${r.tier}">${r.tier}</span></td>
        <td class="num">${r.count}</td>
        <td class="num">${Math.round(r.inventory_mw).toLocaleString()}</td>
        <td class="num">${Math.round(r.under_construction_mw).toLocaleString()}</td>
        <td class="num">${Math.round(r.planned_mw).toLocaleString()}</td>
        <td class="num">${(r.pipeline_ratio).toFixed(2)}×</td>
        <td class="num">${(r.spec_ratio).toFixed(2)}</td>
      </tr>`).join('');
  },

  renderTopTables() {
    const tierPill = m => `<span class="pill ${m.tier}">${m.tier[0].toUpperCase()}</span>`;
    const fillTable = (sel, rows, valFn, pctFn, valLabel, pctLabel) => {
      const tbody = document.querySelector(sel + ' tbody');
      if (!rows?.length) { tbody.innerHTML = `<tr><td colspan="4" class="loading">no data</td></tr>`; return; }
      tbody.innerHTML = rows.map(r => `
        <tr data-market="${r.market}">
          <td>${r.market}</td>
          <td>${tierPill(r)}</td>
          <td class="num">${valFn(r)}</td>
          <td class="num">${pctFn(r)}</td>
        </tr>`).join('');
      tbody.querySelectorAll('tr').forEach(tr => {
        tr.addEventListener('click', () => {
          this.selectedMarket = tr.dataset.market;
          this.renderBubbles();
        });
      });
    };
    fillTable('#overbuild-table', this.summary?.top_overbuild,
      r => Math.round(r.speculative_uc_mw).toLocaleString(),
      r => r.spec_ratio.toFixed(2));
    fillTable('#concentration-table', this.summary?.top_concentration,
      r => Math.round(r.inventory_mw).toLocaleString(),
      r => (r.inventory_share * 100).toFixed(1) + '%');
    fillTable('#pipeline-table', this.summary?.top_pipeline_share,
      r => Math.round(r.pipeline_mw).toLocaleString(),
      r => (r.pipeline_share * 100).toFixed(1) + '%');
  },

  // ─── Bubbles ───────────────────────────────────────────────────────────
  metricExtent() {
    const vals = this.markets.map(m => m[this.activeMetric] || 0);
    const max = Math.max(0.01, d3.max(vals) || 0.01);
    return [0, max];
  },

  colorScale() {
    const [lo, hi] = this.metricExtent();
    return d3.scaleLinear()
      .domain([lo, lo + (hi - lo) * 0.33, lo + (hi - lo) * 0.66, hi])
      .range(['#10b981', '#fde047', '#f97316', '#b91c1c'])
      .clamp(true);
  },

  radiusScale() {
    const max = d3.max(this.markets, m => m.pipeline_mw) || 1;
    // bubble area ∝ pipeline_mw  →  r ∝ sqrt
    return d3.scaleSqrt().domain([0, max]).range([4, 38]);
  },

  renderBubbles() {
    if (!this.markets.length) return;
    const color = this.colorScale();
    const radius = this.radiusScale();

    const visible = this.markets.filter(m => {
      if (this.activeTier === 'all') return true;
      return m.tier === this.activeTier;
    });

    // Use full set so dimmed markets still render.
    const join = this.bubbleLayer.selectAll('g.dc-marker')
      .data(this.markets, d => d.market);

    const enter = join.enter().append('g').attr('class', 'dc-marker');

    // Tier ring (outer, colored by tier)
    enter.append('circle').attr('class', 'dc-tier-ring');
    // Filled bubble (colored by metric)
    enter.append('circle').attr('class', 'dc-bubble');
    // Optional label
    enter.append('text').attr('class', 'dc-label');

    const merged = enter.merge(join);

    merged.attr('transform', d => {
      const p = this.projection([d.lon, d.lat]);
      return p ? `translate(${p[0]},${p[1]})` : 'translate(-9999,-9999)';
    });

    merged.select('circle.dc-bubble')
      .attr('r', d => radius(d.pipeline_mw))
      .attr('fill', d => color(d[this.activeMetric] || 0))
      .attr('fill-opacity', 0.78)
      .attr('stroke', '#0f172a')
      .attr('stroke-width', 0.8)
      .classed('dim', d => this.activeTier !== 'all' && d.tier !== this.activeTier)
      .on('mouseover', (e, d) => this.showTip(e, d))
      .on('mousemove', (e) => this.moveTip(e))
      .on('mouseout', () => this.hideTip())
      .on('click', (e, d) => {
        this.selectedMarket = d.market;
        this.renderBubbles();
      });

    merged.select('circle.dc-tier-ring')
      .attr('r', d => radius(d.pipeline_mw) + 3)
      .attr('stroke', d => TIER_COLOR[d.tier])
      .attr('stroke-width', d => d.market === this.selectedMarket ? 3.2 : 1.6)
      .attr('stroke-opacity', d => (this.activeTier !== 'all' && d.tier !== this.activeTier) ? 0.18 : 0.85)
      .attr('fill', 'none');

    // Only label markets above a size threshold to keep it readable
    merged.select('text.dc-label')
      .attr('y', d => -(radius(d.pipeline_mw) + 6))
      .attr('text-anchor', 'middle')
      .style('opacity', d => {
        if (radius(d.pipeline_mw) < 12) return 0;
        if (this.activeTier !== 'all' && d.tier !== this.activeTier) return 0.2;
        return 1;
      })
      .text(d => d.market.length > 22 ? d.market.split(/[-/ ]/)[0] : d.market);
  },

  // ─── Facilities ────────────────────────────────────────────────────────
  facilityRadius() {
    const max = d3.max(this.facilities, f => f.mw) || 1;
    return d3.scaleSqrt().domain([0, max]).range([3, 22]);
  },

  riskScale() {
    return d3.scaleLinear()
      .domain([0, 30, 55, 75, 100])
      .range(['#10b981', '#fde047', '#f97316', '#dc2626', '#7f1d1d'])
      .clamp(true);
  },

  colorForFacility(d) {
    if (this.colorBy === 'tenant') {
      const key = d.tenant_norm || 'Unleased';
      return TENANT_COLOR[key] || TENANT_FALLBACK;
    }
    if (this.colorBy === 'developer') {
      return (_developerColor && _developerColor(d.developer)) || '#9ca3af';
    }
    if (this.colorBy === 'risk') {
      return this.riskScale()(d.stranded_risk || 0);
    }
    return FUNDING_COLOR[d.funding_type] || '#9ca3af';
  },

  facilityPasses(f) {
    if (this.activeStatus !== 'all' && f.status !== this.activeStatus) return false;
    if (this.activeFunding !== 'all' && f.funding_type !== this.activeFunding) return false;
    if (this.activeTenant !== 'all' && (f.tenant_norm || 'Unleased') !== this.activeTenant) return false;
    if (this.activeDeveloper !== 'all' && f.developer !== this.activeDeveloper) return false;
    return true;
  },

  renderFacilities() {
    if (!this.facilities.length) return;
    const radius = this.facilityRadius();

    const join = this.facilityLayer.selectAll('circle.dc-fac')
      .data(this.facilities, d => d.name);

    const enter = join.enter().append('circle').attr('class', 'dc-fac');
    const merged = enter.merge(join);

    merged
      .attr('cx', d => { const p = this.projection([d.lon, d.lat]); return p ? p[0] : -9999; })
      .attr('cy', d => { const p = this.projection([d.lon, d.lat]); return p ? p[1] : -9999; })
      .attr('r', d => radius(d.mw))
      .attr('fill', d => d.status === 'planned' ? '#fff' : this.colorForFacility(d))
      .attr('fill-opacity', d => d.status === 'planned' ? 0 : (d.status === 'under_construction' ? 0.55 : 0.85))
      .attr('stroke', d => this.colorForFacility(d))
      .attr('stroke-width', d => d.status === 'planned' ? 1.6 : (d.status === 'under_construction' ? 1.8 : 0.8))
      .attr('stroke-dasharray', d => d.status === 'under_construction' ? '3 2' : null)
      .style('opacity', d => this.facilityPasses(d) ? 1 : 0.08)
      .style('cursor', 'pointer')
      .on('mouseover', (e, d) => this.showFacilityTip(e, d))
      .on('mousemove', (e) => this.moveTip(e))
      .on('mouseout', () => this.hideTip());

    document.getElementById('mode-help').textContent =
      `Marker color = ${this.colorBy} · shape = build status · radius ∝ √MW`;
  },

  renderFacilityLegend() {
    const host = document.getElementById('legend-facilities-swatches');
    if (!host) return;
    const swatch = (color, label) =>
      `<span><span class="swatch" style="background:${color}"></span>${label}</span>`;

    let items = [];
    if (this.colorBy === 'funding') {
      const types = this.fundingTypes || {};
      items = Object.entries(types).map(([k, label]) => swatch(FUNDING_COLOR[k] || '#9ca3af', label));
    } else if (this.colorBy === 'tenant') {
      const present = (this.summary?.by_tenant || []).map(t => t.tenant);
      items = present.map(name => swatch(TENANT_COLOR[name] || TENANT_FALLBACK, name));
    } else if (this.colorBy === 'developer') {
      // developer — show top 8 to keep it readable, plus an "other" swatch
      const top = (this.summary?.top_developers || []).slice(0, 8).map(d => d.developer);
      items = top.map(dev => swatch((_developerColor && _developerColor(dev)) || '#9ca3af', dev));
      items.push(`<span style="color:#6b7280;font-size:10px;">…hover any marker for full attribution</span>`);
    } else {
      // risk — gradient bar with low/mid/high anchors
      items = [
        `<span style="display:inline-flex;align-items:center;gap:8px;">
          low
          <span style="display:inline-block;width:160px;height:8px;border-radius:4px;
            background:linear-gradient(90deg,#10b981,#fde047,#f97316,#dc2626,#7f1d1d);"></span>
          high (0 → 100 stranded-risk score)
        </span>`,
        swatch('#10b981', 'Locked / IG'),
        swatch('#fde047', 'Watch'),
        swatch('#f97316', 'Elevated'),
        swatch('#dc2626', 'High'),
        swatch('#7f1d1d', 'Severe'),
      ];
    }
    host.innerHTML = items.join('');
  },

  showFacilityTip(event, d) {
    const fundingLabel = (this.fundingTypes && this.fundingTypes[d.funding_type]) || d.funding_type;
    const statusLabel = { built: 'Built', under_construction: 'Under construction', planned: 'Planned' }[d.status] || d.status;
    const target = d.target_online ? ` · target online ${d.target_online}` : '';
    const tt = this.tooltipEl;
    const drv = d.risk_drivers || {};
    const riskColor = this.riskScale()(d.stranded_risk || 0);
    const credit = d.tenant_credit_label || d.tenant_credit_tier || '';
    tt.innerHTML = `
      <div><span class="ttl">${d.name}</span></div>
      <dl>
        <dt>Status</dt><dd>${statusLabel}${target}</dd>
        <dt>Capacity</dt><dd>${Math.round(d.mw).toLocaleString()} MW</dd>
        <dt>Operator</dt><dd>${d.operator || '—'}</dd>
        <dt>Developer</dt><dd>${d.developer || '—'}</dd>
        <dt>Funding</dt><dd><span style="color:${FUNDING_COLOR[d.funding_type] || '#fff'};font-weight:600;">${fundingLabel}</span></dd>
        <dt></dt><dd style="color:#cbd5e1;">${d.funding_detail || ''}</dd>
        ${d.tenant ? `<dt>Tenant</dt><dd>${d.tenant} <span style="color:#94a3b8;">(${credit})</span></dd>` : ''}
        ${d.tenant_rating ? `<dt>Rating</dt><dd>${d.tenant_rating} <span style="color:#94a3b8;">· ${d.tenant_spread_bps != null ? d.tenant_spread_bps + ' bps spread' : 'no spread'} · ~${(d.tenant_annual_pd*100).toFixed(2)}% PD/yr</span></dd>` : ''}
        <dt>Market</dt><dd>${d.market}</dd>
      </dl>
      <div style="margin-top:8px;padding-top:6px;border-top:1px solid #334155;">
        <div style="display:flex;align-items:center;gap:8px;">
          <span style="color:#94a3b8;font-size:10px;text-transform:uppercase;letter-spacing:0.05em;">Stranded risk</span>
          <span style="display:inline-block;flex:1;height:5px;background:#1f2937;border-radius:3px;">
            <span style="display:block;height:100%;width:${(d.stranded_risk||0).toFixed(0)}%;background:${riskColor};border-radius:3px;"></span>
          </span>
          <span style="color:${riskColor};font-weight:700;font-size:11px;">${(d.stranded_risk||0).toFixed(0)}/100</span>
        </div>
        <div style="font-size:10px;color:#94a3b8;margin-top:4px;line-height:1.5;">
          tenant ${drv.tenant_concentration ?? 0} · credit ${drv.tenant_credit ?? 0} ·
          spec ${drv.speculative_build ?? 0} · funding ${drv.funding_resilience ?? 0} ·
          geo ${drv.geographic_correlation ?? 0} · stretch ${drv.tenant_stretch ?? 0}
        </div>
        ${(d.tenant_fcf_b != null && d.tenant_capex_b != null) ? `
          <div style="font-size:10px;color:#cbd5e1;margin-top:3px;">
            ${d.tenant} ~$${d.tenant_capex_b}B 2025 AI capex on ~$${d.tenant_fcf_b}B FCF
          </div>` : ''}
        ${(() => {
          const sb = d.at_risk_mw_baselines || {};
          return `<div style="font-size:10px;color:#fca5a5;margin-top:5px;line-height:1.5;">
            <div style="font-weight:600;">At-risk MW (${this.scenarioLabel()}):
              ${Math.round(d.at_risk_mw || 0).toLocaleString()}
              <span style="color:#94a3b8;font-weight:400;">
                · ${((d.writedown_prob || 0) * 100).toFixed(0)}% prob × MW
              </span>
            </div>
            <div style="color:#94a3b8;">
              baselines: mild ${Math.round(sb.mild ?? 0)} · moderate ${Math.round(sb.moderate ?? 0)} · severe ${Math.round(sb.severe ?? 0)} MW
            </div>
          </div>`;
        })()}
      </div>
      ${d.notes ? `<div class="note" style="margin-top:6px;">${d.notes}</div>` : ''}
    `;
    tt.classList.add('show');
    this.moveTip(event);
  },

  // ─── Tooltip ───────────────────────────────────────────────────────────
  showTip(event, d) {
    const tt = this.tooltipEl;
    const pct = v => (v * 100).toFixed(1) + '%';
    const num = v => Math.round(v).toLocaleString();
    tt.innerHTML = `
      <div><span class="ttl">${d.market}</span><span class="tier ${d.tier}">${d.tier}</span></div>
      <dl>
        <dt>Built</dt><dd>${num(d.inventory_mw)} MW</dd>
        <dt>Under construction</dt><dd>${num(d.under_construction_mw)} MW (${d.preleased_pct.toFixed(0)}% pre-leased)</dd>
        <dt>Planned</dt><dd>${num(d.planned_mw)} MW</dd>
        <dt>Pipeline / Built</dt><dd>${d.pipeline_ratio.toFixed(2)}×</dd>
        <dt>Spec ratio</dt><dd>${d.spec_ratio.toFixed(2)}</dd>
        <dt>Vacancy</dt><dd>${d.vacancy_pct.toFixed(1)}%</dd>
        <dt>US share</dt><dd>${pct(d.inventory_share)}</dd>
      </dl>
      ${d.power_note ? `<div class="note">${d.power_note}</div>` : ''}
    `;
    tt.classList.add('show');
    this.moveTip(event);
  },

  moveTip(event) {
    // Tooltip is position:fixed, appended to <body>, so we use viewport
    // coords (event.clientX/Y) and clamp against window dimensions —
    // never against the map host (whose overflow:hidden would clip).
    const tt = this.tooltipEl;
    const w = tt.offsetWidth  || 320;
    const h = tt.offsetHeight || 220;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const margin = 12;
    let px = event.clientX + 14;
    let py = event.clientY + 14;
    if (px + w + margin > vw) px = event.clientX - 14 - w;
    if (py + h + margin > vh) py = event.clientY - 14 - h;
    px = Math.max(margin, Math.min(px, vw - w - margin));
    py = Math.max(margin, Math.min(py, vh - h - margin));
    tt.style.left = `${px}px`;
    tt.style.top  = `${py}px`;
  },

  hideTip() {
    this.tooltipEl.classList.remove('show');
  },
};
