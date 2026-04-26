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
  activeTier: 'all',
  activeMetric: 'spec_ratio',
  activeStatus: 'all',
  activeFunding: 'all',
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

  async loadData() {
    try {
      const [m, s, f] = await Promise.all([
        fetch('/api/data-centers/markets').then(r => r.json()),
        fetch('/api/data-centers/summary').then(r => r.json()),
        fetch('/api/data-centers/facilities').then(r => r.json()),
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

  bindToolbar() {
    document.querySelectorAll('.dc-tab[data-mode]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.dc-tab[data-mode]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.setMode(btn.dataset.mode);
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
    const sel = document.getElementById('funding-filter');
    if (sel) {
      sel.addEventListener('change', () => {
        this.activeFunding = sel.value;
        this.renderFacilities();
      });
    }
  },

  setMode(mode) {
    this.mode = mode;
    const isFacilities = mode === 'facilities';
    document.getElementById('markets-toolbar').style.display = isFacilities ? 'none' : '';
    document.getElementById('facilities-toolbar').style.display = isFacilities ? '' : 'none';
    document.getElementById('legend-markets').style.display = isFacilities ? 'none' : '';
    document.getElementById('legend-facilities').style.display = isFacilities ? '' : 'none';
    document.getElementById('mode-help').textContent = isFacilities
      ? 'Marker color = funding source · shape = build status · radius ∝ √MW'
      : 'Bubble area ∝ pipeline MW · color = selected metric';
    this.bubbleLayer.style('display', isFacilities ? 'none' : '');
    this.facilityLayer.style('display', isFacilities ? '' : 'none');
    if (isFacilities) this.renderFacilities();
    else this.renderBubbles();
  },

  // ─── Render ────────────────────────────────────────────────────────────
  render() {
    this.renderKPIs();
    this.renderTierTable();
    this.renderTopTables();
    this.renderFundingTable();
    this.renderDeveloperTable();
    this.populateFundingFilter();
    this.renderBubbles();
    this.renderFacilities();
  },

  populateFundingFilter() {
    const sel = document.getElementById('funding-filter');
    if (!sel) return;
    const types = this.fundingTypes || {};
    const opts = ['<option value="all">All funding sources</option>']
      .concat(Object.entries(types).map(([k, v]) => `<option value="${k}">${v}</option>`));
    sel.innerHTML = opts.join('');
  },

  renderFundingTable() {
    const tbody = document.querySelector('#funding-table tbody');
    const rows = this.summary?.by_funding || [];
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="4" class="loading">no data</td></tr>'; return; }
    tbody.innerHTML = rows.map(r => `
      <tr data-funding="${r.funding_type}">
        <td>
          <span class="swatch" style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${FUNDING_COLOR[r.funding_type] || '#9ca3af'};margin-right:6px;vertical-align:middle;"></span>
          ${r.label}
        </td>
        <td class="num">${r.count}</td>
        <td class="num">${Math.round(r.mw).toLocaleString()}</td>
        <td class="num">${(r.share * 100).toFixed(1)}%</td>
      </tr>`).join('');
    tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        document.querySelectorAll('.dc-tab[data-mode]').forEach(b => b.classList.toggle('active', b.dataset.mode === 'facilities'));
        this.setMode('facilities');
        this.activeFunding = tr.dataset.funding;
        const sel = document.getElementById('funding-filter');
        if (sel) sel.value = this.activeFunding;
        this.renderFacilities();
      });
    });
  },

  renderDeveloperTable() {
    const tbody = document.querySelector('#developer-table tbody');
    const rows = this.summary?.top_developers || [];
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="4" class="loading">no data</td></tr>'; return; }
    tbody.innerHTML = rows.map(r => `
      <tr>
        <td>${r.developer}</td>
        <td>
          <span class="swatch" style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${FUNDING_COLOR[r.funding_type] || '#9ca3af'};vertical-align:middle;"></span>
        </td>
        <td class="num">${Math.round(r.mw).toLocaleString()}</td>
        <td class="num">${(r.share * 100).toFixed(1)}%</td>
      </tr>`).join('');
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

  renderFacilities() {
    if (!this.facilities.length) return;
    const radius = this.facilityRadius();

    const visible = this.facilities.filter(f => {
      if (this.activeStatus !== 'all' && f.status !== this.activeStatus) return false;
      if (this.activeFunding !== 'all' && f.funding_type !== this.activeFunding) return false;
      return true;
    });

    const join = this.facilityLayer.selectAll('circle.dc-fac')
      .data(this.facilities, d => d.name);

    const enter = join.enter().append('circle').attr('class', 'dc-fac');
    const merged = enter.merge(join);

    merged
      .attr('cx', d => { const p = this.projection([d.lon, d.lat]); return p ? p[0] : -9999; })
      .attr('cy', d => { const p = this.projection([d.lon, d.lat]); return p ? p[1] : -9999; })
      .attr('r', d => radius(d.mw))
      .attr('fill', d => d.status === 'planned' ? '#fff' : (FUNDING_COLOR[d.funding_type] || '#9ca3af'))
      .attr('fill-opacity', d => d.status === 'planned' ? 0 : (d.status === 'under_construction' ? 0.55 : 0.85))
      .attr('stroke', d => FUNDING_COLOR[d.funding_type] || '#374151')
      .attr('stroke-width', d => d.status === 'planned' ? 1.6 : (d.status === 'under_construction' ? 1.8 : 0.8))
      .attr('stroke-dasharray', d => d.status === 'under_construction' ? '3 2' : null)
      .style('opacity', d => visible.includes(d) ? 1 : 0.08)
      .style('cursor', 'pointer')
      .on('mouseover', (e, d) => this.showFacilityTip(e, d))
      .on('mousemove', (e) => this.moveTip(e))
      .on('mouseout', () => this.hideTip());
  },

  showFacilityTip(event, d) {
    const fundingLabel = (this.fundingTypes && this.fundingTypes[d.funding_type]) || d.funding_type;
    const statusLabel = { built: 'Built', under_construction: 'Under construction', planned: 'Planned' }[d.status] || d.status;
    const target = d.target_online ? ` · target online ${d.target_online}` : '';
    const tt = this.tooltipEl;
    tt.innerHTML = `
      <div><span class="ttl">${d.name}</span></div>
      <dl>
        <dt>Status</dt><dd>${statusLabel}${target}</dd>
        <dt>Capacity</dt><dd>${Math.round(d.mw).toLocaleString()} MW</dd>
        <dt>Operator</dt><dd>${d.operator || '—'}</dd>
        <dt>Developer</dt><dd>${d.developer || '—'}</dd>
        <dt>Funding</dt><dd><span style="color:${FUNDING_COLOR[d.funding_type] || '#fff'};font-weight:600;">${fundingLabel}</span></dd>
        <dt></dt><dd style="color:#cbd5e1;">${d.funding_detail || ''}</dd>
        ${d.tenant ? `<dt>Tenant</dt><dd>${d.tenant}</dd>` : ''}
        <dt>Market</dt><dd>${d.market}</dd>
      </dl>
      ${d.notes ? `<div class="note">${d.notes}</div>` : ''}
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
    const host = document.getElementById('dc-map-host');
    const r = host.getBoundingClientRect();
    const x = event.clientX - r.left;
    const y = event.clientY - r.top;
    const tt = this.tooltipEl;
    const w = tt.offsetWidth || 220;
    const h = tt.offsetHeight || 140;
    const px = (x + 14 + w > r.width) ? x - 14 - w : x + 14;
    const py = (y + 14 + h > r.height) ? y - 14 - h : y + 14;
    tt.style.left = `${px}px`;
    tt.style.top = `${py}px`;
  },

  hideTip() {
    this.tooltipEl.classList.remove('show');
  },
};
