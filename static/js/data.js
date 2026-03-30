/**
 * Data Page — Main application.
 * Phases 1-5: catalog-driven sidebar, pill tabs, search, mobile, WEO.
 */

(function () {
    'use strict';

    const PD = window.ParraData;
    const state = PD.state;

    const COLORS = [
        '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#ec4899',
        '#8b5cf6', '#f97316', '#06b6d4', '#84cc16', '#e11d48',
        '#6366f1', '#14b8a6', '#f43f5e', '#a855f7', '#22c55e',
        '#eab308', '#0ea5e9', '#d946ef', '#64748b', '#fb923c',
    ];

    let searchIndex = null;

    // ══════════════════════════════════════════════════════
    // BOOTSTRAP
    // ══════════════════════════════════════════════════════

    document.addEventListener('DOMContentLoaded', () => {
        // Build search index
        searchIndex = PD.buildSearchIndex();

        // Parse URL to set initial state
        PD.parseUrl();

        // Render the sidebar
        renderSidebar();

        // Render breadcrumb
        renderBreadcrumb();

        // Setup search overlay
        setupSearchOverlay();

        // Setup mobile bottom sheet
        setupBottomSheet();

        // Setup mobile nav bar
        setupMobileNavBar();

        // Handle browser back/forward
        window.addEventListener('popstate', () => {
            PD.parseUrl();
            renderSidebar();
            renderBreadcrumb();
            loadAndRender();
        });

        // Navigation callback
        PD.onNavigate = () => {
            renderSidebar();
            renderBreadcrumb();
            loadAndRender();
        };

        // Initial load
        loadAndRender();
    });

    // ══════════════════════════════════════════════════════
    // SIDEBAR RENDERING (Phase 2)
    // ══════════════════════════════════════════════════════

    function renderSidebar() {
        const nav = document.getElementById('sidebar-nav');
        if (!nav) return;

        let html = '';
        for (const cat of PD.CATALOG.categories) {
            // Only show categories that have datasets
            if (!cat.datasets || cat.datasets.length === 0) continue;

            html += '<div class="sidebar-category-heading">' + cat.icon + ' ' + cat.label + '</div>';
            for (const ds of cat.datasets) {
                const isActive = state.category === cat.id && state.dataset === ds.id;
                html += '<button class="sidebar-item' + (isActive ? ' active' : '') +
                    '" data-cat="' + cat.id + '" data-ds="' + ds.id + '">' +
                    ds.label + '</button>';
            }
        }
        nav.innerHTML = html;

        // Bind click handlers
        nav.querySelectorAll('.sidebar-item').forEach(btn => {
            btn.addEventListener('click', () => {
                PD.navigate(btn.dataset.cat, btn.dataset.ds, null);
            });
        });
    }

    // ══════════════════════════════════════════════════════
    // BREADCRUMB (Phase 2 + Phase 4 mobile)
    // ══════════════════════════════════════════════════════

    function renderBreadcrumb() {
        const bc = document.getElementById('data-breadcrumb');
        if (!bc) return;

        const cat = PD.findCategory(state.category);
        const ds = PD.findDataset(state.category, state.dataset);

        let html = '<span class="bc-segment bc-root" data-level="root">Data</span>';

        if (cat) {
            html += '<span class="bc-sep">\u203a</span>';
            html += '<span class="bc-segment" data-level="category" data-id="' + cat.id + '">' + cat.label + '</span>';
        }
        if (ds) {
            html += '<span class="bc-sep">\u203a</span>';
            html += '<span class="bc-segment" data-level="dataset" data-id="' + ds.id + '">' + ds.label + '</span>';
        }
        if (state.subview && state.subview !== 'overview' && ds && ds.subviews) {
            const sv = ds.subviews.find(s => s.id === state.subview);
            if (sv) {
                html += '<span class="bc-sep">\u203a</span>';
                html += '<span class="bc-segment bc-active">' + sv.label + '</span>';
            }
        }

        bc.innerHTML = html;

        // Mobile: breadcrumb taps open bottom sheet
        bc.querySelectorAll('.bc-segment').forEach(seg => {
            seg.addEventListener('click', () => {
                if (window.innerWidth > 768) return;
                const level = seg.dataset.level;
                openBottomSheet(level, seg.dataset.id);
            });
        });
    }

    // ══════════════════════════════════════════════════════
    // TAB STRIP (Phase 2)
    // ══════════════════════════════════════════════════════

    function renderTabStrip() {
        const container = document.getElementById('dataset-tabs');
        if (!container) return;

        const ds = PD.findDataset(state.category, state.dataset);
        if (!ds || !ds.subviews || ds.subviews.length <= 1) {
            container.innerHTML = '';
            container.style.display = 'none';
            return;
        }

        container.style.display = '';
        const activeSubview = state.subview || 'overview';
        let html = '';
        for (const sv of ds.subviews) {
            const isActive = sv.id === activeSubview;
            html += '<button class="dataset-tab' + (isActive ? ' active' : '') +
                '" data-sv="' + sv.id + '">' + sv.label + '</button>';
        }
        container.innerHTML = html;

        container.querySelectorAll('.dataset-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                PD.navigate(state.category, state.dataset, tab.dataset.sv);
            });
        });
    }

    // ══════════════════════════════════════════════════════
    // DYNAMIC PANEL RENDERING (Phase 2)
    // ══════════════════════════════════════════════════════

    function renderPanel() {
        const panel = document.getElementById('active-panel');
        if (!panel) return;

        // Reset country picker binding flag — the old <select> DOM node
        // is destroyed when we rebuild the panel, so the new one needs
        // a fresh event listener.
        _countryPickerBound = false;

        const ds = PD.findDataset(state.category, state.dataset);
        if (!ds) {
            panel.innerHTML = '<p style="color:var(--text-muted);padding:40px;">Select a dataset from the sidebar.</p>';
            return;
        }

        const subview = state.subview || 'overview';
        const isComponent = subview !== 'overview' && ds.subviews && ds.subviews.length > 1;
        const svLabel = isComponent ? (ds.subviews.find(s => s.id === subview) || {}).label || subview : '';
        const title = isComponent ? ds.label + ': ' + svLabel : ds.label;
        const exportUrl = isComponent && ds.componentExportUrl ? ds.componentExportUrl : ds.exportUrl;

        let controlsHtml = buildControlsHtml(ds);

        const exportBtn = exportUrl
            ? '<a href="' + exportUrl + '" class="export-btn-data" title="Download Excel">' +
              '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
              '<path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>' +
              '<polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>' +
              '</svg>Excel</a>'
            : '';

        panel.innerHTML = `
            <div class="data-section-header">
                <div>
                    <h1 class="data-title" id="panel-title">${title}</h1>
                    <p class="data-source">${ds.source} &mdash; ${ds.sourceDetail || ''}</p>
                </div>
                <div class="data-controls" id="panel-controls">
                    ${controlsHtml}
                    ${exportBtn}
                </div>
            </div>
            <div class="cpi-summary-cards" id="panel-summary"></div>
            <div class="chart-container" id="panel-chart-container">
                <canvas id="panel-chart"></canvas>
                <div class="chart-loading" id="panel-loading">
                    <div class="spinner"></div>
                    <p>Loading data...</p>
                </div>
            </div>
            <div class="data-table-container" id="panel-table-container">
                <table class="data-table" id="panel-table">
                    <thead id="panel-thead"></thead>
                    <tbody id="panel-tbody"></tbody>
                </table>
            </div>
            <div class="data-meta" id="panel-meta"></div>
            <div id="panel-history-section" style="display:none; margin-top: 32px;">
                <div class="data-section-header" style="margin-bottom: 0;">
                    <div>
                        <h2 class="data-title" style="font-size: 1.1rem;">Historical Quarterly Averages</h2>
                        <p class="data-source">yfinance &mdash; 10-Year Quarterly Average Prices</p>
                    </div>
                </div>
                <div class="chart-container" id="history-chart-container">
                    <canvas id="history-chart"></canvas>
                </div>
                <div class="data-table-container" id="history-table-container">
                    <table class="data-table" id="history-table">
                        <thead id="history-thead"></thead>
                        <tbody id="history-tbody"></tbody>
                    </table>
                </div>
            </div>
        `;

        bindControlListeners(ds);
    }

    function buildControlsHtml(ds) {
        let html = '';
        if (ds.controls.includes('freq')) {
            html += '<select id="ctrl-freq" class="data-select">' +
                '<option value="monthly"' + (state.freq === 'monthly' ? ' selected' : '') + '>Monthly</option>' +
                '<option value="quarterly"' + (state.freq === 'quarterly' ? ' selected' : '') + '>Quarterly</option>' +
                '<option value="yearly"' + (state.freq === 'yearly' ? ' selected' : '') + '>Yearly</option>' +
                '</select>';
        }
        if (ds.controls.includes('view')) {
            html += '<select id="ctrl-view" class="data-select">' +
                '<option value="yoy"' + (state.view === 'yoy' ? ' selected' : '') + '>YoY %</option>' +
                '<option value="qoq"' + (state.view === 'qoq' ? ' selected' : '') + '>QoQ %</option>' +
                '<option value="level"' + (state.view === 'level' ? ' selected' : '') + '>Level</option>' +
                '</select>';
        }
        if (ds.controls.includes('range')) {
            html += '<select id="ctrl-range" class="data-select">' +
                '<option value="5"' + (state.range === '5' ? ' selected' : '') + '>Last 5 Years</option>' +
                '<option value="10"' + (state.range === '10' ? ' selected' : '') + '>Last 10 Years</option>' +
                '<option value="all"' + (state.range === 'all' ? ' selected' : '') + '>All Available</option>' +
                '</select>';
        }
        if (ds.controls.includes('scenario')) {
            html += '<select id="ctrl-scenario" class="data-select">' +
                '<option value="Weighted Avg"' + (state.scenario === 'Weighted Avg' ? ' selected' : '') + '>Weighted Avg</option>' +
                '</select>';
        }
        if (ds.controls.includes('comm-freq')) {
            html += '<select id="ctrl-comm-freq" class="data-select">' +
                '<option value="quarterly"' + (state.commFreq === 'quarterly' ? ' selected' : '') + '>Quarterly</option>' +
                '<option value="yearly"' + (state.commFreq === 'yearly' ? ' selected' : '') + '>Yearly</option>' +
                '</select>';
        }
        if (ds.controls.includes('region')) {
            html += '<select id="ctrl-region" class="data-select">' +
                '<option value="World">Top 20</option>' +
                '</select>';
        }
        if (ds.controls.includes('reserve-type')) {
            html += '<select id="ctrl-reserve-type" class="data-select">' +
                '<option value="total"' + (state.reserveType === 'total' ? ' selected' : '') + '>Total Reserves</option>' +
                '<option value="fx"' + (state.reserveType === 'fx' ? ' selected' : '') + '>FX Reserves</option>' +
                '<option value="gold"' + (state.reserveType === 'gold' ? ' selected' : '') + '>Gold Reserves</option>' +
                '</select>';
        }
        if (ds.controls.includes('countries')) {
            html += '<select id="ctrl-countries" class="data-select" multiple>' +
                '</select>';
        }
        return html;
    }

    function bindControlListeners(ds) {
        function bind(id, setter) {
            const el = document.getElementById(id);
            if (el) el.addEventListener('change', (e) => { setter(e.target.value); PD.pushState(); renderCurrentDataset(); });
        }
        bind('ctrl-freq', v => { state.freq = v; });
        bind('ctrl-view', v => { state.view = v; });
        bind('ctrl-range', v => { state.range = v; });
        bind('ctrl-scenario', v => { state.scenario = v; });
        bind('ctrl-comm-freq', v => { state.commFreq = v; });
        bind('ctrl-region', v => { state.region = v; });
        bind('ctrl-reserve-type', v => { state.reserveType = v; });
    }

    // ══════════════════════════════════════════════════════
    // LOAD AND RENDER DISPATCHER
    // ══════════════════════════════════════════════════════

    function loadAndRender() {
        renderTabStrip();
        renderPanel();

        const ds = PD.findDataset(state.category, state.dataset);
        if (!ds) return;

        const apiUrl = ds.api;
        const cached = PD.getCached(apiUrl);

        // Common post-load setup: populate dynamic dropdowns + render
        function onDataReady() {
            hideLoading();
            renderCurrentDataset();
            if (ds.type === 'forecast-group') populateScenarioDropdown(ds);
            if (ds.type === 'cofer') populateRegionDropdown();
        }

        if (cached) {
            onDataReady();
        } else {
            fetchData(apiUrl, onDataReady);
        }

        // CPI component data — let renderCpi() handle fetching internally
        // to avoid double-fetch race conditions. renderCpi() already calls
        // fetchData(componentApi) if the component data isn't cached.
    }

    function hideLoading() {
        const el = document.getElementById('panel-loading');
        if (el) el.style.display = 'none';
    }

    async function fetchData(url, callback) {
        // Deduplicate in-flight requests — if already fetching this URL,
        // queue the callback to fire when the existing request completes.
        if (PD._fetching[url]) {
            PD._fetching[url].push(callback);
            return;
        }
        PD._fetching[url] = [callback];

        try {
            const resp = await fetch(url);
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            const data = await resp.json();
            PD.setCached(url, data);
            // Fire all queued callbacks
            const callbacks = PD._fetching[url] || [];
            delete PD._fetching[url];
            callbacks.forEach(cb => { if (cb) cb(); });
        } catch (err) {
            console.error('Fetch failed for ' + url + ':', err);
            delete PD._fetching[url];
            const el = document.getElementById('panel-loading');
            if (el) {
                el.innerHTML = '<p style="color:var(--text-muted)">Failed to load data.</p>' +
                    '<button onclick="location.reload()" style="margin-top:8px;padding:6px 16px;' +
                    'border-radius:6px;border:1px solid var(--border);background:var(--bg-tertiary);' +
                    'color:var(--text-muted);cursor:pointer;font-size:12px;">Retry</button>';
                el.style.display = '';
            }
        }
    }

    function renderCurrentDataset() {
        const ds = PD.findDataset(state.category, state.dataset);
        if (!ds) return;

        // Guard: don't render if primary data isn't cached yet (still loading)
        if (!PD.getCached(ds.api)) return;

        switch (ds.type) {
            case 'cofer': renderCofer(ds); break;
            case 'cpi': renderCpi(ds); break;
            case 'forecast-group': renderForecast(ds); break;
            case 'weo': renderWeo(ds); break;
            case 'wb': renderWeo(ds); break;  // World Bank uses same data shape as WEO
            case 'sovereign-debt': renderSovereignDebt(ds); break;
        }
    }

    // ══════════════════════════════════════════════════════
    // DATA AGGREGATION UTILITIES (unchanged from original)
    // ══════════════════════════════════════════════════════

    function toQuarterly(points) {
        const buckets = {};
        for (const p of points) {
            const q = Math.ceil(p.month / 3);
            const key = p.year + '-Q' + q;
            if (!buckets[key]) buckets[key] = { year: p.year, quarter: q, values: [], date: key };
            buckets[key].values.push(p.value);
        }
        return Object.values(buckets)
            .map(b => ({ year: b.year, quarter: b.quarter, value: b.values.reduce((a, c) => a + c, 0) / b.values.length, date: b.date }))
            .sort((a, b) => a.year - b.year || a.quarter - b.quarter);
    }

    function toYearly(points) {
        const buckets = {};
        for (const p of points) {
            if (!buckets[p.year]) buckets[p.year] = { year: p.year, values: [], date: String(p.year) };
            buckets[p.year].values.push(p.value);
        }
        return Object.values(buckets)
            .map(b => ({ year: b.year, value: b.values.reduce((a, c) => a + c, 0) / b.values.length, date: b.date }))
            .sort((a, b) => a.year - b.year);
    }

    function computeYoY(points, freq) {
        const result = [];
        for (let i = 0; i < points.length; i++) {
            const pt = points[i];
            let prev = null;
            if (freq === 'monthly') prev = points.find(p => p.year === pt.year - 1 && p.month === pt.month);
            else if (freq === 'quarterly') prev = points.find(p => p.year === pt.year - 1 && p.quarter === pt.quarter);
            else prev = points.find(p => p.year === pt.year - 1);
            let yoy = null;
            if (prev && prev.value !== 0) yoy = ((pt.value - prev.value) / Math.abs(prev.value)) * 100;
            result.push({ ...pt, yoy });
        }
        return result;
    }

    function computePoP(points) {
        const result = [];
        for (let i = 0; i < points.length; i++) {
            let pop = null;
            if (i > 0 && points[i - 1].value !== 0) pop = ((points[i].value - points[i - 1].value) / Math.abs(points[i - 1].value)) * 100;
            result.push({ ...points[i], pop });
        }
        return result;
    }

    function transformSeries(rawPoints, freq, view, isUs) {
        let points;
        if (freq === 'quarterly') points = toQuarterly(rawPoints);
        else if (freq === 'yearly') points = toYearly(rawPoints);
        else points = rawPoints.map(p => ({ ...p }));

        if (isUs) {
            if (view === 'level') return points.map(p => ({ date: p.date, y: round2(p.value), year: p.year }));
            else if (view === 'yoy') { const w = computeYoY(points, freq); return w.map(p => ({ date: p.date, y: p.yoy != null ? round2(p.yoy) : null, year: p.year })); }
            else { const w = computePoP(points); return w.map(p => ({ date: p.date, y: p.pop != null ? round2(p.pop) : null, year: p.year })); }
        } else {
            if (view === 'level' || view === 'yoy') return points.map(p => ({ date: p.date, y: round2(p.value), year: p.year }));
            else { const w = computePoP(points); return w.map(p => ({ date: p.date, y: p.pop != null ? round2(p.pop) : null, year: p.year })); }
        }
    }

    function round2(v) { return v != null ? Math.round(v * 100) / 100 : null; }

    function getYAxisLabel(view, isUs) { return (isUs && view === 'level') ? 'Index' : '%'; }
    function getTooltipSuffix(view, isUs) {
        if (isUs && view === 'level') return '';
        if (view === 'qoq') return ' pp';
        return '%';
    }

    function prepareChartData(rawPoints, rangeVal, freq, view, isUs) {
        const currentYear = new Date().getFullYear();
        let filtered = rawPoints;
        if (rangeVal !== 'all') {
            const computeMinYear = currentYear - parseInt(rangeVal) - 1;
            filtered = rawPoints.filter(p => p.year >= computeMinYear);
        }
        let transformed = transformSeries(filtered, freq, view, isUs);
        if (rangeVal !== 'all') {
            const minYear = currentYear - parseInt(rangeVal);
            transformed = transformed.filter(p => p.year >= minYear);
        }
        return transformed;
    }

    // ══════════════════════════════════════════════════════
    // COFER RENDERER
    // ══════════════════════════════════════════════════════

    function populateRegionDropdown() {
        const data = PD.getCached('/api/cofer');
        if (!data) return;
        const sel = document.getElementById('ctrl-region');
        if (!sel) return;
        const regions = data.regions || [];
        regions.forEach(r => {
            if (r === 'World') return;
            const opt = document.createElement('option');
            opt.value = r;
            opt.textContent = r;
            if (r === state.region) opt.selected = true;
            sel.appendChild(opt);
        });
    }

    function renderCofer(ds) {
        const data = PD.getCached(ds.api);
        if (!data) return;

        const years = data.years || [];
        let countries = data.countries || [];
        const regionMembers = data.region_members || {};

        if (state.region === 'World') countries = countries.slice(0, 20);
        else {
            const members = regionMembers[state.region] || [];
            countries = countries.filter(c => members.includes(c.iso3));
        }

        // Chart
        PD.destroyChart('main');
        const canvasEl = document.getElementById('panel-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const datasets = countries.map((c, i) => {
            let values;
            if (state.reserveType === 'fx') values = c.fx_reserves;
            else if (state.reserveType === 'gold') values = c.gold_reserves;
            else values = c.total_reserves;
            return {
                label: c.name,
                data: values.map(v => v != null ? v : null),
                borderColor: COLORS[i % COLORS.length],
                backgroundColor: 'transparent',
                borderWidth: 2, fill: false, pointRadius: 0, pointHitRadius: 8, tension: 0.3,
            };
        });

        PD.setChart('main', new Chart(ctx, {
            type: 'line',
            data: { labels: years, datasets },
            options: chartOptions({
                legend: true,
                tooltip: (ctx) => {
                    const val = ctx.parsed.y;
                    if (val == null) return ctx.dataset.label + ': N/A';
                    return ctx.dataset.label + ': $' + val.toFixed(1) + 'B';
                },
                yCallback: (val) => '$' + val.toLocaleString() + 'B',
                beginAtZero: true,
            }),
        }));

        // Table
        const thead = document.getElementById('panel-thead');
        const tbody = document.getElementById('panel-tbody');
        if (thead && tbody) {
            let hdr = '<tr><th>Country</th>';
            for (let i = years.length - 1; i >= 0; i--) hdr += '<th>' + years[i] + '</th>';
            hdr += '</tr>';
            thead.innerHTML = hdr;

            let rows = '';
            for (const c of countries) {
                let values;
                if (state.reserveType === 'fx') values = c.fx_reserves;
                else if (state.reserveType === 'gold') values = c.gold_reserves;
                else values = c.total_reserves;
                rows += '<tr><td>' + c.name + '</td>';
                for (let i = years.length - 1; i >= 0; i--) {
                    const v = values[i];
                    rows += v == null ? '<td>\u2014</td>' : '<td>$' + v.toFixed(1) + 'B</td>';
                }
                rows += '</tr>';
            }
            tbody.innerHTML = rows;
        }

        // Meta
        const metaEl = document.getElementById('panel-meta');
        if (metaEl) {
            const meta = data.meta || {};
            const parts = [];
            if (meta.source) parts.push(meta.source);
            if (meta.frequency) parts.push(meta.frequency);
            if (meta.year_range) parts.push(meta.year_range);
            parts.push(countries.length + ' countries shown');
            if (meta.country_count) parts.push(meta.country_count + ' total');
            metaEl.textContent = parts.join(' \u00b7 ');
        }

        // Summary not needed for COFER
        const summary = document.getElementById('panel-summary');
        if (summary) summary.innerHTML = '';
    }

    // ══════════════════════════════════════════════════════
    // CPI RENDERER
    // ══════════════════════════════════════════════════════

    function renderCpi(ds) {
        const subview = state.subview || 'overview';
        const isComponent = subview !== 'overview';

        if (isComponent) {
            // Need component data
            const compData = PD.getCached(ds.componentApi);
            if (!compData) {
                fetchData(ds.componentApi, () => renderCpi(ds));
                return;
            }
            renderCpiComponent(ds, compData, subview);
        } else {
            const data = PD.getCached(ds.api);
            if (!data) return;
            renderCpiOverview(ds, data);
        }
    }

    function renderCpiOverview(ds, data) {
        const isUs = ds.isUs;
        const series = data.series || {};
        const categories = data.categories || {};
        const colors = data.colors || {};
        const suffix = getTooltipSuffix(state.view, isUs);
        const yLabel = getYAxisLabel(state.view, isUs);

        // Summary cards
        renderCpiSummary(data, isUs);

        // Chart
        PD.destroyChart('main');
        const canvasEl = document.getElementById('panel-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const datasets = Object.entries(categories).map(([key, label]) => {
            const rawPoints = series[key] || [];
            const transformed = prepareChartData(rawPoints, state.range, state.freq, state.view, isUs);
            return {
                label: label,
                data: transformed.filter(d => d.y !== null && d.y !== undefined).map(d => ({ x: d.date, y: d.y })),
                borderColor: colors[key] || COLORS[0],
                backgroundColor: 'transparent',
                borderWidth: 2, fill: false, pointRadius: 0, pointHitRadius: 8, tension: 0.3,
            };
        });

        PD.setChart('main', new Chart(ctx, {
            type: 'line',
            data: { datasets },
            options: chartOptions({
                legend: true,
                xType: 'category',
                tooltip: (ctx) => {
                    const val = ctx.parsed.y;
                    if (val == null) return ctx.dataset.label + ': N/A';
                    return ctx.dataset.label + ': ' + val.toFixed(2) + suffix;
                },
                yCallback: (val) => yLabel === 'Index' ? val.toFixed(1) : val.toFixed(1) + '%',
            }),
        }));

        // Table
        const thead = document.getElementById('panel-thead');
        const tbody = document.getElementById('panel-tbody');
        if (thead && tbody) {
            const transformed = {};
            Object.keys(categories).forEach(key => {
                transformed[key] = prepareChartData(series[key] || [], state.range, state.freq, state.view, isUs);
            });
            const firstKey = Object.keys(categories)[0];
            const dates = (transformed[firstKey] || []).map(p => p.date);
            const reversedDates = dates.slice().reverse();
            const isIndex = isUs && state.view === 'level';

            let hdr = '<tr><th>Date</th>';
            Object.values(categories).forEach(label => { hdr += '<th>' + label + '</th>'; });
            hdr += '</tr>';
            thead.innerHTML = hdr;

            const lookups = {};
            Object.keys(categories).forEach(key => {
                lookups[key] = {};
                (transformed[key] || []).forEach(p => { lookups[key][p.date] = p.y; });
            });

            let rows = '';
            for (const date of reversedDates) {
                rows += '<tr><td>' + date + '</td>';
                Object.keys(categories).forEach(key => {
                    const val = lookups[key][date];
                    if (val == null) rows += '<td>--</td>';
                    else if (isIndex) rows += '<td>' + val.toFixed(1) + '</td>';
                    else rows += '<td>' + val.toFixed(2) + suffix + '</td>';
                });
                rows += '</tr>';
            }
            tbody.innerHTML = rows;
        }

        // Meta
        renderCpiMeta(data, ds.isUs);
    }

    function renderCpiComponent(ds, data, key) {
        const isUs = ds.isUs;
        const series = data.series || {};
        const categories = data.categories || {};
        const colors = data.colors || {};
        const label = categories[key] || key;
        const suffix = getTooltipSuffix(state.view, isUs);
        const yLabel = getYAxisLabel(state.view, isUs);
        const color = colors[key] || COLORS[0];
        const isIndex = isUs && state.view === 'level';

        // Update title
        const titleEl = document.getElementById('panel-title');
        if (titleEl) titleEl.textContent = ds.label + ': ' + label;

        // Summary
        renderCompSummary(data, key, isUs);

        // Chart
        PD.destroyChart('main');
        const canvasEl = document.getElementById('panel-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const rawPoints = series[key] || [];
        const transformed = prepareChartData(rawPoints, state.range, state.freq, state.view, isUs);
        const chartData = transformed.filter(d => d.y !== null && d.y !== undefined).map(d => ({ x: d.date, y: d.y }));

        PD.setChart('main', new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    label: label,
                    data: chartData,
                    borderColor: color,
                    backgroundColor: color + '1A',
                    borderWidth: 2, fill: true, pointRadius: 0, pointHitRadius: 8, tension: 0.3,
                }]
            },
            options: chartOptions({
                legend: false,
                xType: 'category',
                tooltip: (ctx) => {
                    const val = ctx.parsed.y;
                    if (val == null) return 'N/A';
                    return val.toFixed(2) + suffix;
                },
                yCallback: (val) => yLabel === 'Index' ? val.toFixed(1) : val.toFixed(1) + '%',
            }),
        }));

        // Table
        const thead = document.getElementById('panel-thead');
        const tbody = document.getElementById('panel-tbody');
        if (thead && tbody) {
            thead.innerHTML = '<tr><th>Date</th><th>' + label + '</th></tr>';
            const reversedPts = transformed.slice().reverse();
            let rows = '';
            for (const pt of reversedPts) {
                rows += '<tr><td>' + pt.date + '</td>';
                if (pt.y == null) rows += '<td>--</td>';
                else if (isIndex) rows += '<td>' + pt.y.toFixed(1) + '</td>';
                else rows += '<td>' + pt.y.toFixed(2) + suffix + '</td>';
                rows += '</tr>';
            }
            tbody.innerHTML = rows;
        }

        // Meta
        renderCpiMeta(data, isUs, label);
    }

    function renderCpiSummary(data, isUs) {
        const container = document.getElementById('panel-summary');
        if (!container) return;

        const series = data.series || {};
        const categories = data.categories || {};
        const colors = data.colors || {};
        const suffix = getTooltipSuffix(state.view, isUs);
        const isIndex = isUs && state.view === 'level';

        let html = '';
        for (const [key, label] of Object.entries(categories)) {
            const rawPoints = series[key] || [];
            const transformed = prepareChartData(rawPoints, state.range, state.freq, state.view, isUs);
            let latest = null, prev = null;
            for (let i = transformed.length - 1; i >= 0; i--) {
                if (transformed[i].y != null) {
                    if (!latest) latest = transformed[i];
                    else if (!prev) { prev = transformed[i]; break; }
                }
            }
            if (!latest) continue;
            const val = latest.y;
            const dir = prev ? (val > prev.y ? 'up' : val < prev.y ? 'down' : 'flat') : 'flat';
            const arrow = dir === 'up' ? '&#9650;' : dir === 'down' ? '&#9660;' : '';
            const dirClass = dir === 'up' ? 'summary-up' : dir === 'down' ? 'summary-down' : '';
            const formatted = isIndex ? val.toFixed(1) : val.toFixed(2) + suffix;
            const borderColor = colors[key] || '#3b82f6';
            html += '<div class="cpi-summary-card" style="border-top: 3px solid ' + borderColor + '">' +
                '<div class="cpi-summary-label">' + label + '</div>' +
                '<div class="cpi-summary-value ' + dirClass + '">' + formatted +
                '<span class="cpi-summary-arrow ' + dirClass + '">' + arrow + '</span></div>' +
                '<div class="cpi-summary-date">' + latest.date + '</div></div>';
        }
        container.innerHTML = html;
    }

    function renderCompSummary(data, key, isUs) {
        const container = document.getElementById('panel-summary');
        if (!container) return;

        const series = data.series || {};
        const categories = data.categories || {};
        const colors = data.colors || {};
        const suffix = getTooltipSuffix(state.view, isUs);
        const isIndex = isUs && state.view === 'level';
        const rawPoints = series[key] || [];
        const transformed = prepareChartData(rawPoints, state.range, state.freq, state.view, isUs);
        const label = categories[key] || key;
        const borderColor = colors[key] || '#3b82f6';

        let latest = null, prev = null;
        for (let i = transformed.length - 1; i >= 0; i--) {
            if (transformed[i].y != null) {
                if (!latest) latest = transformed[i];
                else if (!prev) { prev = transformed[i]; break; }
            }
        }
        if (!latest) { container.innerHTML = ''; return; }

        const val = latest.y;
        const dir = prev ? (val > prev.y ? 'up' : val < prev.y ? 'down' : 'flat') : 'flat';
        const arrow = dir === 'up' ? '&#9650;' : dir === 'down' ? '&#9660;' : '';
        const dirClass = dir === 'up' ? 'summary-up' : dir === 'down' ? 'summary-down' : '';
        const formatted = isIndex ? val.toFixed(1) : val.toFixed(2) + suffix;

        container.innerHTML = '<div class="cpi-summary-card" style="border-top: 3px solid ' + borderColor + '">' +
            '<div class="cpi-summary-label">' + label + '</div>' +
            '<div class="cpi-summary-value ' + dirClass + '">' + formatted +
            '<span class="cpi-summary-arrow ' + dirClass + '">' + arrow + '</span></div>' +
            '<div class="cpi-summary-date">' + latest.date + '</div></div>';
    }

    function renderCpiMeta(data, isUs, componentLabel) {
        const el = document.getElementById('panel-meta');
        if (!el) return;
        const meta = data.meta || {};
        const parts = [];
        if (meta.source) parts.push(meta.source);
        if (componentLabel) parts.push(componentLabel);
        const freqLabels = { monthly: 'Monthly', quarterly: 'Quarterly', yearly: 'Yearly' };
        parts.push(freqLabels[state.freq] || 'Monthly');
        const viewLabels = {
            yoy: 'Year-over-Year',
            qoq: state.freq === 'monthly' ? 'Month-over-Month' : state.freq === 'quarterly' ? 'Quarter-over-Quarter' : 'Year-over-Year',
            level: isUs ? 'Index Level' : 'Annual Rate',
        };
        parts.push(viewLabels[state.view] || '');
        if (meta.year_range) parts.push(meta.year_range);
        el.textContent = parts.join(' \u00b7 ');
    }

    // ══════════════════════════════════════════════════════
    // FORECAST RENDERER
    // ══════════════════════════════════════════════════════

    function populateScenarioDropdown(ds) {
        const data = PD.getCached(ds.api);
        if (!data) return;
        const sel = document.getElementById('ctrl-scenario');
        if (!sel) return;
        const group = (data.groups || {})[ds.forecastGroupName];
        if (!group) return;
        const order = group.scenario_order || [];
        sel.innerHTML = '';
        // "All Scenarios" option for overview mode
        const allOpt = document.createElement('option');
        allOpt.value = 'All'; allOpt.textContent = 'All Scenarios';
        if (state.scenario === 'All') allOpt.selected = true;
        sel.appendChild(allOpt);
        // Weighted Avg
        const waOpt = document.createElement('option');
        waOpt.value = 'Weighted Avg'; waOpt.textContent = 'Weighted Avg';
        if (state.scenario === 'Weighted Avg') waOpt.selected = true;
        sel.appendChild(waOpt);
        order.forEach(sc => {
            if (sc === 'Actual' || sc === 'Weighted Avg') return;
            const opt = document.createElement('option');
            opt.value = sc; opt.textContent = sc;
            if (state.scenario === sc) opt.selected = true;
            sel.appendChild(opt);
        });
    }

    function renderForecast(ds) {
        const data = PD.getCached(ds.api);
        if (!data) return;

        const subview = state.subview || 'overview';
        if (subview !== 'overview') {
            renderForecastCommodity(ds, data, subview);
        } else {
            renderForecastGroup(ds, data);
        }
    }

    function renderForecastGroup(ds, data) {
        const groupName = ds.forecastGroupName;
        const group = (data.groups || {})[groupName];
        if (!group) return;

        const scenario = state.scenario;
        const isAllScenarios = scenario === 'All';
        const commFreq = state.commFreq || 'quarterly';
        const commodities = group.commodities || {};
        const groupColors = group.colors || {};
        const scenarioColors = group.scenario_colors || {};
        const scenarioOrder = group.scenario_order || [];
        const timeCtx = data.time_context || {};
        const forecastLabels = timeCtx.labels || [];
        const labelTypes = timeCtx.label_types || [];

        // ── Build unified timeline: historical quarters + current year forecast ──
        const histLabelSet = new Set();
        Object.values(commodities).forEach(info => {
            (info.historical || []).forEach(h => histLabelSet.add(h.label));
        });
        const histLabels = Array.from(histLabelSet).sort((a, b) => {
            const [ya, qa] = a.split(' Q'); const [yb, qb] = b.split(' Q');
            return (parseInt(ya) * 10 + parseInt(qa)) - (parseInt(yb) * 10 + parseInt(qb));
        });

        const allQLabels = histLabels.concat(forecastLabels);
        const forecastStartIdx = histLabels.length;

        // Build per-scenario maps for each commodity: { commodityName: { scenario: { label: val } } }
        const scenariosToShow = isAllScenarios
            ? scenarioOrder.filter(sc => sc !== 'Actual')
            : [scenario];

        const commodityScenMaps = {};
        Object.entries(commodities).forEach(([name, info]) => {
            const scMaps = {};
            scenariosToShow.forEach(sc => {
                const valMap = {};
                // Historical data (same for all scenarios)
                (info.historical || []).forEach(h => { valMap[h.label] = h.avg_price; });
                // Forecast portion from this scenario
                const scenData = (info.scenarios || {})[sc] || {};
                forecastLabels.forEach(l => { if (scenData[l] != null) valMap[l] = scenData[l]; });
                scMaps[sc] = valMap;
            });
            commodityScenMaps[name] = scMaps;
        });

        // ── Helper: parse year from label ──
        function labelYear(l) {
            if (l.includes(' Q')) return parseInt(l.split(' Q')[0]);
            if (l.includes("'")) return 2000 + parseInt(l.split("'")[1]);
            return data.forecast_year || new Date().getFullYear();
        }

        // ── Aggregate to yearly if needed ──
        let chartLabels, chartForecastStartIdx, chartIsForecast;
        const chartDatasets = [];

        if (commFreq === 'yearly') {
            // Aggregate quarterly data into yearly averages
            const yearSet = new Set();
            allQLabels.forEach(l => yearSet.add(labelYear(l)));
            const sortedYears = Array.from(yearSet).sort();
            const forecastYear = data.forecast_year || new Date().getFullYear();

            chartLabels = sortedYears.map(String);
            chartForecastStartIdx = sortedYears.findIndex(y => y >= forecastYear);
            if (chartForecastStartIdx < 0) chartForecastStartIdx = chartLabels.length;
            chartIsForecast = (idx) => idx >= chartForecastStartIdx;

            const commNames = Object.keys(commodities);
            commNames.forEach((name, ci) => {
                scenariosToShow.forEach((sc, si) => {
                    const valMap = commodityScenMaps[name][sc];
                    const yearAggs = {};
                    allQLabels.forEach(l => {
                        const yr = labelYear(l);
                        const v = valMap[l];
                        if (v != null) {
                            if (!yearAggs[yr]) yearAggs[yr] = [];
                            yearAggs[yr].push(v);
                        }
                    });
                    const lineColor = isAllScenarios
                        ? (scenarioColors[sc] || COLORS[si % COLORS.length])
                        : (groupColors[name] || COLORS[ci % COLORS.length]);
                    const label = isAllScenarios ? name + ' (' + sc + ')' : name;
                    chartDatasets.push({
                        label: label,
                        data: sortedYears.map(yr => {
                            const vals = yearAggs[yr];
                            return vals && vals.length > 0 ? vals.reduce((s, v) => s + v, 0) / vals.length : null;
                        }),
                        borderColor: lineColor,
                        backgroundColor: 'transparent',
                        borderWidth: sc === 'Weighted Avg' ? 3 : 2,
                        borderDash: sc === 'Weighted Avg' ? [6, 3] : [],
                        fill: false, pointRadius: 3, pointHitRadius: 8,
                        pointBackgroundColor: lineColor, pointBorderColor: lineColor, tension: 0.3,
                        segment: chartForecastStartIdx > 0 ? {
                            borderDash: ctx2 => ctx2.p0DataIndex >= chartForecastStartIdx - 1 ? [4, 3] : [],
                        } : undefined,
                    });
                });
            });
        } else {
            // Quarterly mode — use allQLabels directly
            chartLabels = allQLabels;
            chartForecastStartIdx = forecastStartIdx;
            chartIsForecast = (idx) => idx >= forecastStartIdx;

            const commNames = Object.keys(commodities);
            commNames.forEach((name, ci) => {
                scenariosToShow.forEach((sc, si) => {
                    const valMap = commodityScenMaps[name][sc];
                    const lineColor = isAllScenarios
                        ? (scenarioColors[sc] || COLORS[si % COLORS.length])
                        : (groupColors[name] || COLORS[ci % COLORS.length]);
                    const label = isAllScenarios ? name + ' (' + sc + ')' : name;
                    chartDatasets.push({
                        label: label,
                        data: allQLabels.map(l => valMap[l] != null ? valMap[l] : null),
                        borderColor: lineColor,
                        backgroundColor: 'transparent',
                        borderWidth: sc === 'Weighted Avg' ? 3 : 2,
                        borderDash: sc === 'Weighted Avg' ? [6, 3] : [],
                        fill: false, pointRadius: 0, pointHitRadius: 8,
                        pointBackgroundColor: lineColor, pointBorderColor: lineColor, tension: 0.3,
                        segment: forecastStartIdx > 0 ? {
                            borderDash: ctx2 => ctx2.p0DataIndex >= forecastStartIdx - 1 ? [4, 3] : [],
                        } : undefined,
                    });
                });
            });
        }

        // Summary not used
        const summary = document.getElementById('panel-summary');
        if (summary) summary.innerHTML = '';

        // ── Chart ──
        PD.destroyChart('main');
        const canvasEl = document.getElementById('panel-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        PD.setChart('main', new Chart(ctx, {
            type: 'line',
            data: { labels: chartLabels, datasets: chartDatasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: { color: '#9ca3af', font: { size: 11 }, boxWidth: 12, padding: 10, usePointStyle: true, pointStyle: 'line' }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#fff',
                        bodyColor: '#d1d5db',
                        borderColor: 'rgba(255,255,255,0.1)',
                        borderWidth: 1,
                        bodyFont: { size: 12 },
                        callbacks: {
                            label: (tooltipCtx) => {
                                const val = tooltipCtx.parsed.y;
                                if (val == null) return tooltipCtx.dataset.label + ': N/A';
                                return tooltipCtx.dataset.label + ': ' + val.toFixed(2);
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'category',
                        ticks: {
                            color: (tickCtx) => {
                                if (chartIsForecast(tickCtx.index)) return '#6b7280';
                                return '#9ca3af';
                            },
                            font: { size: 11 },
                            maxRotation: 45,
                            autoSkip: true,
                            maxTicksLimit: commFreq === 'yearly' ? 15 : 20,
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' },
                    },
                    y: {
                        ticks: { color: '#6b7280', font: { size: 10 } },
                        grid: { color: 'rgba(55,65,81,0.3)' },
                    }
                }
            },
        }));

        // ── Table: always show yearly averages ──
        const thead = document.getElementById('panel-thead');
        const tbody = document.getElementById('panel-tbody');
        if (thead && tbody) {
            const yearMap = {};
            allQLabels.forEach(l => {
                const yr = labelYear(l);
                if (!yearMap[yr]) yearMap[yr] = {};
                Object.entries(commodities).forEach(([name]) => {
                    if (!yearMap[yr][name]) yearMap[yr][name] = [];
                    // Use the first scenario's data for table
                    const scMap = commodityScenMaps[name][scenariosToShow[0]];
                    const v = scMap[l];
                    if (v != null) yearMap[yr][name].push(v);
                });
            });

            const sortedYears = Object.keys(yearMap).map(Number).sort();
            const forecastYear = data.forecast_year || new Date().getFullYear();

            let hdr = '<tr><th>Commodity</th><th>Unit</th>';
            sortedYears.forEach(yr => {
                const cls = yr >= forecastYear ? ' class="col-forecast"' : '';
                hdr += '<th' + cls + '>' + yr + '</th>';
            });
            hdr += '</tr>';
            thead.innerHTML = hdr;

            let rows = '';
            Object.entries(commodities).forEach(([name, info]) => {
                rows += '<tr><td>' + name + '</td><td>' + (info.unit || '') + '</td>';
                sortedYears.forEach(yr => {
                    const vals = (yearMap[yr] || {})[name] || [];
                    const cls = yr >= forecastYear ? ' class="col-forecast"' : '';
                    if (vals.length > 0) {
                        const avg = vals.reduce((s, v) => s + v, 0) / vals.length;
                        rows += '<td' + cls + '>' + avg.toFixed(2) + '</td>';
                    } else {
                        rows += '<td' + cls + '>\u2014</td>';
                    }
                });
                rows += '</tr>';
            });
            tbody.innerHTML = rows;
        }

        // Meta / Footnote
        const metaEl = document.getElementById('panel-meta');
        if (metaEl) {
            const meta = data.meta || {};
            const weights = group.scenario_weights || {};
            const yearEndLabels = timeCtx.year_end_labels || [];

            // Line 1: Source · Scenario · Frequency · Horizon
            const line1 = [];
            if (meta.source) line1.push(meta.source);
            line1.push(isAllScenarios ? 'All Scenarios' : scenario);
            if (!isAllScenarios && weights[scenario]) line1.push('Weight: ' + (weights[scenario] * 100).toFixed(0) + '%');
            line1.push(commFreq === 'yearly' ? 'Yearly averages' : 'Quarterly');
            const fqCount = forecastLabels.filter((_, i) => labelTypes[i] === 'forecast').length;
            line1.push(fqCount + '-quarter rolling forecast');
            if (yearEndLabels.length > 1) line1.push(yearEndLabels.join(' & '));

            // Line 2: Methodology details
            const line2 = [];
            line2.push('Absolute price targets per scenario');
            line2.push('Current quarter from live YTD data (yfinance)');
            if (meta.last_updated) line2.push('Updated ' + meta.last_updated.split('T')[0]);

            metaEl.innerHTML = '<span>' + line1.join(' · ') + '</span><br><span style="opacity:0.7">' + line2.join(' · ') + '</span>';
        }

        // Hide history section (unified into main chart)
        const histSection = document.getElementById('panel-history-section');
        if (histSection) histSection.style.display = 'none';
    }

    function renderForecastCommodity(ds, data, commodityName) {
        const groupName = ds.forecastGroupName;
        const group = (data.groups || {})[groupName];
        if (!group) return;

        const info = (group.commodities || {})[commodityName];
        if (!info) return;

        const scenarios = info.scenarios || {};
        const scenarioColors = group.scenario_colors || {};
        const scenarioLabels = group.scenario_labels || {};
        const scenarioOrder = group.scenario_order || ['Actual', 'Base Case', 'Severe Case', 'Worst Case', 'Weighted Avg'];
        const timeCtx = data.time_context || {};
        const forecastLabels = timeCtx.labels || [];
        const labelTypes = timeCtx.label_types || [];
        const yearEndLabel = timeCtx.year_end_label || 'FY Avg';
        const yearEndLabels = timeCtx.year_end_labels || [yearEndLabel];
        const fyKeys = ['FY'].concat(yearEndLabels.slice(1).map((_, i) => 'FY' + (i + 2)));
        const forecastYear = data.forecast_year || new Date().getFullYear();

        // ── Build unified timeline: historical + current year scenarios ──
        const historical = info.historical || [];
        const histLabels = historical.map(h => h.label);
        const histMap = {};
        historical.forEach(h => { histMap[h.label] = h.avg_price; });

        const allLabels = histLabels.concat(forecastLabels);
        const forecastStartIdx = histLabels.length;

        // Update title
        const titleEl = document.getElementById('panel-title');
        if (titleEl) titleEl.textContent = commodityName + ' \u2014 Scenario Forecast';

        // Summary not used
        const summary = document.getElementById('panel-summary');
        if (summary) summary.innerHTML = '';

        // ── Chart ──
        PD.destroyChart('main');
        const canvasEl = document.getElementById('panel-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const baseScenario = scenarioOrder.find(sc => sc === 'Base Case' || sc === 'Base') || scenarioOrder[1];

        // Build one "History" line + scenario fan lines
        const datasets = [];

        // Historical line (single line covering the historical portion)
        if (histLabels.length > 0) {
            datasets.push({
                label: 'Historical',
                data: allLabels.map((l, i) => i < forecastStartIdx ? (histMap[l] != null ? histMap[l] : null) : null),
                borderColor: '#94a3b8',
                backgroundColor: '#94a3b81A',
                borderWidth: 2, fill: true, pointRadius: 0, pointHitRadius: 8,
                pointBackgroundColor: '#94a3b8', pointBorderColor: '#94a3b8', tension: 0.3,
            });
        }

        // Scenario lines (covering the forecast portion, connecting from last historical point)
        scenarioOrder
            .filter(sc => scenarios[sc] && sc !== 'Actual')
            .forEach(sc => {
                const scenData = scenarios[sc];
                const color = scenarioColors[sc] || '#9ca3af';
                const isDashed = sc === 'Weighted Avg';
                const isBase = sc === baseScenario;

                // Build data array: null for historical, then scenario values
                // But connect to the last historical point for continuity
                const lineData = allLabels.map((l, i) => {
                    if (i < forecastStartIdx) {
                        // Show the last historical point as the starting anchor
                        if (i === forecastStartIdx - 1 && histMap[l] != null) return histMap[l];
                        return null;
                    }
                    return scenData[forecastLabels[i - forecastStartIdx]] != null ? scenData[forecastLabels[i - forecastStartIdx]] : null;
                });

                datasets.push({
                    label: sc,
                    data: lineData,
                    borderColor: color,
                    backgroundColor: isBase ? color + '1A' : 'transparent',
                    borderWidth: sc === 'Weighted Avg' ? 3 : 2,
                    borderDash: isDashed ? [6, 3] : [],
                    fill: false, pointRadius: 4, pointHitRadius: 8,
                    pointBackgroundColor: color, pointBorderColor: color, tension: 0.3,
                    spanGaps: true,
                });
            });

        const unit = info.unit || '';

        PD.setChart('main', new Chart(ctx, {
            type: 'line',
            data: { labels: allLabels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: { color: '#9ca3af', font: { size: 11 }, boxWidth: 12, padding: 10, usePointStyle: true, pointStyle: 'line' }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#fff',
                        bodyColor: '#d1d5db',
                        borderColor: 'rgba(255,255,255,0.1)',
                        borderWidth: 1,
                        bodyFont: { size: 12 },
                        callbacks: {
                            label: (tooltipCtx) => {
                                const val = tooltipCtx.parsed.y;
                                if (val == null) return tooltipCtx.dataset.label + ': N/A';
                                return tooltipCtx.dataset.label + ': ' + val.toFixed(2) + ' ' + unit;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'category',
                        ticks: {
                            color: (tickCtx) => {
                                if (tickCtx.index >= forecastStartIdx) {
                                    const fIdx = tickCtx.index - forecastStartIdx;
                                    if (labelTypes[fIdx] === 'current_q') return '#f59e0b';
                                    if (labelTypes[fIdx] === 'forecast') return '#6b7280';
                                }
                                return '#9ca3af';
                            },
                            font: { size: 11 },
                            maxRotation: 45,
                            autoSkip: true,
                            maxTicksLimit: 20,
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' },
                    },
                    y: {
                        ticks: { color: '#6b7280', font: { size: 10 } },
                        grid: { color: 'rgba(55,65,81,0.3)' },
                    }
                }
            },
        }));

        // ── Table: scenarios × forecast labels (keep existing format) ──
        const thead = document.getElementById('panel-thead');
        const tbody = document.getElementById('panel-tbody');
        if (thead && tbody) {
            let hdr = '<tr><th>Scenario</th>';
            forecastLabels.forEach((l, i) => {
                const cls = labelTypes[i] === 'forecast' ? ' class="col-forecast"' : labelTypes[i] === 'current_q' ? ' class="col-current"' : ' class="col-actual"';
                hdr += '<th' + cls + '>' + l + '</th>';
            });
            yearEndLabels.forEach(lbl => { hdr += '<th>' + lbl + '</th>'; });
            hdr += '</tr>';
            thead.innerHTML = hdr;

            let rows = '';
            scenarioOrder.forEach(sc => {
                const scenData = scenarios[sc];
                if (!scenData) return;
                const color = scenarioColors[sc] || '#9ca3af';
                rows += '<tr><td><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + color + ';margin-right:6px;"></span>' + sc + '</td>';
                forecastLabels.forEach((l, i) => {
                    const v = scenData[l];
                    const cls = labelTypes[i] === 'forecast' ? ' class="col-forecast"' : labelTypes[i] === 'current_q' ? ' class="col-current"' : ' class="col-actual"';
                    rows += v != null ? '<td' + cls + '>' + v.toFixed(2) + '</td>' : '<td' + cls + '>\u2014</td>';
                });
                fyKeys.forEach(key => {
                    const fy = scenData[key];
                    rows += fy != null ? '<td>' + fy.toFixed(2) + '</td>' : '<td>\u2014</td>';
                });
                rows += '</tr>';
            });
            tbody.innerHTML = rows;
        }

        // Meta / Footnote
        const metaEl = document.getElementById('panel-meta');
        if (metaEl) {
            const meta = data.meta || {};
            const weights = group.scenario_weights || {};

            // Line 1: Scenario narratives with weights
            const scenParts = [];
            scenarioOrder.filter(sc => sc !== 'Actual' && sc !== 'Weighted Avg').forEach(sc => {
                const w = weights[sc] ? (weights[sc] * 100).toFixed(0) + '%' : '';
                const label = scenarioLabels[sc] || '';
                if (label) scenParts.push('<strong>' + sc + ' (' + w + ')</strong>: ' + label);
            });

            // Line 2: Methodology
            const fqCount = forecastLabels.filter((_, i) => labelTypes[i] === 'forecast').length;
            const methParts = [];
            methParts.push(fqCount + '-quarter rolling forecast');
            methParts.push('Current quarter uses live YTD data');
            if (yearEndLabels.length > 1) methParts.push(yearEndLabels.join(' & ') + ' annual averages');
            else if (yearEndLabels.length === 1) methParts.push(yearEndLabels[0] + ' annual average');
            if (meta.last_updated) methParts.push('Updated ' + meta.last_updated.split('T')[0]);

            metaEl.innerHTML = '<div style="margin-bottom:4px">' + scenParts.join('<br>') + '</div>' +
                '<div style="opacity:0.7">' + methParts.join(' · ') + '</div>';
        }

        // Hide separate history section (now unified into main chart)
        const histSection = document.getElementById('panel-history-section');
        if (histSection) histSection.style.display = 'none';
    }

    // renderForecastHistory removed — historical data now unified into main chart

    // ══════════════════════════════════════════════════════
    // WEO RENDERER (Phase 5)
    // ══════════════════════════════════════════════════════

    function weoFormatValue(val, ds) {
        if (val == null) return '\u2014';
        const decimals = ds.weoValueDecimals != null ? ds.weoValueDecimals : 2;
        const prefix = ds.weoValuePrefix || '';
        const suffix = ds.weoValueSuffix || '%';
        return prefix + val.toFixed(decimals) + suffix;
    }

    let _countryPickerBound = false;

    function populateCountryPicker(ds) {
        const data = PD.getCached(ds.api);
        if (!data) return;
        const sel = document.getElementById('ctrl-countries');
        if (!sel) return;

        const countryData = data.countries || {};
        const groups = ds.countryGroups || {};

        // Build options: group presets first, separator, then individual countries
        let html = '<option value="" disabled selected>Select countries\u2026</option>';
        Object.entries(groups).forEach(([name, isos]) => {
            html += '<option value="group:' + name + '">\u25B8 ' + name + '</option>';
        });
        if (Object.keys(groups).length > 0) html += '<option disabled>\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500</option>';

        // Sort countries by name
        const sorted = Object.entries(countryData)
            .map(([iso, c]) => ({ iso, name: c.name || iso }))
            .sort((a, b) => a.name.localeCompare(b.name));
        sorted.forEach(c => {
            html += '<option value="' + c.iso + '">' + c.name + '</option>';
        });
        sel.innerHTML = html;

        // Make it a single-select dropdown
        sel.size = 1;
        sel.removeAttribute('multiple');

        // Only bind the change listener once to prevent stacking
        if (!_countryPickerBound) {
            _countryPickerBound = true;
            sel.addEventListener('change', () => {
                const v = sel.value;
                const currentDs = PD.findDataset(state.category, state.dataset);
                const currentGroups = currentDs ? (currentDs.countryGroups || {}) : {};
                if (v.startsWith('group:')) {
                    const groupName = v.replace('group:', '');
                    state.countries = currentGroups[groupName] || [];
                } else if (v) {
                    state.countries = [v];
                }
                PD.pushState();
                // Re-render just chart + table, not the whole panel
                renderWeoContent(currentDs);
            });
        }
    }

    function renderWeo(ds) {
        const data = PD.getCached(ds.api);
        if (!data) return;

        // Populate country picker (builds options, binds listener once)
        populateCountryPicker(ds);

        // Summary not used
        const summary = document.getElementById('panel-summary');
        if (summary) summary.innerHTML = '';

        renderWeoContent(ds);
    }

    function renderWeoContent(ds) {
        const data = PD.getCached(ds.api);
        if (!data) return;

        const countries = state.countries.length > 0 ? state.countries : (ds.defaultCountries || []);
        const countryData = data.countries || {};
        const years = data.years || [];
        const forecastStart = data.forecast_start_year || null;

        // Filter years by range
        const currentYear = new Date().getFullYear();
        let filteredYears = years;
        if (state.range !== 'all') {
            const minYear = currentYear - parseInt(state.range);
            filteredYears = years.filter(y => y >= minYear);
        }

        // Unit formatting from catalog
        const valueSuffix = ds.weoValueSuffix != null ? ds.weoValueSuffix : '%';
        const valuePrefix = ds.weoValuePrefix || '';
        const decimals = ds.weoValueDecimals != null ? ds.weoValueDecimals : 2;

        // Chart
        PD.destroyChart('main');
        const canvasEl = document.getElementById('panel-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const forecastIdx = forecastStart ? filteredYears.indexOf(forecastStart) : -1;
        const yearLabels = filteredYears.map(y => String(y));

        const datasets = countries.map((iso, i) => {
            const cData = countryData[iso];
            if (!cData) return null;
            const color = COLORS[i % COLORS.length];
            return {
                label: cData.name || iso,
                data: filteredYears.map(y => {
                    const val = cData.values[String(y)];
                    return val != null ? val : null;
                }),
                borderColor: color,
                backgroundColor: 'transparent',
                borderWidth: 2, fill: false, pointRadius: 3,
                pointBackgroundColor: color, pointBorderColor: color, tension: 0.3,
                segment: forecastIdx > 0 ? {
                    borderDash: ctx2 => ctx2.p0DataIndex >= forecastIdx - 1 ? [4, 3] : [],
                } : undefined,
            };
        }).filter(Boolean);

        PD.setChart('main', new Chart(ctx, {
            type: 'line',
            data: { labels: yearLabels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: { color: '#9ca3af', font: { size: 11 }, boxWidth: 12, padding: 10, usePointStyle: true, pointStyle: 'line' }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#fff',
                        bodyColor: '#d1d5db',
                        borderColor: 'rgba(255,255,255,0.1)',
                        borderWidth: 1,
                        bodyFont: { size: 12 },
                        callbacks: {
                            label: (tooltipCtx) => {
                                const val = tooltipCtx.parsed.y;
                                if (val == null) return tooltipCtx.dataset.label + ': N/A';
                                return tooltipCtx.dataset.label + ': ' + valuePrefix + val.toFixed(decimals) + valueSuffix;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'category',
                        ticks: {
                            color: (tickCtx) => {
                                const yr = parseInt(yearLabels[tickCtx.index]);
                                if (forecastStart && yr >= forecastStart) return '#6b7280';
                                return '#9ca3af';
                            },
                            font: { size: 11 },
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 15,
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' },
                        title: { display: true, text: 'Year', color: '#6b7280', font: { size: 11 } },
                    },
                    y: {
                        ticks: {
                            color: '#6b7280', font: { size: 10 },
                            callback: (val) => valuePrefix + val.toFixed(decimals > 1 ? 1 : decimals) + valueSuffix,
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' },
                    }
                }
            },
        }));

        // Table
        const thead = document.getElementById('panel-thead');
        const tbody = document.getElementById('panel-tbody');
        if (thead && tbody) {
            const reversedYears = filteredYears.slice().reverse();
            let hdr = '<tr><th>Country</th>';
            reversedYears.forEach(y => {
                const cls = forecastStart && y >= forecastStart ? ' class="col-forecast"' : '';
                hdr += '<th' + cls + '>' + y + '</th>';
            });
            hdr += '</tr>';
            thead.innerHTML = hdr;

            let rows = '';
            countries.forEach(iso => {
                const cData = countryData[iso];
                if (!cData) return;
                rows += '<tr><td>' + (cData.name || iso) + '</td>';
                reversedYears.forEach(y => {
                    const val = cData.values[String(y)];
                    const cls = forecastStart && y >= forecastStart ? ' class="col-forecast"' : '';
                    rows += '<td' + cls + '>' + weoFormatValue(val, ds) + '</td>';
                });
                rows += '</tr>';
            });
            tbody.innerHTML = rows;
        }

        // Meta
        const metaEl = document.getElementById('panel-meta');
        if (metaEl) {
            const meta = data.meta || {};
            const parts = [];
            if (meta.source) parts.push(meta.source);
            if (meta.indicator_name) parts.push(meta.indicator_name);
            if (forecastStart) parts.push('Forecast from ' + forecastStart);
            parts.push(countries.length + ' countries');
            if (meta.last_updated) parts.push('Updated: ' + meta.last_updated);
            metaEl.textContent = parts.join(' \u00b7 ');
        }

        // Hide history section
        const histSection = document.getElementById('panel-history-section');
        if (histSection) histSection.style.display = 'none';
    }

    // ══════════════════════════════════════════════════════
    // CHART OPTIONS HELPERS
    // ══════════════════════════════════════════════════════

    function chartOptions(opts) {
        return {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: opts.legend ? {
                    position: 'top',
                    labels: { color: '#9ca3af', font: { size: 11 }, boxWidth: 12, padding: 10, usePointStyle: true, pointStyle: 'line' }
                } : { display: false },
                tooltip: {
                    backgroundColor: 'rgba(0,0,0,0.9)',
                    titleColor: '#fff',
                    bodyColor: '#d1d5db',
                    borderColor: 'rgba(255,255,255,0.1)',
                    borderWidth: 1,
                    bodyFont: { size: 12 },
                    callbacks: opts.tooltip ? { label: opts.tooltip } : {},
                }
            },
            scales: {
                x: {
                    type: opts.xType || undefined,
                    ticks: {
                        color: '#6b7280', font: { size: 10 },
                        maxRotation: opts.maxRotation || 0,
                        autoSkip: true,
                        maxTicksLimit: opts.maxTicksLimitX || 12,
                    },
                    grid: { color: 'rgba(55,65,81,0.3)' }
                },
                y: {
                    ticks: {
                        color: '#6b7280', font: { size: 10 },
                        callback: opts.yCallback || undefined,
                    },
                    grid: { color: 'rgba(55,65,81,0.3)' },
                    beginAtZero: opts.beginAtZero || false,
                }
            }
        };
    }

    // forecastChartOptions removed — chart options now inline in each renderer

    // ══════════════════════════════════════════════════════
    // SEARCH OVERLAY (Phase 3)
    // ══════════════════════════════════════════════════════

    function setupSearchOverlay() {
        const overlay = document.getElementById('search-overlay');
        const input = document.getElementById('search-input');
        const results = document.getElementById('search-results');
        if (!overlay || !input || !results) return;

        let selectedIdx = -1;

        // Open with Ctrl+K / Cmd+K
        document.addEventListener('keydown', (e) => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
                e.preventDefault();
                openSearch();
            }
            if (e.key === 'Escape' && overlay.style.display !== 'none') {
                closeSearch();
            }
        });

        // Sidebar search trigger
        const sidebarSearch = document.getElementById('sidebar-search-input');
        if (sidebarSearch) {
            sidebarSearch.addEventListener('focus', (e) => {
                e.target.blur();
                openSearch();
            });
        }

        // Overlay click to close
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) closeSearch();
        });

        // Filter on input
        input.addEventListener('input', () => {
            selectedIdx = -1;
            const q = input.value.toLowerCase().trim();
            if (!q) { results.innerHTML = ''; return; }

            const matches = searchIndex.filter(item => item.terms.includes(q)).slice(0, 10);
            results.innerHTML = matches.map((m, i) =>
                '<div class="search-result' + (i === selectedIdx ? ' selected' : '') + '" data-idx="' + i + '">' +
                '<span class="search-result-label">' + m.label + '</span>' +
                '<span class="search-result-category">' + m.category + '</span>' +
                '</div>'
            ).join('');

            results.querySelectorAll('.search-result').forEach((el, i) => {
                el.addEventListener('click', () => {
                    navigateToResult(matches[i]);
                    closeSearch();
                });
            });
        });

        // Keyboard navigation
        input.addEventListener('keydown', (e) => {
            const items = results.querySelectorAll('.search-result');
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                selectedIdx = Math.min(selectedIdx + 1, items.length - 1);
                items.forEach((el, i) => el.classList.toggle('selected', i === selectedIdx));
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                selectedIdx = Math.max(selectedIdx - 1, 0);
                items.forEach((el, i) => el.classList.toggle('selected', i === selectedIdx));
            } else if (e.key === 'Enter' && selectedIdx >= 0) {
                e.preventDefault();
                const q = input.value.toLowerCase().trim();
                const matches = searchIndex.filter(item => item.terms.includes(q)).slice(0, 10);
                if (matches[selectedIdx]) {
                    navigateToResult(matches[selectedIdx]);
                    closeSearch();
                }
            }
        });

        function openSearch() {
            overlay.style.display = '';
            input.value = '';
            results.innerHTML = '';
            selectedIdx = -1;
            setTimeout(() => input.focus(), 50);
        }

        function closeSearch() {
            overlay.style.display = 'none';
        }

        function navigateToResult(item) {
            PD.navigate(item.categoryId, item.datasetId, item.subviewId);
        }
    }

    // ══════════════════════════════════════════════════════
    // MOBILE BOTTOM SHEET (Phase 4)
    // ══════════════════════════════════════════════════════

    function setupBottomSheet() {
        const sheet = document.getElementById('bottom-sheet');
        const sheetOverlay = document.getElementById('bottom-sheet-overlay');
        if (!sheet || !sheetOverlay) return;

        sheetOverlay.addEventListener('click', closeBottomSheet);

        const closeBtn = sheet.querySelector('.bottom-sheet-close');
        if (closeBtn) closeBtn.addEventListener('click', closeBottomSheet);

        // Filter input inside the bottom sheet
        const filterInput = document.getElementById('bottom-sheet-search-input');
        if (filterInput) {
            filterInput.addEventListener('input', () => {
                const q = filterInput.value.toLowerCase().trim();
                const items = sheet.querySelectorAll('.bs-item');
                const headings = sheet.querySelectorAll('.bs-heading');

                if (!q) {
                    items.forEach(el => el.style.display = '');
                    headings.forEach(el => el.style.display = '');
                    return;
                }

                // Track which headings have visible children
                const headingVisible = new Map();
                headings.forEach(h => headingVisible.set(h, false));

                items.forEach(el => {
                    const text = el.textContent.toLowerCase();
                    const match = text.includes(q);
                    el.style.display = match ? '' : 'none';
                    if (match) {
                        // Find preceding heading
                        let prev = el.previousElementSibling;
                        while (prev && !prev.classList.contains('bs-heading')) {
                            prev = prev.previousElementSibling;
                        }
                        if (prev) headingVisible.set(prev, true);
                    }
                });

                headings.forEach(h => {
                    h.style.display = headingVisible.get(h) ? '' : 'none';
                });
            });
        }
    }

    // Current bottom sheet level for reuse by nav bar
    let _sheetLevel = 'root';

    function openBottomSheet(level, id) {
        const sheet = document.getElementById('bottom-sheet');
        const sheetOverlay = document.getElementById('bottom-sheet-overlay');
        const sheetContent = document.getElementById('bottom-sheet-content');
        const filterInput = document.getElementById('bottom-sheet-search-input');
        if (!sheet || !sheetOverlay || !sheetContent) return;

        _sheetLevel = level;

        // Reset filter
        if (filterInput) {
            filterInput.value = '';
            // Show/hide filter — only useful for browse level
            filterInput.parentElement.style.display = (level === 'root' || level === 'category') ? '' : 'none';
        }

        let html = '';

        if (level === 'root' || level === 'category') {
            // Show categories and their datasets
            for (const cat of PD.CATALOG.categories) {
                if (!cat.datasets || cat.datasets.length === 0) continue;
                html += '<div class="bs-heading">' + cat.label + '</div>';
                for (const ds of cat.datasets) {
                    const isActive = state.category === cat.id && state.dataset === ds.id;
                    html += '<button class="bs-item' + (isActive ? ' active' : '') +
                        '" data-cat="' + cat.id + '" data-ds="' + ds.id + '">' + ds.label + '</button>';
                }
            }
        } else if (level === 'dataset') {
            // Show sub-views for current dataset
            const ds = PD.findDataset(state.category, state.dataset);
            if (ds && ds.subviews && ds.subviews.length > 1) {
                html += '<div class="bs-heading">' + ds.label + '</div>';
                for (const sv of ds.subviews) {
                    const isActive = (state.subview || 'overview') === sv.id;
                    html += '<button class="bs-item' + (isActive ? ' active' : '') +
                        '" data-sv="' + sv.id + '">' + sv.label + '</button>';
                }
            }
        }

        sheetContent.innerHTML = html;

        // Bind clicks
        sheetContent.querySelectorAll('.bs-item').forEach(btn => {
            btn.addEventListener('click', () => {
                if (btn.dataset.cat) {
                    PD.navigate(btn.dataset.cat, btn.dataset.ds, null);
                } else if (btn.dataset.sv) {
                    PD.navigate(state.category, state.dataset, btn.dataset.sv);
                }
                closeBottomSheet();
            });
        });

        sheet.classList.add('open');
        sheetOverlay.classList.add('open');
    }

    function closeBottomSheet() {
        const sheet = document.getElementById('bottom-sheet');
        const sheetOverlay = document.getElementById('bottom-sheet-overlay');
        if (sheet) sheet.classList.remove('open');
        if (sheetOverlay) sheetOverlay.classList.remove('open');
    }

    // ══════════════════════════════════════════════════════
    // MOBILE NAV BAR
    // ══════════════════════════════════════════════════════

    function setupMobileNavBar() {
        const browseBtn = document.getElementById('mobile-nav-browse');
        const searchBtn = document.getElementById('mobile-nav-search');

        if (browseBtn) {
            browseBtn.addEventListener('click', () => {
                openBottomSheet('root', null);
            });
        }

        if (searchBtn) {
            searchBtn.addEventListener('click', () => {
                // Reuse the existing Ctrl+K search overlay
                const overlay = document.getElementById('search-overlay');
                const input = document.getElementById('search-input');
                const results = document.getElementById('search-results');
                if (overlay && input) {
                    overlay.style.display = '';
                    input.value = '';
                    if (results) results.innerHTML = '';
                    setTimeout(() => input.focus(), 50);
                }
            });
        }
    }


    // ══════════════════════════════════════════════════════
    // SOVEREIGN DEBT INDICATOR RENDERER
    // ══════════════════════════════════════════════════════

    let _debtMapChart = null;
    let _debtBarChart = null;

    const TIER_COLORS = {
        Critical: { bg: '#991b1b', light: '#fecaca', text: '#fca5a5' },
        High:     { bg: '#c2410c', light: '#fed7aa', text: '#fdba74' },
        Elevated: { bg: '#a16207', light: '#fef08a', text: '#fde047' },
        Moderate: { bg: '#15803d', light: '#bbf7d0', text: '#86efac' },
        Low:      { bg: '#1d4ed8', light: '#bfdbfe', text: '#93c5fd' },
    };

    const TIER_ORDER = ['Critical', 'High', 'Elevated', 'Moderate', 'Low'];

    // Persistent map state — so we don't re-fetch world atlas on every filter change
    let _sdMapState = null;  // { svg, g, paths, codeMap, gapLookup, colorScale }

    function renderSovereignDebt(ds) {
        const data = PD.getCached(ds.api);
        if (!data || !data.countries) return;

        const panel = document.getElementById('active-panel');
        const subview = state.subview || 'map';

        const summary = data.summary || {};
        const tierCounts = summary.tier_counts || {};

        const matTierCounts = summary.maturity_tier_counts || {};

        let tierBadges = TIER_ORDER.map(t => {
            const c = TIER_COLORS[t];
            const n = tierCounts[t] || 0;
            return `<span class="sd-tier-badge" style="background:${c.bg};color:${c.text}">${t}: ${n}</span>`;
        }).join('');

        let matBadges = TIER_ORDER.filter(t => (matTierCounts[t] || 0) > 0).map(t => {
            const c = TIER_COLORS[t];
            return `<span class="sd-tier-badge" style="background:${c.bg};color:${c.text};font-size:10px">${t}: ${matTierCounts[t]}</span>`;
        }).join('');

        const regions = [...new Set(Object.values(data.countries).map(c => c.region).filter(Boolean))].sort();
        let regionOpts = '<option value="all">All Regions</option>' +
            regions.map(r => `<option value="${r}">${r}</option>`).join('');
        let tierOpts = '<option value="all">All Tiers</option>' +
            TIER_ORDER.map(t => `<option value="${t}">${t}</option>`).join('');

        panel.innerHTML = `
            <div class="sd-header">
                <div class="sd-title-row">
                    <h2 class="sd-title">Shadow Debt Indicator</h2>
                    <a href="${ds.exportUrl}" class="sd-export-btn" title="Export Excel">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
                            <polyline points="7 10 12 15 17 10"/>
                            <line x1="12" y1="15" x2="12" y2="3"/>
                        </svg>
                        Export
                    </a>
                </div>
                <div class="sd-summary">
                    <span class="sd-stat">${summary.total_countries || 0} countries</span>
                    <span class="sd-stat">Avg official: ${summary.avg_official || '—'}%</span>
                    <span class="sd-stat">Avg estimated: ${summary.avg_estimated || '—'}%</span>
                    <span class="sd-stat">Avg gap: ${summary.avg_gap || '—'}pp</span>
                </div>
                <div class="sd-tier-row">${tierBadges}</div>
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
                    <span style="font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px">Maturity Risk</span>
                    ${matBadges}
                </div>
                <div class="sd-controls">
                    <select id="sd-region-filter" class="sd-select">${regionOpts}</select>
                    <select id="sd-tier-filter" class="sd-select">${tierOpts}</select>
                </div>
            </div>
            <div id="sd-content"></div>
        `;

        // Reset map state when switching into this view
        _sdMapState = null;

        const regionSelect = document.getElementById('sd-region-filter');
        const tierSelect = document.getElementById('sd-tier-filter');

        function onFilterChange() {
            const r = regionSelect.value, t = tierSelect.value;
            if (subview === 'map') {
                // Update map colors + opacity in place (no full re-render)
                _updateMapFilters(data, r, t);
                // Re-draw only the bar chart
                _drawDebtTopChart(_filterDebtCountries(data, r, t));
            } else {
                _renderDebtSubview(subview, data, r, t);
            }
        }

        regionSelect.addEventListener('change', onFilterChange);
        tierSelect.addEventListener('change', onFilterChange);

        _renderDebtSubview(subview, data, regionSelect.value, tierSelect.value);
    }

    function _filterDebtCountries(data, regionFilter, tierFilter) {
        let entries = Object.entries(data.countries);
        if (regionFilter !== 'all') {
            entries = entries.filter(([, c]) => c.region === regionFilter);
        }
        if (tierFilter !== 'all') {
            entries = entries.filter(([, c]) => c.risk_tier === tierFilter);
        }
        return entries;
    }

    function _renderDebtSubview(subview, data, regionFilter, tierFilter) {
        const container = document.getElementById('sd-content');
        if (!container) return;
        const filtered = _filterDebtCountries(data, regionFilter, tierFilter);

        switch (subview) {
            case 'map':
                _renderDebtMap(container, filtered, data, regionFilter, tierFilter);
                break;
            case 'ranking':
                _renderDebtRanking(container, filtered);
                break;
            case 'table':
                _renderDebtTable(container, filtered);
                break;
            default:
                _renderDebtMap(container, filtered, data, regionFilter, tierFilter);
        }
    }

    function _renderDebtMap(container, entries, data, regionFilter, tierFilter) {
        container.innerHTML = `
            <div class="sd-map-wrap" id="sd-map-wrap">
                <div id="sd-map-svg"></div>
                <div id="sd-map-tooltip" class="sd-tooltip hidden"></div>
                <div class="sd-map-legend">
                    <span>Low gap</span>
                    <div class="sd-legend-bar"></div>
                    <span>High gap</span>
                </div>
            </div>
            <div class="sd-map-below">
                <h3>Top 20 — Largest Shadow Debt Gaps</h3>
                <div id="sd-top-chart-wrap"><canvas id="sd-top-chart"></canvas></div>
            </div>
        `;

        if (typeof d3 === 'undefined') {
            document.getElementById('sd-map-svg').innerHTML =
                '<p style="text-align:center;color:var(--text-muted);padding:40px;">Map requires D3.js.</p>';
        } else {
            _drawDebtChoropleth(data, regionFilter, tierFilter);
        }
        _drawDebtTopChart(entries);
    }

    async function _drawDebtChoropleth(data, regionFilter, tierFilter) {
        const svgContainer = document.getElementById('sd-map-svg');
        if (!svgContainer || typeof d3 === 'undefined') return;

        const width = 960, height = 500;

        // Clear any previous SVG
        d3.select('#sd-map-svg').selectAll('*').remove();

        const svg = d3.select('#sd-map-svg')
            .append('svg')
            .attr('viewBox', `0 0 ${width} ${height}`)
            .attr('preserveAspectRatio', 'xMidYMid meet')
            .style('width', '100%')
            .style('display', 'block');

        svg.append('rect').attr('width', width).attr('height', height).attr('fill', '#0a0e1a');

        const g = svg.append('g');
        const projection = d3.geoNaturalEarth1().scale(160).translate([width / 2, height / 2 + 20]);
        const pathGen = d3.geoPath().projection(projection);

        // Build numeric-id → ISO3 code map
        let codeMap = {};
        try {
            const res = await fetch('/static/data/country_codes.json');
            const codes = await res.json();
            codes.forEach(c => {
                if (c['country-code'] && c['alpha-3']) {
                    codeMap[String(parseInt(c['country-code']))] = c['alpha-3'];
                }
            });
        } catch (e) {}

        // Gap lookup from full data (not filtered — filters just change opacity)
        const gapLookup = {};
        for (const [iso3, c] of Object.entries(data.countries)) {
            gapLookup[iso3] = c;
        }

        const colorScale = d3.scaleLinear()
            .domain([0, 5, 15, 30, 50])
            .range(['#1e3a5f', '#2563eb', '#f59e0b', '#f97316', '#dc2626'])
            .clamp(true);

        // Build a Set of currently filtered ISO3 codes
        const filtered = _filterDebtCountries(data, regionFilter, tierFilter);
        const filteredSet = new Set(filtered.map(([iso3]) => iso3));

        try {
            const world = await d3.json('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json');
            const geoCountries = topojson.feature(world, world.objects.countries);

            const paths = g.selectAll('path.sd-country')
                .data(geoCountries.features)
                .join('path')
                .attr('class', 'sd-country')
                .attr('d', pathGen)
                .attr('fill', d => {
                    const iso3 = codeMap[String(d.id)];
                    const c = gapLookup[iso3];
                    if (!c || c.debt_gap_pp == null) return '#1f2937';
                    return colorScale(c.debt_gap_pp);
                })
                .attr('opacity', d => {
                    const iso3 = codeMap[String(d.id)];
                    if (!iso3 || !gapLookup[iso3]) return 0.3;
                    return filteredSet.has(iso3) ? 1 : 0.15;
                })
                .attr('stroke', '#2d3748')
                .attr('stroke-width', 0.5)
                .style('cursor', 'pointer');

            // Tooltip — use mouse position relative to the map container
            const mapWrap = document.getElementById('sd-map-wrap');
            const tooltip = document.getElementById('sd-map-tooltip');

            paths
                .on('mouseenter', function (event, d) {
                    const iso3 = codeMap[String(d.id)];
                    const c = gapLookup[iso3];
                    d3.select(this).attr('stroke', '#fff').attr('stroke-width', 1.5).raise();

                    if (!c) {
                        tooltip.innerHTML = `<strong>${iso3 || '—'}</strong><br><span style="color:#6b7280">No data</span>`;
                    } else {
                        const tc = TIER_COLORS[c.risk_tier] || TIER_COLORS.Low;
                        const mc = TIER_COLORS[c.maturity_risk_tier] || TIER_COLORS.Low;
                        tooltip.innerHTML =
                            `<strong>${c.name}</strong>` +
                            `<span class="sd-tip-tier" style="background:${tc.bg};color:${tc.text}">${c.risk_tier}</span>` +
                            `<div class="sd-tip-grid">` +
                            `<span>Official</span><span>${c.official_debt_gdp != null ? c.official_debt_gdp.toFixed(1) + '%' : '—'}</span>` +
                            `<span>Estimated</span><span>${c.estimated_debt_gdp != null ? c.estimated_debt_gdp.toFixed(1) + '%' : '—'}</span>` +
                            `<span>Gap</span><span style="color:${c.debt_gap_pp > 10 ? '#f59e0b' : '#9ca3af'}">${c.debt_gap_pp != null ? c.debt_gap_pp.toFixed(1) + 'pp' : '—'}</span>` +
                            `</div>` +
                            `<div class="sd-tip-mat" style="margin-top:6px;padding-top:6px;border-top:1px solid rgba(255,255,255,0.1)">` +
                            `<span style="font-size:9px;color:#6b7280;text-transform:uppercase;letter-spacing:0.5px">Maturity Risk</span>` +
                            `<span class="sd-tip-tier" style="background:${mc.bg};color:${mc.text};font-size:9px">${c.maturity_risk_tier || '—'} (${c.maturity_risk_score || '—'})</span>` +
                            `<div class="sd-tip-grid" style="font-size:10px">` +
                            `<span>ST share</span><span>${c.short_term_share != null ? c.short_term_share.toFixed(0) + '%' : '—'}</span>` +
                            `<span>Svc/Exports</span><span>${c.debt_service_pct_exports != null ? c.debt_service_pct_exports.toFixed(0) + '%' : '—'}</span>` +
                            `</div></div>`;
                    }
                    tooltip.classList.remove('hidden');
                })
                .on('mousemove', function (event) {
                    if (!mapWrap || !tooltip) return;
                    const rect = mapWrap.getBoundingClientRect();
                    let x = event.clientX - rect.left + 14;
                    let y = event.clientY - rect.top - 10;
                    // Keep tooltip inside the map container
                    if (x + 220 > rect.width) x = event.clientX - rect.left - 230;
                    if (y < 0) y = 10;
                    tooltip.style.left = x + 'px';
                    tooltip.style.top = y + 'px';
                })
                .on('mouseleave', function () {
                    d3.select(this).attr('stroke', '#2d3748').attr('stroke-width', 0.5);
                    tooltip.classList.add('hidden');
                });

            // Zoom — use filter to allow mouse wheel scroll through page
            const zoom = d3.zoom()
                .scaleExtent([1, 8])
                .filter(event => {
                    // Only zoom on ctrl+wheel or pinch, not plain scroll
                    if (event.type === 'wheel') return event.ctrlKey;
                    return !event.button; // allow drag pan
                })
                .on('zoom', e => g.attr('transform', e.transform));
            svg.call(zoom);
            svg.on('dblclick.zoom', () =>
                svg.transition().duration(400).call(zoom.transform, d3.zoomIdentity));

            // Save state for filter updates
            _sdMapState = { paths, codeMap, gapLookup, colorScale, filteredSet };

        } catch (e) {
            svgContainer.innerHTML = '<p style="text-align:center;color:var(--text-muted);padding:40px;">Could not load map data</p>';
        }
    }

    function _updateMapFilters(data, regionFilter, tierFilter) {
        if (!_sdMapState || !_sdMapState.paths) return;
        const { paths, codeMap, gapLookup } = _sdMapState;

        // Rebuild filtered set
        const filtered = _filterDebtCountries(data, regionFilter, tierFilter);
        const filteredSet = new Set(filtered.map(([iso3]) => iso3));
        _sdMapState.filteredSet = filteredSet;

        // Update opacity only — no full re-render
        paths.transition().duration(300)
            .attr('opacity', d => {
                const iso3 = codeMap[String(d.id)];
                if (!iso3 || !gapLookup[iso3]) return 0.15;
                return filteredSet.has(iso3) ? 1 : 0.15;
            });
    }

    function _drawDebtTopChart(entries) {
        const canvas = document.getElementById('sd-top-chart');
        if (!canvas) return;

        if (_debtBarChart) { _debtBarChart.destroy(); _debtBarChart = null; }

        // Top 20 by gap
        const top20 = [...entries]
            .sort((a, b) => (b[1].debt_gap_pp || 0) - (a[1].debt_gap_pp || 0))
            .slice(0, 20);

        const labels = top20.map(([, c]) => c.name || c.iso3);
        const officialData = top20.map(([, c]) => c.official_debt_gdp || 0);
        const gapData = top20.map(([, c]) => c.debt_gap_pp || 0);

        _debtBarChart = new Chart(canvas, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Official Debt (% GDP)',
                        data: officialData,
                        backgroundColor: '#2563eb',
                        borderRadius: 2,
                    },
                    {
                        label: 'Shadow Debt Gap (pp)',
                        data: gapData,
                        backgroundColor: '#dc2626',
                        borderRadius: 2,
                    },
                ],
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { labels: { color: '#9ca3af', font: { size: 11 } } },
                    tooltip: {
                        callbacks: {
                            label: ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}${ctx.datasetIndex === 0 ? '%' : 'pp'}`,
                        },
                    },
                },
                scales: {
                    x: {
                        stacked: true,
                        grid: { color: '#1f2937' },
                        ticks: { color: '#9ca3af', font: { size: 10 } },
                        title: { display: true, text: '% of GDP', color: '#6b7280', font: { size: 10 } },
                    },
                    y: {
                        stacked: true,
                        grid: { display: false },
                        ticks: { color: '#e5e7eb', font: { size: 11 } },
                    },
                },
            },
        });
    }

    function _renderDebtRanking(container, entries) {
        const sorted = [...entries].sort((a, b) => (b[1].debt_gap_pp || 0) - (a[1].debt_gap_pp || 0));

        let rows = sorted.map(([iso3, c], i) => {
            const tier = c.risk_tier || 'Low';
            const tc = TIER_COLORS[tier];
            const mt = c.maturity_risk_tier || 'Low';
            const mc = TIER_COLORS[mt] || TIER_COLORS.Low;
            const gapWidth = Math.min(100, (c.debt_gap_pp || 0) / 60 * 100);
            return `
                <div class="sd-rank-row">
                    <span class="sd-rank-num">${i + 1}</span>
                    <span class="sd-rank-name">${c.name || iso3}</span>
                    <span class="sd-rank-tier" style="background:${tc.bg};color:${tc.text}">${tier}</span>
                    <div class="sd-rank-bar-wrap">
                        <div class="sd-rank-bar-official" style="width:${Math.min(100, (c.official_debt_gdp || 0) / 250 * 100)}%"></div>
                        <div class="sd-rank-bar-gap" style="width:${gapWidth}%"></div>
                    </div>
                    <span class="sd-rank-val">${(c.official_debt_gdp || 0).toFixed(0)}%</span>
                    <span class="sd-rank-gap">+${(c.debt_gap_pp || 0).toFixed(1)}pp</span>
                    <span class="sd-rank-tier" style="background:${mc.bg};color:${mc.text};font-size:9px" title="Maturity risk: ${c.maturity_risk_score || '—'}">${mt}</span>
                </div>
            `;
        }).join('');

        container.innerHTML = `
            <div class="sd-ranking">
                <div class="sd-rank-header">
                    <span></span><span>Country</span><span>Debt Tier</span>
                    <span>Official + Gap</span><span>Official</span><span>Gap</span><span>Maturity</span>
                </div>
                ${rows}
            </div>
        `;
    }

    function _renderDebtTable(container, entries) {
        const sorted = [...entries].sort((a, b) => (b[1].estimated_debt_gdp || 0) - (a[1].estimated_debt_gdp || 0));

        let rows = sorted.map(([iso3, c]) => {
            const tier = c.risk_tier || 'Low';
            const tc = TIER_COLORS[tier];
            const mt = c.maturity_risk_tier || 'Low';
            const mc = TIER_COLORS[mt] || TIER_COLORS.Low;
            return `<tr>
                <td>${c.name || iso3}</td>
                <td>${iso3}</td>
                <td>${c.region || ''}</td>
                <td>${_fmtNum(c.official_debt_gdp)}%</td>
                <td class="sd-cell-est">${_fmtNum(c.estimated_debt_gdp)}%</td>
                <td class="sd-cell-gap" style="color:${(c.debt_gap_pp||0) > 10 ? '#f59e0b' : '#9ca3af'}">${_fmtNum(c.debt_gap_pp)}pp</td>
                <td><span class="sd-tier-cell" style="background:${tc.bg};color:${tc.text}">${tier}</span></td>
                <td>${_fmtNum(c.short_term_share)}%</td>
                <td>${_fmtNum(c.debt_service_pct_exports)}%</td>
                <td>${_fmtNum(c.interest_pct_revenue)}%</td>
                <td>${_fmtNum(c.reserve_coverage_pct, '', '%', 0)}</td>
                <td>${_fmtNum(c.maturity_risk_score, '', '', 0)}</td>
                <td><span class="sd-tier-cell" style="background:${mc.bg};color:${mc.text}">${mt}</span></td>
            </tr>`;
        }).join('');

        container.innerHTML = `
            <div class="sd-table-wrap">
                <table class="sd-table">
                    <thead>
                        <tr>
                            <th>Country</th><th>ISO3</th><th>Region</th>
                            <th>Official<br>(%GDP)</th><th>Estimated<br>(%GDP)</th><th>Gap</th>
                            <th>Debt<br>Tier</th>
                            <th>ST Debt<br>Share</th><th>Svc/<br>Exports</th><th>Int/<br>Revenue</th>
                            <th>Reserve<br>Cover</th><th>Mat.<br>Score</th><th>Mat.<br>Tier</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;
    }

    function _fmtNum(val, prefix, suffix, dec) {
        if (val == null) return '—';
        const d = dec != null ? dec : 1;
        return (prefix || '') + val.toFixed(d) + (suffix || '');
    }

})();
