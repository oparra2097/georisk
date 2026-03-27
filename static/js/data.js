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

        if (cached) {
            hideLoading();
            renderCurrentDataset();
            // Also fetch components if needed
            if (ds.type === 'cpi' && state.subview && state.subview !== 'overview' && ds.componentApi) {
                const compCached = PD.getCached(ds.componentApi);
                if (compCached) {
                    renderCurrentDataset();
                } else {
                    fetchData(ds.componentApi, () => renderCurrentDataset());
                }
            }
        } else {
            fetchData(apiUrl, () => {
                hideLoading();
                renderCurrentDataset();
                // Populate region dropdown for COFER
                if (ds.type === 'cofer') populateRegionDropdown();
                // Populate scenario dropdown for forecasts
                if (ds.type === 'forecast-group') populateScenarioDropdown(ds);
            });
            // Also fetch components if CPI component view
            if (ds.type === 'cpi' && state.subview && state.subview !== 'overview' && ds.componentApi) {
                fetchData(ds.componentApi, () => renderCurrentDataset());
            }
        }
    }

    function hideLoading() {
        const el = document.getElementById('panel-loading');
        if (el) el.style.display = 'none';
    }

    async function fetchData(url, callback) {
        try {
            const resp = await fetch(url);
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            const data = await resp.json();
            PD.setCached(url, data);
            if (callback) callback();
        } catch (err) {
            console.error('Fetch failed for ' + url + ':', err);
            const el = document.getElementById('panel-loading');
            if (el) el.innerHTML = '<p style="color:var(--text-muted)">Failed to load data.</p>';
        }
    }

    function renderCurrentDataset() {
        const ds = PD.findDataset(state.category, state.dataset);
        if (!ds) return;

        switch (ds.type) {
            case 'cofer': renderCofer(ds); break;
            case 'cpi': renderCpi(ds); break;
            case 'forecast-group': renderForecast(ds); break;
            case 'weo': renderWeo(ds); break;
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
        const waOpt = document.createElement('option');
        waOpt.value = 'Weighted Avg'; waOpt.textContent = 'Weighted Avg'; waOpt.selected = true;
        sel.appendChild(waOpt);
        order.forEach(sc => {
            if (sc === 'Actual' || sc === 'Weighted Avg') return;
            const opt = document.createElement('option');
            opt.value = sc; opt.textContent = sc;
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
        const commodities = group.commodities || {};
        const groupColors = group.colors || {};
        const timeCtx = data.time_context || {};
        const forecastLabels = timeCtx.labels || [];
        const labelTypes = timeCtx.label_types || [];
        const yearEndLabel = timeCtx.year_end_label || 'FY Avg';

        // ── Build unified timeline: historical quarters + current year forecast ──
        // Collect all historical labels across commodities, then append forecast labels
        const histLabelSet = new Set();
        Object.values(commodities).forEach(info => {
            (info.historical || []).forEach(h => histLabelSet.add(h.label));
        });
        // Sort historical labels chronologically ("2015 Q1" < "2015 Q2" < "2016 Q1" ...)
        const histLabels = Array.from(histLabelSet).sort((a, b) => {
            const [ya, qa] = a.split(' Q'); const [yb, qb] = b.split(' Q');
            return (parseInt(ya) * 10 + parseInt(qa)) - (parseInt(yb) * 10 + parseInt(qb));
        });

        // Combine: all historical + forecast labels
        const allLabels = histLabels.concat(forecastLabels);
        const forecastStartIdx = histLabels.length; // index where forecast portion begins

        // Build a lookup map for each commodity: label → value
        const commodityMaps = {};
        Object.entries(commodities).forEach(([name, info]) => {
            const valMap = {};
            // Historical data
            (info.historical || []).forEach(h => { valMap[h.label] = h.avg_price; });
            // Forecast / current year scenario data
            const scenData = (info.scenarios || {})[scenario] || {};
            forecastLabels.forEach(l => { if (scenData[l] != null) valMap[l] = scenData[l]; });
            commodityMaps[name] = valMap;
        });

        // Summary not used for forecast groups
        const summary = document.getElementById('panel-summary');
        if (summary) summary.innerHTML = '';

        // ── Chart ──
        PD.destroyChart('main');
        const canvasEl = document.getElementById('panel-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const datasets = Object.entries(commodities).map(([name, info]) => {
            const valMap = commodityMaps[name];
            const color = groupColors[name] || COLORS[0];
            return {
                label: name,
                data: allLabels.map(l => valMap[l] != null ? valMap[l] : null),
                borderColor: color,
                backgroundColor: 'transparent',
                borderWidth: 2, fill: false, pointRadius: 0, pointHitRadius: 8,
                pointBackgroundColor: color, pointBorderColor: color, tension: 0.3,
                segment: forecastStartIdx > 0 ? {
                    borderDash: ctx2 => ctx2.p0DataIndex >= forecastStartIdx - 1 ? [4, 3] : [],
                } : undefined,
            };
        });

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
                                const name = tooltipCtx.dataset.label;
                                const info = commodities[name];
                                const unit = info ? info.unit : '';
                                if (val == null) return name + ': N/A';
                                return name + ': ' + val.toFixed(2) + ' ' + unit;
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
                                    // Color forecast ticks by their type
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

        // ── Table: show yearly averages from historical + forecast quarters ──
        const thead = document.getElementById('panel-thead');
        const tbody = document.getElementById('panel-tbody');
        if (thead && tbody) {
            // Build yearly summary from historical + forecast data
            const yearMap = {}; // year → { commodity → [values] }
            allLabels.forEach((l, idx) => {
                // Parse year from label: "2015 Q1" → 2015, "Q3" → forecastYear, "Q1'27" → 2027
                let yr;
                if (l.includes(' Q')) {
                    yr = parseInt(l.split(' Q')[0]);
                } else if (l.includes("'")) {
                    yr = 2000 + parseInt(l.split("'")[1]);
                } else {
                    yr = data.forecast_year || new Date().getFullYear();
                }
                if (!yearMap[yr]) yearMap[yr] = {};
                Object.entries(commodities).forEach(([name]) => {
                    if (!yearMap[yr][name]) yearMap[yr][name] = [];
                    const v = commodityMaps[name][l];
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

        // Meta
        const metaEl = document.getElementById('panel-meta');
        if (metaEl) {
            const meta = data.meta || {};
            const weights = group.scenario_weights || {};
            const parts = [];
            if (meta.source) parts.push(meta.source);
            parts.push(scenario);
            if (weights[scenario]) parts.push('Weight: ' + (weights[scenario] * 100).toFixed(0) + '%');
            if (meta.method) parts.push(meta.method);
            if (meta.baseline) parts.push('Baseline: ' + meta.baseline);
            if (meta.last_updated) parts.push('Updated: ' + meta.last_updated.split('T')[0]);
            metaEl.textContent = parts.join(' \u00b7 ');
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
            hdr += '<th>' + yearEndLabel + '</th></tr>';
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
                const fy = scenData['FY'];
                rows += fy != null ? '<td>' + fy.toFixed(2) + '</td>' : '<td>\u2014</td>';
                rows += '</tr>';
            });
            tbody.innerHTML = rows;
        }

        // Meta
        const metaEl = document.getElementById('panel-meta');
        if (metaEl) {
            const weights = group.scenario_weights || {};
            const parts = [];
            scenarioOrder.filter(sc => sc !== 'Actual' && sc !== 'Weighted Avg').forEach(sc => {
                const w = weights[sc] ? (weights[sc] * 100).toFixed(0) + '%' : '';
                const label = scenarioLabels[sc] || '';
                if (label) parts.push(sc + ' (' + w + '): ' + label);
            });
            metaEl.textContent = parts.join(' \u00b7 ');
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
    }

    function openBottomSheet(level, id) {
        const sheet = document.getElementById('bottom-sheet');
        const sheetOverlay = document.getElementById('bottom-sheet-overlay');
        const sheetContent = document.getElementById('bottom-sheet-content');
        if (!sheet || !sheetOverlay || !sheetContent) return;

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

})();
