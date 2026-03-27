/* ===== Data Page: Multi-Dataset Handler ===== */

(function () {
    'use strict';

    const COLORS = [
        '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#ec4899',
        '#8b5cf6', '#f97316', '#06b6d4', '#84cc16', '#e11d48',
        '#6366f1', '#14b8a6', '#f43f5e', '#a855f7', '#22c55e',
        '#eab308', '#0ea5e9', '#d946ef', '#64748b', '#fb923c',
    ];

    // ── State ────────────────────────────────────────────
    let currentDataset = 'cofer';

    // COFER state
    let coferData = null;
    let coferChart = null;
    let coferRegion = 'World';
    let coferType = 'total';

    // US CPI state
    let usCpiData = null;
    let usCpiChart = null;
    let usCpiRange = '10';
    let usCpiFreq = 'monthly';
    let usCpiView = 'yoy';

    // UK CPI state
    let ukCpiData = null;
    let ukCpiChart = null;
    let ukCpiRange = '10';
    let ukCpiFreq = 'monthly';
    let ukCpiView = 'yoy';

    // Component state
    let usCpiComponents = null;
    let ukCpiComponents = null;
    let usCompChart = null;
    let ukCompChart = null;
    let usCompRange = '10';
    let usCompFreq = 'monthly';
    let usCompView = 'yoy';
    let ukCompRange = '10';
    let ukCompFreq = 'monthly';
    let ukCompView = 'yoy';
    let activeUsComponent = 'overview';
    let activeUkComponent = 'overview';

    // Forecast state
    let forecastData = null;
    let fcOilChart = null;
    let fcAgChart = null;
    let fcMetalsChart = null;
    let fcCommodityChart = null;
    let fcHistoryChart = null;
    let activeForecastGroup = null;
    let activeForecastCommodity = null;
    let fcOilScenario = 'Weighted Avg';
    let fcAgScenario = 'Weighted Avg';
    let fcMetalsScenario = 'Weighted Avg';

    // Map group keys to group names in API data
    const FORECAST_GROUP_MAP = {
        forecast_oil: 'Oil & Gas',
        forecast_ag: 'Agriculture',
        forecast_metals: 'Metals',
    };

    // Track which datasets have been fetched (lazy loading)
    const fetched = { cofer: false, us_cpi: false, uk_cpi: false, us_components: false, uk_components: false, forecasts: false };

    // Track which submenus are expanded
    const expanded = { us_cpi: false, uk_cpi: false, forecast_oil: false, forecast_ag: false, forecast_metals: false };

    // ── Bootstrap ────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', () => {
        // Sidebar parent items (COFER + CPI parents)
        document.querySelectorAll('.sidebar-item').forEach(btn => {
            btn.addEventListener('click', () => onSidebarItemClick(btn));
        });

        // Sidebar sub-items
        document.querySelectorAll('.sidebar-subitem').forEach(btn => {
            btn.addEventListener('click', () => onSubitemClick(btn));
        });

        // COFER controls
        const regionFilter = document.getElementById('region-filter');
        if (regionFilter) {
            regionFilter.addEventListener('change', (e) => {
                coferRegion = e.target.value;
                if (coferData) renderCofer();
            });
        }

        const reserveType = document.getElementById('reserve-type');
        if (reserveType) {
            reserveType.addEventListener('change', (e) => {
                coferType = e.target.value;
                if (coferData) renderCofer();
            });
        }

        // US CPI overview controls
        bindCpiControl('us-cpi-range', (v) => { usCpiRange = v; }, () => usCpiData && renderUsCpi());
        bindCpiControl('us-cpi-freq', (v) => { usCpiFreq = v; }, () => usCpiData && renderUsCpi());
        bindCpiControl('us-cpi-view', (v) => { usCpiView = v; }, () => usCpiData && renderUsCpi());

        // UK CPI overview controls
        bindCpiControl('uk-cpi-range', (v) => { ukCpiRange = v; }, () => ukCpiData && renderUkCpi());
        bindCpiControl('uk-cpi-freq', (v) => { ukCpiFreq = v; }, () => ukCpiData && renderUkCpi());
        bindCpiControl('uk-cpi-view', (v) => { ukCpiView = v; }, () => ukCpiData && renderUkCpi());

        // US component controls
        bindCpiControl('us-comp-range', (v) => { usCompRange = v; }, () => usCpiComponents && renderUsComponent());
        bindCpiControl('us-comp-freq', (v) => { usCompFreq = v; }, () => usCpiComponents && renderUsComponent());
        bindCpiControl('us-comp-view', (v) => { usCompView = v; }, () => usCpiComponents && renderUsComponent());

        // UK component controls
        bindCpiControl('uk-comp-range', (v) => { ukCompRange = v; }, () => ukCpiComponents && renderUkComponent());
        bindCpiControl('uk-comp-freq', (v) => { ukCompFreq = v; }, () => ukCpiComponents && renderUkComponent());
        bindCpiControl('uk-comp-view', (v) => { ukCompView = v; }, () => ukCpiComponents && renderUkComponent());

        // Forecast group scenario controls
        bindCpiControl('fc-oil-scenario', (v) => { fcOilScenario = v; }, () => forecastData && renderForecastGroup('forecast_oil'));
        bindCpiControl('fc-ag-scenario', (v) => { fcAgScenario = v; }, () => forecastData && renderForecastGroup('forecast_ag'));
        bindCpiControl('fc-metals-scenario', (v) => { fcMetalsScenario = v; }, () => forecastData && renderForecastGroup('forecast_metals'));

        // Load default dataset
        loadDataset('cofer');
    });

    function bindCpiControl(id, setter, renderer) {
        const el = document.getElementById(id);
        if (el) {
            el.addEventListener('change', (e) => {
                setter(e.target.value);
                renderer();
            });
        }
    }

    // ── Sidebar Click Handlers ───────────────────────────

    function onSidebarItemClick(btn) {
        const dataset = btn.dataset.dataset;
        const isForecastGroup = dataset in FORECAST_GROUP_MAP;
        const hasSubmenu = dataset === 'us_cpi' || dataset === 'uk_cpi' || isForecastGroup;

        // Highlight parent items
        document.querySelectorAll('.sidebar-item').forEach(b => {
            b.classList.toggle('active', b.dataset.dataset === dataset);
        });

        // For items with submenus, toggle expand/collapse
        if (hasSubmenu) {
            toggleSubmenu(dataset);

            if (dataset === 'us_cpi' || dataset === 'uk_cpi') {
                const activeComp = dataset === 'us_cpi' ? activeUsComponent : activeUkComponent;
                if (activeComp === 'overview') {
                    showPanel('panel-' + dataset);
                } else {
                    showPanel('panel-' + dataset + '_component');
                }
            } else if (isForecastGroup) {
                // Forecast group: show group overview or individual commodity
                if (activeForecastCommodity && activeForecastGroup === dataset) {
                    showPanel('panel-forecast_commodity');
                } else {
                    activeForecastGroup = dataset;
                    activeForecastCommodity = null;
                    showPanel('panel-' + dataset);
                }
            }

            // Collapse non-active submenus
            const allSubmenus = ['us_cpi', 'uk_cpi', 'forecast_oil', 'forecast_ag', 'forecast_metals'];
            allSubmenus.forEach(k => {
                if (k !== dataset) collapseSubmenu(k);
            });
        } else {
            // Collapse all submenus
            collapseSubmenu('us_cpi');
            collapseSubmenu('uk_cpi');
            collapseSubmenu('forecast_oil');
            collapseSubmenu('forecast_ag');
            collapseSubmenu('forecast_metals');
            showPanel('panel-' + dataset);
        }

        currentDataset = dataset;
        loadDataset(dataset);
    }

    function onSubitemClick(btn) {
        const dataset = btn.dataset.dataset;
        const component = btn.dataset.component;
        const commodity = btn.dataset.commodity;

        // Update sub-item active state within this submenu
        const submenu = document.getElementById('submenu-' + dataset);
        if (submenu) {
            submenu.querySelectorAll('.sidebar-subitem').forEach(b => {
                if (commodity !== undefined) {
                    b.classList.toggle('active', b.dataset.commodity === commodity);
                } else {
                    b.classList.toggle('active', b.dataset.component === component);
                }
            });
        }

        // Forecast sub-items
        if (dataset in FORECAST_GROUP_MAP) {
            activeForecastGroup = dataset;
            if (commodity === 'overview') {
                activeForecastCommodity = null;
                showPanel('panel-' + dataset);
                loadDataset(dataset);
            } else {
                activeForecastCommodity = commodity;
                showPanel('panel-forecast_commodity');
                loadDataset(dataset);
            }
            return;
        }

        // CPI sub-items
        if (component === 'overview') {
            if (dataset === 'us_cpi') activeUsComponent = 'overview';
            else activeUkComponent = 'overview';
            showPanel('panel-' + dataset);
            loadDataset(dataset);
        } else {
            if (dataset === 'us_cpi') {
                activeUsComponent = component;
                showPanel('panel-us_cpi_component');
                loadComponents('us');
            } else {
                activeUkComponent = component;
                showPanel('panel-uk_cpi_component');
                loadComponents('uk');
            }
        }
    }

    function toggleSubmenu(dataset) {
        const submenu = document.getElementById('submenu-' + dataset);
        const btn = document.querySelector('.sidebar-item[data-dataset="' + dataset + '"]');
        if (!submenu) return;

        expanded[dataset] = !expanded[dataset];

        if (expanded[dataset]) {
            submenu.classList.add('open');
            if (btn) btn.classList.add('expanded');
        } else {
            submenu.classList.remove('open');
            if (btn) btn.classList.remove('expanded');
        }
    }

    function collapseSubmenu(dataset) {
        const submenu = document.getElementById('submenu-' + dataset);
        const btn = document.querySelector('.sidebar-item[data-dataset="' + dataset + '"]');
        if (submenu) submenu.classList.remove('open');
        if (btn) btn.classList.remove('expanded');
        expanded[dataset] = false;
    }

    function showPanel(panelId) {
        document.querySelectorAll('.dataset-panel').forEach(panel => {
            panel.style.display = 'none';
        });
        const target = document.getElementById(panelId);
        if (target) target.style.display = '';
    }

    function loadDataset(dataset) {
        // Forecast groups all share one fetch
        if (dataset in FORECAST_GROUP_MAP) {
            if (!fetched.forecasts) {
                fetched.forecasts = true;
                fetchForecasts();
            } else if (forecastData) {
                // Data already loaded — just render
                if (activeForecastCommodity) {
                    renderForecastCommodity();
                } else {
                    renderForecastGroup(dataset);
                }
            }
            return;
        }

        if (fetched[dataset]) return;
        fetched[dataset] = true;

        switch (dataset) {
            case 'cofer':   fetchCofer(); break;
            case 'us_cpi':  fetchUsCpi(); break;
            case 'uk_cpi':  fetchUkCpi(); break;
        }
    }

    function loadComponents(prefix) {
        const key = prefix + '_components';
        if (fetched[key]) {
            // Data already loaded, just render
            if (prefix === 'us') renderUsComponent();
            else renderUkComponent();
            return;
        }
        fetched[key] = true;

        if (prefix === 'us') fetchUsComponents();
        else fetchUkComponents();
    }

    // ══════════════════════════════════════════════════════
    // DATA AGGREGATION UTILITIES
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
            .map(b => ({
                year: b.year,
                quarter: b.quarter,
                value: b.values.reduce((a, c) => a + c, 0) / b.values.length,
                date: b.date,
            }))
            .sort((a, b) => a.year - b.year || a.quarter - b.quarter);
    }

    function toYearly(points) {
        const buckets = {};
        for (const p of points) {
            if (!buckets[p.year]) buckets[p.year] = { year: p.year, values: [], date: String(p.year) };
            buckets[p.year].values.push(p.value);
        }
        return Object.values(buckets)
            .map(b => ({
                year: b.year,
                value: b.values.reduce((a, c) => a + c, 0) / b.values.length,
                date: b.date,
            }))
            .sort((a, b) => a.year - b.year);
    }

    function computeYoY(points, freq) {
        const result = [];
        for (let i = 0; i < points.length; i++) {
            const pt = points[i];
            let prev = null;

            if (freq === 'monthly') {
                prev = points.find(p => p.year === pt.year - 1 && p.month === pt.month);
            } else if (freq === 'quarterly') {
                prev = points.find(p => p.year === pt.year - 1 && p.quarter === pt.quarter);
            } else {
                prev = points.find(p => p.year === pt.year - 1);
            }

            let yoy = null;
            if (prev && prev.value !== 0) {
                yoy = ((pt.value - prev.value) / Math.abs(prev.value)) * 100;
            }
            result.push({ ...pt, yoy });
        }
        return result;
    }

    function computePoP(points) {
        const result = [];
        for (let i = 0; i < points.length; i++) {
            let pop = null;
            if (i > 0 && points[i - 1].value !== 0) {
                pop = ((points[i].value - points[i - 1].value) / Math.abs(points[i - 1].value)) * 100;
            }
            result.push({ ...points[i], pop });
        }
        return result;
    }

    function transformSeries(rawPoints, freq, view, isUs) {
        let points;
        if (freq === 'quarterly') {
            points = toQuarterly(rawPoints);
        } else if (freq === 'yearly') {
            points = toYearly(rawPoints);
        } else {
            points = rawPoints.map(p => ({ ...p }));
        }

        if (isUs) {
            if (view === 'level') {
                return points.map(p => ({ date: p.date, y: round2(p.value), year: p.year }));
            } else if (view === 'yoy') {
                const withYoY = computeYoY(points, freq);
                return withYoY.map(p => ({ date: p.date, y: p.yoy != null ? round2(p.yoy) : null, year: p.year }));
            } else {
                const withPoP = computePoP(points);
                return withPoP.map(p => ({ date: p.date, y: p.pop != null ? round2(p.pop) : null, year: p.year }));
            }
        } else {
            if (view === 'level' || view === 'yoy') {
                return points.map(p => ({ date: p.date, y: round2(p.value), year: p.year }));
            } else {
                const withPoP = computePoP(points);
                return withPoP.map(p => ({ date: p.date, y: p.pop != null ? round2(p.pop) : null, year: p.year }));
            }
        }
    }

    function round2(v) {
        return v != null ? Math.round(v * 100) / 100 : null;
    }

    function getYAxisLabel(view, isUs) {
        if (isUs && view === 'level') return 'Index';
        return '%';
    }

    function getTooltipSuffix(view, isUs) {
        if (isUs && view === 'level') return '';
        if (view === 'qoq') return ' pp';
        return '%';
    }

    /**
     * Filter + transform raw points with range/freq/view and return chart-ready data.
     */
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
    // COFER RESERVES
    // ══════════════════════════════════════════════════════

    async function fetchCofer() {
        try {
            const resp = await fetch('/api/cofer');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            coferData = await resp.json();
            document.getElementById('reserves-loading').style.display = 'none';

            const regionSelect = document.getElementById('region-filter');
            const regions = coferData.regions || [];
            regions.forEach(r => {
                if (r === 'World') return;
                const opt = document.createElement('option');
                opt.value = r;
                opt.textContent = r;
                regionSelect.appendChild(opt);
            });

            renderCofer();
        } catch (err) {
            console.error('Reserves fetch failed:', err);
            document.getElementById('reserves-loading').innerHTML =
                '<p style="color:var(--text-muted)">Failed to load reserves data.</p>';
        }
    }

    function renderCofer() {
        const years = coferData.years || [];
        let countries = coferData.countries || [];
        const regionMembers = coferData.region_members || {};

        if (coferRegion === 'World') {
            countries = countries.slice(0, 20);
        } else {
            const members = regionMembers[coferRegion] || [];
            countries = countries.filter(c => members.includes(c.iso3));
        }

        renderReservesChart(years, countries);
        renderReservesTable(years, countries);
        renderReservesMeta(coferData.meta || {}, countries.length);
    }

    function renderReservesChart(years, countries) {
        const ctx = document.getElementById('reserves-chart').getContext('2d');

        const datasets = countries.map((c, i) => {
            let values;
            if (coferType === 'fx') values = c.fx_reserves;
            else if (coferType === 'gold') values = c.gold_reserves;
            else values = c.total_reserves;

            return {
                label: c.name,
                data: values.map(v => v != null ? v : null),
                borderColor: COLORS[i % COLORS.length],
                backgroundColor: 'transparent',
                borderWidth: 2,
                fill: false,
                pointRadius: 0,
                pointHitRadius: 8,
                tension: 0.3,
            };
        });

        if (coferChart) coferChart.destroy();

        coferChart = new Chart(ctx, {
            type: 'line',
            data: { labels: years, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            color: '#9ca3af',
                            font: { size: 11 },
                            boxWidth: 12,
                            padding: 10,
                            usePointStyle: true,
                            pointStyle: 'line',
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#fff',
                        bodyColor: '#d1d5db',
                        borderColor: 'rgba(255,255,255,0.1)',
                        borderWidth: 1,
                        bodyFont: { size: 12 },
                        callbacks: {
                            label: (ctx) => {
                                const val = ctx.parsed.y;
                                if (val == null) return ctx.dataset.label + ': N/A';
                                return ctx.dataset.label + ': $' + val.toFixed(1) + 'B';
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { color: '#6b7280', font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 15 },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    },
                    y: {
                        ticks: { color: '#6b7280', font: { size: 10 }, callback: (val) => '$' + val.toLocaleString() + 'B' },
                        grid: { color: 'rgba(55,65,81,0.3)' },
                        beginAtZero: true,
                    }
                }
            }
        });
    }

    function renderReservesTable(years, countries) {
        const thead = document.getElementById('reserves-thead');
        const tbody = document.getElementById('reserves-tbody');

        let hdr = '<tr><th>Country</th>';
        for (let i = years.length - 1; i >= 0; i--) hdr += '<th>' + years[i] + '</th>';
        hdr += '</tr>';
        thead.innerHTML = hdr;

        let rows = '';
        for (const c of countries) {
            let values;
            if (coferType === 'fx') values = c.fx_reserves;
            else if (coferType === 'gold') values = c.gold_reserves;
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

    function renderReservesMeta(meta, shown) {
        const el = document.getElementById('reserves-meta');
        const parts = [];
        if (meta.source) parts.push(meta.source);
        if (meta.frequency) parts.push(meta.frequency);
        if (meta.year_range) parts.push(meta.year_range);
        parts.push(shown + ' countries shown');
        if (meta.country_count) parts.push(meta.country_count + ' total');
        el.textContent = parts.join(' \u00b7 ');
    }

    // ══════════════════════════════════════════════════════
    // US CPI (BLS) — Overview
    // ══════════════════════════════════════════════════════

    async function fetchUsCpi() {
        try {
            const resp = await fetch('/api/cpi/us');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            usCpiData = await resp.json();
            document.getElementById('us-cpi-loading').style.display = 'none';
            renderUsCpi();
        } catch (err) {
            console.error('US CPI fetch failed:', err);
            document.getElementById('us-cpi-loading').innerHTML =
                '<p style="color:var(--text-muted)">Failed to load US CPI data.</p>';
        }
    }

    function renderUsCpi() {
        renderCpiSummary('us', usCpiData, usCpiRange, usCpiFreq, usCpiView);
        renderCpiChart('us', usCpiData, usCpiRange, usCpiFreq, usCpiView, usCpiChart, (c) => { usCpiChart = c; });
        renderCpiTable('us', usCpiData, usCpiRange, usCpiFreq, usCpiView);
        renderCpiMeta('us', usCpiData, usCpiFreq, usCpiView);
    }

    // ══════════════════════════════════════════════════════
    // UK CPI (ONS) — Overview
    // ══════════════════════════════════════════════════════

    async function fetchUkCpi() {
        try {
            const resp = await fetch('/api/cpi/uk');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            ukCpiData = await resp.json();
            document.getElementById('uk-cpi-loading').style.display = 'none';
            renderUkCpi();
        } catch (err) {
            console.error('UK CPI fetch failed:', err);
            document.getElementById('uk-cpi-loading').innerHTML =
                '<p style="color:var(--text-muted)">Failed to load UK CPI data.</p>';
        }
    }

    function renderUkCpi() {
        renderCpiSummary('uk', ukCpiData, ukCpiRange, ukCpiFreq, ukCpiView);
        renderCpiChart('uk', ukCpiData, ukCpiRange, ukCpiFreq, ukCpiView, ukCpiChart, (c) => { ukCpiChart = c; });
        renderCpiTable('uk', ukCpiData, ukCpiRange, ukCpiFreq, ukCpiView);
        renderCpiMeta('uk', ukCpiData, ukCpiFreq, ukCpiView);
    }

    // ══════════════════════════════════════════════════════
    // CPI COMPONENTS
    // ══════════════════════════════════════════════════════

    async function fetchUsComponents() {
        try {
            const resp = await fetch('/api/cpi/us/components');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            usCpiComponents = await resp.json();
            document.getElementById('us-comp-loading').style.display = 'none';
            renderUsComponent();
        } catch (err) {
            console.error('US components fetch failed:', err);
            document.getElementById('us-comp-loading').innerHTML =
                '<p style="color:var(--text-muted)">Failed to load component data.</p>';
        }
    }

    async function fetchUkComponents() {
        try {
            const resp = await fetch('/api/cpi/uk/components');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            ukCpiComponents = await resp.json();
            document.getElementById('uk-comp-loading').style.display = 'none';
            renderUkComponent();
        } catch (err) {
            console.error('UK components fetch failed:', err);
            document.getElementById('uk-comp-loading').innerHTML =
                '<p style="color:var(--text-muted)">Failed to load component data.</p>';
        }
    }

    function renderUsComponent() {
        if (!usCpiComponents) return;
        const key = activeUsComponent;
        const label = (usCpiComponents.categories || {})[key] || key;
        document.getElementById('us-comp-title').textContent = 'US CPI: ' + label;
        renderCompSummary('us', usCpiComponents, key, usCompRange, usCompFreq, usCompView);
        renderComponentChart('us', usCpiComponents, key, usCompRange, usCompFreq, usCompView, usCompChart, (c) => { usCompChart = c; });
        renderComponentTable('us', usCpiComponents, key, usCompRange, usCompFreq, usCompView);
        renderComponentMeta('us', usCpiComponents, usCompFreq, usCompView, label);
    }

    function renderUkComponent() {
        if (!ukCpiComponents) return;
        const key = activeUkComponent;
        const label = (ukCpiComponents.categories || {})[key] || key;
        document.getElementById('uk-comp-title').textContent = 'UK CPI: ' + label;
        renderCompSummary('uk', ukCpiComponents, key, ukCompRange, ukCompFreq, ukCompView);
        renderComponentChart('uk', ukCpiComponents, key, ukCompRange, ukCompFreq, ukCompView, ukCompChart, (c) => { ukCompChart = c; });
        renderComponentTable('uk', ukCpiComponents, key, ukCompRange, ukCompFreq, ukCompView);
        renderComponentMeta('uk', ukCpiComponents, ukCompFreq, ukCompView, label);
    }

    function renderComponentChart(prefix, data, key, rangeVal, freq, view, existingChart, setChart) {
        const canvasEl = document.getElementById(prefix + '-comp-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const series = data.series || {};
        const colors = data.colors || {};
        const isUs = prefix === 'us';
        const rawPoints = series[key] || [];

        const transformed = prepareChartData(rawPoints, rangeVal, freq, view, isUs);

        const chartData = transformed
            .filter(d => d.y !== null && d.y !== undefined)
            .map(d => ({ x: d.date, y: d.y }));

        if (existingChart) existingChart.destroy();

        const suffix = getTooltipSuffix(view, isUs);
        const yLabel = getYAxisLabel(view, isUs);
        const color = colors[key] || COLORS[0];

        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    label: (data.categories || {})[key] || key,
                    data: chartData,
                    borderColor: color,
                    backgroundColor: color + '1A',
                    borderWidth: 2,
                    fill: true,
                    pointRadius: 0,
                    pointHitRadius: 8,
                    tension: 0.3,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#fff',
                        bodyColor: '#d1d5db',
                        callbacks: {
                            label: (ctx) => {
                                const val = ctx.parsed.y;
                                if (val == null) return 'N/A';
                                return val.toFixed(2) + suffix;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'category',
                        ticks: { color: '#6b7280', font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    },
                    y: {
                        ticks: {
                            color: '#6b7280',
                            font: { size: 10 },
                            callback: (val) => yLabel === 'Index' ? val.toFixed(1) : val.toFixed(1) + '%',
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    }
                }
            }
        });

        setChart(chart);
    }

    function renderComponentTable(prefix, data, key, rangeVal, freq, view) {
        const thead = document.getElementById(prefix + '-comp-thead');
        const tbody = document.getElementById(prefix + '-comp-tbody');
        if (!thead || !tbody) return;

        const series = data.series || {};
        const categories = data.categories || {};
        const isUs = prefix === 'us';
        const rawPoints = series[key] || [];
        const label = categories[key] || key;
        const suffix = getTooltipSuffix(view, isUs);
        const isIndex = isUs && view === 'level';

        const transformed = prepareChartData(rawPoints, rangeVal, freq, view, isUs);
        const reversedPts = transformed.slice().reverse();

        thead.innerHTML = '<tr><th>Date</th><th>' + label + '</th></tr>';

        let rows = '';
        for (const pt of reversedPts) {
            rows += '<tr><td>' + pt.date + '</td>';
            if (pt.y == null) {
                rows += '<td>--</td>';
            } else if (isIndex) {
                rows += '<td>' + pt.y.toFixed(1) + '</td>';
            } else {
                rows += '<td>' + pt.y.toFixed(2) + suffix + '</td>';
            }
            rows += '</tr>';
        }
        tbody.innerHTML = rows;
    }

    function renderComponentMeta(prefix, data, freq, view, label) {
        const el = document.getElementById(prefix + '-comp-meta');
        if (!el) return;
        const meta = data.meta || {};
        const isUs = prefix === 'us';
        const parts = [];
        if (meta.source) parts.push(meta.source);
        parts.push(label);
        const freqLabels = { monthly: 'Monthly', quarterly: 'Quarterly', yearly: 'Yearly' };
        parts.push(freqLabels[freq] || 'Monthly');
        const viewLabels = {
            yoy: 'Year-over-Year',
            qoq: freq === 'monthly' ? 'Month-over-Month' : freq === 'quarterly' ? 'Quarter-over-Quarter' : 'Year-over-Year',
            level: isUs ? 'Index Level' : 'Annual Rate',
        };
        parts.push(viewLabels[view] || '');
        if (meta.year_range) parts.push(meta.year_range);
        el.textContent = parts.join(' \u00b7 ');
    }

    // ══════════════════════════════════════════════════════
    // CPI SUMMARY CARDS (latest reading per category)
    // ══════════════════════════════════════════════════════

    function renderCpiSummary(prefix, data, rangeVal, freq, view) {
        const container = document.getElementById(prefix + '-cpi-summary');
        if (!container) return;

        const series = data.series || {};
        const categories = data.categories || {};
        const colors = data.colors || {};
        const isUs = prefix === 'us';
        const suffix = getTooltipSuffix(view, isUs);
        const isIndex = isUs && view === 'level';

        let html = '';
        for (const [key, label] of Object.entries(categories)) {
            const rawPoints = series[key] || [];
            const transformed = prepareChartData(rawPoints, rangeVal, freq, view, isUs);
            // Get last non-null point
            let latest = null;
            let prev = null;
            for (let i = transformed.length - 1; i >= 0; i--) {
                if (transformed[i].y != null) {
                    if (!latest) { latest = transformed[i]; }
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

            html += `
                <div class="cpi-summary-card" style="border-top: 3px solid ${borderColor}">
                    <div class="cpi-summary-label">${label}</div>
                    <div class="cpi-summary-value ${dirClass}">
                        ${formatted}
                        <span class="cpi-summary-arrow ${dirClass}">${arrow}</span>
                    </div>
                    <div class="cpi-summary-date">${latest.date}</div>
                </div>
            `;
        }
        container.innerHTML = html;
    }

    function renderCompSummary(prefix, data, key, rangeVal, freq, view) {
        const container = document.getElementById(prefix + '-comp-summary');
        if (!container) return;

        const series = data.series || {};
        const categories = data.categories || {};
        const colors = data.colors || {};
        const isUs = prefix === 'us';
        const suffix = getTooltipSuffix(view, isUs);
        const isIndex = isUs && view === 'level';

        const rawPoints = series[key] || [];
        const transformed = prepareChartData(rawPoints, rangeVal, freq, view, isUs);
        const label = categories[key] || key;
        const borderColor = colors[key] || '#3b82f6';

        let latest = null;
        let prev = null;
        for (let i = transformed.length - 1; i >= 0; i--) {
            if (transformed[i].y != null) {
                if (!latest) { latest = transformed[i]; }
                else if (!prev) { prev = transformed[i]; break; }
            }
        }

        if (!latest) {
            container.innerHTML = '';
            return;
        }

        const val = latest.y;
        const dir = prev ? (val > prev.y ? 'up' : val < prev.y ? 'down' : 'flat') : 'flat';
        const arrow = dir === 'up' ? '&#9650;' : dir === 'down' ? '&#9660;' : '';
        const dirClass = dir === 'up' ? 'summary-up' : dir === 'down' ? 'summary-down' : '';
        const formatted = isIndex ? val.toFixed(1) : val.toFixed(2) + suffix;

        container.innerHTML = `
            <div class="cpi-summary-card" style="border-top: 3px solid ${borderColor}">
                <div class="cpi-summary-label">${label}</div>
                <div class="cpi-summary-value ${dirClass}">
                    ${formatted}
                    <span class="cpi-summary-arrow ${dirClass}">${arrow}</span>
                </div>
                <div class="cpi-summary-date">${latest.date}</div>
            </div>
        `;
    }

    // ══════════════════════════════════════════════════════
    // SHARED CPI OVERVIEW RENDERING
    // ══════════════════════════════════════════════════════

    function renderCpiChart(prefix, data, rangeVal, freq, view, existingChart, setChart) {
        const canvasEl = document.getElementById(prefix + '-cpi-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const series = data.series || {};
        const categories = data.categories || {};
        const colors = data.colors || {};
        const isUs = prefix === 'us';

        const datasets = Object.entries(categories).map(([key, label]) => {
            const rawPoints = series[key] || [];
            const transformed = prepareChartData(rawPoints, rangeVal, freq, view, isUs);

            return {
                label: label,
                data: transformed
                    .filter(d => d.y !== null && d.y !== undefined)
                    .map(d => ({ x: d.date, y: d.y })),
                borderColor: colors[key] || COLORS[0],
                backgroundColor: 'transparent',
                borderWidth: 2,
                fill: false,
                pointRadius: 0,
                pointHitRadius: 8,
                tension: 0.3,
            };
        });

        if (existingChart) existingChart.destroy();

        const suffix = getTooltipSuffix(view, isUs);
        const yLabel = getYAxisLabel(view, isUs);

        const chart = new Chart(ctx, {
            type: 'line',
            data: { datasets },
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
                        callbacks: {
                            label: (ctx) => {
                                const val = ctx.parsed.y;
                                if (val == null) return ctx.dataset.label + ': N/A';
                                return ctx.dataset.label + ': ' + val.toFixed(2) + suffix;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'category',
                        ticks: { color: '#6b7280', font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    },
                    y: {
                        ticks: {
                            color: '#6b7280',
                            font: { size: 10 },
                            callback: (val) => yLabel === 'Index' ? val.toFixed(1) : val.toFixed(1) + '%',
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    }
                }
            }
        });

        setChart(chart);
    }

    function renderCpiTable(prefix, data, rangeVal, freq, view) {
        const thead = document.getElementById(prefix + '-cpi-thead');
        const tbody = document.getElementById(prefix + '-cpi-tbody');
        if (!thead || !tbody) return;

        const series = data.series || {};
        const categories = data.categories || {};
        const isUs = prefix === 'us';
        const suffix = getTooltipSuffix(view, isUs);
        const isIndex = isUs && view === 'level';

        const transformed = {};
        Object.keys(categories).forEach(key => {
            transformed[key] = prepareChartData(series[key] || [], rangeVal, freq, view, isUs);
        });

        const firstKey = Object.keys(categories)[0];
        const dates = (transformed[firstKey] || []).map(p => p.date);
        const reversedDates = dates.slice().reverse();

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

    function renderCpiMeta(prefix, data, freq, view) {
        const el = document.getElementById(prefix + '-cpi-meta');
        if (!el) return;
        const meta = data.meta || {};
        const isUs = prefix === 'us';
        const parts = [];
        if (meta.source) parts.push(meta.source);

        const freqLabels = { monthly: 'Monthly', quarterly: 'Quarterly', yearly: 'Yearly' };
        parts.push(freqLabels[freq] || 'Monthly');

        const viewLabels = {
            yoy: 'Year-over-Year',
            qoq: freq === 'monthly' ? 'Month-over-Month' : freq === 'quarterly' ? 'Quarter-over-Quarter' : 'Year-over-Year',
            level: isUs ? 'Index Level' : 'Annual Rate',
        };
        parts.push(viewLabels[view] || '');

        if (meta.year_range) parts.push(meta.year_range);
        el.textContent = parts.join(' \u00b7 ');
    }

    // ══════════════════════════════════════════════════════
    // COMMODITIES FORECASTS
    // ══════════════════════════════════════════════════════

    function populateScenarioDropdowns() {
        if (!forecastData) return;
        const groupMapping = {
            'fc-oil-scenario': 'Oil & Gas',
            'fc-ag-scenario': 'Agriculture',
            'fc-metals-scenario': 'Metals',
        };

        Object.entries(groupMapping).forEach(([selectId, groupName]) => {
            const sel = document.getElementById(selectId);
            if (!sel) return;
            const group = (forecastData.groups || {})[groupName];
            if (!group) return;

            const order = group.scenario_order || [];
            // Clear existing options
            sel.innerHTML = '';
            // Add Weighted Avg first (default)
            const waOpt = document.createElement('option');
            waOpt.value = 'Weighted Avg';
            waOpt.textContent = 'Weighted Avg';
            waOpt.selected = true;
            sel.appendChild(waOpt);
            // Add the non-meta scenarios (skip Actual and Weighted Avg)
            order.forEach(sc => {
                if (sc === 'Actual' || sc === 'Weighted Avg') return;
                const opt = document.createElement('option');
                opt.value = sc;
                opt.textContent = sc;
                sel.appendChild(opt);
            });
        });
    }

    async function fetchForecasts() {
        try {
            const resp = await fetch('/api/forecasts');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            forecastData = await resp.json();

            // Hide all forecast loading spinners
            ['fc-oil-loading', 'fc-ag-loading', 'fc-metals-loading', 'fc-commodity-loading'].forEach(id => {
                const el = document.getElementById(id);
                if (el) el.style.display = 'none';
            });

            // Populate scenario dropdowns dynamically from per-group configs
            populateScenarioDropdowns();

            // Render the active forecast view
            if (activeForecastCommodity) {
                renderForecastCommodity();
            } else if (activeForecastGroup) {
                renderForecastGroup(activeForecastGroup);
            }
        } catch (err) {
            console.error('Forecast fetch failed:', err);
            ['fc-oil-loading', 'fc-ag-loading', 'fc-metals-loading', 'fc-commodity-loading'].forEach(id => {
                const el = document.getElementById(id);
                if (el) el.innerHTML = '<p style="color:var(--text-muted)">Failed to load forecast data.</p>';
            });
        }
    }

    function getGroupScenario(groupKey) {
        if (groupKey === 'forecast_oil') return fcOilScenario;
        if (groupKey === 'forecast_ag') return fcAgScenario;
        if (groupKey === 'forecast_metals') return fcMetalsScenario;
        return 'Weighted Avg';
    }

    function getGroupChartRef(groupKey) {
        if (groupKey === 'forecast_oil') return fcOilChart;
        if (groupKey === 'forecast_ag') return fcAgChart;
        if (groupKey === 'forecast_metals') return fcMetalsChart;
        return null;
    }

    function setGroupChartRef(groupKey, chart) {
        if (groupKey === 'forecast_oil') fcOilChart = chart;
        else if (groupKey === 'forecast_ag') fcAgChart = chart;
        else if (groupKey === 'forecast_metals') fcMetalsChart = chart;
    }

    function renderForecastGroup(groupKey) {
        if (!forecastData) return;
        const groupName = FORECAST_GROUP_MAP[groupKey];
        const group = (forecastData.groups || {})[groupName];
        if (!group) return;

        const scenario = getGroupScenario(groupKey);
        const prefix = groupKey.replace('forecast_', 'fc-');
        const commodities = group.commodities || {};
        const groupColors = group.colors || {};

        // Dynamic labels from API
        const timeCtx = forecastData.time_context || {};
        const labels = timeCtx.labels || ['Q1', 'Q2', 'Q3', 'Q4'];
        const labelTypes = timeCtx.label_types || [];
        const yearEndLabel = timeCtx.year_end_label || 'FY Avg';

        // ── Chart ──
        const canvasEl = document.getElementById(prefix + '-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const existingChart = getGroupChartRef(groupKey);
        if (existingChart) existingChart.destroy();

        // Find the boundary between actual/current and forecast
        const forecastStartIdx = labelTypes.findIndex(t => t === 'forecast');

        const datasets = Object.entries(commodities).map(([name, info]) => {
            const scenData = (info.scenarios || {})[scenario] || {};
            const color = groupColors[name] || COLORS[0];
            return {
                label: name,
                data: labels.map(l => scenData[l] != null ? scenData[l] : null),
                borderColor: color,
                backgroundColor: color + '1A',
                borderWidth: 2.5,
                fill: false,
                pointRadius: 5,
                pointBackgroundColor: color,
                pointBorderColor: color,
                tension: 0.3,
                segment: forecastStartIdx > 0 ? {
                    borderDash: ctx2 => ctx2.p0DataIndex >= forecastStartIdx - 1 ? [4, 3] : [],
                } : undefined,
            };
        });

        const chart = new Chart(ctx, {
            type: 'line',
            data: { labels: labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: { color: '#9ca3af', font: { size: 11 }, boxWidth: 12, padding: 10, usePointStyle: true, pointStyle: 'circle' }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#fff',
                        bodyColor: '#d1d5db',
                        callbacks: {
                            label: (ctx) => {
                                const val = ctx.parsed.y;
                                if (val == null) return ctx.dataset.label + ': N/A';
                                const name = ctx.dataset.label;
                                const info = commodities[name];
                                const unit = info ? info.unit : '';
                                return name + ': ' + val.toFixed(2) + ' ' + unit;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: {
                            color: (ctx) => {
                                const idx = ctx.index;
                                if (labelTypes[idx] === 'forecast') return '#6b7280';
                                if (labelTypes[idx] === 'current_q') return '#f59e0b';
                                return '#9ca3af';
                            },
                            font: { size: 12, weight: 'bold' },
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    },
                    y: {
                        ticks: { color: '#6b7280', font: { size: 10 } },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    }
                }
            }
        });

        setGroupChartRef(groupKey, chart);

        // ── Table ──
        const thead = document.getElementById(prefix + '-thead');
        const tbody = document.getElementById(prefix + '-tbody');
        if (thead && tbody) {
            let hdr = '<tr><th>Commodity</th><th>Unit</th>';
            labels.forEach((l, i) => {
                const cls = labelTypes[i] === 'forecast' ? ' class="col-forecast"' : labelTypes[i] === 'current_q' ? ' class="col-current"' : ' class="col-actual"';
                hdr += '<th' + cls + '>' + l + '</th>';
            });
            hdr += '<th>' + yearEndLabel + '</th></tr>';
            thead.innerHTML = hdr;

            let rows = '';
            Object.entries(commodities).forEach(([name, info]) => {
                const scenData = (info.scenarios || {})[scenario] || {};
                rows += '<tr><td>' + name + '</td>';
                rows += '<td>' + (info.unit || '') + '</td>';
                labels.forEach((l, i) => {
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

        // ── Meta ──
        const metaEl = document.getElementById(prefix + '-meta');
        if (metaEl) {
            const meta = forecastData.meta || {};
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
    }

    function renderForecastCommodity() {
        if (!forecastData || !activeForecastCommodity || !activeForecastGroup) return;

        const groupName = FORECAST_GROUP_MAP[activeForecastGroup];
        const group = (forecastData.groups || {})[groupName];
        if (!group) return;

        const info = (group.commodities || {})[activeForecastCommodity];
        if (!info) return;

        const scenarios = info.scenarios || {};
        const scenarioColors = group.scenario_colors || {};
        const scenarioLabels = group.scenario_labels || {};
        const scenarioOrder = group.scenario_order || ['Actual', 'Base Case', 'Severe Case', 'Worst Case', 'Weighted Avg'];

        // Dynamic labels from API
        const timeCtx = forecastData.time_context || {};
        const labels = timeCtx.labels || ['Q1', 'Q2', 'Q3', 'Q4'];
        const labelTypes = timeCtx.label_types || [];
        const yearEndLabel = timeCtx.year_end_label || 'FY Avg';
        const forecastYear = forecastData.forecast_year || new Date().getFullYear();

        // Find the boundary between actual/current and forecast
        const forecastStartIdx = labelTypes.findIndex(t => t === 'forecast');

        // Update title
        const titleEl = document.getElementById('fc-commodity-title');
        if (titleEl) titleEl.textContent = activeForecastCommodity + ' \u2014 Scenario Forecast';

        const sourceEl = document.getElementById('fc-commodity-source');
        if (sourceEl) {
            const latestClose = info.latest_close ? ' \u00b7 Last: ' + info.latest_close.toFixed(2) + ' ' + (info.unit || '') : '';
            sourceEl.textContent = 'ParraMacro \u2014 ' + (info.unit || '') + ' \u00b7 ' + forecastYear + latestClose;
        }

        // ── Chart ──
        const canvasEl = document.getElementById('fc-commodity-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        if (fcCommodityChart) fcCommodityChart.destroy();

        // Determine the "base" scenario (middle scenario that gets fill)
        const baseScenario = scenarioOrder.find(sc => sc === 'Base Case' || sc === 'Base') || scenarioOrder[1];

        const datasets = scenarioOrder
            .filter(sc => scenarios[sc] && sc !== 'Actual')
            .map(sc => {
                const scenData = scenarios[sc];
                const color = scenarioColors[sc] || '#9ca3af';
                const isDashed = sc === 'Weighted Avg';
                const isBase = sc === baseScenario;
                return {
                    label: sc,
                    data: labels.map(l => scenData[l] != null ? scenData[l] : null),
                    borderColor: color,
                    backgroundColor: isBase ? color + '1A' : 'transparent',
                    borderWidth: sc === 'Weighted Avg' ? 3 : 2,
                    borderDash: isDashed ? [6, 3] : [],
                    fill: isBase,
                    pointRadius: 5,
                    pointBackgroundColor: color,
                    pointBorderColor: color,
                    tension: 0.3,
                };
            });

        fcCommodityChart = new Chart(ctx, {
            type: 'line',
            data: { labels: labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: { color: '#9ca3af', font: { size: 11 }, boxWidth: 12, padding: 10, usePointStyle: true, pointStyle: 'circle' }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#fff',
                        bodyColor: '#d1d5db',
                        callbacks: {
                            label: (ctx) => {
                                const val = ctx.parsed.y;
                                if (val == null) return ctx.dataset.label + ': N/A';
                                return ctx.dataset.label + ': ' + val.toFixed(2) + ' ' + (info.unit || '');
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: {
                            color: (ctx) => {
                                const idx = ctx.index;
                                if (labelTypes[idx] === 'forecast') return '#6b7280';
                                if (labelTypes[idx] === 'current_q') return '#f59e0b';
                                return '#9ca3af';
                            },
                            font: { size: 12, weight: 'bold' },
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    },
                    y: {
                        ticks: { color: '#6b7280', font: { size: 10 } },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    }
                }
            }
        });

        // ── Table ──
        const thead = document.getElementById('fc-commodity-thead');
        const tbody = document.getElementById('fc-commodity-tbody');
        if (thead && tbody) {
            let hdr = '<tr><th>Scenario</th>';
            labels.forEach((l, i) => {
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
                labels.forEach((l, i) => {
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

        // ── Meta: Scenario narratives ──
        const metaEl = document.getElementById('fc-commodity-meta');
        if (metaEl) {
            const weights = group.scenario_weights || {};
            const parts = [];
            // Use all weighted scenarios (exclude Actual and Weighted Avg)
            scenarioOrder.filter(sc => sc !== 'Actual' && sc !== 'Weighted Avg').forEach(sc => {
                const w = weights[sc] ? (weights[sc] * 100).toFixed(0) + '%' : '';
                const label = scenarioLabels[sc] || '';
                if (label) parts.push(sc + ' (' + w + '): ' + label);
            });
            metaEl.textContent = parts.join(' \u00b7 ');
        }

        // ── Historical Data ──
        renderForecastHistory(info);
    }

    function renderForecastHistory(info) {
        const historical = info.historical || [];
        if (historical.length === 0) return;

        const unit = info.unit || '';

        // ── Historical Chart ──
        const histCanvasEl = document.getElementById('fc-history-chart');
        if (histCanvasEl) {
            const histCtx = histCanvasEl.getContext('2d');
            if (fcHistoryChart) fcHistoryChart.destroy();

            const histLabels = historical.map(h => h.label);
            const histValues = historical.map(h => h.avg_price);

            fcHistoryChart = new Chart(histCtx, {
                type: 'line',
                data: {
                    labels: histLabels,
                    datasets: [{
                        label: activeForecastCommodity + ' (Quarterly Avg)',
                        data: histValues,
                        borderColor: '#3b82f6',
                        backgroundColor: '#3b82f61A',
                        borderWidth: 2,
                        fill: true,
                        pointRadius: 0,
                        pointHitRadius: 8,
                        tension: 0.3,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: 'rgba(0,0,0,0.9)',
                            titleColor: '#fff',
                            bodyColor: '#d1d5db',
                            callbacks: {
                                label: (ctx) => {
                                    const val = ctx.parsed.y;
                                    if (val == null) return 'N/A';
                                    return val.toFixed(2) + ' ' + unit;
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { color: '#6b7280', font: { size: 10 }, maxRotation: 45, autoSkip: true, maxTicksLimit: 20 },
                            grid: { color: 'rgba(55,65,81,0.3)' }
                        },
                        y: {
                            ticks: { color: '#6b7280', font: { size: 10 } },
                            grid: { color: 'rgba(55,65,81,0.3)' }
                        }
                    }
                }
            });

            // Show the history section
            const histSection = document.getElementById('fc-history-section');
            if (histSection) histSection.style.display = '';
        }

        // ── Historical Table ──
        const histThead = document.getElementById('fc-history-thead');
        const histTbody = document.getElementById('fc-history-tbody');
        if (histThead && histTbody) {
            histThead.innerHTML = '<tr><th>Period</th><th>Avg Price (' + unit + ')</th></tr>';

            let rows = '';
            // Reverse: most recent first
            for (let i = historical.length - 1; i >= 0; i--) {
                const rec = historical[i];
                rows += '<tr><td>' + rec.label + '</td>';
                rows += '<td>' + rec.avg_price.toFixed(2) + '</td></tr>';
            }
            histTbody.innerHTML = rows;
        }
    }

})();
