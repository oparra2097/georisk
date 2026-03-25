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

    // Track which datasets have been fetched (lazy loading)
    const fetched = { cofer: false, us_cpi: false, uk_cpi: false };

    // ── Bootstrap ────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', () => {
        // Sidebar navigation
        document.querySelectorAll('.sidebar-item').forEach(btn => {
            btn.addEventListener('click', () => switchDataset(btn.dataset.dataset));
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

        // US CPI controls
        bindCpiControl('us-cpi-range', (v) => { usCpiRange = v; }, () => usCpiData && renderUsCpi());
        bindCpiControl('us-cpi-freq', (v) => { usCpiFreq = v; }, () => usCpiData && renderUsCpi());
        bindCpiControl('us-cpi-view', (v) => { usCpiView = v; }, () => usCpiData && renderUsCpi());

        // UK CPI controls
        bindCpiControl('uk-cpi-range', (v) => { ukCpiRange = v; }, () => ukCpiData && renderUkCpi());
        bindCpiControl('uk-cpi-freq', (v) => { ukCpiFreq = v; }, () => ukCpiData && renderUkCpi());
        bindCpiControl('uk-cpi-view', (v) => { ukCpiView = v; }, () => ukCpiData && renderUkCpi());

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

    // ── Dataset Switching ────────────────────────────────
    function switchDataset(dataset) {
        if (dataset === currentDataset) return;
        currentDataset = dataset;

        // Update sidebar active state
        document.querySelectorAll('.sidebar-item').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.dataset === dataset);
        });

        // Show/hide panels
        document.querySelectorAll('.dataset-panel').forEach(panel => {
            panel.style.display = 'none';
        });
        const targetPanel = document.getElementById('panel-' + dataset);
        if (targetPanel) targetPanel.style.display = '';

        // Lazy-load data if not yet fetched
        loadDataset(dataset);
    }

    function loadDataset(dataset) {
        if (fetched[dataset]) return;
        fetched[dataset] = true;

        switch (dataset) {
            case 'cofer':   fetchCofer(); break;
            case 'us_cpi':  fetchUsCpi(); break;
            case 'uk_cpi':  fetchUkCpi(); break;
        }
    }

    // ══════════════════════════════════════════════════════
    // DATA AGGREGATION UTILITIES
    // ══════════════════════════════════════════════════════

    /**
     * Aggregate monthly points to quarterly by averaging values within each quarter.
     * Input: [{year, month, value, ...}, ...]
     * Output: [{year, quarter, value, date}, ...]
     */
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

    /**
     * Aggregate monthly points to yearly by averaging values within each year.
     * Input: [{year, month, value, ...}, ...]
     * Output: [{year, value, date}, ...]
     */
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

    /**
     * Compute YoY % change on aggregated data.
     * For monthly: compare same month prior year.
     * For quarterly: compare same quarter prior year.
     * For yearly: compare prior year.
     */
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

    /**
     * Compute period-over-period % change (MoM, QoQ, or YoY for yearly).
     * Compares each point to the immediately preceding point.
     */
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

    /**
     * Transform raw monthly series based on frequency and view mode.
     *
     * For BLS (isUs=true): raw points have index-level `value` field.
     *   - level: show the index value
     *   - yoy: show YoY % change of the index
     *   - qoq: show period-over-period % change
     *
     * For ONS (isUs=false): raw points have `value` which IS already a YoY annual rate.
     *   - level: show the raw rate (which is already YoY %)
     *   - yoy: same as level (the value IS the YoY rate)
     *   - qoq: period-over-period change in the rate (change in % points)
     */
    function transformSeries(rawPoints, freq, view, isUs) {
        // Step 1: Aggregate to desired frequency
        let points;
        if (freq === 'quarterly') {
            points = toQuarterly(rawPoints);
        } else if (freq === 'yearly') {
            points = toYearly(rawPoints);
        } else {
            // Monthly — keep original, but normalize shape
            points = rawPoints.map(p => ({ ...p }));
        }

        // Step 2: Compute the display value based on view mode
        if (isUs) {
            // BLS: value is a raw index level
            if (view === 'level') {
                // Show raw index, no transformation
                return points.map(p => ({ date: p.date, y: round2(p.value), year: p.year }));
            } else if (view === 'yoy') {
                const withYoY = computeYoY(points, freq);
                return withYoY.map(p => ({ date: p.date, y: p.yoy != null ? round2(p.yoy) : null, year: p.year }));
            } else {
                // qoq = period-over-period
                const withPoP = computePoP(points);
                return withPoP.map(p => ({ date: p.date, y: p.pop != null ? round2(p.pop) : null, year: p.year }));
            }
        } else {
            // ONS: value is already a YoY annual rate
            if (view === 'level' || view === 'yoy') {
                // Show the rate directly
                return points.map(p => ({ date: p.date, y: round2(p.value), year: p.year }));
            } else {
                // qoq = change in the rate (percentage point change)
                const withPoP = computePoP(points);
                return withPoP.map(p => ({ date: p.date, y: p.pop != null ? round2(p.pop) : null, year: p.year }));
            }
        }
    }

    function round2(v) {
        return v != null ? Math.round(v * 100) / 100 : null;
    }

    /**
     * Get the Y-axis label based on view mode and data source.
     */
    function getYAxisLabel(view, isUs) {
        if (isUs && view === 'level') return 'Index';
        return '%';
    }

    /**
     * Get the tooltip suffix based on view mode and data source.
     */
    function getTooltipSuffix(view, isUs) {
        if (isUs && view === 'level') return '';
        if (view === 'qoq') return ' pp';
        return '%';
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
                        ticks: {
                            color: '#6b7280',
                            font: { size: 10 },
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 15,
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    },
                    y: {
                        ticks: {
                            color: '#6b7280',
                            font: { size: 10 },
                            callback: (val) => '$' + val.toLocaleString() + 'B',
                        },
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
        for (let i = years.length - 1; i >= 0; i--) {
            hdr += '<th>' + years[i] + '</th>';
        }
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
                if (v == null) {
                    rows += '<td>\u2014</td>';
                } else {
                    rows += '<td>$' + v.toFixed(1) + 'B</td>';
                }
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
    // US CPI (BLS)
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
        renderCpiChart('us', usCpiData, usCpiRange, usCpiFreq, usCpiView, usCpiChart, (c) => { usCpiChart = c; });
        renderCpiTable('us', usCpiData, usCpiRange, usCpiFreq, usCpiView);
        renderCpiMeta('us', usCpiData, usCpiFreq, usCpiView);
    }

    // ══════════════════════════════════════════════════════
    // UK CPI (ONS)
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
        renderCpiChart('uk', ukCpiData, ukCpiRange, ukCpiFreq, ukCpiView, ukCpiChart, (c) => { ukCpiChart = c; });
        renderCpiTable('uk', ukCpiData, ukCpiRange, ukCpiFreq, ukCpiView);
        renderCpiMeta('uk', ukCpiData, ukCpiFreq, ukCpiView);
    }

    // ══════════════════════════════════════════════════════
    // SHARED CPI RENDERING
    // ══════════════════════════════════════════════════════

    function renderCpiChart(prefix, data, rangeVal, freq, view, existingChart, setChart) {
        const canvasEl = document.getElementById(prefix + '-cpi-chart');
        if (!canvasEl) return;
        const ctx = canvasEl.getContext('2d');

        const series = data.series || {};
        const categories = data.categories || {};
        const colors = data.colors || {};
        const currentYear = new Date().getFullYear();
        const isUs = prefix === 'us';

        const datasets = Object.entries(categories).map(([key, label]) => {
            const rawPoints = series[key] || [];

            // Filter by time range first (on raw monthly data)
            let filtered = rawPoints;
            if (rangeVal !== 'all') {
                const minYear = currentYear - parseInt(rangeVal);
                // Include one extra year before for YoY computation
                const computeMinYear = minYear - 1;
                filtered = rawPoints.filter(p => p.year >= computeMinYear);
            }

            // Transform: aggregate + compute view values
            let transformed = transformSeries(filtered, freq, view, isUs);

            // Now trim to actual display range (remove the extra year)
            if (rangeVal !== 'all') {
                const minYear = currentYear - parseInt(rangeVal);
                transformed = transformed.filter(p => p.year >= minYear);
            }

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
                        ticks: {
                            color: '#6b7280',
                            font: { size: 10 },
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 12,
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    },
                    y: {
                        ticks: {
                            color: '#6b7280',
                            font: { size: 10 },
                            callback: (val) => {
                                if (yLabel === 'Index') return val.toFixed(1);
                                return val.toFixed(1) + '%';
                            },
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
        const currentYear = new Date().getFullYear();
        const suffix = getTooltipSuffix(view, isUs);
        const isIndex = isUs && view === 'level';

        // Transform all series
        const transformed = {};
        Object.keys(categories).forEach(key => {
            const rawPoints = series[key] || [];
            let filtered = rawPoints;
            if (rangeVal !== 'all') {
                const minYear = currentYear - parseInt(rangeVal);
                const computeMinYear = minYear - 1;
                filtered = rawPoints.filter(p => p.year >= computeMinYear);
            }
            let pts = transformSeries(filtered, freq, view, isUs);
            if (rangeVal !== 'all') {
                const minYear = currentYear - parseInt(rangeVal);
                pts = pts.filter(p => p.year >= minYear);
            }
            transformed[key] = pts;
        });

        // Get date list from first category
        const firstKey = Object.keys(categories)[0];
        const dates = (transformed[firstKey] || []).map(p => p.date);
        const reversedDates = dates.slice().reverse();

        // Header
        let hdr = '<tr><th>Date</th>';
        Object.values(categories).forEach(label => {
            hdr += '<th>' + label + '</th>';
        });
        hdr += '</tr>';
        thead.innerHTML = hdr;

        // Build lookups
        const lookups = {};
        Object.keys(categories).forEach(key => {
            lookups[key] = {};
            (transformed[key] || []).forEach(p => {
                lookups[key][p.date] = p.y;
            });
        });

        // Rows
        let rows = '';
        for (const date of reversedDates) {
            rows += '<tr><td>' + date + '</td>';
            Object.keys(categories).forEach(key => {
                const val = lookups[key][date];
                if (val == null) {
                    rows += '<td>--</td>';
                } else if (isIndex) {
                    rows += '<td>' + val.toFixed(1) + '</td>';
                } else {
                    rows += '<td>' + val.toFixed(2) + suffix + '</td>';
                }
            });
            rows += '</tr>';
        }
        tbody.innerHTML = rows;
    }

    function renderCpiMeta(prefix, data, freq, view) {
        const el = document.getElementById(prefix + '-cpi-meta');
        if (!el) return;
        const meta = data.meta || {};
        const parts = [];
        if (meta.source) parts.push(meta.source);

        // Show current frequency
        const freqLabels = { monthly: 'Monthly', quarterly: 'Quarterly', yearly: 'Yearly' };
        parts.push(freqLabels[freq] || 'Monthly');

        // Show current view
        const isUs = prefix === 'us';
        const viewLabels = {
            yoy: 'Year-over-Year',
            qoq: freq === 'monthly' ? 'Month-over-Month' : freq === 'quarterly' ? 'Quarter-over-Quarter' : 'Year-over-Year',
            level: isUs ? 'Index Level' : 'Annual Rate',
        };
        parts.push(viewLabels[view] || '');

        if (meta.year_range) parts.push(meta.year_range);
        el.textContent = parts.join(' \u00b7 ');
    }

})();
