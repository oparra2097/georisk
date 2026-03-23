/* ===== Data Page: Central Bank Reserves ===== */

(function () {
    'use strict';

    const COLORS = [
        '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#ec4899',
        '#8b5cf6', '#f97316', '#06b6d4', '#84cc16', '#e11d48',
        '#6366f1', '#14b8a6', '#f43f5e', '#a855f7', '#22c55e',
        '#eab308', '#0ea5e9', '#d946ef', '#64748b', '#fb923c',
    ];

    let rawData = null;
    let chart = null;
    let currentRegion = 'World';
    let currentType = 'total'; // 'total', 'fx', 'gold'

    // ── Bootstrap ──────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', () => {
        fetchData();

        document.getElementById('region-filter').addEventListener('change', (e) => {
            currentRegion = e.target.value;
            if (rawData) render();
        });

        document.getElementById('reserve-type').addEventListener('change', (e) => {
            currentType = e.target.value;
            if (rawData) render();
        });
    });

    // ── Fetch ──────────────────────────────────────────────
    async function fetchData() {
        try {
            const resp = await fetch('/api/cofer');
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            rawData = await resp.json();
            document.getElementById('reserves-loading').style.display = 'none';

            // Populate region dropdown
            const regionSelect = document.getElementById('region-filter');
            const regions = rawData.regions || [];
            regions.forEach(r => {
                if (r === 'World') return; // already default option
                const opt = document.createElement('option');
                opt.value = r;
                opt.textContent = r;
                regionSelect.appendChild(opt);
            });

            render();
        } catch (err) {
            console.error('Reserves fetch failed:', err);
            document.getElementById('reserves-loading').innerHTML =
                '<p style="color:var(--text-muted)">Failed to load reserves data. Try refreshing.</p>';
        }
    }

    // ── Filter + Render ───────────────────────────────────
    function render() {
        const years = rawData.years || [];
        let countries = rawData.countries || [];
        const regionMembers = rawData.region_members || {};

        // Filter by region
        if (currentRegion === 'World') {
            countries = countries.slice(0, 20); // top 20 by reserves
        } else {
            const members = regionMembers[currentRegion] || [];
            countries = countries.filter(c => members.includes(c.iso3));
        }

        renderChart(years, countries);
        renderTable(years, countries);
        renderMeta(rawData.meta || {}, countries.length);
    }

    // ── Chart ─────────────────────────────────────────────
    function renderChart(years, countries) {
        const ctx = document.getElementById('reserves-chart').getContext('2d');

        const datasets = countries.map((c, i) => {
            let values;
            if (currentType === 'fx') values = c.fx_reserves;
            else if (currentType === 'gold') values = c.gold_reserves;
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

        if (chart) chart.destroy();

        chart = new Chart(ctx, {
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
                                if (val == null) return `${ctx.dataset.label}: N/A`;
                                return `${ctx.dataset.label}: $${val.toFixed(1)}B`;
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

    // ── Table ─────────────────────────────────────────────
    function renderTable(years, countries) {
        const thead = document.getElementById('reserves-thead');
        const tbody = document.getElementById('reserves-tbody');

        // Header: Country + years (most recent first)
        let hdr = '<tr><th>Country</th>';
        for (let i = years.length - 1; i >= 0; i--) {
            hdr += `<th>${years[i]}</th>`;
        }
        hdr += '</tr>';
        thead.innerHTML = hdr;

        // Rows
        let rows = '';
        for (const c of countries) {
            let values;
            if (currentType === 'fx') values = c.fx_reserves;
            else if (currentType === 'gold') values = c.gold_reserves;
            else values = c.total_reserves;

            rows += `<tr><td>${c.name}</td>`;
            for (let i = years.length - 1; i >= 0; i--) {
                const v = values[i];
                if (v == null) {
                    rows += '<td>—</td>';
                } else {
                    rows += `<td>$${v.toFixed(1)}B</td>`;
                }
            }
            rows += '</tr>';
        }
        tbody.innerHTML = rows;
    }

    // ── Meta ──────────────────────────────────────────────
    function renderMeta(meta, shown) {
        const el = document.getElementById('reserves-meta');
        const parts = [];
        if (meta.source) parts.push(meta.source);
        if (meta.frequency) parts.push(meta.frequency);
        if (meta.year_range) parts.push(meta.year_range);
        parts.push(`${shown} countries shown`);
        if (meta.country_count) parts.push(`${meta.country_count} total`);
        el.textContent = parts.join(' · ');
    }

})();
