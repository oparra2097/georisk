/* ===== Data Page: COFER Chart + Table ===== */

(function () {
    'use strict';

    let coferData = null;
    let coferChart = null;
    let currentView = 'share'; // 'share' or 'amount'

    // ── Bootstrap ──────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', () => {
        fetchCOFER();
        document.getElementById('cofer-view').addEventListener('change', (e) => {
            currentView = e.target.value;
            if (coferData) render(coferData);
        });
    });

    // ── Fetch ──────────────────────────────────────────────
    async function fetchCOFER() {
        try {
            const resp = await fetch('/api/cofer');
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            coferData = await resp.json();
            document.getElementById('cofer-loading').style.display = 'none';
            render(coferData);
        } catch (err) {
            console.error('COFER fetch failed:', err);
            document.getElementById('cofer-loading').innerHTML =
                '<p style="color:var(--text-muted)">Failed to load COFER data. Try refreshing.</p>';
        }
    }

    // ── Render both chart + table ─────────────────────────
    function render(data) {
        const quarters = data.quarters || [];
        const currencies = data.currencies || [];

        // Filter to current view type
        const series = currencies.filter(c => c.type === currentView);

        // If no series match the current view, try showing all
        const displaySeries = series.length > 0 ? series : currencies;

        renderChart(quarters, displaySeries);
        renderTable(quarters, displaySeries);
        renderMeta(data.meta || {}, quarters.length, displaySeries.length);
    }

    // ── Chart (stacked area) ──────────────────────────────
    function renderChart(quarters, series) {
        const ctx = document.getElementById('cofer-chart').getContext('2d');

        // Format quarter labels: "1999-Q1" → "Q1 '99"
        const labels = quarters.map(q => {
            const parts = q.split('-');
            if (parts.length === 1 && parts[0].length === 4) return parts[0]; // year only
            const year = parts[0].slice(-2);
            const qtr = parts[1] || '';
            return `${qtr} '${year}`;
        });

        const datasets = series.map(c => ({
            label: c.label,
            data: c.values.map(v => v != null ? v : null),
            backgroundColor: hexToRgba(c.color, 0.7),
            borderColor: c.color,
            borderWidth: 1,
            fill: true,
            pointRadius: 0,
            pointHitRadius: 6,
            tension: 0.3,
        }));

        if (coferChart) coferChart.destroy();

        coferChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            color: '#9ca3af',
                            font: { size: 11 },
                            boxWidth: 12,
                            padding: 12,
                            usePointStyle: true,
                            pointStyle: 'rectRounded',
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
                                if (currentView === 'share') {
                                    return `${ctx.dataset.label}: ${val.toFixed(2)}%`;
                                }
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
                            maxTicksLimit: 20,
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' }
                    },
                    y: {
                        stacked: true,
                        ticks: {
                            color: '#6b7280',
                            font: { size: 10 },
                            callback: (val) => {
                                if (currentView === 'share') return val + '%';
                                return '$' + val.toLocaleString() + 'B';
                            }
                        },
                        grid: { color: 'rgba(55,65,81,0.3)' },
                        beginAtZero: true,
                    }
                }
            }
        });
    }

    // ── Data Table ─────────────────────────────────────────
    function renderTable(quarters, series) {
        const thead = document.getElementById('cofer-thead');
        const tbody = document.getElementById('cofer-tbody');

        // Header
        let headerHTML = '<tr><th>Quarter</th>';
        series.forEach(c => {
            headerHTML += `<th>${c.label}</th>`;
        });
        headerHTML += '</tr>';
        thead.innerHTML = headerHTML;

        // Rows — show most recent first
        let rowsHTML = '';
        for (let i = quarters.length - 1; i >= 0; i--) {
            rowsHTML += `<tr><td>${quarters[i]}</td>`;
            series.forEach(c => {
                const val = c.values[i];
                if (val == null) {
                    rowsHTML += '<td>—</td>';
                } else if (currentView === 'share') {
                    rowsHTML += `<td>${val.toFixed(2)}%</td>`;
                } else {
                    rowsHTML += `<td>$${val.toFixed(1)}B</td>`;
                }
            });
            rowsHTML += '</tr>';
        }
        tbody.innerHTML = rowsHTML;
    }

    // ── Meta ───────────────────────────────────────────────
    function renderMeta(meta, qCount, sCount) {
        const el = document.getElementById('cofer-meta');
        const parts = [];
        if (meta.source) parts.push(meta.source);
        if (meta.frequency) parts.push(meta.frequency);
        parts.push(`${qCount} quarters`);
        parts.push(`${sCount} series`);
        el.textContent = parts.join(' · ');
    }

    // ── Util ───────────────────────────────────────────────
    function hexToRgba(hex, alpha) {
        const r = parseInt(hex.slice(1, 3), 16);
        const g = parseInt(hex.slice(3, 5), 16);
        const b = parseInt(hex.slice(5, 7), 16);
        return `rgba(${r},${g},${b},${alpha})`;
    }

})();
