/**
 * Home page — headline vitals, grouped indicator tables, expandable
 * history chart and the latest Substack note. Market data comes from
 * /api/markets (Yahoo Finance, cached server-side for 5 minutes).
 */

let marketChart = null;
let activeSymbol = null;
let activePeriod = '1mo';
let marketsCache = null;

document.addEventListener('DOMContentLoaded', () => {
    renderDateline();
    fetchMarkets();
    fetchLatestPost();
    // Auto-refresh every 5 minutes
    setInterval(fetchMarkets, 5 * 60 * 1000);

    // Close button
    const closeBtn = document.getElementById('chart-close-btn');
    if (closeBtn) closeBtn.addEventListener('click', closeChartPanel);

    // Timeframe buttons
    const tfContainer = document.getElementById('chart-timeframes');
    if (tfContainer) {
        tfContainer.addEventListener('click', (e) => {
            const btn = e.target.closest('.tf-btn');
            if (!btn || !activeSymbol) return;

            activePeriod = btn.dataset.period;
            document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            fetchAndRenderHistory(activeSymbol, activePeriod);
        });
    }
});


function renderDateline() {
    const el = document.getElementById('hv-date');
    if (!el) return;
    el.textContent = new Date().toLocaleDateString('en-US', {
        weekday: 'long', month: 'long', day: 'numeric', year: 'numeric',
    });
}


async function fetchMarkets() {
    try {
        const resp = await fetch('/api/markets');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        if (data.markets) {
            marketsCache = data.markets;
            renderVitals(data.markets);
            renderMarketGrid(data.markets);
            renderTimestamp(data.updated_at);
        }
    } catch (err) {
        console.error('Failed to fetch markets:', err);
        const grid = document.getElementById('market-grid');
        if (grid && !marketsCache) {
            grid.innerHTML = '<div class="hv-table-empty">Market data is unavailable right now.</div>';
        }
    }
}


function renderTimestamp(iso) {
    const el = document.getElementById('market-timestamp');
    if (!el || !iso) return;
    // updated_at is naive UTC from the server
    const d = new Date(iso.endsWith('Z') ? iso : iso + 'Z');
    if (isNaN(d)) return;
    el.textContent = ' · Markets as of ' + d.toLocaleTimeString('en-US', {
        hour: 'numeric', minute: '2-digit', timeZoneName: 'short',
    });
}


// ===== Headline vitals =====
//
// One headline instrument per theme plus a supporting line, the way a
// macro dashboard leads with output / inflation / rates before detail.

const VITALS = [
    { topic: 'Growth',    main: '^GSPC',    support: 'HG=F' },
    { topic: 'Inflation', main: 'CL=F',     support: 'GC=F' },
    { topic: 'Rates',     main: '^TNX',     support: 'TERM_SPREAD' },
    { topic: 'Dollar',    main: 'DX-Y.NYB', support: 'EURUSD=X' },
    { topic: 'Risk',      main: '^VIX',     support: null },
];

function renderVitals(markets) {
    const wrap = document.getElementById('hv-vitals');
    if (!wrap) return;
    const bySym = Object.fromEntries(markets.map(m => [m.symbol, m]));

    wrap.innerHTML = VITALS.map(v => {
        const m = bySym[v.main];
        const s = v.support ? bySym[v.support] : null;
        if (!m || m.price === null) {
            return `<div class="hv-vital"><div class="hv-vital-topic">${v.topic}</div>
                    <div class="hv-vital-value flat">—</div></div>`;
        }
        const dir = direction(m);
        return `
            <div class="hv-vital">
                <div class="hv-vital-topic">${v.topic}</div>
                <div class="hv-vital-name">${m.name}</div>
                <div class="hv-vital-value">${formatPrice(m.price, m.type)}</div>
                <div class="hv-vital-change ${dir}">${formatChange(m)}</div>
                <div class="hv-vital-read">${vitalRead(v, m, s)}</div>
            </div>`;
    }).join('');
}

// Short plain-language line under each headline number.
function vitalRead(v, m, s) {
    if (v.main === '^VIX') {
        const x = m.price;
        const level = x < 15 ? 'Calm' : x < 20 ? 'Normal' : x < 30 ? 'Elevated' : 'Stressed';
        return `${level} equity volatility (long-run average near 19).`;
    }
    if (v.support === 'TERM_SPREAD' && s && s.price !== null) {
        const bp = Math.round(s.price * 100);
        const shape = bp < 0 ? 'inverted' : bp < 50 ? 'flat' : 'upward-sloping';
        return `10Y–3M curve ${bp >= 0 ? '+' : ''}${bp}bp, ${shape}.`;
    }
    if (!s || s.price === null) return '';
    return `${s.name} ${formatPrice(s.price, s.type)}, ${formatChange(s)}.`;
}


// ===== Indicator tables (theme-grouped) =====

const MARKET_GROUPS = [
    { key: 'growth',    label: 'Growth & equities' },
    { key: 'inflation', label: 'Inflation & commodities' },
    { key: 'rates',     label: 'Rates & curve' },
    { key: 'dollar',    label: 'Dollar & FX' },
    { key: 'risk',      label: 'Risk' },
];

function groupOf(m) {
    if (m.group) return m.group;
    // Fallback for payloads cached before groups were added.
    return { equity: 'growth', commodity: 'inflation', bond: 'rates',
             spread: 'rates', fx: 'dollar' }[m.type] || 'risk';
}

function renderMarketGrid(markets) {
    const grid = document.getElementById('market-grid');
    if (!grid) return;

    grid.innerHTML = '';

    for (const g of MARKET_GROUPS) {
        const items = markets.filter(m => groupOf(m) === g.key);
        if (items.length === 0) continue;

        const block = document.createElement('div');
        block.className = 'hv-table-block';
        block.innerHTML = `
            <h3>${g.label}</h3>
            <table class="hv-table">
                <thead><tr>
                    <th scope="col">Indicator</th>
                    <th scope="col">Last</th>
                    <th scope="col">Change</th>
                    <th scope="col">5 days</th>
                </tr></thead>
                <tbody></tbody>
            </table>`;
        const tbody = block.querySelector('tbody');

        items.forEach(m => {
            const tr = document.createElement('tr');
            tr.dataset.symbol = m.symbol;
            tr.tabIndex = 0;
            tr.classList.toggle('is-active', m.symbol === activeSymbol);
            tr.innerHTML = marketRowHTML(m);
            tr.addEventListener('click', () => onMarketCardClick(m));
            tr.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onMarketCardClick(m); }
            });
            tbody.appendChild(tr);
        });
        grid.appendChild(block);
    }
}

function marketRowHTML(m) {
    const dir = direction(m);
    const price = m.price !== null ? formatPrice(m.price, m.type) : '—';
    const spark = (m.sparkline && m.sparkline.length > 2) ? buildSparklineSVG(m.sparkline, dir) : '';
    return `
        <td>${m.name}</td>
        <td>${price}</td>
        <td class="${dir}">${formatChange(m, false) || '—'}</td>
        <td><span class="hv-spark">${spark}</span></td>`;
}

function direction(m) {
    const isYield = m.type === 'bond' || m.type === 'spread';
    // Yields read in whole basis points, so a sub-0.5bp move is flat.
    const v = isYield ? Math.round((m.change || 0) * 100)
            : (m.change_pct !== null ? m.change_pct : m.change);
    return (v || 0) > 0 ? 'up' : (v || 0) < 0 ? 'down' : 'flat';
}

// "+0.42%" for prices, "+3bp" for yields and spreads.
function formatChange(m, withToday = true) {
    const suffix = withToday ? ' today' : '';
    if (m.type === 'bond' || m.type === 'spread') {
        if (m.change === null) return '';
        const bp = Math.round(m.change * 100);
        return `${bp >= 0 ? '+' : ''}${bp}bp${suffix}`;
    }
    if (m.change_pct === null) return '';
    return `${m.change_pct >= 0 ? '+' : ''}${m.change_pct.toFixed(2)}%${suffix}`;
}


// ===== SVG Sparkline =====

function buildSparklineSVG(data, dir) {
    const w = 72;
    const h = 20;
    const pad = 1.5;
    const n = data.length;
    if (n < 2) return '';

    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;

    const points = data.map((v, i) => {
        const x = pad + (i / (n - 1)) * (w - pad * 2);
        const y = h - pad - ((v - min) / range) * (h - pad * 2);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    });

    const color = dir === 'up' ? 'var(--hv-up)' : dir === 'down' ? 'var(--hv-down)' : 'var(--hv-ink-3)';

    return `
        <svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" style="width:100%;height:100%;" aria-hidden="true">
            <polyline points="${points.join(' ')}" fill="none" stroke="${color}" stroke-width="1.25" vector-effect="non-scaling-stroke"/>
        </svg>
    `;
}


// ===== Expandable Chart =====

function onMarketCardClick(market) {
    const panel = document.getElementById('market-chart-panel');
    if (!panel) return;

    // Toggle off if same card clicked
    if (activeSymbol === market.symbol) {
        closeChartPanel();
        return;
    }

    activeSymbol = market.symbol;
    activePeriod = '1mo';

    // Highlight active card
    document.querySelectorAll('.hv-table tbody tr').forEach(r => {
        r.classList.toggle('is-active', r.dataset.symbol === market.symbol);
    });

    // Update header info
    document.getElementById('chart-symbol-name').textContent = market.name;
    updateChartPrice(market);

    // Reset timeframe buttons
    document.querySelectorAll('.tf-btn').forEach(b => {
        b.classList.toggle('active', b.dataset.period === activePeriod);
    });

    // Show panel and fetch data
    panel.style.display = '';
    fetchAndRenderHistory(market.symbol, activePeriod);
}


function updateChartPrice(market) {
    const priceEl = document.getElementById('chart-price');
    const changeEl = document.getElementById('chart-change');
    if (priceEl) priceEl.textContent = market.price !== null ? formatPrice(market.price, market.type) : '--';
    if (changeEl) {
        const dir = (market.change_pct || 0) >= 0 ? 'up' : 'down';
        const sign = dir === 'up' ? '+' : '';
        const pct = market.change_pct !== null ? `${sign}${market.change_pct.toFixed(2)}%` : '';
        changeEl.textContent = pct;
        changeEl.className = 'market-chart-change ' + dir;
    }
}


function closeChartPanel() {
    const panel = document.getElementById('market-chart-panel');
    if (panel) panel.style.display = 'none';
    activeSymbol = null;
    document.querySelectorAll('.hv-table tbody tr').forEach(r => r.classList.remove('is-active'));
    if (marketChart) {
        marketChart.destroy();
        marketChart = null;
    }
}


// Chart.js is loaded lazily — on-demand when the user clicks a market
// card. Saves ~70 KB of upfront JS on every home visit. The returned
// promise resolves to the global `Chart` constructor once the script
// finishes loading.
let _chartJsPromise = null;
function ensureChartJs() {
    if (_chartJsPromise) return _chartJsPromise;
    _chartJsPromise = new Promise((resolve, reject) => {
        if (typeof Chart !== 'undefined') return resolve(window.Chart);
        const s = document.createElement('script');
        s.src = '/static/vendor/chart.umd.js';
        s.async = true;
        s.onload  = () => resolve(window.Chart);
        s.onerror = () => reject(new Error('chart.umd.js failed to load'));
        document.head.appendChild(s);
    });
    return _chartJsPromise;
}


async function fetchAndRenderHistory(symbol, period) {
    const loading = document.getElementById('market-chart-loading');
    const canvas = document.getElementById('market-history-chart');
    if (loading) loading.style.display = '';

    try {
        // Load Chart.js (first click only) in parallel with the fetch.
        const [, data] = await Promise.all([
            ensureChartJs(),
            fetch(`/api/markets/history?symbol=${encodeURIComponent(symbol)}&period=${period}`)
                .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
        ]);

        if (loading) loading.style.display = 'none';
        renderHistoryChart(data);
    } catch (err) {
        console.error('Failed to fetch market history:', err);
        if (loading) loading.style.display = 'none';
    }
}


function renderHistoryChart(data) {
    const ctx = document.getElementById('market-history-chart');
    if (!ctx) return;

    if (marketChart) marketChart.destroy();

    const closes = data.closes || [];
    const dates = data.dates || [];

    // Determine color based on first vs last value
    const first = closes.find(v => v !== null);
    const last = [...closes].reverse().find(v => v !== null);
    const isUp = last >= first;
    const lineColor = isUp ? '#4fae7f' : '#d9705b';
    const fillColor = isUp ? 'rgba(79, 174, 127, 0.08)' : 'rgba(217, 112, 91, 0.08)';

    // Update header % change to reflect selected period
    const changeEl = document.getElementById('chart-change');
    if (changeEl && first != null && last != null && first !== 0) {
        const periodPct = ((last - first) / first) * 100;
        const dir = periodPct >= 0 ? 'up' : 'down';
        const sign = periodPct >= 0 ? '+' : '';
        changeEl.textContent = `${sign}${periodPct.toFixed(2)}%`;
        changeEl.className = 'market-chart-change ' + dir;
    }

    marketChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: dates,
            datasets: [{
                data: closes,
                borderColor: lineColor,
                backgroundColor: fillColor,
                borderWidth: 1.5,
                fill: true,
                pointRadius: 0,
                pointHitRadius: 8,
                tension: 0.2,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1b1e23',
                    borderColor: '#3a3f48',
                    borderWidth: 1,
                    titleColor: '#e8e6e1',
                    bodyColor: '#b4b1aa',
                    callbacks: {
                        label: (ctx) => {
                            const val = ctx.parsed.y;
                            if (val == null) return 'N/A';
                            if (data.type === 'bond' || data.type === 'spread') return val.toFixed(3) + '%';
                            if (data.type === 'fx') return formatPrice(val, 'fx');
                            if (data.type === 'commodity') return '$' + val.toFixed(2);
                            // equity, indicator, and anything else
                            return val.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#85827c',
                        font: { size: 11 },
                        maxRotation: 0,
                        autoSkip: true,
                        maxTicksLimit: 10,
                    },
                    grid: { display: false }
                },
                y: {
                    ticks: {
                        color: '#85827c',
                        font: { size: 11 },
                    },
                    grid: { color: 'rgba(255,255,255,0.06)' }
                }
            }
        }
    });
}


// ===== Latest Substack Post =====

// Swaps the static Substack card for the newest post. On any failure
// the static card stays as is.
async function fetchLatestPost() {
    try {
        const resp = await fetch('/api/substack');
        if (!resp.ok) return;
        const data = await resp.json();
        if (!data.posts || data.posts.length === 0) return;

        const p = data.posts[0];
        const link = document.getElementById('latest-post');
        if (!link || !p.url || !p.title) return;

        link.href = p.url;
        document.getElementById('latest-post-title').textContent = p.title;
        document.getElementById('latest-post-desc').textContent = p.description || '';
        document.getElementById('latest-post-meta').textContent =
            p.published_at ? `The Parrable · ${p.published_at}` : 'The Parrable';
    } catch (err) {
        // Silently fail — static card stays
    }
}


// ===== Formatting =====

function formatPrice(price, type) {
    if (price === null || price === undefined) return '--';

    switch (type) {
        case 'equity':
        case 'indicator':
            return price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        case 'commodity':
            return '$' + price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        case 'bond':
            return price.toFixed(2) + '%';
        case 'spread':
            return (price >= 0 ? '+' : '') + Math.round(price * 100) + 'bp';
        case 'fx':
            // EUR/USD needs four decimals; DXY and USD/JPY read in two.
            return price >= 20 ? price.toFixed(2) : price.toFixed(4);
        default:
            return price.toFixed(2);
    }
}
