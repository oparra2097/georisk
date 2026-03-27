/**
 * Home page - Market data fetching, ticker rendering, sparklines, expandable charts
 */

let marketChart = null;
let activeSymbol = null;
let activePeriod = '1mo';
let marketsCache = null;

document.addEventListener('DOMContentLoaded', () => {
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


async function fetchMarkets() {
    try {
        const resp = await fetch('/api/markets');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        if (data.markets) {
            marketsCache = data.markets;
            renderTicker(data.markets);
            renderMarketGrid(data.markets);
        }
    } catch (err) {
        console.error('Failed to fetch markets:', err);
    }
}


// ===== Ticker Bar =====

function renderTicker(markets) {
    const track = document.getElementById('ticker-track');
    if (!track) return;

    const items = markets
        .filter(m => m.price !== null)
        .map(m => tickerItemHTML(m))
        .join('<span class="ticker-sep">|</span>');

    // Duplicate for infinite scroll effect
    track.innerHTML = items + '<span class="ticker-sep">|</span>' + items;
}

function tickerItemHTML(m) {
    const dir = (m.change_pct || 0) >= 0 ? 'up' : 'down';
    const sign = dir === 'up' ? '+' : '';
    const pct = m.change_pct !== null ? `${sign}${m.change_pct.toFixed(2)}%` : '';
    const chg = m.change !== null ? `${sign}${m.change.toFixed(2)}` : '';
    return `
        <span class="ticker-item">
            <span class="ticker-name">${m.name}</span>
            <span class="ticker-price">${formatPrice(m.price, m.type)}</span>
            <span class="ticker-change ${dir}">${chg} (${pct})</span>
        </span>
    `;
}


// ===== Market Grid =====

function renderMarketGrid(markets) {
    const grid = document.getElementById('market-grid');
    if (!grid) return;

    grid.innerHTML = '';
    markets.forEach(m => {
        const card = document.createElement('div');
        card.className = 'market-card';
        card.dataset.symbol = m.symbol;
        card.innerHTML = marketCardHTML(m);
        card.addEventListener('click', () => onMarketCardClick(m));
        card.style.cursor = 'pointer';
        grid.appendChild(card);
    });
}

function marketCardHTML(m) {
    const dir = (m.change_pct || 0) > 0 ? 'up' : (m.change_pct || 0) < 0 ? 'down' : 'flat';
    const sign = dir === 'up' ? '+' : '';
    const pct = m.change_pct !== null ? `${sign}${m.change_pct.toFixed(2)}%` : 'N/A';
    const chg = m.change !== null ? `${sign}${m.change.toFixed(2)}` : '';
    const price = m.price !== null ? formatPrice(m.price, m.type) : '--';

    let sparkSVG = '';
    if (m.sparkline && m.sparkline.length > 2) {
        sparkSVG = buildSparklineSVG(m.sparkline, dir);
    }

    return `
        <div class="market-card-header">
            <span class="market-label">${m.name}</span>
            <span class="market-type-badge">${m.type}</span>
        </div>
        <div class="market-price">${price}</div>
        <div class="market-change ${dir}">${chg} (${pct})</div>
        <div class="market-sparkline">${sparkSVG}</div>
    `;
}


// ===== SVG Sparkline =====

function buildSparklineSVG(data, dir) {
    const w = 200;
    const h = 40;
    const pad = 2;
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

    const color = dir === 'up' ? 'var(--green)' : dir === 'down' ? 'var(--red)' : 'var(--text-muted)';

    // Create fill area
    const firstX = pad;
    const lastX = pad + ((n - 1) / (n - 1)) * (w - pad * 2);
    const fillPoints = points.join(' ') + ` ${lastX.toFixed(1)},${h} ${firstX.toFixed(1)},${h}`;

    return `
        <svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" style="width:100%;height:100%;">
            <polygon points="${fillPoints}" fill="${color}" opacity="0.1"/>
            <polyline points="${points.join(' ')}" fill="none" stroke="${color}" stroke-width="1.5" vector-effect="non-scaling-stroke"/>
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
    document.querySelectorAll('.market-card').forEach(c => {
        c.classList.toggle('market-card-active', c.dataset.symbol === market.symbol);
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
    document.querySelectorAll('.market-card').forEach(c => c.classList.remove('market-card-active'));
    if (marketChart) {
        marketChart.destroy();
        marketChart = null;
    }
}


async function fetchAndRenderHistory(symbol, period) {
    const loading = document.getElementById('market-chart-loading');
    const canvas = document.getElementById('market-history-chart');
    if (loading) loading.style.display = '';

    try {
        const resp = await fetch(`/api/markets/history?symbol=${encodeURIComponent(symbol)}&period=${period}`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();

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
    const lineColor = isUp ? 'rgb(16, 185, 129)' : 'rgb(239, 68, 68)';
    const fillColor = isUp ? 'rgba(16, 185, 129, 0.1)' : 'rgba(239, 68, 68, 0.1)';

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
                borderWidth: 2,
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
                    backgroundColor: 'rgba(0,0,0,0.9)',
                    titleColor: '#fff',
                    bodyColor: '#d1d5db',
                    callbacks: {
                        label: (ctx) => {
                            const val = ctx.parsed.y;
                            if (val == null) return 'N/A';
                            if (data.type === 'bond' || data.type === 'spread') return val.toFixed(3) + '%';
                            if (data.type === 'fx') return val.toFixed(4);
                            if (data.type === 'commodity') return '$' + val.toFixed(2);
                            return val.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
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
                        maxTicksLimit: 10,
                    },
                    grid: { color: 'rgba(55,65,81,0.3)' }
                },
                y: {
                    ticks: {
                        color: '#6b7280',
                        font: { size: 10 },
                    },
                    grid: { color: 'rgba(55,65,81,0.3)' }
                }
            }
        }
    });
}


// ===== Latest Substack Post =====

async function fetchLatestPost() {
    try {
        const resp = await fetch('/api/substack');
        if (!resp.ok) return;
        const data = await resp.json();
        if (!data.posts || data.posts.length === 0) return;

        const p = data.posts[0];
        const section = document.getElementById('latest-research-section');
        const container = document.getElementById('latest-post');
        if (!section || !container) return;

        const img = p.image
            ? `<img class="latest-post-image" src="${p.image}" alt="" loading="lazy">`
            : '';

        container.innerHTML = `
            <a href="${p.url}" target="_blank" rel="noopener" class="latest-post-card">
                ${img}
                <div class="latest-post-body">
                    <div class="latest-post-date">${p.published_at}</div>
                    <div class="latest-post-title">${p.title}</div>
                    <div class="latest-post-desc">${p.description}</div>
                    <span class="latest-post-read">Read on Substack &rarr;</span>
                </div>
            </a>
        `;
        section.style.display = '';
    } catch (err) {
        // Silently fail — section stays hidden
    }
}


// ===== Formatting =====

function formatPrice(price, type) {
    if (price === null || price === undefined) return '--';

    switch (type) {
        case 'index':
            return price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        case 'commodity':
            return '$' + price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        case 'bond':
            return price.toFixed(3) + '%';
        case 'spread':
            return price.toFixed(3) + '%';
        case 'fx':
            return price.toFixed(4);
        default:
            return price.toFixed(2);
    }
}
