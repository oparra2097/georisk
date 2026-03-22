/**
 * Home page - Market data fetching, ticker rendering, sparklines
 */

document.addEventListener('DOMContentLoaded', () => {
    fetchMarkets();
    // Auto-refresh every 5 minutes
    setInterval(fetchMarkets, 5 * 60 * 1000);
});


async function fetchMarkets() {
    try {
        const resp = await fetch('/api/markets');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        if (data.markets) {
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
        card.innerHTML = marketCardHTML(m);
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
        case 'fx':
            return price.toFixed(4);
        default:
            return price.toFixed(2);
    }
}
