/**
 * Data State — centralized state, data cache, and URL router.
 */

window.ParraData = window.ParraData || {};

// ── Centralized State ──────────────────────────────────────────────────────

window.ParraData.state = {
    category: null,
    dataset: null,
    subview: null,
    // Controls
    freq: 'monthly',
    view: 'yoy',
    range: '10',
    scenario: 'All',
    commFreq: 'quarterly',
    region: 'World',
    reserveType: 'total',
    insRegion: 'all',
    countries: [],
    emUniverse: 'em40',
};

// ── Data Cache (with TTL) ─────────────────────────────────────────────────

window.ParraData._CACHE_TTL = 15 * 60 * 1000; // 15 minutes
window.ParraData.cache = {};
window.ParraData.charts = {};
window.ParraData._fetching = {}; // Track in-flight requests to prevent duplicates

window.ParraData.getCached = function (url) {
    var entry = window.ParraData.cache[url];
    if (!entry) return null;
    if (Date.now() - entry.ts > window.ParraData._CACHE_TTL) {
        delete window.ParraData.cache[url];
        return null;
    }
    return entry.data;
};

window.ParraData.setCached = function (url, data) {
    window.ParraData.cache[url] = { data: data, ts: Date.now() };
};

window.ParraData.clearCache = function (url) {
    if (url) {
        delete window.ParraData.cache[url];
    } else {
        window.ParraData.cache = {};
    }
};

window.ParraData.destroyChart = function (key) {
    if (window.ParraData.charts[key]) {
        window.ParraData.charts[key].destroy();
        window.ParraData.charts[key] = null;
    }
};

window.ParraData.setChart = function (key, chart) {
    window.ParraData.charts[key] = chart;
};

// ── URL Router ─────────────────────────────────────────────────────────────

window.ParraData.parseUrl = function () {
    const path = window.location.pathname;
    const params = new URLSearchParams(window.location.search);
    const state = window.ParraData.state;

    // Parse path: /data/category/dataset/subview
    const parts = path.replace(/^\/data\/?/, '').split('/').filter(Boolean);

    if (parts.length >= 1) state.category = decodeURIComponent(parts[0]);
    if (parts.length >= 2) state.dataset = decodeURIComponent(parts[1]);
    if (parts.length >= 3) state.subview = decodeURIComponent(parts[2]);

    // Parse query params
    if (params.has('freq')) state.freq = params.get('freq');
    if (params.has('view')) state.view = params.get('view');
    if (params.has('range')) state.range = params.get('range');
    if (params.has('scenario')) state.scenario = params.get('scenario');
    if (params.has('commFreq')) state.commFreq = params.get('commFreq');
    if (params.has('region')) state.region = params.get('region');
    if (params.has('type')) state.reserveType = params.get('type');
    if (params.has('insRegion')) state.insRegion = params.get('insRegion');
    if (params.has('countries')) state.countries = params.get('countries').split(',').filter(Boolean);
    if (params.has('emUniverse')) state.emUniverse = params.get('emUniverse');

    // Defaults if nothing in URL
    if (!state.category || !state.dataset) {
        state.category = 'trade';
        state.dataset = 'cofer';
        state.subview = null;
    }
};

window.ParraData.buildUrl = function () {
    const state = window.ParraData.state;
    let path = '/data/' + state.category + '/' + state.dataset;
    if (state.subview && state.subview !== 'overview') {
        path += '/' + encodeURIComponent(state.subview);
    }

    const params = new URLSearchParams();
    const ds = window.ParraData.findDataset(state.category, state.dataset);
    if (!ds) return path;

    if (ds.controls.includes('freq') && state.freq !== 'monthly') params.set('freq', state.freq);
    if (ds.controls.includes('view') && state.view !== 'yoy') params.set('view', state.view);
    if (ds.controls.includes('range') && state.range !== '10') params.set('range', state.range);
    if (ds.controls.includes('scenario') && state.scenario !== 'Weighted Avg') params.set('scenario', state.scenario);
    if (ds.controls.includes('comm-freq') && state.commFreq !== 'quarterly') params.set('commFreq', state.commFreq);
    if (ds.controls.includes('region') && state.region !== 'World') params.set('region', state.region);
    if (ds.controls.includes('reserve-type') && state.reserveType !== 'total') params.set('type', state.reserveType);
    if (ds.controls.includes('ins-region') && state.insRegion !== 'all') params.set('insRegion', state.insRegion);
    if (ds.controls.includes('countries') && state.countries.length > 0) params.set('countries', state.countries.join(','));
    if (ds.controls.includes('em-universe') && state.emUniverse && state.emUniverse !== 'em40') params.set('emUniverse', state.emUniverse);

    const qs = params.toString();
    return qs ? path + '?' + qs : path;
};

window.ParraData.pushState = function () {
    const url = window.ParraData.buildUrl();
    if (window.location.pathname + window.location.search !== url) {
        history.pushState(null, '', url);
    }
};

window.ParraData.navigate = function (categoryId, datasetId, subviewId) {
    const state = window.ParraData.state;
    const prevDataset = state.dataset;

    state.category = categoryId;
    state.dataset = datasetId;
    state.subview = subviewId || (datasetId === 'insurance-inflation' ? 'medical' : null);

    // Reset controls when switching datasets
    if (prevDataset !== datasetId) {
        // Default to quarterly for insurance inflation, monthly for everything else
        state.freq = (datasetId === 'insurance-inflation') ? 'quarterly' : 'monthly';
        state.view = 'yoy';
        state.range = '10';
        state.scenario = 'Weighted Avg';
        state.region = 'World';
        state.reserveType = 'total';
        state.insRegion = 'all';
        state.countries = [];
        state.emUniverse = 'em40';
        state.emCustomTouched = false;
    }

    window.ParraData.pushState();

    // Trigger render via the main app
    if (window.ParraData.onNavigate) {
        window.ParraData.onNavigate();
    }
};
