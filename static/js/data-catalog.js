/**
 * Data Catalog — single source of truth for all datasets, categories, and search index.
 * Drives sidebar rendering, URL routing, search, and panel generation.
 */

window.ParraData = window.ParraData || {};

window.ParraData.CATALOG = {
    categories: [
        {
            id: 'prices',
            label: 'Prices & Inflation',
            icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
            datasets: [
                {
                    id: 'us-cpi',
                    label: 'US CPI',
                    source: 'Bureau of Labor Statistics',
                    sourceDetail: 'CPI-U, Seasonally Adjusted',
                    api: '/api/cpi/us',
                    componentApi: '/api/cpi/us/components',
                    exportUrl: '/api/cpi/us/export',
                    componentExportUrl: '/api/cpi/us/components/export',
                    type: 'cpi',
                    isUs: true,
                    controls: ['freq', 'view', 'range'],
                    subviews: [
                        { id: 'overview', label: 'Overview' },
                        { id: 'food_bev', label: 'Food & Beverages' },
                        { id: 'housing', label: 'Housing' },
                        { id: 'apparel', label: 'Apparel' },
                        { id: 'transportation', label: 'Transportation' },
                        { id: 'medical', label: 'Medical Care' },
                        { id: 'recreation', label: 'Recreation' },
                        { id: 'education', label: 'Education & Comm' },
                        { id: 'other', label: 'Other Goods & Services' },
                    ],
                    searchTerms: ['inflation', 'consumer price', 'CPI-U', 'BLS', 'prices', 'food', 'housing', 'energy'],
                },
                {
                    id: 'uk-cpi',
                    label: 'UK CPI',
                    source: 'Office for National Statistics',
                    sourceDetail: 'MM23',
                    api: '/api/cpi/uk',
                    componentApi: '/api/cpi/uk/components',
                    exportUrl: '/api/cpi/uk/export',
                    componentExportUrl: '/api/cpi/uk/components/export',
                    type: 'cpi',
                    isUs: false,
                    controls: ['freq', 'view', 'range'],
                    subviews: [
                        { id: 'overview', label: 'Overview' },
                        { id: 'food', label: 'Food & Non-Alc Bev' },
                        { id: 'alcohol', label: 'Alcohol & Tobacco' },
                        { id: 'clothing', label: 'Clothing & Footwear' },
                        { id: 'housing', label: 'Housing & Fuels' },
                        { id: 'furniture', label: 'Furniture & HH' },
                        { id: 'health', label: 'Health' },
                        { id: 'transport', label: 'Transport' },
                        { id: 'communication', label: 'Communication' },
                        { id: 'recreation', label: 'Recreation & Culture' },
                        { id: 'education', label: 'Education' },
                        { id: 'restaurants', label: 'Restaurants & Hotels' },
                        { id: 'misc', label: 'Misc Goods & Services' },
                    ],
                    searchTerms: ['inflation', 'consumer price', 'ONS', 'UK', 'prices', 'Britain'],
                },
            ],
        },
        {
            id: 'commodities',
            label: 'Commodities',
            icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>',
            datasets: [
                {
                    id: 'forecast-oil',
                    label: 'Oil & Gas',
                    source: 'ParraMacro',
                    sourceDetail: 'Scenario-Based Commodity Forecasts',
                    api: '/api/forecasts',
                    exportUrl: '/api/forecasts/export',
                    type: 'forecast-group',
                    forecastGroupName: 'Oil & Gas',
                    controls: ['scenario'],
                    subviews: [
                        { id: 'overview', label: 'Overview' },
                        { id: 'WTI Crude', label: 'WTI Crude' },
                        { id: 'Brent Crude', label: 'Brent Crude' },
                        { id: 'Natural Gas (HH)', label: 'Natural Gas (HH)' },
                        { id: 'TTF Gas', label: 'TTF Gas' },
                    ],
                    searchTerms: ['oil', 'gas', 'crude', 'WTI', 'Brent', 'natural gas', 'TTF', 'energy', 'petroleum'],
                },
                {
                    id: 'forecast-ag',
                    label: 'Agriculture',
                    source: 'ParraMacro',
                    sourceDetail: 'Scenario-Based Commodity Forecasts',
                    api: '/api/forecasts',
                    exportUrl: '/api/forecasts/export',
                    type: 'forecast-group',
                    forecastGroupName: 'Agriculture',
                    controls: ['scenario'],
                    subviews: [
                        { id: 'overview', label: 'Overview' },
                        { id: 'Cocoa', label: 'Cocoa' },
                        { id: 'Wheat', label: 'Wheat' },
                        { id: 'Soybeans', label: 'Soybeans' },
                        { id: 'Coffee', label: 'Coffee' },
                    ],
                    searchTerms: ['agriculture', 'cocoa', 'wheat', 'soybeans', 'coffee', 'grain', 'farming', 'food commodities'],
                },
                {
                    id: 'forecast-metals',
                    label: 'Metals',
                    source: 'ParraMacro',
                    sourceDetail: 'Scenario-Based Commodity Forecasts',
                    api: '/api/forecasts',
                    exportUrl: '/api/forecasts/export',
                    type: 'forecast-group',
                    forecastGroupName: 'Metals',
                    controls: ['scenario'],
                    subviews: [
                        { id: 'overview', label: 'Overview' },
                        { id: 'Copper', label: 'Copper' },
                        { id: 'Gold', label: 'Gold' },
                    ],
                    searchTerms: ['metals', 'copper', 'gold', 'precious', 'industrial metals', 'mining'],
                },
            ],
        },
        {
            id: 'trade',
            label: 'Trade & Reserves',
            icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 7V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v3"/></svg>',
            datasets: [
                {
                    id: 'cofer',
                    label: 'Central Bank Reserves',
                    source: 'World Bank',
                    sourceDetail: 'International Financial Statistics',
                    api: '/api/cofer',
                    exportUrl: '/api/cofer/export',
                    type: 'cofer',
                    controls: ['region', 'reserve-type'],
                    subviews: [],
                    searchTerms: ['reserves', 'COFER', 'central bank', 'FX', 'foreign exchange', 'gold reserves', 'IMF'],
                },
            ],
        },
        {
            id: 'growth',
            label: 'Growth & Output',
            icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>',
            datasets: [
                {
                    id: 'weo-gdp',
                    label: 'GDP Growth',
                    source: 'IMF World Economic Outlook',
                    sourceDetail: 'Real GDP, Annual % Change',
                    api: '/api/weo/NGDP_RPCH',
                    exportUrl: '/api/weo/NGDP_RPCH/export',
                    type: 'weo',
                    weoIndicator: 'NGDP_RPCH',
                    controls: ['countries', 'range'],
                    subviews: [],
                    defaultCountries: ['USA', 'CHN', 'DEU', 'JPN', 'GBR', 'BRA', 'IND'],
                    countryGroups: {
                        'G7': ['USA', 'GBR', 'DEU', 'FRA', 'JPN', 'CAN', 'ITA'],
                        'BRICS': ['BRA', 'RUS', 'IND', 'CHN', 'ZAF'],
                        'Major EM': ['BRA', 'IND', 'CHN', 'MEX', 'IDN', 'TUR', 'ZAF'],
                    },
                    searchTerms: ['GDP', 'growth', 'output', 'economic growth', 'real GDP', 'WEO'],
                },
            ],
        },
    ],
};

// Build flat search index
window.ParraData.buildSearchIndex = function () {
    const index = [];
    const catalog = window.ParraData.CATALOG;

    for (const cat of catalog.categories) {
        for (const ds of cat.datasets) {
            // Dataset-level entry
            index.push({
                label: ds.label,
                category: cat.label,
                categoryId: cat.id,
                datasetId: ds.id,
                subviewId: null,
                path: '/data/' + cat.id + '/' + ds.id,
                terms: [ds.label, ds.source, cat.label, ...(ds.searchTerms || [])].join(' ').toLowerCase(),
            });

            // Sub-view entries
            if (ds.subviews) {
                for (const sv of ds.subviews) {
                    if (sv.id === 'overview') continue; // Skip overview — same as dataset level
                    index.push({
                        label: ds.label + ' \u203a ' + sv.label,
                        category: cat.label,
                        categoryId: cat.id,
                        datasetId: ds.id,
                        subviewId: sv.id,
                        path: '/data/' + cat.id + '/' + ds.id + '/' + sv.id,
                        terms: [ds.label, sv.label, ds.source, cat.label, ...(ds.searchTerms || [])].join(' ').toLowerCase(),
                    });
                }
            }
        }
    }

    return index;
};

// Lookup helpers
window.ParraData.findCategory = function (categoryId) {
    return window.ParraData.CATALOG.categories.find(c => c.id === categoryId) || null;
};

window.ParraData.findDataset = function (categoryId, datasetId) {
    const cat = window.ParraData.findCategory(categoryId);
    if (!cat) return null;
    return cat.datasets.find(d => d.id === datasetId) || null;
};

window.ParraData.findDatasetGlobal = function (datasetId) {
    for (const cat of window.ParraData.CATALOG.categories) {
        const ds = cat.datasets.find(d => d.id === datasetId);
        if (ds) return { category: cat, dataset: ds };
    }
    return null;
};
