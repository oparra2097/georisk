document.addEventListener('DOMContentLoaded', async () => {
    // Initialize map
    await MapModule.init('#map-svg-wrapper');

    // Initialize sidebar
    await SidebarModule.init();

    // Start polling for scores (data loads in background on server)
    pollForScores();

    // Panel close handlers
    document.getElementById('panel-close').addEventListener('click', () => PanelModule.close());
    document.querySelector('.panel-backdrop').addEventListener('click', () => PanelModule.close());
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') PanelModule.close();
    });

    // Auto-refresh every 5 minutes
    setInterval(async () => {
        await MapModule.updateScores();
        await updateStatusBar();
        await SidebarModule.loadGlobalHeadlines();
    }, 5 * 60 * 1000);
});

async function pollForScores() {
    // Poll every 3 seconds until we get data, then switch to normal refresh
    const poll = async () => {
        const status = await ApiClient.getStatus();
        if (status.countries_tracked > 0) {
            await MapModule.updateScores();
            await updateStatusBar();
            return;
        }
        setTimeout(poll, 3000);
    };
    await poll();
}

async function updateStatusBar() {
    const status = await ApiClient.getStatus();
    if (status.last_refresh) {
        const date = new Date(status.last_refresh);
        document.getElementById('last-update').textContent = date.toLocaleTimeString();
    }
    if (status.hotspot_count !== undefined) {
        document.getElementById('hotspot-count').textContent = status.hotspot_count;
    }
    if (status.countries_tracked !== undefined) {
        document.getElementById('country-count').textContent = status.countries_tracked;
    }
}
