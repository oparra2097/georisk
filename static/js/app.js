document.addEventListener('DOMContentLoaded', async () => {
    // Initialize map
    await MapModule.init('#map-svg-wrapper');

    // Initialize sidebar
    await SidebarModule.init();

    // Start polling for scores (data loads in background on server)
    pollForScores();

    // Score filter toggle
    document.querySelectorAll('.score-filter-btn').forEach(btn => {
        btn.addEventListener('click', () => MapModule.setScoreField(btn.dataset.score));
    });

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
    // Poll every 3 seconds until backend has refreshed at least once
    const poll = async () => {
        const status = await ApiClient.getStatus();
        if (status.last_refresh) {
            // Data is ready
            await MapModule.updateScores();
            await updateStatusBar();
            return;
        }
        // Also try loading whatever partial data exists
        if (status.countries_tracked > 0) {
            await MapModule.updateScores();
            await updateStatusBar();
        }
        setTimeout(poll, 4000);
    };
    await poll();
}

async function updateStatusBar() {
    const status = await ApiClient.getStatus();
    if (status.last_refresh) {
        const date = new Date(status.last_refresh + 'Z');  // ensure parsed as UTC
        const estStr = date.toLocaleString('en-US', {
            timeZone: 'America/New_York',
            hour: 'numeric',
            minute: '2-digit',
            second: '2-digit',
            hour12: true,
            month: 'short',
            day: 'numeric',
        });
        document.getElementById('last-update').textContent = estStr + ' EST';
    } else {
        document.getElementById('last-update').textContent = 'Refreshing...';
    }
    if (status.hotspot_count !== undefined) {
        document.getElementById('hotspot-count').textContent = status.hotspot_count;
    }
    if (status.countries_tracked !== undefined) {
        document.getElementById('country-count').textContent = status.countries_tracked;
    }
}
