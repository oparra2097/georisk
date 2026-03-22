const SidebarModule = {
    refreshInterval: null,

    async init() {
        await this.loadGlobalHeadlines();
        this.refreshInterval = setInterval(() => this.loadGlobalHeadlines(), 5 * 60 * 1000);

        document.getElementById('news-region-filter').addEventListener('change', (e) => {
            if (e.target.value === 'global') {
                this.loadGlobalHeadlines();
            } else if (e.target.value === 'hotspots') {
                this.loadHotspotHeadlines();
            }
        });
    },

    async loadGlobalHeadlines() {
        const data = await ApiClient.getGlobalHeadlines();
        this.render(data.articles || []);
    },

    async loadHotspotHeadlines() {
        const hotspots = await ApiClient.getHotspots();
        const allArticles = [];
        for (const h of (hotspots.hotspots || []).slice(0, 5)) {
            const data = await ApiClient.getHeadlines(h.country_code);
            (data.articles || []).forEach(a => {
                a._country = h.country_name;
                allArticles.push(a);
            });
        }
        this.render(allArticles);
    },

    async loadCountryHeadlines(countryCode) {
        const data = await ApiClient.getHeadlines(countryCode);
        this.render(data.articles || []);
    },

    render(articles) {
        const feed = document.getElementById('news-feed');
        if (!articles.length) {
            feed.innerHTML = '<li class="news-loading">No headlines available yet. Data is loading...</li>';
            return;
        }
        feed.innerHTML = '';
        articles.forEach(article => {
            const li = document.createElement('li');
            li.className = 'news-item';
            const title = Utils.escapeHtml(article.title || 'Untitled');
            const source = Utils.escapeHtml(article.source || '');
            const time = Utils.timeAgo(article.publishedAt);
            const country = article._country ? ` | ${Utils.escapeHtml(article._country)}` : '';
            const url = article.url || '#';

            li.innerHTML = `
                <a href="${Utils.escapeHtml(url)}" target="_blank" rel="noopener noreferrer">
                    <span class="news-title">${title}</span>
                </a>
                <span class="news-meta">${source} | ${time}${country}</span>
            `;
            feed.appendChild(li);
        });
    }
};
