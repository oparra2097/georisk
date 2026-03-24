const PanelModule = {
    radarChart: null,

    async open(countryCode) {
        const detail = await ApiClient.getCountryDetail(countryCode);
        if (!detail) return;

        const headlines = await ApiClient.getHeadlines(countryCode);

        document.getElementById('panel-country-name').textContent = detail.country_name || countryCode;

        const badge = document.getElementById('panel-composite-score');
        const score = detail.composite || 0;
        badge.textContent = score;
        badge.style.color = Utils.riskColor(score);
        badge.style.background = Utils.riskBg(score);

        // Two-tier score breakdown
        const baseScore = detail.base_score || 0;
        const newsScore = detail.news_score || 0;
        const articles = detail.headline_count || 0;
        const tone = detail.avg_tone || 0;

        const baseEl = document.getElementById('panel-base-score');
        baseEl.textContent = baseScore;
        baseEl.style.color = Utils.riskColor(baseScore);

        const newsEl = document.getElementById('panel-news-score');
        newsEl.textContent = newsScore;
        newsEl.style.color = Utils.riskColor(newsScore);

        document.getElementById('panel-article-count').textContent = articles;

        const toneEl = document.getElementById('panel-avg-tone');
        toneEl.textContent = tone.toFixed(1);
        toneEl.style.color = tone < -3 ? '#ef4444' : tone < -1 ? '#f59e0b' : '#10b981';

        this.renderRadarChart(detail.indicators || {});
        this.renderIndicatorBars(detail.indicators || {});
        this.renderHeadlines(headlines.articles || []);

        document.getElementById('country-panel').classList.remove('hidden');
    },

    renderRadarChart(indicators) {
        const ctx = document.getElementById('panel-radar-chart').getContext('2d');
        if (this.radarChart) this.radarChart.destroy();

        const labels = Utils.INDICATOR_ORDER.map(k => Utils.INDICATOR_LABELS[k]);
        const data = Utils.INDICATOR_ORDER.map(k => indicators[k] || 0);

        this.radarChart = new Chart(ctx, {
            type: 'radar',
            data: {
                labels: labels,
                datasets: [{
                    data: data,
                    backgroundColor: 'rgba(239, 68, 68, 0.15)',
                    borderColor: 'rgba(239, 68, 68, 0.8)',
                    pointBackgroundColor: 'rgba(239, 68, 68, 1)',
                    pointRadius: 4,
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                scales: {
                    r: {
                        min: 0,
                        max: 100,
                        ticks: {
                            stepSize: 25,
                            color: '#6b7280',
                            backdropColor: 'transparent',
                            font: { size: 10 }
                        },
                        grid: { color: 'rgba(107, 114, 128, 0.2)' },
                        angleLines: { color: 'rgba(107, 114, 128, 0.2)' },
                        pointLabels: {
                            color: '#9ca3af',
                            font: { size: 11 }
                        }
                    }
                },
                plugins: {
                    legend: { display: false }
                }
            }
        });
    },

    renderIndicatorBars(indicators) {
        const container = document.getElementById('panel-indicator-bars');
        container.innerHTML = '';

        Utils.INDICATOR_ORDER.forEach(key => {
            const value = Math.round(indicators[key] || 0);
            const color = Utils.riskColor(value);
            const label = Utils.INDICATOR_LABELS[key];

            const bar = document.createElement('div');
            bar.className = 'indicator-bar';
            bar.innerHTML = `
                <span class="indicator-name">${label}</span>
                <div class="indicator-track">
                    <div class="indicator-fill" style="width: ${value}%; background: ${color}"></div>
                </div>
                <span class="indicator-value" style="color: ${color}">${value}</span>
            `;
            container.appendChild(bar);
        });
    },

    renderHeadlines(articles) {
        const list = document.getElementById('panel-headlines');
        if (!articles.length) {
            list.innerHTML = '<li style="color: #6b7280; padding: 12px 0; font-size: 13px;">No headlines available for this country.</li>';
            return;
        }
        list.innerHTML = '';
        articles.slice(0, 10).forEach(art => {
            const li = document.createElement('li');
            const title = Utils.escapeHtml(art.title || 'Untitled');
            const source = Utils.escapeHtml(art.source || '');
            const time = Utils.timeAgo(art.publishedAt);
            const url = art.url || '#';

            li.innerHTML = `
                <a href="${Utils.escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${title}</a>
                <div class="headline-meta">${source} | ${time}</div>
            `;
            list.appendChild(li);
        });
    },

    close() {
        document.getElementById('country-panel').classList.add('hidden');
    }
};
