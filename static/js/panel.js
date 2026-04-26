const PanelModule = {
    radarChart: null,
    historyChart: null,
    currentCountryCode: null,
    currentHistoryDays: 30,

    async open(countryCode) {
        this.currentCountryCode = countryCode;

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
        this.renderHistoryChart(countryCode, this.currentHistoryDays);
        this.renderHeadlines(headlines.articles || []);
        this.bindHistoryControls();
        this.bindShareButton(detail.country_name || countryCode, countryCode);

        document.getElementById('country-panel').classList.remove('hidden');
    },

    bindShareButton(countryName, countryCode) {
        const btn = document.getElementById('panel-share-btn');
        if (!btn || btn._bound) {
            // Still need to update the dataset URL on re-open
            if (btn) btn.dataset.shareUrl = this._shareUrl(countryCode);
            return;
        }
        btn._bound = true;
        btn.dataset.shareUrl = this._shareUrl(countryCode);

        btn.addEventListener('click', (e) => {
            e.preventDefault();
            const url = btn.dataset.shareUrl;
            const label = btn.querySelector('.share-btn-label');
            const prev = label ? label.textContent : null;
            const doCopy = navigator.clipboard && window.isSecureContext
                ? navigator.clipboard.writeText(url).then(() => true, () => false)
                : Promise.resolve((() => {
                    try {
                        const ta = document.createElement('textarea');
                        ta.value = url;
                        ta.style.position = 'fixed';
                        ta.style.opacity = '0';
                        document.body.appendChild(ta);
                        ta.select();
                        const ok = document.execCommand('copy');
                        document.body.removeChild(ta);
                        return ok;
                    } catch (_) { return false; }
                })());

            doCopy.then((ok) => {
                if (!ok) return;
                btn.classList.add('copied');
                if (label) label.textContent = 'Copied';
                let toast = document.getElementById('share-toast');
                if (!toast) {
                    toast = document.createElement('div');
                    toast.id = 'share-toast';
                    toast.className = 'share-toast';
                    document.body.appendChild(toast);
                }
                toast.textContent = 'Link copied — paste it into LinkedIn, X, or Substack';
                requestAnimationFrame(() => toast.classList.add('show'));
                clearTimeout(toast._hideTimer);
                toast._hideTimer = setTimeout(() => toast.classList.remove('show'), 2200);
                setTimeout(() => {
                    btn.classList.remove('copied');
                    if (label && prev !== null) label.textContent = prev;
                }, 1800);
            });
        });
    },

    _shareUrl(countryCode) {
        // A georisk country detail is a modal, not its own URL, so we share
        // the GeoRisk page itself (which has an OG preview) plus a hash so
        // the modal can reopen when the page loads (future enhancement).
        const base = window.location.origin + '/georisk';
        return countryCode ? base + '#country=' + encodeURIComponent(countryCode) : base;
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

    async renderHistoryChart(countryCode, days) {
        const canvasEl = document.getElementById('panel-history-chart');
        if (!canvasEl) return;

        const data = await ApiClient.getCountryHistory(countryCode, days);
        const series = data.series || [];

        const ctx = canvasEl.getContext('2d');
        if (this.historyChart) this.historyChart.destroy();

        if (series.length === 0) {
            // No history data yet — show placeholder
            canvasEl.style.display = 'none';
            let placeholder = document.getElementById('history-placeholder');
            if (!placeholder) {
                placeholder = document.createElement('p');
                placeholder.id = 'history-placeholder';
                placeholder.style.cssText = 'color: #6b7280; font-size: 12px; text-align: center; padding: 20px 0;';
                canvasEl.parentNode.insertBefore(placeholder, canvasEl.nextSibling);
            }
            placeholder.textContent = 'Score history will appear after daily data accumulates.';
            placeholder.style.display = '';
            return;
        }

        canvasEl.style.display = '';
        const placeholder = document.getElementById('history-placeholder');
        if (placeholder) placeholder.style.display = 'none';

        const labels = series.map(d => d.date);
        const compositeData = series.map(d => d.composite_score);
        const baseData = series.map(d => d.base_score);
        const newsData = series.map(d => d.news_score);

        this.historyChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Composite',
                        data: compositeData,
                        borderColor: '#ef4444',
                        backgroundColor: 'rgba(239, 68, 68, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        pointRadius: 0,
                        tension: 0.3,
                    },
                    {
                        label: 'Base Score',
                        data: baseData,
                        borderColor: 'rgba(59, 130, 246, 0.5)',
                        borderWidth: 1.5,
                        borderDash: [4, 2],
                        fill: false,
                        pointRadius: 0,
                        tension: 0.3,
                    },
                    {
                        label: 'News Score',
                        data: newsData,
                        borderColor: 'rgba(245, 158, 11, 0.5)',
                        borderWidth: 1.5,
                        borderDash: [4, 2],
                        fill: false,
                        pointRadius: 0,
                        tension: 0.3,
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            color: '#9ca3af',
                            font: { size: 10 },
                            boxWidth: 10,
                            padding: 8,
                            usePointStyle: true,
                            pointStyle: 'line'
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#fff',
                        bodyColor: '#d1d5db',
                        callbacks: {
                            label: (ctx) => {
                                const val = ctx.parsed.y;
                                return ctx.dataset.label + ': ' + (val != null ? val.toFixed(1) : 'N/A');
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: {
                            color: '#6b7280',
                            font: { size: 9 },
                            maxTicksLimit: 8,
                            maxRotation: 0
                        },
                        grid: { color: 'rgba(55,65,81,0.2)' }
                    },
                    y: {
                        min: 0,
                        max: 100,
                        ticks: {
                            color: '#6b7280',
                            font: { size: 9 },
                            stepSize: 25
                        },
                        grid: { color: 'rgba(55,65,81,0.2)' }
                    }
                }
            }
        });
    },

    bindHistoryControls() {
        const buttons = document.querySelectorAll('.history-range-btn');
        buttons.forEach(btn => {
            // Remove existing listeners by cloning
            const newBtn = btn.cloneNode(true);
            btn.parentNode.replaceChild(newBtn, btn);

            newBtn.addEventListener('click', () => {
                const days = parseInt(newBtn.dataset.days);
                this.currentHistoryDays = days;

                // Update active state
                document.querySelectorAll('.history-range-btn').forEach(b => b.classList.remove('active'));
                newBtn.classList.add('active');

                // Re-render chart
                if (this.currentCountryCode) {
                    this.renderHistoryChart(this.currentCountryCode, days);
                }
            });
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
        if (this.historyChart) {
            this.historyChart.destroy();
            this.historyChart = null;
        }
        this.currentCountryCode = null;
        document.getElementById('country-panel').classList.add('hidden');
    }
};
