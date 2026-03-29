const SCORE_LABELS = {
    composite: 'GeoRisk Index',
    base_score: 'Base Model',
    news_score: 'News Signal'
};

const MapModule = {
    svg: null,
    g: null,  // group for zoomable content
    projection: null,
    pathGenerator: null,
    colorScale: null,
    countryCodeMap: {},
    scores: {},
    tooltip: null,
    zoom: null,
    activeScoreField: 'composite',

    async init(containerId) {
        this.tooltip = document.getElementById('map-tooltip');

        const container = document.querySelector(containerId);
        const width = 960;
        const height = 500;

        this.svg = d3.select(containerId)
            .append('svg')
            .attr('viewBox', `0 0 ${width} ${height}`)
            .attr('preserveAspectRatio', 'xMidYMid meet');

        // Ocean background (fixed, not zoomable)
        this.svg.append('rect')
            .attr('width', width)
            .attr('height', height)
            .attr('fill', '#0a0e1a');

        // Zoomable group for all map content
        this.g = this.svg.append('g');

        this.projection = d3.geoNaturalEarth1()
            .scale(153)
            .translate([width / 2, height / 2]);

        this.pathGenerator = d3.geoPath().projection(this.projection);

        this.colorScale = d3.scaleLinear()
            .domain([0, 25, 50, 75, 100])
            .range(['#10b981', '#10b981', '#f59e0b', '#f97316', '#ef4444'])
            .clamp(true);

        // Setup zoom behavior
        this.zoom = d3.zoom()
            .scaleExtent([1, 8])
            .on('zoom', (event) => {
                this.g.attr('transform', event.transform);
            });

        this.svg.call(this.zoom);

        // Double-click to reset zoom
        this.svg.on('dblclick.zoom', () => {
            this.svg.transition()
                .duration(500)
                .call(this.zoom.transform, d3.zoomIdentity);
        });

        await this.loadCountryCodes();
        await this.loadMap();

        this.renderLegend();
    },

    async loadCountryCodes() {
        try {
            const res = await fetch('/static/data/country_codes.json');
            const countries = await res.json();
            countries.forEach(c => {
                const num = c['country-code'];
                const alpha2 = c['alpha-2'];
                if (num && alpha2 && num !== '-99') {
                    // Store both zero-padded ("032") and stripped ("32") keys
                    // world-atlas TopoJSON uses zero-padded string IDs
                    this.countryCodeMap[num] = alpha2;
                    this.countryCodeMap[String(parseInt(num))] = alpha2;
                }
            });
        } catch (e) {
            console.error('Failed to load country codes:', e);
        }
    },

    async loadMap() {
        try {
            const world = await d3.json(
                'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json'
            );
            const countries = topojson.feature(world, world.objects.countries);

            this.g.selectAll('path.country')
                .data(countries.features)
                .join('path')
                .attr('class', 'country')
                .attr('d', this.pathGenerator)
                .attr('fill', '#1f2937')
                .attr('stroke', '#2d3748')
                .attr('stroke-width', 0.5)
                .on('mouseover', (event, d) => this.handleHover(event, d))
                .on('mousemove', (event) => this.moveTooltip(event))
                .on('mouseout', () => this.handleHoverEnd())
                .on('click', (event, d) => this.handleClick(event, d));

        } catch (e) {
            console.error('Failed to load map:', e);
        }
    },

    getScoreValue(scoreData) {
        if (!scoreData) return undefined;
        return scoreData[this.activeScoreField];
    },

    setScoreField(field) {
        this.activeScoreField = field;

        // Update button active states
        document.querySelectorAll('.score-filter-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.score === field);
        });

        // Re-color the map with smooth transition
        this.g.selectAll('path.country')
            .transition()
            .duration(600)
            .attr('fill', d => {
                const alpha2 = this.countryCodeMap[String(d.id)];
                const scoreData = this.scores[alpha2];
                const value = this.getScoreValue(scoreData);
                if (value === undefined) return '#1f2937';
                return this.colorScale(value);
            });

        // Re-evaluate hotspots based on selected score type
        this.highlightHotspots();

        // Update legend label
        this.renderLegend();
    },

    async updateScores() {
        this.scores = await ApiClient.getScores();

        this.g.selectAll('path.country')
            .transition()
            .duration(600)
            .attr('fill', d => {
                const alpha2 = this.countryCodeMap[String(d.id)];
                const scoreData = this.scores[alpha2];
                const value = this.getScoreValue(scoreData);
                if (value === undefined) return '#1f2937';
                return this.colorScale(value);
            });

        this.highlightHotspots();

        // Hide loading
        const loading = document.getElementById('map-loading');
        if (loading) loading.classList.add('hidden');
    },

    highlightHotspots() {
        this.g.selectAll('path.country')
            .classed('warzone', d => {
                const alpha2 = this.countryCodeMap[String(d.id)];
                const scoreData = this.scores[alpha2];
                if (!scoreData || !scoreData.indicators) return false;
                return scoreData.indicators.military_conflict > 85;
            })
            .classed('hotspot', d => {
                const alpha2 = this.countryCodeMap[String(d.id)];
                const scoreData = this.scores[alpha2];
                if (!scoreData || !scoreData.indicators) return false;
                // hotspot only if NOT already a warzone
                if (scoreData.indicators.military_conflict > 85) return false;
                const value = this.getScoreValue(scoreData);
                return value !== undefined && value > 70;
            });
    },

    handleHover(event, d) {
        const alpha2 = this.countryCodeMap[String(d.id)];
        const scoreData = this.scores[alpha2];

        let name = alpha2 || 'Unknown';
        let scoreHtml = '<span class="tooltip-label">No data</span>';

        if (scoreData) {
            name = scoreData.country_name || alpha2;
            const score = this.getScoreValue(scoreData);
            const color = Utils.riskColor(score);
            const label = Utils.riskLabel(score);
            const base = scoreData.base_score || 0;
            const news = scoreData.news_score || 0;
            const articles = scoreData.headline_count || 0;
            const baseColor = Utils.riskColor(base);
            const newsColor = Utils.riskColor(news);
            const isBaseActive = this.activeScoreField === 'base_score';
            const isNewsActive = this.activeScoreField === 'news_score';
            const scoreLabel = SCORE_LABELS[this.activeScoreField];
            const isWarzone = scoreData.indicators && scoreData.indicators.military_conflict > 85;

            // Show "No news data" hint when viewing News Signal with zero articles
            const newsDisplay = (isNewsActive && news === 0 && articles === 0)
                ? '<span class="tier-value" style="color: #6b7280">N/A</span>'
                : `<span class="tier-value" style="color: ${newsColor}">${news}</span>`;

            const warzoneBadge = isWarzone
                ? '<div class="tooltip-warzone-badge">Active Conflict</div>'
                : '';

            scoreHtml = `
                ${warzoneBadge}
                <div class="tooltip-score" style="color: ${color}">${score}</div>
                <span class="tooltip-label">${scoreLabel} — ${label} Risk</span>
                <div class="tooltip-breakdown">
                    <div class="tooltip-tier ${isBaseActive ? 'tier-active' : ''}">
                        <span class="tier-label">Base</span>
                        <span class="tier-value" style="color: ${baseColor}">${base}</span>
                    </div>
                    <div class="tooltip-tier ${isNewsActive ? 'tier-active' : ''}">
                        <span class="tier-label">News</span>
                        ${newsDisplay}
                    </div>
                    <div class="tooltip-tier">
                        <span class="tier-label">Articles</span>
                        <span class="tier-value">${articles}</span>
                    </div>
                </div>
            `;
        }

        this.tooltip.innerHTML = `
            <div class="tooltip-name">${Utils.escapeHtml(name)}</div>
            ${scoreHtml}
        `;
        this.tooltip.classList.remove('hidden');
        this.moveTooltip(event);
    },

    moveTooltip(event) {
        const x = event.pageX + 12;
        const y = event.pageY - 10;
        this.tooltip.style.left = x + 'px';
        this.tooltip.style.top = y + 'px';
    },

    handleHoverEnd() {
        this.tooltip.classList.add('hidden');
    },

    handleClick(event, d) {
        const alpha2 = this.countryCodeMap[String(d.id)];
        if (alpha2 && this.scores[alpha2]) {
            PanelModule.open(alpha2);
        }
    },

    renderLegend() {
        const legend = document.getElementById('map-legend');
        const title = SCORE_LABELS[this.activeScoreField] || 'GeoRisk Index';
        legend.innerHTML = `
            <span>Low Risk</span>
            <div>
                <div class="legend-bar"></div>
                <div class="legend-labels">
                    <span>0</span>
                    <span>25</span>
                    <span>50</span>
                    <span>75</span>
                    <span>100</span>
                </div>
                <div class="legend-title">${title}</div>
            </div>
            <span>Critical</span>
            <span class="legend-separator"></span>
            <span class="legend-warzone-icon"></span>
            <span>Active Conflict</span>
        `;
    }
};
