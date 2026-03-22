const MapModule = {
    svg: null,
    projection: null,
    pathGenerator: null,
    colorScale: null,
    countryCodeMap: {},
    scores: {},
    tooltip: null,

    async init(containerId) {
        this.tooltip = document.getElementById('map-tooltip');

        const container = document.querySelector(containerId);
        const width = 960;
        const height = 500;

        this.svg = d3.select(containerId)
            .append('svg')
            .attr('viewBox', `0 0 ${width} ${height}`)
            .attr('preserveAspectRatio', 'xMidYMid meet');

        // Ocean background
        this.svg.append('rect')
            .attr('width', width)
            .attr('height', height)
            .attr('fill', '#0a0e1a');

        this.projection = d3.geoNaturalEarth1()
            .scale(153)
            .translate([width / 2, height / 2]);

        this.pathGenerator = d3.geoPath().projection(this.projection);

        this.colorScale = d3.scaleLinear()
            .domain([0, 25, 50, 75, 100])
            .range(['#10b981', '#10b981', '#f59e0b', '#f97316', '#ef4444'])
            .clamp(true);

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

            this.svg.selectAll('path.country')
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

    async updateScores() {
        this.scores = await ApiClient.getScores();

        this.svg.selectAll('path.country')
            .transition()
            .duration(600)
            .attr('fill', d => {
                const alpha2 = this.countryCodeMap[String(d.id)];
                const scoreData = this.scores[alpha2];
                if (!scoreData || scoreData.composite === undefined) return '#1f2937';
                return this.colorScale(scoreData.composite);
            });

        this.highlightHotspots();

        // Hide loading
        const loading = document.getElementById('map-loading');
        if (loading) loading.classList.add('hidden');
    },

    highlightHotspots() {
        this.svg.selectAll('path.country')
            .classed('hotspot', d => {
                const alpha2 = this.countryCodeMap[String(d.id)];
                const scoreData = this.scores[alpha2];
                return scoreData && scoreData.composite > 70;
            });
    },

    handleHover(event, d) {
        const alpha2 = this.countryCodeMap[String(d.id)];
        const scoreData = this.scores[alpha2];

        let name = alpha2 || 'Unknown';
        let scoreHtml = '<span class="tooltip-label">No data</span>';

        if (scoreData) {
            name = scoreData.country_name || alpha2;
            const score = scoreData.composite;
            const color = Utils.riskColor(score);
            const label = Utils.riskLabel(score);
            scoreHtml = `
                <div class="tooltip-score" style="color: ${color}">${score}</div>
                <span class="tooltip-label">${label} Risk</span>
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
            </div>
            <span>Critical</span>
        `;
    }
};
