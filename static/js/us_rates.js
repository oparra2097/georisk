/*
 * /us-rates — market fed funds futures curve vs. macro-model p10/p50/p90
 * fan, plus a CME FedWatch–style per-FOMC-meeting probability grid.
 *
 * Fetches:
 *   /api/us-rates/curve       - ZQ futures curve (yfinance)
 *   /api/us-rates/fedwatch    - probability grid
 *   /api/us-rates/edge        - top-3 market-vs-model divergences
 *   /api/macro-model/us/fan   - macro model fedfunds p10/p50/p90 (needs
 *                               macro access; degrades gracefully)
 *
 * All rendering is dark-theme-first — colors come from CSS custom
 * properties on `.usr-page` (--usr-accent, --usr-warn, --usr-good, etc.).
 */

(function () {
  'use strict';

  const $ = (id) => document.getElementById(id);

  // ── State ────────────────────────────────────────────────────────────
  const state = { curve: null, fedwatch: null, edge: null, modelFan: null };

  // ── Kick off all fetches in parallel ─────────────────────────────────
  Promise.all([
    fetch('/api/us-rates/curve').then((r) => (r.ok ? r.json() : null)).catch(() => null),
    fetch('/api/us-rates/fedwatch').then((r) => (r.ok ? r.json() : null)).catch(() => null),
    fetch('/api/us-rates/edge?top_n=5').then((r) => (r.ok ? r.json() : null)).catch(() => null),
    fetch('/api/macro-model/us/fan?horizon=8&n_draws=30')
      .then((r) => (r.ok ? r.json() : null))
      .catch(() => null),
  ]).then(([curve, fedwatch, edge, fan]) => {
    state.curve = curve;
    state.fedwatch = fedwatch;
    state.edge = edge;
    state.modelFan = fan;
    renderHero();
    renderChart();
    renderFedWatch();
    renderEdge();
  });

  // ── Hero strip ───────────────────────────────────────────────────────
  function renderHero() {
    const fw = state.fedwatch;
    if (!fw) return;

    // Target midpoint
    if (fw.target_mid != null) {
      const lo = fw.target_mid - 0.125;
      const hi = fw.target_mid + 0.125;
      $('hero-target').textContent = `${lo.toFixed(2)}–${hi.toFixed(2)}%`;
    }

    const nearest = (fw.grid || []).find((r) => r.probs != null);
    if (!nearest) return;

    // Next FOMC
    const d = new Date(nearest.meeting_date + 'T12:00:00Z');
    $('hero-next-meeting').textContent = d.toLocaleDateString('en-US', {
      month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC',
    });
    const daysAway = Math.round((d - new Date()) / (1000 * 60 * 60 * 24));
    $('hero-next-days').textContent = daysAway > 0
      ? `${daysAway} day${daysAway === 1 ? '' : 's'} away`
      : 'today or past';

    // Market call — highest-probability bucket for the near meeting
    const marketBucket = topBucket(nearest.probs);
    if (marketBucket) {
      $('hero-market-call').textContent = formatBucket(marketBucket.label);
      const pct = Math.round(marketBucket.p * 100);
      $('hero-market-detail').textContent = `${pct}% probability`;
    }

    // Model call — from edge table if we have it
    const edges = (state.edge || {}).edges || [];
    const modelForNext = edges.find((e) => e.meeting_date === nearest.meeting_date);
    if (modelForNext) {
      const delta = modelForNext.model_post_rate - nearest.pre_rate;
      $('hero-model-call').textContent = formatDeltaCall(delta * 100);
      $('hero-model-detail').textContent = modelForNext.model_post_rate.toFixed(2) + '% post-meeting';
    } else {
      $('hero-model-call').textContent = 'n/a';
      $('hero-model-detail').textContent = 'macro model unavailable';
    }
  }

  function topBucket(probs) {
    if (!probs) return null;
    let top = null;
    for (const label of Object.keys(probs)) {
      const p = probs[label];
      if (top == null || p > top.p) top = { label, p };
    }
    return top;
  }

  function formatBucket(label) {
    if (label === 'hold') return 'Hold';
    return label.startsWith('-') ? label + ' cut' : label + ' hike';
  }

  function formatDeltaCall(deltaBp) {
    const rounded = Math.round(deltaBp / 25) * 25;
    if (Math.abs(rounded) < 12.5) return 'Hold';
    return rounded > 0 ? `+${rounded}bp hike` : `${rounded}bp cut`;
  }

  // ── D3 fan chart ─────────────────────────────────────────────────────
  function renderChart() {
    const host = $('usr-chart');
    if (!host || typeof d3 === 'undefined') return;

    const curve = ((state.curve || {}).curve || []).filter((c) => c.implied_rate != null);
    const modelBands = ((state.modelFan || {}).bands || {}).fedfunds || [];
    const spot = (state.curve || {}).spot || null;

    if (!curve.length && !modelBands.length) {
      host.innerHTML = '<div class="usr-empty">No curve data available. Try again in a few minutes.</div>';
      return;
    }
    $('chart-note').textContent = ((state.curve || {}).source || 'unknown') +
      ' · ' + curve.length + ' contracts';

    // Build combined series with parseable dates
    const marketSeries = curve.map((c) => ({
      x: new Date(c.year, c.month - 1, 15),   // mid-month
      y: c.implied_rate,
    }));
    const modelSeries = modelBands.map((r) => ({
      x: new Date(r.quarter),
      y_p10: r.p10, y_p50: r.p50, y_p90: r.p90,
    })).filter((r) => r.y_p50 != null);

    // Combined x/y extents
    const allX = [
      ...marketSeries.map((d) => d.x),
      ...modelSeries.map((d) => d.x),
    ];
    if (spot && spot.date) allX.push(new Date(spot.date));
    const allY = [
      ...marketSeries.map((d) => d.y),
      ...modelSeries.flatMap((d) => [d.y_p10, d.y_p50, d.y_p90]),
    ].filter((y) => y != null);
    if (spot && spot.rate != null) allY.push(spot.rate);

    const xExtent = d3.extent(allX);
    const yExtent = d3.extent(allY);
    // Pad y by 10% each side for visual breathing room
    const yPad = (yExtent[1] - yExtent[0]) * 0.15 || 0.5;
    const yMin = Math.max(0, yExtent[0] - yPad);
    const yMax = yExtent[1] + yPad;

    // Layout
    const W = host.clientWidth || 800;
    const H = 380;
    const M = { top: 20, right: 24, bottom: 40, left: 44 };
    const iw = W - M.left - M.right;
    const ih = H - M.top - M.bottom;

    host.innerHTML = '';
    const svg = d3.select(host).append('svg')
      .attr('viewBox', `0 0 ${W} ${H}`)
      .attr('preserveAspectRatio', 'xMidYMid meet');

    const g = svg.append('g').attr('transform', `translate(${M.left},${M.top})`);

    const x = d3.scaleTime().domain(xExtent).range([0, iw]).nice();
    const y = d3.scaleLinear().domain([yMin, yMax]).range([ih, 0]).nice();

    // Horizontal grid lines
    y.ticks(5).forEach((tv) => {
      g.append('line')
        .attr('class', 'grid-line')
        .attr('x1', 0).attr('x2', iw)
        .attr('y1', y(tv)).attr('y2', y(tv));
    });

    // Axes labels
    x.ticks(6).forEach((tv) => {
      g.append('text')
        .attr('class', 'axis-label')
        .attr('x', x(tv)).attr('y', ih + 18)
        .attr('text-anchor', 'middle')
        .text(d3.timeFormat('%b %Y')(tv));
    });
    y.ticks(5).forEach((tv) => {
      g.append('text')
        .attr('class', 'axis-label')
        .attr('x', -8).attr('y', y(tv) + 4)
        .attr('text-anchor', 'end')
        .text(tv.toFixed(2) + '%');
    });
    g.append('line').attr('class', 'axis-line')
      .attr('x1', 0).attr('x2', iw).attr('y1', ih).attr('y2', ih);
    g.append('line').attr('class', 'axis-line')
      .attr('x1', 0).attr('x2', 0).attr('y1', 0).attr('y2', ih);

    // Model fan band (p10 → p90)
    if (modelSeries.length) {
      const area = d3.area()
        .defined((d) => d.y_p10 != null && d.y_p90 != null)
        .x((d) => x(d.x))
        .y0((d) => y(d.y_p10))
        .y1((d) => y(d.y_p90))
        .curve(d3.curveMonotoneX);
      g.append('path')
        .datum(modelSeries)
        .attr('class', 'fan-band')
        .attr('d', area);

      const line = d3.line()
        .defined((d) => d.y_p50 != null)
        .x((d) => x(d.x))
        .y((d) => y(d.y_p50))
        .curve(d3.curveMonotoneX);
      g.append('path')
        .datum(modelSeries)
        .attr('class', 'model-line')
        .attr('d', line);
    }

    // Market line
    if (marketSeries.length) {
      const mline = d3.line()
        .x((d) => x(d.x))
        .y((d) => y(d.y))
        .curve(d3.curveMonotoneX);
      g.append('path')
        .datum(marketSeries)
        .attr('class', 'market-line')
        .attr('d', mline);
      g.selectAll('.market-dot')
        .data(marketSeries)
        .enter().append('circle')
        .attr('class', 'market-dot')
        .attr('cx', (d) => x(d.x))
        .attr('cy', (d) => y(d.y))
        .attr('r', 2.5);
    }

    // Spot dot
    if (spot && spot.rate != null && spot.date) {
      g.append('circle')
        .attr('class', 'spot-dot')
        .attr('cx', x(new Date(spot.date)))
        .attr('cy', y(spot.rate))
        .attr('r', 5);
    }
  }

  // ── FedWatch grid ────────────────────────────────────────────────────
  function renderFedWatch() {
    const tbody = $('fedwatch-tbody');
    if (!tbody) return;
    const grid = ((state.fedwatch || {}).grid || []);

    if (!grid.length) {
      tbody.innerHTML = '<tr><td colspan="6" class="usr-empty">No FOMC meetings on the near-term calendar.</td></tr>';
      $('fedwatch-note').textContent = 'no data';
      return;
    }
    $('fedwatch-note').textContent = grid.length + ' meetings · CME methodology';

    const buckets = ['-50bp', '-25bp', 'hold', '+25bp', '+50bp'];
    tbody.innerHTML = grid.map((row) => {
      const dateStr = formatMeetingDate(row.meeting_date);
      if (!row.probs) {
        return `
          <tr>
            <td>
              <span class="meeting-date">${dateStr}</span>
              <span class="meeting-post">no contract available</span>
            </td>
            <td colspan="5" class="usr-empty" style="text-align:center;padding:12px">—</td>
          </tr>
        `;
      }
      // Highlight the leading (highest-p) bucket
      const top = topBucket(row.probs);
      const cells = buckets.map((b) => {
        const p = row.probs[b] || 0;
        const cls = b === 'hold' ? 'prob-hold'
                   : b.startsWith('-') ? 'prob-cut' : 'prob-hike';
        const leading = top && top.label === b ? ' leading' : '';
        const pct = (p * 100).toFixed(0);
        return `<td class="prob-cell ${cls}${leading}" style="--p:${p}">${pct}%</td>`;
      }).join('');
      const postStr = row.post_rate != null
        ? `implied post ${row.post_rate.toFixed(2)}%`
        : '';
      return `
        <tr>
          <td>
            <span class="meeting-date">${dateStr}</span>
            <span class="meeting-post">${postStr}</span>
          </td>
          ${cells}
        </tr>
      `;
    }).join('');
  }

  function formatMeetingDate(iso) {
    const d = new Date(iso + 'T12:00:00Z');
    return d.toLocaleDateString('en-US', {
      month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC',
    });
  }

  // ── Edge table ───────────────────────────────────────────────────────
  function renderEdge() {
    const tbody = $('edge-tbody');
    if (!tbody) return;
    const edges = ((state.edge || {}).edges || []);

    if (!edges.length) {
      const err = (state.edge || {}).error;
      tbody.innerHTML = `<tr><td colspan="5" class="usr-empty">${err || 'No divergences over the current horizon.'}</td></tr>`;
      return;
    }

    tbody.innerHTML = edges.map((e) => {
      const dateStr = formatMeetingDate(e.meeting_date);
      const dirCls = e.direction && e.direction.startsWith('SHORT') ? 'short' : 'long';
      const edgeStr = (e.edge_bp > 0 ? '+' : '') + e.edge_bp;
      return `
        <tr>
          <td>${dateStr}</td>
          <td><span class="usr-edge-side ${dirCls}">${e.direction}</span></td>
          <td class="num">${e.market_post_rate.toFixed(2)}%</td>
          <td class="num">${e.model_post_rate.toFixed(2)}%</td>
          <td class="num" style="font-weight:600">${edgeStr}</td>
        </tr>
      `;
    }).join('');
  }
})();
