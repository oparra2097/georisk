/* US Labor Market dashboard — economist-grade layout.
 *
 * Single fetch of /api/labor-market/us; everything else is client-side
 * (window toggles, MoM/YoY ranking switch, countdown timer).  The
 * payload includes:
 *   bls.series           per-series points with mom_change, yoy_change…
 *   bls.rankings.mom/yoy sectoral payrolls already ranked at the latest month
 *   bls.sector_keys      ordered list of sector keys used in the sparkline grid
 *   nowcast              bridge-model backcast + forward nowcast
 *   next_release         {release_date, time_eastern, seconds_until, …}
 *   upcoming_releases    next ~6 BLS releases across employment + CPI
 */
/* eslint-env browser */
/* global Chart */

(function () {
  'use strict';

  const API = {
    bundle: '/api/labor-market/us',
    refresh: '/api/labor-market/us/refresh',
  };

  const COLOR = {
    bls:           '#3b82f6',
    blsBar:        'rgba(59,130,246,0.65)',
    estLine:       '#f59e0b',
    estBar:        'rgba(245,158,11,0.45)',
    u3:            '#ef4444',
    u6:            '#f97316',
    lfp:           '#a855f7',
    eppop:         '#8b5cf6',
    wage:          '#10b981',
    hours:         '#14b8a6',
    pos:           '#10b981',
    neg:           '#ef4444',
    grid:          'rgba(255,255,255,0.06)',
    text:          '#9ca3af',
  };

  const state = {
    bundle: null,
    windowMonths: 24,
    rankBy: 'mom',          // 'mom' or 'yoy'
    tileSort: 'group',      // 'group' or 'yoy'
    charts: {},             // keyed by canvas id
    countdownTimer: null,
  };

  const SECTOR_GROUP_LABELS = {
    goods:      'Goods-producing',
    services:   'Services',
    government: 'Government',
    headline:   'Headline aggregates',
  };

  document.addEventListener('DOMContentLoaded', () => {
    bindToolbar();
    bindRankToggle();
    bindTileSort();
    bindRefresh();
    bindStaleRefresh();
    fetchBundle();
  });

  // ── Wiring ─────────────────────────────────────────────────

  function bindToolbar() {
    document.querySelectorAll('.lm-toggle[data-window]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.lm-toggle[data-window]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.windowMonths = parseInt(btn.dataset.window, 10) || 0;
        drawPayrollChart();
        drawUnrateChart();
        drawWagesChart();
        drawParticipationChart();
        renderSparklines();
      });
    });
  }

  function bindRankToggle() {
    document.querySelectorAll('.lm-toggle[data-rank]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.lm-toggle[data-rank]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.rankBy = btn.dataset.rank;
        drawSectorRanking();
      });
    });
  }

  function bindTileSort() {
    document.querySelectorAll('.lm-toggle[data-tile-sort]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.lm-toggle[data-tile-sort]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.tileSort = btn.dataset.tileSort;
        renderSparklines();
      });
    });
  }

  function bindRefresh() {
    const btn = document.getElementById('lm-refresh-btn');
    if (!btn) return;
    btn.addEventListener('click', () => doRefresh(btn));
  }
  function bindStaleRefresh() {
    const btn = document.getElementById('lm-stale-refresh');
    if (!btn) return;
    btn.addEventListener('click', () => doRefresh(btn));
  }
  async function doRefresh(btn) {
    btn.disabled = true;
    const prev = btn.textContent;
    btn.textContent = 'Refreshing…';
    try {
      await fetch(API.refresh, { method: 'POST' });
      await fetchBundle();
    } catch (e) { console.error(e); }
    finally {
      btn.disabled = false;
      btn.textContent = prev;
    }
  }

  async function fetchBundle() {
    try {
      const resp = await fetch(API.bundle, { credentials: 'same-origin' });
      const data = await resp.json();
      state.bundle = data || {};
      renderStaleBanner();
      renderReleaseBanner();
      renderKPIs();
      drawSectorRanking();
      renderSparklines();
      drawPayrollChart();
      drawUnrateChart();
      drawWagesChart();
      drawParticipationChart();
      renderTrackRecord();
      renderIndicators();
      renderUpcomingReleases();
    } catch (e) {
      console.error('labor market fetch failed', e);
    }
  }

  function renderStaleBanner() {
    const blsMeta = ((state.bundle.bls || {}).meta) || {};
    const freshness = ((state.bundle.nowcast || {}).data_freshness) || {};
    const banner = document.getElementById('lm-stale-banner');
    if (!banner) return;
    const blsStale = !!blsMeta.is_stale;
    const fredStale = !!freshness.is_stale;
    if (!blsStale && !fredStale) { banner.hidden = true; return; }

    const detail = document.getElementById('lm-stale-detail');
    if (detail) detail.innerHTML = buildStaleDetail(blsMeta, freshness, '/api/labor-market/us/diagnostics');
    banner.hidden = false;
  }

  function buildStaleDetail(blsMeta, freshness, diagUrl) {
    const parts = [];
    const blsStale = !!blsMeta.is_stale;
    const fredStale = !!freshness.is_stale;

    // BLS feed (sectoral charts, KPIs)
    if (blsStale) {
      const blsM = blsMeta.months_behind;
      parts.push(`<strong>BLS feed:</strong> latest <code>${blsMeta.latest_month || 'unknown'}</code>` +
        (blsM > 0 ? ` (${blsM} months behind).` : '.'));
      if (blsMeta.has_api_key === false) {
        parts.push('<code>BLS_API_KEY</code> not set on server.');
      } else if (blsMeta.bls_api_status && blsMeta.bls_api_status !== 'REQUEST_SUCCEEDED') {
        parts.push(`API status <code>${blsMeta.bls_api_status}</code>` +
          (blsMeta.bls_api_message ? ` &mdash; ${blsMeta.bls_api_message}.` : '.'));
      }
    }

    // FRED feed (labor model / nowcast)
    if (fredStale) {
      const m = freshness.months_behind;
      const sep = parts.length ? ' <br>' : '';
      parts.push(`${sep}<strong>Labor model (FRED feed):</strong> latest PAYEMS <code>${freshness.last_fred_payems_month || 'unknown'}</code>` +
        (m > 0 ? ` (${m} months behind).` : '.') +
        ' The model uses FRED, not BLS direct &mdash; if BLS looks current but the model lags, the FRED cache or <code>FRED_API_KEY</code> is the culprit.');
    }

    parts.push(`<br><a href="${diagUrl}" target="_blank" rel="noopener" class="lm-stale-link">Run diagnostics (BLS + FRED) &rarr;</a>`);
    return parts.join(' ');
  }

  // ── Release banner + countdown ─────────────────────────────

  function renderReleaseBanner() {
    const r = state.bundle.next_release;
    if (!r || r.unavailable) return;
    setText('lm-release-date', formatReleaseDate(r.release_date));
    setText('lm-release-time', r.time_eastern || '08:30 ET');
    if (r.data_label) setText('lm-release-data-label', r.data_label);

    if (state.countdownTimer) clearInterval(state.countdownTimer);
    const target = new Date(r.release_at_utc).getTime();
    function tick() {
      const now = Date.now();
      const delta = Math.max(0, Math.floor((target - now) / 1000));
      const days = Math.floor(delta / 86400);
      const hours = Math.floor((delta % 86400) / 3600);
      const mins = Math.floor((delta % 3600) / 60);
      setText('lm-cd-days', String(days));
      setText('lm-cd-hours', String(hours));
      setText('lm-cd-mins', String(mins));
      const banner = document.getElementById('lm-release-banner');
      if (banner) {
        banner.classList.toggle('lm-release-banner--imminent', delta <= 86400);
        banner.classList.toggle('lm-release-banner--today', delta <= 12 * 3600);
      }
    }
    tick();
    state.countdownTimer = setInterval(tick, 30000);
  }

  function renderUpcomingReleases() {
    const list = document.getElementById('lm-release-list');
    if (!list) return;
    const releases = state.bundle.upcoming_releases || [];
    if (!releases.length) {
      list.innerHTML = '<li class="lm-release-empty">No scheduled releases on file.</li>';
      return;
    }
    list.innerHTML = releases.map(r => `
      <li class="lm-release-row ${r.is_today ? 'lm-release-row--today' : ''}">
        <span class="lm-rel-date">${formatReleaseDate(r.release_date)}</span>
        <span class="lm-rel-label">${r.label}</span>
        <span class="lm-rel-meta">${r.data_label || ''}</span>
        <span class="lm-rel-countdown">${r.days_until <= 0 ? 'today' : 'in ' + r.days_until + 'd'}</span>
      </li>`).join('');
  }

  // ── KPI strip ──────────────────────────────────────────────

  function renderKPIs() {
    const series = ((state.bundle.bls || {}).series) || {};
    const ind = ((state.bundle.bls || {}).meta) || {};

    const payrolls = series.payrolls || [];
    const last = payrolls[payrolls.length - 1];
    if (last) {
      setText('lm-kpi-payrolls', formatJobs(last.mom_change));
      setText('lm-kpi-payrolls-meta', `${formatMonth(last.date)} · YoY +${formatJobs(last.yoy_change_level, false)}`);
    }

    bindRate('lm-kpi-unrate', 'lm-kpi-unrate-meta', series.unemployment);
    bindRate('lm-kpi-u6', 'lm-kpi-u6-meta', series.u6);
    bindRate('lm-kpi-lfp', 'lm-kpi-lfp-meta', series.participation);
    bindRate('lm-kpi-eppop', 'lm-kpi-eppop-meta', series.employment_population);

    const ahe = series.avg_hourly_earnings || [];
    const lastAHE = ahe[ahe.length - 1];
    if (lastAHE) {
      setText('lm-kpi-ahe', `${lastAHE.yoy_change != null ? lastAHE.yoy_change.toFixed(2) + '%' : '—'}`);
      setText('lm-kpi-ahe-meta', `$${lastAHE.value.toFixed(2)}/hr · ${formatMonth(lastAHE.date)}`);
    }

    const hours = series.avg_weekly_hours || [];
    const lastH = hours[hours.length - 1];
    if (lastH) {
      setText('lm-kpi-aweek', `${lastH.value.toFixed(1)} hrs`);
      const delta = lastH.mom_change != null ? (lastH.mom_change > 0 ? '+' : '') + lastH.mom_change.toFixed(2) : '—';
      setText('lm-kpi-aweek-meta', `MoM ${delta} · ${formatMonth(lastH.date)}`);
    }

    const nc = (state.bundle.nowcast && state.bundle.nowcast.nowcast) || null;
    if (nc && nc.available) {
      setText('lm-kpi-nowcast', `${formatJobs(nc.payroll_estimate_change)}`);
      setText('lm-kpi-nowcast-meta', `${formatMonth(nc.month)} payroll Δ · UR Δ ${nc.unrate_estimate_change >= 0 ? '+' : ''}${nc.unrate_estimate_change.toFixed(2)}pp`);
    }
  }

  function bindRate(valId, metaId, points) {
    if (!points || !points.length) return;
    const last = points[points.length - 1];
    setText(valId, `${last.value.toFixed(1)}%`);
    if (last.mom_change != null) {
      const sign = last.mom_change > 0 ? '+' : '';
      setText(metaId, `MoM ${sign}${last.mom_change.toFixed(2)}pp · ${formatMonth(last.date)}`);
    } else {
      setText(metaId, formatMonth(last.date));
    }
  }

  // ── Sector ranking ─────────────────────────────────────────

  function drawSectorRanking() {
    const ranking = (((state.bundle.bls || {}).rankings) || {})[state.rankBy] || [];
    // Exclude headline aggregates so the rank shows actual sector moves
    const sectors = ranking.filter(r => !r.is_headline);
    drawSectorRankChart(sectors);
    drawSectorRankTable(ranking);
  }

  function drawSectorRankChart(rows) {
    const ctx = document.getElementById('lm-sector-rank-chart');
    if (!ctx) return;
    if (state.charts.sectorRank) state.charts.sectorRank.destroy();

    const labels = rows.map(r => r.label);
    const isMoM = state.rankBy === 'mom';
    const data = rows.map(r => isMoM ? r.change_thousands : r.change_pct);
    const colors = rows.map(r => (data[rows.indexOf(r)] >= 0 ? COLOR.pos : COLOR.neg));
    const unit = isMoM ? 'k' : '%';

    state.charts.sectorRank = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: isMoM ? 'MoM change (thousands)' : 'YoY change (%)',
          data,
          backgroundColor: colors,
          borderWidth: 0,
        }],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#0a0e1a', borderColor: '#1f2937', borderWidth: 1,
            titleColor: '#e5e7eb', bodyColor: '#cbd5e1',
            callbacks: {
              label: (c) => {
                const v = c.parsed.x;
                if (v == null) return null;
                if (isMoM) return `${v >= 0 ? '+' : ''}${Math.round(v)}k jobs`;
                return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
              },
            },
          },
        },
        scales: {
          x: {
            ticks: { color: COLOR.text, callback: (v) => `${v >= 0 ? '+' : ''}${isMoM ? Math.round(v) + 'k' : v.toFixed(1) + '%'}` },
            grid: { color: COLOR.grid },
          },
          y: { ticks: { color: COLOR.text }, grid: { display: false } },
        },
      },
    });
  }

  function drawSectorRankTable(rows) {
    const tbody = document.querySelector('#lm-sector-rank-table tbody');
    if (!tbody) return;
    tbody.innerHTML = rows.map(r => {
      const lvl = r.level != null ? Math.round(r.level).toLocaleString() : '—';
      const ch = r.change_thousands;
      const pct = r.change_pct;
      const chCls = ch != null && ch < 0 ? 'lm-neg' : 'lm-pos';
      const pctCls = pct != null && pct < 0 ? 'lm-neg' : 'lm-pos';
      const dot = `<span class="lm-color-dot" style="background:${r.color}"></span>`;
      return `<tr ${r.is_headline ? 'class="lm-row-headline"' : ''}>
        <td>${dot}${r.label}</td>
        <td class="num">${lvl}</td>
        <td class="num ${chCls}">${ch != null ? (ch >= 0 ? '+' : '') + Math.round(ch).toLocaleString() : '—'}</td>
        <td class="num ${pctCls}">${pct != null ? (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%' : '—'}</td>
      </tr>`;
    }).join('');
  }

  // ── Sparkline grid ─────────────────────────────────────────

  function renderSparklines() {
    const container = document.getElementById('lm-sparks');
    if (!container) return;
    const bls = state.bundle.bls || {};
    const series = bls.series || {};
    const sectorKeys = (bls.sector_keys || []).filter(k =>
      k !== 'payrolls' && k !== 'payrolls_private'
    );
    const sectorGroups = bls.sector_groups || {};

    // Latest YoY per key (used for sorting + tile-value display)
    const yoyByKey = {};
    sectorKeys.forEach(k => {
      const pts = (series[k] || []).filter(p => p.yoy_change != null);
      const last = pts[pts.length - 1];
      yoyByKey[k] = last ? last.yoy_change : null;
    });

    // Bucket keys by group (fallback to 'services')
    const buckets = {};
    sectorKeys.forEach(k => {
      const g = sectorGroups[k] || 'services';
      (buckets[g] = buckets[g] || []).push(k);
    });
    // Within each bucket, sort by YoY desc
    Object.keys(buckets).forEach(g => buckets[g].sort((a, b) => (yoyByKey[b] ?? -1e9) - (yoyByKey[a] ?? -1e9)));

    let html = '';
    if (state.tileSort === 'yoy') {
      // Flat list sorted by YoY desc
      const ordered = sectorKeys.slice().sort((a, b) => (yoyByKey[b] ?? -1e9) - (yoyByKey[a] ?? -1e9));
      html = `<div class="lm-spark-grid">${ordered.map(k => tileHtml(k, bls)).join('')}</div>`;
    } else {
      // Grouped sections in canonical order
      ['goods', 'services', 'government'].forEach(g => {
        if (!buckets[g] || !buckets[g].length) return;
        const label = SECTOR_GROUP_LABELS[g] || g;
        html += `
          <div class="lm-spark-group">
            <div class="lm-spark-group-head">
              <span class="lm-spark-group-name">${label}</span>
              <span class="lm-spark-group-count">${buckets[g].length} ${buckets[g].length === 1 ? 'sector' : 'sectors'}</span>
            </div>
            <div class="lm-spark-grid">${buckets[g].map(k => tileHtml(k, bls)).join('')}</div>
          </div>`;
      });
    }
    container.innerHTML = html;

    sectorKeys.forEach(k => drawSparkline(k, series[k]));
    container.querySelectorAll('.lm-spark-card').forEach(card => {
      card.addEventListener('click', () => {
        // Tile click on labor market: no detail chart, so just visually flash
        // (clicking a sector in the ranking table is the canonical entry point)
        card.classList.add('lm-spark-card--flash');
        setTimeout(() => card.classList.remove('lm-spark-card--flash'), 600);
      });
    });
  }

  function tileHtml(key, bls) {
    const cats = bls.categories || {};
    const colors = bls.colors || {};
    const series = (bls.series || {})[key] || [];
    const pts = series.filter(p => p.yoy_change != null);
    const last = pts[pts.length - 1];
    const yoy = last ? last.yoy_change : null;
    const moLabel = last ? formatMonth(last.date) : '—';
    const cls = yoy != null && yoy >= 0 ? 'lm-pos' : 'lm-neg';
    const valTxt = yoy != null ? `${yoy >= 0 ? '+' : ''}${yoy.toFixed(2)}%` : '—';
    return `
      <div class="lm-spark-card" data-spark="${key}">
        <div class="lm-spark-head">
          <span class="lm-color-dot" style="background:${colors[key] || '#64748b'}"></span>
          <span class="lm-spark-label" title="${cats[key] || key}">${cats[key] || key}</span>
        </div>
        <div class="lm-spark-value-row">
          <span class="lm-spark-value ${cls}">${valTxt}</span>
          <span class="lm-spark-when">${moLabel}</span>
        </div>
        <canvas class="lm-spark-canvas" data-key="${key}"></canvas>
      </div>`;
  }

  function drawSparkline(key, points) {
    const el = document.querySelector(`canvas[data-key="${key}"]`);
    if (!el) return;
    const window = 60;            // 5 years of YoY
    const slice = (points || []).filter(p => p.yoy_change != null).slice(-window);
    if (!slice.length) return;
    const data = slice.map(p => p.yoy_change);
    const labels = slice.map(p => p.date);

    if (state.charts[`spark_${key}`]) state.charts[`spark_${key}`].destroy();
    state.charts[`spark_${key}`] = new Chart(el, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          data,
          borderColor: ((state.bundle.bls||{}).colors||{})[key] || '#64748b',
          backgroundColor: 'transparent',
          borderWidth: 1.6,
          pointRadius: 0,
          tension: 0.3,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#0a0e1a', borderColor: '#1f2937', borderWidth: 1,
            titleColor: '#e5e7eb', bodyColor: '#cbd5e1',
            callbacks: {
              label: (c) => `${c.parsed.y >= 0 ? '+' : ''}${c.parsed.y.toFixed(2)}% YoY`,
              title: (items) => items[0] ? formatMonth(items[0].label) : '',
            },
          },
        },
        scales: { x: { display: false }, y: { display: false } },
        elements: { line: { borderJoinStyle: 'round' } },
      },
    });
  }

  // ── Payrolls Δ chart (BLS bars vs estimate line) ───────────

  function drawPayrollChart() {
    const series = ((state.bundle.bls || {}).series) || {};
    const payrolls = series.payrolls || [];
    const track = (state.bundle.nowcast && state.bundle.nowcast.payroll_track_full) || [];

    const actualByMonth = {};
    payrolls.forEach(p => { if (p.mom_change != null) actualByMonth[p.date] = p.mom_change; });
    const estByMonth = {};
    track.forEach(r => { estByMonth[r.month] = r.estimate_change; });

    let months = Array.from(new Set([
      ...Object.keys(actualByMonth),
      ...Object.keys(estByMonth),
    ])).sort();

    const nc = state.bundle.nowcast && state.bundle.nowcast.nowcast;
    if (nc && nc.available && nc.month && !months.includes(nc.month)) {
      months.push(nc.month); months.sort();
    }

    const slice = state.windowMonths === 0 ? months : months.slice(-state.windowMonths);
    const labels = slice.map(formatMonth);
    const actualBars = slice.map(m => actualByMonth[m] != null ? actualByMonth[m] : null);
    const estLine = slice.map(m => estByMonth[m] != null ? estByMonth[m] : null);
    const forecastBars = slice.map(m =>
      (nc && nc.available && nc.month === m && actualByMonth[m] == null)
        ? nc.payroll_estimate_change : null
    );
    if (nc && nc.available) {
      const idx = slice.indexOf(nc.month);
      if (idx >= 0 && estLine[idx] == null) estLine[idx] = nc.payroll_estimate_change;
    }

    const ctx = document.getElementById('lm-payroll-chart');
    if (!ctx) return;
    if (state.charts.payroll) state.charts.payroll.destroy();
    state.charts.payroll = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { type: 'bar', label: 'BLS actual (Δ jobs, 000s)',
            data: actualBars, backgroundColor: COLOR.blsBar, borderColor: COLOR.bls,
            borderWidth: 1, order: 2 },
          { type: 'bar', label: 'Forward nowcast',
            data: forecastBars, backgroundColor: COLOR.estBar, borderColor: COLOR.estLine,
            borderWidth: 1, borderDash: [4, 3], order: 2 },
          { type: 'line', label: 'Bridge model estimate',
            data: estLine, borderColor: COLOR.estLine, backgroundColor: 'rgba(245,158,11,0.18)',
            tension: 0.25, pointRadius: 0, pointHoverRadius: 3, borderWidth: 2,
            spanGaps: true, order: 1 },
        ],
      },
      options: chartOpts({
        yTitle: 'Δ jobs (thousands)',
        yFmt: (v) => `${v >= 0 ? '+' : ''}${Math.round(v)}k`,
      }),
    });
  }

  // ── U-3 vs U-6 chart with model estimate ────────────────────

  function drawUnrateChart() {
    const series = ((state.bundle.bls || {}).series) || {};
    const u3 = series.unemployment || [];
    const u6 = series.u6 || [];
    const urTrack = (state.bundle.nowcast && state.bundle.nowcast.unrate_track_full) || [];

    const u3ByMonth = {}; u3.forEach(p => { u3ByMonth[p.date] = p.value; });
    const u6ByMonth = {}; u6.forEach(p => { u6ByMonth[p.date] = p.value; });
    const estTrack = {}; urTrack.forEach(r => { estTrack[r.month] = r.estimate_change; });

    let months = Array.from(new Set([
      ...Object.keys(u3ByMonth), ...Object.keys(u6ByMonth), ...Object.keys(estTrack),
    ])).sort();

    const nc = state.bundle.nowcast && state.bundle.nowcast.nowcast;
    if (nc && nc.available && nc.month && !months.includes(nc.month)) {
      months.push(nc.month); months.sort();
    }
    const slice = state.windowMonths === 0 ? months : months.slice(-state.windowMonths);
    const labels = slice.map(formatMonth);

    const u3Series = slice.map(m => u3ByMonth[m] != null ? u3ByMonth[m] : null);
    const u6Series = slice.map(m => u6ByMonth[m] != null ? u6ByMonth[m] : null);

    const estLevel = slice.map(m => {
      const prev = shiftMonth(m, -1);
      if (u3ByMonth[prev] != null && estTrack[m] != null)
        return +(u3ByMonth[prev] + estTrack[m]).toFixed(2);
      if (nc && nc.available && nc.month === m && nc.implied_unrate_level != null)
        return nc.implied_unrate_level;
      return null;
    });

    const ctx = document.getElementById('lm-unrate-chart');
    if (!ctx) return;
    if (state.charts.unrate) state.charts.unrate.destroy();
    state.charts.unrate = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'U-3 (BLS)', data: u3Series, borderColor: COLOR.u3,
            backgroundColor: 'rgba(239,68,68,0.10)', tension: 0.25,
            pointRadius: 0, borderWidth: 2, fill: false },
          { label: 'U-6 (BLS)', data: u6Series, borderColor: COLOR.u6,
            backgroundColor: 'rgba(249,115,22,0.08)', tension: 0.25,
            pointRadius: 0, borderWidth: 2, fill: false },
          { label: 'U-3 model estimate', data: estLevel, borderColor: COLOR.estLine,
            borderDash: [5, 4], tension: 0.25, pointRadius: 0, borderWidth: 2,
            spanGaps: true, fill: false },
        ],
      },
      options: chartOpts({ yTitle: 'Rate (%)', yFmt: (v) => `${v.toFixed(1)}%`, showLegend: true }),
    });
  }

  // ── Wages + hours dual-axis ────────────────────────────────

  function drawWagesChart() {
    const series = ((state.bundle.bls || {}).series) || {};
    const ahe = series.avg_hourly_earnings || [];
    const hrs = series.avg_weekly_hours || [];

    const aheByMonth = {}; ahe.forEach(p => { if (p.yoy_change != null) aheByMonth[p.date] = p.yoy_change; });
    const hrsByMonth = {}; hrs.forEach(p => { hrsByMonth[p.date] = p.value; });

    let months = Array.from(new Set([...Object.keys(aheByMonth), ...Object.keys(hrsByMonth)])).sort();
    const slice = state.windowMonths === 0 ? months : months.slice(-state.windowMonths);
    const labels = slice.map(formatMonth);

    const ctx = document.getElementById('lm-wages-chart');
    if (!ctx) return;
    if (state.charts.wages) state.charts.wages.destroy();
    state.charts.wages = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Avg hourly earnings YoY (%)',
            data: slice.map(m => aheByMonth[m] != null ? aheByMonth[m] : null),
            borderColor: COLOR.wage, backgroundColor: 'rgba(16,185,129,0.10)',
            tension: 0.25, pointRadius: 0, borderWidth: 2, yAxisID: 'y' },
          { label: 'Avg weekly hours',
            data: slice.map(m => hrsByMonth[m] != null ? hrsByMonth[m] : null),
            borderColor: COLOR.hours, borderDash: [4, 3],
            backgroundColor: 'transparent',
            tension: 0.25, pointRadius: 0, borderWidth: 2, yAxisID: 'y1' },
        ],
      },
      options: dualAxisOpts({
        yLeft: { title: 'AHE YoY (%)', fmt: (v) => `${v.toFixed(1)}%` },
        yRight: { title: 'Hours', fmt: (v) => `${v.toFixed(1)}` },
      }),
    });
  }

  // ── Participation + EPOP ───────────────────────────────────

  function drawParticipationChart() {
    const series = ((state.bundle.bls || {}).series) || {};
    const lfp = series.participation || [];
    const epop = series.employment_population || [];

    const lfpBy = {}; lfp.forEach(p => { lfpBy[p.date] = p.value; });
    const epopBy = {}; epop.forEach(p => { epopBy[p.date] = p.value; });
    let months = Array.from(new Set([...Object.keys(lfpBy), ...Object.keys(epopBy)])).sort();
    const slice = state.windowMonths === 0 ? months : months.slice(-state.windowMonths);
    const labels = slice.map(formatMonth);

    const ctx = document.getElementById('lm-participation-chart');
    if (!ctx) return;
    if (state.charts.participation) state.charts.participation.destroy();
    state.charts.participation = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Labor-force participation', data: slice.map(m => lfpBy[m] != null ? lfpBy[m] : null),
            borderColor: COLOR.lfp, tension: 0.25, pointRadius: 0, borderWidth: 2,
            backgroundColor: 'rgba(168,85,247,0.10)' },
          { label: 'Employment-to-population', data: slice.map(m => epopBy[m] != null ? epopBy[m] : null),
            borderColor: COLOR.eppop, tension: 0.25, pointRadius: 0, borderWidth: 2,
            backgroundColor: 'rgba(139,92,246,0.10)' },
        ],
      },
      options: chartOpts({ yTitle: 'Rate (%)', yFmt: (v) => `${v.toFixed(1)}%`, showLegend: true }),
    });
  }

  // ── Track record + indicator coefficients ──────────────────

  function renderTrackRecord() {
    const nc = state.bundle.nowcast || {};
    const summary = nc.summary || {};
    const sample = nc.sample_size || {};
    setText('lm-stat-pay-mae', summary.payroll_mae_thousands != null ? `${summary.payroll_mae_thousands}k` : '—');
    setText('lm-stat-pay-hit', summary.payroll_direction_hit_rate_pct != null ? `${summary.payroll_direction_hit_rate_pct}%` : '—');
    setText('lm-stat-ur-mae', summary.unrate_mae_pp != null ? `${summary.unrate_mae_pp} pp` : '—');
    setText('lm-stat-sample', sample.payrolls != null ? `${sample.payrolls} mo` : '—');

    const payTbody = document.querySelector('#lm-payroll-track-table tbody');
    const urTbody = document.querySelector('#lm-unrate-track-table tbody');
    const pTrack = nc.payroll_track || [];
    const uTrack = nc.unrate_track || [];

    if (payTbody) {
      payTbody.innerHTML = pTrack.slice().reverse().map(r => `
        <tr>
          <td>${formatMonth(r.month)}</td>
          <td class="num ${r.actual_change >= 0 ? 'lm-pos' : 'lm-neg'}">${formatChange(r.actual_change, 'k')}</td>
          <td class="num ${r.estimate_change >= 0 ? 'lm-pos' : 'lm-neg'}">${formatChange(r.estimate_change, 'k')}</td>
          <td class="num">${formatChange(r.error, 'k')}</td>
        </tr>`).join('');
    }
    if (urTbody) {
      urTbody.innerHTML = uTrack.slice().reverse().map(r => `
        <tr>
          <td>${formatMonth(r.month)}</td>
          <td class="num ${r.actual_change >= 0 ? 'lm-neg' : 'lm-pos'}">${formatChange(r.actual_change, 'pp')}</td>
          <td class="num ${r.estimate_change >= 0 ? 'lm-neg' : 'lm-pos'}">${formatChange(r.estimate_change, 'pp')}</td>
          <td class="num">${formatChange(r.error, 'pp')}</td>
        </tr>`).join('');
    }
  }

  function renderIndicators() {
    const inds = (state.bundle.nowcast && state.bundle.nowcast.indicators) || [];
    const tbody = document.querySelector('#lm-indicators-table tbody');
    if (!tbody) return;
    tbody.innerHTML = inds.map(r => `
      <tr>
        <td><code>${r.id}</code></td>
        <td>${r.label}</td>
        <td class="num">${r.lag}</td>
        <td class="num">${formatNum(r.coefficient_payrolls, 4)}</td>
        <td class="num">${formatNum(r.coefficient_unrate, 6)}</td>
      </tr>`).join('');
  }

  // ── Chart option factories ─────────────────────────────────

  function chartOpts({ yTitle, yFmt, showLegend = false }) {
    return {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: showLegend, position: 'top', labels: { color: COLOR.text, boxWidth: 12 } },
        tooltip: {
          backgroundColor: '#0a0e1a', borderColor: '#1f2937', borderWidth: 1,
          titleColor: '#e5e7eb', bodyColor: '#cbd5e1',
          callbacks: {
            label: (c) => c.parsed.y == null ? null : `${c.dataset.label}: ${yFmt(c.parsed.y)}`,
          },
        },
      },
      scales: {
        x: { ticks: { color: COLOR.text, maxRotation: 0, autoSkipPadding: 14 }, grid: { color: COLOR.grid } },
        y: { title: { display: true, text: yTitle, color: COLOR.text },
             ticks: { color: COLOR.text, callback: yFmt }, grid: { color: COLOR.grid } },
      },
    };
  }

  function dualAxisOpts({ yLeft, yRight }) {
    return {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, position: 'top', labels: { color: COLOR.text, boxWidth: 12 } },
        tooltip: {
          backgroundColor: '#0a0e1a', borderColor: '#1f2937', borderWidth: 1,
          titleColor: '#e5e7eb', bodyColor: '#cbd5e1',
        },
      },
      scales: {
        x: { ticks: { color: COLOR.text, maxRotation: 0, autoSkipPadding: 14 }, grid: { color: COLOR.grid } },
        y:  { position: 'left', title: { display: true, text: yLeft.title, color: COLOR.text },
              ticks: { color: COLOR.text, callback: yLeft.fmt }, grid: { color: COLOR.grid } },
        y1: { position: 'right', title: { display: true, text: yRight.title, color: COLOR.text },
              ticks: { color: COLOR.text, callback: yRight.fmt }, grid: { drawOnChartArea: false } },
      },
    };
  }

  // ── Helpers ────────────────────────────────────────────────

  function setText(id, val) { const el = document.getElementById(id); if (el) el.textContent = val; }
  function formatMonth(s) {
    if (!s) return '';
    const [y, m] = s.split('-');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return `${months[parseInt(m,10)-1] || m} ${String(y).slice(-2)}`;
  }
  function formatReleaseDate(s) {
    if (!s) return '—';
    try {
      const d = new Date(s + 'T12:00:00Z');
      return d.toLocaleDateString('en-US', { weekday: 'short', year: 'numeric', month: 'long', day: 'numeric' });
    } catch { return s; }
  }
  function formatJobs(v, withSign = true) {
    if (v == null) return '—';
    const sign = withSign && v > 0 ? '+' : '';
    return `${sign}${Math.round(v).toLocaleString()}k`;
  }
  function formatChange(v, suffix) {
    if (v == null) return '—';
    const sign = v > 0 ? '+' : '';
    if (suffix === 'k') return `${sign}${Math.round(v)}k`;
    if (suffix === 'pp') return `${sign}${v.toFixed(2)}`;
    return `${sign}${v}`;
  }
  function formatNum(v, dp) { return v == null ? '—' : Number(v).toFixed(dp || 2); }
  function shiftMonth(monthKey, by) {
    const [y, m] = monthKey.split('-').map(Number);
    const total = y * 12 + (m - 1) + by;
    const ny = Math.floor(total / 12);
    const nm = (total % 12) + 1;
    return `${String(ny).padStart(4, '0')}-${String(nm).padStart(2, '0')}`;
  }
})();
