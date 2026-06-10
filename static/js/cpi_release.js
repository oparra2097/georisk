/* CPI Release Monitor — economist-grade CPI dashboard.
 *
 * Single fetch of /api/cpi/us/detail; renders the release banner,
 * headline KPIs, headline+core YoY history, the component ranking
 * (MoM / YoY toggle), a click-to-drill detail chart, and a 5-year
 * YoY sparkline grid per component.  Refresh hits POST /api/cpi/us/refresh.
 */
/* eslint-env browser */
/* global Chart */

(function () {
  'use strict';

  const API = {
    bundle: '/api/cpi/us/detail',
    refresh: '/api/cpi/us/refresh',
  };

  const COLOR = {
    headline: '#3b82f6',
    core:     '#10b981',
    food:     '#f59e0b',
    energy:   '#ef4444',
    shelter:  '#8b5cf6',
    pos:      '#ef4444',          // CPI pos = bad, so red
    neg:      '#10b981',
    grid:     'rgba(255,255,255,0.06)',
    text:     '#9ca3af',
    target:   'rgba(96, 165, 250, 0.45)',
  };

  const GROUP_COLOR = {
    shelter:  '#8b5cf6',
    food:     '#f59e0b',
    energy:   '#ef4444',
    vehicles: '#06b6d4',
    medical:  '#f43f5e',
    services: '#10b981',
    goods:    '#ec4899',
    aggregate:'#64748b',
    other:    '#94a3b8',
  };

  const state = {
    bundle: null,
    windowMonths: 60,
    rankBy: 'mom',
    detailKey: null,
    detailBasis: 'yoy',
    tileSort: 'group',
    charts: {},
    countdownTimer: null,
  };

  const GROUP_ORDER = ['shelter', 'food', 'energy', 'vehicles', 'medical', 'services', 'goods', 'aggregate', 'other'];
  const GROUP_LABEL = {
    shelter:   'Shelter',
    food:      'Food',
    energy:    'Energy',
    vehicles:  'Vehicles & insurance',
    medical:   'Medical care',
    services:  'Services',
    goods:     'Goods',
    aggregate: 'Core aggregates',
    other:     'Other',
  };

  document.addEventListener('DOMContentLoaded', () => {
    bindToolbar();
    bindRankToggle();
    bindDetailToggle();
    bindTileSort();
    bindRefresh();
    bindStaleRefresh();
    fetchBundle();
  });

  // ── Wiring ───────────────────────────────────────────────

  function bindToolbar() {
    document.querySelectorAll('.lm-toggle[data-window]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.lm-toggle[data-window]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.windowMonths = parseInt(btn.dataset.window, 10) || 0;
        drawHistoryChart();
        drawDetailChart();
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
        drawRanking();
      });
    });
  }

  function bindDetailToggle() {
    document.querySelectorAll('.lm-toggle[data-detail]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.lm-toggle[data-detail]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.detailBasis = btn.dataset.detail;
        drawDetailChart();
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
    const btn = document.getElementById('cpi-refresh-btn');
    if (!btn) return;
    btn.addEventListener('click', () => doRefresh(btn));
  }
  function bindStaleRefresh() {
    const btn = document.getElementById('cpi-stale-refresh');
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
    finally { btn.disabled = false; btn.textContent = prev; }
  }

  async function fetchBundle() {
    try {
      const resp = await fetch(API.bundle, { credentials: 'same-origin' });
      const data = await resp.json();
      state.bundle = data || {};

      // Default detail = top MoM mover (excluding aggregates)
      const rank = (((state.bundle.detail || {}).rankings) || {}).mom || [];
      const top = rank.find(r => !r.is_aggregate);
      if (top) state.detailKey = top.key;

      renderStaleBanner();
      renderReleaseBanner();
      renderKPIs();
      drawHistoryChart();
      drawRanking();
      drawDetailChart();
      renderSparklines();
      renderUpcomingReleases();
    } catch (e) {
      console.error('CPI release fetch failed', e);
    }
  }

  function renderStaleBanner() {
    const banner = document.getElementById('cpi-stale-banner');
    if (!banner) return;
    // Both detail + overview can be stale; banner if either is.
    const dMeta = ((state.bundle.detail || {}).meta) || {};
    const oMeta = ((state.bundle.overview || {}).meta) || {};
    const stale = !!(dMeta.is_stale || oMeta.is_stale);
    if (stale) {
      const latest = dMeta.latest_month || oMeta.latest_month || 'unknown';
      const months = Math.max(dMeta.months_behind || 0, oMeta.months_behind || 0);
      const detail = document.getElementById('cpi-stale-detail');
      if (detail) detail.textContent =
        `Latest BLS CPI month: ${latest}` +
        (months > 0 ? ` (${months} months behind today). Background refresh in progress.` : '. Background refresh in progress.');
      banner.hidden = false;
    } else {
      banner.hidden = true;
    }
  }

  // ── Release banner ───────────────────────────────────────

  function renderReleaseBanner() {
    const r = state.bundle.next_release;
    if (!r || r.unavailable) return;
    setText('cpi-release-date', formatReleaseDate(r.release_date));
    setText('cpi-release-time', r.time_eastern || '08:30 ET');
    if (r.data_label) setText('cpi-release-data-label', r.data_label);

    if (state.countdownTimer) clearInterval(state.countdownTimer);
    const target = new Date(r.release_at_utc).getTime();
    function tick() {
      const delta = Math.max(0, Math.floor((target - Date.now()) / 1000));
      const days = Math.floor(delta / 86400);
      const hours = Math.floor((delta % 86400) / 3600);
      const mins = Math.floor((delta % 3600) / 60);
      setText('cpi-cd-days', String(days));
      setText('cpi-cd-hours', String(hours));
      setText('cpi-cd-mins', String(mins));
      const banner = document.getElementById('cpi-release-banner');
      if (banner) {
        banner.classList.toggle('lm-release-banner--imminent', delta <= 86400);
        banner.classList.toggle('lm-release-banner--today', delta <= 12 * 3600);
      }
    }
    tick();
    state.countdownTimer = setInterval(tick, 30000);
  }

  function renderUpcomingReleases() {
    const list = document.getElementById('cpi-release-list');
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

  // ── Headline KPIs ────────────────────────────────────────

  function renderKPIs() {
    const ov = ((state.bundle.overview || {}).series) || {};

    const apply = (key, valEl, metaEl) => {
      const series = ov[key];
      if (!series || !series.length) return;
      const last = series[series.length - 1];
      const yoy = last.yoy_change;
      setText(valEl, yoy != null ? `${yoy >= 0 ? '+' : ''}${yoy.toFixed(2)}%` : '—');
      setText(metaEl, `${formatMonth(last.date)} · index ${last.value.toFixed(2)}`);
    };
    apply('all_items', 'cpi-kpi-headline', 'cpi-kpi-headline-meta');
    apply('core',      'cpi-kpi-core',     'cpi-kpi-core-meta');
    apply('food',      'cpi-kpi-food',     'cpi-kpi-food-meta');
    apply('energy',    'cpi-kpi-energy',   'cpi-kpi-energy-meta');
    apply('housing',   'cpi-kpi-shelter',  'cpi-kpi-shelter-meta');

    // MoM headline + core from detail rankings (already computed)
    const dRank = (((state.bundle.detail || {}).rankings) || {}).mom || [];
    // Headline/core aren't in the detail set; pull from overview directly
    const applyMoM = (key, valEl, metaEl) => {
      const series = ov[key];
      if (!series || series.length < 2) return;
      const last = series[series.length - 1];
      const prev = series[series.length - 2];
      const mom = prev.value !== 0 ? ((last.value - prev.value) / prev.value) * 100 : null;
      if (mom == null) return;
      setText(valEl, `${mom >= 0 ? '+' : ''}${mom.toFixed(2)}%`);
      setText(metaEl, `${formatMonth(last.date)} · prev ${prev.value.toFixed(2)}`);
    };
    applyMoM('all_items', 'cpi-kpi-headline-mom', 'cpi-kpi-headline-mom-meta');
    applyMoM('core',      'cpi-kpi-core-mom',     'cpi-kpi-core-mom-meta');
  }

  // ── Headline + core history chart ───────────────────────

  function drawHistoryChart() {
    const ov = ((state.bundle.overview || {}).series) || {};
    const headline = (ov.all_items || []).filter(p => p.yoy_change != null);
    const core = (ov.core || []).filter(p => p.yoy_change != null);
    const food = (ov.food || []).filter(p => p.yoy_change != null);
    const energy = (ov.energy || []).filter(p => p.yoy_change != null);

    const all = headline.length ? headline : core;
    const slice = state.windowMonths === 0 ? all : all.slice(-state.windowMonths);
    const labels = slice.map(p => p.date);
    const byDate = (series) => {
      const m = {}; series.forEach(p => { m[p.date] = p.yoy_change; }); return m;
    };
    const hMap = byDate(headline);
    const cMap = byDate(core);
    const fMap = byDate(food);
    const eMap = byDate(energy);

    const ctx = document.getElementById('cpi-history-chart');
    if (!ctx) return;
    if (state.charts.history) state.charts.history.destroy();
    state.charts.history = new Chart(ctx, {
      type: 'line',
      data: {
        labels: labels.map(formatMonth),
        datasets: [
          { label: 'Headline CPI', data: labels.map(l => hMap[l] != null ? hMap[l] : null),
            borderColor: COLOR.headline, backgroundColor: 'rgba(59,130,246,0.10)',
            tension: 0.25, pointRadius: 0, borderWidth: 2.4 },
          { label: 'Core CPI', data: labels.map(l => cMap[l] != null ? cMap[l] : null),
            borderColor: COLOR.core, backgroundColor: 'rgba(16,185,129,0.10)',
            tension: 0.25, pointRadius: 0, borderWidth: 2.4 },
          { label: 'Food', data: labels.map(l => fMap[l] != null ? fMap[l] : null),
            borderColor: COLOR.food, backgroundColor: 'transparent',
            tension: 0.25, pointRadius: 0, borderWidth: 1.4, borderDash: [4, 3] },
          { label: 'Energy', data: labels.map(l => eMap[l] != null ? eMap[l] : null),
            borderColor: COLOR.energy, backgroundColor: 'transparent',
            tension: 0.25, pointRadius: 0, borderWidth: 1.4, borderDash: [4, 3] },
          // Fed 2% target reference line
          { label: 'Fed 2 % target', data: labels.map(() => 2),
            borderColor: COLOR.target, backgroundColor: 'transparent',
            borderDash: [2, 4], borderWidth: 1.2, pointRadius: 0,
            tension: 0, hidden: false },
        ],
      },
      options: chartOpts({
        yTitle: 'YoY change (%)',
        yFmt: (v) => `${v.toFixed(1)}%`,
        showLegend: true,
      }),
    });
  }

  // ── Component ranking ────────────────────────────────────

  function drawRanking() {
    const det = state.bundle.detail || {};
    const ranking = (det.rankings || {})[state.rankBy] || [];

    drawRankChart(ranking);
    drawRankTable(ranking);
  }

  function drawRankChart(rows) {
    const ctx = document.getElementById('cpi-rank-chart');
    if (!ctx) return;
    if (state.charts.rank) state.charts.rank.destroy();

    const isMoM = state.rankBy === 'mom';
    const labels = rows.map(r => r.label);
    const data = rows.map(r => r.change_pct);
    const colors = rows.map(r => (r.change_pct != null && r.change_pct >= 0)
      ? GROUP_COLOR[r.group] || COLOR.pos
      : 'rgba(16,185,129,0.65)');

    state.charts.rank = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: isMoM ? 'MoM %' : 'YoY %',
          data,
          backgroundColor: colors,
          borderWidth: 0,
        }],
      },
      options: {
        indexAxis: 'y',
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#0a0e1a', borderColor: '#1f2937', borderWidth: 1,
            titleColor: '#e5e7eb', bodyColor: '#cbd5e1',
            callbacks: {
              label: (c) => c.parsed.x == null ? null
                : `${c.parsed.x >= 0 ? '+' : ''}${c.parsed.x.toFixed(2)}%`,
            },
          },
        },
        scales: {
          x: { ticks: { color: COLOR.text, callback: (v) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%` },
               grid: { color: COLOR.grid } },
          y: { ticks: { color: COLOR.text }, grid: { display: false } },
        },
        onClick: (evt, items) => {
          if (!items.length) return;
          const idx = items[0].index;
          state.detailKey = rows[idx].key;
          drawDetailChart();
          // Highlight in the table
          document.querySelectorAll('.cpi-rank-table tbody tr').forEach(tr => {
            tr.classList.toggle('cpi-row-active', tr.dataset.key === state.detailKey);
          });
        },
      },
    });
  }

  function drawRankTable(rows) {
    const tbody = document.querySelector('#cpi-rank-table tbody');
    if (!tbody) return;
    // Build a lookup of MoM and YoY per key so the table shows both columns
    // regardless of which basis is sorted on.
    const det = state.bundle.detail || {};
    const momByKey = {}; (det.rankings && det.rankings.mom || []).forEach(r => momByKey[r.key] = r);
    const yoyByKey = {}; (det.rankings && det.rankings.yoy || []).forEach(r => yoyByKey[r.key] = r);

    tbody.innerHTML = rows.map(r => {
      const mom = momByKey[r.key] || {};
      const yoy = yoyByKey[r.key] || {};
      const mClass = mom.change_pct != null && mom.change_pct < 0 ? 'lm-neg' : 'lm-pos';
      const yClass = yoy.change_pct != null && yoy.change_pct < 0 ? 'lm-neg' : 'lm-pos';
      const isActive = state.detailKey === r.key;
      return `<tr data-key="${r.key}"
        class="${r.is_aggregate ? 'cpi-row-aggregate' : ''} ${isActive ? 'cpi-row-active' : ''}">
        <td><span class="lm-color-dot" style="background:${r.color}"></span>${r.label}</td>
        <td><span class="cpi-group-chip" style="color:${GROUP_COLOR[r.group] || COLOR.text}">${r.group}</span></td>
        <td class="num ${mClass}">${formatPct(mom.change_pct)}</td>
        <td class="num ${yClass}">${formatPct(yoy.change_pct)}</td>
      </tr>`;
    }).join('');

    tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        state.detailKey = tr.dataset.key;
        drawDetailChart();
        tbody.querySelectorAll('tr').forEach(t => t.classList.toggle('cpi-row-active', t.dataset.key === state.detailKey));
      });
    });
  }

  // ── Component detail chart ───────────────────────────────

  function drawDetailChart() {
    const det = state.bundle.detail || {};
    const series = (det.series || {})[state.detailKey] || [];
    const meta = ((det.categories || {})[state.detailKey]) || state.detailKey || '—';
    setText('cpi-detail-title', meta);
    const seriesId = (((det.meta || {}).series_ids) || {})[state.detailKey];
    setText('cpi-detail-note',
      seriesId ? `BLS series ${seriesId}. Reading: ${labelBasis()}.`
               : `Reading: ${labelBasis()}.`);

    const filtered = series.filter(p => p[basisField()] != null);
    const slice = state.windowMonths === 0 ? filtered : filtered.slice(-state.windowMonths);
    const labels = slice.map(p => p.date);
    const data = slice.map(p => p[basisField()]);
    const color = ((det.colors || {})[state.detailKey]) || '#8b5cf6';

    const ctx = document.getElementById('cpi-detail-chart');
    if (!ctx) return;
    if (state.charts.detail) state.charts.detail.destroy();
    state.charts.detail = new Chart(ctx, {
      type: 'line',
      data: {
        labels: labels.map(formatMonth),
        datasets: [{
          label: `${meta} (${labelBasis()})`,
          data,
          borderColor: color,
          backgroundColor: hexA(color, 0.10),
          tension: 0.25,
          pointRadius: 0,
          borderWidth: 2.2,
          fill: state.detailBasis === 'level',
        }],
      },
      options: chartOpts({
        yTitle: yAxisTitle(),
        yFmt: yAxisFmt(),
        showLegend: false,
      }),
    });
  }

  function basisField() {
    if (state.detailBasis === 'mom') return 'mom_pct';
    if (state.detailBasis === 'level') return 'value';
    return 'yoy_change';
  }
  function labelBasis() {
    if (state.detailBasis === 'mom') return 'MoM %';
    if (state.detailBasis === 'level') return 'Index level';
    return 'YoY %';
  }
  function yAxisTitle() {
    if (state.detailBasis === 'level') return 'Index level (1982-84=100)';
    return state.detailBasis.toUpperCase() + ' %';
  }
  function yAxisFmt() {
    if (state.detailBasis === 'level') return (v) => v.toFixed(1);
    return (v) => `${v.toFixed(1)}%`;
  }

  // ── Sparkline grid (5y YoY per component) ───────────────

  function renderSparklines() {
    const container = document.getElementById('cpi-sparks');
    if (!container) return;
    const det = state.bundle.detail || {};
    const cats = det.categories || {};
    const colors = det.colors || {};
    const groups = det.groups || {};
    const series = det.series || {};

    // Skip aggregate buckets in the grid (they're already a different lens)
    const aggregates = det.aggregates || [];
    const allKeys = Object.keys(cats).filter(k => !aggregates.includes(k));

    // Latest YoY per key for sorting + tile-value display
    const yoyByKey = {};
    allKeys.forEach(k => {
      const pts = (series[k] || []).filter(p => p.yoy_change != null);
      const last = pts[pts.length - 1];
      yoyByKey[k] = last ? last.yoy_change : null;
    });

    // Bucket by group
    const buckets = {};
    allKeys.forEach(k => {
      const g = groups[k] || 'other';
      (buckets[g] = buckets[g] || []).push(k);
    });
    Object.keys(buckets).forEach(g => buckets[g].sort((a, b) => (yoyByKey[b] ?? -1e9) - (yoyByKey[a] ?? -1e9)));

    let html = '';
    if (state.tileSort === 'yoy') {
      const ordered = allKeys.slice().sort((a, b) => (yoyByKey[b] ?? -1e9) - (yoyByKey[a] ?? -1e9));
      html = `<div class="lm-spark-grid">${ordered.map(k => tileHtml(k, det)).join('')}</div>`;
    } else {
      GROUP_ORDER.forEach(g => {
        if (!buckets[g] || !buckets[g].length) return;
        const label = GROUP_LABEL[g] || g;
        html += `
          <div class="lm-spark-group">
            <div class="lm-spark-group-head">
              <span class="lm-spark-group-name">${label}</span>
              <span class="lm-spark-group-count">${buckets[g].length} ${buckets[g].length === 1 ? 'item' : 'items'}</span>
            </div>
            <div class="lm-spark-grid">${buckets[g].map(k => tileHtml(k, det)).join('')}</div>
          </div>`;
      });
    }
    container.innerHTML = html;

    allKeys.forEach(k => drawSparkline(k, series[k], colors[k]));
    container.querySelectorAll('.lm-spark-card').forEach(card => {
      const k = card.dataset.spark;
      card.addEventListener('click', () => {
        state.detailKey = k;
        state.detailBasis = 'yoy';
        document.querySelectorAll('.lm-toggle[data-detail]').forEach(b =>
          b.classList.toggle('active', b.dataset.detail === 'yoy'));
        drawDetailChart();
        document.querySelectorAll('.cpi-rank-table tbody tr').forEach(t =>
          t.classList.toggle('cpi-row-active', t.dataset.key === k));
        const target = document.getElementById('cpi-detail-chart');
        if (target) target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      });
    });
  }

  function tileHtml(key, det) {
    const cats = det.categories || {};
    const colors = det.colors || {};
    const series = (det.series || {})[key] || [];
    const pts = series.filter(p => p.yoy_change != null);
    const last = pts[pts.length - 1];
    const yoy = last ? last.yoy_change : null;
    const moLabel = last ? formatMonth(last.date) : '—';
    // For CPI, positive = price increase = "bad" → red
    const cls = yoy != null && yoy >= 0 ? 'lm-neg' : 'lm-pos';
    const valTxt = yoy != null ? `${yoy >= 0 ? '+' : ''}${yoy.toFixed(2)}%` : '—';
    const active = state.detailKey === key ? 'lm-spark-card--active' : '';
    return `
      <div class="lm-spark-card ${active}" data-spark="${key}">
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

  function drawSparkline(key, points, color) {
    const el = document.querySelector(`canvas[data-key="${key}"]`);
    if (!el) return;
    const window = 60;
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
          borderColor: color || '#64748b',
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
      },
    });
  }

  // ── Chart options ────────────────────────────────────────

  function chartOpts({ yTitle, yFmt, showLegend = false }) {
    return {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: showLegend, position: 'top',
                  labels: { color: COLOR.text, boxWidth: 12 } },
        tooltip: {
          backgroundColor: '#0a0e1a', borderColor: '#1f2937', borderWidth: 1,
          titleColor: '#e5e7eb', bodyColor: '#cbd5e1',
          callbacks: {
            label: (c) => c.parsed.y == null ? null
              : `${c.dataset.label}: ${yFmt(c.parsed.y)}`,
          },
        },
      },
      scales: {
        x: { ticks: { color: COLOR.text, maxRotation: 0, autoSkipPadding: 14 },
             grid: { color: COLOR.grid } },
        y: { title: { display: true, text: yTitle, color: COLOR.text },
             ticks: { color: COLOR.text, callback: yFmt }, grid: { color: COLOR.grid } },
      },
    };
  }

  // ── Helpers ──────────────────────────────────────────────

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
  function formatPct(v) {
    if (v == null) return '—';
    return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
  }
  function hexA(hex, alpha) {
    // Naive #rrggbb -> rgba(); falls back to a neutral fill if hex is non-standard.
    const m = /^#([a-f0-9]{6})$/i.exec(hex);
    if (!m) return 'rgba(139,92,246,' + alpha + ')';
    const num = parseInt(m[1], 16);
    return `rgba(${(num >> 16) & 255},${(num >> 8) & 255},${num & 255},${alpha})`;
  }
})();
