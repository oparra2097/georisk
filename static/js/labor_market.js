/* US Labor Market dashboard — Chart.js renderer.
 *
 * Pulls /api/labor-market/us once, then re-draws the two charts and the
 * track-record tables when the user changes the history window.  The
 * `nowcast.payroll_track_full` and `unrate_track_full` arrays are the
 * full backcast series (one row per fitted month); they're sliced
 * client-side so the toggle is instant and we don't re-fetch.
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
    actualBar:    '#3b82f6',
    actualBarRGB: 'rgba(59,130,246,0.65)',
    estLine:      '#f59e0b',
    estLineSoft:  'rgba(245,158,11,0.85)',
    forecastBar:  'rgba(245,158,11,0.45)',
    unrateActual: '#ef4444',
    grid:         'rgba(255,255,255,0.06)',
    text:         '#9ca3af',
  };

  const state = {
    bundle: null,
    windowMonths: 24,
    payrollChart: null,
    unrateChart: null,
  };

  document.addEventListener('DOMContentLoaded', () => {
    bindToolbar();
    bindRefresh();
    fetchBundle();
  });

  // ── Wiring ──────────────────────────────────────────────────────

  function bindToolbar() {
    document.querySelectorAll('.lm-toggle[data-window]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.lm-toggle[data-window]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.windowMonths = parseInt(btn.dataset.window, 10) || 0;
        renderCharts();
        renderTracks();
      });
    });
  }

  function bindRefresh() {
    const btn = document.getElementById('lm-refresh-btn');
    if (!btn) return;
    btn.addEventListener('click', async () => {
      btn.disabled = true;
      btn.textContent = 'Refreshing…';
      try {
        await fetch(API.refresh, { method: 'POST' });
        await fetchBundle();
      } catch (e) {
        console.error('refresh failed', e);
      } finally {
        btn.disabled = false;
        btn.textContent = 'Refresh data';
      }
    });
  }

  async function fetchBundle() {
    try {
      const resp = await fetch(API.bundle, { credentials: 'same-origin' });
      const data = await resp.json();
      state.bundle = data || {};
      renderHeader();
      renderSummary();
      renderCharts();
      renderTracks();
      renderIndicators();
    } catch (e) {
      console.error('labor market fetch failed', e);
    }
  }

  // ── Header + summary ─────────────────────────────────────────────

  function renderHeader() {
    const bls = (state.bundle && state.bundle.bls) || {};
    const series = bls.series || {};
    const payrolls = series.payrolls || [];
    const unemp = series.unemployment || [];

    const lastPay = payrolls[payrolls.length - 1];
    const lastUR = unemp[unemp.length - 1];
    const nowcast = (state.bundle.nowcast && state.bundle.nowcast.nowcast) || null;

    setText('lm-latest-month', lastPay ? formatMonth(lastPay.date) : '—');
    setText('lm-latest-payroll', lastPay && lastPay.mom_change != null
      ? formatChange(lastPay.mom_change, 'k') : '—');
    setText('lm-latest-unrate', lastUR ? `${lastUR.value.toFixed(1)}%` : '—');

    if (nowcast && nowcast.available) {
      const v = nowcast.payroll_estimate_change;
      setText('lm-nowcast-payroll', `${formatChange(v, 'k')} for ${formatMonth(nowcast.month)}`);
    } else {
      setText('lm-nowcast-payroll', '—');
    }
  }

  function renderSummary() {
    const summary = (state.bundle.nowcast && state.bundle.nowcast.summary) || {};
    const sample = (state.bundle.nowcast && state.bundle.nowcast.sample_size) || {};
    setText('lm-payroll-mae', summary.payroll_mae_thousands != null
      ? `${summary.payroll_mae_thousands}k` : '—');
    setText('lm-payroll-hit', summary.payroll_direction_hit_rate_pct != null
      ? `${summary.payroll_direction_hit_rate_pct}%` : '—');
    setText('lm-unrate-mae', summary.unrate_mae_pp != null
      ? `${summary.unrate_mae_pp} pp` : '—');
    setText('lm-sample', sample.payrolls != null
      ? `${sample.payrolls} mo` : '—');
  }

  // ── Charts ───────────────────────────────────────────────────────

  function renderCharts() {
    if (!state.bundle) return;
    drawPayrollChart();
    drawUnrateChart();
  }

  function sliceWindow(arr, key) {
    if (!arr) return [];
    if (state.windowMonths === 0) return arr;
    return arr.slice(-state.windowMonths);
  }

  function drawPayrollChart() {
    const bls = state.bundle.bls || {};
    const series = (bls.series || {}).payrolls || [];
    const track = (state.bundle.nowcast && state.bundle.nowcast.payroll_track_full) || [];

    // Map estimates by month so we can align with BLS actuals.
    const estByMonth = {};
    track.forEach(r => { estByMonth[r.month] = r.estimate_change; });

    // Actuals: BLS MoM change in thousands.  Pair to month key 'YYYY-MM'.
    const actualByMonth = {};
    series.forEach(pt => {
      if (pt.mom_change == null) return;
      actualByMonth[pt.date] = pt.mom_change;
    });

    // Use the union of months (so the bridge model can extend slightly
    // past the last BLS print when we have a forward nowcast).
    const months = Array.from(new Set([
      ...Object.keys(actualByMonth),
      ...Object.keys(estByMonth),
    ])).sort();

    // Append the forward nowcast month if missing
    const nc = state.bundle.nowcast && state.bundle.nowcast.nowcast;
    if (nc && nc.available && nc.month && !months.includes(nc.month)) {
      months.push(nc.month);
      months.sort();
    }

    const slice = state.windowMonths === 0 ? months : months.slice(-state.windowMonths);

    const labels = slice.map(formatMonth);
    const actualBars = slice.map(m => actualByMonth[m] != null ? actualByMonth[m] : null);
    const estLine = slice.map(m => estByMonth[m] != null ? estByMonth[m] : null);

    // Forward nowcast: one bar at the last month with a dashed/translucent fill
    const forecastBars = slice.map(m =>
      (nc && nc.available && nc.month === m && actualByMonth[m] == null)
        ? nc.payroll_estimate_change
        : null
    );
    if (nc && nc.available && estLine[slice.indexOf(nc.month)] == null) {
      // Make sure the orange line shows the forward nowcast point too
      const idx = slice.indexOf(nc.month);
      if (idx >= 0) estLine[idx] = nc.payroll_estimate_change;
    }

    const ctx = document.getElementById('lm-payroll-chart');
    if (!ctx) return;
    if (state.payrollChart) state.payrollChart.destroy();

    state.payrollChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            type: 'bar',
            label: 'BLS actual (Δ jobs, 000s)',
            data: actualBars,
            backgroundColor: COLOR.actualBarRGB,
            borderColor: COLOR.actualBar,
            borderWidth: 1,
            order: 2,
          },
          {
            type: 'bar',
            label: 'Forward nowcast (Δ jobs, 000s)',
            data: forecastBars,
            backgroundColor: COLOR.forecastBar,
            borderColor: COLOR.estLine,
            borderWidth: 1,
            borderDash: [4, 3],
            order: 2,
          },
          {
            type: 'line',
            label: 'Bridge model estimate',
            data: estLine,
            borderColor: COLOR.estLine,
            backgroundColor: COLOR.estLineSoft,
            tension: 0.25,
            pointRadius: 0,
            pointHoverRadius: 3,
            borderWidth: 2,
            spanGaps: true,
            order: 1,
          },
        ],
      },
      options: chartCommonOptions({
        yTitle: 'Δ jobs (thousands)',
        yFmt: (v) => `${v >= 0 ? '+' : ''}${Math.round(v)}k`,
      }),
    });
  }

  function drawUnrateChart() {
    const bls = state.bundle.bls || {};
    const series = (bls.series || {}).unemployment || [];
    const urTrack = (state.bundle.nowcast && state.bundle.nowcast.unrate_track_full) || [];

    // Build estimated unemployment LEVEL series by chaining the
    // monthly Δ estimates onto the BLS history.  Anchor each estimate
    // to the prior-month BLS level so a single bad month doesn't drift
    // away from reality (this is the standard convention for nowcast
    // overlays).
    const urByMonth = {};
    series.forEach(pt => { urByMonth[pt.date] = pt.value; });
    const months = Array.from(new Set([
      ...Object.keys(urByMonth),
      ...urTrack.map(r => r.month),
    ])).sort();

    const nc = state.bundle.nowcast && state.bundle.nowcast.nowcast;
    if (nc && nc.available && nc.month && !months.includes(nc.month)) {
      months.push(nc.month);
      months.sort();
    }

    const slice = state.windowMonths === 0 ? months : months.slice(-state.windowMonths);
    const labels = slice.map(formatMonth);
    const actual = slice.map(m => urByMonth[m] != null ? urByMonth[m] : null);

    const estTrack = {};
    urTrack.forEach(r => { estTrack[r.month] = r.estimate_change; });

    // Estimated level: prior BLS + estimated Δ.  For the forward nowcast
    // month, anchor on the latest BLS unemployment level and add the
    // bridge-model Δ estimate.
    const estLevel = slice.map((m, i) => {
      const prevMonth = shiftMonth(m, -1);
      const prevActual = urByMonth[prevMonth];
      if (prevActual != null && estTrack[m] != null) {
        return +(prevActual + estTrack[m]).toFixed(2);
      }
      if (nc && nc.available && nc.month === m && nc.implied_unrate_level != null) {
        return nc.implied_unrate_level;
      }
      return null;
    });

    const ctx = document.getElementById('lm-unrate-chart');
    if (!ctx) return;
    if (state.unrateChart) state.unrateChart.destroy();

    state.unrateChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'BLS actual',
            data: actual,
            borderColor: COLOR.unrateActual,
            backgroundColor: 'rgba(239,68,68,0.12)',
            tension: 0.25,
            pointRadius: 0,
            pointHoverRadius: 3,
            borderWidth: 2,
            fill: false,
          },
          {
            label: 'Bridge model estimate',
            data: estLevel,
            borderColor: COLOR.estLine,
            backgroundColor: 'rgba(245,158,11,0.12)',
            borderDash: [5, 4],
            tension: 0.25,
            pointRadius: 0,
            pointHoverRadius: 3,
            borderWidth: 2,
            spanGaps: true,
            fill: false,
          },
        ],
      },
      options: chartCommonOptions({
        yTitle: 'Unemployment rate (%)',
        yFmt: (v) => `${v.toFixed(1)}%`,
      }),
    });
  }

  function chartCommonOptions({ yTitle, yFmt }) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#0a0e1a',
          borderColor: '#1f2937',
          borderWidth: 1,
          titleColor: '#e5e7eb',
          bodyColor: '#cbd5e1',
          callbacks: {
            label: (ctx) => {
              const v = ctx.parsed.y;
              if (v == null) return null;
              return `${ctx.dataset.label}: ${yFmt(v)}`;
            },
          },
        },
      },
      scales: {
        x: {
          ticks: { color: COLOR.text, maxRotation: 0, autoSkipPadding: 14 },
          grid:  { color: COLOR.grid },
        },
        y: {
          title: { display: true, text: yTitle, color: COLOR.text },
          ticks: { color: COLOR.text, callback: yFmt },
          grid:  { color: COLOR.grid },
        },
      },
    };
  }

  // ── Track tables ─────────────────────────────────────────────────

  function renderTracks() {
    const payrollTrack = (state.bundle.nowcast && state.bundle.nowcast.payroll_track) || [];
    const urTrack = (state.bundle.nowcast && state.bundle.nowcast.unrate_track) || [];

    const payTbody = document.querySelector('#lm-payroll-track-table tbody');
    const urTbody = document.querySelector('#lm-unrate-track-table tbody');
    if (payTbody) {
      payTbody.innerHTML = payrollTrack.slice().reverse().map(r => `
        <tr>
          <td>${formatMonth(r.month)}</td>
          <td class="num ${r.actual_change >= 0 ? 'lm-pos' : 'lm-neg'}">${formatChange(r.actual_change, 'k')}</td>
          <td class="num ${r.estimate_change >= 0 ? 'lm-pos' : 'lm-neg'}">${formatChange(r.estimate_change, 'k')}</td>
          <td class="num">${formatChange(r.error, 'k')}</td>
        </tr>`).join('');
    }
    if (urTbody) {
      urTbody.innerHTML = urTrack.slice().reverse().map(r => `
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

  // ── Helpers ──────────────────────────────────────────────────────

  function setText(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
  }
  function formatMonth(s) {
    if (!s) return '';
    const [y, m] = s.split('-');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const mi = parseInt(m, 10) - 1;
    return `${months[mi] || m} ${String(y).slice(-2)}`;
  }
  function formatChange(v, suffix) {
    if (v == null) return '—';
    const sign = v > 0 ? '+' : '';
    if (suffix === 'k') return `${sign}${Math.round(v)}k`;
    if (suffix === 'pp') return `${sign}${v.toFixed(2)}`;
    return `${sign}${v}`;
  }
  function formatNum(v, dp) {
    if (v == null) return '—';
    return Number(v).toFixed(dp || 2);
  }
  function shiftMonth(monthKey, by) {
    const [y, m] = monthKey.split('-').map(Number);
    const total = y * 12 + (m - 1) + by;
    const ny = Math.floor(total / 12);
    const nm = (total % 12) + 1;
    return `${String(ny).padStart(4, '0')}-${String(nm).padStart(2, '0')}`;
  }
})();
