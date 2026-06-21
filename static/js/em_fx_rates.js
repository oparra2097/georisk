/* EM FX & Rates — Beta Opportunity Screen.
 *
 * One fetch of /api/em-fx-rates; everything else is client-side. The page
 * leads with the analytical opportunity screen (sortable, filterable table),
 * backed by FX moves, the US curve, EM bond proxies and EM 10Y yields, plus a
 * macro backdrop strip and an auto-generated narrative.
 */
/* eslint-env browser */
/* global Chart */

(function () {
  'use strict';

  const API = { bundle: '/api/em-fx-rates', refresh: '/api/em-fx-rates/refresh' };

  const COLOR = { pos: '#10b981', neg: '#ef4444', dim: '#9ca3af', grid: 'rgba(255,255,255,0.06)' };

  const state = {
    data: null,
    sortKey: 'rank',
    sortDir: 1,        // 1 asc, -1 desc
    sigFilter: 'all',
    charts: {},
  };

  // Which signal labels belong to each filter bucket.
  const SIG_BUCKETS = {
    long: s => /long/i.test(s) && !/vulnerable/i.test(s),
    rich: s => /rich|vulnerable/i.test(s),
    value: s => /value/i.test(s),
  };

  document.addEventListener('DOMContentLoaded', () => {
    bindRefresh();
    bindSort();
    bindSigFilter();
    fetchBundle();
  });

  function bindRefresh() {
    const btn = document.getElementById('efr-refresh');
    if (!btn) return;
    btn.addEventListener('click', async () => {
      btn.disabled = true; const prev = btn.textContent; btn.textContent = 'Refreshing…';
      try { await fetch(API.refresh, { method: 'POST' }); await fetchBundle(); }
      catch (e) { console.error(e); }
      finally { btn.disabled = false; btn.textContent = prev; }
    });
  }

  function bindSort() {
    document.querySelectorAll('#efr-screen th.efr-sortable').forEach(th => {
      th.addEventListener('click', () => {
        const key = th.dataset.sort;
        if (state.sortKey === key) state.sortDir *= -1;
        else { state.sortKey = key; state.sortDir = (key === 'rank' || key === 'name') ? 1 : -1; }
        renderScreen();
      });
    });
  }

  function bindSigFilter() {
    document.querySelectorAll('.efr-toggle[data-sig]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.efr-toggle[data-sig]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.sigFilter = btn.dataset.sig;
        renderScreen();
      });
    });
  }

  async function fetchBundle() {
    try {
      const resp = await fetch(API.bundle, { credentials: 'same-origin' });
      state.data = await resp.json() || {};
      renderHeader();
      renderBackdrop();
      renderScreen();
      renderFxTable();
      renderRates();
    } catch (e) {
      console.error('EM FX fetch failed', e);
    }
  }

  // ── Header ────────────────────────────────────────────────
  function renderHeader() {
    const meta = state.data.meta || {};
    setText('efr-updated', meta.updated_at ? 'Updated ' + fmtTime(meta.updated_at) : '');
    setText('efr-source', meta.source ? 'Sources: ' + meta.source + ` · trailing-${meta.corr_window_days || 90}d betas/correlations` : '');
    setText('efr-method', (state.data.signals && state.data.signals.method) || '');
  }

  // ── Backdrop KPI strip + narrative ────────────────────────
  function renderBackdrop() {
    const b = state.data.benchmarks || {};
    const bd = state.data.backdrop || {};
    const curve = (state.data.rates || {}).us_curve || [];
    const us10 = curve.find(c => c.tenor === '10Y') || {};
    const tiles = [
      kpi('US Dollar (DXY)', (b.dxy || {}).level, (b.dxy || {}).chg, 'level'),
      kpi('MSCI EM (EEM)', (b.em_equity || {}).level, (b.em_equity || {}).chg, 'level'),
      kpi('US 10Y', us10.yield, us10.chg_bp, 'bp'),
      kpi('Brent', (bd.brent || {}).level, (bd.brent || {}).chg, 'level'),
      kpi('Gold', (bd.gold || {}).level, (bd.gold || {}).chg, 'level'),
      kpi('VIX', (bd.vix || {}).level, (bd.vix || {}).chg, 'level', true),
    ];
    const el = document.getElementById('efr-kpis');
    if (el) el.innerHTML = tiles.join('');
    setText('efr-narrative', bd.narrative || '');
  }

  function kpi(label, level, chg, kind, invert) {
    const m1 = (chg || {}).m1;
    const isBp = kind === 'bp';
    let chgTxt = '—', cls = '';
    if (m1 != null) {
      const good = invert ? m1 < 0 : m1 > 0;
      cls = good ? 'efr-pos' : 'efr-neg';
      chgTxt = isBp ? `${m1 >= 0 ? '+' : ''}${Math.round(m1)}bp 1m` : `${m1 >= 0 ? '+' : ''}${m1.toFixed(1)}% 1m`;
    }
    const lvlTxt = level != null ? Number(level).toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—';
    return `<div class="efr-kpi">
      <span class="efr-kpi-label">${label}</span>
      <span class="efr-kpi-value">${lvlTxt}</span>
      <span class="efr-kpi-chg ${cls}">${chgTxt}</span>
    </div>`;
  }

  // ── Opportunity screen ────────────────────────────────────
  function renderScreen() {
    const tbody = document.querySelector('#efr-screen tbody');
    if (!tbody) return;
    const sig = state.data.signals || {};
    let rows = (sig.rows || []).slice();

    if (state.sigFilter !== 'all') {
      const test = SIG_BUCKETS[state.sigFilter];
      rows = rows.filter(r => test && test(r.signal || ''));
    }

    const k = state.sortKey, dir = state.sortDir;
    rows.sort((a, b) => {
      if (k === 'name') return dir * String(a.name).localeCompare(String(b.name));
      const av = a[k], bv = b[k];
      const an = av == null ? -Infinity : av, bn = bv == null ? -Infinity : bv;
      return dir * (an - bn);
    });

    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="11" class="efr-empty">No currencies match this filter.</td></tr>`;
      return;
    }

    tbody.innerHTML = rows.map(r => {
      const badge = signalBadge(r.signal);
      return `<tr>
        <td class="efr-rank">${r.rank ?? '—'}</td>
        <td class="efr-ccy"><span class="efr-flag">${r.flag || ''}</span><span>${r.name}</span><span class="efr-bloc">${r.bloc || ''}</span></td>
        <td>${badge}</td>
        <td class="num ${signCls(r.score)}"><b>${fmt(r.score, 2)}</b></td>
        <td class="num">${r.carry_bp != null ? Math.round(r.carry_bp).toLocaleString() : '—'}</td>
        <td class="num">${fmt(r.carry_to_vol, 2)}</td>
        <td class="num ${signCls(r.mom_3m)}">${pct(r.mom_3m)}</td>
        <td class="num">${fmt(r.beta_dxy, 2)}</td>
        <td class="num ${signCls(r.residual_1m)}" title="DXY-implied 1m: ${pct(r.dxy_implied_1m)}">${pct(r.residual_1m)}</td>
        <td class="num">${fmt(r.ann_vol_pct, 1)}</td>
        <td class="efr-rationale">${r.rationale || ''}</td>
      </tr>`;
    }).join('');

    // Sort indicator on headers
    document.querySelectorAll('#efr-screen th.efr-sortable').forEach(th => {
      th.classList.toggle('efr-sorted', th.dataset.sort === k);
      th.dataset.dir = th.dataset.sort === k ? (dir === 1 ? 'asc' : 'desc') : '';
    });
  }

  function signalBadge(sig) {
    if (!sig) return '';
    let cls = 'efr-badge-neutral';
    if (/value/i.test(sig)) cls = 'efr-badge-value';
    else if (/rich|vulnerable/i.test(sig)) cls = 'efr-badge-rich';
    else if (/long/i.test(sig)) cls = 'efr-badge-long';
    return `<span class="efr-badge ${cls}">${sig}</span>`;
  }

  // ── FX moves table ────────────────────────────────────────
  function renderFxTable() {
    const tbody = document.querySelector('#efr-fx tbody');
    if (!tbody) return;
    const fx = state.data.fx || [];
    tbody.innerHTML = fx.map(f => {
      const c = f.chg || {};
      return `<tr>
        <td class="efr-ccy"><span class="efr-flag">${f.flag || ''}</span><span>${f.name}</span><span class="efr-bloc">${f.code}</span></td>
        <td class="num">${f.spot != null ? f.spot.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'}</td>
        <td class="num ${signCls(c.d1)}">${pct(c.d1)}</td>
        <td class="num ${signCls(c.w1)}">${pct(c.w1)}</td>
        <td class="num ${signCls(c.m1)}">${pct(c.m1)}</td>
        <td class="num ${signCls(c.ytd)}">${pct(c.ytd)}</td>
        <td class="num ${signCls(c.y1)}">${pct(c.y1)}</td>
        <td class="efr-spark-cell"><canvas data-spark="${f.code}" width="120" height="28"></canvas></td>
      </tr>`;
    }).join('');
    fx.forEach(f => drawSpark(f.code, f.spark, (f.chg || {}).m1));
  }

  function drawSpark(code, spark, m1) {
    const el = document.querySelector(`canvas[data-spark="${code}"]`);
    if (!el || !spark || !spark.length) return;
    if (state.charts[code]) state.charts[code].destroy();
    const up = (m1 || 0) >= 0;
    state.charts[code] = new Chart(el, {
      type: 'line',
      data: { labels: spark.map((_, i) => i), datasets: [{ data: spark, borderColor: up ? COLOR.pos : COLOR.neg, borderWidth: 1.4, pointRadius: 0, tension: 0.3, fill: false }] },
      options: { responsive: false, animation: false, plugins: { legend: { display: false }, tooltip: { enabled: false } }, scales: { x: { display: false }, y: { display: false } } },
    });
  }

  // ── Rates ─────────────────────────────────────────────────
  function renderRates() {
    const rates = state.data.rates || {};
    const slope = rates.us_slope_10y_3m;
    setText('efr-slope', slope != null ? `10Y−3M ${slope >= 0 ? '+' : ''}${slope.toFixed(2)}%` : '');

    const curveBody = document.querySelector('#efr-curve tbody');
    if (curveBody) {
      curveBody.innerHTML = (rates.us_curve || []).map(c => {
        const b = c.chg_bp || {};
        return `<tr><td>${c.tenor}</td><td class="num">${fmt(c.yield, 2)}</td>
          <td class="num ${signCls(b.d1)}">${bp(b.d1)}</td>
          <td class="num ${signCls(b.m1)}">${bp(b.m1)}</td>
          <td class="num ${signCls(b.ytd)}">${bp(b.ytd)}</td></tr>`;
      }).join('');
    }

    const etfBody = document.querySelector('#efr-etfs tbody');
    if (etfBody) {
      etfBody.innerHTML = (rates.em_bond_etfs || []).map(e => {
        const c = e.chg || {};
        return `<tr><td title="${e.desc || ''}">${e.name}</td><td class="num">${fmt(e.price, 2)}</td>
          <td class="num ${signCls(c.m1)}">${pct(c.m1)}</td>
          <td class="num ${signCls(c.ytd)}">${pct(c.ytd)}</td></tr>`;
      }).join('');
    }

    const yBody = document.querySelector('#efr-yields tbody');
    const yields = rates.em_10y || [];
    const hasFred = (state.data.meta || {}).has_fred;
    if (yBody) {
      if (!yields.length) {
        const msg = hasFred
          ? 'No EM 10Y series returned data from FRED right now.'
          : 'FRED API key not configured on the server — EM 10Y yields unavailable. FX, the US curve and EM bond proxies above are unaffected.';
        yBody.innerHTML = `<tr><td colspan="5" class="efr-empty">${msg}</td></tr>`;
      } else {
        yBody.innerHTML = yields.map(r => `<tr>
          <td>${r.name}</td>
          <td class="num"><b>${fmt(r.yield, 2)}</b></td>
          <td class="num ${signCls(r.chg_3m_bp)}">${bp(r.chg_3m_bp)}</td>
          <td class="num ${signCls(r.chg_12m_bp)}">${bp(r.chg_12m_bp)}</td>
          <td class="efr-asof">${r.asof || ''}</td>
        </tr>`).join('');
      }
    }
    setText('efr-yield-src', yields.length ? 'FRED · OECD' : '');
    setText('efr-yield-note', yields.length
      ? 'Monthly OECD long-term (10Y) benchmark yields — slower-moving than the market proxies, but the cleanest cross-country level comparison.'
      : '');
  }

  // ── Helpers ───────────────────────────────────────────────
  function setText(id, v) { const el = document.getElementById(id); if (el) el.textContent = v; }
  function fmt(v, dp) { return v == null ? '—' : Number(v).toFixed(dp == null ? 2 : dp); }
  function pct(v) { return v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}%`; }
  function bp(v) { return v == null ? '—' : `${v >= 0 ? '+' : ''}${Math.round(v)}`; }
  function signCls(v) { return v == null ? '' : (v >= 0 ? 'efr-pos' : 'efr-neg'); }
  function fmtTime(iso) {
    try { return new Date(iso).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }); }
    catch { return iso; }
  }
})();
