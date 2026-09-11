/* Equity Vol & Options Model.
 *
 * One fetch of /api/options-model/model per parameter set; everything else is
 * client-side. Control changes re-fetch (the simulation runs server-side), so
 * the fetch is debounced and the page keeps the previous render until the new
 * payload lands rather than flashing empty tables.
 */
/* eslint-env browser */
/* global Chart */

(function () {
  'use strict';

  const API = {
    model: '/api/options-model/model',
    refresh: '/api/options-model/refresh',
    export: '/api/options-model/export',
  };

  const COLOR = {
    up: '#10b981', down: '#ef4444', accent: '#e11d48', blue: '#3b82f6',
    amber: '#f59e0b', violet: '#8b5cf6', dim: '#9ca3af',
    grid: 'rgba(255,255,255,0.06)',
  };

  const ENGINE_COLOR = {
    garch_t: COLOR.accent, student_t: COLOR.violet,
    gbm: COLOR.blue, bootstrap: COLOR.amber,
  };

  const state = {
    data: null,
    ticker: 'TSEM',
    tradeView: 'ranked',
    strikeSide: 'call',
    sortKey: 'ev_on_capital',
    sortDir: -1,
    charts: {},
    inFlight: null,
  };

  document.addEventListener('DOMContentLoaded', () => {
    state.ticker = (new URLSearchParams(location.search).get('ticker') || 'TSEM').toUpperCase();
    document.getElementById('om-ticker-input').value = state.ticker;
    bindControls();
    bindTickerForm();
    bindToggles();
    bindSort();
    bindJumpNav();
    fetchModel();
  });

  // ── Params & fetching ─────────────────────────────────────
  function params() {
    const p = new URLSearchParams({
      ticker: state.ticker,
      engine: val('om-engine'),
      horizon: val('om-horizon'),
      drift: val('om-drift'),
      paths: val('om-paths'),
    });
    if (val('om-drift') === 'custom') {
      p.set('mu', String((parseFloat(val('om-mu')) || 0) / 100));
    }
    return p;
  }

  async function fetchModel() {
    const qs = params().toString();
    setLoading(true);
    document.getElementById('om-export').href = API.export + '?' + qs;
    try {
      const resp = await fetch(API.model + '?' + qs, { credentials: 'same-origin' });
      const json = await resp.json();
      if (!resp.ok || json.error) {
        setText('om-narrative', json.error || 'Could not build the model for this ticker.');
        return;
      }
      state.data = json;
      renderAll();
    } catch (e) {
      console.error('options model fetch failed', e);
      setText('om-narrative', 'Request failed — the data provider may be rate-limiting. Try Refresh in a moment.');
    } finally {
      setLoading(false);
    }
  }

  function setLoading(on) {
    document.getElementById('om-page').classList.toggle('om-loading', !!on);
  }

  function bindControls() {
    ['om-engine', 'om-horizon', 'om-drift', 'om-paths'].forEach(id => {
      document.getElementById(id).addEventListener('change', () => {
        const custom = val('om-drift') === 'custom';
        document.getElementById('om-custom-wrap').hidden = !custom;
        renderDriftNote();
        fetchModel();
      });
    });
    const mu = document.getElementById('om-mu');
    let t = null;
    mu.addEventListener('input', () => { clearTimeout(t); t = setTimeout(fetchModel, 600); });

    document.getElementById('om-refresh').addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      btn.disabled = true;
      const prev = btn.textContent;
      btn.textContent = 'Refreshing…';
      try {
        await fetch(API.refresh + '?ticker=' + encodeURIComponent(state.ticker), { method: 'POST' });
        await fetchModel();
      } catch (err) { console.error(err); }
      finally { btn.disabled = false; btn.textContent = prev; }
    });
    renderDriftNote();
  }

  function renderDriftNote() {
    const notes = {
      risk_neutral: 'Risk-neutral drift (r − q): the convention the option market must use. P(up) lands under 50% purely from the half-variance drag.',
      zero: 'Zero log drift: an exact coin flip on direction. Use this to see the distribution with no view at all embedded in it.',
      historical: 'The stock’s own mean return, shrunk 75% toward zero — a two-year sample mean on a name this volatile is almost pure noise.',
      custom: 'Your view, as an annual simple return. Everything downstream — probabilities, EV, the ranking — now inherits it.',
    };
    setText('om-drift-note', notes[val('om-drift')] || '');
  }

  function bindTickerForm() {
    document.getElementById('om-ticker-form').addEventListener('submit', (e) => {
      e.preventDefault();
      const v = document.getElementById('om-ticker-input').value.trim().toUpperCase();
      if (!v) return;
      state.ticker = v;
      history.replaceState(null, '', '?ticker=' + encodeURIComponent(v));
      fetchModel();
    });
  }

  function bindToggles() {
    document.querySelectorAll('.om-toggle[data-view]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.om-toggle[data-view]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.tradeView = btn.dataset.view;
        renderTrades();
      });
    });
    document.querySelectorAll('.om-toggle[data-strike]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.om-toggle[data-strike]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.strikeSide = btn.dataset.strike;
        renderStrikes();
      });
    });
  }

  function bindSort() {
    document.querySelectorAll('#om-trades th.om-sortable').forEach(th => {
      th.addEventListener('click', () => {
        const key = th.dataset.sort;
        if (state.sortKey === key) state.sortDir *= -1;
        else { state.sortKey = key; state.sortDir = (key === 'name') ? 1 : -1; }
        renderTrades();
      });
    });
  }

  function bindJumpNav() {
    const links = Array.from(document.querySelectorAll('.om-jump-link'));
    links.forEach(a => a.addEventListener('click', e => {
      e.preventDefault();
      const el = document.querySelector(a.getAttribute('href'));
      if (el) window.scrollTo({ top: el.offsetTop - 70, behavior: 'smooth' });
    }));
    const sections = links.map(a => document.querySelector(a.getAttribute('href'))).filter(Boolean);
    window.addEventListener('scroll', () => {
      const y = window.scrollY + 130;
      let active = sections[0];
      sections.forEach(s => { if (s.offsetTop <= y) active = s; });
      links.forEach(a => a.classList.toggle('active', a.getAttribute('href') === '#' + (active && active.id)));
    }, { passive: true });
  }

  // ── Render ────────────────────────────────────────────────
  function renderAll() {
    renderHeader();
    renderPresets();
    renderKpis();
    renderDirection();
    renderCone();
    renderEngines();
    renderDistribution();
    renderVol();
    renderTermChart();
    renderChain();
    renderSmile();
    renderTrades();
    renderStrikes();
    renderCalibration();
  }

  function renderHeader() {
    const m = state.data.meta;
    setText('om-ticker-badge', m.ticker);
    document.title = `${m.ticker} Vol & Options — Parra Macro`;
    setText('om-narrative', state.data.narrative || '');
    setText('om-updated', m.updated_at
      ? (m.cached ? 'Cached · ' : 'Updated ') + fmtTime(m.updated_at) : '');
    setText('om-source', `${m.name} · close ${m.as_of} · ${m.history_days} sessions of history · ${m.source}`);
    const stress = (state.data.strategies || {}).vol_stress_points;
    setText('om-stress-pts', stress ? `${(stress * 100).toFixed(1)} vol points` : 'its historical error');
  }

  function renderPresets() {
    const box = document.getElementById('om-presets');
    const presets = (state.data.meta.presets || []);
    box.innerHTML = presets.map(p =>
      `<button class="om-chip${p.symbol === state.data.meta.ticker ? ' active' : ''}" data-sym="${p.symbol}" title="${esc(p.name)}">${p.symbol}</button>`
    ).join('');
    box.querySelectorAll('.om-chip').forEach(b => b.addEventListener('click', () => {
      state.ticker = b.dataset.sym;
      document.getElementById('om-ticker-input').value = state.ticker;
      history.replaceState(null, '', '?ticker=' + encodeURIComponent(state.ticker));
      fetchModel();
    }));
  }

  function renderKpis() {
    const d = state.data;
    const m = d.meta;
    const v = d.vol;
    const near = (d.chain_vol || [])[0];
    const prim = d.simulation.primary;
    const items = [
      { label: 'Spot', value: fmtUsd(m.spot),
        sub: m.change_pct == null ? '' : `${m.change_pct >= 0 ? '+' : ''}${m.change_pct.toFixed(2)}% on the day`,
        cls: (m.change_pct || 0) >= 0 ? 'pos' : 'neg' },
      { label: 'Realized vol 21d', value: pct(v.realized.close_21),
        sub: v.percentile == null ? '' : `${Math.round(v.percentile)}th pctile of its 2y range` },
      { label: 'Model vol · horizon', value: pct(d.simulation.settings.sigma_ann),
        sub: `${d.simulation.settings.engine_label}, ${d.simulation.settings.horizon_days}d` },
      near && near.atm_iv
        ? { label: `ATM implied · ${near.dte}d`, value: pct(near.atm_iv),
            sub: near.vrp == null ? '' : `${near.vrp >= 0 ? '+' : ''}${(near.vrp * 100).toFixed(1)} pts vs model`,
            cls: (near.vrp || 0) >= 0 ? 'pos' : 'neg' }
        : { label: 'ATM implied', value: '—', sub: 'no option chain available' },
      { label: `P(up) · ${d.simulation.settings.horizon_days}d`, value: pct1(prim.p_up),
        sub: `median ${signPct(prim.median_return)}, mean ${signPct(prim.mean_return)}` },
      { label: 'Expected move', value: `±${(prim.expected_abs_move * 100).toFixed(1)}%`,
        sub: `68% band ${fmtUsd(prim.band68[0])}–${fmtUsd(prim.band68[1])}` },
      { label: 'Next earnings', value: m.earnings_date || '—',
        sub: m.earnings_date ? earningsNote(m.earnings_date, d.simulation.settings.horizon_calendar_days) : 'date not published' },
    ];
    document.getElementById('om-kpis').innerHTML = items.map(k => `
      <div class="om-kpi">
        <span class="om-kpi-label">${esc(k.label)}</span>
        <span class="om-kpi-value ${k.cls || ''}">${k.value}</span>
        <span class="om-kpi-sub">${esc(k.sub || '')}</span>
      </div>`).join('');
  }

  function earningsNote(dateStr, horizonCal) {
    const days = Math.round((new Date(dateStr + 'T00:00:00') - new Date()) / 86400000);
    if (days < 0) return 'already reported';
    return days <= horizonCal
      ? `${days}d away — inside the horizon, so the sim understates the gap`
      : `${days}d away — outside the simulated horizon`;
  }

  function renderDirection() {
    const rows = state.data.direction || [];
    const spot = state.data.meta.spot;
    tbody('om-direction', rows.map(d => `
      <tr>
        <td>${d.days}d <span class="om-muted">(${d.calendar_days} cal)</span></td>
        <td class="num">${pct(d.vol)}</td>
        <td class="num ${d.p_up >= 0.5 ? 'om-pos' : 'om-neg'}"><b>${pct1(d.p_up)}</b></td>
        <td class="num">±${(d.one_sd_move * 100).toFixed(1)}%</td>
        <td class="num">${fmtUsd(d.band68[0])}–${fmtUsd(d.band68[1])}</td>
        <td class="num om-neg">${(d.variance_drag * 100).toFixed(2)}%</td>
        <td class="num om-muted">${fmtUsd(spot * (1 + d.p10_move_down))}–${fmtUsd(spot * (1 + d.p90_move_up))}</td>
      </tr>`).join(''));
  }

  function renderEngines() {
    const engines = state.data.simulation.engines || [];
    const chosen = state.data.simulation.settings.engine;
    document.getElementById('om-engines').innerHTML = engines.map(e => `
      <div class="om-engine-card${e.engine === chosen ? ' active' : ''}">
        <span class="om-engine-dot" style="background:${ENGINE_COLOR[e.engine] || COLOR.dim}"></span>
        <div>
          <span class="om-engine-name">${esc(e.engine_label)}</span>
          <span class="om-engine-stats">P(up) ${pct1(e.p_up)} · ±${(e.expected_abs_move * 100).toFixed(1)}% · kurt ${e.excess_kurtosis.toFixed(1)} · CVaR5 ${(e.cvar05 * 100).toFixed(1)}%</span>
        </div>
      </div>`).join('');
  }

  const CONE_HISTORY_DAYS = 60;

  function renderCone() {
    const fan = state.data.simulation.fan || {};
    if (!fan.q50 || !fan.q50.length) return;
    const n = fan.q50.length;
    const spot = state.data.meta.spot;
    const hist = (state.data.price_history || {}).close || [];
    const dates = (state.data.price_history || {}).dates || [];
    const past = hist.slice(-CONE_HISTORY_DAYS);
    const pastDates = dates.slice(-CONE_HISTORY_DAYS);
    const h = past.length;

    // Labels run from -h (oldest close shown) to +n (the horizon). The fan
    // starts at spot so history and simulation join without a visible step.
    const labels = [];
    for (let i = -h + 1; i <= n; i++) labels.push(i);
    const pad = arr => new Array(h - 1).fill(null).concat([spot], arr);
    const histSeries = past.concat(new Array(n).fill(null));

    setText('om-cone-label', `${state.data.simulation.settings.engine_label}, ${n} trading days, ${fmtInt(state.data.simulation.settings.n_paths)} paths`);
    drawChart('om-cone', {
      type: 'line',
      data: {
        labels,
        datasets: [
          band('95th', pad(fan.q95), 'rgba(225,29,72,0.10)', '+1'),
          band('75th', pad(fan.q75), 'rgba(225,29,72,0.18)', '+1'),
          { label: 'Median', data: pad(fan.q50), borderColor: COLOR.accent, borderWidth: 2, pointRadius: 0, fill: false, tension: 0.1, spanGaps: false },
          band('25th', pad(fan.q25), 'rgba(225,29,72,0.18)', '+1'),
          band('5th', pad(fan.q05), 'rgba(225,29,72,0.10)', false),
          { label: 'Actual', data: histSeries, borderColor: COLOR.dim, borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.1 },
        ],
      },
      options: baseOpts({
        x: {
          title: { display: true, text: 'Trading days (0 = today)', color: COLOR.dim },
          ticks: {
            maxTicksLimit: 10,
            callback(v) {
              const lbl = this.getLabelForValue(v);
              const i = Number(lbl);
              if (i === 0) return 'today';
              return i < 0 ? (pastDates[h - 1 + i] || '').slice(5) : '+' + i + 'd';
            },
          },
        },
        y: { ticks: { callback: v => '$' + v.toFixed(0) } },
      }, { legend: false }),
    });
  }

  function band(label, data, color, fill) {
    return { label, data, borderColor: 'transparent', backgroundColor: color, pointRadius: 0, fill, tension: 0.1 };
  }

  function renderDistribution() {
    const h = state.data.simulation.histogram;
    const prim = state.data.simulation.primary;
    const spot = state.data.meta.spot;
    if (h && h.centers.length) {
      drawChart('om-hist', {
        type: 'bar',
        data: {
          labels: h.centers.map(c => c.toFixed(1)),
          datasets: [{
            label: 'Probability',
            data: h.density,
            backgroundColor: h.centers.map(c => c >= spot ? 'rgba(16,185,129,0.55)' : 'rgba(239,68,68,0.55)'),
            borderWidth: 0,
          }],
        },
        options: baseOpts({
          x: { ticks: { maxTicksLimit: 12, callback(v) { return '$' + this.getLabelForValue(v); } } },
          y: { ticks: { callback: v => (v * 100).toFixed(1) + '%' } },
        }, {
          legend: false,
          tooltip: { callbacks: { label: c => `${(c.parsed.y * 100).toFixed(2)}% of paths` } },
        }),
      });
    }
    tbody('om-moves', (prim.moves || []).map(m => `
      <tr>
        <td>±${(m.move * 100).toFixed(0)}%</td>
        <td class="num om-muted">${fmtUsd(spot * (1 + m.move))}</td>
        <td class="num om-pos">${pct1(m.p_up_beyond)}</td>
        <td class="num">${pct1(m.p_touch_up)}</td>
        <td class="num om-muted">${fmtUsd(spot * (1 - m.move))}</td>
        <td class="num om-neg">${pct1(m.p_down_beyond)}</td>
        <td class="num">${pct1(m.p_touch_down)}</td>
      </tr>`).join(''));
    setText('om-tail-note',
      `Simulated tail shape: skew ${prim.skew.toFixed(2)}, excess kurtosis ${prim.excess_kurtosis.toFixed(1)}. ` +
      `Average of the worst 5% of outcomes: ${(prim.cvar05 * 100).toFixed(1)}%. ` +
      `Probability of losing more than 20%: ${pct1(prim.prob_loss_gt_20pct)}.`);
  }

  const VOL_READS = {
    close_5: 'Last week only — fast, and mostly noise on its own',
    close_10: 'Two weeks',
    close_21: 'One month — the standard reference window',
    close_63: 'One quarter',
    close_126: 'Six months',
    close_252: 'One year — the long-run anchor',
    ewma: 'RiskMetrics λ=0.94 — reacts immediately, no fitting',
    yang_zhang_21: 'Gap-aware and drift-independent — the one to trust here',
    parkinson_21: 'Range-based, blind to overnight gaps (so it reads low)',
    garman_klass_21: 'Range + open-close, assumes no gaps',
    rogers_satchell_21: 'Range-based, immune to trend',
  };

  function renderVol() {
    const rv = state.data.vol.realized || {};
    const order = ['close_5', 'close_10', 'close_21', 'close_63', 'close_126', 'close_252',
      'ewma', 'yang_zhang_21', 'parkinson_21', 'garman_klass_21', 'rogers_satchell_21'];
    tbody('om-vol-table', order.filter(k => rv[k] != null).map(k => `
      <tr${k === 'yang_zhang_21' ? ' class="om-row-hl"' : ''}>
        <td>${esc(labelFor(k))}</td>
        <td class="num"><b>${pct(rv[k])}</b></td>
        <td class="om-muted">${esc(VOL_READS[k] || '')}</td>
      </tr>`).join(''));

    const g = state.data.vol.garch;
    const box = document.getElementById('om-garch');
    if (!g) { box.innerHTML = '<p class="om-note">Not enough history to fit a GARCH model.</p>'; return; }
    box.innerHTML = `
      <h4>GARCH(1,1) fit</h4>
      <div class="om-garch-grid">
        ${stat('α (shock)', g.alpha.toFixed(3))}
        ${stat('β (persistence)', g.beta.toFixed(3))}
        ${stat('α+β', g.persistence.toFixed(3))}
        ${stat('Half-life', g.half_life_days ? g.half_life_days.toFixed(1) + 'd' : '—')}
        ${stat('Long-run vol', pct(g.long_run_vol))}
        ${stat('Spot vol', pct(g.spot_vol))}
        ${stat('Student-t df', g.nu.toFixed(1))}
        ${stat('Observations', fmtInt(g.n_obs))}
      </div>
      <p class="om-note">
        A shock decays halfway back to the ${pct(g.long_run_vol)} long-run level in
        ${g.half_life_days ? g.half_life_days.toFixed(0) : '—'} trading days, which is why the
        forecast for a far-dated option differs from today's ${pct(g.spot_vol)} spot vol.
        A df of ${g.nu.toFixed(1)} means the daily shocks are materially fatter-tailed than a normal
        ${g.nu < 6 ? '— wing options are worth more than a Gaussian model says' : ''}.
      </p>`;
  }

  function renderTermChart() {
    const gts = state.data.vol.garch_term_structure || [];
    const cone = state.data.vol.cone || [];
    const chain = state.data.chain_vol || [];
    if (!gts.length && !cone.length) return;
    const labels = gts.map(g => g.days);
    const coneAt = d => {
      const c = cone.find(x => x.window === d);
      return c ? c.median : null;
    };
    const impliedPts = chain.filter(c => c.atm_iv).map(c => ({ x: c.trading_days, y: c.atm_iv }));
    drawChart('om-term', {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'GARCH forecast', data: gts.map(g => g.vol), borderColor: COLOR.accent, borderWidth: 2, pointRadius: 2, tension: 0.2 },
          { label: 'Realized median (cone)', data: labels.map(coneAt), borderColor: COLOR.dim, borderDash: [4, 4], borderWidth: 1.5, pointRadius: 0, spanGaps: true, tension: 0.2 },
          { label: 'ATM implied', data: impliedPts, borderColor: COLOR.blue, backgroundColor: COLOR.blue, showLine: false, pointRadius: 5, pointStyle: 'rectRot' },
        ],
      },
      options: baseOpts({
        x: { type: 'linear', title: { display: true, text: 'Trading days to expiry', color: COLOR.dim } },
        y: { ticks: { callback: v => (v * 100).toFixed(0) + '%' } },
      }, { legend: true, tooltip: { callbacks: { label: c => `${c.dataset.label}: ${(c.parsed.y * 100).toFixed(1)}%` } } }),
    });
  }

  function renderChain() {
    const rows = state.data.chain_vol || [];
    if (!rows.length) {
      tbody('om-chain', '<tr><td colspan="9" class="om-muted">No option chain available for this symbol.</td></tr>');
      return;
    }
    tbody('om-chain', rows.map(c => {
      const vrp = c.vrp;
      const read = vrp == null ? '—'
        : vrp > 0.03 ? 'Options rich — favours selling the range'
        : vrp < -0.03 ? 'Options cheap — favours owning gamma'
        : 'Close to fair';
      return `<tr data-expiry="${esc(c.expiry)}" class="om-chain-row">
        <td>${esc(c.expiry)}</td>
        <td class="num">${c.dte}</td>
        <td class="num">${pct(c.atm_iv)}</td>
        <td class="num">${pct(c.model_vol)}</td>
        <td class="num ${vrp == null ? '' : vrp > 0 ? 'om-pos' : 'om-neg'}"><b>${vrp == null ? '—' : (vrp >= 0 ? '+' : '') + (vrp * 100).toFixed(1)}</b></td>
        <td class="num">${c.skew_25d == null ? '—' : (c.skew_25d >= 0 ? '+' : '') + (c.skew_25d * 100).toFixed(1)}</td>
        <td class="num">±${c.implied_move_pct == null ? '—' : c.implied_move_pct.toFixed(1) + '%'}</td>
        <td class="num om-muted">±${c.model_move_pct == null ? '—' : c.model_move_pct.toFixed(1) + '%'}</td>
        <td class="om-muted">${read}</td>
      </tr>`;
    }).join(''));
    document.querySelectorAll('.om-chain-row').forEach(tr => tr.addEventListener('click', () => {
      renderSmile(tr.dataset.expiry);
    }));
  }

  function renderSmile(expiry) {
    const rows = state.data.chain_vol || [];
    if (!rows.length) return;
    const block = rows.find(r => r.expiry === expiry) || rows[0];
    const smile = (block.smile || []).filter(s => s.iv);
    if (!smile.length) return;
    setText('om-smile-label', `${block.expiry} · ${block.dte}d`);
    const spot = state.data.meta.spot;
    drawChart('om-smile', {
      type: 'line',
      data: {
        datasets: [
          {
            label: 'Implied (OTM quotes)',
            data: smile.map(s => ({ x: s.strike, y: s.iv })),
            borderColor: COLOR.blue, backgroundColor: COLOR.blue,
            pointRadius: 3, borderWidth: 2, tension: 0.25,
          },
          {
            label: 'Model forecast',
            data: [{ x: smile[0].strike, y: block.model_vol }, { x: smile[smile.length - 1].strike, y: block.model_vol }],
            borderColor: COLOR.accent, borderDash: [6, 4], borderWidth: 2, pointRadius: 0,
          },
          {
            label: 'Spot',
            data: [{ x: spot, y: Math.min(...smile.map(s => s.iv)) }, { x: spot, y: Math.max(...smile.map(s => s.iv)) }],
            borderColor: COLOR.dim, borderDash: [2, 3], borderWidth: 1, pointRadius: 0,
          },
        ],
      },
      options: baseOpts({
        x: { type: 'linear', title: { display: true, text: 'Strike', color: COLOR.dim }, ticks: { callback: v => '$' + v } },
        y: { ticks: { callback: v => (v * 100).toFixed(0) + '%' } },
      }, { legend: true, tooltip: { callbacks: { label: c => `${c.dataset.label}: ${(c.parsed.y * 100).toFixed(1)}% @ $${c.parsed.x}` } } }),
    });
  }

  function renderTrades() {
    const s = state.data.strategies || {};
    const rows = (state.tradeView === 'ranked' ? s.ranked : s.all) || [];
    const sorted = rows.slice().sort((a, b) => {
      const k = state.sortKey;
      const av = a[k], bv = b[k];
      if (typeof av === 'string') return state.sortDir * av.localeCompare(bv);
      return state.sortDir * ((av == null ? -9e9 : av) - (bv == null ? -9e9 : bv));
    });

    if (!sorted.length) {
      tbody('om-trades', `<tr><td colspan="15" class="om-muted">${
        state.tradeView === 'ranked'
          ? 'Nothing survives the stress test right now — no structure keeps a positive expected value once you pay the spread and allow the vol forecast to be wrong by its historical error. That is a finding, not a gap: it means there is no edge in this chain worth the risk today.'
          : 'No option chain available for this symbol.'}</td></tr>`);
    } else {
      tbody('om-trades', sorted.map(t => {
        const st = t.ev_stress || {};
        return `<tr class="${t.robust ? 'om-robust' : ''}">
          <td><b>${esc(t.name)}</b><span class="om-trade-type">${esc(t.type)}</span></td>
          <td class="om-legs">${t.legs.filter(l => l.kind !== 'stock').map(l =>
            `<span class="om-leg ${l.qty > 0 ? 'long' : 'short'}">${l.qty > 0 ? '+' : '−'}${Math.abs(l.qty)} ${l.kind === 'call' ? 'C' : 'P'}${fmtNum(l.strike)}</span>`).join('')}
            ${t.legs.some(l => l.kind === 'stock') ? '<span class="om-leg long">+100 shares</span>' : ''}</td>
          <td class="num">${t.dte}</td>
          <td class="num ${t.net_debit > 0 ? '' : 'om-pos'}">${fmtSigned(t.net_debit)}</td>
          <td class="num ${t.ev > 0 ? 'om-pos' : 'om-neg'}"><b>${fmtSigned(t.ev)}</b></td>
          <td class="num ${cls(st.vol_down)}">${st.vol_down == null ? '—' : fmtSigned(st.vol_down)}</td>
          <td class="num ${cls(st.vol_up)}">${st.vol_up == null ? '—' : fmtSigned(st.vol_up)}</td>
          <td class="num">${pct1(t.pop)}</td>
          <td class="num ${t.ev_on_capital > 0 ? 'om-pos' : 'om-neg'}">${t.ev_on_capital == null ? '—' : (t.ev_on_capital * 100).toFixed(1) + '%'}</td>
          <td class="num om-muted">${t.kelly ? (t.kelly * 100).toFixed(0) + '%' : '—'}</td>
          <td class="num">${t.greeks.delta.toFixed(2)}</td>
          <td class="num ${t.greeks.vega > 0 ? 'om-pos' : 'om-neg'}">${t.greeks.vega.toFixed(3)}</td>
          <td class="num">${t.greeks.theta.toFixed(3)}</td>
          <td class="num">${t.max_loss == null ? '<span class="om-warn">unbounded</span>' : fmtUsd(Math.abs(t.max_loss))}</td>
          <td>${liqBadge(t)}</td>
        </tr>`;
      }).join(''));
    }

    const note = state.tradeView === 'ranked'
      ? `${s.n_credible || 0} of ${s.n_total || 0} structures survive all three filters: tradeable quotes, positive expected value after paying the spread, and still positive with the vol forecast wrong in either direction.`
      : `All ${s.n_total || 0} structures, ranked by expected value per dollar of capital. Rows without the green marker fail at least one robustness filter — most often the vol stress.`;
    setText('om-trades-note', note + ' Short structures marked "unbounded" can lose more than the capital shown; Kelly is a growth-optimal ceiling on a distribution this model could be wrong about, not a position size.');
  }

  function liqBadge(t) {
    const map = {
      ok: ['om-badge-ok', 'tradeable'],
      thin: ['om-badge-warn', 'no open interest'],
      wide: ['om-badge-warn', 'wide spread'],
      untradeable: ['om-badge-bad', 'no bid'],
    };
    const [cls_, label] = map[t.liquidity] || map.ok;
    const fill = t.worst_fill_flag ? '<span class="om-badge om-badge-warn" title="The edge disappears if you pay the spread">fill-sensitive</span>' : '';
    return `<span class="om-badge ${cls_}">${label}</span>${fill}`;
  }

  function renderStrikes() {
    const all = (state.data.strike_edges || []).filter(e => e.kind === state.strikeSide);
    if (!all.length) {
      tbody('om-strikes', '<tr><td colspan="11" class="om-muted">No option chain available for this symbol.</td></tr>');
      return;
    }
    tbody('om-strikes', all.map(e => `
      <tr>
        <td class="om-muted">${e.dte}d</td>
        <td class="num"><b>${fmtNum(e.strike)}</b></td>
        <td class="num om-muted">${(e.moneyness * 100).toFixed(0)}%</td>
        <td class="num">${pct(e.iv)}</td>
        <td class="num">${e.delta == null ? '—' : e.delta.toFixed(2)}</td>
        <td class="num">${fmtNum(e.mid)}</td>
        <td class="num">${pct1(e.p_itm_market)}</td>
        <td class="num">${pct1(e.p_itm_model)}</td>
        <td class="num ${e.prob_edge > 0 ? 'om-pos' : 'om-neg'}"><b>${(e.prob_edge >= 0 ? '+' : '') + (e.prob_edge * 100).toFixed(1)}</b></td>
        <td class="num om-muted">${e.model_price == null ? '—' : fmtNum(e.model_price)}</td>
        <td class="num om-muted">${fmtInt(e.open_interest)}</td>
      </tr>`).join(''));
  }

  function renderCalibration() {
    const bt = state.data.backtest;
    const dirBox = document.getElementById('om-calib-direction');
    const volBox = document.getElementById('om-calib-vol');
    const covBox = document.getElementById('om-calib-cover');
    if (!bt || !bt.available) {
      const msg = `<p class="om-note">${esc((bt && bt.reason) || 'Not enough history for an out-of-sample test.')}</p>`;
      dirBox.innerHTML = volBox.innerHTML = covBox.innerHTML = msg;
      setText('om-verdict', '');
      return;
    }
    const d = bt.direction || {};
    dirBox.innerHTML = `
      <table class="om-mini">
        <tr><td>Model</td><td class="num"><b>${d.brier_model.toFixed(4)}</b></td></tr>
        <tr><td>Coin flip</td><td class="num">${d.brier_coinflip.toFixed(4)}</td></tr>
        <tr><td>Momentum tilt</td><td class="num">${d.brier_momentum.toFixed(4)}</td></tr>
        <tr><td>Base rate (up)</td><td class="num">${pct1(d.base_rate_up)}</td></tr>
      </table>
      <p class="om-calib-verdict ${d.skill_vs_coinflip > 0 ? 'good' : 'bad'}">
        Skill vs coin flip: ${(d.skill_vs_coinflip * 100).toFixed(1)}%
        ${d.skill_vs_coinflip > 0.01 ? '— a real if small directional edge' : '— no directional edge, as expected'}
      </p>
      <p class="om-calib-foot">${d.n} out-of-sample windows.</p>`;

    const vol = bt.volatility || {};
    const names = { garch: 'GARCH(1,1)', ewma: 'EWMA', rv21: 'Realized 21d', rv63: 'Realized 63d' };
    volBox.innerHTML = `
      <table class="om-mini">
        <tr><th></th><th class="num">RMSE</th><th class="num">R²</th><th class="num">Slope</th></tr>
        ${Object.entries(vol).map(([k, v]) => `
          <tr class="${k === bt.best_vol_model ? 'om-row-hl' : ''}">
            <td>${names[k] || k}</td>
            <td class="num">${(v.rmse * 100).toFixed(1)}</td>
            <td class="num">${v.r2 == null ? '—' : v.r2.toFixed(2)}</td>
            <td class="num">${v.mz_slope.toFixed(2)}</td>
          </tr>`).join('')}
      </table>
      <p class="om-calib-verdict good">Best: ${names[bt.best_vol_model] || bt.best_vol_model || '—'}</p>
      <p class="om-calib-foot">RMSE in vol points. This error is what every structure in the trade screen is stress-tested against.</p>`;

    const cov = bt.coverage || {};
    covBox.innerHTML = `
      <table class="om-mini">
        <tr><th>Interval</th><th class="num">Nominal</th><th class="num">Actual</th></tr>
        ${(cov.intervals || []).map(i => {
          const gap = i.empirical - i.nominal;
          return `<tr><td>${(i.nominal * 100).toFixed(0)}%</td>
            <td class="num om-muted">${(i.nominal * 100).toFixed(0)}%</td>
            <td class="num ${Math.abs(gap) < 0.03 ? 'om-pos' : 'om-neg'}">${(i.empirical * 100).toFixed(0)}%</td></tr>`;
        }).join('')}
      </table>
      <p class="om-calib-foot">Standardized outcomes: sd ${cov.z_std == null ? '—' : cov.z_std.toFixed(2)},
        skew ${cov.z_skew == null ? '—' : cov.z_skew.toFixed(2)},
        excess kurtosis ${cov.z_excess_kurtosis == null ? '—' : cov.z_excess_kurtosis.toFixed(1)}.
        ${cov.pct_beyond_2sd == null ? '' : `${(cov.pct_beyond_2sd * 100).toFixed(1)}% of outcomes landed beyond 2sd against ${(cov.normal_pct_beyond_2sd * 100).toFixed(1)}% for a normal.`}</p>`;

    setText('om-verdict', bt.verdict || '');
  }

  // ── Chart helpers ─────────────────────────────────────────
  function drawChart(id, config) {
    const el = document.getElementById(id);
    if (!el) return;
    if (state.charts[id]) state.charts[id].destroy();
    state.charts[id] = new Chart(el.getContext('2d'), config);
  }

  function baseOpts(scales, plugins) {
    const s = {};
    Object.entries(scales || {}).forEach(([k, v]) => {
      s[k] = Object.assign({
        grid: { color: COLOR.grid },
        ticks: { color: COLOR.dim, font: { size: 10 } },
      }, v, { ticks: Object.assign({ color: COLOR.dim, font: { size: 10 } }, v.ticks || {}) });
    });
    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: s,
      plugins: {
        legend: (plugins && plugins.legend)
          ? { labels: { color: COLOR.dim, boxWidth: 10, font: { size: 10 } } }
          : { display: false },
        tooltip: Object.assign({
          backgroundColor: '#111827', borderColor: '#1f2937', borderWidth: 1,
          titleColor: '#e5e7eb', bodyColor: '#e5e7eb',
        }, (plugins && plugins.tooltip) || {}),
      },
    };
  }

  // ── Small helpers ─────────────────────────────────────────
  function val(id) { return document.getElementById(id).value; }
  function setText(id, text) { const el = document.getElementById(id); if (el) el.textContent = text; }
  function tbody(id, html) {
    const el = document.querySelector('#' + id + ' tbody');
    if (el) el.innerHTML = html;
  }
  function stat(label, value) {
    return `<div class="om-stat"><span class="om-stat-label">${esc(label)}</span><span class="om-stat-value">${esc(value)}</span></div>`;
  }
  function cls(v) { return v == null ? '' : v > 0 ? 'om-pos' : 'om-neg'; }
  function labelFor(k) {
    return {
      close_5: 'Close-to-close 5d', close_10: 'Close-to-close 10d',
      close_21: 'Close-to-close 21d', close_63: 'Close-to-close 63d',
      close_126: 'Close-to-close 126d', close_252: 'Close-to-close 252d',
      ewma: 'EWMA (λ 0.94)', yang_zhang_21: 'Yang-Zhang 21d',
      parkinson_21: 'Parkinson 21d', garman_klass_21: 'Garman-Klass 21d',
      rogers_satchell_21: 'Rogers-Satchell 21d',
    }[k] || k;
  }
  function pct(v) { return v == null ? '—' : (v * 100).toFixed(0) + '%'; }
  function pct1(v) { return v == null ? '—' : (v * 100).toFixed(1) + '%'; }
  function signPct(v) { return v == null ? '—' : (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%'; }
  function fmtUsd(v) { return v == null ? '—' : '$' + v.toFixed(2); }
  function fmtNum(v) { return v == null ? '—' : (Math.abs(v) >= 100 ? v.toFixed(0) : v.toFixed(2)).replace(/\.00$/, ''); }
  function fmtSigned(v) { return v == null ? '—' : (v >= 0 ? '+' : '−') + '$' + Math.abs(v).toFixed(0); }
  function fmtInt(v) { return v == null ? '—' : Math.round(v).toLocaleString('en-US'); }
  function fmtTime(iso) {
    try {
      return new Date(iso).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
    } catch (e) { return iso; }
  }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }
})();
