/*
 * /em-rates — LATAM + Asia top 6 policy rate direction + 10Y direction.
 *
 * Two fetches in parallel:
 *   /api/em-rates/panel        — 12-country panel (main render)
 *   /api/em-rates/methodology  — config table for the drawer
 *
 * Renders four tables (LATAM policy, LATAM 10Y, Asia policy, Asia 10Y)
 * plus a hero strip summary. Region tabs (All / LATAM / Asia) filter
 * which sections are visible.
 */

(function () {
  'use strict';

  const $ = (sel, root) => (root || document).querySelector(sel);
  const $$ = (sel, root) => Array.from((root || document).querySelectorAll(sel));

  const state = { panel: null, methodology: null, region: 'all' };

  Promise.all([
    fetch('/api/em-rates/panel').then((r) => (r.ok ? r.json() : null)).catch(() => null),
    fetch('/api/em-rates/methodology').then((r) => (r.ok ? r.json() : null)).catch(() => null),
  ]).then(([panel, methodology]) => {
    state.panel = panel;
    state.methodology = methodology;
    renderHero();
    renderTables();
    renderMethodology();
    bindTabs();
  });

  // ── Hero ──────────────────────────────────────────────────────────
  function renderHero() {
    const summary = ((state.panel || {}).summary) || {};
    const h = summary.n_hikes ?? 0;
    const d = summary.n_holds ?? 0;
    const c = summary.n_cuts ?? 0;

    const stanceEl = $('#hero-stance');
    let stance = 'MIXED', cls = 'hold';
    if (h > c + 1) { stance = 'TIGHTENING'; cls = 'hike'; }
    else if (c > h + 1) { stance = 'EASING'; cls = 'cut'; }
    else { stance = 'BALANCED'; cls = 'hold'; }
    stanceEl.textContent = stance;
    stanceEl.className = 'val ' + cls;

    $('#hero-counts').textContent = `${h} · ${d} · ${c}`;
    $('#hero-avg-rate').textContent = summary.panel_avg_rate != null
      ? summary.panel_avg_rate.toFixed(2) + '%' : '—';
    $('#hero-avg-cpi').textContent = summary.panel_avg_cpi_yoy != null
      ? summary.panel_avg_cpi_yoy.toFixed(2) + '%' : '—';
  }

  // ── Tables ────────────────────────────────────────────────────────
  function renderTables() {
    const countries = ((state.panel || {}).countries) || [];
    for (const region of ['LATAM', 'Asia']) {
      const rows = countries.filter((c) => c.region === region);
      renderPolicyRows(region, rows);
      renderYieldRows(region, rows);
    }
  }

  function renderPolicyRows(region, rows) {
    const tbody = document.querySelector(`[data-region-body="${region}-policy"]`);
    if (!tbody) return;
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="7" class="emr-empty">No data for ${region}.</td></tr>`;
      return;
    }
    tbody.innerHTML = rows.map((r) => {
      const model = r.model || {};
      const dir = model.direction || 'HOLD';
      const dirCls = dir === 'HIKE' ? 'hike' : (dir === 'CUT' ? 'cut' : 'hold');
      const dirLabel = model.magnitude_bp
        ? `${dir} ${model.magnitude_bp > 0 ? '+' : ''}${model.magnitude_bp}bp`
        : dir;
      const gapBp = model.gap_bp;
      const gapCls = gapBp == null ? '' : gapBp > 25 ? 'emr-gap-hike'
                    : gapBp < -25 ? 'emr-gap-cut' : 'emr-gap-hold';
      const gapStr = gapBp == null ? '—' : (gapBp > 0 ? '+' : '') + gapBp.toFixed(0);
      const confCls = (model.confidence || 'LOW').toLowerCase();
      const nextMeeting = r.next_meeting ? formatShortDate(r.next_meeting) : '—';
      const fitBadge = r.fit_ok === false
        ? '<span class="emr-fit-issue" title="Fit failed">fit</span>' : '';

      // Strategist expected direction (optional override per country).
      const strat = r.strategist_expected || null;
      let stratChip = '';
      let divergenceFlag = '';
      if (strat && strat.direction) {
        const sDir = strat.direction;
        const sCls = sDir === 'HIKE' ? 'hike' : (sDir === 'CUT' ? 'cut' : 'hold');
        const sLbl = strat.magnitude_bp
          ? `${sDir} ${strat.magnitude_bp > 0 ? '+' : ''}${strat.magnitude_bp}bp`
          : sDir;
        stratChip = `<span class="emr-chip strategist ${sCls}" title="${escapeAttr(strat.note || '')}">${sLbl}</span>`;
        if (sDir !== dir) {
          const flagTitle = `Model: ${dir}. Strategist: ${sDir}. ${strat.note || ''}`;
          divergenceFlag = `<span class="emr-divergence-flag" title="${escapeAttr(flagTitle)}">⚡ divergence</span>`;
        }
      }
      const callCell = strat
        ? `
          <div class="emr-call-stack">
            <div class="emr-call-row">
              <span class="emr-call-label">Model</span>
              <span class="emr-chip ${dirCls}">${dirLabel}</span>
              <span class="emr-conf-dot ${confCls}" title="Confidence: ${model.confidence || 'LOW'}"></span>
            </div>
            <div class="emr-call-row">
              <span class="emr-call-label">Strat</span>
              ${stratChip}
            </div>
            ${divergenceFlag}
          </div>
        `
        : `
          <span class="emr-chip ${dirCls}">${dirLabel}</span>
          <span class="emr-conf-dot ${confCls}" title="Confidence: ${model.confidence || 'LOW'}"></span>
        `;

      return `
        <tr>
          <td>
            <div class="country">
              <span class="name">${r.name} ${fitBadge}</span>
              <span class="cb">${r.cb_name} · ${r.rate_name || ''}</span>
            </div>
          </td>
          <td class="num">${fmtRate(r.current_rate)}</td>
          <td class="num">${fmtRate(r.real_rate)}</td>
          <td class="num">${fmtRate(r.cpi_yoy)}</td>
          <td>${nextMeeting}</td>
          <td>${callCell}</td>
          <td class="num ${gapCls}">${gapStr}</td>
        </tr>
      `;
    }).join('');
  }

  function renderYieldRows(region, rows) {
    const tbody = document.querySelector(`[data-region-body="${region}-yield"]`);
    if (!tbody) return;
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="5" class="emr-empty">No data for ${region}.</td></tr>`;
      return;
    }
    tbody.innerHTML = rows.map((r) => {
      const dir = r.yield_direction || 'UNKNOWN';
      const dirCls = dir.toLowerCase();
      const d3 = r.delta_3m_bp;
      const d12 = r.delta_12m_bp;
      return `
        <tr>
          <td>
            <div class="country">
              <span class="name">${r.name}</span>
              <span class="cb">${r.ccy} 10Y</span>
            </div>
          </td>
          <td class="num">${fmtRate(r.yield_10y)}</td>
          <td class="num ${deltaCls(d3)}">${fmtBp(d3)}</td>
          <td class="num ${deltaCls(d12)}">${fmtBp(d12)}</td>
          <td><span class="emr-chip ${dirCls}">${dir}</span></td>
        </tr>
      `;
    }).join('');
  }

  // ── Region tabs ───────────────────────────────────────────────────
  function bindTabs() {
    $$('.emr-region-tab').forEach((btn) => {
      btn.addEventListener('click', () => {
        $$('.emr-region-tab').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.region = btn.dataset.region;
        applyFilter();
      });
    });
  }

  function applyFilter() {
    const wanted = state.region;
    $$('.emr-region').forEach((section) => {
      const r = section.dataset.region;
      section.style.display =
        (wanted === 'all' || r === wanted) ? '' : 'none';
    });
  }

  // ── Methodology drawer ────────────────────────────────────────────
  function renderMethodology() {
    const tbody = $('#emr-config-tbody');
    if (!tbody || !state.methodology) return;
    const countries = state.methodology.countries || [];
    if (!countries.length) {
      tbody.innerHTML = '<tr><td colspan="5">Loading…</td></tr>';
      return;
    }
    tbody.innerHTML = countries.map((c) => `
      <tr>
        <td>${c.name}</td>
        <td>${c.cb_name}</td>
        <td>${c.rate_name || '—'}</td>
        <td>${c.target_cpi != null ? c.target_cpi.toFixed(1) + '%' : '—'}</td>
        <td>${c.neutral_r != null ? c.neutral_r.toFixed(1) + '%' : '—'}</td>
      </tr>
    `).join('');
  }

  // ── Formatters ────────────────────────────────────────────────────
  function fmtRate(v) {
    if (v == null || isNaN(v)) return '—';
    return v.toFixed(2) + '%';
  }
  function fmtBp(v) {
    if (v == null || isNaN(v)) return '—';
    return (v > 0 ? '+' : '') + v.toFixed(0);
  }
  function deltaCls(v) {
    if (v == null) return '';
    if (v > 15) return 'emr-delta-up';
    if (v < -15) return 'emr-delta-down';
    return 'emr-delta-flat';
  }
  function escapeAttr(s) {
    return (s || '').toString()
      .replace(/&/g, '&amp;').replace(/"/g, '&quot;')
      .replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
  function formatShortDate(iso) {
    try {
      const d = new Date(iso + 'T12:00:00Z');
      return d.toLocaleDateString('en-US', {
        month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC',
      });
    } catch (e) {
      return iso;
    }
  }
})();
