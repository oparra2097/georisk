/* Sovereign Credit Default — dashboard interactions ─────────────────── */
/* eslint-env browser */

(function () {
  'use strict';

  // Cadence + horizon are passed as query params on every fetch so the
  // backend loads the right fit_state JSON. Annual horizons are years
  // (1/3/5); quarterly horizons are quarters (4/12/20). The headline
  // chart toggle still says 1y/3y/5y — we map it to quarters when
  // quarterly mode is active.
  function _yearsToCadenceHorizon(years) {
    if (state.cadence === 'quarterly') return Math.max(1, years) * 4;
    return years || 1;
  }
  function _qs(extra) {
    const horizon = _yearsToCadenceHorizon(state.historyHorizon || 1);
    const params = new URLSearchParams({
      cadence: state.cadence || 'annual',
      horizon: String(horizon),
      ...(extra || {}),
    });
    return `?${params.toString()}`;
  }

  const API = {
    table: () => `/api/credit-default/table${_qs()}`,
    country: (iso3) => `/api/credit-default/country/${iso3}${_qs()}`,
    history: (iso3, years) =>
      `/api/credit-default/country/${iso3}/history${_qs({
        horizon: String(_yearsToCadenceHorizon(years || 1)),
      })}`,
    dashboard: () => `/api/credit-default/dashboard${_qs()}`,
  };

  let historyChart = null;

  // Indicator labels mirror backend INDICATORS — kept in JS for the
  // contribution panel so we don't make an extra round-trip.
  const INDICATOR_LABELS = {
    gross_debt_pct_gdp: 'Gross debt / GDP',
    fiscal_balance_pct_gdp: 'Fiscal balance',
    interest_pct_revenue: 'Interest / revenue',
    shadow_debt_gap_pp: 'Shadow debt gap',
    current_account_pct_gdp: 'Current account',
    reserves_to_imports_months: 'Import cover (mo)',
    short_term_debt_pct_reserves: 'ST debt / reserves',
    external_debt_pct_gni: 'External debt / GNI',
    real_gdp_growth: 'Real GDP growth',
    inflation: 'Inflation',
    gdp_per_capita_ppp: 'GDP per capita',
    unemployment: 'Unemployment',
    interest_pct_gdp: 'Interest / expense',
  };

  const INDICATOR_UNITS = {
    gross_debt_pct_gdp: '% GDP',
    fiscal_balance_pct_gdp: '% GDP',
    interest_pct_revenue: '%',
    shadow_debt_gap_pp: 'pp',
    current_account_pct_gdp: '% GDP',
    reserves_to_imports_months: 'mo',
    short_term_debt_pct_reserves: '%',
    external_debt_pct_gni: '% GNI',
    real_gdp_growth: '%',
    inflation: '%',
    gdp_per_capita_ppp: '$',
    unemployment: '%',
    interest_pct_gdp: '%',
  };

  // ── State ───────────────────────────────────────────────────────────
  const state = {
    rows: [],
    filtered: [],
    sort: { key: 'pd_1y', dir: 'desc' },
    selectedIso3: null,
    grade: 'all',
    region: '',
    search: '',
    historyHorizon: 1,    // 1y by default, toggleable to 3y / 5y
    cadence: 'annual',    // 'annual' | 'quarterly' — switches fit_state file
  };

  // Agency consensus notch (1=AAA, 22=D) → benchmark default probability
  // at horizons {1, 3, 5}. Mirrors the backend _CONSENSUS_NUM_TO_PD
  // table in fit.py so the chart's reference line uses the same anchor
  // values the model is calibrated against.
  const AGENCY_PD_BY_HORIZON = {
    1:  { 1: 0.0000, 3: 0.0010, 5: 0.0020 },
    2:  { 1: 0.0010, 3: 0.0020, 5: 0.0050 },
    3:  { 1: 0.0010, 3: 0.0030, 5: 0.0070 },
    4:  { 1: 0.0020, 3: 0.0050, 5: 0.0100 },
    5:  { 1: 0.0030, 3: 0.0080, 5: 0.0150 },
    6:  { 1: 0.0050, 3: 0.0120, 5: 0.0200 },
    7:  { 1: 0.0080, 3: 0.0180, 5: 0.0300 },
    8:  { 1: 0.0120, 3: 0.0300, 5: 0.0500 },
    9:  { 1: 0.0180, 3: 0.0450, 5: 0.0750 },
    10: { 1: 0.0250, 3: 0.0600, 5: 0.1000 },
    11: { 1: 0.0400, 3: 0.0900, 5: 0.1500 },
    12: { 1: 0.0600, 3: 0.1300, 5: 0.2000 },
    13: { 1: 0.0800, 3: 0.1700, 5: 0.2500 },
    14: { 1: 0.1100, 3: 0.2300, 5: 0.3300 },
    15: { 1: 0.1500, 3: 0.3000, 5: 0.4200 },
    16: { 1: 0.2000, 3: 0.3800, 5: 0.5200 },
    17: { 1: 0.3500, 3: 0.5600, 5: 0.7000 },
    18: { 1: 0.3500, 3: 0.5600, 5: 0.7000 },
    19: { 1: 0.3500, 3: 0.5600, 5: 0.7000 },
    20: { 1: 0.5800, 3: 0.7500, 5: 0.8300 },
    21: { 1: 0.7000, 3: 0.8300, 5: 0.8800 },
    22: { 1: 1.0000, 3: 1.0000, 5: 1.0000 },
  };

  // ── Boot ────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', () => {
    bindToolbar();
    bindTable();
    bindMethodology();
    fetchTable();
  });

  function fetchTable() {
    fetch(API.table())
      .then((r) => r.json())
      .then((data) => {
        state.rows = data.rows || [];
        document.getElementById('cd-as-of').textContent = formatAsOf(data.as_of);
        populateRegionFilter(state.rows);
        updateSummary(state.rows);
        applyFilters();
      })
      .catch((err) => {
        document.getElementById('cd-tbody').innerHTML =
          `<tr><td colspan="12" class="cd-loading">Failed to load: ${escapeHtml(String(err))}</td></tr>`;
      });
  }

  // ── Toolbar bindings ────────────────────────────────────────────────
  function bindToolbar() {
    document.getElementById('cd-search').addEventListener('input', (e) => {
      state.search = (e.target.value || '').trim().toLowerCase();
      applyFilters();
    });
    document.getElementById('cd-region-filter').addEventListener('change', (e) => {
      state.region = e.target.value;
      applyFilters();
    });
    // Scope each toggle group to its own buttons so the grade and
    // horizon toggles don't fight over the global `.cd-toggle` class.
    const gradeGroup = document.querySelector('.cd-toolbar .cd-toggle-group');
    if (gradeGroup) {
      gradeGroup.querySelectorAll('.cd-toggle').forEach((btn) => {
        btn.addEventListener('click', () => {
          gradeGroup.querySelectorAll('.cd-toggle')
            .forEach((b) => b.classList.remove('active'));
          btn.classList.add('active');
          state.grade = btn.dataset.grade;
          applyFilters();
        });
      });
    }
    const back = document.getElementById('cd-back-btn');
    if (back) back.addEventListener('click', exitDetailView);
    const detailSearch = document.getElementById('cd-detail-search');
    if (detailSearch) {
      detailSearch.addEventListener('input', (e) =>
        renderDetailSidebar(e.target.value));
    }
    const horizonGroup = document.querySelector('.cd-history-horizon');
    if (horizonGroup) {
      horizonGroup.querySelectorAll('.cd-toggle').forEach((btn) => {
        btn.addEventListener('click', () => {
          state.historyHorizon = parseInt(btn.dataset.horizon, 10) || 1;
          syncHorizonButtons();
          reloadHistoryForCurrent();
        });
      });
    }
    const cadenceGroup = document.querySelector('.cd-cadence-group');
    if (cadenceGroup) {
      cadenceGroup.querySelectorAll('.cd-toggle').forEach((btn) => {
        btn.addEventListener('click', () => {
          const cad = btn.dataset.cadence === 'quarterly' ? 'quarterly' : 'annual';
          if (cad === state.cadence) return;
          state.cadence = cad;
          cadenceGroup.querySelectorAll('.cd-toggle')
            .forEach((b) => b.classList.remove('active'));
          btn.classList.add('active');
          // Re-fetch the table at the new cadence so PD columns and the
          // Watch indicator reflect the active fit_state.
          fetchTable();
          if (state.selectedIso3) {
            loadDetail(state.selectedIso3);
          }
        });
      });
    }
  }

  function syncHorizonButtons() {
    document.querySelectorAll('.cd-history-horizon .cd-toggle').forEach((b) => {
      const on = parseInt(b.dataset.horizon, 10) === state.historyHorizon;
      b.classList.toggle('active', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
  }

  function populateRegionFilter(rows) {
    const sel = document.getElementById('cd-region-filter');
    const regions = Array.from(new Set(rows.map((r) => r.region).filter(Boolean))).sort();
    // Keep "All regions" first; clear any prior options.
    sel.innerHTML = '<option value="">All regions</option>';
    regions.forEach((r) => {
      const opt = document.createElement('option');
      opt.value = r; opt.textContent = r;
      sel.appendChild(opt);
    });
  }

  function updateSummary(rows) {
    document.getElementById('cd-country-count').textContent = String(rows.length);
    const pds = rows.map((r) => r.pd_1y).filter((v) => typeof v === 'number');
    const avg = pds.length ? (pds.reduce((a, b) => a + b, 0) / pds.length) : null;
    document.getElementById('cd-avg-pd').textContent = avg !== null ? formatPct(avg) : '—';
    const inDef = rows.filter((r) => r.defaulted).length;
    document.getElementById('cd-in-default').textContent = String(inDef);
  }

  // ── Filtering / sorting ─────────────────────────────────────────────
  function applyFilters() {
    let rows = state.rows.slice();

    if (state.search) {
      rows = rows.filter((r) =>
        (r.name || '').toLowerCase().includes(state.search) ||
        (r.iso3 || '').toLowerCase().includes(state.search)
      );
    }
    if (state.region) {
      rows = rows.filter((r) => r.region === state.region);
    }
    if (state.grade !== 'all') {
      rows = rows.filter((r) => gradeBucket(r) === state.grade);
    }

    rows.sort((a, b) => compareRows(a, b, state.sort.key, state.sort.dir));
    state.filtered = rows;
    renderTable();
    if (state.selectedIso3 && !rows.some((r) => r.iso3 === state.selectedIso3)) {
      // Selection was filtered out — clear panel.
      state.selectedIso3 = null;
      clearPanel();
    }
  }

  function compareRows(a, b, key, dir) {
    // Synthetic key: rank watchlist entries by signed notch delta so
    // sorting "watch" descending puts the strongest downgrade signals
    // at the top, ascending puts the strongest upgrade signals first.
    if (key === 'watch') {
      key = 'notch_delta_sp';
    }
    const va = a[key]; const vb = b[key];
    const sign = dir === 'asc' ? 1 : -1;
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (typeof va === 'number' && typeof vb === 'number') {
      return sign * (va - vb);
    }
    return sign * String(va).localeCompare(String(vb));
  }

  function gradeBucket(r) {
    if (r.defaulted || r.sp_equiv === 'D' || r.sp_equiv === 'SD') return 'distressed';
    if (r.is_investment_grade) return 'ig';
    // PD-band proxy for the HY vs distressed split.
    if (r.pd_1y == null) return 'hy';
    if (r.pd_1y >= 0.20) return 'distressed';
    return 'hy';
  }

  // ── Table rendering ─────────────────────────────────────────────────
  function bindTable() {
    document.querySelectorAll('.cd-table thead th.sortable').forEach((th) => {
      th.addEventListener('click', () => {
        const key = th.dataset.sort;
        if (state.sort.key === key) {
          state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
        } else {
          state.sort.key = key;
          state.sort.dir = ['name', 'region', 'agency_sp', 'agency_moodys',
                            'agency_fitch', 'pm_numeric'].includes(key) ? 'asc' : 'desc';
        }
        // Update aria/visual indicators
        document.querySelectorAll('.cd-table thead th.sortable').forEach((t) => {
          t.classList.remove('sort-asc', 'sort-desc');
        });
        th.classList.add(state.sort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
        applyFilters();
      });
    });
  }

  function renderTable() {
    const tbody = document.getElementById('cd-tbody');
    if (!state.filtered.length) {
      tbody.innerHTML = '<tr><td colspan="12" class="cd-loading">No countries match the current filters.</td></tr>';
      return;
    }
    const html = state.filtered.map((r) => rowHtml(r)).join('');
    tbody.innerHTML = html;
    tbody.querySelectorAll('tr[data-iso3]').forEach((tr) => {
      tr.addEventListener('click', () => selectCountry(tr.dataset.iso3));
    });
    if (state.selectedIso3) {
      const sel = tbody.querySelector(`tr[data-iso3="${state.selectedIso3}"]`);
      if (sel) sel.classList.add('selected');
    }
  }

  function rowHtml(r) {
    return `
      <tr data-iso3="${r.iso3}">
        <td><span class="cd-flag">${flagEmoji(r.iso3)}</span>${escapeHtml(r.name || r.iso3)}<span class="cd-iso">${r.iso3}</span></td>
        <td>${ratingChipHtml(r)}</td>
        <td class="num">${compositeScoreHtml(r)}</td>
        <td>${escapeHtml(r.agency_sp || '—')}</td>
        <td>${escapeHtml(r.agency_moodys || '—')}</td>
        <td>${escapeHtml(r.agency_fitch || '—')}</td>
        <td class="num">${notchDeltaHtml(r.notch_delta_sp)}</td>
        <td>${watchIndicatorHtml(r)}</td>
        <td class="num">${pdHtml(r.pd_1y)}</td>
        <td class="num">${pdHtml(r.pd_3y)}</td>
        <td class="num">${pdHtml(r.pd_5y)}</td>
        <td class="num">${formatNumber(r.shadow_debt_gap_pp, 1, 'pp')}</td>
      </tr>
    `;
  }

  // Downgrade / upgrade indicator: positive notch_delta means the model
  // rates the country worse than S&P (candidate for downgrade); negative
  // means it rates better (candidate for upgrade). Magnitude ≥3 gets a
  // double arrow as a stronger signal.
  function watchIndicatorHtml(r) {
    const d = r.notch_delta_sp;
    if (d == null) return '<span class="cd-watch-none">—</span>';
    if (d >= 3)  return '<span class="cd-watch-down" title="Strong downgrade signal (model 3+ notches harsher than S&amp;P)">↓↓</span>';
    if (d >= 1)  return '<span class="cd-watch-down" title="Downgrade candidate (model harsher than S&amp;P)">↓</span>';
    if (d <= -3) return '<span class="cd-watch-up" title="Strong upgrade signal (model 3+ notches more lenient than S&amp;P)">↑↑</span>';
    if (d <= -1) return '<span class="cd-watch-up" title="Upgrade candidate (model more lenient than S&amp;P)">↑</span>';
    return '<span class="cd-watch-none">—</span>';
  }

  function compositeScoreHtml(r) {
    if (r.composite_score == null) return '—';
    return r.composite_score.toFixed(1);
  }

  function ratingChipHtml(r) {
    if (r.defaulted) return '<span class="cd-rating-chip default">D</span>';
    const letter = r.sp_equiv || '';
    const cls = chipClassForLetter(letter);
    return `<span class="cd-rating-chip ${cls}">${escapeHtml(letter || '—')}</span>`;
  }

  function compositeChipHtml(r) {
    if (r.defaulted) return '<span class="cd-rating-chip default">D</span>';
    const letter = r.composite_sp_equiv || r.composite_pm_notch || '';
    const cls = chipClassForLetter(letter);
    return `<span class="cd-rating-chip ${cls}" title="Composite reference score">${escapeHtml(letter || '—')}</span>`;
  }

  function chipClassForLetter(letter) {
    // Map an S&P-equivalent letter ('AAA', 'AA+', 'BBB-', 'CCC', 'D')
    // back to the legacy notch-based chip styling. Internally the
    // pm_numeric 1..20 ladder still exists for sort and risk-tier
    // logic; we just don't surface it on screen.
    if (!letter) return '';
    if (letter === 'D' || letter === 'SD') return 'default';
    const SP_TO_NUMERIC = {
      'AAA': 1,  'AA+': 2,  'AA': 3,   'AA-': 4,
      'A+':  5,  'A':   6,  'A-': 7,
      'BBB+':8,  'BBB': 9,  'BBB-':10,
      'BB+': 11, 'BB':  12, 'BB-': 13,
      'B+':  14, 'B':   15, 'B-':  16,
      'CCC+':17, 'CCC': 17, 'CCC-':17,
      'CC':  18, 'C':   19,
    };
    const whole = SP_TO_NUMERIC[letter] || NaN;
    if (isNaN(whole)) return '';
    if (whole <= 7) return 'ig';            // AAA through A-
    if (whole === 10) return 'crossover';   // BBB- straddle to HY
    if (whole <= 10) return 'ig';           // BBB+/BBB
    if (whole <= 16) return 'hy';           // BB+ through B-
    return 'distressed';                    // CCC and below
  }

  function notchDeltaHtml(d) {
    if (d == null) return '<span class="cd-delta-zero">—</span>';
    if (d > 0)  return `<span class="cd-delta-pos">+${d}</span>`;
    if (d < 0)  return `<span class="cd-delta-neg">${d}</span>`;
    return '<span class="cd-delta-zero">0</span>';
  }

  function pdHtml(p) {
    if (p == null) return '—';
    const widthPct = Math.min(80, Math.max(2, p * 80));
    return `<span class="cd-pd-bar" style="width:${widthPct.toFixed(1)}px"></span>${formatPct(p)}`;
  }

  // ── Country drilldown / detail-view navigation ─────────────────────
  function selectCountry(iso3) {
    state.selectedIso3 = iso3;
    document.querySelectorAll('.cd-table tbody tr')
      .forEach((tr) => tr.classList.toggle('selected', tr.dataset.iso3 === iso3));
    enterDetailView(iso3);
  }

  function enterDetailView(iso3) {
    const page = document.getElementById('cd-page');
    const detailView = document.getElementById('cd-detail-view');
    if (!page || !detailView) return;
    page.dataset.view = 'detail';
    detailView.hidden = false;
    renderDetailSidebar();
    loadDetail(iso3);
    window.scrollTo({ top: 0, behavior: 'instant' });
  }

  function exitDetailView() {
    const page = document.getElementById('cd-page');
    const detailView = document.getElementById('cd-detail-view');
    if (!page || !detailView) return;
    page.dataset.view = 'browse';
    detailView.hidden = true;
  }

  function loadDetail(iso3) {
    state.selectedIso3 = iso3;
    syncHorizonButtons();
    document.querySelectorAll('#cd-detail-list li')
      .forEach((li) => li.classList.toggle('selected', li.dataset.iso3 === iso3));
    fetch(API.country(iso3))
      .then((r) => r.json())
      .then((c) => renderPanel(c))
      .catch(() => clearPanel());
    fetch(API.history(iso3, state.historyHorizon))
      .then((r) => (r.ok ? r.json() : null))
      .then((h) => renderHistoryChart(h))
      .catch(() => renderHistoryChart(null));
    // Scroll the detail panel back to the top so a navigated-to country
    // doesn't inherit the previous scroll position.
    const panel = document.getElementById('cd-panel');
    if (panel) panel.scrollTop = 0;
  }

  function reloadHistoryForCurrent() {
    if (!state.selectedIso3) return;
    fetch(API.history(state.selectedIso3, state.historyHorizon))
      .then((r) => (r.ok ? r.json() : null))
      .then((h) => renderHistoryChart(h))
      .catch(() => renderHistoryChart(null));
  }

  function renderDetailSidebar(filterText) {
    const list = document.getElementById('cd-detail-list');
    if (!list) return;
    const q = (filterText || '').trim().toLowerCase();
    const rows = state.rows.slice().sort((a, b) => {
      const an = (a.name || a.iso3 || '').toLowerCase();
      const bn = (b.name || b.iso3 || '').toLowerCase();
      return an.localeCompare(bn);
    });
    const matched = q
      ? rows.filter((r) =>
          (r.name || '').toLowerCase().includes(q) ||
          (r.iso3 || '').toLowerCase().includes(q))
      : rows;
    list.innerHTML = matched.map((r) => `
      <li data-iso3="${r.iso3}" role="option">
        <span class="cd-detail-name">${flagEmoji(r.iso3)} ${escapeHtml(r.name || r.iso3)}</span>
        <span class="cd-detail-rating">${escapeHtml(r.sp_equiv || '—')}</span>
        <span class="cd-detail-pd">${formatPct(r.pd_1y)}</span>
      </li>
    `).join('');
    list.querySelectorAll('li[data-iso3]').forEach((li) => {
      li.addEventListener('click', () => loadDetail(li.dataset.iso3));
      if (li.dataset.iso3 === state.selectedIso3) li.classList.add('selected');
    });
  }

  function renderPanel(c) {
    if (!c || c.error) return clearPanel();
    document.querySelector('.cd-panel-empty').hidden = true;
    document.querySelector('.cd-panel-content').hidden = false;

    document.getElementById('cd-panel-name').textContent = c.name || c.iso3;
    document.getElementById('cd-panel-region').textContent = c.region || '';

    const rating = c.rating || {};
    document.getElementById('cd-panel-pm-sp').textContent = rating.sp_equiv || '—';
    const sourceTag = rating.source === 'fitted' ? ' · fitted' : ' · composite';
    document.getElementById('cd-panel-pm-grade').textContent =
      (rating.is_investment_grade ? 'IG' :
        (rating.defaulted ? 'In default' : 'HY')) + sourceTag;
    // Raw model rating (pre-anchor-pull). Show only when the displayed
    // letter has been pulled — otherwise headline IS the raw model.
    const rawEl = document.getElementById('cd-panel-pm-raw');
    if (rawEl) {
      const pulled = rating.anchor_pull;
      if (pulled && rating.raw_sp_equiv && rating.raw_sp_equiv !== rating.sp_equiv) {
        rawEl.textContent = `raw model: ${rating.raw_sp_equiv}`;
        rawEl.hidden = false;
      } else {
        rawEl.hidden = true;
      }
    }

    // Composite reference score on a 0–100 log-odds scale, with HIGHER
    // = higher default risk (0 best, 100 worst). Independent of the
    // fitted PD model: built from the transparent weighted z-sum.
    const composite = rating.composite || {};
    const compEl = document.getElementById('cd-panel-composite');
    if (compEl) {
      compEl.textContent = composite.score != null ? composite.score.toFixed(1) : '—';
    }
    const compMetaEl = document.getElementById('cd-panel-composite-meta');
    if (compMetaEl) {
      compMetaEl.textContent = '0 best, 100 worst';
    }

    const agency = c.agency || {};
    setRating('cd-panel-sp', agency.sp, agency.sp_outlook);
    setRating('cd-panel-moodys', agency.moodys, agency.moodys_outlook);
    setRating('cd-panel-fitch', agency.fitch, agency.fitch_outlook);

    document.getElementById('cd-panel-pd1').textContent = formatPct(rating.pd_1y);
    document.getElementById('cd-panel-pd3').textContent = formatPct(rating.pd_3y);
    document.getElementById('cd-panel-pd5').textContent = formatPct(rating.pd_5y);
    document.getElementById('cd-panel-score').textContent = rating.score != null ? rating.score.toFixed(1) : '—';

    const shadow = c.shadow_debt || {};
    document.getElementById('cd-panel-official').textContent = formatNumber(shadow.official_debt_gdp, 1, '%');
    document.getElementById('cd-panel-estimated').textContent = formatNumber(shadow.estimated_debt_gdp, 1, '%');
    document.getElementById('cd-panel-gap').textContent = formatNumber(shadow.debt_gap_pp, 1, 'pp');
    document.getElementById('cd-panel-tier').textContent = shadow.risk_tier || '—';

    renderContributions(rating.contributions || [], c.indicator_periods || {});
  }

  function setRating(id, value, outlook) {
    document.getElementById(id).textContent = value || '—';
    const meta = document.getElementById(id + '-outlook');
    if (meta) meta.textContent = outlook || '';
  }

  function renderContributions(contribs, periods) {
    const wrap = document.getElementById('cd-panel-contributions');
    // Sort by absolute contribution; place missing at the bottom.
    const sorted = contribs.slice().sort((a, b) => {
      const sa = a.contribution == null ? -Infinity : Math.abs(a.contribution);
      const sb = b.contribution == null ? -Infinity : Math.abs(b.contribution);
      return sb - sa;
    });
    const maxAbs = Math.max(0.01, ...sorted.map((c) => Math.abs(c.contribution || 0)));
    const html = sorted.map((c) => contribRowHtml(c, maxAbs, periods || {})).join('');
    wrap.innerHTML = html;
  }

  function contribRowHtml(c, maxAbs, periods) {
    const label = INDICATOR_LABELS[c.indicator] || c.indicator;
    const units = INDICATOR_UNITS[c.indicator] || '';
    const period = (periods || {})[c.indicator];
    const periodTag = period ? ` <span class="cd-contrib-period">${escapeHtml(period)}</span>` : '';
    if (c.contribution == null) {
      return `
        <div class="cd-contrib-row">
          <div class="cd-contrib-label">${escapeHtml(label)}${periodTag}</div>
          <div class="cd-contrib-value">—</div>
          <div class="cd-contrib-bar-track"><div class="cd-contrib-bar missing"></div></div>
        </div>`;
    }
    const ratio = Math.abs(c.contribution) / maxAbs;
    const widthPct = (ratio * 50).toFixed(1);  // half-track from centre
    const cls = c.contribution >= 0 ? 'risk' : 'hedge';
    const valueStr = formatNumber(c.value, 1, units);
    return `
      <div class="cd-contrib-row">
        <div class="cd-contrib-label">${escapeHtml(label)}${periodTag}</div>
        <div class="cd-contrib-value">${valueStr}</div>
        <div class="cd-contrib-bar-track">
          <div class="cd-contrib-bar ${cls}" style="width:${widthPct}%"></div>
        </div>
      </div>
    `;
  }

  function clearPanel() {
    document.querySelector('.cd-panel-empty').hidden = false;
    document.querySelector('.cd-panel-content').hidden = true;
  }

  // ── Methodology dialog ──────────────────────────────────────────────
  function bindMethodology() {
    const dlg = document.getElementById('cd-methodology-dialog');
    document.getElementById('cd-methodology-btn').addEventListener('click', () => {
      if (typeof dlg.showModal === 'function') dlg.showModal();
      else dlg.setAttribute('open', '');
    });
    dlg.querySelector('.cd-dialog-close').addEventListener('click', () => {
      if (typeof dlg.close === 'function') dlg.close();
      else dlg.removeAttribute('open');
    });
    dlg.addEventListener('click', (e) => {
      if (e.target === dlg && typeof dlg.close === 'function') dlg.close();
    });
  }

  // ── Formatters ──────────────────────────────────────────────────────
  function formatPct(v) {
    if (v == null || isNaN(v)) return '—';
    if (v >= 1) return '100%';
    if (v < 0.001) return '<0.1%';
    return (v * 100).toFixed(1) + '%';
  }
  function formatNumber(v, dp, unit) {
    if (v == null || isNaN(v)) return '—';
    const num = Number(v).toFixed(dp == null ? 1 : dp);
    return unit ? `${num}${unit === '$' ? '' : ' '}${unit}` : num;
  }
  function formatAsOf(s) {
    if (!s) return '—';
    return s.replace('T', ' ').replace('Z', ' UTC');
  }
  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function flagEmoji(iso3) {
    // Approximate ISO3 -> ISO2 -> regional indicator emoji. Falls back to '🏳️'
    // for codes we don't map (a handful of micro-states). This is a pragmatic
    // mapping — not exhaustive, but covers all sovereigns the IMF reports.
    const m = ISO3_TO_ISO2[iso3];
    if (!m) return '🏳️';
    return String.fromCodePoint(...m.split('').map((c) => 0x1F1E6 + c.charCodeAt(0) - 65));
  }

  // ── ISO3 → ISO2 (for flag emoji) ────────────────────────────────────
  const ISO3_TO_ISO2 = {
    AFG:'AF',ALB:'AL',DZA:'DZ',AND:'AD',AGO:'AO',ATG:'AG',ARG:'AR',ARM:'AM',
    AUS:'AU',AUT:'AT',AZE:'AZ',BHS:'BS',BHR:'BH',BGD:'BD',BRB:'BB',BLR:'BY',
    BEL:'BE',BLZ:'BZ',BEN:'BJ',BTN:'BT',BOL:'BO',BIH:'BA',BWA:'BW',BRA:'BR',
    BRN:'BN',BGR:'BG',BFA:'BF',BDI:'BI',CPV:'CV',KHM:'KH',CMR:'CM',CAN:'CA',
    CAF:'CF',TCD:'TD',CHL:'CL',CHN:'CN',COL:'CO',COM:'KM',COG:'CG',COD:'CD',
    CRI:'CR',CIV:'CI',HRV:'HR',CUB:'CU',CYP:'CY',CZE:'CZ',DNK:'DK',DJI:'DJ',
    DMA:'DM',DOM:'DO',ECU:'EC',EGY:'EG',SLV:'SV',GNQ:'GQ',ERI:'ER',EST:'EE',
    SWZ:'SZ',ETH:'ET',FJI:'FJ',FIN:'FI',FRA:'FR',GAB:'GA',GMB:'GM',GEO:'GE',
    DEU:'DE',GHA:'GH',GRC:'GR',GRD:'GD',GTM:'GT',GIN:'GN',GNB:'GW',GUY:'GY',
    HTI:'HT',HND:'HN',HKG:'HK',HUN:'HU',ISL:'IS',IND:'IN',IDN:'ID',IRN:'IR',
    IRQ:'IQ',IRL:'IE',ISR:'IL',ITA:'IT',JAM:'JM',JPN:'JP',JOR:'JO',KAZ:'KZ',
    KEN:'KE',KIR:'KI',PRK:'KP',KOR:'KR',KWT:'KW',KGZ:'KG',LAO:'LA',LVA:'LV',
    LBN:'LB',LSO:'LS',LBR:'LR',LBY:'LY',LIE:'LI',LTU:'LT',LUX:'LU',MDG:'MG',
    MWI:'MW',MYS:'MY',MDV:'MV',MLI:'ML',MLT:'MT',MHL:'MH',MRT:'MR',MUS:'MU',
    MEX:'MX',FSM:'FM',MDA:'MD',MCO:'MC',MNG:'MN',MNE:'ME',MAR:'MA',MOZ:'MZ',
    MMR:'MM',NAM:'NA',NRU:'NR',NPL:'NP',NLD:'NL',NZL:'NZ',NIC:'NI',NER:'NE',
    NGA:'NG',MKD:'MK',NOR:'NO',OMN:'OM',PAK:'PK',PLW:'PW',PSE:'PS',PAN:'PA',
    PNG:'PG',PRY:'PY',PER:'PE',PHL:'PH',POL:'PL',PRT:'PT',QAT:'QA',ROU:'RO',
    RUS:'RU',RWA:'RW',KNA:'KN',LCA:'LC',VCT:'VC',WSM:'WS',SMR:'SM',STP:'ST',
    SAU:'SA',SEN:'SN',SRB:'RS',SYC:'SC',SLE:'SL',SGP:'SG',SVK:'SK',SVN:'SI',
    SLB:'SB',SOM:'SO',ZAF:'ZA',SSD:'SS',ESP:'ES',LKA:'LK',SDN:'SD',SUR:'SR',
    SWE:'SE',CHE:'CH',SYR:'SY',TWN:'TW',TJK:'TJ',TZA:'TZ',THA:'TH',TLS:'TL',
    TGO:'TG',TON:'TO',TTO:'TT',TUN:'TN',TUR:'TR',TKM:'TM',TUV:'TV',UGA:'UG',
    UKR:'UA',ARE:'AE',GBR:'GB',USA:'US',URY:'UY',UZB:'UZ',VUT:'VU',VAT:'VA',
    VEN:'VE',VNM:'VN',YEM:'YE',ZMB:'ZM',ZWE:'ZW',
  };

  // ── Historical PD trend chart ───────────────────────────────────────
  function renderHistoryChart(h) {
    const canvas = document.getElementById('cd-panel-history');
    const empty = document.getElementById('cd-panel-history-empty');
    if (!canvas) return;

    if (!h || !h.history || !h.history.length || typeof Chart === 'undefined') {
      if (historyChart) {
        historyChart.destroy();
        historyChart = null;
      }
      canvas.style.display = 'none';
      if (empty) empty.hidden = false;
      return;
    }
    canvas.style.display = '';
    if (empty) empty.hidden = true;

    // The API returns ``horizon_years = quarters`` in quarterly mode
    // (4/12/20). Translate back to display years so the chart label
    // matches the toggle (1y/3y/5y) regardless of cadence.
    const rawHorizon = h.horizon_years || state.historyHorizon || 1;
    const horizon = (h.cadence === 'quarterly')
      ? Math.max(1, Math.round(rawHorizon / 4))
      : rawHorizon;
    const years = h.history.map((r) => r.year);
    const pd = h.history.map((r) => (r.model_pd != null ? r.model_pd * 100 : null));
    const composite = h.history.map((r) => (r.composite_score != null ? r.composite_score : null));
    const yMin = Math.min(...years);
    const yMax = Math.max(...years);

    // Default-event spans → vertical red bands. We restrict to events
    // that count as a hard credit event in the model's binary target so
    // the chart isn't cluttered with Paris-Club rescheduling history.
    // Multiple CRAG events for the same country often overlap (the BoC
    // panel keeps Paris Club / bank loan / domestic-arrears entries
    // running in parallel) — we merge them into a single band per
    // overlapping cluster so the chart doesn't end up as a wall of
    // stacked transparent reds with overlapping labels.
    const HARD_EVENT_TYPES = new Set(['default', 'restructuring', 'arrears']);
    const rawBands = (h.default_events || [])
      .filter((e) => HARD_EVENT_TYPES.has(e.event_type))
      .map((e) => {
        const start = Math.max(yMin, e.start_year || yMin);
        const end = Math.min(yMax, e.end_year || yMax);
        if (end < yMin || start > yMax) return null;
        return { start, end, type: e.event_type, instrument: e.instrument || '' };
      })
      .filter(Boolean)
      .sort((a, b) => a.start - b.start || a.end - b.end);

    const eventBands = [];
    rawBands.forEach((b) => {
      const last = eventBands[eventBands.length - 1];
      if (last && b.start <= last.end + 1) {
        last.end = Math.max(last.end, b.end);
        last.types.add(b.type);
      } else {
        eventBands.push({
          start: b.start, end: b.end, types: new Set([b.type]),
        });
      }
    });
    eventBands.forEach((b) => {
      // Pick the most-severe label: default > restructuring > arrears.
      const order = ['default', 'restructuring', 'arrears'];
      const top = order.find((t) => b.types.has(t)) || [...b.types][0];
      b.label = top.toUpperCase();
    });

    // Agency consensus → implied PD at the active horizon. Two paths:
    //
    // (a) If we have a historical rating timeline (h.agency_history),
    //     project each rating action onto the chart's year axis and
    //     emit a step-line of agency-implied PD over time. This is
    //     the proper back-test view — model PD line vs agency rating
    //     line over the same window, with default-event bands as
    //     ground truth.
    // (b) Otherwise fall back to a single horizontal reference line
    //     at the current consensus PD.
    const consensusNum = (h.agency || {}).consensus_num;
    const fallbackAgencyPd = (
      consensusNum != null && AGENCY_PD_BY_HORIZON[consensusNum]
        ? (AGENCY_PD_BY_HORIZON[consensusNum][horizon] || 0) * 100
        : null
    );
    let agencyLine = null;
    let usingAgencyHistory = false;
    const agencyHistory = h.agency_history || [];
    if (agencyHistory.length) {
      // Build a {year -> consensus_num} map by stepping through the
      // sorted rating actions; carry the last seen rating forward.
      const sorted = agencyHistory.slice().sort(
        (a, b) => (a.as_of || '').localeCompare(b.as_of || '')
      );
      let curr = null;
      let idx = 0;
      agencyLine = years.map((yr) => {
        // Advance the pointer through any actions that happened
        // on or before this year.
        while (idx < sorted.length) {
          const actionYear = parseInt(String(sorted[idx].as_of).slice(0, 4), 10);
          if (Number.isFinite(actionYear) && actionYear <= yr) {
            const c = sorted[idx].consensus_num;
            if (c != null && AGENCY_PD_BY_HORIZON[c]) {
              curr = (AGENCY_PD_BY_HORIZON[c][horizon] || 0) * 100;
            }
            idx += 1;
          } else {
            break;
          }
        }
        return curr;
      });
      // Only count it as "history" if at least one year has a value;
      // otherwise fall back to the single-anchor line below.
      if (agencyLine.some((v) => v != null)) {
        usingAgencyHistory = true;
      }
    }
    if (!usingAgencyHistory && fallbackAgencyPd != null) {
      agencyLine = years.map(() => fallbackAgencyPd);
    }

    // Plugin: red event bands + a per-event label so "default years"
    // are visible at a glance + a 50% PD threshold line marking the
    // "default territory" cutoff (the user wants this visible because
    // even the most distressed model PD typically sits well below 50%,
    // so the threshold line is a clear marker for "would the model
    // call this country a default").
    // Default-territory threshold: tighter at 1y (catches near-term
    // distress earlier), looser at 5y (only sovereigns we genuinely
    // expect to default cross 75% over a 5-year window).
    const PD_THRESHOLDS_BY_HORIZON = { 1: 25, 3: 50, 5: 75 };
    const DEFAULT_PD_THRESHOLD_PCT = PD_THRESHOLDS_BY_HORIZON[horizon] || 50;
    const eventBandsPlugin = {
      id: 'cd-event-bands',
      beforeDatasetsDraw(chart) {
        const { ctx, chartArea, scales } = chart;
        if (!chartArea || !scales.x) return;

        // 1. Red event bands (merged so overlaps don't stack).
        ctx.save();
        ctx.fillStyle = 'rgba(229, 57, 53, 0.20)';
        eventBands.forEach((b) => {
          const x0 = scales.x.getPixelForValue(String(b.start));
          const x1 = scales.x.getPixelForValue(String(b.end));
          const left = Math.min(x0, x1);
          const width = Math.max(2, Math.abs(x1 - x0));
          ctx.fillRect(left, chartArea.top, width, chartArea.bottom - chartArea.top);
        });

        // 2. Single label per merged band: full word for wide bands,
        //    one-letter tag (D / R / A) for narrow bands so no event
        //    is anonymous. Stroked white halo + red fill keeps text
        //    legible whether it lands over the blue PD line or the
        //    purple agency line.
        ctx.font = '10px ui-sans-serif, system-ui, sans-serif';
        ctx.textBaseline = 'top';
        ctx.lineWidth = 3;
        ctx.lineJoin = 'round';
        eventBands.forEach((b) => {
          const x0 = scales.x.getPixelForValue(String(b.start));
          const x1 = scales.x.getPixelForValue(String(b.end));
          const left = Math.min(x0, x1);
          const width = Math.abs(x1 - x0);
          const text = width >= 36 ? b.label : (b.label[0] || '');
          if (!text) return;
          ctx.strokeStyle = 'rgba(255, 255, 255, 0.95)';
          ctx.fillStyle = 'rgba(153, 27, 27, 0.95)';
          const tx = left + (width >= 36 ? 4 : 1);
          const ty = chartArea.top + 4;
          ctx.strokeText(text, tx, ty);
          ctx.fillText(text, tx, ty);
        });
        ctx.restore();
      },
      afterDatasetsDraw(chart) {
        // 3. Default-territory threshold line at 50% PD.
        const { ctx, chartArea, scales } = chart;
        if (!chartArea || !scales.y) return;
        if (scales.y.max < DEFAULT_PD_THRESHOLD_PCT) return;
        const y = scales.y.getPixelForValue(DEFAULT_PD_THRESHOLD_PCT);
        if (y < chartArea.top || y > chartArea.bottom) return;
        ctx.save();
        ctx.strokeStyle = 'rgba(220, 38, 38, 0.55)';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(chartArea.left, y);
        ctx.lineTo(chartArea.right, y);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = 'rgba(220, 38, 38, 0.85)';
        ctx.font = '10px ui-sans-serif, system-ui, sans-serif';
        ctx.textBaseline = 'bottom';
        ctx.fillText(`Default threshold (${DEFAULT_PD_THRESHOLD_PCT}%)`, chartArea.left + 6, y - 2);
        ctx.restore();
      },
    };

    if (historyChart) historyChart.destroy();

    const datasets = [{
      label: `Model PD ${horizon}y (%)`,
      data: pd,
      borderColor: '#3b82f6',
      backgroundColor: 'rgba(59, 130, 246, 0.18)',
      tension: 0.2,
      spanGaps: true,
      pointRadius: 2,
      fill: true,
      yAxisID: 'y',
    }];
    if (composite.some((v) => v != null)) {
      datasets.push({
        label: 'Composite z-score (50 = panel median)',
        data: composite,
        borderColor: '#f59e0b',
        backgroundColor: 'transparent',
        borderDash: [3, 3],
        borderWidth: 1.5,
        tension: 0.2,
        spanGaps: true,
        pointRadius: 1.5,
        fill: false,
        yAxisID: 'yc',
      });
    }
    if (agencyLine) {
      const sp = (h.agency || {}).sp;
      const moodys = (h.agency || {}).moodys;
      const fitch = (h.agency || {}).fitch;
      const tag = [sp, moodys, fitch].filter(Boolean).join(' / ');
      const baseLabel = usingAgencyHistory
        ? `Agency consensus PD ${horizon}y (history)`
        : `Agency consensus PD ${horizon}y (${tag || '?'})`;
      datasets.push({
        label: baseLabel,
        data: agencyLine,
        borderColor: '#a855f7',
        borderDash: [6, 4],
        borderWidth: 1.5,
        pointRadius: usingAgencyHistory ? 1.5 : 0,
        spanGaps: true,
        stepped: usingAgencyHistory ? 'before' : false,
        fill: false,
      });
    }

    historyChart = new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: { labels: years.map(String), datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true,
            position: 'left',
            ticks: { callback: (v) => `${v}%`, color: '#9ca3af', font: { size: 10 } },
            title: { display: true, text: `PD ${horizon}y`, color: '#e5e7eb', font: { size: 11 } },
            grid: { color: 'rgba(255, 255, 255, 0.06)' },
          },
          yc: {
            display: composite.some((v) => v != null),
            position: 'right',
            min: 0,
            max: 100,
            ticks: { color: '#f59e0b', font: { size: 10 } },
            title: { display: true, text: 'Composite (0-100)', color: '#f59e0b', font: { size: 11 } },
            grid: { drawOnChartArea: false },
          },
          x: {
            title: { display: true, text: 'Year', color: '#e5e7eb', font: { size: 11 } },
            ticks: { color: '#9ca3af', font: { size: 10 } },
            grid: { color: 'rgba(255, 255, 255, 0.04)' },
          },
        },
        plugins: {
          legend: {
            display: true, position: 'bottom',
            labels: { boxWidth: 10, font: { size: 10 }, color: '#e5e7eb' },
          },
          tooltip: {
            callbacks: {
              afterBody(ctxs) {
                if (!ctxs || !ctxs.length) return '';
                const yr = parseInt(ctxs[0].label, 10);
                const ev = eventBands.filter((b) => yr >= b.start && yr <= b.end);
                return ev.length ? ev.map((e) => `In default: ${e.label}`) : '';
              },
            },
          },
        },
      },
      plugins: [eventBandsPlugin],
    });
  }

})();
