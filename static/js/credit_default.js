/* Sovereign Credit Default — dashboard interactions ─────────────────── */
/* eslint-env browser */

(function () {
  'use strict';

  const API = {
    table: '/api/credit-default/table',
    country: (iso3) => `/api/credit-default/country/${iso3}`,
    history: (iso3) => `/api/credit-default/country/${iso3}/history`,
    dashboard: '/api/credit-default/dashboard',
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
  };

  // ── Boot ────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', () => {
    bindToolbar();
    bindTable();
    bindMethodology();
    fetchTable();
  });

  function fetchTable() {
    fetch(API.table)
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
          `<tr><td colspan="11" class="cd-loading">Failed to load: ${escapeHtml(String(err))}</td></tr>`;
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
    document.querySelectorAll('.cd-toggle').forEach((btn) => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.cd-toggle').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.grade = btn.dataset.grade;
        applyFilters();
      });
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
    if (r.defaulted || r.pm_notch === '10') return 'distressed';
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
                            'agency_fitch', 'pm_notch'].includes(key) ? 'asc' : 'desc';
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
      tbody.innerHTML = '<tr><td colspan="11" class="cd-loading">No countries match the current filters.</td></tr>';
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
        <td>${escapeHtml(r.region || '')}</td>
        <td>${ratingChipHtml(r)}</td>
        <td>${compositeChipHtml(r)}</td>
        <td>${escapeHtml(r.agency_sp || '—')}</td>
        <td>${escapeHtml(r.agency_moodys || '—')}</td>
        <td>${escapeHtml(r.agency_fitch || '—')}</td>
        <td class="num">${notchDeltaHtml(r.notch_delta_sp)}</td>
        <td class="num">${pdHtml(r.pd_1y)}</td>
        <td class="num">${pdHtml(r.pd_3y)}</td>
        <td class="num">${pdHtml(r.pd_5y)}</td>
        <td class="num">${formatNumber(r.shadow_debt_gap_pp, 1, 'pp')}</td>
      </tr>
    `;
  }

  function ratingChipHtml(r) {
    if (r.defaulted) return '<span class="cd-rating-chip default">10</span>';
    const notch = r.pm_notch || '';
    const cls = chipClassForNotch(notch);
    return `<span class="cd-rating-chip ${cls}">${escapeHtml(notch || '—')}</span>`;
  }

  function compositeChipHtml(r) {
    if (r.defaulted) return '<span class="cd-rating-chip default">10</span>';
    const notch = r.composite_pm_notch || '';
    const cls = chipClassForNotch(notch);
    return `<span class="cd-rating-chip ${cls}" title="Composite reference score">${escapeHtml(notch || '—')}</span>`;
  }

  function chipClassForNotch(notch) {
    if (!notch) return '';
    if (notch === '10') return 'default';
    const wholeStr = notch.replace(/[+-]/g, '');
    const whole = parseInt(wholeStr, 10);
    if (isNaN(whole)) return '';
    if (whole <= 4 && !(whole === 4 && notch === '4-')) return 'ig';
    if (whole === 4) return 'crossover';   // BBB- straddle
    if (whole <= 6) return 'hy';
    return 'distressed';
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

  // ── Country drilldown ───────────────────────────────────────────────
  function selectCountry(iso3) {
    state.selectedIso3 = iso3;
    document.querySelectorAll('.cd-table tbody tr').forEach((tr) => tr.classList.toggle('selected', tr.dataset.iso3 === iso3));
    fetch(API.country(iso3))
      .then((r) => r.json())
      .then((c) => renderPanel(c))
      .catch(() => clearPanel());
    fetch(API.history(iso3))
      .then((r) => (r.ok ? r.json() : null))
      .then((h) => renderHistoryChart(h))
      .catch(() => renderHistoryChart(null));
  }

  function renderPanel(c) {
    if (!c || c.error) return clearPanel();
    document.querySelector('.cd-panel-empty').hidden = true;
    document.querySelector('.cd-panel-content').hidden = false;

    document.getElementById('cd-panel-name').textContent = c.name || c.iso3;
    document.getElementById('cd-panel-region').textContent = c.region || '';

    const rating = c.rating || {};
    document.getElementById('cd-panel-pm-notch').textContent = rating.pm_notch || '—';
    const sourceTag = rating.source === 'fitted' ? ' · fitted' : ' · composite';
    document.getElementById('cd-panel-pm-grade').textContent =
      (rating.is_investment_grade ? 'IG' :
        (rating.defaulted ? 'In default' : 'HY')) + sourceTag;

    // Composite reference score (the "separate score").
    const composite = rating.composite || {};
    const compEl = document.getElementById('cd-panel-composite');
    if (compEl) {
      compEl.textContent = composite.pm_notch || '—';
    }
    const compMetaEl = document.getElementById('cd-panel-composite-meta');
    if (compMetaEl) {
      compMetaEl.textContent = composite.score != null ? composite.score.toFixed(1) : '';
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

    renderContributions(rating.contributions || []);
  }

  function setRating(id, value, outlook) {
    document.getElementById(id).textContent = value || '—';
    const meta = document.getElementById(id + '-outlook');
    if (meta) meta.textContent = outlook || '';
  }

  function renderContributions(contribs) {
    const wrap = document.getElementById('cd-panel-contributions');
    // Sort by absolute contribution; place missing at the bottom.
    const sorted = contribs.slice().sort((a, b) => {
      const sa = a.contribution == null ? -Infinity : Math.abs(a.contribution);
      const sb = b.contribution == null ? -Infinity : Math.abs(b.contribution);
      return sb - sa;
    });
    const maxAbs = Math.max(0.01, ...sorted.map((c) => Math.abs(c.contribution || 0)));
    const html = sorted.map((c) => contribRowHtml(c, maxAbs)).join('');
    wrap.innerHTML = html;
  }

  function contribRowHtml(c, maxAbs) {
    const label = INDICATOR_LABELS[c.indicator] || c.indicator;
    const units = INDICATOR_UNITS[c.indicator] || '';
    if (c.contribution == null) {
      return `
        <div class="cd-contrib-row">
          <div class="cd-contrib-label">${escapeHtml(label)}</div>
          <div class="cd-contrib-value">—</div>
          <div class="cd-contrib-bar-track"><div class="cd-contrib-bar missing"></div></div>
          <div class="cd-contrib-z">no data</div>
        </div>`;
    }
    const ratio = Math.abs(c.contribution) / maxAbs;
    const widthPct = (ratio * 50).toFixed(1);  // half-track from centre
    const cls = c.contribution >= 0 ? 'risk' : 'hedge';
    const valueStr = formatNumber(c.value, 1, units);
    const zStr = c.z != null ? `z=${c.z.toFixed(1)}` : '';
    return `
      <div class="cd-contrib-row">
        <div class="cd-contrib-label">${escapeHtml(label)}</div>
        <div class="cd-contrib-value">${valueStr}</div>
        <div class="cd-contrib-bar-track">
          <div class="cd-contrib-bar ${cls}" style="width:${widthPct}%"></div>
        </div>
        <div class="cd-contrib-z">${zStr}</div>
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

    const years = h.history.map((r) => r.year);
    const pd = h.history.map((r) => (r.model_pd != null ? r.model_pd * 100 : null));
    const yMin = Math.min(...years);
    const yMax = Math.max(...years);

    // Default-event spans → vertical red bands. We restrict to events
    // that count as a hard credit event in the model's binary target so
    // the chart isn't cluttered with Paris-Club rescheduling history.
    const HARD_EVENT_TYPES = new Set(['default', 'restructuring', 'arrears']);
    const eventBands = (h.default_events || [])
      .filter((e) => HARD_EVENT_TYPES.has(e.event_type))
      .map((e) => {
        const start = Math.max(yMin, e.start_year || yMin);
        const end = Math.min(yMax, e.end_year || yMax);
        if (end < yMin || start > yMax) return null;
        return { start, end, label: `${e.event_type}/${e.instrument}` };
      })
      .filter(Boolean);

    const annotationPlugin = {
      id: 'cd-event-bands',
      beforeDatasetsDraw(chart) {
        const { ctx, chartArea, scales } = chart;
        if (!chartArea || !scales.x) return;
        ctx.save();
        ctx.fillStyle = 'rgba(229, 57, 53, 0.18)';
        eventBands.forEach((b) => {
          const x0 = scales.x.getPixelForValue(String(b.start));
          const x1 = scales.x.getPixelForValue(String(b.end));
          const left = Math.min(x0, x1);
          const width = Math.max(2, Math.abs(x1 - x0));
          ctx.fillRect(left, chartArea.top, width, chartArea.bottom - chartArea.top);
        });
        ctx.restore();
      },
    };

    if (historyChart) historyChart.destroy();

    historyChart = new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: years.map(String),
        datasets: [{
          label: 'Model PD 1y (%)',
          data: pd,
          borderColor: '#1976d2',
          backgroundColor: 'rgba(25, 118, 210, 0.12)',
          tension: 0.2,
          spanGaps: true,
          pointRadius: 2,
          fill: true,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true,
            ticks: { callback: (v) => `${v}%` },
            title: { display: true, text: 'PD 1y' },
          },
          x: { title: { display: true, text: 'Year' } },
        },
        plugins: {
          legend: { display: false },
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
      plugins: [annotationPlugin],
    });
  }

})();
