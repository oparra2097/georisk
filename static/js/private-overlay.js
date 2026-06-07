/* Private overlay analysis.
 *
 * Compares a CSV the user uploads against the public facilities + the
 * public securitization deals.  100% client-side: the user's file is
 * read via FileReader, parsed and matched in-browser, and discarded
 * on page reload.  This script makes NO outbound request that
 * includes user data — only the two GETs below that fetch our
 * public reference data:
 *     GET /api/data-centers/facilities
 *     GET /api/securitizations/deals
 *
 * Auditable.  Search this file for 'fetch(' — you will find exactly
 * two calls, both to the GET endpoints above.
 */

(function () {
  'use strict';

  // ── Tiny RFC-4180-ish CSV parser ─────────────────────────────────
  function parseCSV(text) {
    const rows = [];
    let row = [], field = '', inQuotes = false;
    for (let i = 0; i < text.length; i++) {
      const c = text[i], n = text[i + 1];
      if (inQuotes) {
        if (c === '"' && n === '"') { field += '"'; i++; }
        else if (c === '"') inQuotes = false;
        else field += c;
      } else {
        if (c === '"') inQuotes = true;
        else if (c === ',') { row.push(field); field = ''; }
        else if (c === '\r') { /* ignore */ }
        else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
        else field += c;
      }
    }
    if (field.length || row.length) { row.push(field); rows.push(row); }
    if (!rows.length) return { headers: [], rows: [] };
    const headers = rows.shift().map(h => h.trim());
    const out = rows
      .filter(r => r.some(cell => cell && cell.trim()))
      .map(r => {
        const o = {};
        headers.forEach((h, i) => { o[h] = (r[i] || '').trim(); });
        return o;
      });
    return { headers, rows: out };
  }

  // ── Column auto-detect ───────────────────────────────────────────
  const COLUMN_ALIASES = {
    name:     ['name', 'facility', 'facility_name', 'building', 'dc_name', 'asset', 'site', 'property'],
    city:     ['city', 'town', 'municipality'],
    state:    ['state', 'region', 'province', 'state_abbr'],
    mw:       ['mw', 'capacity_mw', 'power_mw', 'critical_load', 'load_mw', 'it_load', 'mw_critical'],
    operator: ['operator', 'sponsor', 'manager', 'owner', 'asset_manager'],
    tenant:   ['tenant', 'lessee', 'customer', 'anchor_tenant'],
    status:   ['status', 'operational_status', 'stage', 'phase', 'state_status'],
  };
  function autoDetectColumns(headers) {
    const lower = headers.map(h => h.toLowerCase().replace(/[\s_\-]/g, '_'));
    const map = {};
    Object.entries(COLUMN_ALIASES).forEach(([key, aliases]) => {
      for (const alias of aliases) {
        const idx = lower.indexOf(alias);
        if (idx >= 0) { map[key] = headers[idx]; break; }
      }
    });
    return map;
  }

  // ── Token-set similarity ─────────────────────────────────────────
  function tokenize(s) {
    return (s || '').toLowerCase()
      .replace(/[^a-z0-9 ]+/g, ' ')
      .split(/\s+/)
      .filter(t => t && t.length >= 2);
  }
  function jaccard(a, b) {
    if (!a.length || !b.length) return 0;
    const sa = new Set(a), sb = new Set(b);
    let inter = 0;
    sa.forEach(x => { if (sb.has(x)) inter++; });
    return inter / (sa.size + sb.size - inter);
  }
  function nameMatchScore(uploadedName, facilityName) {
    const a = tokenize(uploadedName), b = tokenize(facilityName);
    if (!a.length || !b.length) return 0;
    const j = jaccard(a, b);
    // Penalty for very different lengths
    const lenRatio = Math.min(a.length, b.length) / Math.max(a.length, b.length);
    return j * (0.7 + 0.3 * lenRatio);
  }

  function parseMW(v) {
    if (v == null) return null;
    const s = String(v).replace(/,/g, '').replace(/[^\d.]/g, '');
    const n = parseFloat(s);
    return isFinite(n) ? n : null;
  }

  // ── State ────────────────────────────────────────────────────────
  let UPLOADED = null;        // { headers, rows, columnMap }
  let FACILITIES = null;       // from /api/data-centers/facilities
  let DEALS = null;            // from /api/securitizations/deals

  const $ = (id) => document.getElementById(id);
  function setStatus(msg) { const el = $('overlay-status'); if (el) el.textContent = msg || ''; }

  // ── File reader ──────────────────────────────────────────────────
  function readUploadedFile(file) {
    return new Promise((resolve, reject) => {
      const r = new FileReader();
      r.onload = () => resolve(r.result);
      r.onerror = () => reject(r.error);
      r.readAsText(file);
    });
  }

  async function loadReferenceData() {
    if (FACILITIES && DEALS) return;
    setStatus('loading public reference data…');
    const [fRes, dRes] = await Promise.all([
      fetch('/api/data-centers/facilities'),
      fetch('/api/securitizations/deals'),
    ]);
    const fJ = await fRes.json();
    const dJ = await dRes.json();
    FACILITIES = fJ.facilities || fJ.rows || [];
    DEALS = dJ.deals || [];
    setStatus(`reference loaded: ${FACILITIES.length} facilities · ${DEALS.length} securitized deals`);
  }

  // ── Matching engine ──────────────────────────────────────────────
  function matchRow(row, col) {
    const upName  = row[col.name] || '';
    const upCity  = (row[col.city] || '').toLowerCase();
    const upState = (row[col.state] || '').toLowerCase();
    const upMw    = parseMW(row[col.mw]);
    const upOp    = (row[col.operator] || '').toLowerCase();

    const ranked = [];
    for (const f of FACILITIES) {
      let score = 0;
      const nScore = nameMatchScore(upName, f.name);
      if (nScore > 0) score += nScore * 0.6;

      const fCity  = (f.city  || '').toLowerCase();
      const fState = (f.state || '').toLowerCase();
      const fMkt   = (f.market || '').toLowerCase();
      if (upCity && (fCity === upCity || fMkt.includes(upCity))) score += 0.15;
      if (upState && fState === upState) score += 0.1;

      if (upMw && f.mw) {
        const ratio = Math.min(upMw, f.mw) / Math.max(upMw, f.mw);
        if (ratio > 0.85) score += 0.1 * ratio;
      }

      if (upOp) {
        const ops = ((f.operator || '') + ' ' + (f.tenant_norm || '')).toLowerCase();
        if (ops.includes(upOp.split(' ')[0])) score += 0.05;
      }

      if (score > 0.15) ranked.push({ facility: f, score });
    }
    ranked.sort((a, b) => b.score - a.score);
    return ranked.slice(0, 3);
  }

  function matchSecuritizationOperator(uploadedOperator) {
    if (!uploadedOperator) return [];
    const tokens = tokenize(uploadedOperator);
    if (!tokens.length) return [];
    const hits = [];
    for (const d of DEALS) {
      const blob = (d.sponsor + ' ' + (d.collateral_facilities || []).join(' ')).toLowerCase();
      if (tokens.some(t => blob.includes(t))) hits.push(d);
    }
    return hits;
  }

  // ── Analysis ─────────────────────────────────────────────────────
  async function runAnalysis() {
    if (!UPLOADED) { setStatus('upload a CSV first'); return; }
    await loadReferenceData();
    setStatus('matching…');

    const col = UPLOADED.columnMap;
    const matched = [], unmatched = [];
    let totalUpMw = 0, matchedMw = 0;

    for (const row of UPLOADED.rows) {
      const mw = parseMW(row[col.mw]);
      if (mw != null) totalUpMw += mw;
      const ranked = matchRow(row, col);
      const sec = matchSecuritizationOperator(row[col.operator] || row[col.tenant]);
      if (ranked.length && ranked[0].score >= 0.40) {
        matched.push({ row, top: ranked[0], rest: ranked.slice(1), sec });
        if (mw != null) matchedMw += mw;
      } else {
        unmatched.push({ row, candidates: ranked, sec });
      }
    }

    // Group unmatched by state for gap analysis.
    const gapByState = new Map();
    for (const u of unmatched) {
      const st = (u.row[col.state] || '?').toUpperCase();
      const mw = parseMW(u.row[col.mw]) || 0;
      const b = gapByState.get(st) || { state: st, count: 0, mw: 0 };
      b.count++; b.mw += mw;
      gapByState.set(st, b);
    }

    renderResults({
      total:           UPLOADED.rows.length,
      matched:         matched.length,
      unmatched:       unmatched.length,
      totalUpMw,
      matchedMw,
      gapMw:           totalUpMw - matchedMw,
      matchedRows:     matched,
      unmatchedRows:   unmatched,
      gapByState:      [...gapByState.values()].sort((a, b) => b.mw - a.mw),
      col,
    });
    setStatus(`done: ${matched.length}/${UPLOADED.rows.length} matched (${((matched.length / UPLOADED.rows.length) * 100).toFixed(0)}%)`);
  }

  // ── Renderer ─────────────────────────────────────────────────────
  function fmt(n) { return n == null ? '—' : Math.round(n).toLocaleString(); }
  function riskColor(s) {
    if (s == null) return '#d1d5db';
    if (s >= 75) return '#dc2626';
    if (s >= 50) return '#f59e0b';
    if (s >= 25) return '#facc15';
    return '#10b981';
  }
  function renderResults(r) {
    const box = $('overlay-results');
    const pct = r.total ? Math.round(100 * r.matched / r.total) : 0;
    const mwPct = r.totalUpMw ? Math.round(100 * r.matchedMw / r.totalUpMw) : 0;

    const kpiRow = `
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:14px;">
        <div style="background:#fff;border:1px solid #bfdbfe;padding:10px;border-radius:5px;">
          <div style="font-size:20px;font-weight:600;color:#1e40af;">${r.matched}/${r.total}</div>
          <div style="font-size:10px;color:#6b21a8;text-transform:uppercase;letter-spacing:0.06em;">Rows matched (${pct}%)</div>
        </div>
        <div style="background:#fff;border:1px solid #bfdbfe;padding:10px;border-radius:5px;">
          <div style="font-size:20px;font-weight:600;color:#1e40af;">${fmt(r.matchedMw)}</div>
          <div style="font-size:10px;color:#6b21a8;text-transform:uppercase;letter-spacing:0.06em;">MW matched (${mwPct}%)</div>
        </div>
        <div style="background:#fff;border:1px solid #fca5a5;padding:10px;border-radius:5px;">
          <div style="font-size:20px;font-weight:600;color:#b91c1c;">${fmt(r.gapMw)}</div>
          <div style="font-size:10px;color:#b91c1c;text-transform:uppercase;letter-spacing:0.06em;">MW gap (not in map)</div>
        </div>
        <div style="background:#fff;border:1px solid #bfdbfe;padding:10px;border-radius:5px;">
          <div style="font-size:20px;font-weight:600;color:#1e40af;">${r.unmatched}</div>
          <div style="font-size:10px;color:#6b21a8;text-transform:uppercase;letter-spacing:0.06em;">Unmatched rows</div>
        </div>
      </div>`;

    const gapTable = r.gapByState.length ? `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;font-weight:600;color:#1e40af;text-transform:uppercase;letter-spacing:0.04em;margin-bottom:6px;">
          Coverage gap by state
        </div>
        <table style="width:100%;font-size:11px;border-collapse:collapse;">
          <thead><tr style="color:#6b7280;font-size:9px;text-transform:uppercase;letter-spacing:0.04em;border-bottom:1px solid #e5e7eb;">
            <th style="text-align:left;padding:4px;font-weight:500;">State</th>
            <th style="text-align:right;padding:4px;font-weight:500;">Unmatched #</th>
            <th style="text-align:right;padding:4px;font-weight:500;">Unmatched MW</th>
          </tr></thead>
          <tbody>
            ${r.gapByState.slice(0, 8).map(g => `<tr style="border-bottom:1px solid #f3f4f6;">
              <td style="padding:4px;">${g.state}</td>
              <td style="padding:4px;text-align:right;">${g.count}</td>
              <td style="padding:4px;text-align:right;color:#b91c1c;font-weight:600;">${fmt(g.mw)}</td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>` : '';

    const col = r.col;
    const matchedRows = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;font-weight:600;color:#1e40af;text-transform:uppercase;letter-spacing:0.04em;margin-bottom:6px;">
          Matched rows with risk overlay (${Math.min(20, r.matchedRows.length)} of ${r.matchedRows.length} shown)
        </div>
        <div style="overflow-x:auto;">
        <table style="width:100%;min-width:780px;font-size:11px;border-collapse:collapse;">
          <thead><tr style="color:#6b7280;font-size:9px;text-transform:uppercase;letter-spacing:0.04em;border-bottom:1px solid #e5e7eb;">
            <th style="text-align:left;padding:4px;font-weight:500;">Uploaded name</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Matched facility</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Market</th>
            <th style="text-align:right;padding:4px;font-weight:500;">MW</th>
            <th style="text-align:right;padding:4px;font-weight:500;">Risk</th>
            <th style="text-align:right;padding:4px;font-weight:500;">At-risk MW</th>
            <th style="text-align:right;padding:4px;font-weight:500;">Score</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Securitized?</th>
          </tr></thead>
          <tbody>
            ${r.matchedRows.slice(0, 20).map(m => {
              const f = m.top.facility, sr = f.stranded_risk;
              return `<tr style="border-bottom:1px solid #f3f4f6;">
                <td style="padding:4px;">${m.row[col.name] || '—'}</td>
                <td style="padding:4px;color:#1e40af;">${f.name}</td>
                <td style="padding:4px;color:#6b7280;">${f.market || '—'}</td>
                <td style="padding:4px;text-align:right;">${fmt(f.mw)}</td>
                <td style="padding:4px;text-align:right;">
                  ${sr != null ? `<span style="display:inline-block;padding:1px 5px;border-radius:3px;background:${riskColor(sr)}22;color:${riskColor(sr)};font-weight:600;">${Math.round(sr)}</span>` : '—'}
                </td>
                <td style="padding:4px;text-align:right;">${fmt(f.at_risk_mw)}</td>
                <td style="padding:4px;text-align:right;color:#6b7280;">${(m.top.score * 100).toFixed(0)}%</td>
                <td style="padding:4px;font-size:10px;color:#7c3aed;">${(m.sec || []).slice(0, 2).map(d => d.deal_name).join('; ') || '—'}</td>
              </tr>`;
            }).join('')}
          </tbody>
        </table>
        </div>
      </div>`;

    const unmatchedRows = r.unmatchedRows.length ? `
      <div>
        <div style="font-size:11px;font-weight:600;color:#b91c1c;text-transform:uppercase;letter-spacing:0.04em;margin-bottom:6px;">
          Unmatched rows (${Math.min(20, r.unmatchedRows.length)} of ${r.unmatchedRows.length} shown) — your map gap
        </div>
        <div style="overflow-x:auto;">
        <table style="width:100%;min-width:780px;font-size:11px;border-collapse:collapse;">
          <thead><tr style="color:#6b7280;font-size:9px;text-transform:uppercase;letter-spacing:0.04em;border-bottom:1px solid #e5e7eb;">
            <th style="text-align:left;padding:4px;font-weight:500;">Uploaded name</th>
            <th style="text-align:left;padding:4px;font-weight:500;">City</th>
            <th style="text-align:left;padding:4px;font-weight:500;">State</th>
            <th style="text-align:right;padding:4px;font-weight:500;">MW</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Operator</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Best near-match (score)</th>
          </tr></thead>
          <tbody>
            ${r.unmatchedRows.slice(0, 20).map(u => {
              const c = u.candidates[0];
              return `<tr style="border-bottom:1px solid #f3f4f6;">
                <td style="padding:4px;">${u.row[col.name] || '—'}</td>
                <td style="padding:4px;color:#6b7280;">${u.row[col.city] || '—'}</td>
                <td style="padding:4px;color:#6b7280;">${u.row[col.state] || '—'}</td>
                <td style="padding:4px;text-align:right;">${fmt(parseMW(u.row[col.mw]))}</td>
                <td style="padding:4px;color:#6b7280;">${u.row[col.operator] || u.row[col.tenant] || '—'}</td>
                <td style="padding:4px;font-size:10px;color:#9ca3af;">${c ? `${c.facility.name} (${(c.score*100).toFixed(0)}%)` : '—'}</td>
              </tr>`;
            }).join('')}
          </tbody>
        </table>
        </div>
      </div>` : '';

    box.innerHTML = kpiRow + gapTable + matchedRows + unmatchedRows;
    box.style.display = '';
  }

  // ── Wire UI ──────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', () => {
    const fileInput = $('overlay-file');
    const runBtn    = $('overlay-run');
    const clearBtn  = $('overlay-clear');
    if (!fileInput) return;

    fileInput.addEventListener('change', async () => {
      const f = fileInput.files?.[0];
      if (!f) return;
      setStatus(`parsing ${f.name} (${(f.size / 1024).toFixed(1)} KB) locally…`);
      try {
        const text = await readUploadedFile(f);
        const parsed = parseCSV(text);
        if (!parsed.rows.length) { setStatus('no data rows found'); return; }
        const col = autoDetectColumns(parsed.headers);
        if (!col.name) {
          setStatus(`could not find a name column. headers: ${parsed.headers.slice(0, 6).join(', ')}…`);
          return;
        }
        UPLOADED = { ...parsed, columnMap: col };
        const recognized = Object.entries(col).map(([k, v]) => `${k}→${v}`).join(', ');
        setStatus(`loaded ${parsed.rows.length} rows locally. columns: ${recognized}`);
        runBtn.disabled = false;
      } catch (e) {
        setStatus(`parse error: ${e.message}`);
      }
    });

    runBtn.addEventListener('click', async () => {
      runBtn.disabled = true;
      try { await runAnalysis(); }
      catch (e) { setStatus(`analysis error: ${e.message}`); }
      finally { runBtn.disabled = false; }
    });

    clearBtn.addEventListener('click', () => {
      UPLOADED = null;
      fileInput.value = '';
      runBtn.disabled = true;
      const box = $('overlay-results');
      if (box) { box.innerHTML = ''; box.style.display = 'none'; }
      setStatus('cleared from memory');
    });
  });
})();
