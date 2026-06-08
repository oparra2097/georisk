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
  // Auto-detects delimiter (comma / semicolon / tab) and strips
  // UTF-8 BOM. Returns ALL rows; header-row detection happens later.
  function parseCSV(text) {
    if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);  // strip BOM
    const delim = pickDelimiter(text);
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
        else if (c === delim) { row.push(field); field = ''; }
        else if (c === '\r') { /* ignore */ }
        else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
        else field += c;
      }
    }
    if (field.length || row.length) { row.push(field); rows.push(row); }
    return { rows, delim };
  }
  function pickDelimiter(text) {
    // Sample the first ~2 KB and count delimiter occurrences.
    const sample = text.slice(0, 2048);
    const counts = { ',': 0, ';': 0, '\t': 0 };
    let inQuotes = false;
    for (const c of sample) {
      if (c === '"') inQuotes = !inQuotes;
      else if (!inQuotes && counts[c] != null) counts[c]++;
    }
    let best = ',', bestN = counts[',']; // default to comma
    for (const [d, n] of Object.entries(counts)) if (n > bestN) { best = d; bestN = n; }
    return best;
  }

  // Pick the header row by scoring each candidate against the alias dictionary.
  // Real headers usually match at least 2-3 known column types; title rows
  // and blank rows score 0.
  function findHeaderRow(rows) {
    let bestIdx = 0, bestScore = 0;
    const maxLook = Math.min(rows.length, 20);
    for (let i = 0; i < maxLook; i++) {
      const r = rows[i];
      if (!r || r.length < 2) continue;
      const detected = autoDetectColumns(r.map(c => (c || '').toString()));
      const score = Object.keys(detected).length;
      if (score > bestScore) { bestScore = score; bestIdx = i; }
      // 4+ matches is "definitely the header row"; stop looking.
      if (score >= 4) break;
    }
    return { headerIdx: bestIdx, headerScore: bestScore };
  }

  function tabulate(parsed) {
    if (!parsed.rows.length) return { headers: [], rows: [], delim: parsed.delim, headerScore: 0 };
    const { headerIdx, headerScore } = findHeaderRow(parsed.rows);
    const headers = (parsed.rows[headerIdx] || []).map(h => (h || '').toString().trim());
    const out = parsed.rows.slice(headerIdx + 1)
      .filter(r => r.some(cell => cell && cell.toString().trim()))
      .map(r => {
        const o = {};
        headers.forEach((h, i) => { o[h] = (r[i] || '').toString().trim(); });
        return o;
      });
    return { headers, rows: out, delim: parsed.delim, headerIdx, headerScore };
  }

  // ── Column auto-detect ───────────────────────────────────────────
  // Each value is a list of column-name aliases (header-normalized:
  // lowercased, spaces/dashes/dots → underscores).  Exact match on
  // normalized headers.  Order MATTERS for ambiguous cases — first
  // hit wins.  In particular, "dc_location" is checked before "location"
  // so the building's location takes precedence over the issuer-LLC's
  // HQ address (AIG sheets often include both).
  const COLUMN_ALIASES = {
    name:        ['name', 'facility', 'facility_name', 'building', 'dc_name', 'asset',
                  'asset_name', 'site', 'site_name', 'property', 'property_name',
                  'issuer_name', 'issuer', 'holding', 'holding_name',
                  'security_name', 'security_description', 'description',
                  'instrument', 'investment', 'investment_name', 'position_name',
                  'company', 'company_name'],
    city:        ['city', 'town', 'municipality'],
    state:       ['state', 'region', 'province', 'state_abbr'],
    location:    ['dc_location', 'site_location', 'building_location',
                  'location', 'address', 'full_address'],
    mw:          ['mw', 'capacity_mw', 'power_mw', 'critical_load', 'load_mw',
                  'it_load', 'mw_critical', 'critical_it_load_mw'],
    operator:    ['operator', 'sponsor', 'manager', 'owner', 'asset_manager',
                  'portfolio_manager', 'asset_manager_name', 'investment_manager'],
    tenant:      ['tenant', 'lessee', 'customer', 'anchor_tenant'],
    status:      ['status', 'operational_status', 'stage', 'phase'],
    rating:      ['rating', 'effective_rating', 'sr_rating', 'senior_rating',
                  'security_rating', 'cra_rating', 'kbra_rating', 'sp_rating',
                  'moodys_rating', 'fitch_rating'],
    dc_type:     ['dc_type', 'datacenter_type', 'facility_type', 'asset_type'],
    security_id: ['security_g', 'security_g_name', 'security_id', 'cusip', 'isin',
                  'security', 'identifier'],
    // Dollar exposure fields (AIG: Sum of Total AIG Credit Exp is the
    // headline; market_value / book_value are also useful)
    exposure:    ['sum_of_total_aig_credit_exp', 'sum_of_total_credit_exp',
                  'credit_exposure', 'exposure', 'total_exposure',
                  'aig_credit_exposure', 'credit_exp', 'notional', 'par',
                  'principal', 'par_amount'],
    market_value: ['market_value', 'mark_to_market', 'mtm', 'fair_value'],
    book_value:  ['book_value', 'cost_basis', 'amortized_cost', 'cost'],
    new_holding: ['new_holding', 'new_position', 'recent_add', 'flag_new'],
  };
  function _normHeader(h) {
    return (h || '')
      .toLowerCase()
      .replace(/[.\s\-\/]+/g, '_')
      .replace(/[^a-z0-9_]/g, '')
      .replace(/^_+|_+$/g, '');
  }
  function autoDetectColumns(headers) {
    const normalized = headers.map(_normHeader);
    const map = {};
    Object.entries(COLUMN_ALIASES).forEach(([key, aliases]) => {
      for (const alias of aliases) {
        const idx = normalized.indexOf(alias);
        if (idx >= 0) { map[key] = headers[idx]; break; }
      }
    });
    // Fallbacks for the critical name field — catch unrecognized aliases
    // by suffix / contained-keyword match.  Order matters; more specific
    // suffixes preferred.
    if (!map.name) {
      const prefer = ['_name', 'name'];
      for (const suffix of prefer) {
        for (let i = 0; i < normalized.length; i++) {
          if (normalized[i].endsWith(suffix) && !Object.values(map).includes(headers[i])) {
            map.name = headers[i]; break;
          }
        }
        if (map.name) break;
      }
    }
    // Same fallback for state — any column ending in "_state" if Texas etc.
    if (!map.state) {
      for (let i = 0; i < normalized.length; i++) {
        if ((normalized[i].endsWith('_state') || normalized[i] === 'st') &&
            !Object.values(map).includes(headers[i])) {
          map.state = headers[i]; break;
        }
      }
    }
    return map;
  }

  // ── Location parser ──────────────────────────────────────────────
  // Splits combined "City, State" / "City, ST" / "City, State, Country"
  // fields. Two-letter all-caps → US state abbr.  Else first token
  // after city = state.
  const US_STATES = new Set([
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN',
    'IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV',
    'NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN',
    'TX','UT','VT','VA','WA','WV','WI','WY','DC',
  ]);
  function parseLocation(raw) {
    if (!raw) return { city: '', state: '' };
    const parts = String(raw).split(/[,;]/).map(s => s.trim()).filter(Boolean);
    if (!parts.length) return { city: '', state: '' };
    if (parts.length === 1) {
      // Either just "Ashburn" or "Ashburn VA"
      const m = parts[0].match(/^(.+?)\s+([A-Z]{2})$/);
      if (m && US_STATES.has(m[2])) return { city: m[1].trim(), state: m[2] };
      return { city: parts[0], state: '' };
    }
    const city = parts[0];
    let state = parts[1];
    const upper = state.toUpperCase();
    if (US_STATES.has(upper)) state = upper;
    return { city, state };
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
  function rowCityState(row, col) {
    let city  = (row[col.city]  || '').toLowerCase();
    let state = (row[col.state] || '').toLowerCase();
    if ((!city || !state) && col.location) {
      const p = parseLocation(row[col.location]);
      if (!city) city = p.city.toLowerCase();
      if (!state) state = p.state.toLowerCase();
    }
    return { city, state };
  }
  function rowExposureUsd(row, col) {
    for (const k of ['exposure', 'market_value', 'book_value']) {
      if (col[k]) {
        const n = parseMW(row[col[k]]);
        if (n != null) return n;
      }
    }
    return null;
  }

  // CUSIP detector — 9-char alphanumeric anywhere in the row's string fields.
  // Returns the first that matches a known deal, or null.
  const _CUSIP_RE = /\b([0-9A-Z]{8}[0-9A-Z])\b/i;
  function extractCusip(row, col) {
    const candidates = [];
    for (const k of ['security_id', 'name']) {
      if (col[k] && row[col[k]]) candidates.push(row[col[k]]);
    }
    for (const c of candidates) {
      const m = String(c).toUpperCase().match(_CUSIP_RE);
      if (m) return m[1];
    }
    return null;
  }

  // ── Operator / REIT corporate-bond matcher ──────────────────────
  // AIG-style portfolios often hold corporate bonds of DC operators
  // (Digital Realty Trust, Equinix, Iron Mountain).  These aren't ABS
  // — they're general unsecured REIT debt backed by the issuer's
  // whole portfolio.  When the row name matches one of these, we
  // route through the operator's facility footprint instead of a
  // single deal.
  const OPERATORS = [
    // Public DC REITs (corporate bond issuers in AIG portfolios)
    { canonical: 'Digital Realty Trust', type: 'reit_public',
      patterns: ['digital realty'], facility_match: 'digital realty' },
    { canonical: 'Equinix Inc',          type: 'reit_public',
      patterns: ['equinix'],            facility_match: 'equinix' },
    { canonical: 'Iron Mountain Inc',    type: 'reit_public',
      patterns: ['iron mountain'],      facility_match: 'iron mountain' },
    // Private operators — usually surface via ABS issuer, but if AIG
    // holds their parent-level paper, catch it here.
    { canonical: 'QTS Realty Trust',     type: 'private_operator',
      patterns: ['qts realty', 'qts data centers', 'qts inc'],
      facility_match: 'qts' },
    { canonical: 'CoreSite Realty',      type: 'reit_private',
      patterns: ['coresite'],           facility_match: 'coresite' },
    { canonical: 'CyrusOne',             type: 'private_operator',
      patterns: ['cyrusone'],           facility_match: 'cyrusone' },
    { canonical: 'EdgeConneX',           type: 'private_operator',
      patterns: ['edgeconnex', 'edgecore'], facility_match: 'edge' },
    { canonical: 'T5 Data Centers',      type: 'private_operator',
      patterns: ['t5 data centers', 't5 facilities'],
      facility_match: 't5' },
    { canonical: 'Sabey Data Centers',   type: 'private_operator',
      patterns: ['sabey data centers', 'sabey corp'],
      facility_match: 'sabey' },
    { canonical: 'Stream Data Centers',  type: 'private_operator',
      patterns: ['stream data centers', 'stream dc'],
      facility_match: 'stream' },
  ];

  function matchOperator(row, col) {
    if (!FACILITIES) return null;
    const blob = (((row[col.name] || '') + ' ' + (row[col.tenant] || '') + ' ' +
                    (row[col.operator] || '')).toLowerCase());
    for (const op of OPERATORS) {
      for (const p of op.patterns) {
        if (blob.includes(p)) {
          // Find our facilities operated by this operator (by operator
          // field or by name substring on the facility name).
          const fmKey = op.facility_match;
          const facs = FACILITIES.filter(f => {
            const opField = ((f.operator || f.developer || f.tenant_norm || '')).toLowerCase();
            const nameField = (f.name || '').toLowerCase();
            return opField.includes(fmKey) || nameField.includes(fmKey);
          });
          // Aggregate the operator's MW + risk across our map.
          const totalMw = facs.reduce((s, f) => s + (f.mw || 0), 0);
          const totalAtRisk = facs.reduce((s, f) => s + (f.at_risk_mw || 0), 0);
          const scored = facs.filter(f => f.stranded_risk != null && (f.mw || 0) > 0);
          const num = scored.reduce((s, f) => s + (f.stranded_risk || 0) * (f.mw || 0), 0);
          const den = scored.reduce((s, f) => s + (f.mw || 0), 0);
          const weightedRisk = den > 0 ? num / den : null;
          return {
            operator:        op,
            facilities:      facs,
            facility_count:  facs.length,
            total_mw:        totalMw,
            at_risk_mw:      totalAtRisk,
            stranded_risk:   weightedRisk,
            matchKind:       'operator',
            score:           0.85,
          };
        }
      }
    }
    return null;
  }

  // Try to match against a securitization DEAL.  AIG-style portfolios
  // hold bonds, not buildings — the Name field describes a security
  // ("VANTAGE DC ISSUER 2021-1A AA RT"), so token-set similarity vs
  // facility name will be ~0.  Match against deal_name + sponsor instead;
  // an exact CUSIP match wins over a fuzzy name match.
  function matchDeal(row, col) {
    if (!DEALS || !DEALS.length) return null;
    const upName = (row[col.name] || '').toString();
    const upTenant = (row[col.tenant] || '').toString();
    const upBlob = (upName + ' ' + upTenant).toLowerCase();
    const upTokens = tokenize(upBlob);
    if (!upTokens.length) return null;

    // Try exact CUSIP match first.
    const cusip = extractCusip(row, col);
    if (cusip) {
      const hit = DEALS.find(d => (d.cusip_senior || '').toUpperCase() === cusip);
      if (hit) return { deal: hit, score: 1.0, matchKind: 'cusip' };
    }

    // Fuzzy: token-set similarity against (deal_name + sponsor) blob.
    let best = null;
    for (const d of DEALS) {
      const dBlob = (d.deal_name + ' ' + d.sponsor + ' ' + (d.collateral_facilities || []).join(' ')).toLowerCase();
      const dTokens = tokenize(dBlob);
      if (!dTokens.length) continue;
      const sim = jaccard(upTokens, dTokens);
      // Bonus for sponsor token overlap specifically (high-signal).
      const sponsorTokens = tokenize(d.sponsor);
      const sponsorHit = sponsorTokens.some(t => upTokens.includes(t)) ? 0.2 : 0;
      const score = sim + sponsorHit;
      if (!best || score > best.score) best = { deal: d, score, matchKind: 'deal' };
    }
    if (best && best.score >= 0.25) return best;
    return null;
  }

  function matchRow(row, col) {
    const upName  = row[col.name] || '';
    const ls = rowCityState(row, col);
    const upCity = ls.city, upState = ls.state;
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
    const hasExposure = !!(col.exposure || col.market_value || col.book_value);
    const matched = [], unmatched = [];
    let totalUpMw = 0,  matchedMw  = 0;
    let totalUpUsd = 0, matchedUsd = 0;

    // AIG-style holdings are SECURITIES, not buildings.  Strategy:
    //   1. Try matching against a securitization deal (by CUSIP or
    //      fuzzy deal_name+sponsor token similarity).
    //   2. If that hits, the deal carries us through to its collateral
    //      facility list with the risk overlay already pre-computed.
    //   3. If no deal match, fall back to the original facility-name
    //      token match (covers cases where the user uploaded an
    //      operator inventory rather than a bond portfolio).
    for (const row of UPLOADED.rows) {
      const mw  = parseMW(row[col.mw]);
      const usd = rowExposureUsd(row, col);
      if (mw  != null) totalUpMw  += mw;
      if (usd != null) totalUpUsd += usd;

      let dealHit = matchDeal(row, col);
      let opHit = !dealHit ? matchOperator(row, col) : null;
      let facRanked = [];

      if (dealHit) {
        matched.push({
          row,
          matchKind: dealHit.matchKind,
          dealMatch: dealHit.deal,
          dealScore: dealHit.score,
          sec:       [dealHit.deal],
        });
        if (mw  != null) matchedMw  += mw;
        if (usd != null) matchedUsd += usd;
      } else if (opHit) {
        matched.push({
          row,
          matchKind:    'operator',
          operatorHit:  opHit,
          sec:          [],
        });
        if (mw  != null) matchedMw  += mw;
        if (usd != null) matchedUsd += usd;
      } else {
        facRanked = matchRow(row, col);
        const sec = matchSecuritizationOperator(row[col.operator] || row[col.tenant]);
        if (facRanked.length && facRanked[0].score >= 0.30) {
          matched.push({
            row,
            matchKind: 'facility',
            top:       facRanked[0],
            rest:      facRanked.slice(1),
            sec,
          });
          if (mw  != null) matchedMw  += mw;
          if (usd != null) matchedUsd += usd;
        } else {
          unmatched.push({ row, candidates: facRanked, sec });
        }
      }
    }

    // Group unmatched by state for gap analysis — uses combined Location field
    // when explicit state column is missing.
    const gapByState = new Map();
    for (const u of unmatched) {
      const ls = rowCityState(u.row, col);
      const st = (ls.state || '?').toUpperCase();
      const mw  = parseMW(u.row[col.mw]) || 0;
      const usd = rowExposureUsd(u.row, col) || 0;
      const b = gapByState.get(st) || { state: st, count: 0, mw: 0, usd: 0 };
      b.count++; b.mw += mw; b.usd += usd;
      gapByState.set(st, b);
    }

    // Group everything by portfolio_manager (BlackRock, etc) if present —
    // this is how AIG breaks down exposure internally.
    const byManager = new Map();
    if (col.operator) {
      for (const m of matched.concat(unmatched.map(u => ({ row: u.row, _unmatched: true })))) {
        const mgr = (m.row[col.operator] || 'unattributed').trim() || 'unattributed';
        const mw  = parseMW(m.row[col.mw]) || 0;
        const usd = rowExposureUsd(m.row, col) || 0;
        const b = byManager.get(mgr) || { manager: mgr, count: 0, mw: 0, usd: 0, unmatched: 0 };
        b.count++; b.mw += mw; b.usd += usd;
        if (m._unmatched) b.unmatched++;
        byManager.set(mgr, b);
      }
    }

    renderResults({
      total:           UPLOADED.rows.length,
      matched:         matched.length,
      unmatched:       unmatched.length,
      totalUpMw, matchedMw, gapMw: totalUpMw - matchedMw,
      totalUpUsd, matchedUsd, gapUsd: totalUpUsd - matchedUsd,
      hasExposure,
      matchedRows:     matched,
      unmatchedRows:   unmatched,
      gapByState:      [...gapByState.values()].sort((a, b) => (b.usd || b.mw) - (a.usd || a.mw)),
      byManager:       [...byManager.values()].sort((a, b) => (b.usd || b.mw) - (a.usd || a.mw)),
      col,
    });
    setStatus(`done: ${matched.length}/${UPLOADED.rows.length} matched (${((matched.length / UPLOADED.rows.length) * 100).toFixed(0)}%)`);
  }

  // ── Map exposure overlay ─────────────────────────────────────────
  // Plots matched-row exposure as purple circles on the existing
  // data-centers map.  100% client-side: pulls coordinates from
  // FACILITIES (already fetched) and never touches the user's file
  // again.  The map layer is cleared on "Clear from memory".
  function plotExposureOnMap(matched) {
    if (!window.DataCenterMap?.svg || !window.DataCenterMap?.projection) {
      setStatus('map not ready — switch the toolbar to Facilities mode first, then click Plot again');
      return;
    }
    if (!FACILITIES) {
      setStatus('reference data not loaded');
      return;
    }
    const { svg, projection } = window.DataCenterMap;
    const col = UPLOADED.columnMap;

    // Build per-facility marker rows.
    const markers = [];
    for (const m of matched) {
      const usd = rowExposureUsd(m.row, col) || 0;
      let facs = [];
      let sourceLabel = m.row[col.name] || '';
      if (m.operatorHit) {
        facs = m.operatorHit.facilities || [];
        sourceLabel = `${m.row[col.name]} → ${m.operatorHit.operator.canonical} (operator)`;
      } else if (m.dealMatch) {
        // The deal's facility_matches are names; look them up in FACILITIES
        // for the coordinates.  Loose match because deal collateral_facilities
        // sometimes use abbreviated names.
        const fmNames = (m.dealMatch.facility_matches || []).map(fm => (fm.name || '').toLowerCase());
        if (fmNames.length) {
          facs = FACILITIES.filter(f => fmNames.includes((f.name || '').toLowerCase()));
        }
        if (!facs.length) {
          // Fall back: token-match by sponsor.
          const sponsorLow = (m.dealMatch.sponsor || '').toLowerCase();
          if (sponsorLow) {
            facs = FACILITIES.filter(f =>
              ((f.operator || f.developer || f.tenant_norm || '') + ' ' + (f.name || ''))
                .toLowerCase().includes(sponsorLow)
            );
          }
        }
        sourceLabel = `${m.row[col.name]} → ${m.dealMatch.deal_name}`;
      } else if (m.top?.facility) {
        facs = [m.top.facility];
      }

      facs = facs.filter(f => f.lat != null && f.lon != null);
      if (!facs.length) continue;
      const perFac = usd / facs.length;
      for (const f of facs) {
        markers.push({ lat: f.lat, lon: f.lon, exposure: perFac, label: sourceLabel, facility: f.name });
      }
    }
    if (!markers.length) {
      setStatus('no plottable matches — none had facility coordinates');
      return;
    }

    // Aggregate by coordinate (rounded to ~1 km).
    const agg = new Map();
    for (const mk of markers) {
      const key = `${mk.lat.toFixed(2)},${mk.lon.toFixed(2)}`;
      const e = agg.get(key) || { lat: mk.lat, lon: mk.lon, exposure: 0, items: [] };
      e.exposure += mk.exposure;
      e.items.push(`${mk.label} (${mk.facility})`);
      agg.set(key, e);
    }
    const data = [...agg.values()];
    const maxExp = Math.max(...data.map(d => d.exposure));
    const radiusScale = d => Math.max(4, Math.sqrt(d / maxExp) * 28);

    // Find or create our exposure layer (above all existing map layers).
    let layer = svg.select('g.aig-exposure-layer');
    if (layer.empty()) {
      layer = svg.append('g').attr('class', 'aig-exposure-layer');
    }
    layer.style('pointer-events', 'all');

    // Solo mode: hide the map's own markets/facilities so only the AIG
    // exposure markers are visible.  We remember the previous display
    // state so "Clear map overlay" can restore it.
    const dcm = window.DataCenterMap;
    if (!dcm._aigSoloActive) {
      dcm._priorBubbleDisplay   = dcm.bubbleLayer.style('display');
      dcm._priorFacilityDisplay = dcm.facilityLayer.style('display');
      dcm._aigSoloActive = true;
    }
    dcm.bubbleLayer.style('display', 'none');
    dcm.facilityLayer.style('display', 'none');

    const join = layer.selectAll('circle.aig-marker').data(data, d => `${d.lat},${d.lon}`);
    join.exit().remove();
    const enter = join.enter().append('circle').attr('class', 'aig-marker');
    enter.append('title');
    const merged = enter.merge(join);
    merged
      .attr('cx', d => { const p = projection([d.lon, d.lat]); return p ? p[0] : null; })
      .attr('cy', d => { const p = projection([d.lon, d.lat]); return p ? p[1] : null; })
      .attr('r',  d => radiusScale(d.exposure))
      .style('fill', '#7c3aed').style('fill-opacity', 0.45)
      .style('stroke', '#5b21b6').style('stroke-width', 1.5)
      .style('cursor', 'pointer');
    merged.select('title').text(d =>
      `$${fmtUsd(d.exposure)} portfolio exposure mapped here\n\n` +
      d.items.slice(0, 8).join('\n') +
      (d.items.length > 8 ? `\n…and ${d.items.length - 8} more` : '')
    );

    setStatus(`solo-plotted ${data.length} locations · max single exposure ≈ ${fmtUsd(maxExp)} · click "Clear map overlay" to restore the facility map`);
  }

  function clearExposureMap() {
    const dcm = window.DataCenterMap;
    if (!dcm?.svg) return;
    dcm.svg.select('g.aig-exposure-layer').remove();
    // Restore the map's own marker visibility.
    if (dcm._aigSoloActive) {
      if (dcm._priorBubbleDisplay !== undefined)
        dcm.bubbleLayer.style('display', dcm._priorBubbleDisplay);
      if (dcm._priorFacilityDisplay !== undefined)
        dcm.facilityLayer.style('display', dcm._priorFacilityDisplay);
      dcm._aigSoloActive = false;
      dcm._priorBubbleDisplay   = undefined;
      dcm._priorFacilityDisplay = undefined;
    }
  }

  // ── Stage + structuring helpers ──────────────────────────────────
  // Returns construction-stage MW breakdown ({built_mw, uc_mw,
  // planned_mw, label}) for any match type — pulls from the deal's
  // issuer-disclosed splits where populated, otherwise aggregates
  // from facility_matches[] / operator footprint / single facility.
  function stageBreakdown(m) {
    let built = 0, uc = 0, planned = 0;
    if (m.dealMatch) {
      built = m.dealMatch.collateral_mw_built || 0;
      uc    = m.dealMatch.collateral_mw_uc    || 0;
      if (!built && !uc && Array.isArray(m.dealMatch.facility_matches)) {
        for (const f of m.dealMatch.facility_matches) {
          if (f.status === 'built')                  built   += f.mw || 0;
          else if (f.status === 'under_construction') uc     += f.mw || 0;
          else if (f.status === 'planned')           planned += f.mw || 0;
        }
      }
    } else if (m.operatorHit) {
      for (const f of m.operatorHit.facilities || []) {
        if (f.status === 'built')                  built   += f.mw || 0;
        else if (f.status === 'under_construction') uc     += f.mw || 0;
        else if (f.status === 'planned')           planned += f.mw || 0;
      }
    } else if (m.top?.facility) {
      const f = m.top.facility;
      if (f.status === 'built')                  built   = f.mw || 0;
      else if (f.status === 'under_construction') uc     = f.mw || 0;
      else if (f.status === 'planned')           planned = f.mw || 0;
    }
    const total = built + uc + planned;
    let label;
    if (!total)                                   label = '<span style="color:#9ca3af;">not disclosed</span>';
    else if (built && !uc && !planned)            label = `<span style="color:#10b981;font-weight:600;">100% built</span>`;
    else if (!built && uc && !planned)            label = `<span style="color:#f59e0b;font-weight:600;">100% UC</span>`;
    else if (!built && !uc && planned)            label = `<span style="color:#6366f1;font-weight:600;">100% plan</span>`;
    else {
      const pct = v => Math.round(100 * v / total);
      const parts = [];
      if (built)   parts.push(`<span style="color:#10b981;">${pct(built)}%B</span>`);
      if (uc)      parts.push(`<span style="color:#f59e0b;">${pct(uc)}%U</span>`);
      if (planned) parts.push(`<span style="color:#6366f1;">${pct(planned)}%P</span>`);
      label = parts.join(' · ');
    }
    return { built_mw: built, uc_mw: uc, planned_mw: planned, total, label };
  }

  // Pulls deal_type / tenant_type / dc_type / final maturity year.
  function classification(m) {
    if (m.dealMatch) {
      const d = m.dealMatch;
      return {
        deal_type:       d.deal_type_label || d.deal_type || '',
        deal_type_key:   d.deal_type || '',
        tenant_type:     d.tenant_type_label || '',
        tenant_type_key: d.tenant_type || '',
        dc_type:         d.datacenter_type_label || '',
        dc_type_key:     d.datacenter_type || '',
        maturity:        (d.final_maturity || '').slice(0, 4),
      };
    }
    if (m.operatorHit) {
      const t = m.operatorHit.operator.type;
      return {
        deal_type:       t === 'reit_public' ? 'REIT corporate bond' : 'Operator-level',
        deal_type_key:   'unsecured',
        tenant_type:     '', tenant_type_key: '',
        dc_type:         '', dc_type_key: '',
        maturity:        '',
      };
    }
    return { deal_type: '', deal_type_key: '', tenant_type: '', tenant_type_key: '',
              dc_type: '', dc_type_key: '', maturity: '' };
  }

  // Returns HTML for the full bond-structuring panel shown when a
  // matched row is expanded.
  function structuringDetails(m) {
    if (m.dealMatch) {
      const d = m.dealMatch;
      const cusip   = d.cusip_senior ? `<code>${d.cusip_senior}</code>` : '<span style="color:#9ca3af;">pending</span>';
      const rating  = d.rating_senior ? `${d.rating_senior} (${d.rater || '?'})` : '—';
      const wal     = d.wal_years ? `${d.wal_years.toFixed(1)} yrs` : '—';
      const tcShare = d.top_tenant_share_pct ? ` (top tenant ${d.top_tenant_share_pct}%)` : '';
      const topT    = (d.top_tenants || []).join(', ') || '—';
      const src     = d.source_url
        ? `<a href="${d.source_url}" target="_blank" style="color:#1d4ed8;">primary source ↗</a>`
        : '—';
      const notes   = d.notes
        ? `<div style="margin-top:6px;color:#6b7280;font-size:10px;line-height:1.5;">${d.notes}</div>` : '';
      return `
        <div style="display:grid;grid-template-columns:auto 1fr;gap:3px 12px;font-size:11px;">
          <div style="color:#9ca3af;">Sponsor</div>          <div>${d.sponsor || '—'}</div>
          <div style="color:#9ca3af;">Deal type</div>        <div>${d.deal_type_label}</div>
          <div style="color:#9ca3af;">Issued → Maturity</div><div>${d.issue_date || '—'} → ${d.final_maturity || '—'}</div>
          <div style="color:#9ca3af;">Senior rating</div>    <div>${rating}</div>
          <div style="color:#9ca3af;">Senior CUSIP</div>     <div>${cusip}</div>
          <div style="color:#9ca3af;">WAL</div>              <div>${wal}</div>
          <div style="color:#9ca3af;">Tenant type</div>      <div>${d.tenant_type_label || '—'}</div>
          <div style="color:#9ca3af;">Facility type</div>    <div>${d.datacenter_type_label || '—'}</div>
          <div style="color:#9ca3af;">Top tenants</div>      <div>${topT}${tcShare}</div>
          <div style="color:#9ca3af;">Collateral DCs</div>   <div>${(d.collateral_facilities || []).join('; ') || '—'}</div>
          <div style="color:#9ca3af;">Source</div>           <div>${src}</div>
        </div>${notes}`;
    }
    if (m.operatorHit) {
      const o = m.operatorHit;
      const facList = (o.facilities || []).slice(0, 10).map(f =>
        `<li>${f.name} <span style="color:#9ca3af;">— ${f.market || ''} · ${(f.status || '?').replace('_',' ')} · ${Math.round(f.mw || 0)} MW</span></li>`
      ).join('');
      return `
        <div style="font-size:11px;">
          <div><strong>${o.operator.canonical}</strong>
            <span style="color:#6b7280;">· ${o.operator.type === 'reit_public' ? 'public REIT (corporate bond — unsecured, claims operator\'s whole portfolio)' : 'private operator'}</span></div>
          <div style="margin-top:4px;color:#6b7280;">
            ${o.facility_count} of their US facilities in our map · ${Math.round(o.total_mw)} MW total · ${Math.round(o.at_risk_mw)} at-risk MW
          </div>
          <ul style="margin:6px 0 0 18px;padding:0;font-size:10px;color:#374151;">
            ${facList}${o.facility_count > 10 ? `<li style="color:#9ca3af;">…and ${o.facility_count - 10} more</li>` : ''}
          </ul>
        </div>`;
    }
    if (m.top?.facility) {
      const f = m.top.facility;
      return `
        <div style="font-size:11px;">
          <strong>${f.name}</strong> · ${f.market || ''} · ${(f.status || '?').replace('_',' ')} · ${Math.round(f.mw || 0)} MW
          ${f.operator    ? `<div style="color:#6b7280;margin-top:4px;">Operator: ${f.operator}</div>` : ''}
          ${f.tenant_norm ? `<div style="color:#6b7280;">Tenant: ${f.tenant_norm}</div>` : ''}
        </div>`;
    }
    return '<div style="color:#9ca3af;">No structuring details available.</div>';
  }

  // ── Renderer ─────────────────────────────────────────────────────
  function fmt(n) { return n == null ? '—' : Math.round(n).toLocaleString(); }
  function fmtUsd(n) {
    if (n == null || n === 0) return '—';
    const abs = Math.abs(n);
    if (abs >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
    return `$${Math.round(n).toLocaleString()}`;
  }
  function riskColor(s) {
    if (s == null) return '#d1d5db';
    if (s >= 75) return '#dc2626';
    if (s >= 50) return '#f59e0b';
    if (s >= 25) return '#facc15';
    return '#10b981';
  }
  function renderResults(r) {
    const box = $('overlay-results');
    const pct   = r.total      ? Math.round(100 * r.matched    / r.total)      : 0;
    const mwPct = r.totalUpMw  ? Math.round(100 * r.matchedMw  / r.totalUpMw)  : 0;
    const usdPct = r.totalUpUsd ? Math.round(100 * r.matchedUsd / r.totalUpUsd) : 0;

    // KPI tiles vary by what columns were detected.  AIG sheets have $-exposure
    // but typically no MW column → we hide MW tiles and surface $ instead.
    const tiles = [
      `<div style="background:#fff;border:1px solid #bfdbfe;padding:10px;border-radius:5px;">
        <div style="font-size:20px;font-weight:600;color:#1e40af;">${r.matched}/${r.total}</div>
        <div style="font-size:10px;color:#6b21a8;text-transform:uppercase;letter-spacing:0.06em;">Rows matched (${pct}%)</div>
      </div>`,
      `<div style="background:#fff;border:1px solid #bfdbfe;padding:10px;border-radius:5px;">
        <div style="font-size:20px;font-weight:600;color:#1e40af;">${r.unmatched}</div>
        <div style="font-size:10px;color:#6b21a8;text-transform:uppercase;letter-spacing:0.06em;">Unmatched rows</div>
      </div>`,
    ];
    if (r.totalUpMw > 0) {
      tiles.push(`<div style="background:#fff;border:1px solid #bfdbfe;padding:10px;border-radius:5px;">
        <div style="font-size:20px;font-weight:600;color:#1e40af;">${fmt(r.matchedMw)}</div>
        <div style="font-size:10px;color:#6b21a8;text-transform:uppercase;letter-spacing:0.06em;">MW matched (${mwPct}%)</div>
      </div>`);
      tiles.push(`<div style="background:#fff;border:1px solid #fca5a5;padding:10px;border-radius:5px;">
        <div style="font-size:20px;font-weight:600;color:#b91c1c;">${fmt(r.gapMw)}</div>
        <div style="font-size:10px;color:#b91c1c;text-transform:uppercase;letter-spacing:0.06em;">MW gap (not in map)</div>
      </div>`);
    }
    if (r.hasExposure && r.totalUpUsd > 0) {
      tiles.push(`<div style="background:#fff;border:1px solid #c7d2fe;padding:10px;border-radius:5px;">
        <div style="font-size:20px;font-weight:600;color:#3730a3;">${fmtUsd(r.totalUpUsd)}</div>
        <div style="font-size:10px;color:#3730a3;text-transform:uppercase;letter-spacing:0.06em;">Total credit exposure</div>
      </div>`);
      tiles.push(`<div style="background:#fff;border:1px solid #c7d2fe;padding:10px;border-radius:5px;">
        <div style="font-size:20px;font-weight:600;color:#3730a3;">${fmtUsd(r.matchedUsd)}</div>
        <div style="font-size:10px;color:#3730a3;text-transform:uppercase;letter-spacing:0.06em;">Exposure on mapped DCs (${usdPct}%)</div>
      </div>`);
      tiles.push(`<div style="background:#fff;border:1px solid #fca5a5;padding:10px;border-radius:5px;">
        <div style="font-size:20px;font-weight:600;color:#b91c1c;">${fmtUsd(r.gapUsd)}</div>
        <div style="font-size:10px;color:#b91c1c;text-transform:uppercase;letter-spacing:0.06em;">Exposure on UN-mapped DCs</div>
      </div>`);
    }
    const kpiRow = `
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:14px;">
        ${tiles.join('')}
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;">
        <button type="button" id="overlay-plot-map"
                style="padding:6px 12px;border:1px solid #7c3aed;background:#faf5ff;color:#6d28d9;border-radius:4px;cursor:pointer;font-size:12px;font-weight:600;">
          📍 Plot exposure on map
        </button>
        <button type="button" id="overlay-clear-map"
                style="padding:6px 12px;border:1px solid #d1d5db;background:#fff;color:#6b7280;border-radius:4px;cursor:pointer;font-size:11px;">
          Clear map overlay
        </button>
        <span style="align-self:center;font-size:10px;color:#9ca3af;">
          markers scaled by exposure $; tooltip shows portfolio holdings at each location
        </span>
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
            ${r.totalUpMw > 0   ? '<th style="text-align:right;padding:4px;font-weight:500;">Unmatched MW</th>'        : ''}
            ${r.hasExposure     ? '<th style="text-align:right;padding:4px;font-weight:500;">Unmatched exposure</th>' : ''}
          </tr></thead>
          <tbody>
            ${r.gapByState.slice(0, 12).map(g => `<tr style="border-bottom:1px solid #f3f4f6;">
              <td style="padding:4px;">${g.state}</td>
              <td style="padding:4px;text-align:right;">${g.count}</td>
              ${r.totalUpMw > 0   ? `<td style="padding:4px;text-align:right;color:#b91c1c;font-weight:600;">${fmt(g.mw)}</td>`         : ''}
              ${r.hasExposure     ? `<td style="padding:4px;text-align:right;color:#b91c1c;font-weight:600;">${fmtUsd(g.usd)}</td>`    : ''}
            </tr>`).join('')}
          </tbody>
        </table>
      </div>` : '';

    const managerTable = (r.byManager && r.byManager.length) ? `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;font-weight:600;color:#1e40af;text-transform:uppercase;letter-spacing:0.04em;margin-bottom:6px;">
          By portfolio manager / operator
        </div>
        <table style="width:100%;font-size:11px;border-collapse:collapse;">
          <thead><tr style="color:#6b7280;font-size:9px;text-transform:uppercase;letter-spacing:0.04em;border-bottom:1px solid #e5e7eb;">
            <th style="text-align:left;padding:4px;font-weight:500;">Manager</th>
            <th style="text-align:right;padding:4px;font-weight:500;">Rows</th>
            <th style="text-align:right;padding:4px;font-weight:500;">Unmatched</th>
            ${r.totalUpMw > 0 ? '<th style="text-align:right;padding:4px;font-weight:500;">MW</th>'           : ''}
            ${r.hasExposure   ? '<th style="text-align:right;padding:4px;font-weight:500;">Exposure</th>'    : ''}
          </tr></thead>
          <tbody>
            ${r.byManager.slice(0, 15).map(m => `<tr style="border-bottom:1px solid #f3f4f6;">
              <td style="padding:4px;">${m.manager}</td>
              <td style="padding:4px;text-align:right;">${m.count}</td>
              <td style="padding:4px;text-align:right;color:${m.unmatched > 0 ? '#b91c1c' : '#10b981'};font-weight:600;">${m.unmatched}</td>
              ${r.totalUpMw > 0 ? `<td style="padding:4px;text-align:right;">${fmt(m.mw)}</td>`        : ''}
              ${r.hasExposure   ? `<td style="padding:4px;text-align:right;">${fmtUsd(m.usd)}</td>`   : ''}
            </tr>`).join('')}
          </tbody>
        </table>
      </div>` : '';

    const col = r.col;
    const matchedRows = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;font-weight:600;color:#1e40af;text-transform:uppercase;letter-spacing:0.04em;margin-bottom:6px;">
          Matched rows with risk overlay (${Math.min(20, r.matchedRows.length)} of ${r.matchedRows.length} shown)
          <span style="font-weight:400;color:#6b7280;text-transform:none;letter-spacing:0;margin-left:6px;font-size:10px;">
            · click any row to see full deal structuring
          </span>
        </div>
        <div style="overflow-x:auto;">
        <table style="width:100%;min-width:1100px;font-size:11px;border-collapse:collapse;">
          <thead><tr style="color:#6b7280;font-size:9px;text-transform:uppercase;letter-spacing:0.04em;border-bottom:1px solid #e5e7eb;">
            <th style="text-align:left;padding:4px;font-weight:500;"></th>
            <th style="text-align:left;padding:4px;font-weight:500;">Uploaded name</th>
            <th style="text-align:left;padding:4px;font-weight:500;">How</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Matched to</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Stage</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Tenant</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Facility</th>
            <th style="text-align:right;padding:4px;font-weight:500;">MW</th>
            <th style="text-align:right;padding:4px;font-weight:500;">Risk</th>
            <th style="text-align:right;padding:4px;font-weight:500;">At-risk MW</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Maturity</th>
            ${r.hasExposure ? '<th style="text-align:right;padding:4px;font-weight:500;">Your exposure</th>' : ''}
            ${col.rating    ? '<th style="text-align:left;padding:4px;font-weight:500;">Your rating</th>'    : ''}
          </tr></thead>
          <tbody>
            ${r.matchedRows.slice(0, 20).map((m, idx) => {
              const usd = rowExposureUsd(m.row, col);
              const stage = stageBreakdown(m);
              const cls   = classification(m);
              let matchedTo, dealMw, sr, atRisk, kindLabel;
              if (m.dealMatch) {
                const d = m.dealMatch;
                matchedTo = d.deal_name;
                dealMw    = (d.collateral_mw_built || 0) + (d.collateral_mw_uc || 0);
                sr        = d.stranded_risk_avg;
                atRisk    = d.at_risk_mw_total;
                kindLabel = m.matchKind === 'cusip'
                  ? '<span style="background:#d1fae5;color:#065f46;padding:1px 5px;border-radius:3px;font-size:9px;font-weight:600;">CUSIP</span>'
                  : '<span style="background:#dbeafe;color:#1d4ed8;padding:1px 5px;border-radius:3px;font-size:9px;font-weight:600;">DEAL</span>';
              } else if (m.operatorHit) {
                const o = m.operatorHit;
                matchedTo = `${o.operator.canonical} <span style="color:#9ca3af;font-size:9px;">(${o.operator.type})</span>`;
                dealMw    = o.total_mw;
                sr        = o.stranded_risk;
                atRisk    = o.at_risk_mw;
                kindLabel = '<span style="background:#ede9fe;color:#6d28d9;padding:1px 5px;border-radius:3px;font-size:9px;font-weight:600;">OP</span>';
              } else {
                const f = m.top.facility;
                matchedTo = f.name;
                dealMw    = f.mw;
                sr        = f.stranded_risk;
                atRisk    = f.at_risk_mw;
                kindLabel = '<span style="background:#fef3c7;color:#92400e;padding:1px 5px;border-radius:3px;font-size:9px;font-weight:600;">FAC</span>';
              }
              const tenantPill = cls.tenant_type
                ? `<span title="Who's the credit counterparty — hyperscaler vs colocation customer vs wholesale enterprise" style="background:#dbeafe;color:#1e40af;padding:1px 5px;border-radius:999px;font-size:9px;letter-spacing:0.04em;">${cls.tenant_type}</span>` : '—';
              // Facility-axis tooltips spell out the BTS vs wholesale vs campus distinction.
              const dcTip = {
                'hyperscale_bts':       'Hyperscale BTS: single hyperscaler is the only tenant in the deal pool',
                'hyperscale_wholesale': 'Hyperscale wholesale: multiple hyperscalers across a master-trust pool (each building may be BTS-style)',
                'hyperscale_campus':    'Hyperscale campus: multi-building site where the campus concentration is the dominant risk',
                'retail_colo':          'Retail colo: many small enterprise tenants per building, interconnection-heavy',
                'wholesale':            'Wholesale colo: multi-tenant wholesale suites',
                'ai_campus':            'AI campus: purpose-built GPU/HPC training site',
                'mixed':                'Mixed: blend of tenant/building types',
              }[cls.dc_type_key] || '';
              const facPill = cls.dc_type
                ? `<span title="${dcTip}" style="background:#fce7f3;color:#9d174d;padding:1px 5px;border-radius:3px;font-size:9px;letter-spacing:0.04em;cursor:help;">${cls.dc_type}</span>` : '—';
              return `
              <tr class="match-row" data-row-idx="${idx}" style="border-bottom:1px solid #f3f4f6;cursor:pointer;">
                <td style="padding:4px;color:#9ca3af;width:18px;">▸</td>
                <td style="padding:4px;">${m.row[col.name] || '—'}</td>
                <td style="padding:4px;">${kindLabel}</td>
                <td style="padding:4px;color:#1e40af;">${matchedTo}</td>
                <td style="padding:4px;font-size:10px;">${stage.label}</td>
                <td style="padding:4px;">${tenantPill}</td>
                <td style="padding:4px;">${facPill}</td>
                <td style="padding:4px;text-align:right;">${fmt(dealMw)}</td>
                <td style="padding:4px;text-align:right;">
                  ${sr != null ? `<span style="display:inline-block;padding:1px 5px;border-radius:3px;background:${riskColor(sr)}22;color:${riskColor(sr)};font-weight:600;">${Math.round(sr)}</span>` : '—'}
                </td>
                <td style="padding:4px;text-align:right;">${fmt(atRisk)}</td>
                <td style="padding:4px;color:#6b7280;">${cls.maturity || '—'}</td>
                ${r.hasExposure ? `<td style="padding:4px;text-align:right;color:#3730a3;font-weight:600;">${fmtUsd(usd)}</td>` : ''}
                ${col.rating    ? `<td style="padding:4px;color:#6b7280;">${m.row[col.rating] || '—'}</td>` : ''}
              </tr>
              <tr class="match-row-details" data-row-idx="${idx}" style="display:none;background:#f9fafb;border-bottom:1px solid #e5e7eb;">
                <td colspan="13" style="padding:10px 14px;">${structuringDetails(m)}</td>
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
        <table style="width:100%;min-width:900px;font-size:11px;border-collapse:collapse;">
          <thead><tr style="color:#6b7280;font-size:9px;text-transform:uppercase;letter-spacing:0.04em;border-bottom:1px solid #e5e7eb;">
            <th style="text-align:left;padding:4px;font-weight:500;">Uploaded name</th>
            <th style="text-align:left;padding:4px;font-weight:500;">City</th>
            <th style="text-align:left;padding:4px;font-weight:500;">State</th>
            <th style="text-align:right;padding:4px;font-weight:500;">MW</th>
            ${r.hasExposure ? '<th style="text-align:right;padding:4px;font-weight:500;">Your exposure</th>' : ''}
            <th style="text-align:left;padding:4px;font-weight:500;">Operator</th>
            <th style="text-align:left;padding:4px;font-weight:500;">Best near-match (score)</th>
          </tr></thead>
          <tbody>
            ${r.unmatchedRows.slice(0, 20).map(u => {
              const c = u.candidates[0];
              const ls = rowCityState(u.row, col);
              const usd = rowExposureUsd(u.row, col);
              return `<tr style="border-bottom:1px solid #f3f4f6;">
                <td style="padding:4px;">${u.row[col.name] || '—'}</td>
                <td style="padding:4px;color:#6b7280;">${ls.city || '—'}</td>
                <td style="padding:4px;color:#6b7280;">${(ls.state || '').toUpperCase() || '—'}</td>
                <td style="padding:4px;text-align:right;">${fmt(parseMW(u.row[col.mw]))}</td>
                ${r.hasExposure ? `<td style="padding:4px;text-align:right;color:#3730a3;font-weight:600;">${fmtUsd(usd)}</td>` : ''}
                <td style="padding:4px;color:#6b7280;">${u.row[col.operator] || u.row[col.tenant] || '—'}</td>
                <td style="padding:4px;font-size:10px;color:#9ca3af;">${c ? `${c.facility.name} (${(c.score*100).toFixed(0)}%)` : '—'}</td>
              </tr>`;
            }).join('')}
          </tbody>
        </table>
        </div>
      </div>` : '';

    box.innerHTML = kpiRow + gapTable + managerTable + matchedRows + unmatchedRows;
    box.style.display = '';

    window.__overlayMatched = r.matchedRows;
    document.getElementById('overlay-plot-map')?.addEventListener('click',
      () => plotExposureOnMap(window.__overlayMatched || []));
    document.getElementById('overlay-clear-map')?.addEventListener('click',
      () => { clearExposureMap(); setStatus('map overlay cleared'); });

    // Row expand/collapse for full deal structuring.
    box.querySelectorAll('tr.match-row').forEach(tr => {
      tr.addEventListener('click', () => {
        const idx = tr.dataset.rowIdx;
        const details = box.querySelector(`tr.match-row-details[data-row-idx="${idx}"]`);
        if (!details) return;
        const open = details.style.display !== 'none';
        details.style.display = open ? 'none' : '';
        const caret = tr.querySelector('td:first-child');
        if (caret) caret.textContent = open ? '▸' : '▾';
      });
    });
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
        const raw = parseCSV(text);
        if (!raw.rows.length) {
          setStatus('no rows found in file — is it actually a CSV?');
          return;
        }
        const parsed = tabulate(raw);
        const delimName = { ',': 'comma', ';': 'semicolon', '\t': 'tab' }[raw.delim] || raw.delim;
        if (!parsed.headers.length || !parsed.rows.length) {
          setStatus(`detected ${delimName}-delimited but couldn't extract headers + rows`);
          showDiagnostics(parsed, raw);
          return;
        }
        const col = autoDetectColumns(parsed.headers);
        if (!col.name) {
          setStatus(`could not auto-detect a name column — see headers below`);
          showDiagnostics(parsed, raw, col);
          return;
        }
        UPLOADED = { ...parsed, columnMap: col };
        const recognized = Object.entries(col).map(([k, v]) => `${k}→${v}`).join(', ');
        const headerNote = parsed.headerIdx > 0
          ? ` (header row #${parsed.headerIdx + 1}, ${delimName}-delimited)`
          : ` (${delimName}-delimited)`;
        setStatus(`loaded ${parsed.rows.length} rows locally${headerNote}. columns: ${recognized}`);
        runBtn.disabled = false;
      } catch (e) {
        setStatus(`parse error: ${e.message}`);
      }
    });

    function showDiagnostics(parsed, raw, col) {
      const box = $('overlay-results');
      const looked = (raw.rows || []).slice(0, 6).map((r, i) =>
        `<tr><td style="padding:4px;color:#9ca3af;">#${i + 1}</td>
          ${r.slice(0, 8).map(c => `<td style="padding:4px;border:1px solid #fde68a;">${(c || '').slice(0, 30)}</td>`).join('')}
        </tr>`).join('');
      const detectedList = col ? Object.entries(col).map(([k, v]) =>
        `<li><code>${k}</code> ← <code>${v}</code></li>`).join('') : '';
      box.style.display = '';
      box.innerHTML = `
        <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:5px;padding:12px;font-size:12px;color:#374151;">
          <div style="font-weight:600;color:#92400e;margin-bottom:6px;">Parser diagnostics</div>
          <div style="margin-bottom:8px;">First ${Math.min(6, raw.rows?.length || 0)} rows of your file (first 8 columns shown):</div>
          <div style="overflow-x:auto;margin-bottom:10px;">
            <table style="font-size:11px;border-collapse:collapse;">${looked}</table>
          </div>
          ${parsed.headers?.length ? `
            <div style="margin-bottom:8px;"><strong>Detected header row:</strong>
              <code>${parsed.headers.join(' | ')}</code></div>` : ''}
          ${detectedList ? `<div style="margin-bottom:8px;">
            <strong>Columns I recognized:</strong>
            <ul style="margin:4px 0 0 18px;">${detectedList}</ul>
          </div>` : ''}
          <div style="margin-top:8px;color:#6b7280;font-size:11px;line-height:1.5;">
            <strong>To fix:</strong> rename one of your columns to <code>name</code>,
            <code>asset_name</code>, <code>holding_name</code>, <code>security_name</code>,
            <code>issuer_name</code>, <code>facility</code>, or anything ending in
            <code>_name</code>. Or save the sheet with the header row at the top
            (no title/blank rows above it).
          </div>
        </div>`;
    }

    runBtn.addEventListener('click', async () => {
      runBtn.disabled = true;
      try { await runAnalysis(); }
      catch (e) { setStatus(`analysis error: ${e.message}`); }
      finally { runBtn.disabled = false; }
    });

    clearBtn.addEventListener('click', () => {
      UPLOADED = null;
      window.__overlayMatched = null;
      fileInput.value = '';
      runBtn.disabled = true;
      const box = $('overlay-results');
      if (box) { box.innerHTML = ''; box.style.display = 'none'; }
      clearExposureMap();
      setStatus('cleared from memory (including map overlay)');
    });
  });
})();
