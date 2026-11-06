/* PNG export for dashboard charts, branded for sharing.
 *
 * ChartExport.attach({ source }) adds a "PNG" button to the header of
 * every .lm-section that holds a chart <canvas id="…">. The download is
 * the chart redrawn onto a solid background with its section title,
 * a source line, a "parramacro.com" footer and a faint "Parra Macro"
 * watermark across the plot. Colours follow the current theme.
 *
 * ChartExport.download(canvas, { title, source, filename }) does the
 * same for any canvas.
 */
/* eslint-env browser */
(function () {
  'use strict';

  const SITE = 'parramacro.com';

  function slug(s) {
    return (s || 'chart').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 60);
  }

  function today() {
    return new Date().toISOString().slice(0, 10);
  }

  function compose(canvas, opts) {
    // Work at the chart's backing-store resolution so the PNG is as
    // sharp as the screen (Chart.js already scales by devicePixelRatio).
    const k = canvas.width / (canvas.clientWidth || canvas.width) || 1;
    const pad = 24 * k;
    const titleSize = 18 * k;
    const metaSize = 12 * k;
    const headH = opts.title ? titleSize + 18 * k : 0;
    const footH = 34 * k;

    const out = document.createElement('canvas');
    out.width = canvas.width + pad * 2;
    out.height = pad + headH + canvas.height + footH;
    const ctx = out.getContext('2d');
    const sans = getComputedStyle(document.body).fontFamily || 'sans-serif';
    const serif = (getComputedStyle(document.documentElement).getPropertyValue('--font-serif') || 'Georgia, serif').trim();

    ctx.fillStyle = Theme.c('surface');
    ctx.fillRect(0, 0, out.width, out.height);

    if (opts.title) {
      ctx.fillStyle = Theme.c('ink-strong');
      ctx.font = `600 ${titleSize}px ${serif}`;
      ctx.textBaseline = 'top';
      ctx.fillText(opts.title, pad, pad);
    }

    const chartTop = pad + headH;
    ctx.drawImage(canvas, pad, chartTop);

    // Watermark: centred over the plot, faint enough not to fight the data.
    ctx.save();
    ctx.fillStyle = Theme.c('ink', 0.07);
    ctx.font = `600 ${Math.min(canvas.width / 9, 64 * k)}px ${serif}`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText('Parra Macro', pad + canvas.width / 2, chartTop + canvas.height / 2);
    ctx.restore();

    // Footer: source left, site right, divided from the chart by a rule.
    const footY = chartTop + canvas.height + 10 * k;
    ctx.fillStyle = Theme.c('rule');
    ctx.fillRect(pad, footY, canvas.width, Math.max(1, k));
    ctx.textBaseline = 'top';
    ctx.font = `${metaSize}px ${sans}`;
    ctx.fillStyle = Theme.c('ink-4');
    ctx.textAlign = 'left';
    ctx.fillText(opts.source ? `Source: ${opts.source}` : '', pad, footY + 9 * k);
    ctx.font = `600 ${metaSize}px ${sans}`;
    ctx.fillStyle = Theme.c('ink-2');
    ctx.textAlign = 'right';
    ctx.fillText(SITE, pad + canvas.width, footY + 9 * k);

    return out;
  }

  function download(canvas, opts) {
    opts = opts || {};
    if (!canvas || !canvas.width || !canvas.height) return false;
    const out = compose(canvas, opts);
    const a = document.createElement('a');
    a.download = opts.filename || `parramacro-${slug(opts.title)}-${today()}.png`;
    a.href = out.toDataURL('image/png');
    document.body.appendChild(a);
    a.click();
    a.remove();
    return true;
  }

  function attach(config) {
    config = config || {};
    document.querySelectorAll('.lm-section').forEach((section) => {
      const canvas = section.querySelector('canvas[id]');
      const header = section.querySelector('.lm-section-header');
      if (!canvas || !header || header.querySelector('.chart-png-btn')) return;

      // Keep any existing toggles and the new button together on the right.
      let actions = header.querySelector('.lm-section-actions');
      if (!actions) {
        actions = document.createElement('div');
        actions.className = 'lm-section-actions';
        const toggles = header.querySelector(':scope > .lm-toggle-group');
        if (toggles) actions.appendChild(toggles);
        header.appendChild(actions);
      }

      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'chart-png-btn';
      btn.title = 'Download this chart as a PNG';
      btn.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>PNG';
      btn.addEventListener('click', () => {
        const h2 = section.querySelector('h2');
        const title = h2 ? h2.textContent.replace(/\s+/g, ' ').trim() : '';
        const prefix = config.titlePrefix ? `${config.titlePrefix}: ` : '';
        if (!download(canvas, { title: prefix + title, source: config.source })) {
          const label = btn.innerHTML;
          btn.textContent = 'No chart yet';
          setTimeout(() => { btn.innerHTML = label; }, 1500);
        }
      });
      actions.appendChild(btn);
    });
  }

  window.ChartExport = { attach, download, compose };
})();
