/* Theme colours for script-drawn graphics (Chart.js, d3, Leaflet,
 * canvas). CSS can use var(--token) directly; canvas and SVG attributes
 * can't, so scripts resolve the same tokens from static/css/theme.css:
 *
 *   Theme.c('accent')        -> '#3b82f6'
 *   Theme.c('accent', 0.12)  -> 'rgba(59,130,246,0.12)'
 *
 * Load in <head>, after theme.css and before any script that draws.
 *
 * When the OS switches between light and dark, CSS updates by itself
 * but already-drawn charts keep their old colours. Pages that can
 * redraw in place register with Theme.onChange(fn); any other page
 * reloads so every chart is drawn again with the new colours.
 */
(function () {
  'use strict';

  var cache = {};
  var handlers = [];

  function read(name) {
    if (!(name in cache)) {
      cache[name] = getComputedStyle(document.documentElement)
        .getPropertyValue('--' + name).trim();
    }
    return cache[name];
  }

  function toRgba(hex, alpha) {
    var h = hex.replace('#', '');
    if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
    var n = parseInt(h, 16);
    return 'rgba(' + ((n >> 16) & 255) + ',' + ((n >> 8) & 255) + ',' + (n & 255) + ',' + alpha + ')';
  }

  function c(name, alpha) {
    var v = read(name);
    if (!v) {
      if (window.console) console.warn('Theme: unknown token --' + name);
      return alpha == null ? '#888888' : 'rgba(136,136,136,' + alpha + ')';
    }
    if (alpha == null) return v;
    return v.charAt(0) === '#' ? toRgba(v, alpha) : v;
  }

  function isLight() {
    return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches);
  }

  if (window.matchMedia) {
    var mq = window.matchMedia('(prefers-color-scheme: light)');
    var onSchemeChange = function () {
      cache = {};
      if (!handlers.length) { window.location.reload(); return; }
      handlers.forEach(function (fn) { try { fn(); } catch (e) { console.error(e); } });
    };
    if (mq.addEventListener) mq.addEventListener('change', onSchemeChange);
    else if (mq.addListener) mq.addListener(onSchemeChange);
  }

  window.Theme = {
    c: c,
    isLight: isLight,
    onChange: function (fn) { handlers.push(fn); },
  };
})();
