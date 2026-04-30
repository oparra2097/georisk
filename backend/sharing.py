"""
Shareable chart links — Open Graph metadata + dynamic preview images.

Turns any chart URL (/data/<category>/<dataset>/<subview>, /georisk) into a
social-media-ready link. When pasted into LinkedIn, X, Substack, Slack, etc.,
the crawler gets:
  - og:title / og:description — chart-specific
  - og:image — a branded 1200x630 preview card rendered on demand with Pillow
"""
from __future__ import annotations

import hashlib
import io
import os
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, urlencode

from flask import Blueprint, abort, request, send_file

from config import Config


sharing_bp = Blueprint("sharing", __name__)


# ──────────────────────────────────────────────────────────────────────────
# Dataset metadata registry
#
# Mirrors the client-side catalog (static/js/data-catalog.js) for the
# category/dataset combinations we want to enable rich previews for. Adding
# a new entry here + in the JS catalog unlocks shareability for it.
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class ChartMeta:
    title: str
    description: str
    source: str


DATASETS: dict[tuple[str, str], ChartMeta] = {
    # Prices & Inflation
    ("prices", "us-cpi"):   ChartMeta("US CPI",  "Consumer price inflation in the United States, BLS CPI-U.", "Bureau of Labor Statistics"),
    ("prices", "uk-cpi"):   ChartMeta("UK CPI",  "UK consumer price inflation, Office for National Statistics MM23.", "ONS"),
    ("prices", "eu-hicp"):  ChartMeta("EU HICP", "Euro Area harmonised consumer price inflation.", "Eurostat"),

    # Commodities
    ("commodities", "forecast-oil"):             ChartMeta("Oil & Gas Forecasts",      "Scenario-based oil and gas price forecasts.",        "Parra Macro"),
    ("commodities", "forecast-ag"):              ChartMeta("Agriculture Forecasts",    "Scenario-based agricultural commodity forecasts.",  "Parra Macro"),
    ("commodities", "forecast-metals"):          ChartMeta("Metals Forecasts",         "Scenario-based metals price forecasts.",             "Parra Macro"),
    ("commodities", "fertilizer-em-inflation"):  ChartMeta("Fertilizer & EM Inflation","Fertilizer price shocks and emerging-market food inflation impact.", "Parra Macro"),

    # Trade & Reserves
    ("trade", "yale-tariff"):      ChartMeta("US Effective Tariff Rate",    "Average effective US tariff rate — policy-driven estimate.",       "Yale Budget Lab"),
    ("trade", "cofer"):            ChartMeta("Central Bank Reserves",       "Global official foreign exchange reserves by currency.",           "IMF COFER"),
    ("trade", "cofer-nowcast"):    ChartMeta("Reserve Currency Composition","Nowcast of global central bank reserve currency allocation.",      "Parra Macro / IMF COFER"),
    ("trade", "em-vulnerability"): ChartMeta("EM External Vulnerability",   "Emerging-market external financing vulnerability indicators.",      "Parra Macro"),
    ("trade", "wb-exports-pct"):   ChartMeta("Exports (% of GDP)",          "Goods and services exports as a share of GDP.",                    "World Bank"),
    ("trade", "wb-imports-pct"):   ChartMeta("Imports (% of GDP)",          "Goods and services imports as a share of GDP.",                    "World Bank"),
    ("trade", "wb-trade-pct"):     ChartMeta("Trade (% of GDP)",            "Total trade (exports + imports) as a share of GDP.",               "World Bank"),
    ("trade", "wb-fdi-pct"):       ChartMeta("FDI Inflows (% of GDP)",      "Foreign direct investment inflows as a share of GDP.",             "World Bank"),
    ("trade", "weo-ca-pct"):       ChartMeta("Current Account (% of GDP)",  "Current account balance as a share of GDP.",                       "IMF WEO"),
    ("trade", "weo-ca-usd"):       ChartMeta("Current Account ($B)",        "Current account balance in US dollars.",                           "IMF WEO"),

    # Growth & Output
    ("growth", "us-gdp-nowcast"): ChartMeta("US GDP Nowcast",           "Real-time US GDP growth nowcast.",                        "Parra Macro"),
    ("growth", "weo-gdp"):        ChartMeta("GDP Growth",               "Real GDP growth forecasts by country.",                   "IMF WEO"),
    ("growth", "weo-gdp-nom"):    ChartMeta("GDP (Nominal, $B)",        "Nominal GDP in US dollars.",                              "IMF WEO"),
    ("growth", "weo-gdp-ppp"):    ChartMeta("GDP (PPP, $B)",            "GDP in purchasing-power-parity dollars.",                 "IMF WEO"),
    ("growth", "weo-gdp-pc"):     ChartMeta("GDP Per Capita (PPP)",     "GDP per capita in PPP dollars.",                          "IMF WEO"),
    ("growth", "weo-inflation"):  ChartMeta("Inflation",                "Inflation forecasts by country.",                          "IMF WEO"),

    # Labor / Fiscal / Monetary
    ("labor",  "weo-unemployment"): ChartMeta("Unemployment Rate",        "Unemployment rate forecasts by country.",                 "IMF WEO"),
    ("fiscal", "weo-debt"):         ChartMeta("Government Debt (% GDP)",  "General government gross debt as a share of GDP.",        "IMF WEO"),
    ("fiscal", "sovereign-debt"):   ChartMeta("Sovereign Debt",           "Sovereign debt and fiscal metrics by country.",           "Parra Macro"),
}


SITE_NAME = "Parra Macro"
SITE_TAGLINE = "Macro research & geopolitical risk analysis"
DEFAULT_TITLE = "Parra Macro — Macro & Geopolitical Risk"
DEFAULT_DESC = "Interactive macro and geopolitical risk analytics: inflation, commodities, reserves, sovereign risk, and more."


# ──────────────────────────────────────────────────────────────────────────
# URL parsing — map request path to chart metadata
# ──────────────────────────────────────────────────────────────────────────

_PATH_RE = re.compile(r"^/data/(?P<cat>[^/]+)(?:/(?P<ds>[^/]+))?(?:/(?P<sv>[^/?]+))?")


def meta_for_path(path: str) -> dict:
    """Return og_title/og_description/og_image/og_url for the given URL path.

    Falls back to site defaults when the path doesn't match a known chart.
    """
    base = {
        "og_title": DEFAULT_TITLE,
        "og_description": DEFAULT_DESC,
        "og_image": "/og/default.png",
        "og_url": path or "/",
        "og_type": "website",
    }

    if path == "/" or path == "":
        return base

    if path.startswith("/georisk"):
        return {
            **base,
            "og_title": "GeoRisk Monitor — Parra Macro",
            "og_description": "Real-time geopolitical risk dashboard: country risk scores, hotspots, and live news signals.",
            "og_image": "/og/preview.png?chart=georisk",
            "og_url": path,
        }

    if path.startswith("/macro-model"):
        return {
            **base,
            "og_title": "US Macro Model — Parra Macro",
            "og_description": "Structural US macro model with scenario simulations: GDP, inflation, unemployment, and rates.",
            "og_image": "/og/preview.png?chart=macro-model",
            "og_url": path,
        }

    if path.startswith("/house-prices"):
        return {
            **base,
            "og_title": "US House Prices — Parra Macro",
            "og_description": "Interactive US house-price index with state and county drill-down and ECM-based forecasts.",
            "og_image": "/og/preview.png?chart=house-prices",
            "og_url": path,
        }

    if path.startswith("/models"):
        return {
            **base,
            "og_title": "Models — Parra Macro",
            "og_description": "GeoRisk Monitor, US Macro Model, and US House Prices — interactive forecasting tools.",
            "og_image": "/og/preview.png?chart=models",
            "og_url": path,
        }

    if path.startswith("/research"):
        return {**base, "og_title": "Research — Parra Macro", "og_description": "Original research on markets, macro, and geopolitical risk.", "og_url": path}

    if path.startswith("/economist"):
        return {**base, "og_title": "Economist — Parra Macro", "og_description": "AI economist workbench for macro analysis.", "og_url": path}

    if path.startswith("/about"):
        return {**base, "og_title": "About — Parra Macro", "og_url": path}

    m = _PATH_RE.match(path)
    if m:
        cat = m.group("cat")
        ds = m.group("ds")
        sv = m.group("sv") or ""
        if cat and ds:
            meta = DATASETS.get((cat, ds))
            if meta:
                svlabel = _subview_label(sv)
                full_title = f"{meta.title}{(' — ' + svlabel) if svlabel else ''} | {SITE_NAME}"
                params = {"cat": cat, "ds": ds}
                if sv:
                    params["sv"] = sv
                # Forward request query params (e.g. ?type=gold, ?freq=yoy)
                # so each variant gets its own preview image and cache key.
                try:
                    extra = request.args.to_dict(flat=True) if request else {}
                except Exception:
                    extra = {}
                for k, v in extra.items():
                    if k in ("cat", "ds", "sv", "chart"):
                        continue
                    params[k] = v
                return {
                    **base,
                    "og_title": full_title,
                    "og_description": meta.description,
                    "og_image": "/og/preview.png?" + urlencode(params),
                    "og_url": quote(path, safe="/"),
                    "og_type": "article",
                }
            # Known category, unknown dataset
            return {**base, "og_title": f"{cat.title()} — Parra Macro", "og_url": quote(path, safe="/")}

    return base


def _subview_label(sv: str) -> str:
    if not sv or sv == "overview":
        return ""
    # Nice-cased fallback (e.g. "food_bev" -> "Food Bev", "WTI Crude" stays as-is)
    if "_" in sv or sv.islower():
        return sv.replace("_", " ").title()
    return sv


# ──────────────────────────────────────────────────────────────────────────
# Social-media crawler detection
# ──────────────────────────────────────────────────────────────────────────

_CRAWLER_RE = re.compile(
    r"(facebookexternalhit|Facebot|Twitterbot|LinkedInBot|Slackbot|"
    r"Discordbot|TelegramBot|WhatsApp|SkypeUriPreview|Applebot|Pinterest|"
    r"redditbot|embedly|Iframely|vkShare|W3C_Validator|Substack|"
    r"Googlebot|bingbot|DuckDuckBot|YandexBot)",
    re.I,
)


def is_social_crawler(user_agent: str) -> bool:
    if not user_agent:
        return False
    return bool(_CRAWLER_RE.search(user_agent))


# ──────────────────────────────────────────────────────────────────────────
# OG image generation — Pillow-based branded preview card (1200×630)
# ──────────────────────────────────────────────────────────────────────────

OG_W, OG_H = 1200, 630

# Parra Macro palette (derived from site.css)
BG_TOP = (15, 23, 42)        # slate-900
BG_BOTTOM = (2, 6, 23)       # near-black
ACCENT = (239, 68, 68)       # red-500
ACCENT_SOFT = (239, 68, 68, 28)
TEXT_PRIMARY = (243, 244, 246)
TEXT_MUTED = (156, 163, 175)
GRID = (55, 65, 81)


def _og_cache_dir() -> str:
    path = os.path.join(Config.DATA_DIR, "og-cache")
    os.makedirs(path, exist_ok=True)
    return path


def _cache_path(key: str) -> str:
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return os.path.join(_og_cache_dir(), f"{h}.png")


def _load_font(size: int, bold: bool = False):
    from PIL import ImageFont
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _wrap(draw, text: str, font, max_width: int) -> list[str]:
    words = text.split()
    lines, cur = [], ""
    for w in words:
        trial = (cur + " " + w).strip()
        if draw.textlength(trial, font=font) <= max_width:
            cur = trial
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def _render_card(title: str, subtitle: str, source: str) -> bytes:
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (OG_W, OG_H), BG_TOP)
    draw = ImageDraw.Draw(img, "RGBA")

    # Vertical gradient background
    for y in range(OG_H):
        t = y / OG_H
        r = int(BG_TOP[0] * (1 - t) + BG_BOTTOM[0] * t)
        g = int(BG_TOP[1] * (1 - t) + BG_BOTTOM[1] * t)
        b = int(BG_TOP[2] * (1 - t) + BG_BOTTOM[2] * t)
        draw.line([(0, y), (OG_W, y)], fill=(r, g, b))

    # Decorative sparkline (stylized — suggests "chart"), kept subtle
    # and low in the card so it doesn't overwhelm the source pill.
    import math
    spark_pts = []
    base_y = 520
    amp = 45
    for i in range(0, OG_W + 1, 10):
        phase = i / OG_W
        y = base_y - amp * (
            math.sin(phase * 6.5) * 0.55
            + math.sin(phase * 2.1 + 1.2) * 0.35
            + phase * 0.4
        )
        spark_pts.append((i, y))
    # Filled area under curve (very subtle)
    poly = spark_pts + [(OG_W, OG_H), (0, OG_H)]
    draw.polygon(poly, fill=ACCENT_SOFT)
    # Sparkline itself
    draw.line(spark_pts, fill=ACCENT, width=3, joint="curve")

    # Accent bar (left)
    draw.rectangle([(56, 96), (64, 540)], fill=ACCENT)

    # Brand mark
    brand_font = _load_font(28, bold=True)
    draw.text((88, 70), SITE_NAME.upper(), font=brand_font, fill=TEXT_PRIMARY)
    tag_font = _load_font(18)
    draw.text((88 + draw.textlength(SITE_NAME.upper(), font=brand_font) + 16, 78),
              SITE_TAGLINE, font=tag_font, fill=TEXT_MUTED)

    # Title (wrap up to 2 lines)
    title_font = _load_font(68, bold=True)
    title_lines = _wrap(draw, title, title_font, OG_W - 180)[:2]
    y = 160
    for line in title_lines:
        draw.text((88, y), line, font=title_font, fill=TEXT_PRIMARY)
        y += 82

    # Subtitle / description
    sub_font = _load_font(28)
    sub_lines = _wrap(draw, subtitle, sub_font, OG_W - 180)[:2]
    y += 12
    for line in sub_lines:
        draw.text((88, y), line, font=sub_font, fill=TEXT_MUTED)
        y += 38

    # Source pill (bottom-left)
    if source:
        src_font = _load_font(20, bold=True)
        src_text = f"SOURCE · {source.upper()}"
        tw = draw.textlength(src_text, font=src_font)
        pad_x, pad_y = 18, 10
        pill_x, pill_y = 88, OG_H - 80
        draw.rounded_rectangle(
            [(pill_x, pill_y), (pill_x + tw + pad_x * 2, pill_y + 40)],
            radius=20,
            fill=(255, 255, 255, 18),
            outline=ACCENT,
            width=2,
        )
        draw.text((pill_x + pad_x, pill_y + pad_y - 2), src_text, font=src_font, fill=TEXT_PRIMARY)

    # URL (bottom-right)
    url_font = _load_font(22)
    url_text = "parramacro.com"
    url_w = draw.textlength(url_text, font=url_font)
    draw.text((OG_W - 88 - url_w, OG_H - 68), url_text, font=url_font, fill=TEXT_MUTED)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def _meta_for_params(chart: str, cat: str, ds: str, sv: str) -> tuple[str, str, str]:
    """Return (title, description, source) for OG image generation."""
    chart_meta = {
        "georisk": (
            "GeoRisk Monitor",
            "Real-time geopolitical risk scores, hotspots, and news signals across 51 countries.",
            "Parra Macro",
        ),
        "macro-model": (
            "US Macro Model",
            "Structural US macro model with scenario simulations across GDP, inflation, unemployment, and rates.",
            "Parra Macro",
        ),
        "house-prices": (
            "US House Prices",
            "House-price index with state and county drill-down and ECM-based forecasts.",
            "FHFA / Case-Shiller / Zillow",
        ),
        "models": (
            "Parra Macro Models",
            "GeoRisk Monitor, US Macro Model, and US House Prices — interactive forecasting tools.",
            "Parra Macro",
        ),
    }
    if chart in chart_meta:
        return chart_meta[chart]
    meta = DATASETS.get((cat, ds)) if cat and ds else None
    if meta:
        svlabel = _subview_label(sv)
        title = f"{meta.title}" + (f" — {svlabel}" if svlabel else "")
        return (title, meta.description, meta.source)
    return (DEFAULT_TITLE, DEFAULT_DESC, "Parra Macro")


# ──────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────

@sharing_bp.route("/og/preview.png")
def og_preview():
    """Dynamic per-chart OG image. Cached on disk."""
    chart = request.args.get("chart", "").strip().lower()
    cat = request.args.get("cat", "").strip().lower()
    ds = request.args.get("ds", "").strip().lower()
    sv = request.args.get("sv", "").strip()

    # Cache key includes the full set of request args so query-param
    # variants (?type=gold, ?freq=yoy, …) produce distinct images.
    extra = {k: v for k, v in request.args.items() if k not in ("chart", "cat", "ds", "sv")}
    extra_key = "|".join(f"{k}={v}" for k, v in sorted(extra.items()))
    # Bump the version prefix any time the renderer or fetchers change so
    # old cached PNGs don't show up after a deploy. v4: real-data fetcher
    # for commodity forecasts now wired correctly.
    cache_key = f"v4|{chart}|{cat}|{ds}|{sv}|{extra_key}"
    cache_file = _cache_path(cache_key)

    if not os.path.exists(cache_file):
        png = None
        # Try the real-data renderer first — produces a card with an actual
        # line chart and headline stat for known datasets.
        try:
            from backend.og_charts import fetch_chart_data, render_chart_card
            data = fetch_chart_data(cat, ds, sv, extra)
            if data and data.points:
                png = render_chart_card(data)
        except Exception as e:
            # Don't let a data-source hiccup break the preview — fall through.
            import logging
            logging.getLogger(__name__).warning("OG real-data render failed: %s", e)
            png = None

        if png is None:
            # Fall back to the simpler branded template card.
            title, desc, source = _meta_for_params(chart, cat, ds, sv)
            try:
                png = _render_card(title, desc, source)
            except Exception:
                default = os.path.join(_og_cache_dir(), "default.png")
                if not os.path.exists(default):
                    try:
                        with open(default, "wb") as fh:
                            fh.write(_render_card(DEFAULT_TITLE, DEFAULT_DESC, "Parra Macro"))
                    except Exception:
                        abort(500)
                cache_file = default
                png = None

        if png is not None:
            try:
                with open(cache_file, "wb") as fh:
                    fh.write(png)
            except Exception:
                pass

    resp = send_file(cache_file, mimetype="image/png", max_age=60 * 60 * 24)
    resp.headers["Cache-Control"] = "public, max-age=86400, immutable"
    return resp


@sharing_bp.route("/og/debug")
def og_debug():
    """Inspect what the OG renderer would do for a given chart.

    Public, read-only — returns JSON describing whether the real-data
    fetcher succeeded, the headline value it picked, and how many points
    it has. Useful for diagnosing why a preview falls back to the
    template card.
    """
    from flask import jsonify
    chart = request.args.get("chart", "").strip().lower()
    cat = request.args.get("cat", "").strip().lower()
    ds = request.args.get("ds", "").strip().lower()
    sv = request.args.get("sv", "").strip()
    extra = {k: v for k, v in request.args.items() if k not in ("chart", "cat", "ds", "sv")}
    out = {"cat": cat, "ds": ds, "sv": sv, "extra": extra, "real_data": None, "fallback_meta": None}
    try:
        from backend.og_charts import fetch_chart_data
        data = fetch_chart_data(cat, ds, sv, extra)
        if data is None:
            out["real_data"] = {"ok": False, "reason": "fetcher returned None"}
        else:
            out["real_data"] = {
                "ok": True,
                "title": data.title,
                "headline_value": data.headline_value,
                "headline_label": data.headline_label,
                "n_points": len(data.points),
                "x_first": data.x_label_first,
                "x_last": data.x_label_last,
            }
    except Exception as e:
        out["real_data"] = {"ok": False, "reason": f"exception: {type(e).__name__}: {e}"}
    title, desc, source = _meta_for_params(chart, cat, ds, sv)
    out["fallback_meta"] = {"title": title, "description": desc, "source": source}
    return jsonify(out)


@sharing_bp.route("/og/default.png")
def og_default():
    cache_file = _cache_path("v2|default")
    if not os.path.exists(cache_file):
        try:
            png = _render_card(DEFAULT_TITLE, DEFAULT_DESC, "Parra Macro")
            with open(cache_file, "wb") as fh:
                fh.write(png)
        except Exception:
            abort(500)
    resp = send_file(cache_file, mimetype="image/png", max_age=60 * 60 * 24)
    resp.headers["Cache-Control"] = "public, max-age=86400, immutable"
    return resp
