"""
Real-data chart rendering for Open Graph preview cards.

For known datasets this module fetches the underlying time series, picks a
headline value, and renders a 1200×630 social-preview card with a real
line chart inside it. Charts not registered here fall back to the simpler
branded template card in `backend.sharing._render_card`.
"""
from __future__ import annotations

import io
import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
STATIC_DATA = REPO_ROOT / "static" / "data"


# ──────────────────────────────────────────────────────────────────────────
# Normalized data model
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class ChartData:
    title: str
    subtitle: str
    source: str
    headline_value: str
    headline_label: str
    points: list[tuple[float, float]] = field(default_factory=list)  # (x_index, y_value)
    x_label_first: str = ""
    x_label_last: str = ""
    y_unit: str = ""           # appended to axis labels ("%", "$B", etc.)
    accent: tuple[int, int, int] = (239, 68, 68)
    direction_up_is_good: bool = True  # used purely for color choice on YoY change


# ──────────────────────────────────────────────────────────────────────────
# Per-chart data fetchers
# Each fetcher receives the parsed (cat, ds, sv, query_params) and returns
# a ChartData instance, or None if data isn't available (we'll fall back).
# ──────────────────────────────────────────────────────────────────────────

def _fmt_pct(v: float, decimals: int = 1) -> str:
    return f"{v:.{decimals}f}%"


def _fmt_usd_b(v: float) -> str:
    if abs(v) >= 1000:
        return f"${v/1000:.2f}T"
    return f"${v:.0f}B"


def _parse_iso(s: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    # Quarterly format, e.g. "2024-Q3"
    if "-Q" in s:
        try:
            year, q = s.split("-Q")
            month = (int(q) - 1) * 3 + 1
            return datetime(int(year), month, 1)
        except Exception:
            return None
    return None


def _short_date(s: str) -> str:
    dt = _parse_iso(s)
    if dt:
        return dt.strftime("%b %Y")
    return s


def _full_date(s: str) -> str:
    dt = _parse_iso(s)
    if dt:
        return dt.strftime("%b %d, %Y")
    return s


# ─── Yale tariff ──────────────────────────────────────────────────────────

def _fetch_yale_tariff(cat, ds, sv, qs) -> Optional[ChartData]:
    try:
        from backend.data_sources.yale_tariff import get_yale_tariff_data
        d = get_yale_tariff_data()
    except Exception as e:
        logger.warning("yale_tariff fetch failed: %s", e)
        return None
    pts = d.get("points") or []
    if len(pts) < 2:
        return None
    series = []
    for i, p in enumerate(pts):
        v = p.get("value")
        if v is None:
            continue
        series.append((float(i), float(v)))
    if len(series) < 2:
        return None
    latest_v = series[-1][1]
    return ChartData(
        title="US Effective Tariff Rate",
        subtitle="Average effective US tariff rate — policy-driven estimate.",
        source="Yale Budget Lab",
        headline_value=_fmt_pct(latest_v),
        headline_label=f"Latest · {_full_date(pts[-1].get('date',''))}",
        points=series,
        x_label_first=_short_date(pts[0].get("date", "")),
        x_label_last=_short_date(pts[-1].get("date", "")),
        y_unit="%",
        accent=(239, 68, 68),
    )


# ─── COFER nowcast (reserve currency composition) ─────────────────────────

def _fetch_cofer_nowcast(cat, ds, sv, qs) -> Optional[ChartData]:
    path = STATIC_DATA / "reserves_nowcast.json"
    if not path.exists():
        return None
    try:
        with open(path) as fh:
            d = json.load(fh)
    except Exception:
        return None

    nowcast = d.get("nowcast") or {}
    historical = d.get("historical") or {}
    dates = nowcast.get("dates") or []
    nc_shares = nowcast.get("shares") or {}
    h_periods = historical.get("periods") or []
    h_shares = historical.get("shares") or {}

    # Default: USD share — the marquee number for COFER previews.
    currency = (qs.get("ccy") or "USD").upper()
    if currency not in nc_shares and currency not in h_shares:
        currency = "USD"

    # Use the quarterly historical series (full COFER history is ~26 years
    # and tells a much clearer story than mixing in the weekly nowcast,
    # which clusters at the end and squashes the chart).
    series_x: list[str] = []
    series_y: list[float] = []
    for i, p in enumerate(h_periods):
        vals = h_shares.get(currency) or []
        if i < len(vals) and vals[i] is not None:
            series_x.append(p)
            series_y.append(float(vals[i]))

    # Then anchor the latest nowcast value on the right edge so the
    # headline number matches today.
    nc_vals = nc_shares.get(currency) or []
    if dates and nc_vals and nc_vals[-1] is not None:
        series_x.append(dates[-1])
        series_y.append(float(nc_vals[-1]))

    if len(series_y) < 2:
        return None

    points = [(float(i), v) for i, v in enumerate(series_y)]
    latest_v = series_y[-1]

    return ChartData(
        title=f"{currency} Share of Global Reserves",
        subtitle="IMF COFER reserves composition + Parra Macro nowcast.",
        source="IMF COFER · Parra Macro Nowcast",
        headline_value=_fmt_pct(latest_v),
        headline_label=f"Latest · {_short_date(series_x[-1])}",
        points=points,
        x_label_first=_short_date(series_x[0]),
        x_label_last=_short_date(series_x[-1]),
        y_unit="%",
        accent=(31, 119, 180),  # USD blue from currency_colors
        direction_up_is_good=False,  # context-specific, ignored for color
    )


# ─── COFER (FX/Gold/Total Reserves by country) ────────────────────────────

def _fetch_cofer_country(cat, ds, sv, qs) -> Optional[ChartData]:
    """COFER country-level reserves (chosen via ?type=total|fx|gold)."""
    cache = REPO_ROOT / "data" / "reserves_cache.json"
    if not cache.exists():
        return None
    try:
        with open(cache) as fh:
            d = json.load(fh)
    except Exception:
        return None
    payload = d.get("data") or {}
    years = payload.get("years") or []
    countries = payload.get("countries") or []
    if not years or not countries:
        return None

    rtype = (qs.get("type") or "total").lower()
    field_map = {
        "total": ("total_reserves", "Total Reserves"),
        "fx":    ("fx_reserves",    "FX Reserves"),
        "gold":  ("gold_reserves",  "Gold Reserves"),
    }
    field_key, type_label = field_map.get(rtype, field_map["total"])

    # Sum across reporting countries to get a "World" aggregate
    n = len(years)
    totals = [0.0] * n
    have = [False] * n
    for c in countries:
        vals = c.get(field_key) or []
        for i in range(min(n, len(vals))):
            v = vals[i]
            if v is not None:
                totals[i] += float(v)
                have[i] = True
    series = [(float(i), totals[i]) for i in range(n) if have[i]]
    if len(series) < 2:
        return None

    latest_v = series[-1][1]
    return ChartData(
        title=f"World {type_label}",
        subtitle="Official central-bank reserves, summed across reporting countries.",
        source="IMF COFER",
        headline_value=_fmt_usd_b(latest_v),
        headline_label=f"Latest · {years[int(series[-1][0])]}",
        points=series,
        x_label_first=str(years[int(series[0][0])]),
        x_label_last=str(years[int(series[-1][0])]),
        y_unit="$B",
        accent=(218, 165, 32) if rtype == "gold" else (31, 119, 180),
    )


# ─── CPI (US/UK/EU) ───────────────────────────────────────────────────────

def _cpi_from(headline: dict, country: str, source: str, accent) -> Optional[ChartData]:
    if not headline:
        return None
    yoy = headline.get("yoy") or []
    dates = headline.get("dates") or []
    if not yoy or not dates or len(yoy) != len(dates):
        return None
    # Trim None tail
    pairs = [(d, v) for d, v in zip(dates, yoy) if v is not None]
    if len(pairs) < 6:
        return None
    pairs = pairs[-120:]  # last 10 years
    points = [(float(i), v) for i, (_, v) in enumerate(pairs)]
    latest = pairs[-1][1]
    return ChartData(
        title=f"{country} CPI Inflation",
        subtitle="Year-on-year consumer price inflation.",
        source=source,
        headline_value=_fmt_pct(latest),
        headline_label=f"YoY · {_short_date(pairs[-1][0])}",
        points=points,
        x_label_first=_short_date(pairs[0][0]),
        x_label_last=_short_date(pairs[-1][0]),
        y_unit="%",
        accent=accent,
    )


def _fetch_us_cpi(cat, ds, sv, qs) -> Optional[ChartData]:
    try:
        from backend.data_sources.bls_cpi import get_bls_cpi_data
        d = get_bls_cpi_data()
    except Exception:
        return None
    headline = (d.get("series") or {}).get("All Items") or (d.get("series") or {}).get("Headline") or {}
    return _cpi_from(headline, "US", "Bureau of Labor Statistics", (239, 68, 68))


def _fetch_uk_cpi(cat, ds, sv, qs) -> Optional[ChartData]:
    try:
        from backend.data_sources.ons_cpi import get_ons_cpi_data
        d = get_ons_cpi_data()
    except Exception:
        return None
    headline = (d.get("series") or {}).get("All Items") or (d.get("series") or {}).get("Headline") or {}
    return _cpi_from(headline, "UK", "Office for National Statistics", (37, 99, 235))


def _fetch_eu_cpi(cat, ds, sv, qs) -> Optional[ChartData]:
    try:
        from backend.data_sources.eurostat_hicp import get_eurostat_cpi_data
        d = get_eurostat_cpi_data()
    except Exception:
        return None
    headline = (d.get("series") or {}).get("All Items") or (d.get("series") or {}).get("Headline") or {}
    return _cpi_from(headline, "Euro Area", "Eurostat", (255, 127, 14))


# ─── Commodity forecasts ──────────────────────────────────────────────────

def _fetch_forecast(cat, ds, sv, qs) -> Optional[ChartData]:
    try:
        from backend.data_sources.commodities_forecast import get_forecast_data
        d = get_forecast_data()
    except Exception:
        return None

    group_map = {
        "forecast-oil":    "Oil & Gas",
        "forecast-ag":     "Agriculture",
        "forecast-metals": "Metals",
    }
    group_name = group_map.get(ds)
    if not group_name:
        return None
    group = (d.get("groups") or {}).get(group_name) or {}
    commodities = group.get("commodities") or []
    if not commodities:
        return None

    # Pick the requested subview commodity, or the first one.
    chosen = None
    for c in commodities:
        if c.get("name") == sv:
            chosen = c
            break
    if not chosen:
        chosen = commodities[0]

    history = chosen.get("history") or []
    forecast = chosen.get("forecast") or []
    if len(history) < 2 and len(forecast) < 2:
        return None

    series_x, series_y = [], []
    for p in history[-60:]:  # last 5y monthly
        v = p.get("price")
        if v is None:
            continue
        series_x.append(p.get("date"))
        series_y.append(float(v))
    for p in forecast:
        v = p.get("price") or p.get("baseline") or p.get("mean")
        if v is None:
            continue
        series_x.append(p.get("date"))
        series_y.append(float(v))

    if len(series_y) < 2:
        return None
    points = [(float(i), v) for i, v in enumerate(series_y)]
    name = chosen.get("name") or group_name
    unit = chosen.get("unit") or ""
    latest = series_y[-1]
    fmt = f"${latest:,.2f}" + (f" / {unit}" if unit else "")
    return ChartData(
        title=f"{name} Forecast",
        subtitle=f"{group_name} — scenario-based price forecast.",
        source="Parra Macro",
        headline_value=fmt,
        headline_label=f"{_short_date(series_x[-1])} forecast",
        points=points,
        x_label_first=_short_date(series_x[0]),
        x_label_last=_short_date(series_x[-1]),
        y_unit=unit,
        accent=(255, 127, 14),
    )


# ─── Registry ─────────────────────────────────────────────────────────────

FETCHERS: dict[tuple[str, str], Callable] = {
    ("trade", "yale-tariff"):     _fetch_yale_tariff,
    ("trade", "cofer"):           _fetch_cofer_country,
    ("trade", "cofer-nowcast"):   _fetch_cofer_nowcast,
    ("prices", "us-cpi"):         _fetch_us_cpi,
    ("prices", "uk-cpi"):         _fetch_uk_cpi,
    ("prices", "eu-hicp"):        _fetch_eu_cpi,
    ("commodities", "forecast-oil"):    _fetch_forecast,
    ("commodities", "forecast-ag"):     _fetch_forecast,
    ("commodities", "forecast-metals"): _fetch_forecast,
}


def fetch_chart_data(cat: str, ds: str, sv: str, qs: dict) -> Optional[ChartData]:
    fn = FETCHERS.get((cat, ds))
    if not fn:
        return None
    try:
        return fn(cat, ds, sv, qs or {})
    except Exception as e:
        logger.warning("OG chart data fetch failed for %s/%s: %s", cat, ds, e)
        return None


# ──────────────────────────────────────────────────────────────────────────
# Renderer — branded card + real line chart
# ──────────────────────────────────────────────────────────────────────────

OG_W, OG_H = 1200, 630

BG_TOP = (15, 23, 42)
BG_BOTTOM = (2, 6, 23)
TEXT_PRIMARY = (243, 244, 246)
TEXT_MUTED = (156, 163, 175)
GRID = (55, 65, 81)

LEFT_W = 460          # text/headline column width
PAD = 56              # outer padding


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


def render_chart_card(data: ChartData) -> bytes:
    """Render a 1200×630 OG card with title/headline on the left and a real
    line chart on the right."""
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

    # Brand mark
    brand_font = _load_font(26, bold=True)
    tag_font = _load_font(16)
    draw.text((PAD, 40), "PARRA MACRO", font=brand_font, fill=TEXT_PRIMARY)
    draw.text((PAD + draw.textlength("PARRA MACRO", font=brand_font) + 14, 48),
              "Macro research & geopolitical risk", font=tag_font, fill=TEXT_MUTED)

    # Accent bar
    accent = data.accent
    draw.rectangle([(PAD - 8, 110), (PAD - 1, 470)], fill=accent)

    # Title (left column, wrap to 2 lines)
    title_font = _load_font(46, bold=True)
    title_lines = _wrap(draw, data.title, title_font, LEFT_W - 20)[:2]
    y = 130
    for line in title_lines:
        draw.text((PAD + 8, y), line, font=title_font, fill=TEXT_PRIMARY)
        y += 56

    # Headline value — the marquee number
    headline_font = _load_font(96, bold=True)
    draw.text((PAD + 8, y + 18), data.headline_value, font=headline_font, fill=accent)

    # Headline label
    label_font = _load_font(20)
    draw.text((PAD + 8, y + 18 + 110), data.headline_label, font=label_font, fill=TEXT_MUTED)

    # ── Chart panel ─────────────────────────────────────────────────────
    chart_x = PAD + LEFT_W + 24
    chart_y = 130
    chart_w = OG_W - chart_x - PAD
    chart_h = 340

    if data.points and len(data.points) >= 2:
        xs = [p[0] for p in data.points]
        ys = [p[1] for p in data.points]
        x_min, x_max = min(xs), max(xs)
        y_min, y_max = min(ys), max(ys)
        if x_max == x_min:
            x_max = x_min + 1
        if y_max == y_min:
            y_max = y_min + 1
        # Pad y range so the line isn't flush with the axis
        y_pad = (y_max - y_min) * 0.12
        y_min -= y_pad
        y_max += y_pad

        def to_px(p):
            x = chart_x + (p[0] - x_min) / (x_max - x_min) * chart_w
            y = chart_y + chart_h - (p[1] - y_min) / (y_max - y_min) * chart_h
            return (x, y)

        # Grid lines (4 horizontal divisions)
        for i in range(5):
            gy = chart_y + chart_h * i / 4
            draw.line([(chart_x, gy), (chart_x + chart_w, gy)], fill=GRID + (130,), width=1)

        # Y axis labels (left of chart)
        ax_font = _load_font(16)
        for i in range(5):
            v = y_max - (y_max - y_min) * i / 4
            label = f"{v:.1f}{data.y_unit}"
            tw = draw.textlength(label, font=ax_font)
            draw.text((chart_x - tw - 8, chart_y + chart_h * i / 4 - 8),
                      label, font=ax_font, fill=TEXT_MUTED)

        # Filled area
        line_pts = [to_px(p) for p in data.points]
        poly = line_pts + [(chart_x + chart_w, chart_y + chart_h),
                           (chart_x, chart_y + chart_h)]
        draw.polygon(poly, fill=accent + (38,))

        # Line
        draw.line(line_pts, fill=accent, width=4, joint="curve")

        # End-point dot
        ex, ey = line_pts[-1]
        draw.ellipse([(ex - 8, ey - 8), (ex + 8, ey + 8)], fill=accent)
        draw.ellipse([(ex - 4, ey - 4), (ex + 4, ey + 4)], fill=TEXT_PRIMARY)

        # X axis labels
        ax_font = _load_font(18)
        if data.x_label_first:
            draw.text((chart_x, chart_y + chart_h + 12),
                      data.x_label_first, font=ax_font, fill=TEXT_MUTED)
        if data.x_label_last:
            tw = draw.textlength(data.x_label_last, font=ax_font)
            draw.text((chart_x + chart_w - tw, chart_y + chart_h + 12),
                      data.x_label_last, font=ax_font, fill=TEXT_MUTED)

    # Subtitle below chart
    sub_font = _load_font(20)
    sub_lines = _wrap(draw, data.subtitle, sub_font, OG_W - PAD * 2)[:1]
    for line in sub_lines:
        draw.text((PAD, OG_H - 130), line, font=sub_font, fill=TEXT_MUTED)

    # Source pill
    src_font = _load_font(18, bold=True)
    src_text = f"SOURCE · {data.source.upper()}"
    tw = draw.textlength(src_text, font=src_font)
    pill_x, pill_y = PAD, OG_H - 78
    draw.rounded_rectangle(
        [(pill_x, pill_y), (pill_x + tw + 32, pill_y + 38)],
        radius=19,
        fill=(255, 255, 255, 14),
        outline=accent,
        width=2,
    )
    draw.text((pill_x + 16, pill_y + 9), src_text, font=src_font, fill=TEXT_PRIMARY)

    # URL
    url_font = _load_font(20)
    url_text = "parramacro.com"
    url_w = draw.textlength(url_text, font=url_font)
    draw.text((OG_W - PAD - url_w, OG_H - 70), url_text, font=url_font, fill=TEXT_MUTED)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
