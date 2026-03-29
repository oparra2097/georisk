import json
import time
import threading
from datetime import datetime
from flask import Blueprint, request, Response, jsonify, stream_with_context
from config import Config

economist_bp = Blueprint('economist', __name__)

# Simple rate limiter: {ip: [timestamps]}
_rate_lock = threading.Lock()
_rate_log = {}
RATE_LIMIT = 20       # requests per window
RATE_WINDOW = 60      # seconds
MAX_MESSAGES = 20      # max conversation length sent to API

SYSTEM_PROMPT = """You are Parra Economist, the AI macro research analyst for Parra Macro (parramacro.com). You are sharp, data-driven, and concise. You specialize in global macro analysis: inflation dynamics, central bank policy, geopolitical risk, commodity markets, trade flows, and sovereign debt.

You have access to real-time tools that pull live data from the Parra Macro platform. Use these tools to ground your analysis in current data. Always cite specific numbers and data points when available. You may call multiple tools in a single turn if the question spans several topics.

Style guidelines:
- Lead with the key insight, then support with data
- Be direct and opinionated — take a stance backed by data
- Use short paragraphs; avoid walls of text
- When discussing risk, reference specific country scores and indicators
- When discussing markets, reference current prices and trends
- Flag when data may be stale or when uncertainty is high
- Use markdown formatting: **bold** for emphasis, bullet points for lists
- Keep responses focused — aim for 150-300 words unless the question warrants more"""

TOOLS = [
    {
        "name": "get_hotspots",
        "description": "Get countries with the highest geopolitical risk scores (above threshold of 70). Returns top risk-scored countries with composite scores, indicators, and recent news counts.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_country_risk",
        "description": "Get detailed geopolitical risk data for a specific country. Returns composite score, base score, news score, 6 indicator breakdown (political stability, military conflict, economic sanctions, protests/civil unrest, terrorism, diplomatic tensions), avg news tone, and headline count.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country_code": {
                    "type": "string",
                    "description": "ISO2 country code (e.g. 'US', 'CN', 'RU', 'UA', 'IL')"
                }
            },
            "required": ["country_code"]
        }
    },
    {
        "name": "get_headlines",
        "description": "Get recent news headlines for a specific country or global geopolitical headlines. Headlines come from GDELT, NewsAPI, NewsData, and GNews with a 72-hour rolling window.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country_code": {
                    "type": "string",
                    "description": "ISO2 country code, or 'GLOBAL' for worldwide geopolitical headlines"
                }
            },
            "required": ["country_code"]
        }
    },
    {
        "name": "get_market_snapshot",
        "description": "Get live market data including major indices (S&P 500, NASDAQ, Dow), currencies (DXY, EUR/USD, GBP/USD, USD/JPY), commodities (Gold, Oil, Natural Gas, Copper), bonds (US 10Y, US 2Y), and crypto (Bitcoin, Ethereum). Returns current prices, daily change, and percent change.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_cpi_data",
        "description": "Get Consumer Price Index (inflation) data for a region. Returns monthly time series with index values and year-over-year change percentages.",
        "input_schema": {
            "type": "object",
            "properties": {
                "region": {
                    "type": "string",
                    "enum": ["us", "uk", "eu"],
                    "description": "Region: 'us' for US CPI (BLS), 'uk' for UK CPI (ONS), 'eu' for EU HICP (Eurostat)"
                }
            },
            "required": ["region"]
        }
    },
    {
        "name": "get_weo_indicator",
        "description": "Get IMF World Economic Outlook data for a macroeconomic indicator across countries. Available indicators: 'NGDP_RPCH' (real GDP growth), 'PCPIPCH' (inflation rate), 'LUR' (unemployment rate), 'GGXWDG_NGDP' (govt debt % GDP), 'BCA_NGDPD' (current account % GDP).",
        "input_schema": {
            "type": "object",
            "properties": {
                "indicator": {
                    "type": "string",
                    "enum": ["NGDP_RPCH", "PCPIPCH", "LUR", "GGXWDG_NGDP", "BCA_NGDPD"],
                    "description": "WEO indicator code"
                }
            },
            "required": ["indicator"]
        }
    },
    {
        "name": "get_commodity_forecasts",
        "description": "Get commodity price forecasts including scenario analysis (bull/base/bear cases) with weighted average projections. Covers oil, natural gas, agriculture, and metals with quarterly forecasts and historical data.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_central_bank_reserves",
        "description": "Get IMF COFER data on central bank reserves by country — total reserves, FX reserves, and gold reserves over time. Useful for analyzing reserve accumulation trends and de-dollarization.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_all_risk_scores",
        "description": "Get a summary of risk scores for all tracked countries. Returns country name, composite score, and trend data. Use this for broad overviews or comparisons across regions.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]


def _check_rate_limit(ip):
    """Return True if request is allowed, False if rate-limited."""
    now = time.time()
    with _rate_lock:
        timestamps = _rate_log.get(ip, [])
        timestamps = [t for t in timestamps if now - t < RATE_WINDOW]
        if len(timestamps) >= RATE_LIMIT:
            return False
        timestamps.append(now)
        _rate_log[ip] = timestamps
        return True


def _execute_tool(name, input_data):
    """Execute a tool call and return the result as a string."""
    from backend.cache.store import store
    try:
        if name == "get_hotspots":
            hotspots = store.get_hotspots(Config.HOTSPOT_THRESHOLD)
            hotspots.sort(key=lambda x: x.composite_score, reverse=True)
            return json.dumps([h.to_dict() for h in hotspots[:15]])

        elif name == "get_country_risk":
            code = input_data.get("country_code", "").upper()
            risk = store.get_country(code)
            if not risk:
                return json.dumps({"error": f"No data for country code '{code}'"})
            return json.dumps(risk.to_dict())

        elif name == "get_headlines":
            code = input_data.get("country_code", "").upper()
            if code == "GLOBAL":
                articles = store.get_global_headlines()
            else:
                articles = store.get_headlines(code)
            return json.dumps([a.to_dict() for a in articles[:20]])

        elif name == "get_market_snapshot":
            from backend.data_sources.market_data import get_market_data
            data = get_market_data()
            return json.dumps(data)

        elif name == "get_cpi_data":
            region = input_data.get("region", "us")
            if region == "us":
                from backend.data_sources.bls_cpi import get_bls_cpi_data
                data = get_bls_cpi_data()
            elif region == "uk":
                from backend.data_sources.ons_cpi import get_ons_cpi_data
                data = get_ons_cpi_data()
            elif region == "eu":
                from backend.data_sources.eurostat_hicp import get_eurostat_cpi_data
                data = get_eurostat_cpi_data()
            else:
                return json.dumps({"error": f"Unknown region '{region}'"})
            # Trim to last 24 data points to save tokens
            for key in data.get("series", {}):
                if isinstance(data["series"][key], list):
                    data["series"][key] = data["series"][key][-24:]
            return json.dumps(data)

        elif name == "get_weo_indicator":
            from backend.data_sources.imf_weo import get_weo_data
            indicator = input_data.get("indicator", "NGDP_RPCH")
            data = get_weo_data(indicator)
            return json.dumps(data)

        elif name == "get_commodity_forecasts":
            from backend.data_sources.commodities_forecast import get_forecast_data
            data = get_forecast_data()
            return json.dumps(data)

        elif name == "get_central_bank_reserves":
            from backend.data_sources.imf_cofer import get_cofer_data
            data = get_cofer_data()
            # Trim to recent years to save tokens
            if "years" in data and len(data["years"]) > 10:
                trim = len(data["years"]) - 10
                data["years"] = data["years"][trim:]
                for c in data.get("countries", []):
                    for field in ["total_reserves", "fx_reserves", "gold_reserves"]:
                        if field in c and isinstance(c[field], list):
                            c[field] = c[field][trim:]
            return json.dumps(data)

        elif name == "get_all_risk_scores":
            all_scores = store.get_all_scores()
            summary = []
            for code, risk in sorted(all_scores.items(),
                                     key=lambda x: x[1].composite_score,
                                     reverse=True):
                summary.append({
                    "code": code,
                    "name": risk.country_name,
                    "composite": round(risk.composite_score, 1),
                    "trend": [round(t, 1) for t in risk.trend[-5:]] if risk.trend else []
                })
            return json.dumps(summary[:50])

        return json.dumps({"error": f"Unknown tool '{name}'"})

    except Exception as e:
        return json.dumps({"error": str(e)})


@economist_bp.route('/economist/chat', methods=['POST'])
def economist_chat():
    """Stream a response from Parra Economist using Claude with tool use."""
    if not Config.ANTHROPIC_API_KEY:
        return jsonify({"error": "Anthropic API key not configured"}), 503

    ip = request.remote_addr or "unknown"
    if not _check_rate_limit(ip):
        return jsonify({"error": "Rate limit exceeded. Please wait a moment."}), 429

    data = request.get_json()
    if not data or "messages" not in data:
        return jsonify({"error": "Missing 'messages' in request body"}), 400

    messages = data["messages"][-MAX_MESSAGES:]

    # Validate message format
    for msg in messages:
        if msg.get("role") not in ("user", "assistant"):
            return jsonify({"error": "Invalid message role"}), 400

    from anthropic import Anthropic
    client = Anthropic(api_key=Config.ANTHROPIC_API_KEY)

    def generate():
        try:
            current_messages = list(messages)
            rounds = 0

            # Tool-use loop: Claude may call tools, then we feed results back
            while rounds < 5:
                rounds += 1

                # Non-streaming call for tool-use rounds (tools need full response)
                # Final text round uses streaming for real-time UX
                response = client.messages.create(
                    model=Config.ANTHROPIC_MODEL,
                    max_tokens=2048,
                    system=SYSTEM_PROMPT,
                    tools=TOOLS,
                    messages=current_messages,
                )

                tool_calls = [b for b in response.content if b.type == "tool_use"]

                if not tool_calls:
                    # No tool calls — this is the final answer
                    # Re-run as streaming for real-time token delivery
                    with client.messages.stream(
                        model=Config.ANTHROPIC_MODEL,
                        max_tokens=2048,
                        system=SYSTEM_PROMPT,
                        tools=TOOLS,
                        messages=current_messages,
                    ) as stream:
                        for text in stream.text_stream:
                            yield f"data: {json.dumps({'type': 'text', 'content': text})}\n\n"
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    return

                # Execute tool calls and build results
                assistant_content = []
                tool_results = []
                for block in response.content:
                    if block.type == "text":
                        assistant_content.append({"type": "text", "text": block.text})
                    elif block.type == "tool_use":
                        assistant_content.append({
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": block.input
                        })
                        yield f"data: {json.dumps({'type': 'status', 'content': f'Fetching {block.name.replace(\"_\", \" \")}...'})}\n\n"
                        result = _execute_tool(block.name, block.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result
                        })

                current_messages.append({"role": "assistant", "content": assistant_content})
                current_messages.append({"role": "user", "content": tool_results})

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )
