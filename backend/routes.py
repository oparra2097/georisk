from io import BytesIO
from datetime import datetime
from flask import Blueprint, jsonify, send_file, request
from backend.cache.store import store
from backend.data_sources.market_data import get_market_data
from backend.data_sources.imf_cofer import get_cofer_data
from backend.cache.persistence import load_history
from config import Config

api_bp = Blueprint('api', __name__)


@api_bp.route('/scores')
def get_all_scores():
    """Return risk scores for all countries."""
    all_scores = store.get_all_scores()
    result = {}
    for code, risk in all_scores.items():
        result[code] = risk.to_dict()
    return jsonify(result)


@api_bp.route('/scores/<country_code>')
def get_country_score(country_code):
    """Return detailed score for a single country."""
    code = country_code.upper()
    risk = store.get_country(code)
    if not risk:
        return jsonify({'error': 'Country not found'}), 404
    return jsonify(risk.to_dict())


@api_bp.route('/headlines/<country_code>')
def get_country_headlines(country_code):
    """Return recent headlines for a country."""
    code = country_code.upper()
    if code == 'GLOBAL':
        articles = store.get_global_headlines()
    else:
        articles = store.get_headlines(code)
    return jsonify({
        'articles': [a.to_dict() for a in articles]
    })


@api_bp.route('/headlines/global')
def get_global_headlines():
    """Return global geopolitical headlines."""
    articles = store.get_global_headlines()
    return jsonify({
        'articles': [a.to_dict() for a in articles]
    })


@api_bp.route('/hotspots')
def get_hotspots():
    """Return countries with risk score above threshold."""
    threshold = Config.HOTSPOT_THRESHOLD
    hotspots = store.get_hotspots(threshold)
    hotspots.sort(key=lambda x: x.composite_score, reverse=True)
    return jsonify({
        'hotspots': [h.to_dict() for h in hotspots]
    })


@api_bp.route('/export')
def export_excel():
    """Generate and return an Excel file with all country risk scores."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    all_scores = store.get_all_scores()
    rows = sorted(all_scores.values(), key=lambda r: r.composite_score, reverse=True)

    wb = Workbook()
    ws = wb.active
    ws.title = 'GeoRisk Scores'

    # Header row
    headers = [
        'Country', 'Code', 'Composite Score',
        'Political Stability', 'Military Conflict', 'Economic Sanctions',
        'Protests/Civil Unrest', 'Terrorism', 'Diplomatic Tensions',
        'Avg Tone', 'GDELT Events', 'Updated At'
    ]
    header_font = Font(bold=True, color='FFFFFF', size=11)
    header_fill = PatternFill(start_color='1F2937', end_color='1F2937', fill_type='solid')
    thin_border = Border(
        bottom=Side(style='thin', color='374151')
    )

    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')

    # Data rows
    for row_idx, risk in enumerate(rows, 2):
        ind = risk.indicators
        ws.cell(row=row_idx, column=1, value=risk.country_name)
        ws.cell(row=row_idx, column=2, value=risk.country_code)
        ws.cell(row=row_idx, column=3, value=round(risk.composite_score, 1))
        ws.cell(row=row_idx, column=4, value=round(ind.political_stability, 1))
        ws.cell(row=row_idx, column=5, value=round(ind.military_conflict, 1))
        ws.cell(row=row_idx, column=6, value=round(ind.economic_sanctions, 1))
        ws.cell(row=row_idx, column=7, value=round(ind.protests_civil_unrest, 1))
        ws.cell(row=row_idx, column=8, value=round(ind.terrorism, 1))
        ws.cell(row=row_idx, column=9, value=round(ind.diplomatic_tensions, 1))
        ws.cell(row=row_idx, column=10, value=round(risk.avg_tone, 2))
        ws.cell(row=row_idx, column=11, value=risk.gdelt_event_count)
        ws.cell(row=row_idx, column=12, value=risk.updated_at.strftime('%Y-%m-%d %H:%M') if risk.updated_at else '')

        # Color composite score cell based on risk level
        score = risk.composite_score
        if score >= 70:
            ws.cell(row=row_idx, column=3).fill = PatternFill(start_color='FEE2E2', end_color='FEE2E2', fill_type='solid')
        elif score >= 40:
            ws.cell(row=row_idx, column=3).fill = PatternFill(start_color='FEF3C7', end_color='FEF3C7', fill_type='solid')

    # Auto-width columns
    for col in range(1, len(headers) + 1):
        max_len = len(headers[col - 1])
        for row in range(2, min(len(rows) + 2, 50)):
            val = ws.cell(row=row, column=col).value
            if val:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[ws.cell(row=1, column=col).column_letter].width = max_len + 3

    # Freeze header row
    ws.freeze_panes = 'A2'

    # Write to bytes
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    today = datetime.utcnow().strftime('%Y-%m-%d')
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'georisk_scores_{today}.xlsx'
    )


@api_bp.route('/markets')
def get_markets():
    """Return live market data (cached 5 minutes)."""
    data = get_market_data()
    return jsonify(data)


@api_bp.route('/cofer')
def get_cofer():
    """Return central bank reserves data (cached 24 hours)."""
    data = get_cofer_data()
    return jsonify(data)


@api_bp.route('/cofer/export')
def export_reserves_excel():
    """Generate Excel file with all reserves data."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment

    data = get_cofer_data()
    years = data.get('years', [])
    countries = data.get('countries', [])

    wb = Workbook()

    # Sheet 1: Total Reserves
    ws1 = wb.active
    ws1.title = 'Total Reserves'
    _write_reserves_sheet(ws1, years, countries, 'total_reserves')

    # Sheet 2: FX Reserves
    ws2 = wb.create_sheet('FX Reserves')
    _write_reserves_sheet(ws2, years, countries, 'fx_reserves')

    # Sheet 3: Gold Reserves
    ws3 = wb.create_sheet('Gold Reserves')
    _write_reserves_sheet(ws3, years, countries, 'gold_reserves')

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    today = datetime.utcnow().strftime('%Y-%m-%d')
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'central_bank_reserves_{today}.xlsx'
    )


def _write_reserves_sheet(ws, years, countries, field):
    """Write a reserves sheet with header + data rows."""
    from openpyxl.styles import Font, PatternFill, Alignment

    header_font = Font(bold=True, color='FFFFFF', size=11)
    header_fill = PatternFill(start_color='1F2937', end_color='1F2937', fill_type='solid')

    # Header
    headers = ['Country', 'ISO3'] + years
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')

    # Data
    for row_idx, c in enumerate(countries, 2):
        ws.cell(row=row_idx, column=1, value=c['name'])
        ws.cell(row=row_idx, column=2, value=c['iso3'])
        values = c.get(field, [])
        for i, val in enumerate(values):
            if val is not None:
                ws.cell(row=row_idx, column=3 + i, value=val)

    # Auto-width first two columns
    ws.column_dimensions['A'].width = 22
    ws.column_dimensions['B'].width = 8
    for i in range(len(years)):
        col_letter = ws.cell(row=1, column=3 + i).column_letter
        ws.column_dimensions[col_letter].width = 12

    ws.freeze_panes = 'C2'


@api_bp.route('/history')
def get_history():
    """Return daily historical snapshots of country scores.
    Optional query params: country=US, days=30
    """
    history = load_history()
    country_filter = request.args.get('country', '').upper()
    days_limit = int(request.args.get('days', 90))

    # Sort dates and limit
    sorted_dates = sorted(history.keys())[-days_limit:]

    if country_filter:
        # Return single country time series
        series = []
        for d in sorted_dates:
            snap = history.get(d, {})
            entry = snap.get(country_filter)
            if entry:
                series.append({'date': d, **entry})
        return jsonify({'country': country_filter, 'series': series})
    else:
        # Return all dates (just composite scores to keep response small)
        result = {}
        for d in sorted_dates:
            snap = history.get(d, {})
            result[d] = {code: data.get('composite', 0) for code, data in snap.items()}
        return jsonify({'dates': sorted_dates, 'snapshots': result})


@api_bp.route('/status')
def get_status():
    """Return system health info."""
    import os
    last_refresh = store.get_last_refresh()
    scores_file_exists = os.path.exists(Config.SCORES_FILE)
    history_file_exists = os.path.exists(Config.HISTORY_FILE)

    history_days = 0
    if history_file_exists:
        try:
            history = load_history()
            history_days = len(history)
        except Exception:
            pass

    return jsonify({
        'status': 'ok',
        'last_refresh': last_refresh.isoformat() if last_refresh else None,
        'countries_tracked': store.country_count(),
        'hotspot_count': len(store.get_hotspots(Config.HOTSPOT_THRESHOLD)),
        'persistence': {
            'scores_file': scores_file_exists,
            'history_file': history_file_exists,
            'history_days': history_days,
        }
    })
