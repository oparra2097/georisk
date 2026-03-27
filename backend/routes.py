from io import BytesIO
from datetime import datetime
from flask import Blueprint, jsonify, send_file, request
from backend.cache.store import store
from backend.data_sources.market_data import get_market_data, get_market_history
from backend.data_sources.imf_cofer import get_cofer_data
from backend.data_sources.bls_cpi import get_bls_cpi_data, get_bls_components, clear_bls_caches
from backend.data_sources.ons_cpi import get_ons_cpi_data, get_ons_components
from backend.data_sources.substack_feed import get_substack_posts
from backend.data_sources.commodities_forecast import get_forecast_data
from backend.cache.database import get_country_history, get_all_history, detect_anomalies, get_score_count
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


@api_bp.route('/markets/history')
def get_markets_history():
    """Return historical price data for a symbol and time period."""
    symbol = request.args.get('symbol', '')
    period = request.args.get('period', '1mo')

    if not symbol:
        return jsonify({'error': 'symbol parameter required'}), 400

    valid_periods = ['1d', '5d', '1mo', '1y', '5y', '10y']
    if period not in valid_periods:
        return jsonify({'error': f'Invalid period. Use: {", ".join(valid_periods)}'}), 400

    data = get_market_history(symbol, period)
    if data is None:
        return jsonify({'error': 'No data available'}), 404

    return jsonify(data)


@api_bp.route('/cofer')
def get_cofer():
    """Return central bank reserves data (cached 24 hours)."""
    data = get_cofer_data()
    return jsonify(data)


@api_bp.route('/cpi/us')
def get_us_cpi():
    """Return US CPI data from BLS (cached 24 hours)."""
    data = get_bls_cpi_data()
    return jsonify(data)


@api_bp.route('/cpi/uk')
def get_uk_cpi():
    """Return UK CPI data from ONS (cached 24 hours)."""
    data = get_ons_cpi_data()
    return jsonify(data)


@api_bp.route('/cpi/us/components')
def get_us_cpi_components():
    """Return US CPI component breakdown from BLS (cached 24 hours)."""
    data = get_bls_components()
    return jsonify(data)


@api_bp.route('/cpi/uk/components')
def get_uk_cpi_components():
    """Return UK CPI component breakdown from ONS (cached 24 hours)."""
    data = get_ons_components()
    return jsonify(data)


@api_bp.route('/cpi/us/refresh', methods=['POST'])
def refresh_us_cpi():
    """Clear BLS caches and force re-fetch on next request."""
    clear_bls_caches()
    return jsonify({'status': 'ok', 'message': 'BLS CPI caches cleared'})


@api_bp.route('/cpi/us/export')
def export_us_cpi_excel():
    """Generate Excel file with US CPI overview data."""
    return _export_cpi_excel(get_bls_cpi_data(), 'US CPI', 'us_cpi_data')


@api_bp.route('/cpi/uk/export')
def export_uk_cpi_excel():
    """Generate Excel file with UK CPI overview data."""
    return _export_cpi_excel(get_ons_cpi_data(), 'UK CPI', 'uk_cpi_data')


@api_bp.route('/cpi/us/components/export')
def export_us_components_excel():
    """Generate Excel file with US CPI component breakdown."""
    return _export_cpi_excel(get_bls_components(), 'US CPI Components', 'us_cpi_components')


@api_bp.route('/cpi/uk/components/export')
def export_uk_components_excel():
    """Generate Excel file with UK CPI component breakdown."""
    return _export_cpi_excel(get_ons_components(), 'UK CPI Components', 'uk_cpi_components')


def _export_cpi_excel(data, title, filename_prefix):
    """Shared helper: export CPI data (overview or components) to Excel."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    series = data.get('series', {})
    categories = data.get('categories', {})
    meta = data.get('meta', {})

    header_font = Font(bold=True, color='FFFFFF', size=11)
    header_fill = PatternFill(start_color='1F2937', end_color='1F2937', fill_type='solid')
    thin_border = Border(
        left=Side(style='thin', color='D1D5DB'),
        right=Side(style='thin', color='D1D5DB'),
        top=Side(style='thin', color='D1D5DB'),
        bottom=Side(style='thin', color='D1D5DB'),
    )

    wb = Workbook()
    first_sheet = True

    for key, label in categories.items():
        points = series.get(key, [])
        if not points:
            continue

        if first_sheet:
            ws = wb.active
            ws.title = label[:31]
            first_sheet = False
        else:
            ws = wb.create_sheet(label[:31])

        # Title row
        ws.cell(row=1, column=1, value=f'{title}: {label}')
        ws.cell(row=1, column=1).font = Font(bold=True, size=13)
        if meta.get('source'):
            ws.cell(row=2, column=1, value=meta['source'])
            ws.cell(row=2, column=1).font = Font(italic=True, size=9, color='6B7280')

        # Header row
        row = 4
        headers = ['Date', 'Index Value', 'YoY Change (%)']
        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=row, column=col, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal='center')
            cell.border = thin_border

        # Data rows (most recent first)
        for pt in reversed(points):
            row += 1
            ws.cell(row=row, column=1, value=pt.get('date', '')).border = thin_border
            cell_val = ws.cell(row=row, column=2, value=pt.get('value'))
            cell_val.number_format = '#,##0.000'
            cell_val.alignment = Alignment(horizontal='center')
            cell_val.border = thin_border
            yoy = pt.get('yoy_change')
            cell_yoy = ws.cell(row=row, column=3, value=yoy)
            cell_yoy.number_format = '0.00'
            cell_yoy.alignment = Alignment(horizontal='center')
            cell_yoy.border = thin_border

        # Auto-width
        ws.column_dimensions['A'].width = 12
        ws.column_dimensions['B'].width = 14
        ws.column_dimensions['C'].width = 16
        ws.freeze_panes = 'A5'

    if first_sheet:
        # No data at all — create empty sheet with message
        ws = wb.active
        ws.title = 'No Data'
        ws.cell(row=1, column=1, value='No data available. Check API key configuration.')

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    today = datetime.utcnow().strftime('%Y-%m-%d')
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'{filename_prefix}_{today}.xlsx'
    )


@api_bp.route('/markets/export')
def export_markets_excel():
    """Generate Excel file with current market data snapshot."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    data = get_market_data()
    markets = data.get('markets', [])

    header_font = Font(bold=True, color='FFFFFF', size=11)
    header_fill = PatternFill(start_color='1F2937', end_color='1F2937', fill_type='solid')
    green_font = Font(color='10B981')
    red_font = Font(color='EF4444')
    thin_border = Border(
        left=Side(style='thin', color='D1D5DB'),
        right=Side(style='thin', color='D1D5DB'),
        top=Side(style='thin', color='D1D5DB'),
        bottom=Side(style='thin', color='D1D5DB'),
    )

    wb = Workbook()
    ws = wb.active
    ws.title = 'Market Data'

    # Title
    ws.cell(row=1, column=1, value='Parra Macro — Market Data Snapshot')
    ws.cell(row=1, column=1).font = Font(bold=True, size=13)
    ws.cell(row=2, column=1, value=f'Generated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}')
    ws.cell(row=2, column=1).font = Font(italic=True, size=9, color='6B7280')

    # Headers
    headers = ['Name', 'Symbol', 'Type', 'Price', 'Change', 'Change %']
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=4, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')
        cell.border = thin_border

    # Data rows
    for i, m in enumerate(markets):
        row = 5 + i
        ws.cell(row=row, column=1, value=m.get('name', '')).border = thin_border
        ws.cell(row=row, column=2, value=m.get('symbol', '')).border = thin_border
        ws.cell(row=row, column=3, value=m.get('type', '')).border = thin_border

        price_cell = ws.cell(row=row, column=4, value=m.get('price'))
        price_cell.number_format = '#,##0.00'
        price_cell.alignment = Alignment(horizontal='center')
        price_cell.border = thin_border

        change = m.get('change')
        change_cell = ws.cell(row=row, column=5, value=change)
        change_cell.number_format = '+#,##0.00;-#,##0.00;0.00'
        change_cell.alignment = Alignment(horizontal='center')
        change_cell.border = thin_border
        if change is not None:
            change_cell.font = green_font if change >= 0 else red_font

        pct = m.get('change_pct')
        pct_cell = ws.cell(row=row, column=6, value=pct)
        pct_cell.number_format = '+0.00%;-0.00%;0.00%'
        pct_cell.alignment = Alignment(horizontal='center')
        pct_cell.border = thin_border
        if pct is not None:
            pct_cell.font = green_font if pct >= 0 else red_font
            # Store as decimal for Excel percentage format
            pct_cell.value = pct / 100.0

    # Auto-width
    ws.column_dimensions['A'].width = 24
    ws.column_dimensions['B'].width = 14
    ws.column_dimensions['C'].width = 12
    ws.column_dimensions['D'].width = 14
    ws.column_dimensions['E'].width = 12
    ws.column_dimensions['F'].width = 12
    ws.freeze_panes = 'A5'

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    today = datetime.utcnow().strftime('%Y-%m-%d')
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'market_data_{today}.xlsx'
    )


@api_bp.route('/forecasts')
def get_forecasts():
    """Return commodities forecast data (cached 24 hours)."""
    data = get_forecast_data()
    return jsonify(data)


@api_bp.route('/forecasts/export')
def export_forecasts_excel():
    """Generate Excel file with commodity forecast + historical data."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    data = get_forecast_data()
    groups = data.get('groups', {})
    time_ctx = data.get('time_context', {})
    labels = time_ctx.get('labels', [])
    label_types = time_ctx.get('label_types', [])
    year_end_label = time_ctx.get('year_end_label', 'FY Avg')
    # Per-group scenario config — extracted per group below

    # Styles
    header_font = Font(bold=True, color='FFFFFF', size=11)
    header_fill = PatternFill(start_color='1F2937', end_color='1F2937', fill_type='solid')
    section_font = Font(bold=True, color='FFFFFF', size=11)
    section_fill = PatternFill(start_color='374151', end_color='374151', fill_type='solid')
    forecast_fill = PatternFill(start_color='EFF6FF', end_color='EFF6FF', fill_type='solid')
    current_fill = PatternFill(start_color='FEF3C7', end_color='FEF3C7', fill_type='solid')
    num_fmt = '#,##0.00'
    thin_border = Border(
        left=Side(style='thin', color='D1D5DB'),
        right=Side(style='thin', color='D1D5DB'),
        top=Side(style='thin', color='D1D5DB'),
        bottom=Side(style='thin', color='D1D5DB'),
    )

    wb = Workbook()
    first_sheet = True

    for group_name in ['Oil & Gas', 'Agriculture', 'Metals']:
        group = groups.get(group_name)
        if not group:
            continue

        # Per-group scenario config
        scenario_order = group.get('scenario_order', ['Actual', 'Weighted Avg'])
        scenario_weights = group.get('scenario_weights', {})
        scenario_labels = group.get('scenario_labels', {})
        commodities = group.get('commodities', {})

        for comm_name, comm_data in commodities.items():
            # Create a sheet per commodity
            if first_sheet:
                ws = wb.active
                ws.title = comm_name[:31]
                first_sheet = False
            else:
                ws = wb.create_sheet(comm_name[:31])

            unit = comm_data.get('unit', '')
            scenarios = comm_data.get('scenarios', {})
            historical = comm_data.get('historical', [])
            latest = comm_data.get('latest_close')

            row = 1

            # ── Title ──
            ws.cell(row=row, column=1, value=f'{comm_name} — {unit}')
            ws.cell(row=row, column=1).font = Font(bold=True, size=14)
            row += 1
            ws.cell(row=row, column=1, value=f'Group: {group_name}')
            row += 1
            if latest:
                ws.cell(row=row, column=1, value=f'Latest Close: {latest} {unit}')
            row += 1
            ws.cell(row=row, column=1, value=f'As of: {time_ctx.get("today", "")}')
            row += 2

            # ── Scenario Forecast Table ──
            ws.cell(row=row, column=1, value='SCENARIO FORECASTS')
            ws.cell(row=row, column=1).font = section_font
            ws.cell(row=row, column=1).fill = section_fill
            for ci in range(2, len(labels) + 3):
                ws.cell(row=row, column=ci).fill = section_fill
            row += 1

            # Header row
            fc_headers = ['Scenario'] + labels + [year_end_label]
            for ci, h in enumerate(fc_headers, 1):
                cell = ws.cell(row=row, column=ci, value=h)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center')
                cell.border = thin_border
            row += 1

            # Data rows
            for sc in scenario_order:
                sc_data = scenarios.get(sc)
                if not sc_data:
                    continue
                ws.cell(row=row, column=1, value=sc).border = thin_border
                weight = scenario_weights.get(sc)
                if weight:
                    ws.cell(row=row, column=1, value=f'{sc} ({weight*100:.0f}%)')
                ws.cell(row=row, column=1).border = thin_border

                for ci, lbl in enumerate(labels, 2):
                    val = sc_data.get(lbl)
                    cell = ws.cell(row=row, column=ci, value=val)
                    cell.number_format = num_fmt
                    cell.alignment = Alignment(horizontal='center')
                    cell.border = thin_border
                    lt = label_types[ci - 2] if (ci - 2) < len(label_types) else ''
                    if lt == 'forecast':
                        cell.fill = forecast_fill
                    elif lt == 'current_q':
                        cell.fill = current_fill

                # FY column
                fy_val = sc_data.get('FY')
                cell = ws.cell(row=row, column=len(labels) + 2, value=fy_val)
                cell.number_format = num_fmt
                cell.alignment = Alignment(horizontal='center')
                cell.border = thin_border
                row += 1

            row += 1

            # ── Scenario Descriptions ──
            for sc in scenario_order:
                if sc in ('Actual', 'Weighted Avg'):
                    continue
                desc = scenario_labels.get(sc, '')
                w = scenario_weights.get(sc)
                w_str = f' ({w*100:.0f}%)' if w else ''
                ws.cell(row=row, column=1, value=f'{sc}{w_str}: {desc}')
                ws.cell(row=row, column=1).font = Font(italic=True, size=9, color='6B7280')
                row += 1

            row += 2

            # ── Historical Quarterly Data ──
            if historical:
                ws.cell(row=row, column=1, value='HISTORICAL QUARTERLY AVERAGES')
                ws.cell(row=row, column=1).font = section_font
                ws.cell(row=row, column=1).fill = section_fill
                for ci in range(2, 4):
                    ws.cell(row=row, column=ci).fill = section_fill
                row += 1

                hist_headers = ['Period', f'Avg Price ({unit})']
                for ci, h in enumerate(hist_headers, 1):
                    cell = ws.cell(row=row, column=ci, value=h)
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = Alignment(horizontal='center')
                    cell.border = thin_border
                row += 1

                # Reverse so most recent is first
                for rec in reversed(historical):
                    ws.cell(row=row, column=1, value=rec['label']).border = thin_border
                    cell = ws.cell(row=row, column=2, value=rec['avg_price'])
                    cell.number_format = num_fmt
                    cell.alignment = Alignment(horizontal='center')
                    cell.border = thin_border
                    row += 1

            # Auto-width columns
            for col in ws.columns:
                max_len = 0
                col_letter = col[0].column_letter
                for cell in col:
                    try:
                        if cell.value:
                            max_len = max(max_len, len(str(cell.value)))
                    except Exception:
                        pass
                ws.column_dimensions[col_letter].width = min(max_len + 3, 30)

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    today = datetime.utcnow().strftime('%Y-%m-%d')
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'parramacro_commodity_forecasts_{today}.xlsx'
    )


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


@api_bp.route('/substack')
def get_substack():
    """Return Substack posts from RSS feed (cached 1 hour)."""
    posts = get_substack_posts()
    return jsonify({'posts': posts})


@api_bp.route('/history')
def get_history():
    """Return daily historical snapshots of country scores (from SQLite).
    Optional query params: country=US, days=30
    """
    country_filter = request.args.get('country', '').upper()
    days_limit = int(request.args.get('days', 90))

    if country_filter:
        series = get_country_history(country_filter, days=days_limit)
        return jsonify({'country': country_filter, 'series': series})
    else:
        history = get_all_history(days=days_limit)
        sorted_dates = sorted(history.keys())
        return jsonify({'dates': sorted_dates, 'snapshots': history})


@api_bp.route('/history/<country_code>')
def get_country_history_endpoint(country_code):
    """Return full time series for a single country with all indicators."""
    code = country_code.upper()
    days = int(request.args.get('days', 90))
    series = get_country_history(code, days=days)
    if not series:
        return jsonify({'country': code, 'days': days, 'data_points': 0, 'series': []})
    return jsonify({
        'country': code,
        'days': days,
        'data_points': len(series),
        'series': series
    })


@api_bp.route('/anomalies')
def get_anomalies():
    """Return countries with significant score changes (>10 points)."""
    threshold = float(request.args.get('threshold', 10.0))
    anomalies = detect_anomalies(threshold_delta=threshold)
    return jsonify({'anomalies': anomalies})


@api_bp.route('/status')
def get_status():
    """Return system health info."""
    import os
    from backend.cache.database import get_history_dates
    last_refresh = store.get_last_refresh()
    scores_file_exists = os.path.exists(Config.SCORES_FILE)
    db_exists = os.path.exists(Config.DB_FILE)

    history_days = 0
    try:
        history_days = len(get_history_dates())
    except Exception:
        pass

    return jsonify({
        'status': 'ok',
        'last_refresh': last_refresh.isoformat() if last_refresh else None,
        'countries_tracked': store.country_count(),
        'hotspot_count': len(store.get_hotspots(Config.HOTSPOT_THRESHOLD)),
        'persistence': {
            'scores_file': scores_file_exists,
            'database': db_exists,
            'history_days': history_days,
            'total_score_rows': get_score_count(),
        }
    })
