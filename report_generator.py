"""
Generate client-facing validation reports in CSV, Excel (colored), and PDF formats.
"""
import csv
import os
from collections import defaultdict
from datetime import datetime

import xlsxwriter
from fpdf import FPDF

# Month names for display
MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

# Color scheme
COLORS = {
    'pass_bg': '#C6EFCE',
    'pass_text': '#006100',
    'warn_bg': '#FFEB9C',
    'warn_text': '#9C6500',
    'fail_bg': '#FFC7CE',
    'fail_text': '#9C0006',
    'header_bg': '#4472C4',
    'header_text': '#FFFFFF',
    'subheader_bg': '#D6E4F0',
    'na_bg': '#F2F2F2',
}


def determine_status(result):
    """Determine PASS/WARNING/FAIL for a month's results."""
    if result.get('error'):
        return 'FAIL'

    # Internal validation check
    if result.get('classes_mismatched', 0) > 0:
        return 'FAIL'

    supa = result.get('supabase')
    if not supa or supa.get('status') == 'NO_SUPABASE_DATA':
        return 'N/A'

    key_rate = supa.get('key_match_rate', 0)
    exact_rate = supa.get('exact_match_rate', 0)

    if key_rate >= 99.5 and exact_rate >= 99.0:
        return 'PASS'
    elif key_rate >= 95.0 and exact_rate >= 95.0:
        return 'WARNING'
    else:
        return 'FAIL'


def get_period_label(year, month):
    """Get human-readable period label."""
    return f"{MONTH_NAMES.get(month, str(month))} {year}"


def classify_root_cause(error):
    """Classify an error record into a root cause category.

    Returns (root_cause, notes) tuple.
    """
    etype = error.get('Error_Type', '')
    field = error.get('Field', '')
    cls = error.get('Class', '').upper()
    page = int(error.get('Page', 0) or 0)

    if etype == 'MISSING_IN_SUPABASE':
        if cls == 'MEZCAL' and page >= 340:
            return ('Page boundary residue', 'Residual text near end of document assigned wrong class')
        return ('Word order difference', 'Our extraction matches PDF; Supabase has words rearranged')

    if etype == 'MISSING_IN_CSV':
        return ('Word order difference', 'Supabase has words rearranged vs PDF source')

    if etype == 'VALUE_MISMATCH':
        if field == '750ml':
            return ('750ml/Traveler column mapping', 'Supabase curr_month_750ml contains our 750ml_Trav value')
        if field in ('375ml', '200ml', '100ml', '50ml'):
            return ('Column shift cascade', 'Downstream effect of 750ml column shift')
        return ('OCR/source data difference', 'Needs individual investigation')

    return ('Unknown', '')


# ============================================================
# CSV Report
# ============================================================

def generate_consolidated_csv(all_results, output_path):
    """Generate consolidated multi-month validation CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Period', 'File_ID', 'Total_Rows', 'Total_Classes',
            'Internal_Validated', 'Internal_Mismatched',
            'Supa_Records', 'Key_Match_Rate', 'Exact_Match_Rate',
            'Missing_In_Supabase', 'Missing_In_CSV', 'Value_Mismatches',
            'Class_Count_Match', 'Class_Count_Mismatch',
            'BL_Negatives', 'BL_Bottle_Errors', 'BL_L12M_Warnings',
            'BL_Grand_Total',
            'Count_CSV', 'Count_Supa', 'Count_Diff',
            'Duplicates', 'Missing_Class',
            'Overall_Status'
        ])

        for r in all_results:
            supa = r.get('supabase', {}) or {}
            cc = r.get('class_counts', {}) or {}
            bl = r.get('business_logic', {}) or {}
            counts = r.get('counts', {}) or {}
            bl_checks = bl.get('checks', {})

            writer.writerow([
                r.get('period', ''),
                r.get('file_id', ''),
                r.get('rows', 0),
                r.get('classes', 0),
                r.get('classes_validated', 0),
                r.get('classes_mismatched', 0),
                supa.get('supa_records', 'N/A'),
                f"{supa.get('key_match_rate', 0):.1f}%" if supa.get('status') != 'NO_SUPABASE_DATA' else 'N/A',
                f"{supa.get('exact_match_rate', 0):.1f}%" if supa.get('status') != 'NO_SUPABASE_DATA' else 'N/A',
                supa.get('csv_only', 'N/A'),
                supa.get('supa_only', 'N/A'),
                supa.get('value_errors', 'N/A'),
                cc.get('matching', 'N/A'),
                cc.get('mismatched', 'N/A'),
                bl_checks.get('negatives', {}).get('failed', 'N/A'),
                bl_checks.get('bottle_sum', {}).get('failed', 'N/A'),
                bl_checks.get('l12m_vs_ytd', {}).get('failed', 'N/A'),
                bl.get('grand_total', {}).get('status', 'N/A'),
                counts.get('total_csv', 'N/A'),
                counts.get('total_supa', 'N/A'),
                counts.get('total_diff', 'N/A'),
                counts.get('data_quality', {}).get('duplicates', 'N/A'),
                counts.get('data_quality', {}).get('missing_class', 'N/A'),
                determine_status(r),
            ])

    print(f"  CSV report: {output_path}")


# ============================================================
# Excel Report (xlsxwriter)
# ============================================================

def generate_consolidated_xlsx(all_results, output_path):
    """Generate consolidated multi-month Excel report with color coding."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    workbook = xlsxwriter.Workbook(output_path)

    # Define formats
    title_fmt = workbook.add_format({
        'bold': True, 'font_size': 16, 'font_color': '#2F5496',
        'bottom': 2, 'bottom_color': '#4472C4'
    })
    subtitle_fmt = workbook.add_format({
        'bold': True, 'font_size': 11, 'font_color': '#595959'
    })
    header_fmt = workbook.add_format({
        'bold': True, 'font_size': 10, 'bg_color': COLORS['header_bg'],
        'font_color': COLORS['header_text'], 'border': 1,
        'text_wrap': True, 'valign': 'vcenter', 'align': 'center'
    })
    pass_fmt = workbook.add_format({
        'bg_color': COLORS['pass_bg'], 'font_color': COLORS['pass_text'],
        'border': 1, 'align': 'center'
    })
    warn_fmt = workbook.add_format({
        'bg_color': COLORS['warn_bg'], 'font_color': COLORS['warn_text'],
        'border': 1, 'align': 'center'
    })
    fail_fmt = workbook.add_format({
        'bg_color': COLORS['fail_bg'], 'font_color': COLORS['fail_text'],
        'border': 1, 'align': 'center'
    })
    na_fmt = workbook.add_format({
        'bg_color': COLORS['na_bg'], 'font_color': '#808080',
        'border': 1, 'align': 'center'
    })
    cell_fmt = workbook.add_format({
        'border': 1, 'align': 'center', 'font_size': 10
    })
    cell_left_fmt = workbook.add_format({
        'border': 1, 'align': 'left', 'font_size': 10
    })
    number_fmt = workbook.add_format({
        'border': 1, 'align': 'center', 'font_size': 10, 'num_format': '#,##0'
    })
    pct_fmt = workbook.add_format({
        'border': 1, 'align': 'center', 'font_size': 10, 'num_format': '0.0%'
    })
    pass_pct_fmt = workbook.add_format({
        'bg_color': COLORS['pass_bg'], 'font_color': COLORS['pass_text'],
        'border': 1, 'align': 'center', 'num_format': '0.0%'
    })
    warn_pct_fmt = workbook.add_format({
        'bg_color': COLORS['warn_bg'], 'font_color': COLORS['warn_text'],
        'border': 1, 'align': 'center', 'num_format': '0.0%'
    })
    fail_pct_fmt = workbook.add_format({
        'bg_color': COLORS['fail_bg'], 'font_color': COLORS['fail_text'],
        'border': 1, 'align': 'center', 'num_format': '0.0%'
    })
    pass_num_fmt = workbook.add_format({
        'bg_color': COLORS['pass_bg'], 'font_color': COLORS['pass_text'],
        'border': 1, 'align': 'center', 'num_format': '#,##0'
    })
    warn_num_fmt = workbook.add_format({
        'bg_color': COLORS['warn_bg'], 'font_color': COLORS['warn_text'],
        'border': 1, 'align': 'center', 'num_format': '#,##0'
    })
    fail_num_fmt = workbook.add_format({
        'bg_color': COLORS['fail_bg'], 'font_color': COLORS['fail_text'],
        'border': 1, 'align': 'center', 'num_format': '#,##0'
    })

    def status_fmt(status):
        if status == 'PASS': return pass_fmt
        elif status == 'WARNING': return warn_fmt
        elif status == 'FAIL': return fail_fmt
        else: return na_fmt

    def rate_fmt(rate):
        if rate >= 99.5: return pass_pct_fmt
        elif rate >= 95.0: return warn_pct_fmt
        else: return fail_pct_fmt

    def error_count_fmt(count):
        if count == 0: return pass_num_fmt
        elif count <= 20: return warn_num_fmt
        else: return fail_num_fmt

    # ---- Sheet 1: Executive Summary ----
    ws1 = workbook.add_worksheet("Executive Summary")
    ws1.set_landscape()
    ws1.set_paper(1)  # Letter
    ws1.fit_to_pages(1, 0)

    row = 0
    ws1.merge_range(row, 0, row, 10, "NABCA Brand Summary - Data Validation Report", title_fmt)
    row += 1
    ws1.merge_range(row, 0, row, 10,
                    f"Generated: {datetime.now().strftime('%B %d, %Y')} | "
                    f"Periods: {all_results[0]['period'] if all_results else 'N/A'} to {all_results[-1]['period'] if all_results else 'N/A'}",
                    subtitle_fmt)
    row += 2

    # Legend
    ws1.write(row, 0, "Legend:", subtitle_fmt)
    row += 1
    ws1.write(row, 0, "PASS", pass_fmt)
    ws1.write(row, 1, "Match rate >= 99.5%", cell_left_fmt)
    ws1.write(row, 3, "WARNING", warn_fmt)
    ws1.write(row, 4, "Match rate 95-99.5%", cell_left_fmt)
    ws1.write(row, 6, "FAIL", fail_fmt)
    ws1.write(row, 7, "Match rate < 95%", cell_left_fmt)
    ws1.write(row, 9, "N/A", na_fmt)
    ws1.write(row, 10, "No Supabase data", cell_left_fmt)
    row += 2

    # Summary table headers
    headers = ['Period', 'File', 'Rows', 'Classes',
               'Internal\nValidated', 'Internal\nMismatch',
               'Key Match\nRate', 'Exact Match\nRate',
               'Missing\nin Supa', 'Missing\nin CSV', 'Value\nMismatch',
               'Status']

    col_widths = [14, 16, 10, 9, 11, 11, 12, 12, 10, 10, 10, 10]
    for c, (h, w) in enumerate(zip(headers, col_widths)):
        ws1.write(row, c, h, header_fmt)
        ws1.set_column(c, c, w)
    row += 1

    # Data rows
    for r in all_results:
        supa = r.get('supabase', {}) or {}
        st = determine_status(r)
        c = 0

        ws1.write(row, c, get_period_label(r['year'], r['month']), cell_left_fmt); c += 1
        ws1.write(row, c, r.get('file_id', ''), cell_left_fmt); c += 1
        ws1.write(row, c, r.get('rows', 0), number_fmt); c += 1
        ws1.write(row, c, r.get('classes', 0), cell_fmt); c += 1
        ws1.write(row, c, r.get('classes_validated', 0),
                  pass_num_fmt if r.get('classes_mismatched', 0) == 0 else fail_num_fmt); c += 1
        ws1.write(row, c, r.get('classes_mismatched', 0),
                  pass_num_fmt if r.get('classes_mismatched', 0) == 0 else fail_num_fmt); c += 1

        if supa.get('status') != 'NO_SUPABASE_DATA' and supa:
            kr = supa.get('key_match_rate', 0)
            er = supa.get('exact_match_rate', 0)
            ws1.write(row, c, kr / 100, rate_fmt(kr)); c += 1
            ws1.write(row, c, er / 100, rate_fmt(er)); c += 1
            ws1.write(row, c, supa.get('csv_only', 0), error_count_fmt(supa.get('csv_only', 0))); c += 1
            ws1.write(row, c, supa.get('supa_only', 0), error_count_fmt(supa.get('supa_only', 0))); c += 1
            ws1.write(row, c, supa.get('value_errors', 0), error_count_fmt(supa.get('value_errors', 0))); c += 1
        else:
            for _ in range(5):
                ws1.write(row, c, 'N/A', na_fmt); c += 1

        ws1.write(row, c, st, status_fmt(st))
        row += 1

    # ---- Sheet 2: Detailed Validation ----
    ws2 = workbook.add_worksheet("Detailed Validation")
    ws2.set_landscape()
    ws2.fit_to_pages(1, 0)

    row = 0
    ws2.merge_range(row, 0, row, 8, "Detailed Validation per Month", title_fmt)
    row += 2

    for r in all_results:
        supa = r.get('supabase', {}) or {}
        cc = r.get('class_counts', {}) or {}
        st = determine_status(r)

        # Period header
        ws2.merge_range(row, 0, row, 8,
                        f"{get_period_label(r['year'], r['month'])} ({r.get('file_id', '')})",
                        workbook.add_format({
                            'bold': True, 'font_size': 12, 'bg_color': COLORS['subheader_bg'],
                            'border': 1
                        }))
        row += 1

        # Extraction results
        metrics = [
            ('Total Rows Extracted', r.get('rows', 0)),
            ('Total Classes', r.get('classes', 0)),
            ('Internal Classes Validated', r.get('classes_validated', 0)),
            ('Internal Classes Mismatched', r.get('classes_mismatched', 0)),
            ('Rows Missing Class', r.get('no_class_rows', 0)),
            ('Duplicate Rows', r.get('duplicate_rows', 0)),
            ('Bottle Size Violations', r.get('bottle_violations', 0)),
        ]

        if supa and supa.get('status') != 'NO_SUPABASE_DATA':
            metrics.extend([
                ('Supabase Records', supa.get('supa_records', 0)),
                ('Key Match Rate', f"{supa.get('key_match_rate', 0):.1f}%"),
                ('Exact Match Rate', f"{supa.get('exact_match_rate', 0):.1f}%"),
                ('Missing in Supabase', supa.get('csv_only', 0)),
                ('Missing in CSV', supa.get('supa_only', 0)),
                ('Value Mismatches', supa.get('value_errors', 0)),
            ])

        if cc and cc.get('status') != 'NO_SUPABASE_DATA':
            metrics.extend([
                ('Class Count Match', cc.get('matching', 0)),
                ('Class Count Mismatch', cc.get('mismatched', 0)),
            ])

        metrics.append(('Overall Status', st))

        for label, value in metrics:
            ws2.write(row, 0, label, cell_left_fmt)
            if label == 'Overall Status':
                ws2.write(row, 1, value, status_fmt(value))
            elif isinstance(value, (int, float)):
                is_error = label in ('Internal Classes Mismatched', 'Missing in Supabase',
                                     'Missing in CSV', 'Value Mismatches', 'Class Count Mismatch')
                if is_error:
                    ws2.write(row, 1, value, error_count_fmt(value))
                else:
                    ws2.write(row, 1, value, number_fmt)
            else:
                ws2.write(row, 1, value, cell_fmt)
            row += 1

        row += 1  # Blank row between months

    ws2.set_column(0, 0, 28)
    ws2.set_column(1, 1, 18)

    # ---- Sheet 3: Class Validation Matrix ----
    ws3 = workbook.add_worksheet("Class Matrix")
    ws3.set_landscape()

    row = 0
    ws3.write(row, 0, "Class Validation Matrix (Record Count per Class per Month)", title_fmt)
    row += 2

    # Collect all classes
    all_classes = set()
    for r in all_results:
        cc = r.get('class_counts', {}) or {}
        for d in cc.get('details', []):
            all_classes.add(d['class'])
    all_classes = sorted(all_classes)

    # Headers
    ws3.write(row, 0, "Class", header_fmt)
    ws3.set_column(0, 0, 32)
    for c, r in enumerate(all_results, 1):
        ws3.write(row, c, r.get('period', ''), header_fmt)
        ws3.set_column(c, c, 12)
    row += 1

    # Build lookup: period -> class -> (csv_count, supa_count, status)
    class_lookup = {}
    for r in all_results:
        cc = r.get('class_counts', {}) or {}
        period = r.get('period', '')
        class_lookup[period] = {}
        for d in cc.get('details', []):
            class_lookup[period][d['class']] = d

    # Data rows
    for cls in all_classes:
        ws3.write(row, 0, cls, cell_left_fmt)
        for c, r in enumerate(all_results, 1):
            period = r.get('period', '')
            d = class_lookup.get(period, {}).get(cls)
            if d:
                count = d['csv_count']
                if d['status'] == 'OK':
                    ws3.write(row, c, count, pass_num_fmt)
                else:
                    ws3.write(row, c, count, fail_num_fmt)
            else:
                ws3.write(row, c, '', na_fmt)
        row += 1

    # ---- Sheet 4: All Errors ----
    ws4 = workbook.add_worksheet("All Errors")
    ws4.set_landscape()

    row = 0
    ws4.write(row, 0, "All Validation Errors Across All Months", title_fmt)
    row += 2

    err_headers = ['Period', 'Error_Type', 'Page', 'Brand', 'Vendor', 'Class',
                   'Field', 'CSV_Value', 'Supa_Value', 'Root_Cause', 'Notes']
    err_widths = [12, 22, 6, 25, 20, 28, 12, 12, 12, 28, 30]
    header_row = row
    for c, (h, w) in enumerate(zip(err_headers, err_widths)):
        ws4.write(row, c, h, header_fmt)
        ws4.set_column(c, c, w)
    row += 1

    # Error type formats
    missing_supa_fmt = workbook.add_format({
        'bg_color': '#FFF2CC', 'border': 1, 'font_size': 9
    })
    missing_csv_fmt = workbook.add_format({
        'bg_color': '#FCE4D6', 'border': 1, 'font_size': 9
    })
    value_mismatch_fmt = workbook.add_format({
        'bg_color': COLORS['fail_bg'], 'border': 1, 'font_size': 9
    })
    normal_err_fmt = workbook.add_format({
        'border': 1, 'font_size': 9
    })

    total_errors = 0
    for r in all_results:
        supa = r.get('supabase', {}) or {}
        period = r.get('period', '')
        error_list = supa.get('error_list', [])

        for e in error_list:
            etype = e.get('Error_Type', '')
            if etype == 'MISSING_IN_SUPABASE':
                fmt = missing_supa_fmt
            elif etype == 'MISSING_IN_CSV':
                fmt = missing_csv_fmt
            elif etype == 'VALUE_MISMATCH':
                fmt = value_mismatch_fmt
            else:
                fmt = normal_err_fmt

            root_cause, notes = classify_root_cause(e)

            ws4.write(row, 0, period, fmt)
            ws4.write(row, 1, etype, fmt)
            ws4.write(row, 2, str(e.get('Page', '')), fmt)
            ws4.write(row, 3, str(e.get('Brand', '')), fmt)
            ws4.write(row, 4, str(e.get('Vendor', '')), fmt)
            ws4.write(row, 5, str(e.get('Class', '')), fmt)
            ws4.write(row, 6, str(e.get('Field', '')), fmt)
            ws4.write(row, 7, str(e.get('CSV_Value', '')), fmt)
            ws4.write(row, 8, str(e.get('Supa_Value', '')), fmt)
            ws4.write(row, 9, root_cause, fmt)
            ws4.write(row, 10, notes, fmt)
            row += 1
            total_errors += 1

    if total_errors == 0:
        ws4.write(row, 0, "No errors found", na_fmt)

    # Add auto-filter across all columns
    if total_errors > 0:
        ws4.autofilter(header_row, 0, header_row + total_errors, len(err_headers) - 1)

    # ---- Sheet 5: Data Quality ----
    ws5 = workbook.add_worksheet("Data Quality")
    ws5.set_landscape()
    ws5.fit_to_pages(1, 0)

    row = 0
    ws5.merge_range(row, 0, row, 10, "Data Quality - Business Logic & Count Validation", title_fmt)
    row += 2

    # --- Business Logic Summary ---
    ws5.write(row, 0, "Business Logic Checks", subtitle_fmt)
    row += 1

    bl_headers = ['Period', 'Negatives', 'Bottle\nErrors', 'L12M\nWarnings',
                   'Pct Range\nWarnings', 'Class Pct\nOff', 'Grand\nTotal', 'BL Status']
    bl_widths = [16, 12, 12, 12, 12, 12, 12, 12]
    for c, (h, w) in enumerate(zip(bl_headers, bl_widths)):
        ws5.write(row, c, h, header_fmt)
        ws5.set_column(c, c, w)
    row += 1

    for r in all_results:
        bl = r.get('business_logic', {}) or {}
        bl_checks = bl.get('checks', {})
        bl_status = bl.get('overall_status', 'N/A')
        c = 0

        ws5.write(row, c, get_period_label(r['year'], r['month']), cell_left_fmt); c += 1

        neg = bl_checks.get('negatives', {}).get('failed', 0)
        ws5.write(row, c, neg, error_count_fmt(neg) if isinstance(neg, int) else na_fmt); c += 1

        bottle = bl_checks.get('bottle_sum', {}).get('failed', 0)
        ws5.write(row, c, bottle, error_count_fmt(bottle) if isinstance(bottle, int) else na_fmt); c += 1

        l12m = bl_checks.get('l12m_vs_ytd', {}).get('failed', 0)
        ws5.write(row, c, l12m, error_count_fmt(l12m) if isinstance(l12m, int) else na_fmt); c += 1

        pct_row = bl_checks.get('pct_of_type', {}).get('row_failed', 0)
        ws5.write(row, c, pct_row, error_count_fmt(pct_row) if isinstance(pct_row, int) else na_fmt); c += 1

        pct_cls = bl_checks.get('pct_of_type', {}).get('classes_off', 0)
        ws5.write(row, c, pct_cls, error_count_fmt(pct_cls) if isinstance(pct_cls, int) else na_fmt); c += 1

        gt_status = bl.get('grand_total', {}).get('status', 'N/A')
        ws5.write(row, c, gt_status, status_fmt(gt_status)); c += 1

        ws5.write(row, c, bl_status, status_fmt(bl_status))
        row += 1

    row += 2

    # --- Grand Total Comparison ---
    ws5.write(row, 0, "Grand Total Comparison (CSV Sum vs PDF DISTILLED SPIRITS TOTAL)", subtitle_fmt)
    row += 1

    gt_headers = ['Period', 'Field', 'CSV Total', 'PDF Total', 'Diff', 'Status']
    gt_widths = [16, 16, 14, 14, 12, 10]
    for c, (h, w) in enumerate(zip(gt_headers, gt_widths)):
        ws5.write(row, c, h, header_fmt)
        ws5.set_column(c, c, max(w, bl_widths[c] if c < len(bl_widths) else w))
    row += 1

    for r in all_results:
        bl = r.get('business_logic', {}) or {}
        gt = bl.get('grand_total', {})
        if gt.get('status') == 'SKIPPED':
            ws5.write(row, 0, get_period_label(r['year'], r['month']), cell_left_fmt)
            ws5.write(row, 1, 'SKIPPED', na_fmt)
            row += 1
            continue

        mismatches = gt.get('mismatches', [])
        if not mismatches:
            ws5.write(row, 0, get_period_label(r['year'], r['month']), cell_left_fmt)
            ws5.write(row, 1, 'All fields match', pass_fmt)
            row += 1
        else:
            for i, m in enumerate(mismatches):
                ws5.write(row, 0, get_period_label(r['year'], r['month']) if i == 0 else '', cell_left_fmt)
                ws5.write(row, 1, m['field'], cell_left_fmt)
                ws5.write(row, 2, m['csv_total'], number_fmt)
                ws5.write(row, 3, m['pdf_total'], number_fmt)
                ws5.write(row, 4, m['diff'], fail_num_fmt if m['diff'] != 0 else pass_num_fmt)
                ws5.write(row, 5, 'MISMATCH', fail_fmt)
                row += 1

    row += 2

    # --- Count Comparison Summary ---
    ws5.write(row, 0, "Count Comparison Summary", subtitle_fmt)
    row += 1

    cnt_headers = ['Period', 'CSV\nTotal', 'Supa\nTotal', 'Diff',
                    'Classes\nMatch', 'Classes\nMismatch', 'Duplicates',
                    'Missing\nClass', 'Status']
    cnt_widths = [16, 10, 10, 8, 10, 12, 10, 10, 10]
    for c, (h, w) in enumerate(zip(cnt_headers, cnt_widths)):
        ws5.write(row, c, h, header_fmt)
        ws5.set_column(c, c, w)
    row += 1

    for r in all_results:
        counts = r.get('counts', {}) or {}
        cnt_status = counts.get('overall_status', 'N/A')
        cc_data = counts.get('class_comparison', {})
        dq = counts.get('data_quality', {})
        c = 0

        ws5.write(row, c, get_period_label(r['year'], r['month']), cell_left_fmt); c += 1
        ws5.write(row, c, counts.get('total_csv', 0), number_fmt); c += 1
        ws5.write(row, c, counts.get('total_supa', 0), number_fmt); c += 1

        diff = counts.get('total_diff', 0)
        ws5.write(row, c, diff, pass_num_fmt if diff == 0 else fail_num_fmt); c += 1

        match_n = cc_data.get('matching', 0)
        ws5.write(row, c, match_n, pass_num_fmt); c += 1

        mis_n = cc_data.get('mismatched', 0)
        ws5.write(row, c, mis_n, error_count_fmt(mis_n) if isinstance(mis_n, int) else na_fmt); c += 1

        dups = dq.get('duplicates', 0)
        ws5.write(row, c, dups, error_count_fmt(dups) if isinstance(dups, int) else na_fmt); c += 1

        miss_cls = dq.get('missing_class', 0)
        ws5.write(row, c, miss_cls, error_count_fmt(miss_cls) if isinstance(miss_cls, int) else na_fmt); c += 1

        ws5.write(row, c, cnt_status, status_fmt(cnt_status))
        row += 1

    workbook.close()
    print(f"  Excel report: {output_path}")


# ============================================================
# PDF Report (fpdf2)
# ============================================================

class ValidationPDF(FPDF):
    """Custom PDF class for validation reports."""

    def __init__(self):
        super().__init__(orientation='L', unit='mm', format='A4')
        self.set_auto_page_break(auto=True, margin=15)

    def header(self):
        self.set_font('Helvetica', 'B', 14)
        self.set_text_color(47, 84, 150)
        self.cell(0, 8, 'NABCA Brand Summary - Data Validation Report', new_x="LMARGIN", new_y="NEXT")
        self.set_font('Helvetica', '', 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, f'Generated: {datetime.now().strftime("%B %d, %Y")}', new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 287, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', align='C')

    def section_title(self, title):
        self.set_font('Helvetica', 'B', 12)
        self.set_text_color(47, 84, 150)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def colored_table_header(self, headers, col_widths):
        self.set_fill_color(68, 114, 196)  # Blue
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 7)
        for h, w in zip(headers, col_widths):
            self.cell(w, 6, h, border=1, fill=True, align='C')
        self.ln()

    def status_cell(self, w, status, align='C'):
        if status == 'PASS':
            self.set_fill_color(198, 239, 206)
            self.set_text_color(0, 97, 0)
        elif status == 'WARNING':
            self.set_fill_color(255, 235, 156)
            self.set_text_color(156, 101, 0)
        elif status == 'FAIL':
            self.set_fill_color(255, 199, 206)
            self.set_text_color(156, 0, 6)
        else:
            self.set_fill_color(242, 242, 242)
            self.set_text_color(128, 128, 128)
        self.cell(w, 5, status, border=1, fill=True, align=align)
        self.set_text_color(0, 0, 0)

    def rate_cell(self, w, rate_str, rate_val):
        if rate_val >= 99.5:
            self.set_fill_color(198, 239, 206)
            self.set_text_color(0, 97, 0)
        elif rate_val >= 95.0:
            self.set_fill_color(255, 235, 156)
            self.set_text_color(156, 101, 0)
        else:
            self.set_fill_color(255, 199, 206)
            self.set_text_color(156, 0, 6)
        self.cell(w, 5, rate_str, border=1, fill=True, align='C')
        self.set_text_color(0, 0, 0)

    def error_cell(self, w, count):
        count_int = int(count) if isinstance(count, (int, float)) else 0
        if count_int == 0:
            self.set_fill_color(198, 239, 206)
            self.set_text_color(0, 97, 0)
        elif count_int <= 20:
            self.set_fill_color(255, 235, 156)
            self.set_text_color(156, 101, 0)
        else:
            self.set_fill_color(255, 199, 206)
            self.set_text_color(156, 0, 6)
        self.cell(w, 5, str(count), border=1, fill=True, align='C')
        self.set_text_color(0, 0, 0)

    def na_cell(self, w, text='N/A'):
        self.set_fill_color(242, 242, 242)
        self.set_text_color(128, 128, 128)
        self.cell(w, 5, text, border=1, fill=True, align='C')
        self.set_text_color(0, 0, 0)


def generate_consolidated_pdf(all_results, output_path):
    """Generate consolidated multi-month PDF report with colored tables."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    pdf = ValidationPDF()
    pdf.alias_nb_pages()

    # ---- Page 1: Executive Summary ----
    pdf.add_page()
    pdf.section_title("Executive Summary")

    # Legend
    pdf.set_font('Helvetica', '', 7)
    pdf.set_fill_color(198, 239, 206)
    pdf.cell(12, 4, 'PASS', border=1, fill=True, align='C')
    pdf.cell(30, 4, ' Match >= 99.5%', border=0)
    pdf.set_fill_color(255, 235, 156)
    pdf.cell(16, 4, 'WARNING', border=1, fill=True, align='C')
    pdf.cell(30, 4, ' Match 95-99.5%', border=0)
    pdf.set_fill_color(255, 199, 206)
    pdf.cell(12, 4, 'FAIL', border=1, fill=True, align='C')
    pdf.cell(25, 4, ' Match < 95%', border=0)
    pdf.set_fill_color(242, 242, 242)
    pdf.cell(10, 4, 'N/A', border=1, fill=True, align='C')
    pdf.cell(30, 4, ' No Supabase data', border=0)
    pdf.ln(8)

    # Summary table
    headers = ['Period', 'File', 'Rows', 'Classes', 'Int.Valid', 'Int.Mis',
               'Key Match', 'Exact Match', 'Miss Supa', 'Miss CSV', 'Val Mis', 'Status']
    widths = [24, 22, 18, 14, 16, 14, 22, 22, 18, 18, 18, 16]

    # Adjust widths to fit landscape A4
    total_w = sum(widths)
    available = 277  # A4 landscape - margins
    if total_w > available:
        scale = available / total_w
        widths = [w * scale for w in widths]

    pdf.colored_table_header(headers, widths)

    pdf.set_font('Helvetica', '', 7)
    for r in all_results:
        supa = r.get('supabase', {}) or {}
        st = determine_status(r)

        pdf.cell(widths[0], 5, get_period_label(r['year'], r['month']), border=1, align='L')
        pdf.cell(widths[1], 5, r.get('file_id', ''), border=1, align='L')
        pdf.cell(widths[2], 5, f"{r.get('rows', 0):,}", border=1, align='C')
        pdf.cell(widths[3], 5, str(r.get('classes', 0)), border=1, align='C')

        # Internal validation
        if r.get('classes_mismatched', 0) == 0:
            pdf.set_fill_color(198, 239, 206)
            pdf.set_text_color(0, 97, 0)
        else:
            pdf.set_fill_color(255, 199, 206)
            pdf.set_text_color(156, 0, 6)
        pdf.cell(widths[4], 5, str(r.get('classes_validated', 0)), border=1, fill=True, align='C')
        pdf.cell(widths[5], 5, str(r.get('classes_mismatched', 0)), border=1, fill=True, align='C')
        pdf.set_text_color(0, 0, 0)

        if supa and supa.get('status') != 'NO_SUPABASE_DATA':
            kr = supa.get('key_match_rate', 0)
            er = supa.get('exact_match_rate', 0)
            pdf.rate_cell(widths[6], f"{kr:.1f}%", kr)
            pdf.rate_cell(widths[7], f"{er:.1f}%", er)
            pdf.error_cell(widths[8], supa.get('csv_only', 0))
            pdf.error_cell(widths[9], supa.get('supa_only', 0))
            pdf.error_cell(widths[10], supa.get('value_errors', 0))
        else:
            for w in widths[6:11]:
                pdf.na_cell(w)

        pdf.status_cell(widths[11], st)
        pdf.ln()

    # ---- Page 2: Detailed Validation ----
    pdf.add_page()
    pdf.section_title("Detailed Validation per Month")

    for r in all_results:
        supa = r.get('supabase', {}) or {}
        cc = r.get('class_counts', {}) or {}
        st = determine_status(r)

        # Check if we need a new page
        if pdf.get_y() > 160:
            pdf.add_page()

        # Month header
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_fill_color(214, 228, 240)
        pdf.cell(0, 6, f"{get_period_label(r['year'], r['month'])} ({r.get('file_id', '')})",
                 border=1, fill=True, new_x="LMARGIN", new_y="NEXT")

        pdf.set_font('Helvetica', '', 7)
        lw = 60
        vw = 30

        metrics = [
            ('Total Rows', f"{r.get('rows', 0):,}"),
            ('Classes', str(r.get('classes', 0))),
            ('Internal Validated', str(r.get('classes_validated', 0))),
            ('Internal Mismatched', str(r.get('classes_mismatched', 0))),
        ]
        if supa and supa.get('status') != 'NO_SUPABASE_DATA':
            metrics.extend([
                ('Key Match Rate', f"{supa.get('key_match_rate', 0):.1f}%"),
                ('Exact Match Rate', f"{supa.get('exact_match_rate', 0):.1f}%"),
                ('Missing in Supa', str(supa.get('csv_only', 0))),
                ('Missing in CSV', str(supa.get('supa_only', 0))),
                ('Value Mismatches', str(supa.get('value_errors', 0))),
            ])
        if cc and cc.get('status') != 'NO_SUPABASE_DATA':
            metrics.extend([
                ('Class Count Match', str(cc.get('matching', 0))),
                ('Class Count Mismatch', str(cc.get('mismatched', 0))),
            ])

        # Print metrics in 2 columns
        col1 = metrics[:len(metrics)//2 + 1]
        col2 = metrics[len(metrics)//2 + 1:]

        start_y = pdf.get_y()
        for label, value in col1:
            pdf.cell(lw, 5, label, border=1)
            pdf.cell(vw, 5, value, border=1, align='C')
            pdf.ln()

        y_after_col1 = pdf.get_y()
        pdf.set_y(start_y)
        pdf.set_x(10 + lw + vw + 5)

        for label, value in col2:
            pdf.set_x(10 + lw + vw + 5)
            pdf.cell(lw, 5, label, border=1)
            pdf.cell(vw, 5, value, border=1, align='C')
            pdf.ln()

        pdf.set_y(max(y_after_col1, pdf.get_y()))

        # Status
        pdf.cell(lw, 5, 'Overall Status', border=1)
        pdf.status_cell(vw, st)
        pdf.ln(8)

    # ---- Page 3: Error Summary by Month ----
    pdf.add_page()
    pdf.section_title("Error Summary by Month")

    err_headers = ['Period', 'Missing in Supa', 'Missing in CSV', '750ml Mismatch',
                   'Column Shift', 'Other Field', 'Total Errors']
    err_widths = [40, 35, 35, 35, 35, 35, 35]

    pdf.colored_table_header(err_headers, err_widths)
    pdf.set_font('Helvetica', '', 7)

    for r in all_results:
        supa = r.get('supabase', {}) or {}
        error_list = supa.get('error_list', [])

        miss_supa = sum(1 for e in error_list if e.get('Error_Type') == 'MISSING_IN_SUPABASE')
        miss_csv = sum(1 for e in error_list if e.get('Error_Type') == 'MISSING_IN_CSV')
        vm_750 = sum(1 for e in error_list if e.get('Error_Type') == 'VALUE_MISMATCH' and e.get('Field') == '750ml')
        vm_shift = sum(1 for e in error_list if e.get('Error_Type') == 'VALUE_MISMATCH' and e.get('Field') in ('375ml', '200ml', '100ml', '50ml'))
        vm_other = sum(1 for e in error_list if e.get('Error_Type') == 'VALUE_MISMATCH' and e.get('Field') not in ('750ml', '375ml', '200ml', '100ml', '50ml', ''))
        total = len(error_list)

        pdf.cell(err_widths[0], 5, get_period_label(r['year'], r['month']), border=1, align='L')
        pdf.error_cell(err_widths[1], miss_supa)
        pdf.error_cell(err_widths[2], miss_csv)
        pdf.error_cell(err_widths[3], vm_750)
        pdf.error_cell(err_widths[4], vm_shift)
        pdf.error_cell(err_widths[5], vm_other)
        pdf.error_cell(err_widths[6], total)
        pdf.ln()

    # ---- Page 4: Data Quality Summary ----
    pdf.add_page()
    pdf.section_title("Data Quality Summary")

    # Business Logic table
    pdf.set_font('Helvetica', 'B', 9)
    pdf.cell(0, 6, 'Business Logic Checks', new_x="LMARGIN", new_y="NEXT")

    bl_headers = ['Period', 'Negatives', 'Bottle Err', 'L12M Warn', 'Pct Warn', 'Grand Total', 'BL Status']
    bl_widths = [38, 28, 28, 28, 28, 28, 28]
    total_bw = sum(bl_widths)
    if total_bw > 277:
        scale = 277 / total_bw
        bl_widths = [w * scale for w in bl_widths]

    pdf.colored_table_header(bl_headers, bl_widths)
    pdf.set_font('Helvetica', '', 7)

    for r in all_results:
        bl = r.get('business_logic', {}) or {}
        bl_checks = bl.get('checks', {})
        bl_status = bl.get('overall_status', 'N/A')

        pdf.cell(bl_widths[0], 5, get_period_label(r['year'], r['month']), border=1, align='L')
        pdf.error_cell(bl_widths[1], bl_checks.get('negatives', {}).get('failed', 0))
        pdf.error_cell(bl_widths[2], bl_checks.get('bottle_sum', {}).get('failed', 0))
        pdf.error_cell(bl_widths[3], bl_checks.get('l12m_vs_ytd', {}).get('failed', 0))
        pdf.error_cell(bl_widths[4], bl_checks.get('pct_of_type', {}).get('row_failed', 0))

        gt_status = bl.get('grand_total', {}).get('status', 'N/A')
        pdf.status_cell(bl_widths[5], gt_status)
        pdf.status_cell(bl_widths[6], bl_status)
        pdf.ln()

    pdf.ln(6)

    # Count Comparison table
    pdf.set_font('Helvetica', 'B', 9)
    pdf.cell(0, 6, 'Count Comparison', new_x="LMARGIN", new_y="NEXT")

    cnt_headers = ['Period', 'CSV Total', 'Supa Total', 'Diff', 'Cls Match', 'Cls Mis', 'Dupes', 'Miss Cls', 'Status']
    cnt_widths = [38, 26, 26, 22, 24, 24, 22, 22, 22]
    total_cw = sum(cnt_widths)
    if total_cw > 277:
        scale = 277 / total_cw
        cnt_widths = [w * scale for w in cnt_widths]

    pdf.colored_table_header(cnt_headers, cnt_widths)
    pdf.set_font('Helvetica', '', 7)

    for r in all_results:
        counts = r.get('counts', {}) or {}
        cc_data = counts.get('class_comparison', {})
        dq = counts.get('data_quality', {})
        cnt_status = counts.get('overall_status', 'N/A')

        pdf.cell(cnt_widths[0], 5, get_period_label(r['year'], r['month']), border=1, align='L')
        pdf.cell(cnt_widths[1], 5, f"{counts.get('total_csv', 0):,}", border=1, align='C')
        pdf.cell(cnt_widths[2], 5, f"{counts.get('total_supa', 0):,}", border=1, align='C')

        diff = counts.get('total_diff', 0)
        if diff == 0:
            pdf.set_fill_color(198, 239, 206)
            pdf.set_text_color(0, 97, 0)
        else:
            pdf.set_fill_color(255, 199, 206)
            pdf.set_text_color(156, 0, 6)
        pdf.cell(cnt_widths[3], 5, f"{diff:+,}", border=1, fill=True, align='C')
        pdf.set_text_color(0, 0, 0)

        pdf.error_cell(cnt_widths[4], cc_data.get('matching', 0))
        pdf.error_cell(cnt_widths[5], cc_data.get('mismatched', 0))
        pdf.error_cell(cnt_widths[6], dq.get('duplicates', 0))
        pdf.error_cell(cnt_widths[7], dq.get('missing_class', 0))
        pdf.status_cell(cnt_widths[8], cnt_status)
        pdf.ln()

    pdf.output(output_path)
    print(f"  PDF report: {output_path}")


# ============================================================
# Master Error CSV (all months, all errors, with root cause)
# ============================================================

def generate_master_error_csv(all_results, output_path):
    """Generate flat CSV with every error across all months + Period + Root_Cause."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Error_#', 'Period', 'Error_Type', 'Root_Cause', 'Page',
            'Brand', 'Vendor', 'Class', 'Field', 'CSV_Value', 'Supa_Value', 'Notes'
        ])

        err_num = 0
        for r in all_results:
            supa = r.get('supabase', {}) or {}
            period = r.get('period', '')
            for e in supa.get('error_list', []):
                err_num += 1
                root_cause, notes = classify_root_cause(e)
                writer.writerow([
                    err_num, period, e.get('Error_Type', ''), root_cause,
                    e.get('Page', ''), e.get('Brand', ''), e.get('Vendor', ''),
                    e.get('Class', ''), e.get('Field', ''),
                    e.get('CSV_Value', ''), e.get('Supa_Value', ''), notes
                ])

    print(f"  Master error CSV: {output_path} ({err_num} errors)")


# ============================================================
# Master Error Report (text, month-by-month with every record)
# ============================================================

def generate_master_error_report(all_results, output_path):
    """Generate single text report: summary table + every error listed month by month."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    lines = []

    # ===== HEADER =====
    lines.append("=" * 120)
    lines.append("NABCA BRAND SUMMARY - VALIDATION ERROR REPORT (ALL MONTHS)")
    lines.append(f"Generated: {datetime.now().strftime('%B %d, %Y %H:%M')}")
    if all_results:
        lines.append(f"Periods: {all_results[0].get('period', 'N/A')} to {all_results[-1].get('period', 'N/A')}")
    lines.append("=" * 120)

    # ===== EXECUTIVE SUMMARY TABLE =====
    lines.append("")
    lines.append("EXECUTIVE SUMMARY")
    lines.append("-" * 120)
    lines.append(
        f"{'Period':<18} {'Rows':>8} {'Classes':>8} {'Key Match%':>11} {'Exact Match%':>13} "
        f"{'MissSupa':>9} {'MissCSV':>8} {'ValMis':>7} {'Total Err':>10} {'Status':>8}"
    )
    lines.append("-" * 120)

    total_rows = 0
    total_errors = 0
    for r in all_results:
        supa = r.get('supabase', {}) or {}
        st = determine_status(r)
        period_label = get_period_label(r.get('year', 0), r.get('month', 0))
        rows = r.get('rows', 0)
        classes = r.get('classes', 0)
        error_list = supa.get('error_list', [])
        err_count = len(error_list)
        total_rows += rows
        total_errors += err_count

        miss_supa = sum(1 for e in error_list if e.get('Error_Type') == 'MISSING_IN_SUPABASE')
        miss_csv = sum(1 for e in error_list if e.get('Error_Type') == 'MISSING_IN_CSV')
        val_mis = sum(1 for e in error_list if e.get('Error_Type') == 'VALUE_MISMATCH')

        if supa.get('status') not in ('NO_SUPABASE_DATA', 'SKIPPED', 'ERROR', None):
            kr = f"{supa.get('key_match_rate', 0):.1f}%"
            er = f"{supa.get('exact_match_rate', 0):.1f}%"
        else:
            kr = 'N/A'
            er = 'N/A'

        lines.append(
            f"{period_label:<18} {rows:>8,} {classes:>8} {kr:>11} {er:>13} "
            f"{miss_supa:>9} {miss_csv:>8} {val_mis:>7} {err_count:>10,} {st:>8}"
        )

    lines.append("-" * 120)
    lines.append(f"{'TOTAL':<18} {total_rows:>8,} {'':>8} {'':>11} {'':>13} {'':>9} {'':>8} {'':>7} {total_errors:>10,}")

    # ===== BUSINESS LOGIC SUMMARY =====
    lines.append("")
    lines.append("")
    lines.append("BUSINESS LOGIC VALIDATION")
    lines.append("-" * 120)
    lines.append(
        f"{'Period':<18} {'Negatives':>10} {'BottleErr':>10} {'L12M Warn':>10} {'PctWarn':>8} {'GrandTotal':>11} {'BL Status':>10}"
    )
    lines.append("-" * 120)

    for r in all_results:
        bl = r.get('business_logic', {}) or {}
        bl_checks = bl.get('checks', {})
        period_label = get_period_label(r.get('year', 0), r.get('month', 0))

        neg = bl_checks.get('negatives', {}).get('failed', 'N/A')
        bottle = bl_checks.get('bottle_sum', {}).get('failed', 'N/A')
        l12m_w = bl_checks.get('l12m_vs_ytd', {}).get('failed', 'N/A')
        pct_w = bl_checks.get('pct_of_type', {}).get('row_failed', 'N/A')
        gt_st = bl.get('grand_total', {}).get('status', 'N/A')
        bl_st = bl.get('overall_status', 'N/A')

        lines.append(
            f"{period_label:<18} {str(neg):>10} {str(bottle):>10} {str(l12m_w):>10} "
            f"{str(pct_w):>8} {gt_st:>11} {bl_st:>10}"
        )

    # ===== COUNT COMPARISON SUMMARY =====
    lines.append("")
    lines.append("")
    lines.append("COUNT COMPARISON")
    lines.append("-" * 120)
    lines.append(
        f"{'Period':<18} {'CSV Total':>10} {'Supa Total':>11} {'Diff':>8} {'ClsMatch':>9} {'ClsMis':>7} {'Dupes':>6} {'MissCls':>8} {'Status':>8}"
    )
    lines.append("-" * 120)

    for r in all_results:
        counts = r.get('counts', {}) or {}
        cc_data = counts.get('class_comparison', {})
        dq = counts.get('data_quality', {})
        period_label = get_period_label(r.get('year', 0), r.get('month', 0))

        lines.append(
            f"{period_label:<18} {counts.get('total_csv', 0):>10,} {counts.get('total_supa', 0):>11,} "
            f"{counts.get('total_diff', 0):>+8} {cc_data.get('matching', 0):>9} "
            f"{cc_data.get('mismatched', 0):>7} {dq.get('duplicates', 0):>6} "
            f"{dq.get('missing_class', 0):>8} {counts.get('overall_status', 'N/A'):>8}"
        )

    # ===== ROOT CAUSE KEY =====
    lines.append("")
    lines.append("")
    lines.append("ROOT CAUSE KEY")
    lines.append("-" * 80)
    lines.append("  MISSING_IN_SUPABASE  = Record in our PDF extraction but not in Supabase (word order difference in Brand/Vendor)")
    lines.append("  MISSING_IN_CSV       = Record in Supabase but not in our extraction (word order difference)")
    lines.append("  VALUE_MISMATCH 750ml = Supabase curr_month_750ml contains our 750ml_Trav column value")
    lines.append("  VALUE_MISMATCH 375ml/200ml/100ml/50ml = Downstream column shift from 750ml mapping")
    lines.append("  VALUE_MISMATCH other = OCR or source data difference")

    # ===== MONTH-BY-MONTH DETAIL =====
    for r in all_results:
        supa = r.get('supabase', {}) or {}
        error_list = supa.get('error_list', [])
        if not error_list:
            continue

        period_label = get_period_label(r.get('year', 0), r.get('month', 0))
        period = r.get('period', '')

        lines.append("")
        lines.append("")
        lines.append("=" * 120)
        lines.append(f"{period_label} ({period})  --  {len(error_list)} errors")
        lines.append(f"  Rows: {r.get('rows', 0):,} | Classes: {r.get('classes', 0)} | "
                     f"Key Match: {supa.get('key_match_rate', 0):.1f}% | "
                     f"Exact Match: {supa.get('exact_match_rate', 0):.1f}%")
        lines.append("=" * 120)

        # --- Missing records ---
        missing_supa = [e for e in error_list if e.get('Error_Type') == 'MISSING_IN_SUPABASE']
        missing_csv = [e for e in error_list if e.get('Error_Type') == 'MISSING_IN_CSV']

        if missing_supa or missing_csv:
            lines.append("")
            lines.append(f"  MISSING RECORDS ({len(missing_supa)} in CSV only, {len(missing_csv)} in Supabase only)")
            lines.append(f"  {'#':<4} {'Type':<22} {'Page':<6} {'Brand':<25} {'Vendor':<22} {'Class'}")
            lines.append(f"  {'-'*110}")

            num = 0
            for e in sorted(missing_supa + missing_csv,
                            key=lambda x: (x.get('Error_Type', ''), int(x.get('Page', 0) or 0))):
                num += 1
                lines.append(
                    f"  {num:<4} {e.get('Error_Type', ''):<22} {e.get('Page', ''):<6} "
                    f"{e.get('Brand', ''):<25} {e.get('Vendor', ''):<22} {e.get('Class', '')}"
                )

        # --- Value mismatches ---
        value_mis = [e for e in error_list if e.get('Error_Type') == 'VALUE_MISMATCH']

        if value_mis:
            # Sub-categorize
            vm_750 = [e for e in value_mis if e.get('Field') == '750ml']
            vm_shift = [e for e in value_mis if e.get('Field') in ('375ml', '200ml', '100ml', '50ml')]
            vm_other = [e for e in value_mis if e.get('Field') not in ('750ml', '375ml', '200ml', '100ml', '50ml')]

            lines.append("")
            lines.append(f"  VALUE MISMATCHES ({len(value_mis)} total: {len(vm_750)} 750ml, {len(vm_shift)} column-shift, {len(vm_other)} other)")
            lines.append(f"  {'#':<4} {'Page':<6} {'Brand':<25} {'Vendor':<20} {'Class':<28} {'Field':<8} {'CSV_Value':<12} {'Supa_Value':<12} {'Root Cause'}")
            lines.append(f"  {'-'*145}")

            num = 0
            for e in sorted(value_mis, key=lambda x: (x.get('Field', ''), int(x.get('Page', 0) or 0))):
                num += 1
                root_cause, _ = classify_root_cause(e)
                lines.append(
                    f"  {num:<4} {e.get('Page', ''):<6} "
                    f"{e.get('Brand', ''):<25} {e.get('Vendor', ''):<20} "
                    f"{e.get('Class', ''):<28} {e.get('Field', ''):<8} "
                    f"{e.get('CSV_Value', '') or '(empty)':<12} {e.get('Supa_Value', '') or '(empty)':<12} "
                    f"{root_cause}"
                )

    lines.append("")
    lines.append("=" * 120)
    lines.append("END OF REPORT")
    lines.append("=" * 120)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))

    print(f"  Master error report: {output_path}")


def generate_all_reports(all_results, reports_dir="reports"):
    """Generate all report formats."""
    os.makedirs(reports_dir, exist_ok=True)

    print("\nGenerating reports...")
    generate_consolidated_csv(all_results, os.path.join(reports_dir, "consolidated_validation.csv"))
    generate_consolidated_xlsx(all_results, os.path.join(reports_dir, "consolidated_validation.xlsx"))
    generate_consolidated_pdf(all_results, os.path.join(reports_dir, "consolidated_validation.pdf"))
    generate_master_error_csv(all_results, os.path.join(reports_dir, "consolidated_all_errors.csv"))
    generate_master_error_report(all_results, os.path.join(reports_dir, "consolidated_error_report.txt"))
    print("  All reports generated!")


if __name__ == '__main__':
    # Test with dummy data
    print("Report generator module. Use pipeline.py to generate reports.")
