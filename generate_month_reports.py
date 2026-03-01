"""
Generate per-month markdown reports and a summary table from pipeline results.

Usage:
    python generate_month_reports.py                    # From default results file
    python generate_month_reports.py --results path.json
    python generate_month_reports.py --output reports/   # Custom output dir
"""
import json
import os
import argparse
from datetime import datetime

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

RESULTS_FILE = "reports/pipeline_results.json"
OUTPUT_DIR = "reports"


def determine_status(result):
    """Determine PASS/WARNING/FAIL for a month's results."""
    if result.get('error'):
        return 'FAIL'
    if result.get('classes_mismatched', 0) > 0:
        return 'FAIL'
    supa = result.get('supabase')
    if not supa or supa.get('status') in ('NO_SUPABASE_DATA', 'SKIPPED', 'ERROR'):
        return 'N/A'
    key_rate = supa.get('key_match_rate', 0)
    exact_rate = supa.get('exact_match_rate', 0)
    if key_rate >= 99.5 and exact_rate >= 99.0:
        return 'PASS'
    elif key_rate >= 95.0 and exact_rate >= 95.0:
        return 'WARNING'
    else:
        return 'FAIL'


def compute_error_metrics(result):
    """Compute Supabase error metrics for a month."""
    supa = result.get('supabase', {}) or {}

    if supa.get('status') in ('NO_SUPABASE_DATA', 'SKIPPED', 'ERROR', None):
        return {
            'total_supa_records': 0,
            'missing_in_supabase': 0,
            'missing_in_csv': 0,
            'value_mismatches': 0,
            'total_errors': 0,
            'error_rate': 0.0,
            'available': False,
        }

    total_supa = supa.get('supa_records', 0)
    missing_in_supa = supa.get('csv_only', 0)
    missing_in_csv = supa.get('supa_only', 0)
    value_mismatches = supa.get('value_errors', 0)
    total_errors = missing_in_supa + missing_in_csv + value_mismatches
    error_rate = (total_errors / total_supa * 100) if total_supa > 0 else 0.0

    return {
        'total_supa_records': total_supa,
        'missing_in_supabase': missing_in_supa,
        'missing_in_csv': missing_in_csv,
        'value_mismatches': value_mismatches,
        'total_errors': total_errors,
        'error_rate': round(error_rate, 2),
        'available': True,
    }


def generate_month_report(result, output_dir):
    """Generate a markdown report for a single month."""
    period = result.get('period', 'unknown')
    year = result.get('year', 0)
    month = result.get('month', 0)
    file_id = result.get('file_id', '')
    status = determine_status(result)
    error_metrics = compute_error_metrics(result)

    month_name = MONTH_NAMES.get(month, str(month))
    title = f"{month_name} {year}"

    lines = []
    lines.append(f"# {title} — Brand Summary Validation Report")
    lines.append("")
    lines.append(f"**Period:** {period}  ")
    lines.append(f"**File ID:** `{file_id}`  ")
    lines.append(f"**Overall Status:** {status}  ")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  ")
    lines.append("")

    # --- Supabase Record Summary (NEW) ---
    lines.append("## Supabase Record Summary")
    lines.append("")
    if error_metrics['available']:
        lines.append("| Metric | Count |")
        lines.append("|--------|------:|")
        lines.append(f"| Total records in Supabase | {error_metrics['total_supa_records']:,} |")
        lines.append(f"| Total records with errors | {error_metrics['total_errors']:,} |")
        lines.append(f"| — Missing in Supabase | {error_metrics['missing_in_supabase']:,} |")
        lines.append(f"| — Missing in CSV (extra in Supa) | {error_metrics['missing_in_csv']:,} |")
        lines.append(f"| — Value mismatches | {error_metrics['value_mismatches']:,} |")
        lines.append(f"| Error rate | {error_metrics['error_rate']:.2f}% |")
    else:
        lines.append("*Supabase validation not available for this month.*")
    lines.append("")

    # --- Extraction Summary ---
    lines.append("## Extraction Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|------:|")
    lines.append(f"| Records extracted | {result.get('rows', 0):,} |")
    lines.append(f"| Classes detected | {result.get('classes', 0)} |")
    lines.append(f"| Classes validated (TOTAL match) | {result.get('classes_validated', 0)} |")
    lines.append(f"| Classes mismatched | {result.get('classes_mismatched', 0)} |")
    lines.append(f"| Rows without class | {result.get('no_class_rows', 0)} |")
    lines.append(f"| Duplicate keys | {result.get('duplicate_rows', 0)} |")
    lines.append(f"| Bottle size violations | {result.get('bottle_violations', 0)} |")
    lines.append("")

    # --- Supabase Validation ---
    supa = result.get('supabase', {}) or {}
    lines.append("## Supabase Validation (CSV vs Database)")
    lines.append("")
    if supa.get('status') not in ('NO_SUPABASE_DATA', 'SKIPPED', 'ERROR', None):
        lines.append("| Metric | Value |")
        lines.append("|--------|------:|")
        lines.append(f"| CSV records | {supa.get('csv_records', 0):,} |")
        lines.append(f"| Supabase records | {supa.get('supa_records', 0):,} |")
        lines.append(f"| Matching keys (brand+vendor+class) | {supa.get('matching_keys', 0):,} |")
        lines.append(f"| CSV only (missing in Supabase) | {supa.get('csv_only', 0):,} |")
        lines.append(f"| Supabase only (missing in CSV) | {supa.get('supa_only', 0):,} |")
        lines.append(f"| Exact value matches | {supa.get('exact_matches', 0):,} |")
        lines.append(f"| Value mismatches | {supa.get('value_errors', 0):,} |")
        lines.append(f"| Key match rate | {supa.get('key_match_rate', 0):.1f}% |")
        lines.append(f"| Exact match rate | {supa.get('exact_match_rate', 0):.1f}% |")
    else:
        supa_status = supa.get('status', 'N/A') if supa else 'N/A'
        lines.append(f"*Status: {supa_status}*")
    lines.append("")

    # --- Business Logic ---
    bl = result.get('business_logic', {}) or {}
    lines.append("## Business Logic Validation")
    lines.append("")
    if bl.get('status') not in ('ERROR', None):
        checks = bl.get('checks', {})
        lines.append(f"**Overall:** {bl.get('overall_status', bl.get('status', 'N/A'))}")
        lines.append("")
        lines.append("| Check | Passed | Failed | Notes |")
        lines.append("|-------|-------:|-------:|-------|")

        neg = checks.get('negatives', {})
        lines.append(f"| No negatives | {neg.get('passed', 0):,} | {neg.get('failed', 0)} | All 12 numeric fields >= 0 |")

        bot = checks.get('bottle_sum', {})
        lines.append(f"| Bottle sum = CurMo | {bot.get('passed', 0):,} | {bot.get('failed', 0)} | {bot.get('skipped', 0):,} skipped |")

        l12 = checks.get('l12m_vs_ytd', {})
        lines.append(f"| L12M >= YTD | {l12.get('passed', 0):,} | {l12.get('failed', 0)} | {l12.get('skipped', 0):,} skipped (warning only) |")

        pct = checks.get('pct_of_type', {})
        lines.append(f"| Pct of Type range | {pct.get('row_passed', 0):,} | {pct.get('row_failed', 0)} | {pct.get('classes_ok', 0)}/{pct.get('classes_total', 0)} classes within +/-2% |")

        gt = bl.get('grand_total', {})
        gt_status = gt.get('status', 'N/A')
        gt_matched = gt.get('fields_matched', 0)
        gt_checked = gt.get('fields_checked', 0)
        lines.append(f"| Grand total | {gt_matched} | {gt_checked - gt_matched} | {gt_status} |")
    else:
        bl_err = bl.get('error', 'N/A') if bl else 'N/A'
        lines.append(f"*Status: ERROR — {bl_err}*")
    lines.append("")

    # --- Count Validation ---
    counts = result.get('counts', {}) or {}
    lines.append("## Count Validation (Per-Class)")
    lines.append("")
    if counts.get('status') not in ('ERROR', 'SKIPPED', 'NO_SUPABASE_DATA', None):
        cc = counts.get('class_comparison', {})
        lines.append("| Metric | Value |")
        lines.append("|--------|------:|")
        lines.append(f"| CSV total | {counts.get('total_csv', 0):,} |")
        lines.append(f"| Supabase total | {counts.get('total_supa', 0):,} |")
        lines.append(f"| Difference | {counts.get('total_diff', 0):+,} |")
        lines.append(f"| Classes matching | {cc.get('matching', 0)} |")
        lines.append(f"| Classes mismatched | {cc.get('mismatched', 0)} |")
        lines.append(f"| Overall | {counts.get('overall_status', counts.get('status', 'N/A'))} |")

        dq = counts.get('data_quality', {})
        if dq:
            lines.append(f"| Duplicates | {dq.get('duplicates', 0)} |")
            lines.append(f"| Missing class | {dq.get('missing_class', 0)} |")
    else:
        cnt_status = counts.get('status', 'N/A') if counts else 'N/A'
        lines.append(f"*Status: {cnt_status}*")
    lines.append("")

    # --- Sample Errors ---
    error_list = supa.get('error_list', []) if supa else []
    if error_list:
        lines.append("## Sample Errors (first 10)")
        lines.append("")
        lines.append("| Type | Page | Brand | Vendor | Class | Field | CSV | Supa |")
        lines.append("|------|------|-------|--------|-------|-------|-----|------|")
        for err in error_list[:10]:
            etype = err.get('Error_Type', err.get('error_type', ''))
            page = err.get('Page', err.get('page', ''))
            brand = str(err.get('Brand', err.get('brand', '')))[:25]
            vendor = str(err.get('Vendor', err.get('vendor', '')))[:18]
            cls = str(err.get('Class', err.get('class', '')))[:25]
            field = err.get('Field', err.get('field', ''))
            csv_val = err.get('CSV_Value', err.get('csv_val', ''))
            supa_val = err.get('Supa_Value', err.get('supa_val', ''))
            lines.append(f"| {etype} | {page} | {brand} | {vendor} | {cls} | {field} | {csv_val} | {supa_val} |")
        lines.append("")

    # Write file
    filepath = os.path.join(output_dir, f"{period}.md")
    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    return filepath


def generate_summary(all_results, output_dir):
    """Generate summary.md with cross-month comparison table."""
    lines = []
    lines.append("# NABCA Brand Summary — Validation Summary")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  ")
    lines.append(f"**Months processed:** {len(all_results)}  ")
    lines.append("")

    # Overview table
    lines.append("## Month-by-Month Results")
    lines.append("")
    lines.append("| Period | Extracted | Supa Records | Errors | Error Rate | Key Match | Exact Match | Status |")
    lines.append("|--------|----------:|-------------:|-------:|-----------:|----------:|------------:|--------|")

    total_extracted = 0
    total_supa = 0
    total_errors = 0
    pass_count = 0
    warn_count = 0
    fail_count = 0

    for r in all_results:
        period = r.get('period', '')
        rows = r.get('rows', 0)
        status = determine_status(r)
        em = compute_error_metrics(r)
        supa = r.get('supabase', {}) or {}

        total_extracted += rows

        if em['available']:
            supa_recs = f"{em['total_supa_records']:,}"
            errors = f"{em['total_errors']:,}"
            err_rate = f"{em['error_rate']:.2f}%"
            kr = f"{supa.get('key_match_rate', 0):.1f}%"
            er = f"{supa.get('exact_match_rate', 0):.1f}%"
            total_supa += em['total_supa_records']
            total_errors += em['total_errors']
        else:
            supa_recs = "N/A"
            errors = "N/A"
            err_rate = "N/A"
            kr = "N/A"
            er = "N/A"

        if status == 'PASS':
            pass_count += 1
        elif status == 'WARNING':
            warn_count += 1
        elif status == 'FAIL':
            fail_count += 1

        lines.append(f"| {period} | {rows:,} | {supa_recs} | {errors} | {err_rate} | {kr} | {er} | {status} |")

    lines.append("")

    # Totals
    overall_err_rate = (total_errors / total_supa * 100) if total_supa > 0 else 0
    lines.append("## Aggregate Totals")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|------:|")
    lines.append(f"| Total records extracted | {total_extracted:,} |")
    lines.append(f"| Total Supabase records | {total_supa:,} |")
    lines.append(f"| Total errors | {total_errors:,} |")
    lines.append(f"| Overall error rate | {overall_err_rate:.2f}% |")
    lines.append(f"| Months PASS | {pass_count} |")
    lines.append(f"| Months WARNING | {warn_count} |")
    lines.append(f"| Months FAIL | {fail_count} |")
    lines.append("")

    # Links to per-month reports
    lines.append("## Per-Month Reports")
    lines.append("")
    for r in all_results:
        period = r.get('period', '')
        month = r.get('month', 0)
        year = r.get('year', 0)
        month_name = MONTH_NAMES.get(month, str(month))
        status = determine_status(r)
        lines.append(f"- [{month_name} {year}]({period}.md) — {status}")
    lines.append("")

    # Write file
    filepath = os.path.join(output_dir, "summary.md")
    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    return filepath


def main():
    parser = argparse.ArgumentParser(description="Generate per-month markdown validation reports")
    parser.add_argument("--results", default=RESULTS_FILE, help="Path to pipeline_results.json")
    parser.add_argument("--output", default=OUTPUT_DIR, help="Output directory for reports")

    args = parser.parse_args()

    # Load results
    if not os.path.exists(args.results):
        print(f"Results file not found: {args.results}")
        print("Run the pipeline first: python pipeline.py --all")
        return

    with open(args.results, 'r', encoding='utf-8') as f:
        all_results = json.load(f)

    print(f"Loaded {len(all_results)} month results from {args.results}")

    # Sort by period
    all_results.sort(key=lambda r: r.get('period', ''))

    # Generate per-month reports
    print(f"\nGenerating per-month reports...")
    for r in all_results:
        filepath = generate_month_report(r, args.output)
        status = determine_status(r)
        em = compute_error_metrics(r)
        err_info = f" (errors: {em['total_errors']}, rate: {em['error_rate']:.2f}%)" if em['available'] else ""
        print(f"  {r.get('period', '')}: {status}{err_info} -> {filepath}")

    # Generate summary
    print(f"\nGenerating summary report...")
    summary_path = generate_summary(all_results, args.output)
    print(f"  Summary -> {summary_path}")

    print(f"\nDone! {len(all_results)} reports + summary written to {args.output}/")


if __name__ == "__main__":
    main()
