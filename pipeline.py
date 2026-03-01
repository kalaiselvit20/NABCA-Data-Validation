"""
Pipeline: Extract and validate Brand Summary for all months.
Processes one month at a time from S3, validates against Supabase,
and generates consolidated client-facing reports.
"""
import os
import gc
import json
import argparse
from datetime import datetime

from extract_from_s3 import extract_brand_summary, S3_FILES
from validate_brand_summary import validate as validate_supabase
from validate_business_logic import validate_business_logic
from validate_counts import validate_counts
from report_generator import generate_all_reports, determine_status, get_period_label

OUTPUT_DIR = "output_raw"
REPORTS_DIR = "reports"
RESULTS_FILE = "reports/pipeline_results.json"


def process_month(file_id, output_dir=OUTPUT_DIR, skip_supabase=False):
    """Process a single month: extract, validate, return results.

    Returns dict with all extraction and validation results.
    """
    if file_id not in S3_FILES:
        print(f"Unknown file ID: {file_id}")
        return None

    year, month, _ = S3_FILES[file_id]
    period = f"{year}-{month:02d}"

    print(f"\n{'#'*70}")
    print(f"# PROCESSING: {file_id} ({period})")
    print(f"{'#'*70}")

    result = {
        'file_id': file_id,
        'period': period,
        'year': year,
        'month': month,
    }

    # Step 1: Extract
    try:
        extraction = extract_brand_summary(file_id, output_dir)
        if not extraction:
            result['error'] = 'Extraction returned None'
            return result

        result.update({
            'rows': extraction['rows'],
            'classes': extraction['classes'],
            'classes_validated': extraction['classes_validated'],
            'classes_mismatched': extraction['classes_mismatched'],
            'no_class_rows': extraction['no_class_rows'],
            'duplicate_rows': extraction['duplicate_rows'],
            'bottle_violations': extraction['bottle_violations'],
            'validation_issues': extraction['validation_issues'],
        })
        csv_path = extraction['output_file']
        grand_total_values = extraction.get('grand_total_values')

    except Exception as e:
        print(f"  EXTRACTION ERROR: {e}")
        import traceback
        traceback.print_exc()
        result['error'] = f'Extraction error: {str(e)}'
        return result

    # Step 2: Business Logic Validation (CSV only, no Supabase needed)
    try:
        print(f"\n  --- Business Logic Validation ---")
        bl_result = validate_business_logic(
            csv_path=csv_path,
            grand_total_values=grand_total_values,
            report_path=os.path.join(output_dir, f"business_logic_{file_id}.csv")
        )
        result['business_logic'] = bl_result

    except Exception as e:
        print(f"  BUSINESS LOGIC VALIDATION ERROR: {e}")
        import traceback
        traceback.print_exc()
        result['business_logic'] = {'status': 'ERROR', 'error': str(e)}

    # Step 3: Validate vs Supabase
    if not skip_supabase:
        try:
            print(f"\n  --- Supabase Validation ---")
            supa_result = validate_supabase(
                csv_path=csv_path,
                year=year,
                month=month,
                error_csv_path=os.path.join(output_dir, f"validation_errors_{file_id}.csv")
            )
            result['supabase'] = supa_result

        except Exception as e:
            print(f"  SUPABASE VALIDATION ERROR: {e}")
            import traceback
            traceback.print_exc()
            result['supabase'] = {'status': 'ERROR', 'error': str(e)}

        # Step 4: Enhanced Count Validation (replaces class_counts)
        try:
            print(f"\n  --- Enhanced Count Validation ---")
            counts_result = validate_counts(
                csv_path=csv_path,
                year=year,
                month=month,
                report_path=os.path.join(output_dir, f"count_validation_{file_id}.csv")
            )
            result['counts'] = counts_result
            # Maintain backward compatibility for class_counts key
            result['class_counts'] = {
                'total_classes': counts_result.get('class_comparison', {}).get('total_classes', 0),
                'matching': counts_result.get('class_comparison', {}).get('matching', 0),
                'mismatched': counts_result.get('class_comparison', {}).get('mismatched', 0),
                'csv_total': counts_result.get('total_csv', 0),
                'supa_total': counts_result.get('total_supa', 0),
                'details': counts_result.get('class_comparison', {}).get('details', []),
                'status': counts_result.get('status', 'OK'),
            }

        except Exception as e:
            print(f"  COUNT VALIDATION ERROR: {e}")
            import traceback
            traceback.print_exc()
            result['counts'] = {'status': 'ERROR', 'error': str(e)}
            result['class_counts'] = {'status': 'ERROR', 'error': str(e)}
    else:
        print(f"  Skipping Supabase validation (--skip-supabase)")
        result['supabase'] = {'status': 'SKIPPED'}
        result['counts'] = {'status': 'SKIPPED'}
        result['class_counts'] = {'status': 'SKIPPED'}

    # Step 3: Cleanup large CSV to save storage
    if os.path.exists(csv_path):
        csv_size = os.path.getsize(csv_path) / (1024 * 1024)
        os.remove(csv_path)
        print(f"\n  Cleaned up {csv_path} ({csv_size:.1f} MB freed)")

    # Also cleanup extraction validation log
    log_path = extraction.get('log_file', '')
    if log_path and os.path.exists(log_path):
        os.remove(log_path)

    gc.collect()

    # Print month summary
    st = determine_status(result)
    print(f"\n  {'='*50}")
    print(f"  MONTH RESULT: {period} = {st}")
    print(f"  {'='*50}")

    return result


def save_results(all_results, filepath=RESULTS_FILE):
    """Save results to JSON for later report generation."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Remove non-serializable data (error_list with complex objects)
    serializable = []
    for r in all_results:
        r_copy = dict(r)
        supa = r_copy.get('supabase', {})
        if supa and isinstance(supa, dict):
            supa_copy = dict(supa)
            # Convert error_list values to strings
            error_list = supa_copy.get('error_list', [])
            clean_errors = []
            for e in error_list:
                clean_e = {}
                for k, v in e.items():
                    clean_e[k] = str(v) if v is not None else ''
                clean_errors.append(clean_e)
            supa_copy['error_list'] = clean_errors
            r_copy['supabase'] = supa_copy
        serializable.append(r_copy)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(serializable, f, indent=2, default=str)
    print(f"\n  Results saved to: {filepath}")


def load_results(filepath=RESULTS_FILE):
    """Load previous results from JSON."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def run_pipeline(file_ids=None, skip_supabase=False, output_dir=OUTPUT_DIR,
                 reports_dir=REPORTS_DIR):
    """Run full pipeline for specified months (or all)."""
    if file_ids is None:
        file_ids = sorted(S3_FILES.keys())

    print("="*70)
    print("NABCA BRAND SUMMARY - EXTRACTION & VALIDATION PIPELINE")
    print(f"Processing {len(file_ids)} months")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # Load any previous results (for resume support)
    all_results = load_results()
    processed_ids = {r['file_id'] for r in all_results if not r.get('error')}

    for i, file_id in enumerate(file_ids, 1):
        if file_id in processed_ids:
            print(f"\n  Skipping {file_id} (already processed)")
            continue

        print(f"\n  [{i}/{len(file_ids)}] Processing {file_id}...")
        result = process_month(file_id, output_dir, skip_supabase)
        if result:
            # Remove any previous failed attempt
            all_results = [r for r in all_results if r.get('file_id') != file_id]
            all_results.append(result)

            # Save after each month (in case of crash)
            save_results(all_results)

    # Sort by period
    all_results.sort(key=lambda r: r.get('period', ''))

    # Generate consolidated reports
    print("\n" + "="*70)
    print("GENERATING CONSOLIDATED REPORTS")
    print("="*70)
    generate_all_reports(all_results, reports_dir)

    # Final summary
    print("\n" + "="*70)
    print("PIPELINE COMPLETE")
    print("="*70)
    print(f"{'Period':<12} {'Rows':>8} {'Classes':>8} {'Key Match':>10} {'Exact Match':>12} {'Status':>8}")
    print("-"*70)

    for r in all_results:
        supa = r.get('supabase', {}) or {}
        st = determine_status(r)
        kr = f"{supa.get('key_match_rate', 0):.1f}%" if supa.get('status') not in ('NO_SUPABASE_DATA', 'SKIPPED', 'ERROR') else 'N/A'
        er = f"{supa.get('exact_match_rate', 0):.1f}%" if supa.get('status') not in ('NO_SUPABASE_DATA', 'SKIPPED', 'ERROR') else 'N/A'
        print(f"{r.get('period', ''):<12} {r.get('rows', 0):>8,} {r.get('classes', 0):>8} {kr:>10} {er:>12} {st:>8}")

    print(f"\nReports saved to: {reports_dir}/")
    print(f"  - consolidated_validation.csv")
    print(f"  - consolidated_validation.xlsx")
    print(f"  - consolidated_validation.pdf")
    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return all_results


def main():
    parser = argparse.ArgumentParser(description="NABCA Brand Summary Pipeline")
    parser.add_argument("--file", help="Process single file ID (e.g., 631_9L_0724)")
    parser.add_argument("--all", action="store_true", help="Process all months")
    parser.add_argument("--list", action="store_true", help="List available files")
    parser.add_argument("--skip-supabase", action="store_true",
                        help="Skip Supabase validation")
    parser.add_argument("--output", default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--reports", default=REPORTS_DIR, help="Reports directory")
    parser.add_argument("--report-only", action="store_true",
                        help="Generate reports from saved results (no extraction)")
    parser.add_argument("--reset", action="store_true",
                        help="Clear saved results and start fresh")

    args = parser.parse_args()

    if args.list:
        print("Available files:")
        for file_id, (year, month, source) in sorted(S3_FILES.items()):
            src_type = "local" if source.startswith("local:") else "S3"
            print(f"  {file_id}: {year}-{month:02d} ({src_type})")
        return

    if args.reset:
        if os.path.exists(RESULTS_FILE):
            os.remove(RESULTS_FILE)
            print("Results cleared.")
        return

    if args.report_only:
        results = load_results()
        if not results:
            print("No saved results found. Run pipeline first.")
            return
        print(f"Loaded {len(results)} month results")
        generate_all_reports(results, args.reports)
        return

    if args.all:
        run_pipeline(skip_supabase=args.skip_supabase,
                     output_dir=args.output, reports_dir=args.reports)
    elif args.file:
        result = process_month(args.file, args.output, args.skip_supabase)
        if result:
            # Save single result
            all_results = load_results()
            all_results = [r for r in all_results if r.get('file_id') != args.file]
            all_results.append(result)
            all_results.sort(key=lambda r: r.get('period', ''))
            save_results(all_results)
            generate_all_reports(all_results, args.reports)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
