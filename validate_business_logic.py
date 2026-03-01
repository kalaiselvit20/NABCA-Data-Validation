"""
Business logic validation for Brand Summary CSV data.

5 checks (no Supabase needed except grand total uses passed-in values):
1. No negatives - all 12 numeric fields must be >= 0
2. Bottle sum = CurMo_Cases - exact match required
3. L12M_TY >= YTD_Cases - rolling 12 months should be >= year-to-date
4. Pct_of_Type range - 0-100 per row, class sums ~100% (±2.0)
5. Grand total - sum of all rows vs DISTILLED SPIRITS TOTAL from PDF
"""
import csv
import argparse
from collections import defaultdict


NUMERIC_FIELDS = [
    'L12M_Cases_TY', 'L12M_Cases_LY', 'YTD_Cases', 'CurMo_Cases',
    '1.75L', '1.0L', '750ml', '750ml_Trav', '375ml', '200ml', '100ml', '50ml',
]

BOTTLE_FIELDS = ['1.75L', '1.0L', '750ml', '750ml_Trav', '375ml', '200ml', '100ml', '50ml']

SUM_FIELDS = [
    'L12M_Cases_TY', 'L12M_Cases_LY', 'YTD_Cases', 'CurMo_Cases',
    '1.75L', '1.0L', '750ml', '750ml_Trav', '375ml', '200ml', '100ml', '50ml',
]


def parse_int(val):
    """Parse a string value to int, returning 0 for empty/invalid."""
    if val is None or val == '' or val == 'None':
        return 0
    val = str(val).strip().replace(',', '')
    if val in ('', '.', '-'):
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def parse_float(val):
    """Parse a string value to float, returning None for empty/invalid."""
    if val is None or val == '' or val == 'None':
        return None
    val = str(val).strip().replace(',', '')
    if val in ('', '.', '-'):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def load_csv_rows(csv_path):
    """Load CSV rows, skipping header/total rows."""
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            brand = row.get('Brand', '').strip()
            if brand and brand not in ['Class & Type', 'Brand'] and not brand.upper().startswith('TOTAL'):
                rows.append(row)
    return rows


def check_no_negatives(rows):
    """Check 1: All 12 numeric fields must be >= 0."""
    issues = []
    failed = 0
    for i, row in enumerate(rows):
        for field in NUMERIC_FIELDS:
            val = parse_int(row.get(field))
            if val < 0:
                failed += 1
                issues.append({
                    'row': i + 1,
                    'page': row.get('Page', ''),
                    'brand': row.get('Brand', ''),
                    'vendor': row.get('Vendor', ''),
                    'class': row.get('Class', ''),
                    'field': field,
                    'value': val,
                    'check': 'NEGATIVE_VALUE',
                })
    return {
        'passed': len(rows) * len(NUMERIC_FIELDS) - failed,
        'failed': failed,
        'issues': issues,
    }


def check_bottle_sum(rows):
    """Check 2: Sum of bottle sizes must equal CurMo_Cases (exact)."""
    issues = []
    failed = 0
    passed = 0
    skipped = 0
    for i, row in enumerate(rows):
        curmo = parse_int(row.get('CurMo_Cases'))
        bottle_sum = sum(parse_int(row.get(f)) for f in BOTTLE_FIELDS)

        # Skip rows where both are 0 (no current month data)
        if curmo == 0 and bottle_sum == 0:
            skipped += 1
            continue

        if bottle_sum != curmo:
            failed += 1
            issues.append({
                'row': i + 1,
                'page': row.get('Page', ''),
                'brand': row.get('Brand', ''),
                'vendor': row.get('Vendor', ''),
                'class': row.get('Class', ''),
                'expected': curmo,
                'actual': bottle_sum,
                'diff': bottle_sum - curmo,
                'check': 'BOTTLE_SUM_MISMATCH',
            })
        else:
            passed += 1

    return {
        'passed': passed,
        'failed': failed,
        'skipped': skipped,
        'issues': issues,
    }


def check_l12m_vs_ytd(rows):
    """Check 3: L12M_TY should be >= YTD_Cases (warning only)."""
    issues = []
    warnings = 0
    passed = 0
    skipped = 0
    for i, row in enumerate(rows):
        l12m = parse_int(row.get('L12M_Cases_TY'))
        ytd = parse_int(row.get('YTD_Cases'))

        # Skip rows where both are 0
        if l12m == 0 and ytd == 0:
            skipped += 1
            continue

        if l12m < ytd:
            warnings += 1
            issues.append({
                'row': i + 1,
                'page': row.get('Page', ''),
                'brand': row.get('Brand', ''),
                'vendor': row.get('Vendor', ''),
                'class': row.get('Class', ''),
                'l12m': l12m,
                'ytd': ytd,
                'diff': ytd - l12m,
                'check': 'L12M_LT_YTD',
            })
        else:
            passed += 1

    return {
        'passed': passed,
        'failed': warnings,
        'skipped': skipped,
        'issues': issues,
    }


def check_pct_of_type(rows):
    """Check 4: Pct_of_Type should be 0-100 per row, class sums ~100% (±2.0)."""
    issues = []
    row_warnings = 0
    row_passed = 0

    class_pct_sums = defaultdict(float)
    class_row_counts = defaultdict(int)

    for i, row in enumerate(rows):
        pct = parse_float(row.get('Pct_of_Type'))
        cls = row.get('Class', '').upper().strip()

        if pct is not None:
            if pct < 0 or pct > 100:
                row_warnings += 1
                issues.append({
                    'row': i + 1,
                    'page': row.get('Page', ''),
                    'brand': row.get('Brand', ''),
                    'class': cls,
                    'value': pct,
                    'check': 'PCT_OUT_OF_RANGE',
                })
            else:
                row_passed += 1
            class_pct_sums[cls] += pct
            class_row_counts[cls] += 1

    # Check class-level sums
    class_issues = []
    for cls in sorted(class_pct_sums.keys()):
        total_pct = class_pct_sums[cls]
        if abs(total_pct - 100.0) > 2.0:
            class_issues.append({
                'class': cls,
                'total_pct': round(total_pct, 2),
                'diff': round(total_pct - 100.0, 2),
                'row_count': class_row_counts[cls],
                'check': 'CLASS_PCT_SUM',
            })

    return {
        'passed': row_passed,
        'failed': row_warnings,
        'issues': issues,
        'class_sums': {
            'total_classes': len(class_pct_sums),
            'within_tolerance': len(class_pct_sums) - len(class_issues),
            'outside_tolerance': len(class_issues),
            'issues': class_issues,
        },
    }


def check_grand_total(rows, grand_total_values=None):
    """Check 5: Sum of all rows should match DISTILLED SPIRITS TOTAL from PDF."""
    # Sum all rows
    csv_totals = {}
    for field in SUM_FIELDS:
        csv_totals[field] = sum(parse_int(row.get(field)) for row in rows)

    if not grand_total_values:
        return {
            'status': 'SKIPPED',
            'reason': 'No grand total values provided',
            'csv_totals': csv_totals,
        }

    # Compare
    mismatches = []
    matched = 0
    for field in SUM_FIELDS:
        csv_val = csv_totals.get(field, 0)
        pdf_val = grand_total_values.get(field, 0)
        if csv_val != pdf_val and (csv_val != 0 or pdf_val != 0):
            diff = csv_val - pdf_val
            pct = abs(diff) / max(abs(pdf_val), 1) * 100
            mismatches.append({
                'field': field,
                'csv_total': csv_val,
                'pdf_total': pdf_val,
                'diff': diff,
                'pct_off': round(pct, 2),
            })
        else:
            matched += 1

    status = 'PASS' if not mismatches else 'FAIL'

    return {
        'status': status,
        'fields_checked': len(SUM_FIELDS),
        'fields_matched': matched,
        'fields_mismatched': len(mismatches),
        'csv_totals': csv_totals,
        'pdf_totals': grand_total_values,
        'mismatches': mismatches,
    }


def validate_business_logic(csv_path, grand_total_values=None, report_path=None):
    """Run all business logic checks on CSV data.

    Args:
        csv_path: Path to Brand Summary CSV
        grand_total_values: Dict of field->value from DISTILLED SPIRITS TOTAL row
        report_path: Optional path to write detailed report CSV

    Returns:
        dict with: total_rows, checks (per-check results), grand_total,
        overall_status, issues (flat list), status
    """
    print("=" * 80)
    print("BUSINESS LOGIC VALIDATION")
    print("=" * 80)

    rows = load_csv_rows(csv_path)
    total_rows = len(rows)
    print(f"  Loaded {total_rows:,} data rows")

    # Run checks
    print("\n  Check 1: No negative values...")
    negatives = check_no_negatives(rows)
    print(f"    {negatives['failed']} negative values found")

    print("  Check 2: Bottle sum = CurMo_Cases...")
    bottles = check_bottle_sum(rows)
    print(f"    {bottles['passed']:,} passed, {bottles['failed']} failed, {bottles['skipped']:,} skipped")

    print("  Check 3: L12M_TY >= YTD_Cases...")
    l12m = check_l12m_vs_ytd(rows)
    print(f"    {l12m['passed']:,} passed, {l12m['failed']} warnings, {l12m['skipped']:,} skipped")

    print("  Check 4: Pct_of_Type range...")
    pct = check_pct_of_type(rows)
    print(f"    Row-level: {pct['passed']:,} passed, {pct['failed']} out of range")
    cs = pct['class_sums']
    print(f"    Class-level: {cs['within_tolerance']}/{cs['total_classes']} classes within ±2.0% of 100%")

    print("  Check 5: Grand total...")
    grand = check_grand_total(rows, grand_total_values)
    if grand['status'] == 'SKIPPED':
        print(f"    SKIPPED - {grand['reason']}")
    else:
        print(f"    {grand['fields_matched']}/{grand['fields_checked']} fields matched ({grand['status']})")
        for m in grand.get('mismatches', []):
            print(f"      {m['field']}: CSV={m['csv_total']:,} PDF={m['pdf_total']:,} (diff={m['diff']:+,})")

    # Determine overall status
    has_fails = (negatives['failed'] > 0 or
                 bottles['failed'] > 0 or
                 grand.get('status') == 'FAIL')
    has_warnings = (l12m['failed'] > 0 or
                    pct['failed'] > 0 or
                    cs['outside_tolerance'] > 0)

    if has_fails:
        overall_status = 'FAIL'
    elif has_warnings:
        overall_status = 'WARNING'
    else:
        overall_status = 'PASS'

    # Build flat issues list
    all_issues = []
    all_issues.extend(negatives['issues'])
    all_issues.extend(bottles['issues'])
    all_issues.extend(l12m['issues'])
    all_issues.extend(pct['issues'])
    all_issues.extend(pct['class_sums']['issues'])

    # Write report if requested
    if report_path:
        with open(report_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Check', 'Page', 'Row', 'Brand', 'Vendor', 'Class',
                             'Field', 'Expected', 'Actual', 'Diff'])
            for issue in all_issues:
                check = issue.get('check', '')
                page = issue.get('page', '')
                row_num = issue.get('row', '')
                brand = issue.get('brand', '')
                vendor = issue.get('vendor', '')
                cls = issue.get('class', '')

                if check == 'NEGATIVE_VALUE':
                    writer.writerow([check, page, row_num, brand, vendor, cls,
                                     issue['field'], '>=0', issue['value'], ''])
                elif check == 'BOTTLE_SUM_MISMATCH':
                    writer.writerow([check, page, row_num, brand, vendor, cls,
                                     'CurMo_Cases', issue['expected'], issue['actual'], issue['diff']])
                elif check == 'L12M_LT_YTD':
                    writer.writerow([check, page, row_num, brand, vendor, cls,
                                     'L12M vs YTD', issue['l12m'], issue['ytd'], issue['diff']])
                elif check == 'PCT_OUT_OF_RANGE':
                    writer.writerow([check, page, row_num, brand, '', cls,
                                     'Pct_of_Type', '0-100', issue['value'], ''])
                elif check == 'CLASS_PCT_SUM':
                    writer.writerow([check, '', '', '', '', issue.get('class', ''),
                                     'Class Pct Sum', '100±2', issue['total_pct'], issue['diff']])

        print(f"\n  Report saved to: {report_path}")

    # Summary
    print(f"\n  {'='*50}")
    print(f"  BUSINESS LOGIC: {overall_status}")
    print(f"  {'='*50}")

    return {
        'total_rows': total_rows,
        'checks': {
            'negatives': {
                'passed': negatives['passed'],
                'failed': negatives['failed'],
                'issues': len(negatives['issues']),
            },
            'bottle_sum': {
                'passed': bottles['passed'],
                'failed': bottles['failed'],
                'skipped': bottles['skipped'],
                'issues': len(bottles['issues']),
            },
            'l12m_vs_ytd': {
                'passed': l12m['passed'],
                'failed': l12m['failed'],
                'skipped': l12m['skipped'],
                'issues': len(l12m['issues']),
            },
            'pct_of_type': {
                'row_passed': pct['passed'],
                'row_failed': pct['failed'],
                'classes_total': cs['total_classes'],
                'classes_ok': cs['within_tolerance'],
                'classes_off': cs['outside_tolerance'],
            },
        },
        'grand_total': grand,
        'overall_status': overall_status,
        'issues': all_issues,
        'status': overall_status,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Business logic validation for Brand Summary CSV')
    parser.add_argument('--csv', default='output_raw/brand_summary_all.csv', help='CSV file path')
    parser.add_argument('--report', default=None, help='Output report CSV path')
    args = parser.parse_args()

    result = validate_business_logic(
        csv_path=args.csv,
        report_path=args.report or args.csv.replace('.csv', '_business_logic.csv'),
    )
    print(f"\n  Total issues: {len(result['issues'])}")
