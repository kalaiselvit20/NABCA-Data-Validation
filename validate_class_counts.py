"""
Validate record counts per class type: CSV vs Supabase.
"""
import csv
from collections import Counter
from validate_brand_summary import run_sql


def validate_class_counts(csv_path='output_raw/brand_summary_all.csv', year=2025, month=9,
                          report_path=None):
    """Validate class-level record counts: CSV vs Supabase.

    Returns:
        dict with keys: total_classes, matching, mismatched, csv_total, supa_total,
        details (list of per-class dicts), status
    """
    period = f"{year}-{month:02d}"
    print("=" * 80)
    print(f"CLASS-LEVEL RECORD COUNT VALIDATION: CSV vs SUPABASE ({period})")
    print("=" * 80)

    # 1. CSV class counts
    print("\nLoading CSV...")
    csv_counts = Counter()
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            brand = row.get('Brand', '').strip()
            if brand and not brand.upper().startswith('TOTAL') and brand not in ['Class & Type', 'Brand']:
                cls = row.get('Class', '').strip().upper()
                csv_counts[cls] += 1

    # 2. Supabase class counts
    print("Fetching Supabase class counts...")
    records = run_sql(f'''
        SELECT class, COUNT(*) as cnt
        FROM "nabca-pre-prod".raw_brand_summary
        WHERE report_year = {year} AND report_month = {month}
        GROUP BY class
        ORDER BY class
    ''')

    supa_counts = {}
    for r in records:
        supa_counts[r['class'].upper().strip()] = int(r['cnt'])

    if not supa_counts:
        print(f"  WARNING: No Supabase data found for {period}")
        return {
            'total_classes': len(csv_counts),
            'matching': 0,
            'mismatched': 0,
            'csv_total': sum(csv_counts.values()),
            'supa_total': 0,
            'details': [],
            'status': 'NO_SUPABASE_DATA',
        }

    # 3. Compare
    all_classes = sorted(set(list(csv_counts.keys()) + list(supa_counts.keys())))

    print(f"\n{'Class':<35} {'CSV':>6} {'Supa':>6} {'Diff':>6}   Status")
    print("-" * 80)

    total_csv = 0
    total_supa = 0
    mismatch_count = 0
    details = []

    for cls in all_classes:
        c = csv_counts.get(cls, 0)
        s = supa_counts.get(cls, 0)
        diff = c - s
        total_csv += c
        total_supa += s

        if diff == 0:
            status = "OK"
        else:
            status = "MISMATCH"
            mismatch_count += 1

        details.append({
            'class': cls, 'csv_count': c, 'supa_count': s,
            'difference': diff, 'status': status
        })
        print(f"{cls:<35} {c:>6} {s:>6} {diff:>+6}   {status}")

    print("-" * 80)
    print(f"{'TOTAL':<35} {total_csv:>6} {total_supa:>6} {total_csv - total_supa:>+6}")

    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}")
    print(f"  Total classes:     {len(all_classes)}")
    print(f"  Matching:          {len(all_classes) - mismatch_count}")
    print(f"  Mismatched:        {mismatch_count}")
    print(f"  CSV total:         {total_csv:,}")
    print(f"  Supabase total:    {total_supa:,}")

    # Write report
    if report_path is None:
        report_path = f'output_raw/class_count_validation_{period}.csv'
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Class', 'CSV_Count', 'Supabase_Count', 'Difference', 'Status'])
        for d in details:
            writer.writerow([d['class'], d['csv_count'], d['supa_count'],
                           d['difference'], d['status']])

    print(f"\n  Report saved to: {report_path}")

    return {
        'total_classes': len(all_classes),
        'matching': len(all_classes) - mismatch_count,
        'mismatched': mismatch_count,
        'csv_total': total_csv,
        'supa_total': total_supa,
        'details': details,
        'status': 'OK',
    }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Validate class counts: CSV vs Supabase')
    parser.add_argument('--csv', default='output_raw/brand_summary_all.csv', help='CSV file path')
    parser.add_argument('--year', type=int, default=2025, help='Report year')
    parser.add_argument('--month', type=int, default=9, help='Report month')
    args = parser.parse_args()
    validate_class_counts(csv_path=args.csv, year=args.year, month=args.month)
