"""
Enhanced CSV vs Supabase count validation with data quality checks.

Superset of validate_class_counts.py:
- Total count comparison
- Per-class count comparison
- Page distribution stats
- Duplicate detection
- Missing class detection
- Summary statistics
"""
import csv
import argparse
from collections import Counter, defaultdict

from validate_brand_summary import run_sql


def load_csv_data(csv_path):
    """Load CSV rows (excluding headers/totals) and return list of dicts."""
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            brand = row.get('Brand', '').strip()
            if brand and brand not in ['Class & Type', 'Brand'] and not brand.upper().startswith('TOTAL'):
                rows.append(row)
    return rows


def fetch_supabase_counts(year, month):
    """Fetch per-class counts from Supabase."""
    print(f"  Fetching Supabase counts for {year}-{month:02d}...")

    # Total count
    result = run_sql(f'''
        SELECT COUNT(*) as cnt FROM "nabca-pre-prod".raw_brand_summary
        WHERE report_year = {year} AND report_month = {month}
    ''')
    total = result[0]['cnt'] if result else 0

    if total == 0:
        return 0, {}

    # Per-class counts
    result = run_sql(f'''
        SELECT class, COUNT(*) as cnt
        FROM "nabca-pre-prod".raw_brand_summary
        WHERE report_year = {year} AND report_month = {month}
        GROUP BY class
        ORDER BY class
    ''')

    class_counts = {}
    for r in result:
        cls = r['class'].upper().strip() if r['class'] else ''
        class_counts[cls] = int(r['cnt'])

    return total, class_counts


def analyze_page_distribution(rows):
    """Analyze row distribution across pages."""
    page_counts = Counter()
    for row in rows:
        page = row.get('Page', '').strip()
        if page:
            try:
                page_counts[int(page)] += 1
            except (ValueError, TypeError):
                pass

    if not page_counts:
        return {'total_pages': 0}

    pages = sorted(page_counts.keys())
    counts = list(page_counts.values())

    # Find pages with 0 rows (gaps in the page range)
    if pages:
        all_pages = set(range(min(pages), max(pages) + 1))
        empty_pages = sorted(all_pages - set(pages))
    else:
        empty_pages = []

    return {
        'total_pages': len(page_counts),
        'page_range': f"{min(pages)}-{max(pages)}" if pages else 'N/A',
        'min_rows': min(counts),
        'max_rows': max(counts),
        'avg_rows': round(sum(counts) / len(counts), 1),
        'empty_pages': len(empty_pages),
        'empty_page_list': empty_pages[:20],  # Cap at 20 for display
    }


def find_duplicates(rows):
    """Find duplicate (Brand, Vendor, Class) keys with page numbers."""
    key_pages = defaultdict(list)
    for row in rows:
        brand = row.get('Brand', '').upper().strip()
        vendor = row.get('Vendor', '').upper().strip()
        cls = row.get('Class', '').upper().strip()
        page = row.get('Page', '')
        if brand:
            key_pages[(brand, vendor, cls)].append(page)

    duplicates = []
    for key, pages in key_pages.items():
        if len(pages) > 1:
            duplicates.append({
                'brand': key[0],
                'vendor': key[1],
                'class': key[2],
                'count': len(pages),
                'pages': sorted(set(pages)),
            })

    return sorted(duplicates, key=lambda d: -d['count'])


def find_missing_class(rows):
    """Find records with empty or invalid Class field."""
    from extract_brand_summary_raw import KNOWN_CLASS_PREFIXES
    known_set = set(c.upper() for c in KNOWN_CLASS_PREFIXES)

    missing = []
    invalid = []
    for i, row in enumerate(rows):
        cls = row.get('Class', '').strip()
        if not cls:
            missing.append({
                'row': i + 1,
                'page': row.get('Page', ''),
                'brand': row.get('Brand', ''),
                'vendor': row.get('Vendor', ''),
            })
        elif cls.upper() not in known_set:
            invalid.append({
                'row': i + 1,
                'page': row.get('Page', ''),
                'brand': row.get('Brand', ''),
                'class': cls,
            })

    return {
        'empty_count': len(missing),
        'invalid_count': len(invalid),
        'empty_records': missing[:20],
        'invalid_records': invalid[:20],
    }


def validate_counts(csv_path, year, month, report_path=None):
    """Enhanced CSV vs Supabase count validation.

    Args:
        csv_path: Path to Brand Summary CSV
        year: Report year
        month: Report month
        report_path: Optional path for detailed report CSV

    Returns:
        dict with total_csv, total_supa, total_diff, class_comparison,
        page_distribution, data_quality, summary_stats, overall_status, status
    """
    period = f"{year}-{month:02d}"
    print("=" * 80)
    print(f"ENHANCED COUNT VALIDATION: CSV vs SUPABASE ({period})")
    print("=" * 80)

    # Load CSV
    print("\n  Loading CSV...")
    rows = load_csv_data(csv_path)
    total_csv = len(rows)
    print(f"  CSV records: {total_csv:,}")

    # CSV class counts
    csv_class_counts = Counter()
    for row in rows:
        cls = row.get('Class', '').strip().upper()
        csv_class_counts[cls] += 1

    # Fetch Supabase
    total_supa, supa_class_counts = fetch_supabase_counts(year, month)
    print(f"  Supabase records: {total_supa:,}")

    # ---- Total Count Comparison ----
    total_diff = total_csv - total_supa
    print(f"\n  Total count: CSV={total_csv:,} Supa={total_supa:,} Diff={total_diff:+,}")

    # ---- Per-Class Comparison ----
    all_classes = sorted(set(list(csv_class_counts.keys()) + list(supa_class_counts.keys())))

    class_details = []
    matching = 0
    mismatched = 0

    print(f"\n  {'Class':<35} {'CSV':>6} {'Supa':>6} {'Diff':>6}   Status")
    print(f"  {'-'*75}")

    for cls in all_classes:
        c = csv_class_counts.get(cls, 0)
        s = supa_class_counts.get(cls, 0)
        diff = c - s

        if diff == 0:
            status = 'OK'
            matching += 1
        else:
            status = 'MISMATCH'
            mismatched += 1

        class_details.append({
            'class': cls,
            'csv_count': c,
            'supa_count': s,
            'difference': diff,
            'status': status,
        })
        print(f"  {cls:<35} {c:>6} {s:>6} {diff:>+6}   {status}")

    print(f"  {'-'*75}")
    print(f"  {'TOTAL':<35} {total_csv:>6} {total_supa:>6} {total_diff:>+6}")

    # ---- Page Distribution ----
    print(f"\n  --- Page Distribution ---")
    page_dist = analyze_page_distribution(rows)
    if page_dist['total_pages'] > 0:
        print(f"  Pages with data: {page_dist['total_pages']} ({page_dist['page_range']})")
        print(f"  Rows per page: min={page_dist['min_rows']} max={page_dist['max_rows']} avg={page_dist['avg_rows']}")
        if page_dist['empty_pages'] > 0:
            print(f"  Pages with 0 rows: {page_dist['empty_pages']}")

    # ---- Duplicates ----
    print(f"\n  --- Duplicate Detection ---")
    duplicates = find_duplicates(rows)
    print(f"  Duplicate (Brand, Vendor, Class) keys: {len(duplicates)}")
    if duplicates:
        for d in duplicates[:5]:
            print(f"    {d['brand'][:25]} | {d['vendor'][:15]} | {d['class'][:25]} x{d['count']} (pages: {', '.join(str(p) for p in d['pages'][:5])})")

    # ---- Missing Class ----
    print(f"\n  --- Missing/Invalid Class ---")
    class_quality = find_missing_class(rows)
    print(f"  Records with empty class: {class_quality['empty_count']}")
    print(f"  Records with invalid class: {class_quality['invalid_count']}")

    # ---- Summary Stats ----
    if csv_class_counts:
        sorted_classes = sorted(csv_class_counts.items(), key=lambda x: -x[1])
        largest = sorted_classes[0]
        smallest = sorted_classes[-1]
        avg_per_class = total_csv / len(csv_class_counts)
    else:
        largest = ('N/A', 0)
        smallest = ('N/A', 0)
        avg_per_class = 0

    summary_stats = {
        'total_classes': len(all_classes),
        'csv_classes': len(csv_class_counts),
        'supa_classes': len(supa_class_counts),
        'largest_class': {'name': largest[0], 'count': largest[1]},
        'smallest_class': {'name': smallest[0], 'count': smallest[1]},
        'avg_per_class': round(avg_per_class, 1),
    }

    # ---- Determine Overall Status ----
    if total_supa == 0:
        overall_status = 'N/A'
    elif total_diff == 0 and mismatched == 0:
        overall_status = 'PASS'
    elif abs(total_diff) <= 5 and mismatched <= 3:
        overall_status = 'WARNING'
    else:
        overall_status = 'FAIL'

    # ---- Write Report ----
    if report_path:
        with open(report_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Class comparison
            writer.writerow(['=== Class Count Comparison ==='])
            writer.writerow(['Class', 'CSV_Count', 'Supabase_Count', 'Difference', 'Status'])
            for d in class_details:
                writer.writerow([d['class'], d['csv_count'], d['supa_count'],
                                 d['difference'], d['status']])
            writer.writerow(['TOTAL', total_csv, total_supa, total_diff, overall_status])

            # Duplicates
            writer.writerow([])
            writer.writerow(['=== Duplicates ==='])
            writer.writerow(['Brand', 'Vendor', 'Class', 'Count', 'Pages'])
            for d in duplicates:
                writer.writerow([d['brand'], d['vendor'], d['class'],
                                 d['count'], '; '.join(str(p) for p in d['pages'])])

        print(f"\n  Report saved to: {report_path}")

    # ---- Summary ----
    print(f"\n  {'='*50}")
    print(f"  COUNT VALIDATION: {overall_status}")
    print(f"    Classes: {matching} matched, {mismatched} mismatched")
    print(f"    Total: CSV={total_csv:,} Supa={total_supa:,}")
    print(f"  {'='*50}")

    return {
        'total_csv': total_csv,
        'total_supa': total_supa,
        'total_diff': total_diff,
        'class_comparison': {
            'total_classes': len(all_classes),
            'matching': matching,
            'mismatched': mismatched,
            'details': class_details,
        },
        'page_distribution': page_dist,
        'data_quality': {
            'duplicates': len(duplicates),
            'duplicate_details': duplicates[:20],
            'missing_class': class_quality['empty_count'],
            'invalid_class': class_quality['invalid_count'],
        },
        'summary_stats': summary_stats,
        'overall_status': overall_status,
        'status': overall_status if total_supa > 0 else 'NO_SUPABASE_DATA',
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Enhanced count validation: CSV vs Supabase')
    parser.add_argument('--csv', default='output_raw/brand_summary_all.csv', help='CSV file path')
    parser.add_argument('--year', type=int, default=2025, help='Report year')
    parser.add_argument('--month', type=int, default=9, help='Report month')
    parser.add_argument('--report', default=None, help='Output report CSV path')
    args = parser.parse_args()

    result = validate_counts(
        csv_path=args.csv,
        year=args.year,
        month=args.month,
        report_path=args.report,
    )
