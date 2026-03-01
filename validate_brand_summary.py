"""
Validate Brand Summary CSV against Supabase.
Compares extracted data with database records.
"""
import csv
import os
import requests
from collections import defaultdict, Counter

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


def run_sql(query):
    """Execute SQL query on Supabase."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    response = requests.post(
        f'{SUPABASE_URL}/rest/v1/rpc/execute_sql_with_schema',
        headers=headers,
        json={
            'query': query,
            'search_path': ['public'],
            'allowed_schemas': ['nabca-pre-prod', 'public']
        }
    )
    result = response.json()
    if isinstance(result, dict) and 'data' in result:
        return result['data']
    return result if isinstance(result, list) else []


def load_csv(filepath):
    """Load CSV data, skipping header rows."""
    records = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip header rows (Class & Type, Brand headers)
            brand = row.get('Brand', '').strip()
            if brand and brand not in ['Class & Type', 'Brand'] and not brand.upper().startswith('TOTAL'):
                records.append(row)
    return records


def fetch_supabase_data(year, month):
    """Fetch all Supabase records for the period."""
    print(f"Fetching Supabase data for {year}-{month:02d}...")

    # Get total count
    result = run_sql(f'''
        SELECT COUNT(*) as cnt FROM "nabca-pre-prod".raw_brand_summary
        WHERE report_year = {year} AND report_month = {month}
    ''')
    total = result[0]['cnt'] if result else 0
    print(f"  Total records in Supabase: {total}")

    if total == 0:
        return []

    # Fetch all records in batches
    all_records = []
    batch_size = 5000
    offset = 0

    while offset < total:
        result = run_sql(f'''
            SELECT brand, vendor, class,
                   l12m_cases_ty, l12m_cases_ly,
                   ytd_cases_ty, curr_month_cases,
                   curr_month_175l, curr_month_1l, curr_month_750ml,
                   curr_month_750ml_traveler,
                   curr_month_375ml, curr_month_200ml, curr_month_100ml, curr_month_50ml
            FROM "nabca-pre-prod".raw_brand_summary
            WHERE report_year = {year} AND report_month = {month}
            ORDER BY class, brand
            LIMIT {batch_size} OFFSET {offset}
        ''')
        if result:
            all_records.extend(result)
            print(f"  Fetched {len(all_records)}/{total}...")
        offset += batch_size

    return all_records


def normalize_value(val):
    """Normalize numeric values for comparison."""
    if val is None or val == '' or val == 'None':
        return None
    if isinstance(val, str):
        val = val.strip().replace(',', '').replace('.0', '')
        if val in ['', '.', '-']:
            return None
        try:
            return int(float(val))
        except:
            return val
    return int(val) if isinstance(val, (int, float)) else val


def validate(csv_path='output_raw/brand_summary_all.csv', year=2025, month=9,
             error_csv_path=None):
    """Run validation. Returns structured results dict.

    Args:
        csv_path: Path to extracted CSV file
        year: Report year
        month: Report month
        error_csv_path: Path to write error CSV (None = auto-generate)

    Returns:
        dict with keys: csv_records, supa_records, matching_keys, csv_only,
        supa_only, exact_matches, value_errors, key_match_rate, exact_match_rate,
        error_list (flat list for CSV writing)
    """
    period = f"{year}-{month:02d}"
    print("="*80)
    print(f"BRAND SUMMARY VALIDATION: CSV vs SUPABASE ({period})")
    print("="*80)

    # Load CSV
    print("\nLoading CSV data...")
    csv_records = load_csv(csv_path)
    print(f"  CSV records (excluding headers/totals): {len(csv_records)}")

    # Fetch Supabase data
    supa_records = fetch_supabase_data(year, month)

    if not supa_records:
        print(f"  WARNING: No Supabase data found for {period}")
        return {
            'csv_records': len(csv_records),
            'supa_records': 0,
            'matching_keys': 0,
            'csv_only': len(csv_records),
            'supa_only': 0,
            'exact_matches': 0,
            'value_errors': 0,
            'key_match_rate': 0.0,
            'exact_match_rate': 0.0,
            'error_list': [],
            'status': 'NO_SUPABASE_DATA',
        }

    # Build indexes (brand + vendor + class as key)
    print("\nBuilding indexes...")
    csv_index = {}
    for rec in csv_records:
        key = (rec.get('Brand', '').upper().strip(), rec.get('Vendor', '').upper().strip(), rec.get('Class', '').upper().strip())
        if key[0]:  # Only if brand exists
            csv_index[key] = rec

    supa_index = {}
    for rec in supa_records:
        key = (rec.get('brand', '').upper().strip(), rec.get('vendor', '').upper().strip(), rec.get('class', '').upper().strip())
        if key[0]:
            supa_index[key] = rec

    print(f"  CSV unique (brand, vendor, class) triples: {len(csv_index)}")
    print(f"  Supabase unique (brand, vendor, class) triples: {len(supa_index)}")

    # Compare
    csv_keys = set(csv_index.keys())
    supa_keys = set(supa_index.keys())

    matching_keys = csv_keys & supa_keys
    csv_only = csv_keys - supa_keys
    supa_only = supa_keys - csv_keys

    print("\n" + "="*80)
    print("RECORD COUNT COMPARISON")
    print("="*80)
    print(f"  Matching (brand, vendor, class) triples: {len(matching_keys)}")
    print(f"  CSV only (missing in Supabase): {len(csv_only)}")
    print(f"  Supabase only (missing in CSV): {len(supa_only)}")
    key_match_rate = len(matching_keys)/len(csv_keys)*100 if csv_keys else 0
    print(f"  Match rate: {key_match_rate:.1f}%")

    # Value comparison for matching records
    print("\n" + "="*80)
    print("VALUE COMPARISON (for matching records)")
    print("="*80)

    field_mapping = [
        ('L12M_Cases_TY', 'l12m_cases_ty'),
        ('L12M_Cases_LY', 'l12m_cases_ly'),
        ('YTD_Cases', 'ytd_cases_ty'),
        ('CurMo_Cases', 'curr_month_cases'),
        ('1.75L', 'curr_month_175l'),
        ('1.0L', 'curr_month_1l'),
        ('750ml', 'curr_month_750ml'),
        ('375ml', 'curr_month_375ml'),
        ('200ml', 'curr_month_200ml'),
        ('100ml', 'curr_month_100ml'),
        ('50ml', 'curr_month_50ml'),
    ]

    value_errors = []
    exact_matches = 0

    for key in matching_keys:
        csv_rec = csv_index[key]
        supa_rec = supa_index[key]

        row_errors = []
        for csv_field, supa_field in field_mapping:
            csv_val = normalize_value(csv_rec.get(csv_field))
            supa_val = normalize_value(supa_rec.get(supa_field))

            if csv_val != supa_val and (csv_val is not None or supa_val is not None):
                row_errors.append({
                    'field': csv_field,
                    'csv_val': csv_val,
                    'supa_val': supa_val
                })

        if row_errors:
            value_errors.append({
                'brand': key[0],
                'vendor': key[1],
                'class': key[2],
                'page': csv_rec.get('Page', ''),
                'errors': row_errors
            })
        else:
            exact_matches += 1

    exact_match_rate = exact_matches/len(matching_keys)*100 if matching_keys else 0
    print(f"  Exact value matches: {exact_matches} ({exact_match_rate:.1f}%)")
    print(f"  Records with value mismatches: {len(value_errors)}")

    # Show sample errors
    if csv_only:
        print("\n" + "-"*80)
        print(f"SAMPLE: CSV ONLY - Missing in Supabase (first 15 of {len(csv_only)})")
        print("-"*80)
        for key in list(csv_only)[:15]:
            rec = csv_index[key]
            print(f"  Page {rec.get('Page', ''):>3}: {key[0][:30]:<30} | {key[1][:18]:<18} | {key[2]}")

    if supa_only:
        print("\n" + "-"*80)
        print(f"SAMPLE: SUPABASE ONLY - Missing in CSV (first 15 of {len(supa_only)})")
        print("-"*80)
        for key in list(supa_only)[:15]:
            rec = supa_index[key]
            print(f"  {key[0][:30]:<30} | {key[1][:18]:<18} | {key[2]}")

    if value_errors:
        print("\n" + "-"*80)
        print(f"SAMPLE: VALUE MISMATCHES (first 15 of {len(value_errors)})")
        print("-"*80)
        for err in value_errors[:15]:
            print(f"  Page {err['page']:>3}: {err['brand'][:25]:<25} | {err['vendor'][:15]:<15} | {err['class']}")
            for e in err['errors'][:3]:
                print(f"           {e['field']}: CSV={e['csv_val']} vs Supa={e['supa_val']}")

    # Build flat error list
    error_list = []
    for key in csv_only:
        rec = csv_index[key]
        error_list.append({
            'Error_Type': 'MISSING_IN_SUPABASE', 'Page': rec.get('Page', ''),
            'Brand': key[0], 'Vendor': key[1], 'Class': key[2],
            'Field': '', 'CSV_Value': '', 'Supa_Value': ''
        })
    for key in supa_only:
        error_list.append({
            'Error_Type': 'MISSING_IN_CSV', 'Page': '',
            'Brand': key[0], 'Vendor': key[1], 'Class': key[2],
            'Field': '', 'CSV_Value': '', 'Supa_Value': ''
        })
    for err in value_errors:
        for e in err['errors']:
            error_list.append({
                'Error_Type': 'VALUE_MISMATCH', 'Page': err['page'],
                'Brand': err['brand'], 'Vendor': err['vendor'], 'Class': err['class'],
                'Field': e['field'], 'CSV_Value': e['csv_val'], 'Supa_Value': e['supa_val']
            })

    # Write error report CSV
    if error_csv_path is None:
        error_csv_path = f'output_raw/validation_errors_{period}.csv'
    with open(error_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Error_Type', 'Page', 'Brand', 'Vendor', 'Class', 'Field', 'CSV_Value', 'Supa_Value'])
        for e in error_list:
            writer.writerow([e['Error_Type'], e['Page'], e['Brand'], e['Vendor'],
                           e['Class'], e['Field'], e['CSV_Value'], e['Supa_Value']])
    print(f"\n  Error report saved to: {error_csv_path}")

    # Summary
    total_errors = len(csv_only) + len(supa_only) + len(value_errors)
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)
    print(f"""
  CSV Records:                    {len(csv_records):,}
  Supabase Records:               {len(supa_records):,}

  MATCHING:
    Brand+Vendor+Class matches:   {len(matching_keys):,}
    Exact value matches:          {exact_matches:,}

  ERRORS:
    Missing in Supabase:          {len(csv_only):,}
    Missing in CSV:               {len(supa_only):,}
    Value mismatches:             {len(value_errors):,}

  TOTAL ERRORS:                   {total_errors:,}
  MATCH RATE:                     {key_match_rate:.1f}%
  EXACT MATCH RATE:               {exact_match_rate:.1f}%
""")

    return {
        'csv_records': len(csv_records),
        'supa_records': len(supa_records),
        'matching_keys': len(matching_keys),
        'csv_only': len(csv_only),
        'supa_only': len(supa_only),
        'exact_matches': exact_matches,
        'value_errors': len(value_errors),
        'key_match_rate': round(key_match_rate, 2),
        'exact_match_rate': round(exact_match_rate, 2),
        'error_list': error_list,
        'status': 'OK',
    }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Validate Brand Summary CSV vs Supabase')
    parser.add_argument('--csv', default='output_raw/brand_summary_all.csv', help='CSV file path')
    parser.add_argument('--year', type=int, default=2025, help='Report year')
    parser.add_argument('--month', type=int, default=9, help='Report month')
    args = parser.parse_args()
    validate(csv_path=args.csv, year=args.year, month=args.month)
