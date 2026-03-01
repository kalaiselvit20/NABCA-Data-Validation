"""
Extract Brand Summary from S3 Textract JSON or local JSON files.
Processes JSON in memory and outputs CSV with built-in validation.
"""
import boto3
import json
import csv
import os
import gc
import time
from collections import Counter

# Import extraction functions from existing script
from extract_brand_summary_raw import (
    extract_page_raw, normalize_class_name,
    KNOWN_CLASS_PREFIXES, PARTIAL_CLASS_NAMES,
    validate_class_totals
)

# S3 Configuration
BUCKET = "inspector-dom-textract"
BASE_PATH = "textract-output/raw-pdfs"

# File mappings: filename -> (year, month, s3_subfolder_or_local_path)
# s3_subfolder: path under BASE_PATH for S3 files
# local files: start with "local:" prefix
S3_FILES = {
    "631_9L_0724": (2024, 7, "631_9L_0724.PDF/20251112_014110"),
    "631_9L_0824": (2024, 8, "631_9L_0824.PDF/20251112_023049"),
    "631_9L_0924": (2024, 9, "631_9L_0924.PDF/20251112_025547"),
    "631_9L_1024": (2024, 10, "631_9L_1024.PDF/20251112_032016"),
    "631_9L_1124": (2024, 11, "631_9L_1124.PDF/20251112_034620"),
    "631_9L_1224": (2024, 12, "631_9L_1224.PDF/20251112_041336"),
    "631_9L_0125": (2025, 1, "631_9L_0125.pdf/20251111_212620"),
    "631_9L_0225": (2025, 2, "631_9L_0225.PDF/20251111_233011"),
    "631_9L_0325": (2025, 3, "631_9L_0325.PDF/20251111_235403"),
    "631_9L_0425": (2025, 4, "631_9L_0425.PDF/20251112_001919"),
    "631_9L_0525": (2025, 5, "631_9L_0525.PDF/20251112_004451"),
    "631_9L_0625": (2025, 6, "631_9L_0625.PDF/20251112_011101"),
    "631_9L_0725": (2025, 7, "631_9L_0725.PDF/20251112_020514"),
    "631_9L_0825": (2025, 8, "631_9L_0825.PDF/20251112_044044"),
    "631_9L_0925": (2025, 9, "local:Textract/631_9L_0925.json"),
}


def get_s3_client():
    """Get boto3 S3 client."""
    return boto3.client('s3')


def load_json_from_s3(s3_client, file_key: str, max_retries: int = 3) -> dict:
    """Load JSON directly from S3 into memory with retry logic."""
    s3_path = f"{BASE_PATH}/{file_key}/textract.json"

    for attempt in range(1, max_retries + 1):
        try:
            print(f"  Loading from s3://{BUCKET}/{s3_path}..." +
                  (f" (attempt {attempt})" if attempt > 1 else ""))

            response = s3_client.get_object(Bucket=BUCKET, Key=s3_path)
            content = response['Body'].read().decode('utf-8')

            print(f"  Parsing JSON ({len(content) / 1024 / 1024:.1f} MB)...")
            data = json.loads(content)

            # Free memory
            del content
            gc.collect()

            return data
        except Exception as e:
            if attempt < max_retries:
                wait = 10 * attempt
                print(f"  S3 read error (attempt {attempt}/{max_retries}): {e}")
                print(f"  Retrying in {wait}s...")
                gc.collect()
                time.sleep(wait)
            else:
                raise


def load_json_from_local(filepath: str) -> dict:
    """Load JSON from local file."""
    print(f"  Loading from local: {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def extract_brand_summary(file_id: str, output_dir: str = "output_raw",
                          start_page: int = 11, end_page: int = 382):
    """Extract Brand Summary from S3 or local Textract JSON with validation.

    Returns:
        dict with extraction stats, validation results, and output file path.
    """
    if file_id not in S3_FILES:
        print(f"Unknown file ID: {file_id}")
        print(f"Available: {list(S3_FILES.keys())}")
        return None

    year, month, source = S3_FILES[file_id]
    period = f"{year}-{month:02d}"

    print(f"\n{'='*70}")
    print(f"EXTRACTING: {file_id} ({period})")
    print(f"{'='*70}")

    # Load JSON (from S3 or local)
    if source.startswith("local:"):
        local_path = source[6:]  # Remove "local:" prefix
        data = load_json_from_local(local_path)
    else:
        s3_client = get_s3_client()
        data = load_json_from_s3(s3_client, source)

    blocks = data.get("Blocks", [])
    blocks_by_id = {b["Id"]: b for b in blocks}
    print(f"  Loaded {len(blocks):,} blocks")

    # Free the raw data dict
    del data
    gc.collect()

    # Extract pages with validation
    print(f"  Extracting Brand Summary (pages {start_page}-{end_page})...")
    all_rows = []
    current_class = ""
    next_expected_class = ""
    validation_log = []
    total_row_values = {}
    grand_total_values = {}

    for page_num in range(start_page, end_page + 1):
        page_rows, current_class, next_expected_class = extract_page_raw(
            blocks, blocks_by_id, page_num, current_class, next_expected_class,
            validation_log, total_row_values, grand_total_values
        )
        all_rows.extend(page_rows)

        if page_num % 50 == 0:
            print(f"    Page {page_num}... ({len(all_rows):,} rows)")

    print(f"  Extraction complete: {len(all_rows):,} rows")

    # Run internal TOTAL validation
    print(f"  Validating class totals...")
    class_validated, class_mismatched = validate_class_totals(
        all_rows, total_row_values, validation_log
    )
    print(f"    Classes validated: {class_validated}")
    print(f"    Classes mismatched: {class_mismatched}")

    # Check for rows without class
    no_class_count = sum(1 for row in all_rows if not row[2] or not row[2].strip())
    if no_class_count > 0:
        print(f"  WARNING: {no_class_count} rows have no class assigned!")

    # Duplicate detection
    seen_keys = Counter()
    duplicate_count = 0
    for row in all_rows:
        if len(row) >= 5:
            key = (str(row[2]).upper().strip(), str(row[3]).upper().strip(), str(row[4]).upper().strip())
            seen_keys[key] += 1
    duplicates = {k: v for k, v in seen_keys.items() if v > 1}
    duplicate_count = len(duplicates)

    # Count validation issues by type
    type_counts = Counter(entry.get('type', 'UNKNOWN') for entry in validation_log)

    # Free blocks from memory
    del blocks, blocks_by_id
    gc.collect()

    # Write CSV
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"brand_summary_{file_id}.csv")

    print(f"  Writing to {output_file}...")
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = [
            "Page", "Row", "Class", "Brand", "Vendor",
            "L12M_Cases_TY", "L12M_Cases_LY", "Pct_of_Type",
            "YTD_Cases", "CurMo_Cases",
            "1.75L", "1.0L", "750ml", "750ml_Trav", "375ml", "200ml", "100ml", "50ml",
            "Avg_Confidence"
        ]
        writer.writerow(header)
        for row in all_rows:
            writer.writerow(row)

    print(f"  Done! {len(all_rows):,} rows written")

    # Count classes
    classes = Counter(row[2] for row in all_rows)

    # Write validation log
    log_path = os.path.join(output_dir, f"extraction_validation_log_{file_id}.csv")
    with open(log_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["type", "page", "row", "class", "brand",
                                                "vendor", "field", "expected", "actual", "warning"])
        writer.writeheader()
        for entry in validation_log:
            writer.writerow({k: entry.get(k, '') for k in writer.fieldnames})
    print(f"  Validation log saved to: {log_path}")

    return {
        "file_id": file_id,
        "period": period,
        "year": year,
        "month": month,
        "rows": len(all_rows),
        "classes": len(classes),
        "classes_validated": class_validated,
        "classes_mismatched": class_mismatched,
        "no_class_rows": no_class_count,
        "duplicate_rows": duplicate_count,
        "bottle_violations": type_counts.get("BOTTLE_SIZE_NO_CURMO", 0),
        "validation_issues": dict(type_counts),
        "output_file": output_file,
        "log_file": log_path,
        "grand_total_values": grand_total_values if grand_total_values else None,
    }


def extract_all_months(output_dir: str = "output_raw"):
    """Extract all available months."""

    print("="*70)
    print("BRAND SUMMARY EXTRACTION FROM S3")
    print("="*70)

    results = []
    for file_id in sorted(S3_FILES.keys()):
        try:
            result = extract_brand_summary(file_id, output_dir)
            if result:
                results.append(result)
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            results.append({
                "file_id": file_id,
                "error": str(e)
            })

    # Summary
    print("\n" + "="*70)
    print("EXTRACTION SUMMARY")
    print("="*70)
    print(f"{'File':<15} {'Period':<10} {'Rows':<10} {'Classes':<10} {'Validated':<10} {'Status'}")
    print("-"*70)
    for r in results:
        if "error" in r:
            print(f"{r['file_id']:<15} {'--':<10} {'--':<10} {'--':<10} {'--':<10} ERROR: {r['error'][:30]}")
        else:
            status = "OK" if r['classes_mismatched'] == 0 else "MISMATCH"
            print(f"{r['file_id']:<15} {r['period']:<10} {r['rows']:<10,} {r['classes']:<10} {r['classes_validated']:<10} {status}")

    return results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract Brand Summary from S3/local")
    parser.add_argument("--file", help="Specific file ID (e.g., 631_9L_0724)")
    parser.add_argument("--all", action="store_true", help="Extract all months")
    parser.add_argument("--list", action="store_true", help="List available files")
    parser.add_argument("--output", default="output_raw", help="Output directory")

    args = parser.parse_args()

    if args.list:
        print("Available files:")
        for file_id, (year, month, source) in sorted(S3_FILES.items()):
            src_type = "local" if source.startswith("local:") else "S3"
            print(f"  {file_id}: {year}-{month:02d} ({src_type})")
        return

    if args.all:
        extract_all_months(args.output)
    elif args.file:
        extract_brand_summary(args.file, args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
