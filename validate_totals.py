"""
Validate Brand Summary extraction by comparing class-level sums
against TOTAL rows from the PDF itself.

Self-validation: sum of extracted rows per class vs PDF TOTAL row.
"""
import json
import csv
import re
from collections import defaultdict

from extract_brand_summary_raw import normalize_class_name, KNOWN_CLASS_PREFIXES

# Manual mapping for OCR-corrupted TOTAL row class names.
# Keys are the combined (brand + vendor) text after removing "TOTAL" prefix.
TOTAL_ROW_CLASS_FIXES = {
    # DOM WHSKY
    "DOM WHSKY -STRT-BRBN/TN": "DOM WHSKY-STRT-BRBN/TN",
    "DOM WHSKY-STRT-SM": "DOM WHSKY-STRT-SM BTCH",
    "DOM WHSKY-STRT-SM BTCH": "DOM WHSKY-STRT-SM BTCH",
    "DOM WHSKY SNGL MALT": "DOM WHSKY-SNGL MALT",
    # SCOTCH
    "SCOTCH-BL ND-FRGN BTLD": "SCOTCH-BLND-FRGN BTLD",
    "SCOTCH-BL ND-US BTLD": "SCOTCH-BLND-US BTLD",
    "SCOTCH-SNGL MALT": "SCOTCH-SNGL MALT",
    "SCOTCH-SNGL": "SCOTCH-SNGL MALT",
    # CAN
    "CAN-FRGN BLND-FRGN BTL": "CAN-FRGN BLND-FRGN BTLD",
    "CAN-FRGN BLND-FRGN BTLD": "CAN-FRGN BLND-FRGN BTLD",
    "CAN-US BLND-US BTLD": "CAN-US BLND-US BTLD",
    # IRISH
    "IRISH-SNGL MALT": "IRISH-SNGL MALT",
    "IRISH-SNGL": "IRISH-SNGL MALT",
    # GIN
    "GIN-CLASS SIC-DOM": "GIN-CLASSIC-DOM",
    "GIN-CLASS SIC-IMP": "GIN-CLASSIC-IMP",
    # VODKA
    "VODKA-CL ASSIC-DOM": "VODKA-CLASSIC-DOM",
    "VODKA-CL ASSIC-IMP": "VODKA-CLASSIC-IMP",
    "VODKA-FL VRD-DOM": "VODKA-FLVRD-DOM",
    "VODKA-FL VRD-IMP": "VODKA-FLVRD-IMP",
    # NEUTRAL
    "NEUTRAL GRAIN SPIRIT": "NEUTRAL GRAIN SPIRIT",
    "NEUTRAL GRAIN": "NEUTRAL GRAIN SPIRIT",
    "NEUTRAL": "NEUTRAL GRAIN SPIRIT",
    # RUM
    "RUM-AGED DARK": "RUM-AGED/DARK",
    "RUM-AGED/DARK": "RUM-AGED/DARK",
    "RUM-AGED": "RUM-AGED/DARK",
    # OTH IMP WHSKY
    "OTH IMP WHSKY-SNGL MAL": "OTH IMP WHSKY-SNGL MALT",
    "OTH IMP WHSKY-SNGL MALT": "OTH IMP WHSKY-SNGL MALT",
    "OTH IMP WHSKY-SNGL": "OTH IMP WHSKY-SNGL MALT",
    # CRDL-LQR&SPC (Textract splits "&" or garbles it as "8")
    "SPC-AMRT": "CRDL-LQR&SPC-AMRT",
    "SPC-CRM": "CRDL-LQR&SPC-CRM",
    "SPC-HZLNT": "CRDL-LQR&SPC-HZLNT",
    "SPC-FRT": "CRDL-LQR&SPC-FRT",
    "SPC-OTH": "CRDL-LQR&SPC-OTH",
    "SPC-TRIPLE SE": "CRDL-LQR&SPC-TRIPLE SEC",
    "SPC-TRIPLE SEC": "CRDL-LQR&SPC-TRIPLE SEC",
    "SPC-WHSKY": "CRDL-LQR&SPC-WHSKY",
    "SPC-SPRT SPCTY": "CRDL-LQR&SPC-SPRT SPCTY",
    "SPC-SPRT SPCT": "CRDL-LQR&SPC-SPRT SPCTY",
    "SPC-ANSE FLVRD": "CRDL-LQR&SPC-ANSE FLVRD",
    "SPC-ANSE FLVR": "CRDL-LQR&SPC-ANSE FLVRD",
    "SPC-SLOE GIN": "CRDL-LQR&SPC-SLOE GIN",
    "SPC-CURACAO": "CRDL-LQR&SPC-CURACAO",
    "CRDL-LQR &SPC-ANSE FLVR": "CRDL-LQR&SPC-ANSE FLVRD",
    "CRDL-LQR &SPC-SLOE GIN": "CRDL-LQR&SPC-SLOE GIN",
    "CRDL-LQR8 SPC-FRT": "CRDL-LQR&SPC-FRT",
    "CRDL-LQR8 SPC-TRIPLE SE": "CRDL-LQR&SPC-TRIPLE SEC",
    "CRDL &SPC-WHSKY SPC": "CRDL-LQR&SPC-WHSKY",
    "CRDL SPC-CURACAO": "CRDL-LQR&SPC-CURACAO",
    "CRDL-LQR SPC-SPRT SPCT": "CRDL-LQR&SPC-SPRT SPCTY",
    "CRDL SPC": "CRDL-LQR&SPC-WHSKY",
    # CRDL-SNPS
    "SNPS-PPRMNT": "CRDL-SNPS-PPRMNT",
    "SNPS-APPL": "CRDL-SNPS-APPL",
    "SNPS-BTRSCTCH": "CRDL-SNPS-BTRSCTCH",
    "SNPS-CNNMN": "CRDL-SNPS-CNNMN",
    "SNPS-PEACH": "CRDL-SNPS-PEACH",
    "SNPS-OTH": "CRDL-SNPS-OTH",
    "-BTRSCTCH": "CRDL-SNPS-BTRSCTCH",
    "CRDL PPRMNT": "CRDL-SNPS-PPRMNT",
    # TEQUILA
    "BLANCO": "TEQUILA-BLANCO",
    "TEQUILA- CRISTALINO": "TEQUILA-CRISTALINO",
    "TEQUILA-| LAVORED": "TEQUILA-FLAVORED",
    "TEQUILA-|LAVORED": "TEQUILA-FLAVORED",
    "TEQUILA- GOLD": "TEQUILA-GOLD",
    "TEQUILA- REPOSADO": "TEQUILA-REPOSADO",
    "TEQUILA- BLANCO": "TEQUILA-BLANCO",
    "TEQUILA- ANEJO": "TEQUILA-ANEJO",
    # MEZCAL
    "MEZCAL CRISTALINO 9": "MEZCAL-CRISTALINO",
    "MEZCAL CRISTALINO": "MEZCAL-CRISTALINO",
    "MEZCAL-CRISTALINO": "MEZCAL-CRISTALINO",
}


def fix_total_class_name(raw_name: str) -> str:
    """Fix OCR-corrupted class names from TOTAL rows."""
    upper = raw_name.upper().strip()
    # Check exact fix first
    if upper in TOTAL_ROW_CLASS_FIXES:
        return TOTAL_ROW_CLASS_FIXES[upper]
    # Try normalize
    return normalize_class_name(upper)


def get_page_column_boundaries(blocks, blocks_by_id, page_num):
    """Get column boundaries for a page from Textract TABLE/CELL blocks."""
    tables = [b for b in blocks if b["BlockType"] == "TABLE" and b.get("Page") == page_num]
    if not tables:
        return [], None
    table = tables[0]
    table_bbox = table.get("Geometry", {}).get("BoundingBox", {})

    cells = []
    for rel in table.get("Relationships", []):
        if rel["Type"] == "CHILD":
            for cell_id in rel.get("Ids", []):
                cell = blocks_by_id.get(cell_id)
                if cell and cell["BlockType"] == "CELL":
                    cells.append(cell)

    if not cells:
        return [], table_bbox

    col_data = defaultdict(list)
    for cell in cells:
        col_idx = cell.get("ColumnIndex", 1)
        bbox = cell.get("Geometry", {}).get("BoundingBox", {})
        left = bbox.get("Left", 0)
        right = left + bbox.get("Width", 0)
        col_data[col_idx].append((left, right))

    boundaries = []
    for col_idx in sorted(col_data.keys()):
        lefts = [x[0] for x in col_data[col_idx]]
        rights = [x[1] for x in col_data[col_idx]]
        boundaries.append({"col": col_idx, "left": min(lefts), "right": max(rights)})

    return boundaries, table_bbox


def assign_word_to_column(x, boundaries):
    """Assign word X position to a column using boundaries."""
    for b in boundaries:
        if b["left"] <= x <= b["right"]:
            return b["col"]
    # Fallback: closest column center
    min_dist = float('inf')
    best_col = 1
    for b in boundaries:
        mid = (b["left"] + b["right"]) / 2
        dist = abs(x - mid)
        if dist < min_dist:
            min_dist = dist
            best_col = b["col"]
    return best_col


def extract_total_rows(json_path: str, start_page: int = 11, end_page: int = 382):
    """Extract TOTAL rows from Textract JSON using column-boundary-based parsing.

    Uses the same column boundaries from Textract CELL blocks as the extraction
    script, ensuring consistent column assignment for TOTAL row values.
    """

    print(f"Loading {json_path}...")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    blocks = data["Blocks"]
    blocks_by_id = {b["Id"]: b for b in blocks}
    del data

    # Column index to field name mapping (1-based column index)
    COL_FIELD_MAP = {
        3: "L12M_Cases_TY", 4: "L12M_Cases_LY", 5: "Pct_of_Type",
        6: "YTD_Cases", 7: "CurMo_Cases",
        8: "1.75L", 9: "1.0L", 10: "750ml", 11: "750ml_Trav",
        12: "375ml", 13: "200ml", 14: "100ml", 15: "50ml",
    }

    total_rows = []

    for page_num in range(start_page, end_page + 1):
        # Get column boundaries for this page
        boundaries, table_bbox = get_page_column_boundaries(blocks, blocks_by_id, page_num)
        if not boundaries or not table_bbox:
            continue

        table_top = table_bbox.get("Top", 0)
        table_bottom = table_top + table_bbox.get("Height", 1)

        # Get words within table bounds
        page_words = [b for b in blocks if b["BlockType"] == "WORD" and b.get("Page") == page_num]
        table_words = [w for w in page_words
                       if table_top <= w["Geometry"]["BoundingBox"]["Top"] <= table_bottom]

        # Group words by Y position
        y_groups = defaultdict(list)
        for w in table_words:
            y = w["Geometry"]["BoundingBox"]["Top"]
            y_groups[round(y, 3)].append(w)

        for y in sorted(y_groups.keys()):
            words = sorted(y_groups[y], key=lambda w: w["Geometry"]["BoundingBox"]["Left"])
            texts = [w["Text"] for w in words]

            if not texts or texts[0].upper() != "TOTAL":
                continue

            # Assign each word to a column using boundaries
            col_texts = defaultdict(list)
            for w in words:
                x = w["Geometry"]["BoundingBox"]["Left"]
                col = assign_word_to_column(x, boundaries)
                col_texts[col].append(w["Text"])

            # Extract class name from columns 1 and 2 (Brand + Vendor)
            # TOTAL row class names often span both columns
            brand_text = " ".join(col_texts.get(1, []))
            vendor_text = " ".join(col_texts.get(2, []))

            # Remove "TOTAL" prefix from brand text
            class_text = brand_text.upper().strip()
            if class_text.startswith("TOTAL"):
                class_text = class_text[5:].strip()

            # Append vendor text if it looks like part of the class name
            # (not a numeric value and not empty)
            if vendor_text:
                vendor_upper = vendor_text.upper().strip()
                vendor_clean = vendor_upper.replace(",", "").replace(".", "")
                if not vendor_clean.isdigit():
                    class_text = (class_text + " " + vendor_upper).strip()

            if not class_text:
                continue

            class_name = re.sub(r'\s+', ' ', class_text).strip().rstrip('-')
            normalized = fix_total_class_name(class_name)

            # Parse numeric values from each column using boundary-based assignment
            field_values = {}
            has_numeric_cols = 0
            for col_idx, field_name in COL_FIELD_MAP.items():
                text = " ".join(col_texts.get(col_idx, [])).strip().replace(",", "")
                if text:
                    try:
                        field_values[field_name] = int(float(text))
                        if col_idx >= 3 and field_name != "Pct_of_Type":
                            has_numeric_cols += 1
                    except ValueError:
                        field_values[field_name] = 0
                else:
                    field_values[field_name] = 0

            # Skip sub-totals (like "TOTAL HAYNER") - need at least 4 numeric columns
            if has_numeric_cols < 4:
                continue

            # Skip parent/category totals that don't match a specific sub-class.
            # e.g., "TOTAL DOM WHSKY" (parent) vs "TOTAL DOM WHSKY-BLND" (sub-class)
            known_set = set(c.upper() for c in KNOWN_CLASS_PREFIXES)
            if normalized.upper() not in known_set:
                continue

            total_rows.append({
                "page": page_num,
                "class_raw": class_name,
                "class_normalized": normalized,
                "L12M_Cases_TY": field_values.get("L12M_Cases_TY", 0),
                "L12M_Cases_LY": field_values.get("L12M_Cases_LY", 0),
                "YTD_Cases": field_values.get("YTD_Cases", 0),
                "CurMo_Cases": field_values.get("CurMo_Cases", 0),
                "1.75L": field_values.get("1.75L", 0),
                "1.0L": field_values.get("1.0L", 0),
                "750ml": field_values.get("750ml", 0),
                "750ml_Trav": field_values.get("750ml_Trav", 0),
                "375ml": field_values.get("375ml", 0),
                "200ml": field_values.get("200ml", 0),
                "100ml": field_values.get("100ml", 0),
                "50ml": field_values.get("50ml", 0),
            })

    return total_rows


def sum_csv_by_class(csv_path: str):
    """Sum extracted CSV values by class."""
    class_sums = defaultdict(lambda: defaultdict(int))
    class_counts = defaultdict(int)

    fields = ["L12M_Cases_TY", "L12M_Cases_LY", "YTD_Cases", "CurMo_Cases",
              "1.75L", "1.0L", "750ml", "750ml_Trav", "375ml", "200ml", "100ml", "50ml"]

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cls = row.get("Class", "").upper().strip()
            if not cls:
                continue
            class_counts[cls] += 1
            for field in fields:
                val = row.get(field, "").strip().replace(",", "")
                if val:
                    try:
                        class_sums[cls][field] += int(float(val))
                    except ValueError:
                        pass

    return class_sums, class_counts


def validate_totals(json_path: str, csv_path: str):
    """Compare PDF TOTAL rows vs sum of extracted CSV rows per class."""

    print("=" * 90)
    print("TOTAL ROW VALIDATION: PDF TOTAL vs Sum of Extracted Rows")
    print("=" * 90)

    # Extract TOTAL rows from PDF
    print("\n1. Extracting TOTAL rows from PDF...")
    total_rows = extract_total_rows(json_path)
    print(f"   Found {len(total_rows)} class TOTAL rows")

    # Sum CSV by class
    print(f"\n2. Summing extracted CSV by class from {csv_path}...")
    class_sums, class_counts = sum_csv_by_class(csv_path)
    print(f"   Found {len(class_sums)} unique classes, {sum(class_counts.values()):,} total rows")

    # Compare
    print(f"\n3. Comparing...\n")

    fields = ["L12M_Cases_TY", "L12M_Cases_LY", "YTD_Cases", "CurMo_Cases",
              "1.75L", "1.0L", "750ml", "750ml_Trav", "375ml", "200ml", "100ml", "50ml"]

    results = []
    total_fields_checked = 0
    total_fields_matched = 0
    total_fields_mismatched = 0

    for total_row in total_rows:
        cls = total_row["class_normalized"].upper().strip()

        if cls not in class_sums:
            results.append({
                "class": cls,
                "class_raw": total_row["class_raw"],
                "page": total_row["page"],
                "status": "MISSING_IN_CSV",
                "csv_rows": 0,
                "mismatches": []
            })
            continue

        csv_sum = class_sums[cls]
        mismatches = []

        for field in fields:
            pdf_val = total_row.get(field, 0)
            csv_val = csv_sum.get(field, 0)
            total_fields_checked += 1

            if pdf_val != csv_val:
                diff = csv_val - pdf_val
                pct = (abs(diff) / max(pdf_val, 1)) * 100
                mismatches.append({
                    "field": field,
                    "pdf_total": pdf_val,
                    "csv_sum": csv_val,
                    "diff": diff,
                    "pct": pct
                })
                total_fields_mismatched += 1
            else:
                total_fields_matched += 1

        results.append({
            "class": cls,
            "class_raw": total_row["class_raw"],
            "page": total_row["page"],
            "status": "MATCH" if not mismatches else "MISMATCH",
            "csv_rows": class_counts.get(cls, 0),
            "mismatches": mismatches
        })

    # Print results
    print(f"{'Class':<30} {'Page':>4} {'Rows':>5} {'Status':<10} {'Details'}")
    print("-" * 90)

    match_count = 0
    mismatch_count = 0
    missing_count = 0

    for r in results:
        if r["status"] == "MATCH":
            match_count += 1
            print(f"{r['class']:<30} {r['page']:>4} {r['csv_rows']:>5} MATCH")
        elif r["status"] == "MISSING_IN_CSV":
            missing_count += 1
            print(f"{r['class']:<30} {r['page']:>4} {r['csv_rows']:>5} MISSING    Class not found in CSV")
        else:
            mismatch_count += 1
            m = r["mismatches"]
            first = m[0]
            print(f"{r['class']:<30} {r['page']:>4} {r['csv_rows']:>5} MISMATCH   {first['field']}: PDF={first['pdf_total']:,} CSV={first['csv_sum']:,} (diff={first['diff']:+,})")
            for mm in m[1:]:
                print(f"{'':>52} {mm['field']}: PDF={mm['pdf_total']:,} CSV={mm['csv_sum']:,} (diff={mm['diff']:+,})")

    # Summary
    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)
    print(f"""
  TOTAL rows found in PDF:    {len(total_rows)}
  Classes matched:            {match_count}
  Classes mismatched:         {mismatch_count}
  Classes missing in CSV:     {missing_count}

  Fields checked:             {total_fields_checked:,}
  Fields matched:             {total_fields_matched:,}
  Fields mismatched:          {total_fields_mismatched:,}
  Field match rate:           {total_fields_matched/max(total_fields_checked,1)*100:.1f}%
""")

    # Write detailed report
    report_path = "output_raw/total_validation_errors.csv"
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Class", "Page", "CSV_Rows", "Field", "PDF_Total", "CSV_Sum", "Diff", "Pct_Off"])
        for r in results:
            if r["status"] == "MISSING_IN_CSV":
                writer.writerow([r["class"], r["page"], 0, "ALL", "", "", "", "MISSING"])
            for mm in r.get("mismatches", []):
                writer.writerow([r["class"], r["page"], r["csv_rows"],
                                mm["field"], mm["pdf_total"], mm["csv_sum"],
                                mm["diff"], f"{mm['pct']:.1f}%"])

    print(f"  Detailed errors saved to: {report_path}")

    return results


if __name__ == "__main__":
    validate_totals(
        json_path="Textract/631_9L_0925.json",
        csv_path="output_raw/brand_summary_all.csv"
    )
