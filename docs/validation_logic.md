# Validation Logic — 17 Checks Across 5 Layers

## Overview

The validation pipeline runs 17 distinct checks organized in 5 layers, from extraction-time integrity through cross-reference with Supabase to self-contained business logic.

```
Layer 1: Extraction-Time (built into extract_brand_summary_raw.py)
Layer 2: CSV vs Supabase (validate_brand_summary.py)
Layer 3: Business Logic (validate_business_logic.py)
Layer 4: Enhanced Counts (validate_counts.py + validate_class_counts.py)
Layer 5: PDF TOTAL Validation (validate_totals.py)
```

---

## Layer 1: Extraction-Time Checks (3 checks)

Built into `extract_brand_summary_raw.py` — run during extraction, not as a separate step.

| # | Check | What It Does | Fail Condition |
|---|-------|-------------|----------------|
| 1 | **Class assignment** | Every data row must have a non-empty Class field | Row output with empty Class &rarr; `MISSING_CLASS` |
| 2 | **Bottle size vs CurMo_Cases** | If any bottle size column has data, CurMo_Cases must also have data | Bottle data exists but CurMo_Cases is empty &rarr; `BOTTLE_SIZE_NO_CURMO` |
| 3 | **Class TOTAL validation** | Sum of all extracted rows per class must match the PDF's TOTAL row for that class | Sum differs from PDF TOTAL &rarr; `TOTAL_MISMATCH` |

### Results (Sep 2025)
- MISSING_CLASS: 0
- BOTTLE_SIZE_NO_CURMO: 133 (mostly L12M-only rows with no current month data)
- TOTAL_MISMATCH: 0 (65/65 classes validated)

---

## Layer 2: CSV vs Supabase (2 checks)

`validate_brand_summary.py` — compares every extracted record against the Supabase database.

| # | Check | What It Does | Key |
|---|-------|-------------|-----|
| 4 | **Record key match** | Match CSV records to Supabase using (Brand, Vendor, Class) triple | Records in CSV but not Supabase &rarr; `MISSING_IN_SUPABASE`; vice versa &rarr; `MISSING_IN_CSV` |
| 5 | **Field value match** | For matched records, compare 11 numeric fields | Any field differs &rarr; `VALUE_MISMATCH` with field name, CSV value, Supabase value |

### Fields Compared
| CSV Column | Supabase Column |
|-----------|----------------|
| L12M_Cases_TY | l12m_cases_ty |
| L12M_Cases_LY | l12m_cases_ly |
| YTD_Cases | ytd_cases_ty |
| CurMo_Cases | curr_month_cases |
| 1.75L | curr_month_175l |
| 1.0L | curr_month_1l |
| 750ml | curr_month_750ml |
| 375ml | curr_month_375ml |
| 200ml | curr_month_200ml |
| 100ml | curr_month_100ml |
| 50ml | curr_month_50ml |

### Results (Sep 2025)
- Key match rate: 99.9% (23,954 / 23,967)
- Exact value match rate: 99.4% (23,814 / 23,954)
- Missing in Supabase: 13 (vendor/brand word order differences)
- Missing in CSV: 13 (same root cause, reverse direction)
- Value mismatches: 140 (mostly 750ml column mapping issue)

---

## Layer 3: Business Logic (5 checks)

`validate_business_logic.py` — self-contained checks on CSV data. No Supabase needed.

| # | Check | What It Does | Severity |
|---|-------|-------------|----------|
| 6 | **No negative values** | All 12 numeric fields must be &ge; 0 | ERROR |
| 7 | **Bottle sum = CurMo_Cases** | Sum of 8 bottle size columns must equal CurMo_Cases exactly | ERROR |
| 8 | **L12M_TY &ge; YTD_Cases** | Rolling 12-month total should be &ge; year-to-date | WARNING |
| 9 | **Pct_of_Type range** | Each row's Pct_of_Type must be 0&ndash;100; class sums must be ~100% (&pm;2.0%) | WARNING |
| 10 | **Grand total** | Sum of all CSV rows must match DISTILLED SPIRITS TOTAL from PDF | ERROR |

### Results (Sep 2025)
- Negative values: 0
- Bottle sum mismatches: 9
- L12M < YTD warnings: 67
- Pct_of_Type out of range: 0
- Grand total: PASS (all 12 fields matched)

---

## Layer 4: Enhanced Count Validation (5 checks)

`validate_counts.py` + `validate_class_counts.py` — record counts and data quality.

| # | Check | What It Does | Detail |
|---|-------|-------------|--------|
| 11 | **Total count comparison** | CSV record count vs Supabase record count | Difference &gt; 5 &rarr; FAIL |
| 12 | **Per-class count** | Record count per class: CSV vs Supabase | Per-class differences listed with MISMATCH status |
| 13 | **Page distribution** | Stats: rows per page (min/max/avg), empty pages | Identifies anomalous pages |
| 14 | **Duplicate detection** | Find duplicate (Brand, Vendor, Class) keys with page numbers | Duplicates listed with occurrence count |
| 15 | **Missing class detection** | Records with empty Class or Class not in known list | empty_count and invalid_count |

### Results (Sep 2025)
- Total count: CSV = 23,968, Supabase = 23,968, Diff = 0
- Per-class: 65/65 matched
- Page distribution: 372 pages, 11&ndash;382, avg 64.4 rows/page
- Duplicates: 1
- Missing/invalid class: 0

---

## Layer 5: PDF TOTAL Row Validation (2 checks)

`validate_totals.py` — extracts TOTAL rows directly from Textract JSON and compares against CSV sums.

| # | Check | What It Does | Detail |
|---|-------|-------------|--------|
| 16 | **TOTAL row extraction** | Parse TOTAL rows from Textract JSON using column boundaries | Uses same boundary logic as extraction script for consistency |
| 17 | **Class TOTAL vs CSV sum** | For each class, compare PDF TOTAL row values against sum of extracted CSV rows | 12 fields compared per class |

### TOTAL Row Parsing Challenges
- OCR garbles class names: "SCOTCH-BL ND-FRGN BTLD", "CRDL-LQR8 SPC-FRT"
- 73+ manual normalization mappings in `TOTAL_ROW_CLASS_FIXES`
- Parent/category totals (e.g., "TOTAL DOM WHSKY") filtered out; only sub-class totals kept
- Sub-totals (e.g., "TOTAL HAYNER") filtered by requiring 4+ numeric columns

### Results (Sep 2025)
- TOTAL rows found: 55 (sub-class level)
- Classes matched: 47
- Classes mismatched: 8 (all due to PDF TOTAL having 0 where data rows have values — a PDF layout artifact, not an extraction error)
- Field match rate: 97.6% (644/660)

---

## Validation Summary Table

| Layer | Script | Checks | Pass Criteria |
|-------|--------|--------|---------------|
| 1. Extraction-Time | extract_brand_summary_raw.py | 3 | 0 MISSING_CLASS, 0 TOTAL_MISMATCH |
| 2. CSV vs Supabase | validate_brand_summary.py | 2 | Key match &ge; 99.5%, Exact match &ge; 99.0% |
| 3. Business Logic | validate_business_logic.py | 5 | 0 negatives, 0 bottle sum errors, grand total PASS |
| 4. Enhanced Counts | validate_counts.py | 5 | Total diff = 0, 0 mismatched classes |
| 5. PDF TOTAL | validate_totals.py | 2 | Field match rate &ge; 95% |

## Report Outputs

The `report_generator.py` produces three consolidated output formats:

| Format | File | Features |
|--------|------|----------|
| CSV | `consolidated_validation.csv` | Machine-readable, all months |
| Excel | `consolidated_validation.xlsx` | Color-coded (green/yellow/red), auto-widths |
| PDF | `consolidated_validation.pdf` | Client-facing summary with status indicators |
