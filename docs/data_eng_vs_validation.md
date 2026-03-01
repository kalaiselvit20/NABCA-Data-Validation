# Data Engineering vs Data Validation — Comparison

Side-by-side analysis of what the data engineering team's extraction produces (inferred from Supabase output) versus what the data validation pipeline produces.

---

## Extraction Logic Comparison

### Data Engineering Extraction (10 steps, inferred from Supabase output)

| # | Step | Evidence |
|---|------|----------|
| 1 | Load Textract JSON | Data exists in Supabase |
| 2 | Identify Brand Summary pages | Correct page range extracted |
| 3 | Parse table structure | Column mapping exists |
| 4 | Extract Brand, Vendor, Class | All three fields populated |
| 5 | Extract L12M, YTD, CurMo | Numeric fields present |
| 6 | Extract bottle size columns | 7 bottle columns populated (750ml + 750ml_Trav merged) |
| 7 | Filter header/total rows | No header rows in output |
| 8 | Assign report_year, report_month | Metadata columns present |
| 9 | Load to Supabase | Data queryable via SQL |
| 10 | Basic deduplication | ~23,968 records (matches expected count) |

### Data Validation Extraction (22 steps)

| # | Step | Detail |
|---|------|--------|
| 1 | Load Textract blocks | Parse JSON, index by block ID |
| 2 | Detect TABLE block | Per-page table identification |
| 3 | Extract CELL children | Grid structure from Textract |
| 4 | Build column boundaries | Min/max X per column from CELL geometry |
| 5 | Detect merged cells | Flag RowSpan/ColumnSpan > 1 |
| 6 | Collect WORD blocks | Within table bounding box |
| 7 | Assign words to columns | X-position &rarr; column mapping |
| 8 | Cluster words into rows | Y-position clustering (threshold 0.005) |
| 9 | Build 2D grid | `grid[row][col] = text` |
| 10 | Skip header rows | CLASS & TYPE, BRAND detection |
| 11 | Skip footer artifacts | COPYRIGHT, NABCA, page numbers |
| 12 | Clean garbage prefixes | Year, NABCA, copyright removal |
| 13 | Detect class headers | 65 known classes, exact + partial match |
| 14 | Handle split classes | Brand + Vendor column combination |
| 15 | Cognac sequence resolution | TOTAL row context for VS/VSOP/XO |
| 16 | Capture TOTAL rows | Store for validation, exclude from output |
| 17 | OCR normalization | 73+ character-level fixes |
| 18 | Skip vendor-only rows | No numeric data &rarr; skip |
| 19 | Validate class assignment | Every row must have Class |
| 20 | Validate bottle vs CurMo | Bottle data requires CurMo_Cases |
| 21 | Vertical leakage check | Y-distance threshold for word assignment |
| 22 | Confidence scoring | Average Textract confidence per row |

---

## Validation Logic Comparison

### Data Engineering Validation

| # | Check | Status |
|---|-------|--------|
| — | *No documented validation checks* | N/A |

Based on the Supabase output, the data engineering pipeline does not appear to include explicit validation steps. Data is loaded directly after extraction.

### Data Validation Pipeline (17 checks, 5 layers)

| Layer | # | Check | Type |
|-------|---|-------|------|
| 1 | 1 | Class assignment enforcement | ERROR |
| 1 | 2 | Bottle size vs CurMo_Cases | WARNING |
| 1 | 3 | Class TOTAL validation | ERROR |
| 2 | 4 | Record key match (CSV vs Supabase) | ERROR |
| 2 | 5 | Field value match (11 fields) | ERROR |
| 3 | 6 | No negative values | ERROR |
| 3 | 7 | Bottle sum = CurMo_Cases | ERROR |
| 3 | 8 | L12M_TY &ge; YTD_Cases | WARNING |
| 3 | 9 | Pct_of_Type range (0&ndash;100, class sums ~100%) | WARNING |
| 3 | 10 | Grand total vs DISTILLED SPIRITS TOTAL | ERROR |
| 4 | 11 | Total record count comparison | ERROR |
| 4 | 12 | Per-class record count comparison | ERROR |
| 4 | 13 | Page distribution analysis | INFO |
| 4 | 14 | Duplicate detection | WARNING |
| 4 | 15 | Missing/invalid class detection | ERROR |
| 5 | 16 | TOTAL row extraction from PDF | INFO |
| 5 | 17 | Class TOTAL vs CSV sum (12 fields) | ERROR |

---

## Head-to-Head Comparison

| Feature | Data Engineering | Data Validation | Finding |
|---------|-----------------|-----------------|---------|
| **Extraction steps** | ~10 | 22 | Validation has 12 additional steps for edge case handling |
| **Validation checks** | 0 | 17 | No documented validation in data eng pipeline |
| **750ml + 750ml_Trav** | Merged into single column | Separate columns (750ml and 750ml_Trav) | **140 value mismatches** where both columns have data; Supabase `curr_month_750ml` contains the 750ml_Trav value instead of 750ml |
| **NULL vs 0 handling** | NULLs stored in Supabase | 0 used for empty numeric fields | Comparison normalizes both to handle this |
| **Page traceability** | No page number column | Page column in every row | Validation can trace any row back to its source page in the PDF |
| **Confidence scores** | Not captured | Avg_Confidence per row | Rows with low OCR confidence can be flagged for manual review |
| **Grand total check** | Not performed | Sum of all rows vs PDF DISTILLED SPIRITS TOTAL | Catches extraction errors that affect overall totals |
| **Bottle sum check** | Not performed | Sum of 8 bottle sizes must equal CurMo_Cases | **9 rows** where bottle sizes don't sum to current month total (found: 335 extracted vs 344 expected in some rows) |
| **OCR normalization** | Undocumented | 73+ documented mappings in `TOTAL_ROW_CLASS_FIXES` | Every OCR fix is documented and traceable |
| **Class detection** | Undocumented | 6-step detection cascade with 65 known classes + partial + cognac sequence | Edge cases like split classes and cognac truncation are handled explicitly |
| **Report formats** | Supabase queries only | CSV + Excel (color-coded) + PDF | Client-facing reports with pass/warning/fail color coding |
| **Multi-month support** | Individual loads | Pipeline with resume support, consolidated reports | Single command processes all months with crash recovery |

---

## Key Findings

### 1. 750ml vs 750ml_Trav Column Mapping

The PDF has **15 bottle size columns** including both "750 ml" (Col 10) and "750 ml Traveler" (Col 11). Our extraction separates these into two distinct fields.

Supabase's `curr_month_750ml` maps to Col 10 for most rows, but for **~140 rows** where both columns have values, Supabase contains the Col 11 (Traveler) value instead of Col 10. This means:

- Data eng merges or misaligns the two 750ml columns
- Our extraction correctly captures both per the PDF headers
- **Impact:** ~0.6% of records have incorrect 750ml value in Supabase

### 2. Bottle Sum Validation

Our Check #7 (bottle sum = CurMo_Cases) found **9 rows** where the sum of 8 bottle size columns does not equal CurMo_Cases. Example:

| Brand | CurMo_Cases | Bottle Sum | Diff |
|-------|------------|------------|------|
| Example brand | 344 | 335 | -9 |

These represent either:
- OCR errors in individual bottle columns
- Rounding in the source PDF
- A genuine data issue in the source report

Without this check in the data eng pipeline, these discrepancies go undetected.

### 3. Page Traceability

Every row in our output includes the source page number. This enables:
- Tracing any data discrepancy back to the PDF
- Identifying pages with extraction anomalies
- Validating page-level consistency

The data eng output has no page reference, making discrepancy investigation manual.

### 4. Confidence Scoring

Our extraction captures the average Textract OCR confidence per row. This enables:
- Flagging low-confidence rows for manual review
- Prioritizing quality review on worst-confidence pages
- Tracking OCR quality trends across months

### 5. Documented OCR Normalization

We maintain 73+ explicit OCR normalization mappings. Each one documents a specific Textract garbling pattern. This provides:
- An auditable record of all OCR corrections
- A reference for expected Textract behavior on NABCA PDFs
- A foundation for automated OCR quality monitoring

---

## Summary

| Metric | Data Engineering | Data Validation |
|--------|-----------------|-----------------|
| Extraction steps | ~10 | 22 |
| Validation checks | 0 | 17 |
| Documented OCR fixes | 0 | 73+ |
| Output columns | 14 | 19 (+Page, +750ml_Trav, +Pct_of_Type, +Row, +Avg_Confidence) |
| Report formats | SQL query | CSV + Excel + PDF |
| Page traceability | No | Yes |
| Confidence scores | No | Yes |
| Known data issues found | — | 750ml mapping (140 rows), bottle sum (9 rows), vendor word order (13 rows) |
