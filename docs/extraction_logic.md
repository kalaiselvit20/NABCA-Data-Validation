# Extraction Logic — 22 Steps

How `extract_brand_summary_raw.py` transforms AWS Textract JSON into a clean, validated CSV.

## Source &rarr; Output

- **Input:** AWS Textract JSON (~200 MB per month, ~800K blocks)
- **Output:** CSV with 19 columns: Page, Row, Class, Brand, Vendor, L12M_Cases_TY, L12M_Cases_LY, Pct_of_Type, YTD_Cases, CurMo_Cases, 1.75L, 1.0L, 750ml, 750ml_Trav, 375ml, 200ml, 100ml, 50ml, Avg_Confidence
- **Pages:** 11&ndash;382 (Brand Summary section of 631 9L report)

## Step-by-Step

| # | Step | Description | Why Needed |
|---|------|-------------|------------|
| 1 | **Load Textract blocks** | Parse JSON, index all blocks by ID | Foundation for all subsequent steps |
| 2 | **Detect TABLE block** | Find the TABLE block for each page | Not all pages have tables; some are title/summary pages |
| 3 | **Extract CELL children** | Get all CELL blocks from table's Relationships | Cells define the grid structure |
| 4 | **Build column boundaries** | Compute min/max X coordinates per column index from CELL geometry | Words are positioned by pixel coordinates, not cell membership; boundaries map X &rarr; column |
| 5 | **Detect merged cells** | Flag cells with RowSpan > 1 or ColumnSpan > 1 | Merged cells affect column boundaries and row assignment |
| 6 | **Collect WORD blocks** | Get all WORD blocks within the table's bounding box | Words contain the actual text and confidence scores |
| 7 | **Assign words to columns** | Map each word's X position to a column using step 4 boundaries | Textract returns flat word list; we need columnar structure |
| 8 | **Cluster words into rows** | Group words by Y position (threshold: 0.005 normalized units) | Adjacent rows may have very close Y values; clustering prevents mixing |
| 9 | **Build 2D grid** | Create `grid[row][col] = text` from clustered words | Transforms flat word list into structured table |
| 10 | **Skip header rows** | Detect and skip "CLASS & TYPE", "BRAND", column headers | These appear on every page and are not data |
| 11 | **Skip footer artifacts** | Filter rows containing COPYRIGHT, NABCA, page numbers | Textract captures footer text within table bounds |
| 12 | **Clean garbage prefixes** | Remove year prefixes ("2025,"), "NABCA", "(c)", "BY" from brand text | OCR captures page header/footer text that bleeds into first data row |
| 13 | **Detect class headers** | Match cleaned brand text against 65 known class names | Class names appear as brand text with no numeric data; they set `current_class` for subsequent rows |
| 14 | **Handle split classes** | Combine Brand + Vendor columns for class names split by OCR | Textract may split "VODKA-CLASSIC-DOM" across two columns as "VODKA-CLASSIC-DO" + "M" |
| 15 | **Cognac sequence resolution** | Use TOTAL row context to disambiguate truncated cognac sub-types | PDF shows "BRNDY/CGNC-CGNC-" without VS/VSOP/XO suffix; sequence after each TOTAL determines the next class |
| 16 | **Capture TOTAL rows** | Extract numeric values from "TOTAL &lt;CLASS&gt;" rows; do not output to CSV | TOTAL rows are used for validation (step 21), not as data records |
| 17 | **OCR normalization** | Apply 73+ character-level fixes to class names from TOTAL rows | Textract garbles class names: spaces in wrong places, "&" &rarr; "8", truncated suffixes |
| 18 | **Skip vendor-only rows** | If all 13 numeric columns are empty, row is vendor continuation | Some pages have vendor name on a separate row with no data |
| 19 | **Validate class assignment** | Ensure every output row has a non-empty Class value | Rows without class indicate a detection failure |
| 20 | **Validate bottle vs CurMo** | If any bottle size column has data, CurMo_Cases must also have data | Bottle sizes break down CurMo_Cases; if sizes exist but total is missing, column alignment is wrong |
| 21 | **Vertical leakage check** | Flag words whose Y distance to row center exceeds 0.004 | Prevents values from one row contaminating adjacent rows |
| 22 | **Confidence scoring** | Average Textract confidence across all words in each row | Rows below 90% confidence are flagged for review |

## Class Detection Order

This order is critical. Getting it wrong causes cascading misassignment:

```
1. Clean garbage prefixes (step 12)
2. Check exact match against 65 known class names
3. Check standalone patterns (COCKTAILS, NEUTRAL GRAIN SPIRIT, etc.)
4. Check split class: Brand + Vendor combined
5. Check partial match: Brand matches known partial, append Vendor
6. Check cognac sequence: use TOTAL row context
```

The fix discovered during development: **clean_brand_name() must run BEFORE get_class_name()**, not after. Without cleaning, garbage prefixes like "2025, CRDL-COFFEE LQR" fail class detection.

## OCR Normalization (73+ mappings)

Textract consistently garbles certain class names. The `TOTAL_ROW_CLASS_FIXES` dictionary in `validate_totals.py` maps raw OCR text to correct class names:

| OCR Output | Corrected |
|------------|-----------|
| `DOM WHSKY -STRT-BRBN/TN` | `DOM WHSKY-STRT-BRBN/TN` |
| `SCOTCH-BL ND-FRGN BTLD` | `SCOTCH-BLND-FRGN BTLD` |
| `VODKA-CL ASSIC-DOM` | `VODKA-CLASSIC-DOM` |
| `GIN-CLASS SIC-DOM` | `GIN-CLASSIC-DOM` |
| `CRDL-LQR8 SPC-FRT` | `CRDL-LQR&SPC-FRT` |
| `TEQUILA-\|LAVORED` | `TEQUILA-FLAVORED` |
| `MEZCAL CRISTALINO 9` | `MEZCAL-CRISTALINO` |
| `RUM-AGED DARK` | `RUM-AGED/DARK` |

## Cognac Sequence

The PDF prints cognac sub-types in a fixed order, but the class header is truncated to "BRNDY/CGNC-CGNC-" (no suffix). We determine the suffix from the most recent TOTAL row:

| After TOTAL for | Next class is |
|----------------|---------------|
| `BRNDY/CGNC-CGNC-OTH` | `BRNDY/CGNC-CGNC-VS` |
| `BRNDY/CGNC-CGNC-VS` | `BRNDY/CGNC-CGNC-VSOP` |
| `BRNDY/CGNC-CGNC-VSOP` | `BRNDY/CGNC-CGNC-XO` |

## Output Columns

| Column | Index | Source |
|--------|-------|--------|
| Page | 0 | Textract page number |
| Row | 1 | Row index within page |
| Class | 2 | Detected from class header rows |
| Brand | 3 | Column 1 text (cleaned) |
| Vendor | 4 | Column 2 text |
| L12M_Cases_TY | 5 | Column 3 — Last 12 months cases, this year |
| L12M_Cases_LY | 6 | Column 4 — Last 12 months cases, last year |
| Pct_of_Type | 7 | Column 5 — Percentage of class type |
| YTD_Cases | 8 | Column 6 — Year-to-date cases |
| CurMo_Cases | 9 | Column 7 — Current month total cases |
| 1.75L | 10 | Column 8 — 1.75 liter bottles |
| 1.0L | 11 | Column 9 — 1.0 liter bottles |
| 750ml | 12 | Column 10 — 750ml bottles |
| 750ml_Trav | 13 | Column 11 — 750ml traveler bottles |
| 375ml | 14 | Column 12 — 375ml bottles |
| 200ml | 15 | Column 13 — 200ml bottles |
| 100ml | 16 | Column 14 — 100ml bottles |
| 50ml | 17 | Column 15 — 50ml bottles |
| Avg_Confidence | 18 | Average Textract OCR confidence for the row |
