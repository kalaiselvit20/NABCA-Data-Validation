# NABCA Brand Summary — Data Validation Pipeline

End-to-end extraction and validation of NABCA (National Alcohol Beverage Control Association) **Brand Summary** reports.

## What This Is

A data validation pipeline that:

1. **Extracts** Brand Summary tables from AWS Textract JSON (PDF &rarr; JSON &rarr; CSV)
2. **Validates** extracted data across 5 layers and 17 checks
3. **Compares** extracted CSV against the Supabase `nabca-pre-prod.raw_brand_summary` table
4. **Reports** results in CSV, Excel (color-coded), and PDF formats

### Scope

| Item | Detail |
|------|--------|
| **Table** | `raw_brand_summary` (1 of 8 tables in `nabca-pre-prod` schema) |
| **Source** | NABCA 631 9L Brand Summary PDFs (monthly) |
| **Pipeline** | PDF &rarr; AWS Textract JSON &rarr; Extraction Script &rarr; CSV &rarr; Validated against Supabase |
| **Coverage** | 15 months (Jul 2024 &ndash; Sep 2025), ~24,000 records/month |
| **Classes** | 65 liquor class/sub-class types |

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variables
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-service-role-key"

# 3a. Extract a single month (from S3 or local JSON)
python extract_from_s3.py --file 631_9L_0925

# 3b. Run full pipeline (extract + validate all months)
python pipeline.py --all

# 4. Run individual validators
python validate_brand_summary.py --csv output_raw/brand_summary_631_9L_0925.csv --year 2025 --month 9
python validate_business_logic.py --csv output_raw/brand_summary_631_9L_0925.csv
python validate_counts.py --csv output_raw/brand_summary_631_9L_0925.csv --year 2025 --month 9
python validate_totals.py   # Requires local Textract JSON
```

## Documentation

| Document | Description |
|----------|-------------|
| [Extraction Logic](docs/extraction_logic.md) | 22-step extraction process from Textract JSON to CSV |
| [Validation Logic](docs/validation_logic.md) | 17 validation checks across 5 layers |
| [Data Eng vs Validation Comparison](docs/data_eng_vs_validation.md) | Side-by-side comparison of approaches and findings |

## Pipeline Architecture

```
AWS Textract JSON (S3 / local)
        |
        v
 extract_brand_summary_raw.py    -- Core extraction (22 steps)
        |
        v
 extract_from_s3.py              -- S3 loader + multi-month orchestration
        |
        v
    CSV output
        |
        +---> validate_brand_summary.py   -- Layer 2: CSV vs Supabase
        +---> validate_business_logic.py  -- Layer 3: Business logic (5 checks)
        +---> validate_counts.py          -- Layer 4: Enhanced count validation
        +---> validate_class_counts.py    -- Layer 4: Per-class count comparison
        +---> validate_totals.py          -- Layer 5: PDF TOTAL row validation
        |
        v
 pipeline.py                     -- Full orchestration (extract + validate)
        |
        v
 report_generator.py             -- CSV / Excel / PDF consolidated reports
```

## File Inventory

| File | Purpose |
|------|---------|
| `extract_brand_summary_raw.py` | Core extraction engine (Textract JSON &rarr; CSV) |
| `extract_from_s3.py` | S3/local JSON loader, multi-month extraction |
| `validate_brand_summary.py` | CSV vs Supabase record + value comparison |
| `validate_business_logic.py` | 5 business logic checks (no Supabase needed) |
| `validate_counts.py` | Enhanced count validation with data quality checks |
| `validate_class_counts.py` | Per-class record count comparison |
| `validate_totals.py` | PDF TOTAL row vs CSV sum-per-class validation |
| `pipeline.py` | End-to-end orchestration with resume support |
| `report_generator.py` | Consolidated CSV / Excel / PDF report generation |

## Key Results (September 2025)

| Metric | Value |
|--------|-------|
| Records extracted | 23,968 |
| Classes | 65 |
| Internal class TOTAL validation | 65/65 matched |
| CSV vs Supabase key match rate | 99.9% |
| CSV vs Supabase exact value match | 99.4% |
| Business logic failures | 9 (bottle sum mismatches) |
| Rows missing class | 0 |
