"""
Extract Brand Summary pages using raw column format with built-in validation.
Output: Page, Row, Class, Brand, Vendor, L12M_Cases_TY, L12M_Cases_LY, etc.

Validation Requirements (7):
1. Class Type Column Enforcement - every row must have a valid Class
2. Merged Cell & Header Handling - detect RowSpan/ColumnSpan from Textract
3. Data Extraction Logic Enhancement - prevent vertical data leakage
4. Total Record Validation - validate extracted count vs TOTAL row per class
5. Vendor Name Detection - first cell with no numbers = vendor name
6. Robust Class Detection - handle class transitions smoothly
7. Bottle Size Validation - if bottle sizes exist, CurMo_Cases must exist
"""
import json
import csv
import os
import re
from collections import defaultdict, Counter
from pathlib import Path

# Known class prefixes for detection
KNOWN_CLASS_PREFIXES = [
    'DOM WHSKY-STRT-BRBN/TN', 'DOM WHSKY-STRT-SM BTCH', 'DOM WHSKY-STRT-RYE',
    'DOM WHSKY-STRT-OTH', 'DOM WHSKY-SNGL MALT', 'DOM WHSKY-BLND',
    'VODKA-CLASSIC-DOM', 'VODKA-CLASSIC-IMP', 'VODKA-FLVRD-DOM', 'VODKA-FLVRD-IMP',
    'GIN-CLASSIC-DOM', 'GIN-CLASSIC-IMP', 'GIN-FLVRD-DOM', 'GIN-FLVRD-IMP',
    'RUM-AGED/DARK', 'RUM-FLVRD', 'RUM-GOLD', 'RUM-LIGHT',
    'TEQUILA-CRISTALINO', 'TEQUILA-FLAVORED', 'TEQUILA-REPOSADO',
    'TEQUILA-BLANCO', 'TEQUILA-ANEJO', 'TEQUILA-GOLD',
    'MEZCAL', 'MEZCAL-CRISTALINO',
    'SCOTCH-BLND-FRGN BTLD', 'SCOTCH-BLND-US BTLD', 'SCOTCH-SNGL MALT',
    'IRISH-SNGL MALT', 'IRISH-BLND', 'IRISH',
    'BRNDY/CGNC-CGNC-VSOP', 'BRNDY/CGNC-CGNC-OTH', 'BRNDY/CGNC-CGNC-VS',
    'BRNDY/CGNC-CGNC-XO', 'BRNDY/CGNC-ARMGNC', 'BRNDY/CGNC-DOM', 'BRNDY/CGNC-IMP',
    'CRDL-LQR&SPC-TRIPLE SEC', 'CRDL-LQR&SPC-SPRT SPCTY', 'CRDL-LQR&SPC-ANSE FLVRD',
    'CRDL-LQR&SPC-SLOE GIN', 'CRDL-LQR&SPC-CURACAO', 'CRDL-LQR&SPC-WHSKY',
    'CRDL-LQR&SPC-HZLNT', 'CRDL-LQR&SPC-AMRT', 'CRDL-LQR&SPC-CRM',
    'CRDL-LQR&SPC-FRT', 'CRDL-LQR&SPC-OTH', 'CRDL-SNPS-BTRSCTCH',
    'CRDL-SNPS-PPRMNT', 'CRDL-SNPS-CNNMN', 'CRDL-SNPS-PEACH',
    'CRDL-SNPS-APPL', 'CRDL-SNPS-OTH', 'CRDL-COFFEE LQR', 'CRDL-CRM LQR',
    'CAN-FRGN BLND-FRGN BTLD', 'CAN-US BLND-US BTLD',
    'OTH IMP WHSKY-SNGL MALT', 'OTH IMP WHSKY-BLND', 'OTH IMP WHSKY',
    'NEUTRAL GRAIN SPIRIT', 'COCKTAILS', 'CACHACA',
]

# Partial class names that might appear split across columns
PARTIAL_CLASS_NAMES = [
    # DOM WHSKY variants
    'DOM WHSKY-STRT', 'DOM WHSKY-SNGL', 'DOM WHSKY-', 'DOM WHSKY',
    # VODKA variants
    'VODKA-CLASSIC', 'VODKA-FLVRD', 'VODKA-', 'VODKA-CLASSIC-DO', 'VODKA-CLASSIC-IM',
    'VODKA-FLVRD-DO', 'VODKA-FLVRD-IM',
    # GIN variants - common split patterns
    'GIN-CLASSIC', 'GIN-FLVRD', 'GIN-', 'GIN-CLASSIC-DO', 'GIN-CLASSIC-IM',
    'GIN-FLVRD-DO', 'GIN-FLVRD-IM',
    # SCOTCH variants
    'SCOTCH-BLND', 'SCOTCH-SNGL', 'SCOTCH-', 'SCOTCH-BLND-FRGN', 'SCOTCH-BLND-US',
    # BRNDY/CGNC variants (including VS, XO, VSOP)
    'BRNDY/CGNC-CGNC', 'BRNDY/CGNC-', 'BRNDY/CGNC-CGNC-',
    'BRNDY/CGNC-CGNC-V', 'BRNDY/CGNC-CGNC-X',  # Partial VS, XO
    # CRDL variants
    'CRDL-LQR&SPC', 'CRDL-SNPS', 'CRDL-', 'CRDL-COFFEE', 'CRDL-CRM',
    'CRDL-LQR&SPC-TRIPLE', 'CRDL-LQR&SPC-SPRT', 'CRDL-LQR&SPC-ANSE',
    'CRDL-LQR&SPC-SLOE',
    # CAN variants
    'CAN-FRGN BLND', 'CAN-US BLND', 'CAN-', 'CAN-FRGN BLND-FRGN', 'CAN-US BLND-US',
    # Other
    'OTH IMP WHSKY', 'NEUTRAL GRAIN',
    'TEQUILA-', 'RUM-', 'IRISH-', 'MEZCAL-',
]

# Garbage prefixes to remove from brand names
GARBAGE_PREFIXES = [
    r'^\d{4},\s*',          # Any year prefix (2024, 2025, etc.)
    r'^NABCA\s+',           # NABCA prefix
    r'^\(c\)\s*',           # Copyright (c)
    r'^\(C\)\s*',           # Copyright (C)
    r'^by\s+',              # by prefix (lowercase)
    r'^BY\s+',              # BY prefix (uppercase)
]

# Class names that may appear as standalone brand values
# These should be detected as class headers even without vendor
STANDALONE_CLASS_PATTERNS = [
    'CRDL-COFFEE LQR',
    'CRDL-CRM LQR',
    'BRNDY/CGNC-CGNC-VS',
    'BRNDY/CGNC-CGNC-XO',
    'BRNDY/CGNC-CGNC-VSOP',
    'BRNDY/CGNC-CGNC-OTH',
    'NEUTRAL GRAIN SPIRIT',
    'COCKTAILS',
]


def clean_brand_name(brand: str) -> str:
    """Remove garbage prefixes from brand name."""
    if not brand:
        return brand

    cleaned = brand.strip()
    for pattern in GARBAGE_PREFIXES:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)

    return cleaned.strip()


def is_class_name(text: str, vendor: str = '') -> bool:
    """Check if text is a class name (not a brand)."""
    if not text:
        return False

    text_upper = text.upper().strip()

    # Normalize multiple spaces to single space
    text_upper = ' '.join(text_upper.split())

    # Check exact matches against known full class names
    for cls in KNOWN_CLASS_PREFIXES:
        if text_upper == cls:
            return True

    # Check standalone class patterns (may appear as brand without vendor)
    for pattern in STANDALONE_CLASS_PATTERNS:
        if text_upper == pattern:
            return True

    # Check partial class names (strip trailing hyphens from both sides)
    text_stripped = text_upper.rstrip('-')
    for partial in PARTIAL_CLASS_NAMES:
        partial_stripped = partial.rstrip('-')
        if text_upper == partial or text_stripped == partial_stripped:
            return True

    # Check if it's a split class (brand=partial, vendor=suffix)
    if vendor:
        vendor_upper = vendor.upper().strip()
        # Try all vendor variants (handles OCR "/" -> " " or " / " conversions)
        for v in normalize_vendor_for_class(vendor_upper):
            combined = text_upper + '-' + v
            combined_space = text_upper + ' ' + v
            combined_no_dash = text_upper + v
            for cls in KNOWN_CLASS_PREFIXES:
                if combined == cls or combined_space == cls or combined_no_dash == cls:
                    return True
            for pattern in STANDALONE_CLASS_PATTERNS:
                if combined == pattern or combined_space == pattern:
                    return True

    return False


def normalize_class_text(text: str) -> str:
    """Normalize class text: collapse spaces around hyphens, fix common OCR errors."""
    if not text:
        return text
    # Collapse spaces around hyphens: "VODKA-CLASSIC- DOM" -> "VODKA-CLASSIC-DOM"
    normalized = re.sub(r'\s*-\s*', '-', text)
    # Collapse spaces around slashes: "BRBN / TN" -> "BRBN/TN"
    normalized = re.sub(r'\s*/\s*', '/', normalized)
    # Normalize multiple spaces
    normalized = ' '.join(normalized.split())
    return normalized.strip()


def normalize_vendor_for_class(vendor: str) -> list:
    """Generate vendor variants for class matching.

    OCR may convert "/" to " " or " / " in class names like BRBN/TN, AGED/DARK.
    Returns list of vendor text variants to try.
    """
    variants = [vendor]
    # "BRBN / TN" -> "BRBN/TN" (collapse spaces around slash)
    collapsed = re.sub(r'\s*/\s*', '/', vendor)
    if collapsed != vendor:
        variants.append(collapsed)
    # "BRBN TN" -> "BRBN/TN" (replace space with slash)
    if ' ' in vendor and '/' not in vendor:
        variants.append(vendor.replace(' ', '/'))
    return list(dict.fromkeys(variants))  # Deduplicate preserving order


def fuzzy_match_class(text: str, vendor: str = '') -> str:
    """Try to match garbled/split class names from Textract OCR.

    Handles:
    - Extra spaces around hyphens: "VODKA-CLASSIC- DOM" -> "VODKA-CLASSIC-DOM"
    - Split across columns: "DOM WHSKY-STRT-" + "BRBN" -> "DOM WHSKY-STRT-BRBN/TN"
    - Truncated suffixes: "CRDL-SNPS-BTRSCTC" -> "CRDL-SNPS-BTRSCTCH"
    - OCR char errors: "BRNDY/CGNC-ARMGNO" -> "BRNDY/CGNC-ARMGNC"
    """
    if not text:
        return ""

    text_upper = text.upper().strip()
    vendor_upper = vendor.upper().strip() if vendor else ""

    # Step 1: Normalize spaces around hyphens
    normalized = normalize_class_text(text_upper)
    combined_with_vendor = normalize_class_text(text_upper + ' ' + vendor_upper) if vendor_upper else normalized

    # Check normalized text against known classes
    for cls in KNOWN_CLASS_PREFIXES:
        if normalized == cls or combined_with_vendor == cls:
            return cls

    # Step 2: Prefix matching - if normalized text is the START of exactly one known class
    # (handles truncation like "BTRSCTC" -> "BTRSCTCH")
    prefix_matches = [cls for cls in KNOWN_CLASS_PREFIXES if cls.startswith(normalized) and len(normalized) >= len(cls) - 2]
    if len(prefix_matches) == 1:
        return prefix_matches[0]

    # Step 3: Try combining brand+vendor with various separators
    if vendor_upper:
        for v in normalize_vendor_for_class(vendor_upper):
            for sep in ['', '-', ' ', '/']:
                combined = normalized.rstrip('-') + sep + v
                for cls in KNOWN_CLASS_PREFIXES:
                    if cls.startswith(combined) and len(combined) >= len(cls) - 3:
                        return cls
                    if combined == cls:
                        return cls

    # Step 4: Levenshtein-style matching (allow 1-2 char difference for OCR errors)
    # Only for strings that are at least 80% of a known class length
    for cls in KNOWN_CLASS_PREFIXES:
        if abs(len(normalized) - len(cls)) <= 2 and len(normalized) >= len(cls) * 0.8:
            diff = sum(1 for a, b in zip(normalized, cls) if a != b)
            if diff <= 2 and normalized[:5] == cls[:5]:  # Same prefix, minor OCR diff
                return cls

    return ""


def get_class_name(text: str, vendor: str = '') -> str:
    """Get the full class name if text is a class, otherwise return empty string."""
    if not text:
        return ""

    text_upper = text.upper().strip()
    # Normalize multiple spaces to single space
    text_upper = ' '.join(text_upper.split())

    # Also try with spaces around hyphens collapsed
    text_normalized = normalize_class_text(text_upper)

    # Check exact matches first against known class prefixes
    for cls in KNOWN_CLASS_PREFIXES:
        if text_upper == cls or text_normalized == cls:
            return cls

    # Check standalone class patterns (may appear as brand without vendor)
    for pattern in STANDALONE_CLASS_PATTERNS:
        if text_upper == pattern:
            return pattern

    # Check if it's a split class (brand=partial, vendor=suffix)
    if vendor:
        vendor_upper = vendor.upper().strip()
        # Try all vendor variants (handles OCR "/" -> " " or " / " conversions)
        for v in normalize_vendor_for_class(vendor_upper):
            combined_dash = text_upper + '-' + v
            combined_space = text_upper + ' ' + v
            combined_no_sep = text_upper + v

            for cls in KNOWN_CLASS_PREFIXES:
                if combined_dash == cls or combined_space == cls or combined_no_sep == cls:
                    return cls

            # Check standalone patterns with combinations
            for pattern in STANDALONE_CLASS_PATTERNS:
                if combined_dash == pattern or combined_space == pattern or combined_no_sep == pattern:
                    return pattern

        # Also check if combined forms a partial that matches a full class
        for v in normalize_vendor_for_class(vendor_upper):
            for cls in KNOWN_CLASS_PREFIXES:
                # Handle cases like "GIN-CLASSIC-DO" + "M" = "GIN-CLASSIC-DOM"
                if cls.startswith(text_upper) and cls.endswith(v):
                    if len(text_upper) + len(v) >= len(cls) - 2:  # Allow for dash/space
                        return cls

    # Check partial class names (return as-is if partial match)
    # Strip trailing hyphens from text for matching
    text_stripped = text_upper.rstrip('-')
    for partial in PARTIAL_CLASS_NAMES:
        partial_stripped = partial.rstrip('-')
        if text_upper == partial or text_stripped == partial_stripped:
            # Try to combine with vendor if available
            if vendor:
                vendor_upper = vendor.upper().strip()
                for v in normalize_vendor_for_class(vendor_upper):
                    # Try to find matching full class
                    for cls in KNOWN_CLASS_PREFIXES:
                        if cls.startswith(text_upper) and cls.endswith(v):
                            return cls
                        combined = text_upper + v
                        if combined == cls:
                            return cls
                        combined_dash = text_upper + '-' + v
                        if combined_dash == cls:
                            return cls
                    # Check standalone patterns
                    for pattern in STANDALONE_CLASS_PATTERNS:
                        if combined_dash == pattern or text_upper + ' ' + v == pattern:
                            return pattern
                # Prefix matching: if combined text is close to a known class, use it
                # This handles cases like "DOM WHSKY-STRT-" + "BRBN" where TN is missing
                for v in normalize_vendor_for_class(vendor_upper):
                    combined_norm = normalize_class_text(text_upper + v)
                    for cls in KNOWN_CLASS_PREFIXES:
                        if cls.startswith(combined_norm) and len(combined_norm) >= len(cls) - 3:
                            return cls
                # Don't return bogus combined class — fall through to fuzzy matching
            return ""  # Return empty to allow fuzzy_match_class to try

    return ""


def is_footer_artifact(brand: str) -> bool:
    """Check if row is a footer artifact (Copyright, NABCA, etc.)."""
    brand_upper = brand.upper().strip()
    if brand_upper in ['COPYRIGHT', 'NABCA', 'BY']:
        return True
    # Match plausible year values only (2020-2030), not brand names like "1675", "1820"
    m = re.match(r'^(\d{4}),?$', brand_upper)
    if m and 2020 <= int(m.group(1)) <= 2030:
        return True
    return False


# Class name normalization - fix truncated/partial class names
CLASS_NORMALIZATION = {
    'GIN-CLASSIC-DO': 'GIN-CLASSIC-DOM',
    'GIN-CLASSIC-IM': 'GIN-CLASSIC-IMP',
    'GIN-FLVRD-DO': 'GIN-FLVRD-DOM',
    'GIN-FLVRD-IM': 'GIN-FLVRD-IMP',
    'VODKA-CLASSIC-DO': 'VODKA-CLASSIC-DOM',
    'VODKA-CLASSIC-IM': 'VODKA-CLASSIC-IMP',
    'VODKA-FLVRD-DO': 'VODKA-FLVRD-DOM',
    'VODKA-FLVRD-IM': 'VODKA-FLVRD-IMP',
    'SCOTCH-BLND-FRGN': 'SCOTCH-BLND-FRGN BTLD',
    'SCOTCH-BLND-US': 'SCOTCH-BLND-US BTLD',
    'CAN-FRGN BLND-FRGN': 'CAN-FRGN BLND-FRGN BTLD',
    'CAN-US BLND-US': 'CAN-US BLND-US BTLD',
    # Note: BRNDY/CGNC-CGNC- is handled dynamically based on TOTAL row context
    'DOM WHSKY-SNGL-MALT': 'DOM WHSKY-SNGL MALT',
    'CRDL-COFFEE': 'CRDL-COFFEE LQR',
    'CRDL-CRM': 'CRDL-CRM LQR',
    'NEUTRAL GRAIN': 'NEUTRAL GRAIN SPIRIT',
}

# Map TOTAL row class to the NEXT expected class (for cognac sub-types)
COGNAC_CLASS_SEQUENCE = {
    'BRNDY/CGNC-CGNC-OTH': 'BRNDY/CGNC-CGNC-VS',
    'BRNDY/CGNC-CGNC-VS': 'BRNDY/CGNC-CGNC-VSOP',
    'BRNDY/CGNC-CGNC-VSOP': 'BRNDY/CGNC-CGNC-XO',
}


def normalize_class_name(class_name: str) -> str:
    """Normalize class name to match Supabase format."""
    if not class_name:
        return class_name

    class_upper = class_name.upper().strip()

    # Direct mapping
    if class_upper in CLASS_NORMALIZATION:
        return CLASS_NORMALIZATION[class_upper]

    # Check if it's a known full class
    if class_upper in KNOWN_CLASS_PREFIXES:
        return class_upper

    return class_upper


# Normalization for OCR-corrupted class names from TOTAL rows.
# These handle cases where Textract splits/garbles class names in TOTAL rows.
TOTAL_CLASS_NORMALIZATION = {
    # DOM WHSKY (space/dash OCR errors)
    "DOM WHSKY -STRT-BRBN/TN": "DOM WHSKY-STRT-BRBN/TN",
    "DOM WHSKY-STRT-SM": "DOM WHSKY-STRT-SM BTCH",
    "DOM WHSKY-STRT-SM BTCH": "DOM WHSKY-STRT-SM BTCH",
    "DOM WHSKY SNGL MALT": "DOM WHSKY-SNGL MALT",  # space instead of dash
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
    # OTH IMP WHSKY
    "OTH IMP WHSKY-SNGL MALT": "OTH IMP WHSKY-SNGL MALT",
    "OTH IMP WHSKY-SNGL MAL": "OTH IMP WHSKY-SNGL MALT",
    "OTH IMP WHSKY-SNGL": "OTH IMP WHSKY-SNGL MALT",
    # GIN (space in CLASS SIC)
    "GIN-CLASS SIC-DOM": "GIN-CLASSIC-DOM",
    "GIN-CLASS SIC-IMP": "GIN-CLASSIC-IMP",
    # NEUTRAL
    "NEUTRAL GRAIN SPIRIT": "NEUTRAL GRAIN SPIRIT",
    "NEUTRAL GRAIN": "NEUTRAL GRAIN SPIRIT",
    "NEUTRAL": "NEUTRAL GRAIN SPIRIT",
    # VODKA (space in CL ASSIC / FL VRD)
    "VODKA-CL ASSIC-DOM": "VODKA-CLASSIC-DOM",
    "VODKA-CL ASSIC-IMP": "VODKA-CLASSIC-IMP",
    "VODKA-FL VRD-DOM": "VODKA-FLVRD-DOM",
    "VODKA-FL VRD-IMP": "VODKA-FLVRD-IMP",
    # RUM
    "RUM-AGED DARK": "RUM-AGED/DARK",
    "RUM-AGED/DARK": "RUM-AGED/DARK",
    "RUM-AGED": "RUM-AGED/DARK",
    # CRDL-LQR&SPC (Textract splits "&" and garbles it)
    "SPC-OTH": "CRDL-LQR&SPC-OTH",
    "SPC-AMRT": "CRDL-LQR&SPC-AMRT",
    "SPC-CRM": "CRDL-LQR&SPC-CRM",
    "SPC-HZLNT": "CRDL-LQR&SPC-HZLNT",
    "SPC-FRT": "CRDL-LQR&SPC-FRT",
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
    # TEQUILA (pipe/dash OCR issues)
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


def is_header_row(brand: str, vendor: str) -> bool:
    """Check if row is a header row."""
    brand_upper = brand.upper().strip()
    return brand_upper in ['CLASS & TYPE', 'BRAND', ''] or 'CLASS & TYPE' in brand_upper


def is_total_row(brand: str, vendor: str = '') -> bool:
    """Check if row is a TOTAL row or grand total row."""
    brand_upper = brand.upper().strip()
    vendor_upper = vendor.upper().strip()
    # Standard TOTAL rows: "TOTAL <CLASS>" - must START with TOTAL
    # (not just contain it, to avoid filtering brands like "HAYNER TOTAL EC")
    if brand_upper.startswith('TOTAL'):
        return True
    # Grand total row: brand="DISTILLED SPIRITS", vendor="TOTAL" or "/ TOTAL"
    if vendor_upper == 'TOTAL' or vendor_upper == '/ TOTAL':
        return True
    # Grand total: "DISTILLED SPIRITS / TOTAL" may be in brand
    if 'DISTILLED SPIRITS' in brand_upper:
        return True
    return False


def extract_class_from_total(total_text: str, vendor_text: str = '') -> str:
    """Extract class name from TOTAL row, combining brand+vendor columns.

    Handles cases where Textract splits class name across columns:
      Brand: "TOTAL VODKA-CL", Vendor: "ASSIC-DOM" → "VODKA-CL ASSIC-DOM"
      Brand: "TOTAL", Vendor: "SPC-OTH" → "SPC-OTH"
      Brand: "TOTAL SCOTCH-SNGL", Vendor: "MALT" → "SCOTCH-SNGL MALT"
    """
    total_upper = total_text.upper().strip()
    if not total_upper.startswith('TOTAL'):
        return ""

    # Remove TOTAL prefix
    remainder = total_upper[5:].strip()

    # Find where numeric data starts in the brand text
    parts = remainder.split()
    class_parts = []
    for part in parts:
        # Stop when we hit a numeric value
        if part.replace(',', '').replace('.', '').replace('-', '').isdigit():
            break
        # Also stop if it looks like a percentage
        if part.startswith('.') and len(part) > 1:
            break
        class_parts.append(part)

    # Also consider vendor text — Textract often places part of the class
    # name in the vendor column (column 2)
    if vendor_text:
        vendor_upper = vendor_text.upper().strip()
        # Skip if vendor is numeric, empty, or a TOTAL marker
        vendor_clean = vendor_upper.replace(',', '').replace('.', '').replace('-', '').replace(' ', '')
        if vendor_clean and not vendor_clean.isdigit() and vendor_upper not in ['TOTAL', '/ TOTAL']:
            # Add non-numeric vendor parts to class name
            for vpart in vendor_upper.split():
                vpart_clean = vpart.replace(',', '').replace('.', '').replace('-', '')
                if vpart_clean.isdigit():
                    break
                class_parts.append(vpart)

    if class_parts:
        return ' '.join(class_parts)
    return ""


# ============================================================
# VALIDATION HELPERS
# ============================================================

def has_numeric_data(text: str) -> bool:
    """Check if text contains numeric data (digits, commas, decimals)."""
    if not text or not text.strip():
        return False
    cleaned = text.strip().replace(',', '').replace('.', '').replace('-', '').replace(' ', '')
    return cleaned.isdigit() and len(cleaned) >= 1


def is_vendor_row(raw_texts: list) -> bool:
    """
    Requirement 5: Vendor Name Detection.
    If the first cell contains no numeric data, treat it as a vendor name.
    Check if ALL cells after Brand (index 0) and Vendor (index 1) are empty or non-numeric.
    """
    # Columns 2+ are numeric fields (L12M_TY, L12M_LY, etc.)
    for text in raw_texts[2:]:
        if has_numeric_data(text):
            return False
    return True


def validate_bottle_size_vs_cases(raw_texts: list) -> dict:
    """
    Requirement 7: Bottle Size Validation.
    If any bottle size column has data, CurMo_Cases must also have data.
    Columns: CurMo_Cases=index 6, bottle sizes=indices 7-14
    Returns: {valid: bool, warning: str}
    """
    # Column indices in raw_texts (0-based after Brand)
    # raw_texts[0]=Brand, [1]=Vendor, [2]=L12M_TY, [3]=L12M_LY, [4]=Pct,
    # [5]=YTD, [6]=CurMo, [7]=1.75L, [8]=1.0L, [9]=750ml, [10]=750ml_Trav,
    # [11]=375ml, [12]=200ml, [13]=100ml, [14]=50ml
    curmo_idx = 6  # CurMo_Cases
    bottle_start = 7  # 1.75L
    bottle_end = 15  # 50ml (exclusive)

    has_bottle_data = any(
        has_numeric_data(raw_texts[i]) for i in range(bottle_start, min(bottle_end, len(raw_texts)))
    )
    has_curmo = has_numeric_data(raw_texts[curmo_idx]) if len(raw_texts) > curmo_idx else False

    if has_bottle_data and not has_curmo:
        return {"valid": False, "warning": "Bottle sizes present but CurMo_Cases is empty"}
    return {"valid": True, "warning": ""}


def detect_merged_cells(cells: list) -> list:
    """
    Requirement 2: Merged Cell Detection.
    Find cells with RowSpan > 1 or ColumnSpan > 1 from Textract CELL blocks.
    Returns list of merged cell info.
    """
    merged = []
    for cell in cells:
        row_span = cell.get("RowSpan", 1)
        col_span = cell.get("ColumnSpan", 1)
        if row_span > 1 or col_span > 1:
            merged.append({
                "row": cell.get("RowIndex", 0),
                "col": cell.get("ColumnIndex", 0),
                "row_span": row_span,
                "col_span": col_span,
                "bbox": cell.get("Geometry", {}).get("BoundingBox", {}),
            })
    return merged


def validate_class_assignment(row_class: str, brand: str, page: int) -> dict:
    """
    Requirement 1: Class Type Column Enforcement.
    Every row must have a valid Class. Flag rows without one.
    """
    if not row_class or not row_class.strip():
        return {"valid": False, "warning": f"Page {page}: No class for brand '{brand}'"}
    # Check against known classes
    class_upper = row_class.upper().strip()
    if class_upper not in KNOWN_CLASS_PREFIXES:
        # Check normalization
        normalized = normalize_class_name(class_upper)
        if normalized not in KNOWN_CLASS_PREFIXES:
            return {"valid": False, "warning": f"Page {page}: Unknown class '{row_class}' for brand '{brand}'"}
    return {"valid": True, "warning": ""}


def parse_total_row_values(raw_texts: list) -> dict:
    """
    Requirement 4: Extract numeric values from a TOTAL row for validation.
    Parse the numeric columns from a TOTAL row's raw_texts.
    """
    values = {}
    field_names = [
        "L12M_Cases_TY", "L12M_Cases_LY", "Pct_of_Type",
        "YTD_Cases", "CurMo_Cases",
        "1.75L", "1.0L", "750ml", "750ml_Trav", "375ml", "200ml", "100ml", "50ml"
    ]
    for i, field in enumerate(field_names):
        idx = i + 2  # Skip Brand(0) and Vendor(1)
        if idx < len(raw_texts):
            text = raw_texts[idx].strip().replace(',', '')
            try:
                values[field] = int(float(text)) if text else 0
            except ValueError:
                values[field] = 0
        else:
            values[field] = 0
    return values


def get_column_boundaries(cells: list) -> list:
    """Get column X boundaries from cells."""
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
        boundaries.append({
            "col": col_idx,
            "left": min(lefts),
            "right": max(rights)
        })
    return boundaries


def assign_to_column(x: float, boundaries: list) -> int:
    """Assign X position to a column."""
    min_dist = float('inf')
    best_col = 1
    for b in boundaries:
        if b["left"] <= x <= b["right"]:
            return b["col"]
        mid = (b["left"] + b["right"]) / 2
        dist = abs(x - mid)
        if dist < min_dist:
            min_dist = dist
            best_col = b["col"]
    return best_col


def cluster_rows(y_positions: list, threshold: float = 0.005) -> list:
    """Cluster Y positions into rows."""
    if not y_positions:
        return []
    sorted_y = sorted(y_positions)
    clusters = []
    current = [sorted_y[0]]
    for y in sorted_y[1:]:
        if y - current[-1] < threshold:
            current.append(y)
        else:
            clusters.append(sum(current) / len(current))
            current = [y]
    if current:
        clusters.append(sum(current) / len(current))
    return clusters


def extract_page_raw(blocks: list, blocks_by_id: dict, page_num: int,
                     current_class: str = "", next_expected_class: str = "",
                     validation_log: list = None, total_row_values: dict = None,
                     grand_total_values: dict = None) -> tuple:
    """
    Extract a single page in raw column format with built-in validation.

    Validation Requirements enforced:
    1. Class Type Column Enforcement - every row gets a valid Class
    2. Merged Cell & Header Handling - detect and handle RowSpan/ColumnSpan
    3. Data Extraction Logic - prevent vertical data leakage via row boundaries
    4. Total Record Validation - capture TOTAL row values for class-level checks
    5. Vendor Name Detection - rows with no numeric data flagged
    6. Robust Class Detection - class transitions tracked
    7. Bottle Size Validation - bottle sizes require CurMo_Cases

    Returns: (list of rows, updated current_class, updated next_expected_class)
    """
    if validation_log is None:
        validation_log = []
    if total_row_values is None:
        total_row_values = {}
    if grand_total_values is None:
        grand_total_values = {}

    # Find table on this page
    tables = [b for b in blocks if b["BlockType"] == "TABLE" and b.get("Page") == page_num]
    if not tables:
        return [], current_class, next_expected_class

    table = tables[0]
    table_bbox = table.get("Geometry", {}).get("BoundingBox", {})
    table_top = table_bbox.get("Top", 0)
    table_bottom = table_top + table_bbox.get("Height", 1)

    # Get cells
    cells = []
    for rel in table.get("Relationships", []):
        if rel["Type"] == "CHILD":
            for cell_id in rel["Ids"]:
                cell = blocks_by_id.get(cell_id)
                if cell and cell["BlockType"] == "CELL":
                    cells.append(cell)

    if not cells:
        return [], current_class, next_expected_class

    # --- Requirement 2: Detect merged cells ---
    merged_cells = detect_merged_cells(cells)
    if merged_cells:
        for mc in merged_cells:
            validation_log.append({
                "type": "MERGED_CELL",
                "page": page_num,
                "row": mc["row"],
                "col": mc["col"],
                "row_span": mc["row_span"],
                "col_span": mc["col_span"],
            })

    col_boundaries = get_column_boundaries(cells)
    max_col = max(c.get("ColumnIndex", 1) for c in cells)

    # --- Requirement 3: Build merged cell boundary map for leakage prevention ---
    # Track which (row, col) cells are spanned by merged cells
    merged_regions = set()
    for mc in merged_cells:
        for r in range(mc["row"], mc["row"] + mc["row_span"]):
            for c in range(mc["col"], mc["col"] + mc["col_span"]):
                merged_regions.add((r, c))

    # Collect words with confidence
    words = []
    for b in blocks:
        if b.get("Page") == page_num and b["BlockType"] == "WORD":
            bbox = b.get("Geometry", {}).get("BoundingBox", {})
            word_y = bbox.get("Top", 0)
            if table_top <= word_y <= table_bottom:
                center_y = word_y + bbox.get("Height", 0) / 2
                word_x = bbox.get("Left", 0)
                col = assign_to_column(word_x, col_boundaries)
                conf = b.get("Confidence", 100)
                words.append({
                    "y": center_y,
                    "x": word_x,
                    "text": b.get("Text", ""),
                    "col": col,
                    "conf": conf
                })

    if not words:
        return [], current_class, next_expected_class

    # Cluster rows
    y_positions = [w["y"] for w in words]
    row_centers = cluster_rows(y_positions, threshold=0.005)

    def find_row(y):
        min_dist = float('inf')
        best_row = 0
        for i, center in enumerate(row_centers):
            dist = abs(y - center)
            if dist < min_dist:
                min_dist = dist
                best_row = i
        return best_row

    # Build grid with confidence
    grid = defaultdict(lambda: defaultdict(list))
    conf_grid = defaultdict(lambda: defaultdict(list))

    for w in words:
        row_idx = find_row(w["y"])
        grid[row_idx][w["col"]].append((w["x"], w["text"]))
        conf_grid[row_idx][w["col"]].append(w["conf"])

    # --- Requirement 3: Vertical data leakage check ---
    # If a word's Y is far from its assigned row center, flag it
    for w in words:
        row_idx = find_row(w["y"])
        if row_idx < len(row_centers):
            distance = abs(w["y"] - row_centers[row_idx])
            if distance > 0.004:  # Close to threshold = potential leakage
                validation_log.append({
                    "type": "POTENTIAL_LEAKAGE",
                    "page": page_num,
                    "row": row_idx,
                    "col": w["col"],
                    "word": w["text"],
                    "y_distance": round(distance, 5),
                })

    # Convert to output rows
    output_rows = []
    for row_idx in sorted(grid.keys()):
        # First, extract raw text for columns 1-15
        raw_texts = []
        row_confs = []

        for col in range(1, 16):
            cell_words = grid[row_idx].get(col, [])
            cell_confs = conf_grid[row_idx].get(col, [])
            if cell_words:
                cell_words.sort(key=lambda x: x[0])
                text = " ".join(w[1] for w in cell_words)
                avg_conf = sum(cell_confs) / len(cell_confs) if cell_confs else 100
            else:
                text = ""
                avg_conf = 100
            raw_texts.append(text)
            row_confs.append(avg_conf)

        # Get brand and vendor (columns 1 and 2, indices 0 and 1)
        brand_raw = raw_texts[0] if len(raw_texts) > 0 else ""
        vendor_raw = raw_texts[1] if len(raw_texts) > 1 else ""

        # Skip header rows
        if is_header_row(brand_raw, vendor_raw):
            continue

        # Clean the brand name FIRST (remove garbage prefixes like "2025,")
        # This must happen BEFORE total row check AND class detection!
        # e.g., "2025, TOTAL DOM WHSKY-BLND" -> "TOTAL DOM WHSKY-BLND"
        brand_cleaned = clean_brand_name(brand_raw)

        # --- Requirement 4: Handle TOTAL rows - capture values for validation ---
        # Uses cleaned brand so "2025, TOTAL..." is caught by startswith('TOTAL')
        # but real brands like "HAYNER TOTAL EC" are NOT filtered
        if is_total_row(brand_cleaned, vendor_raw):
            # Capture DISTILLED SPIRITS grand total row values
            brand_upper = brand_cleaned.upper().strip()
            vendor_upper = vendor_raw.upper().strip()
            if ('DISTILLED SPIRITS' in brand_upper or
                ('DISTILLED' in brand_upper and 'SPIRITS' in vendor_upper)):
                gt_vals = parse_total_row_values(raw_texts)
                grand_total_values.update(gt_vals)

            total_class = extract_class_from_total(brand_cleaned, vendor_raw)
            if total_class:
                # Store TOTAL row numeric values for class-level validation
                total_vals = parse_total_row_values(raw_texts)
                # Normalize: try TOTAL-specific fixes first, then standard normalization
                normalized_total_class = TOTAL_CLASS_NORMALIZATION.get(
                    total_class.upper().strip(),
                    normalize_class_name(total_class)
                )
                total_row_values[normalized_total_class] = total_vals
                # Set the next expected class based on sequence
                if normalized_total_class in COGNAC_CLASS_SEQUENCE:
                    next_expected_class = COGNAC_CLASS_SEQUENCE[normalized_total_class]
                elif total_class in COGNAC_CLASS_SEQUENCE:
                    next_expected_class = COGNAC_CLASS_SEQUENCE[total_class]
            continue

        # Skip footer artifacts
        if is_footer_artifact(brand_raw):
            continue

        # Skip Top Vendor summary rows (vendor column = rank number like "1", "2", "3")
        # These appear within class sections and on summary pages — ranks go up to 100+
        vendor_stripped = vendor_raw.strip()
        if vendor_stripped.isdigit():
            continue

        # Skip Brand Cross-Reference rows (vendor column = class name)
        # These appear on cross-reference pages where class names are in the vendor column
        # OCR may garble them: "DOM DOM WHSKY-BLND WHSKY-BLND", "COCKTAIL" (missing S), etc.
        vendor_upper_check = vendor_raw.upper().strip()
        if vendor_upper_check:
            # Exact match against known class prefixes
            if vendor_upper_check in KNOWN_CLASS_PREFIXES:
                continue
            is_xref = False
            for cls in KNOWN_CLASS_PREFIXES:
                # Substring match: class name with hyphen appears within vendor text
                if '-' in cls and len(cls) >= 8 and cls in vendor_upper_check:
                    is_xref = True
                    break
                # Truncation match: vendor is a truncated class name (off by 1-2 chars)
                if len(vendor_upper_check) >= len(cls) - 2 and len(vendor_upper_check) <= len(cls):
                    if cls.startswith(vendor_upper_check):
                        is_xref = True
                        break
            if is_xref:
                continue

        # --- Requirement 6: Robust Class Detection ---
        # Check if this is a CLASS row - capture it and continue
        detected_class = get_class_name(brand_cleaned, vendor_raw)
        if detected_class:
            normalized = normalize_class_name(detected_class)
            # Special handling for truncated cognac class names like "BRNDY/CGNC-CGNC-" or "BRNDY/CGNC-CGNC"
            detected_upper = detected_class.upper().strip()
            if normalized == detected_class and detected_upper in ('BRNDY/CGNC-CGNC-', 'BRNDY/CGNC-CGNC'):
                # Use next_expected_class if available (set from previous TOTAL row)
                if next_expected_class and next_expected_class.startswith('BRNDY/CGNC-CGNC-'):
                    current_class = next_expected_class
                    next_expected_class = ""  # Clear after use
                else:
                    current_class = 'BRNDY/CGNC-CGNC-OTH'  # Default fallback
            else:
                current_class = normalized
            continue

        # Also check with is_class_name for any remaining class patterns
        if is_class_name(brand_cleaned, vendor_raw):
            # Try to construct class name
            if vendor_raw:
                constructed = brand_cleaned.upper().strip() + '-' + vendor_raw.upper().strip()
            else:
                constructed = brand_cleaned.upper().strip()
            normalized = normalize_class_name(constructed)
            # Handle truncated cognac class
            constructed_upper = constructed.upper().strip()
            if normalized == constructed and constructed_upper in ('BRNDY/CGNC-CGNC-', 'BRNDY/CGNC-CGNC'):
                if next_expected_class and next_expected_class.startswith('BRNDY/CGNC-CGNC-'):
                    current_class = next_expected_class
                    next_expected_class = ""
                else:
                    current_class = 'BRNDY/CGNC-CGNC-OTH'
            else:
                current_class = normalized
            continue

        # Skip if brand is empty after cleaning
        if not brand_cleaned.strip():
            continue

        # --- Requirement 5: Vendor Name Detection ---
        # If no numeric data in the row, it may be a vendor-only row OR a garbled class header
        if is_vendor_row(raw_texts):
            # Before skipping, try fuzzy class matching for OCR-garbled headers
            fuzzy_class = fuzzy_match_class(brand_cleaned, vendor_raw)
            if fuzzy_class:
                current_class = normalize_class_name(fuzzy_class)
                continue

            validation_log.append({
                "type": "VENDOR_ONLY_ROW",
                "page": page_num,
                "row": row_idx + 1,
                "brand": brand_cleaned,
                "vendor": vendor_raw,
            })
            continue

        # --- Requirement 1: Class Type Column Enforcement ---
        row_class = normalize_class_name(current_class)
        class_check = validate_class_assignment(row_class, brand_cleaned, page_num)
        if not class_check["valid"]:
            validation_log.append({
                "type": "MISSING_CLASS",
                "page": page_num,
                "row": row_idx + 1,
                "brand": brand_cleaned,
                "vendor": vendor_raw,
                "warning": class_check["warning"],
            })

        # --- Requirement 7: Bottle Size Validation ---
        bottle_check = validate_bottle_size_vs_cases(raw_texts)
        if not bottle_check["valid"]:
            validation_log.append({
                "type": "BOTTLE_SIZE_NO_CURMO",
                "page": page_num,
                "row": row_idx + 1,
                "brand": brand_cleaned,
                "vendor": vendor_raw,
                "class": row_class,
                "warning": bottle_check["warning"],
            })

        # Build the output row with CLASS column
        row_data = [page_num, row_idx + 1]  # Page, Row (1-based)
        row_data.append(row_class)  # Class (normalized)
        row_data.append(brand_cleaned)  # Cleaned brand
        row_data.extend(raw_texts[1:])  # Vendor and remaining columns

        # Calculate row average confidence
        non_empty_confs = [c for i, c in enumerate(row_confs) if raw_texts[i]]
        avg_row_conf = sum(non_empty_confs) / len(non_empty_confs) if non_empty_confs else 100
        row_data.append(f"{avg_row_conf:.1f}")

        output_rows.append(row_data)

    return output_rows, current_class, next_expected_class


def validate_class_totals(all_rows: list, total_row_values: dict, validation_log: list):
    """
    Requirement 4: Total Record Validation.
    Compare sum of extracted rows per class vs TOTAL row values from PDF.
    """
    # Sum extracted rows by class — validate ALL numeric fields including bottle sizes
    class_sums = defaultdict(lambda: defaultdict(int))
    class_counts = defaultdict(int)
    sum_fields = ["L12M_Cases_TY", "L12M_Cases_LY", "YTD_Cases", "CurMo_Cases",
                  "1.75L", "1.0L", "750ml", "750ml_Trav", "375ml", "200ml", "100ml", "50ml"]
    # Column indices in row_data: [Page, Row, Class, Brand, Vendor, L12M_TY(5), L12M_LY(6), Pct(7), YTD(8), CurMo(9),
    #                              1.75L(10), 1.0L(11), 750ml(12), 750ml_Trav(13), 375ml(14), 200ml(15), 100ml(16), 50ml(17)]
    field_indices = {
        "L12M_Cases_TY": 5, "L12M_Cases_LY": 6, "YTD_Cases": 8, "CurMo_Cases": 9,
        "1.75L": 10, "1.0L": 11, "750ml": 12, "750ml_Trav": 13,
        "375ml": 14, "200ml": 15, "100ml": 16, "50ml": 17,
    }

    for row in all_rows:
        cls = row[2] if len(row) > 2 else ""
        if not cls:
            continue
        class_counts[cls] += 1
        for field, idx in field_indices.items():
            if idx < len(row):
                val_str = str(row[idx]).strip().replace(',', '')
                try:
                    class_sums[cls][field] += int(float(val_str)) if val_str else 0
                except ValueError:
                    pass

    # Compare against TOTAL row values
    validated = 0
    mismatched = 0
    for cls, total_vals in total_row_values.items():
        if cls not in class_sums:
            validation_log.append({
                "type": "TOTAL_CLASS_MISSING",
                "class": cls,
                "warning": f"TOTAL row found for '{cls}' but no extracted rows",
            })
            continue

        csv_sums = class_sums[cls]
        all_match = True
        for field in sum_fields:
            pdf_val = total_vals.get(field, 0)
            csv_val = csv_sums.get(field, 0)
            if pdf_val != 0 and pdf_val != csv_val:
                diff = csv_val - pdf_val
                pct = abs(diff) / max(pdf_val, 1) * 100
                # Only flag significant differences (>0.1%)
                if pct > 0.1:
                    all_match = False
                    validation_log.append({
                        "type": "TOTAL_MISMATCH",
                        "class": cls,
                        "field": field,
                        "pdf_total": pdf_val,
                        "csv_sum": csv_val,
                        "diff": diff,
                        "pct_off": round(pct, 1),
                        "row_count": class_counts[cls],
                    })

        if all_match:
            validated += 1
        else:
            mismatched += 1

    return validated, mismatched


def write_validation_report(validation_log: list, output_dir: str, total_rows: int,
                            class_validated: int, class_mismatched: int):
    """Write validation report to CSV and print summary."""
    # Write validation log CSV
    log_path = os.path.join(output_dir, "extraction_validation_log.csv")
    with open(log_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Type", "Page", "Row", "Class", "Brand", "Vendor", "Field",
                         "PDF_Total", "CSV_Sum", "Diff", "Warning"])
        for entry in validation_log:
            writer.writerow([
                entry.get("type", ""),
                entry.get("page", ""),
                entry.get("row", ""),
                entry.get("class", ""),
                entry.get("brand", ""),
                entry.get("vendor", ""),
                entry.get("field", ""),
                entry.get("pdf_total", ""),
                entry.get("csv_sum", ""),
                entry.get("diff", ""),
                entry.get("warning", ""),
            ])

    # Count by type
    type_counts = Counter(e["type"] for e in validation_log)

    print("\n" + "=" * 70)
    print("EXTRACTION VALIDATION REPORT")
    print("=" * 70)
    print(f"\n  Total rows extracted:         {total_rows:,}")
    print(f"\n  Class TOTAL validation:")
    print(f"    Classes validated (match):   {class_validated}")
    print(f"    Classes mismatched:          {class_mismatched}")
    print(f"\n  Validation issues by type:")
    for vtype, count in type_counts.most_common():
        print(f"    {vtype:<30} {count:>5}")
    print(f"\n  Validation log saved to: {log_path}")

    return type_counts


def extract_all_pages(json_path: str, start_page: int, end_page: int, output_path: str):
    """Extract all pages and write to CSV with built-in validation."""

    print(f"Loading {json_path}...")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    blocks = data.get("Blocks", [])
    blocks_by_id = {b["Id"]: b for b in blocks}
    print(f"  Loaded {len(blocks)} blocks")

    print(f"\nExtracting pages {start_page}-{end_page}...")

    all_rows = []
    current_class = ""
    next_expected_class = ""
    validation_log = []        # Shared validation log across all pages
    total_row_values = {}      # Req 4: TOTAL row values per class

    for page_num in range(start_page, end_page + 1):
        page_rows, current_class, next_expected_class = extract_page_raw(
            blocks, blocks_by_id, page_num, current_class, next_expected_class,
            validation_log, total_row_values
        )
        all_rows.extend(page_rows)

        if page_num % 50 == 0:
            print(f"  Page {page_num}... ({len(all_rows):,} rows)")

    print(f"\nExtraction complete: {len(all_rows):,} rows")

    # --- Requirement 4: Validate class totals ---
    print("\nValidating class totals against PDF TOTAL rows...")
    class_validated, class_mismatched = validate_class_totals(
        all_rows, total_row_values, validation_log
    )

    # --- Requirement 1: Check for rows without class ---
    no_class_count = sum(1 for row in all_rows if not row[2] or not row[2].strip())
    if no_class_count > 0:
        print(f"  WARNING: {no_class_count} rows have no class assigned!")

    # --- Duplicate detection ---
    seen_keys = Counter()
    for row in all_rows:
        if len(row) >= 5:
            key = (str(row[2]).upper().strip(), str(row[3]).upper().strip(), str(row[4]).upper().strip())
            seen_keys[key] += 1
    duplicates = {k: v for k, v in seen_keys.items() if v > 1}
    if duplicates:
        for (cls, brand, vendor), count in list(duplicates.items())[:5]:
            validation_log.append({
                "type": "DUPLICATE_ROW",
                "class": cls,
                "brand": brand,
                "vendor": vendor,
                "warning": f"Appears {count} times",
            })

    # Create output directory if needed
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    # Write to CSV
    print(f"\nWriting to {output_path}...")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
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

    # Write validation report
    type_counts = write_validation_report(
        validation_log, output_dir, len(all_rows), class_validated, class_mismatched
    )

    return all_rows


def extract_single_page(json_path: str, page_num: int, output_path: str):
    """Extract a single page (for eyeball validation)."""

    print(f"Loading {json_path}...")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    blocks = data.get("Blocks", [])
    blocks_by_id = {b["Id"]: b for b in blocks}

    print(f"Extracting page {page_num}...")
    rows, _, _ = extract_page_raw(blocks, blocks_by_id, page_num, "", "")

    # Create output directory if needed
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    print(f"Writing to {output_path}...")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Proper column names for Brand Summary (with Class column)
        header = [
            "Page", "Row", "Class", "Brand", "Vendor",
            "L12M_Cases_TY", "L12M_Cases_LY", "Pct_of_Type",
            "YTD_Cases", "CurMo_Cases",
            "1.75L", "1.0L", "750ml", "750ml_Trav", "375ml", "200ml", "100ml", "50ml",
            "Avg_Confidence"
        ]
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)

    print(f"  Done! {len(rows)} rows written")
    return rows


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract Brand Summary pages")
    parser.add_argument("--json", default="Textract/631_9L_0925.json", help="Textract JSON path")
    parser.add_argument("--start", type=int, default=11, help="Start page (default: 11)")
    parser.add_argument("--end", type=int, default=382, help="End page (default: 382)")
    parser.add_argument("--output", default="output_raw/brand_summary_all.csv", help="Output CSV path")
    parser.add_argument("--page", type=int, help="Extract single page only")

    args = parser.parse_args()

    print("="*70)
    print("BRAND SUMMARY EXTRACTOR")
    print("="*70)

    if args.page:
        # Single page mode
        output_dir = os.path.dirname(args.output) or "eyeball_validation"
        output_file = os.path.join(output_dir, f"brand_summary_page_{args.page}.csv")
        extract_single_page(args.json, args.page, output_file)
    else:
        # All pages mode
        extract_all_pages(args.json, args.start, args.end, args.output)


if __name__ == "__main__":
    main()
