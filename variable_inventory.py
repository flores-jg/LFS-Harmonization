"""
LFS Variable Inventory Scanner
===============================
Scans all raw CSV files and produces a complete inventory of every variable name
across all years. Used to inspect for legacy equivalents when building the crosswalk.

Usage:
    python variable_inventory.py -i ./raw -o variable_inventory.csv
"""

import pandas as pd
import argparse
import re
import sys
from pathlib import Path
from collections import defaultdict

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

# ============================================================
# 2024 TARGET SCHEMA (52 variables)
# ============================================================

TARGET_SCHEMA = [
    'PUFREG', 'PUFHHNUM', 'HHMEM', 'PUFPWGTPRV', 'PUFSVYMO', 'PUFSVYYR',
    'PUFPSU', 'PUFRPL', 'PUFHHSIZE',
    'PUFC01_LNO', 'PUFC03_REL', 'PUFC04_SEX', 'PUFC05_AGE', 'PUFC06_MSTAT',
    'PUFC07_GRADE', 'PUFC08_CURSCH', 'PUFC09_GRADTECH', 'PUFC09A_NFORMAL',
    'PUFC10_CONWR', 'PUFC11_WORK', 'PUFC11A_ARRANGEMENT', 'PUFC12_JOB',
    'PUFC12A_PROVMUN', 'PUFC14_PROCC', 'PUFC16_PKB', 'PUFC17_NATEM',
    'PUFC18_PNWHRS', 'PUFC19_PHOURS', 'PUFC20_PWMORE', 'PUFC21_PLADDW',
    'PUFC22_PFWRK', 'PUFC23_PCLASS', 'PUFC24_PBASIS', 'PUFC25_PBASIC',
    'PUFC26_OJOB', 'PUFC27_NJOBS', 'PUFC28_THOURS', 'PUFC29_WWM48H',
    'PUFC30_LOOKW', 'PUFC31_FLWRK', 'PUFC32_JOBSM', 'PUFC33_WEEKS',
    'PUFC34_WYNOT', 'PUFC35_LTLOOKW', 'PUFC36_AVAIL', 'PUFC37_WILLING',
    'PUFC38_PREVJOB', 'PUFC39_YEAR', 'PUFC39_MONTH', 'PUFC41_POCC',
    'PUFC43_QKB', 'PUFNEWEMPSTAT',
]

# ============================================================
# COLUMN PRIORITY (from lfs_harmonizer_complete_v8.py)
# Used to check which raw variables are already mapped
# ============================================================

COLUMN_PRIORITY = {
    'PUFREG': ['PUFREG', 'CREG', 'REG'],
    'PUFSVYYR': ['PUFSVYYR', 'SVYYR', 'CYEAR'],
    'PUFSVYMO': ['PUFSVYMO', 'SVYMO', 'CMONTH'],
    'PUFHHNUM': ['PUFHHNUM', 'HHNUM'],
    'PUFPSU': ['PUFPSU', 'PSU', 'PSU_NO', 'STRATUM'],
    'PUFHHSIZE': ['PUFHHSIZE', 'HHID'],
    'PUFRPL': ['PUFRPL', 'CRPM'],
    'PUFPWGTPRV': ['PUFPWGTPRV', 'PUFPWGT', 'PUFPWGTFIN', 'CFWGT', 'FWGT', 'PWGT'],
    'PUFC01_LNO': ['PUFC01_LNO', 'C101_LNO', 'CC101_LNO', 'C04_LNO', 'A01_LNO'],
    'PUFC03_REL': ['PUFC03_REL', 'C05_REL', 'CC05_REL', 'C03_NEWMEM', 'CC03_NEWMEM'],
    'PUFC04_SEX': ['PUFC04_SEX', 'C06_SEX', 'CC06_SEX'],
    'PUFC05_AGE': ['PUFC05_AGE', 'C07_AGE', 'CC07_AGE'],
    'PUFC06_MSTAT': ['PUFC06_MSTAT', 'C08_MSTAT', 'C08_MS', 'CC08_MSTAT', 'CC08_MS'],
    'PUFC07_GRADE': ['PUFC07_GRADE', 'J12C09_GRADE', 'C09_GRD', 'C09_GRADE', 'CC09_GRADE'],
    'PUFC08_CURSCH': ['PUFC08_CURSCH', 'A02_CURSCH', 'A02_CSCH'],
    'PUFC09_GRADTECH': ['PUFC09_GRADTECH', 'J12C11_GRADTECH', 'J12C11COURSE'],
    'PUFC10_CONWR': ['PUFC10_CONWR', 'PUFC08_CONWR', 'C10_CONWR', 'C10_CNWR', 'CC10_CONWR'],
    'PUFC11_WORK': ['PUFC11_WORK', 'PUFC09_WORK', 'C13_WORK', 'CC13_WORK', 'CC01_WORK', 'B01_WORK'],
    'PUFC12_JOB': ['PUFC12_JOB', 'PUFC10_JOB', 'C14_JOB', 'CC14_JOB', 'CC02_JOB', 'B02_JOB'],
    'PUFNEWEMPSTAT': ['PUFNEWEMPSTAT', 'NEWEMPSTAT', 'CEMPST1', 'CEMPST2', 'NEWEMPST'],
    'PUFC14_PROCC': ['PUFC14_PROCC', 'PUFC13_PROCC', 'C16_PROCC', 'C16_PROC', 'CC16_PROCC',
                     'C16F2_PROCC', 'C16L2_PROCC', 'CC12_USOCC', 'J01_USOCC', 'J01_USOC'],
    'PUFC16_PKB': ['PUFC16_PKB', 'PUFC15_PKB', 'C18_PKB', 'CC18_PKB',
                   'C18F2_PKB', 'C18L2_PKB', 'CC06_IND', 'J03_OKB'],
    'PUFC17_NATEM': ['PUFC17_NATEM', 'PUFC16_NATEM', 'C20_NATEM', 'C20_NTEM', 'CC20_NATEM'],
    'PUFC18_PNWHRS': ['PUFC18_PNWHRS', 'PUFC17_PNWHRS', 'C21_PNWHRS', 'C21_PWHR', 'CC21_PNWHRS', 'CC18_PNWHRS'],
    'PUFC19_PHOURS': ['PUFC19_PHOURS', 'PUFC18_PHOURS', 'C22_PHOURS', 'C22_PHRS', 'CC22_PHOURS'],
    'PUFC20_PWMORE': ['PUFC20_PWMORE', 'PUFC19_PWMORE', 'C23_PWMORE', 'C23_PWMR', 'CC23_PWMORE'],
    'PUFC21_PLADDW': ['PUFC21_PLADDW', 'PUFC20_PLADDW', 'C24_PLADDW', 'C24_PLAW', 'CC24_PLADDW'],
    'PUFC22_PFWRK': ['PUFC22_PFWRK', 'PUFC20B_FTWORK', 'C25_PFWRK', 'C25_PFWK', 'CC25_PFWRK'],
    'PUFC23_PCLASS': ['PUFC23_PCLASS', 'PUFC21_PCLASS', 'C19_PCLASS', 'C19PCLAS', 'CC19_PCLASS'],
    'PUFC24_PBASIS': ['PUFC24_PBASIS', 'C26_PBASIS', 'C26_PBIS', 'CC26_PBASIS'],
    'PUFC25_PBASIC': ['PUFC25_PBASIC', 'C27_PBASIC', 'C27_PBSC', 'CC27_PBASIC', 'C36_OBASIC', 'C36_OBIC'],
    'PUFC26_OJOB': ['PUFC26_OJOB', 'PUFC22_OJOB', 'C28_OJOB', 'CC28_OJOB'],
    'PUFC27_NJOBS': ['PUFC27_NJOBS', 'A03_JOBS'],
    'PUFC28_THOURS': ['PUFC28_THOURS', 'PUFC23_THOURS', 'A04_THOURS', 'A04_THRS'],
    'PUFC29_WWM48H': ['PUFC29_WWM48H', 'PUFC24_WWM48H', 'A05_RWM48H', 'A05_R48H'],
    'PUFC30_LOOKW': ['PUFC30_LOOKW', 'PUFC25_LOOKW', 'C38_LOOKW', 'C38_LOKW', 'CC38_LOOKW', 'CC30_LOOKW'],
    'PUFC31_FLWRK': ['PUFC31_FLWRK', 'PUFC25B_FTWORK', 'C41_FLWRK', 'C41_FLWK', 'CC41_FLWRK'],
    'PUFC32_JOBSM': ['PUFC32_JOBSM', 'C39_JOBSM', 'C39_JBSM', 'CC39_JOBSM', 'CC32_JOBSM'],
    'PUFC33_WEEKS': ['PUFC33_WEEKS', 'C40_WEEKS', 'C40_WKS', 'CC40_WEEKS', 'CC33_WEEKS'],
    'PUFC34_WYNOT': ['PUFC34_WYNOT', 'PUFC26_WYNOT', 'C42_WYNOT', 'C42_WYNT', 'CC42_WYNOT'],
    'PUFC35_LTLOOKW': ['PUFC35_LTLOOKW', 'A06_LTLOOKW', 'A06_LLKW', 'CC35_LTLOOKW'],
    'PUFC36_AVAIL': ['PUFC36_AVAIL', 'PUFC27_AVAIL', 'C37_AVAIL', 'C37_AVIL', 'CC37_AVAIL', 'CC36_AVAIL'],
    'PUFC37_WILLING': ['PUFC37_WILLING', 'A07_WILLING', 'A07_WLNG'],
    'PUFC38_PREVJOB': ['PUFC38_PREVJOB', 'PUFC28_PREVJOB', 'C43_LBEF', 'CC43_LBEF'],
    'PUFC39_YEAR': ['PUFC39_YEAR', 'PUFC29_YEAR'],
    'PUFC39_MONTH': ['PUFC39_MONTH', 'PUFC29_MONTH'],
    'PUFC41_POCC': ['PUFC41_POCC', 'PUFC40_POCC', 'PUFC31_POCC', 'C45_POCC', 'CC45_POCC',
                    'C45F2_POCC', 'C45L2_POCC', 'CC10_POCC'],
    'PUFC43_QKB': ['PUFC43_QKB', 'PUFC33_QKB', 'A09_PQKB', 'A09F2_PQKB', 'A09L2_PQKB', 'PQKB', 'QKB'],
}

# ============================================================
# HELPERS
# ============================================================

def extract_year_month(filepath):
    """Extract year and month from LFS filename."""
    filename = Path(filepath).stem.upper()
    month_map = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
                 'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    year_match = re.search(r'(20\d{2}|199\d)', filename)
    year = int(year_match.group(1)) if year_match else None
    month = next((m_num for m_name, m_num in month_map.items() if m_name in filename), None)
    return year, month


def read_headers(filepath):
    """Read only column headers from a CSV file (no data loaded)."""
    for enc in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(filepath, nrows=0, encoding=enc)
            return list(df.columns)
        except Exception:
            continue
    # Last resort
    try:
        df = pd.read_csv(filepath, nrows=0, encoding='utf-8', engine='python',
                         on_bad_lines='skip')
        return list(df.columns)
    except Exception:
        return []


def build_reverse_map(column_priority):
    """Build source_var (upper) -> target_var mapping from COLUMN_PRIORITY."""
    reverse = {}
    for target, sources in column_priority.items():
        for src in sources:
            reverse[src.upper()] = target
    return reverse


def detect_gaps(years_set):
    """Detect temporal gaps in a set of years.
    Returns (has_gaps: bool, gap_details: str)."""
    if not years_set:
        return False, ''
    sorted_years = sorted(years_set)
    if len(sorted_years) <= 1:
        return False, ''

    # Build set of all years in the range
    full_range = set(range(sorted_years[0], sorted_years[-1] + 1))
    missing = sorted(full_range - years_set)

    if not missing:
        return False, ''

    # Group consecutive missing years into ranges
    gap_ranges = []
    start = missing[0]
    end = missing[0]
    for y in missing[1:]:
        if y == end + 1:
            end = y
        else:
            gap_ranges.append((start, end))
            start = y
            end = y
    gap_ranges.append((start, end))

    details = ', '.join(
        f"{s}" if s == e else f"{s}-{e}"
        for s, e in gap_ranges
    )
    return True, f"missing: {details}"


def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'replace').decode('ascii'))


# ============================================================
# MAIN SCAN
# ============================================================

def scan_all_files(input_dir):
    """Scan all raw CSVs and build the variable inventory."""
    input_path = Path(input_dir)

    # Discover files (deduplicate by lowercase name)
    all_files = list(input_path.glob('*.csv')) + list(input_path.glob('*.CSV'))
    seen = set()
    files = []
    for f in sorted(all_files, key=lambda x: x.name.lower()):
        name_lower = f.name.lower()
        if name_lower not in seen:
            seen.add(name_lower)
            files.append(f)

    safe_print(f"Found {len(files)} CSV files in {input_dir}")
    safe_print("=" * 60)

    # var_name -> { 'files': [filenames], 'years': set() }
    inventory = defaultdict(lambda: {'files': [], 'years': set()})

    for i, f in enumerate(files, 1):
        year, month = extract_year_month(f)
        headers = read_headers(f)

        label = f"[{i}/{len(files)}] {f.name} (year={year}): {len(headers)} columns"
        safe_print(label)

        for col in headers:
            inventory[col]['files'].append(f.name)
            if year is not None:
                inventory[col]['years'].add(year)

    # Build reverse map for mapped_to lookup
    reverse_map = build_reverse_map(COLUMN_PRIORITY)
    target_upper = {t.upper() for t in TARGET_SCHEMA}

    # Build output rows
    rows = []
    for var_name, info in inventory.items():
        years_set = info['years']
        sorted_years = sorted(years_set)
        has_gaps, gap_details = detect_gaps(years_set)

        first_seen = sorted_years[0] if sorted_years else None
        last_seen = sorted_years[-1] if sorted_years else None
        year_range = f"{first_seen}-{last_seen}" if first_seen and last_seen else ''

        mapped_to = reverse_map.get(var_name.upper(), '')
        in_target = 'YES' if var_name.upper() in target_upper else ''

        rows.append({
            'variable': var_name,
            'file_count': len(info['files']),
            'year_range': year_range,
            'years': ','.join(str(y) for y in sorted_years),
            'first_seen': first_seen,
            'last_seen': last_seen,
            'has_gaps': 'YES' if has_gaps else '',
            'gap_details': gap_details,
            'mapped_to': mapped_to,
            'in_target_schema': in_target,
            'files': ';'.join(info['files']),
        })

    df = pd.DataFrame(rows)
    df = df.sort_values('file_count', ascending=False).reset_index(drop=True)
    return df


def print_summary(df):
    """Print console summary of the inventory."""
    safe_print("\n" + "=" * 60)
    safe_print("VARIABLE INVENTORY SUMMARY")
    safe_print("=" * 60)

    safe_print(f"\nTotal unique variables found: {len(df)}")
    safe_print(f"Variables mapped to a target: {(df['mapped_to'] != '').sum()}")
    safe_print(f"Variables NOT mapped: {(df['mapped_to'] == '').sum()}")
    safe_print(f"Variables with temporal gaps: {(df['has_gaps'] == 'YES').sum()}")

    # Check target schema coverage
    target_upper = {t.upper() for t in TARGET_SCHEMA}
    found_upper = {v.upper() for v in df['variable']}

    missing_from_raw = [t for t in TARGET_SCHEMA if t.upper() not in found_upper]
    present_in_raw = [t for t in TARGET_SCHEMA if t.upper() in found_upper]

    safe_print(f"\n--- TARGET SCHEMA COVERAGE ({len(TARGET_SCHEMA)} variables) ---")
    safe_print(f"Found in raw files: {len(present_in_raw)}")
    safe_print(f"NOT found in any raw file: {len(missing_from_raw)}")

    if missing_from_raw:
        safe_print("\nTarget variables with ZERO matches in raw data:")
        for var in missing_from_raw:
            safe_print(f"  - {var}")

    # Show target vars that only appear in recent years
    safe_print("\n--- TARGET VARIABLES BY YEAR COVERAGE ---")
    for var in TARGET_SCHEMA:
        row = df[df['variable'].str.upper() == var.upper()]
        if len(row) == 0:
            safe_print(f"  {var:<25s}  NOT FOUND")
        else:
            r = row.iloc[0]
            gap_note = f"  GAPS: {r['gap_details']}" if r['has_gaps'] == 'YES' else ''
            safe_print(f"  {var:<25s}  {r['year_range']:<12s}  {r['file_count']:>3d} files{gap_note}")

    # Show variables with gaps
    gapped = df[df['has_gaps'] == 'YES'].sort_values('variable')
    if len(gapped) > 0:
        safe_print(f"\n--- VARIABLES WITH TEMPORAL GAPS ({len(gapped)}) ---")
        for _, r in gapped.iterrows():
            safe_print(f"  {r['variable']:<25s}  {r['year_range']:<12s}  {r['gap_details']}")

    # Show unmapped variables (potential crosswalk candidates)
    unmapped = df[(df['mapped_to'] == '') & (df['in_target_schema'] == '')]
    unmapped = unmapped.sort_values('file_count', ascending=False)
    safe_print(f"\n--- UNMAPPED VARIABLES ({len(unmapped)}) ---")
    safe_print("(These are in raw files but not mapped to any target variable)")
    for _, r in unmapped.head(50).iterrows():
        safe_print(f"  {r['variable']:<25s}  {r['year_range']:<12s}  {r['file_count']:>3d} files")
    if len(unmapped) > 50:
        safe_print(f"  ... and {len(unmapped) - 50} more (see CSV for full list)")


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='LFS Variable Inventory Scanner')
    parser.add_argument('-i', '--input-dir', default='./raw',
                        help='Input directory with raw CSV files (default: ./raw)')
    parser.add_argument('-o', '--output', default='variable_inventory.csv',
                        help='Output CSV file (default: variable_inventory.csv)')
    args = parser.parse_args()

    df = scan_all_files(args.input_dir)
    df.to_csv(args.output, index=False, encoding='utf-8-sig')
    safe_print(f"\nInventory saved to: {args.output}")

    print_summary(df)


if __name__ == '__main__':
    main()
