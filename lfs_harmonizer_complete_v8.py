"""
LFS Complete Harmonization Processor v8
========================================
Final version with 47 core columns, all with good coverage across 2005-2024.

Changes from v7:
- Removed PUFURB2015 (not in 2024)
- Removed derived variables (_1DIG, _HARM)
- Removed PUFC09A_NFORMAL, PUFC11A_ARRANGEMENT, PUFC12A_PROVMUN (too many NaN years)
- All 47 output columns have good coverage for time series analysis

Usage:
    python lfs_harmonizer_complete_v8.py -i ./raw -o ./output --batch-size 5
"""

import pandas as pd
import numpy as np
import json
import argparse
from pathlib import Path
from datetime import datetime
import warnings
import re
import gc
import sys
warnings.filterwarnings('ignore')

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

# ============================================================
# COLUMN PRIORITY - COMPLETE MAPPINGS
# ============================================================
# Format: 'TARGET': ['variant1', 'variant2', ...]
# Order matters - first non-empty match wins

COLUMN_PRIORITY = {
    # ========== SURVEY IDENTIFIERS ==========
    'PUFREG': ['PUFREG', 'CREG', 'REG'],
    'PUFSVYYR': ['PUFSVYYR', 'SVYYR', 'CYEAR'],
    'PUFSVYMO': ['PUFSVYMO', 'SVYMO', 'CMONTH'],
    'PUFHHNUM': ['PUFHHNUM', 'HHNUM'],
    'PUFPSU': ['PUFPSU', 'PSU', 'PSU_NO', 'STRATUM'],
    'PUFHHSIZE': ['PUFHHSIZE', 'HHID'],
    'PUFRPL': ['PUFRPL', 'CRPM'],
    
    # ========== WEIGHT ==========
    'PUFPWGTPRV': ['PUFPWGTPRV', 'PUFPWGT', 'PUFPWGTFIN', 'CFWGT', 'FWGT', 'PWGT'],
    
    # ========== DEMOGRAPHICS ==========
    'PUFC01_LNO': ['PUFC01_LNO', 'C101_LNO', 'CC101_LNO', 'C04_LNO', 'A01_LNO'],
    'PUFC03_REL': ['PUFC03_REL', 'C05_REL', 'CC05_REL', 'C03_NEWMEM', 'CC03_NEWMEM'],
    'PUFC04_SEX': ['PUFC04_SEX', 'C06_SEX', 'CC06_SEX'],
    'PUFC05_AGE': ['PUFC05_AGE', 'C07_AGE', 'CC07_AGE'],
    'PUFC06_MSTAT': ['PUFC06_MSTAT', 'C08_MSTAT', 'C08_MS', 'CC08_MSTAT', 'CC08_MS'],
    'PUFC07_GRADE': ['PUFC07_GRADE', 'J12C09_GRADE', 'C09_GRD', 'C09_GRADE', 'CC09_GRADE'],
    'PUFC08_CURSCH': ['PUFC08_CURSCH', 'A02_CURSCH', 'A02_CSCH'],
    'PUFC09_GRADTECH': ['PUFC09_GRADTECH', 'J12C11_GRADTECH', 'J12C11COURSE'],
    
    # ========== EMPLOYMENT STATUS ==========
    # OFW Indicator: 2021+ uses PUFC08_CONWR, 2016-2020 uses PUFC10_CONWR
    'PUFC10_CONWR': ['PUFC10_CONWR', 'PUFC08_CONWR', 'C10_CONWR', 'C10_CNWR', 'CC10_CONWR'],
    
    # Work Indicator: 2021+ uses PUFC09_WORK, 2016-2020 uses PUFC11_WORK
    'PUFC11_WORK': ['PUFC11_WORK', 'PUFC09_WORK', 'C13_WORK', 'CC13_WORK', 'CC01_WORK', 'B01_WORK'],
    
    # Job Indicator: 2021+ uses PUFC10_JOB, 2016-2020 uses PUFC12_JOB
    'PUFC12_JOB': ['PUFC12_JOB', 'PUFC10_JOB', 'C14_JOB', 'CC14_JOB', 'CC02_JOB', 'B02_JOB'],
    
    'PUFNEWEMPSTAT': ['PUFNEWEMPSTAT', 'NEWEMPSTAT', 'CEMPST1', 'CEMPST2', 'NEWEMPST'],
    
    # ========== OCCUPATION & INDUSTRY ==========
    # Primary Occupation: 2021+ uses PUFC13_PROCC, 2016-2020 uses PUFC14_PROCC
    'PUFC14_PROCC': ['PUFC14_PROCC', 'PUFC13_PROCC', 'C16_PROCC', 'C16_PROC', 'CC16_PROCC',
                     'C16F2_PROCC', 'C16L2_PROCC', 'CC12_USOCC', 'J01_USOCC', 'J01_USOC'],
    
    # Industry: 2021+ uses PUFC15_PKB, 2016-2020 uses PUFC16_PKB
    'PUFC16_PKB': ['PUFC16_PKB', 'PUFC15_PKB', 'C18_PKB', 'CC18_PKB', 
                   'C18F2_PKB', 'C18L2_PKB', 'CC06_IND', 'J03_OKB'],
    
    # Nature of Employment: 2021+ uses PUFC16_NATEM
    'PUFC17_NATEM': ['PUFC17_NATEM', 'PUFC16_NATEM', 'C20_NATEM', 'C20_NTEM', 'CC20_NATEM'],
    
    # ========== WORKING HOURS ==========
    # Normal Hours: 2021+ uses PUFC17_PNWHRS
    'PUFC18_PNWHRS': ['PUFC18_PNWHRS', 'PUFC17_PNWHRS', 'C21_PNWHRS', 'C21_PWHR', 'CC21_PNWHRS', 'CC18_PNWHRS'],
    
    # Hours Worked: 2021+ uses PUFC18_PHOURS
    'PUFC19_PHOURS': ['PUFC19_PHOURS', 'PUFC18_PHOURS', 'C22_PHOURS', 'C22_PHRS', 'CC22_PHOURS'],
    
    # ========== UNDEREMPLOYMENT ==========
    # Want More Hours: 2021+ uses PUFC19_PWMORE
    'PUFC20_PWMORE': ['PUFC20_PWMORE', 'PUFC19_PWMORE', 'C23_PWMORE', 'C23_PWMR', 'CC23_PWMORE'],
    
    # Look for Add Work: 2021+ uses PUFC20_PLADDW
    'PUFC21_PLADDW': ['PUFC21_PLADDW', 'PUFC20_PLADDW', 'C24_PLADDW', 'C24_PLAW', 'CC24_PLADDW'],
    
    # First Time Work: 2021+ uses PUFC20B_FTWORK
    'PUFC22_PFWRK': ['PUFC22_PFWRK', 'PUFC20B_FTWORK', 'C25_PFWRK', 'C25_PFWK', 'CC25_PFWRK'],
    
    # Class of Worker: 2021+ uses PUFC21_PCLASS
    'PUFC23_PCLASS': ['PUFC23_PCLASS', 'PUFC21_PCLASS', 'C19_PCLASS', 'C19PCLAS', 'CC19_PCLASS'],
    
    # ========== PAY ==========
    'PUFC24_PBASIS': ['PUFC24_PBASIS', 'C26_PBASIS', 'C26_PBIS', 'CC26_PBASIS'],
    'PUFC25_PBASIC': ['PUFC25_PBASIC', 'C27_PBASIC', 'C27_PBSC', 'CC27_PBASIC', 'C36_OBASIC', 'C36_OBIC'],
    
    # ========== OTHER JOB ==========
    # Other Job: 2021+ uses PUFC22_OJOB
    'PUFC26_OJOB': ['PUFC26_OJOB', 'PUFC22_OJOB', 'C28_OJOB', 'CC28_OJOB'],
    
    # ========== MULTIPLE JOBS ==========
    'PUFC27_NJOBS': ['PUFC27_NJOBS', 'A03_JOBS'],
    
    # Total Hours: 2021+ uses PUFC23_THOURS
    'PUFC28_THOURS': ['PUFC28_THOURS', 'PUFC23_THOURS', 'A04_THOURS', 'A04_THRS'],
    
    # Reason >48hrs: 2021+ uses PUFC24_WWM48H
    'PUFC29_WWM48H': ['PUFC29_WWM48H', 'PUFC24_WWM48H', 'A05_RWM48H', 'A05_R48H'],
    
    # ========== JOB SEARCH ==========
    # Looked for Work: 2021+ uses PUFC25_LOOKW
    'PUFC30_LOOKW': ['PUFC30_LOOKW', 'PUFC25_LOOKW', 'C38_LOOKW', 'C38_LOKW', 'CC38_LOOKW', 'CC30_LOOKW'],
    
    # First Time Look: 2021+ uses PUFC25B_FTWORK
    'PUFC31_FLWRK': ['PUFC31_FLWRK', 'PUFC25B_FTWORK', 'C41_FLWRK', 'C41_FLWK', 'CC41_FLWRK'],
    
    'PUFC32_JOBSM': ['PUFC32_JOBSM', 'C39_JOBSM', 'C39_JBSM', 'CC39_JOBSM', 'CC32_JOBSM'],
    'PUFC33_WEEKS': ['PUFC33_WEEKS', 'C40_WEEKS', 'C40_WKS', 'CC40_WEEKS', 'CC33_WEEKS'],
    
    # Why Not Looking: 2021+ uses PUFC26_WYNOT
    'PUFC34_WYNOT': ['PUFC34_WYNOT', 'PUFC26_WYNOT', 'C42_WYNOT', 'C42_WYNT', 'CC42_WYNOT'],
    
    'PUFC35_LTLOOKW': ['PUFC35_LTLOOKW', 'A06_LTLOOKW', 'A06_LLKW', 'CC35_LTLOOKW'],
    
    # Available: 2021+ uses PUFC27_AVAIL
    'PUFC36_AVAIL': ['PUFC36_AVAIL', 'PUFC27_AVAIL', 'C37_AVAIL', 'C37_AVIL', 'CC37_AVAIL', 'CC36_AVAIL'],
    
    'PUFC37_WILLING': ['PUFC37_WILLING', 'A07_WILLING', 'A07_WLNG'],
    
    # ========== PREVIOUS WORK ==========
    # Previous Job: 2021+ uses PUFC28_PREVJOB
    'PUFC38_PREVJOB': ['PUFC38_PREVJOB', 'PUFC28_PREVJOB', 'C43_LBEF', 'CC43_LBEF'],
    
    'PUFC39_YEAR': ['PUFC39_YEAR', 'PUFC29_YEAR'],
    'PUFC39_MONTH': ['PUFC39_MONTH', 'PUFC29_MONTH'],
    
    # Previous Occupation: 2021+ uses PUFC31_POCC
    'PUFC41_POCC': ['PUFC41_POCC', 'PUFC40_POCC', 'PUFC31_POCC', 'C45_POCC', 'CC45_POCC', 
                    'C45F2_POCC', 'C45L2_POCC', 'CC10_POCC'],
    
    # Previous Industry: 2021+ uses PUFC33_QKB
    'PUFC43_QKB': ['PUFC43_QKB', 'PUFC33_QKB', 'A09_PQKB', 'A09F2_PQKB', 'A09L2_PQKB', 'PQKB', 'QKB'],
}

# ============================================================
# OUTPUT SCHEMA - 47 COLUMNS
# ============================================================
OUTPUT_SCHEMA = [
    # Survey identifiers (7)
    'PUFREG', 'PUFSVYYR', 'PUFSVYMO', 'PUFHHNUM', 'PUFPSU', 'PUFHHSIZE', 'PUFRPL',
    # Weight (1)
    'PUFPWGTPRV',
    # Demographics (8)
    'PUFC01_LNO', 'PUFC03_REL', 'PUFC04_SEX', 'PUFC05_AGE', 'PUFC06_MSTAT',
    'PUFC07_GRADE', 'PUFC08_CURSCH', 'PUFC09_GRADTECH',
    # Employment status (4)
    'PUFC10_CONWR', 'PUFC11_WORK', 'PUFC12_JOB', 'PUFNEWEMPSTAT',
    # Occupation & Industry (3)
    'PUFC14_PROCC', 'PUFC16_PKB', 'PUFC17_NATEM',
    # Working hours (2)
    'PUFC18_PNWHRS', 'PUFC19_PHOURS',
    # Underemployment & Class (4)
    'PUFC20_PWMORE', 'PUFC21_PLADDW', 'PUFC22_PFWRK', 'PUFC23_PCLASS',
    # Pay (2)
    'PUFC24_PBASIS', 'PUFC25_PBASIC',
    # Other job (1)
    'PUFC26_OJOB',
    # Multiple jobs (3)
    'PUFC27_NJOBS', 'PUFC28_THOURS', 'PUFC29_WWM48H',
    # Job search (8)
    'PUFC30_LOOKW', 'PUFC31_FLWRK', 'PUFC32_JOBSM', 'PUFC33_WEEKS',
    'PUFC34_WYNOT', 'PUFC35_LTLOOKW', 'PUFC36_AVAIL', 'PUFC37_WILLING',
    # Previous work (4)
    'PUFC38_PREVJOB', 'PUFC39_YEAR', 'PUFC39_MONTH', 'PUFC41_POCC', 'PUFC43_QKB',
]

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def safe_numeric(x):
    if x is None: return np.nan
    if isinstance(x, (int, np.integer)): return float(x)
    if isinstance(x, (float, np.floating)): return x if not np.isnan(x) else np.nan
    if isinstance(x, str):
        x = x.strip()
        if x == '' or x.lower() in ['nan', 'na', '.', 'none']: return np.nan
        try: return float(x)
        except: return np.nan
    return np.nan

def safe_int(x):
    val = safe_numeric(x)
    return np.nan if pd.isna(val) else int(val)

def clean_column(series):
    return pd.to_numeric(series, errors='coerce')

def get_column(df, target, col_map_upper):
    """Get first non-empty column from priority list. Returns (col, source_name) or (None, None)."""
    for src in COLUMN_PRIORITY.get(target, [target]):
        if src.upper() in col_map_upper:
            col = df[col_map_upper[src.upper()]].copy()
            non_empty = col.dropna()
            if len(non_empty) > 0:
                if col.dtype == object:
                    non_whitespace = non_empty[non_empty.astype(str).str.strip() != '']
                    if len(non_whitespace) > 0:
                        return col, src
                else:
                    return col, src
            continue
    return None, None

# ============================================================
# REPORTING CONSTANTS & HELPERS
# ============================================================

HIGH_NULL_THRESHOLD = 50.0   # retention % below this is flagged [LOW]
MONTH_NAMES = {
    1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May',  6:'Jun',
    7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec',
}


def build_column_summary(all_file_reports):
    """Aggregate per-column retention / mapping stats across all processed files."""
    n_files = len(all_file_reports)
    summary = {}
    for col in OUTPUT_SCHEMA:
        unmapped_files  = []
        retention_vals  = []
        sources         = {}
        for rep in all_file_reports:
            info = rep['columns'].get(col, {})
            if info.get('status') == 'UNMAPPED':
                unmapped_files.append(rep['file'])
            else:
                retention_vals.append(info.get('retention_pct', 0.0))
                src = info.get('source_col')
                if src:
                    sources[src] = sources.get(src, 0) + 1
        summary[col] = {
            'files_total'               : n_files,
            'files_unmapped'            : len(unmapped_files),
            'files_mapped'              : n_files - len(unmapped_files),
            'unmapped_pct'              : round(len(unmapped_files) / n_files * 100, 2) if n_files else 0.0,
            'avg_retention_when_mapped' : round(sum(retention_vals) / len(retention_vals), 2) if retention_vals else None,
            'min_retention_when_mapped' : round(min(retention_vals), 2) if retention_vals else None,
            'max_retention_when_mapped' : round(max(retention_vals), 2) if retention_vals else None,
            'source_columns_used'       : sources,
            'unmapped_in_files'         : unmapped_files,
        }
    return summary


def save_coverage_matrix(all_file_reports, output_path):
    """Save column_coverage_matrix.csv — files × columns, values = retention % or 'UNMAPPED'."""
    rows = []
    for rep in all_file_reports:
        row = {
            'file'                 : rep['file'],
            'year'                 : rep['year'],
            'month'                : rep['month'],
            'rows'                 : rep['rows'],
            'source_cols'          : rep['source_columns_count'],
            'mapped_count'         : rep['mapped_count'],
            'unmapped_count'       : rep['unmapped_count'],
            'overall_retention_pct': rep['overall_retention_pct'],
        }
        for col in OUTPUT_SCHEMA:
            info = rep['columns'].get(col, {})
            row[col] = 'UNMAPPED' if info.get('status') == 'UNMAPPED' else info.get('retention_pct', 0.0)
        rows.append(row)
    matrix_df = pd.DataFrame(rows)
    if 'year' in matrix_df.columns and 'month' in matrix_df.columns:
        matrix_df = matrix_df.sort_values(['year', 'month']).reset_index(drop=True)
    matrix_path = output_path / 'column_coverage_matrix.csv'
    matrix_df.to_csv(matrix_path, index=False)
    return matrix_path


# ============================================================
# TRANSLATION FUNCTIONS
# ============================================================

def translate_mstat(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    if year <= 2010: return {1:1, 2:2, 3:4, 4:6, 5:8}.get(code, np.nan)
    elif year <= 2014: return {1:1, 2:2, 3:4, 4:6, 5:8, 6:7}.get(code, np.nan)
    elif year <= 2023: return {1:1, 2:2, 3:4, 4:6, 5:7, 6:8}.get(code, np.nan)
    return code

def translate_grade(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    if year <= 2011:
        if code == 0: return 0
        elif code == 1: return 10012
        elif code == 2: return 10018
        elif code == 3: return 24012
        elif code == 4: return 35011
        elif code == 5: return 54011
        elif 60 <= code <= 68: return 55011
        elif 70 <= code <= 76: return 64011
        return np.nan
    elif year <= 2016:
        if code == 0: return 0
        elif code == 10: return 2000
        elif 210 <= code <= 260: return {210:10011,220:10012,230:10013,240:10014,250:10015,260:10016}.get(code,10012)
        elif code == 270: return 10017
        elif code == 280: return 10018
        elif 310 <= code <= 340: return 24011 + (code-310)//10
        elif code == 350: return 35011
        elif 410 <= code <= 499: return 44011
        elif 510 <= code <= 559: return 54011
        elif 560 <= code <= 599: return 55011
        elif 610 <= code <= 699: return 64011
        return np.nan
    elif year <= 2018:
        if code == 0: return 0
        elif code in [1,2,10]: return 2000
        elif 110 <= code <= 160: return 10011 + (code-110)//10
        elif code in [170,180,191,192]: return 10018
        elif 210 <= code <= 240: return 24011 + (code-210)//10
        elif code == 250: return 24015
        elif 310 <= code <= 320: return 34011
        elif code == 350: return 35011
        elif 410 <= code <= 499: return 44011
        elif 510 <= code <= 559: return 54011
        elif 560 <= code <= 599: return 55011
        elif 610 <= code <= 699: return 64011
        return np.nan
    elif year <= 2022:
        if 0 <= code <= 1000: return 0
        elif code == 2000: return 2000
        elif 10011 <= code <= 64011: return code
        return np.nan
    return code

def translate_pclass(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    return code if code in [0,1,2,3,4,5,6] else np.nan

def translate_natem(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    return code if code in [1,2,3] else np.nan

def translate_pbasis(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    return code if code in [0,1,2,3,4,5,6,7] else np.nan

def translate_wynot(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    if year < 2021:
        if code == 6: return 61
        return code if code in [0,1,2,3,4,5,7,8,9] else np.nan
    return code

def translate_conwr(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    return code if code in [1,2,3,4,5] else np.nan

def translate_yesno(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    return code if code in [1,2] else np.nan

def translate_rel(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    return code if 1 <= code <= 26 else np.nan

TRANSLATION_MAP = {
    'PUFC03_REL': translate_rel, 'PUFC06_MSTAT': translate_mstat, 'PUFC07_GRADE': translate_grade,
    'PUFC10_CONWR': translate_conwr, 'PUFC17_NATEM': translate_natem,
    'PUFC20_PWMORE': translate_yesno, 'PUFC21_PLADDW': translate_yesno,
    'PUFC22_PFWRK': translate_yesno, 'PUFC23_PCLASS': translate_pclass, 'PUFC24_PBASIS': translate_pbasis,
    'PUFC26_OJOB': translate_yesno, 'PUFC30_LOOKW': translate_yesno, 'PUFC31_FLWRK': translate_yesno,
    'PUFC34_WYNOT': translate_wynot, 'PUFC36_AVAIL': translate_yesno, 'PUFC37_WILLING': translate_yesno,
    'PUFC38_PREVJOB': translate_yesno, 'PUFC11_WORK': translate_yesno, 'PUFC12_JOB': translate_yesno,
}

# ============================================================
# FILE PROCESSING
# ============================================================

def extract_year_month(filepath):
    filename = Path(filepath).stem.upper()
    month_map = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    year_match = re.search(r'(20\d{2}|199\d)', filename)
    year = int(year_match.group(1)) if year_match else None
    month = next((m_num for m_name, m_num in month_map.items() if m_name in filename), None)
    return year, month

def read_csv(filepath):
    na_vals = ['', '\t', ' ', '  ', '   ', '.', 'NA', 'nan', 'NaN', 'N/A']
    for enc in ['utf-8', 'latin-1', 'cp1252']:
        try:
            return pd.read_csv(
                filepath,
                encoding=enc,
                low_memory=False,
                na_values=na_vals
            )
        except pd.errors.ParserError:
            try:
                return pd.read_csv(
                    filepath,
                    encoding=enc,
                    na_values=na_vals,
                    engine='python',
                    on_bad_lines='warn'
                )
            except Exception:
                continue
        except Exception:
            continue
    return pd.read_csv(
        filepath,
        encoding='utf-8',
        engine='python',
        on_bad_lines='warn'
    )

def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'replace').decode('ascii'))

def process_file(filepath, log_messages):
    fname   = Path(filepath).name
    t_start = datetime.now()

    def log(msg):
        safe_print(msg)
        log_messages.append(msg)

    W = 72   # log line width
    log(f"\n{'=' * W}")
    log(f"  FILE : {fname}")

    try:
        df = read_csv(filepath)
    except Exception as e:
        log(f"  ERROR reading file: {e}")
        log(f"{'=' * W}")
        return None, None

    year, month   = extract_year_month(filepath)
    col_map_upper = {c.upper(): c for c in df.columns}
    n_rows        = len(df)
    n_src_cols    = len(df.columns)

    if year is None:
        for col in ['SVYYR', 'CYEAR', 'PUFSVYYR']:
            if col.upper() in col_map_upper:
                try:
                    year = int(pd.to_numeric(df[col_map_upper[col.upper()]], errors='coerce').mode().iloc[0])
                except Exception:
                    pass
                if year:
                    break
        if year is None:
            log(f"  WARNING: Could not detect year — defaulting to 2020")
            year = 2020

    month_name = MONTH_NAMES.get(month, '???')
    log(f"  Year : {year}   Month : {month} ({month_name})   Rows : {n_rows:,}   Source cols : {n_src_cols}")
    log(f"  Source columns (first 15): {', '.join(list(df.columns)[:15])}{'...' if n_src_cols > 15 else ''}")

    # ── Map & translate every target column ───────────────────────────
    out_df      = pd.DataFrame(index=df.index)
    col_reports = {}
    unmapped_cols = []

    for target in OUTPUT_SCHEMA:
        col_data, source_used = get_column(df, target, col_map_upper)

        if col_data is not None:
            raw_nonnull     = int(col_data.notna().sum())
            col_data_clean  = clean_column(col_data)
            clean_nonnull   = int(col_data_clean.notna().sum())

            if target in TRANSLATION_MAP:
                translated      = col_data_clean.apply(lambda x: TRANSLATION_MAP[target](x, year))
                out_df[target]  = translated
                final_nonnull   = int(translated.notna().sum())
                translation_loss = clean_nonnull - final_nonnull
                status          = 'MAPPED+TRANSLATED'
                was_translated  = True
            else:
                out_df[target]   = col_data_clean
                final_nonnull    = clean_nonnull
                translation_loss = 0
                status           = 'MAPPED'
                was_translated   = False

            null_count    = n_rows - final_nonnull
            retention_pct = round(final_nonnull / n_rows * 100, 2) if n_rows else 0.0
            null_pct      = round(null_count    / n_rows * 100, 2) if n_rows else 0.0

            col_reports[target] = {
                'status'           : status,
                'source_col'       : source_used,
                'translated'       : was_translated,
                'raw_nonnull_count': raw_nonnull,
                'final_nonnull_count': final_nonnull,
                'null_count'       : null_count,
                'retention_pct'    : retention_pct,
                'null_pct'         : null_pct,
                'translation_loss' : translation_loss,
            }
        else:
            out_df[target] = np.nan
            unmapped_cols.append(target)
            col_reports[target] = {
                'status'              : 'UNMAPPED',
                'source_col'          : None,
                'translated'          : False,
                'raw_nonnull_count'   : 0,
                'final_nonnull_count' : 0,
                'null_count'          : n_rows,
                'retention_pct'       : 0.0,
                'null_pct'            : 100.0,
                'translation_loss'    : 0,
                'candidates_searched' : COLUMN_PRIORITY.get(target, [target]),
            }

    # Lock year/month
    out_df['PUFSVYYR'] = year
    if month:
        out_df['PUFSVYMO'] = month
    out_df = out_df[OUTPUT_SCHEMA]

    # ── Derived totals ─────────────────────────────────────────────────
    mapped_list     = [t for t in OUTPUT_SCHEMA if col_reports[t]['status'] != 'UNMAPPED']
    unmapped_list   = [t for t in OUTPUT_SCHEMA if col_reports[t]['status'] == 'UNMAPPED']
    translated_list = [t for t in OUTPUT_SCHEMA if col_reports[t].get('translated')]

    total_cells       = n_rows * len(OUTPUT_SCHEMA)
    nonnull_cells     = sum(col_reports[t]['final_nonnull_count'] for t in OUTPUT_SCHEMA)
    overall_retention = round(nonnull_cells / total_cells * 100, 2) if total_cells else 0.0

    # ── Column mapping table ───────────────────────────────────────────
    log(f"\n  {'─' * (W - 2)}")
    log(f"  COLUMN MAPPING TABLE  ({len(OUTPUT_SCHEMA)} target columns)")
    log(f"  {'─' * (W - 2)}")
    log(f"  {'TARGET COLUMN':<24} {'STATUS':<22} {'SOURCE COLUMN':<22} {'RETAIN%':>8}  {'NULL%':>7}")
    log(f"  {'─' * (W - 2)}")

    for target in OUTPUT_SCHEMA:
        r    = col_reports[target]
        src  = r['source_col'] or '-'
        ret  = r['retention_pct']
        null_p = r['null_pct']
        st   = r['status']

        if st == 'UNMAPPED':
            badge = '[UNMAPPED]  '
        elif ret >= 80:
            badge = '[OK]        '
        elif ret >= HIGH_NULL_THRESHOLD:
            badge = '[PARTIAL]   '
        else:
            badge = '[LOW]       '

        tl    = r.get('translation_loss', 0)
        extra = f'  *{tl:,} translation loss' if tl > 0 else ''
        log(f"  {target:<24} {badge:<22} {src:<22} {ret:>7.2f}%  {null_p:>6.2f}%{extra}")

    log(f"  {'─' * (W - 2)}")

    # ── File summary block ─────────────────────────────────────────────
    log(f"")
    log(f"  FILE SUMMARY")
    log(f"  {'─' * (W - 2)}")
    log(f"  Rows in source file      : {n_rows:,}")
    log(f"  Source columns           : {n_src_cols}")
    log(f"  Target columns           : {len(OUTPUT_SCHEMA)}")
    log(f"  Mapped                   : {len(mapped_list)}/{len(OUTPUT_SCHEMA)}  ({len(mapped_list)/len(OUTPUT_SCHEMA)*100:.1f}%)")
    log(f"  Unmapped                 : {len(unmapped_list)}/{len(OUTPUT_SCHEMA)}  ({len(unmapped_list)/len(OUTPUT_SCHEMA)*100:.1f}%)")
    log(f"  Translated (code-mapped) : {len(translated_list)}")
    log(f"  Overall retention rate   : {overall_retention:.2f}%  "
        f"({nonnull_cells:,} / {total_cells:,} non-null cells)")

    # Unmapped detail
    if unmapped_list:
        log(f"")
        log(f"  UNMAPPED COLUMNS — not found anywhere in source file ({len(unmapped_list)}):")
        for col in unmapped_list:
            candidates = COLUMN_PRIORITY.get(col, [col])
            log(f"    - {col:<26} searched: {', '.join(candidates)}")

    # Low-retention mapped columns
    low_ret = [(t, col_reports[t]['retention_pct'])
               for t in mapped_list
               if col_reports[t]['retention_pct'] < HIGH_NULL_THRESHOLD]
    low_ret.sort(key=lambda x: x[1])
    if low_ret:
        log(f"")
        log(f"  LOW RETENTION MAPPED COLUMNS  (< {HIGH_NULL_THRESHOLD:.0f}% non-null):")
        for col, ret in low_ret:
            r  = col_reports[col]
            tl = r.get('translation_loss', 0)
            tl_note = f'  [{tl:,} lost in code translation]' if tl > 0 else ''
            log(f"    - {col:<26} {ret:6.2f}% retention  (source: {r['source_col']}){tl_note}")

    # Translation loss detail
    trans_losses = [(t, col_reports[t]['translation_loss'])
                    for t in translated_list
                    if col_reports[t].get('translation_loss', 0) > 0]
    if trans_losses:
        log(f"")
        log(f"  TRANSLATION LOSSES  (valid values that became null after code re-mapping):")
        for col, loss in sorted(trans_losses, key=lambda x: -x[1]):
            pct = round(loss / n_rows * 100, 2)
            log(f"    - {col:<26} {loss:,} rows ({pct:.2f}%) became null")

    elapsed = (datetime.now() - t_start).total_seconds()
    log(f"")
    log(f"  Processed in {elapsed:.2f}s")
    log(f"{'=' * W}")

    file_report = {
        'file'                  : fname,
        'year'                  : year,
        'month'                 : month,
        'rows'                  : n_rows,
        'source_columns_count'  : n_src_cols,
        'mapped_count'          : len(mapped_list),
        'unmapped_count'        : len(unmapped_list),
        'translated_count'      : len(translated_list),
        'mapped_pct'            : round(len(mapped_list) / len(OUTPUT_SCHEMA) * 100, 2),
        'overall_retention_pct' : overall_retention,
        'unmapped_columns'      : unmapped_list,
        'columns'               : col_reports,
    }

    return out_df, file_report

def process_all_batched(input_dir, output_dir, batch_size=10):
    input_path  = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    individual_dir = output_path / 'individual_files'
    individual_dir.mkdir(exist_ok=True)

    # Deduplicate files by lowercase name
    all_files = list(input_path.glob('*.csv')) + list(input_path.glob('*.CSV'))
    seen  = set()
    files = []
    for f in sorted(all_files, key=lambda x: x.name.lower()):
        nlower = f.name.lower()
        if nlower not in seen:
            seen.add(nlower)
            files.append(f)

    log_messages     = []
    all_file_reports = []

    def log(msg):
        safe_print(msg)
        log_messages.append(msg)

    W = 72
    log("=" * W)
    log("LFS HARMONIZER v8 — 47 CORE COLUMNS")
    log(f"Started   : {datetime.now().isoformat()}")
    log(f"Input dir : {input_dir}")
    log(f"Output    : {output_dir}")
    log(f"Files     : {len(files)}")
    log(f"Batch sz  : {batch_size}")
    log("=" * W)

    all_parquet_files = []
    errors     = []
    total_rows = 0

    for i, f in enumerate(files, 1):
        log(f"\n[{i}/{len(files)}]")
        try:
            result = process_file(str(f), log_messages)
            if result[0] is not None:
                df, file_report = result
                out_name = f"{f.stem}_harmonized.parquet"
                out_file = individual_dir / out_name
                df.to_parquet(out_file, index=False, compression='snappy')
                all_parquet_files.append(out_file)
                all_file_reports.append(file_report)
                total_rows += len(df)
                del df
                gc.collect()
        except Exception as e:
            log(f"  FATAL ERROR: {e}")
            errors.append((f.name, str(e)))

    # ── Save per-release JSON report ───────────────────────────────────
    if all_file_reports:
        report_path = output_path / 'per_release_report.json'
        with open(report_path, 'w', encoding='utf-8') as fp:
            json.dump(all_file_reports, fp, indent=2)
        log(f"\nPer-release report : {report_path}")

        matrix_path = save_coverage_matrix(all_file_reports, output_path)
        log(f"Coverage matrix    : {matrix_path}")

        col_summary = build_column_summary(all_file_reports)
        col_summary_path = output_path / 'column_summary.json'
        with open(col_summary_path, 'w', encoding='utf-8') as fp:
            json.dump(col_summary, fp, indent=2)
        log(f"Column summary     : {col_summary_path}")
    else:
        col_summary = {}

    log(f"\n{'=' * W}")
    log("INDIVIDUAL FILES COMPLETE")
    log(f"  Processed : {len(all_parquet_files)} files")
    log(f"  Errors    : {len(errors)} files")
    log(f"  Total rows: {total_rows:,}")
    log("=" * W)

    # ── Cross-file column coverage table ──────────────────────────────
    if all_file_reports:
        n_files = len(all_file_reports)
        log(f"\n{'─' * W}")
        log("CROSS-FILE COLUMN COVERAGE SUMMARY  (sorted by avg retention, worst first)")
        log(f"{'─' * W}")
        log(f"  {'COLUMN':<24} {'UNMAPPED':>10} {'MAPPED':>8} {'AVG RET%':>10} {'MIN RET%':>10} {'MAX RET%':>10}")
        log(f"  {'─' * 70}")

        sorted_cols = sorted(
            OUTPUT_SCHEMA,
            key=lambda c: (col_summary[c]['avg_retention_when_mapped'] or -1)
        )
        for col in sorted_cols:
            s   = col_summary[col]
            avg = f"{s['avg_retention_when_mapped']:.2f}%" if s['avg_retention_when_mapped'] is not None else '    N/A'
            mn  = f"{s['min_retention_when_mapped']:.2f}%" if s['min_retention_when_mapped'] is not None else '    N/A'
            mx  = f"{s['max_retention_when_mapped']:.2f}%" if s['max_retention_when_mapped'] is not None else '    N/A'
            unm = f"{s['files_unmapped']}/{n_files}"
            mp  = f"{s['files_mapped']}/{n_files}"
            log(f"  {col:<24} {unm:>10} {mp:>8} {avg:>10} {mn:>10} {mx:>10}")

        log(f"  {'─' * 70}")

        always_unmapped = [c for c in OUTPUT_SCHEMA if col_summary[c]['files_mapped'] == 0]
        partial_unmapped = [
            c for c in OUTPUT_SCHEMA
            if 0 < col_summary[c]['files_unmapped'] < n_files
        ]

        if always_unmapped:
            log(f"\n  ALWAYS UNMAPPED  (0/{n_files} files had data):")
            for c in always_unmapped:
                log(f"    - {c}")

        if partial_unmapped:
            log(f"\n  PARTIALLY UNMAPPED  (missing in at least one file):")
            for c in sorted(partial_unmapped, key=lambda x: -col_summary[x]['files_unmapped']):
                s = col_summary[c]
                srcs = ', '.join(s['source_columns_used'].keys()) or '—'
                log(f"    - {c:<26} missing in {s['files_unmapped']:>3}/{n_files} files  "
                    f"| when mapped, avg ret: "
                    f"{s['avg_retention_when_mapped']:.2f}%  sources used: {srcs}")

        log(f"\n{'─' * W}")

    # ── Combine individual parquets ────────────────────────────────────
    if all_parquet_files:
        log(f"\nCombining {len(all_parquet_files)} files in batches of {batch_size}...")
        combined_file = output_path / 'lfs_harmonized_2024codes.parquet'

        first_batch = True
        for batch_start in range(0, len(all_parquet_files), batch_size):
            batch_end   = min(batch_start + batch_size, len(all_parquet_files))
            batch_files = all_parquet_files[batch_start:batch_end]
            log(f"  Batch {batch_start // batch_size + 1}: files {batch_start + 1}–{batch_end}")

            batch_dfs     = [pd.read_parquet(fp) for fp in batch_files]
            batch_combined = pd.concat(batch_dfs, ignore_index=True)
            del batch_dfs
            gc.collect()

            if first_batch:
                batch_combined.to_parquet(combined_file, index=False, compression='snappy')
                first_batch = False
            else:
                existing = pd.read_parquet(combined_file)
                combined = pd.concat([existing, batch_combined], ignore_index=True)
                combined.to_parquet(combined_file, index=False, compression='snappy')
                del existing, combined

            del batch_combined
            gc.collect()

        log(f"\nFinal sort by year/month...")
        final_df = pd.read_parquet(combined_file)
        if 'PUFSVYYR' in final_df.columns:
            final_df = final_df.sort_values(['PUFSVYYR', 'PUFSVYMO']).reset_index(drop=True)
        final_df.to_parquet(combined_file, index=False, compression='snappy')

        log(f"\n{'=' * W}")
        log("HARMONIZATION COMPLETE!")
        log("=" * W)
        log(f"  Output file  : {combined_file}")
        log(f"  Total rows   : {len(final_df):,}")
        log(f"  Columns      : {len(final_df.columns)}")
        if 'PUFSVYYR' in final_df.columns:
            yr_min = int(final_df['PUFSVYYR'].min())
            yr_max = int(final_df['PUFSVYYR'].max())
            log(f"  Year range   : {yr_min} – {yr_max}")

        # ── Global null analysis on the fully-combined dataset ─────────
        log(f"\n  FINAL DATASET — NULL ANALYSIS  (all {len(final_df):,} rows combined)")
        log(f"  {'─' * 68}")
        log(f"  {'COLUMN':<24} {'NON-NULL':>12} {'NULL':>12} {'RETENTION%':>12}")
        log(f"  {'─' * 68}")
        for col in OUTPUT_SCHEMA:
            nn  = int(final_df[col].notna().sum())
            nl  = len(final_df) - nn
            ret = nn / len(final_df) * 100 if len(final_df) else 0.0
            flag = '  <-- HIGH NULL' if ret < HIGH_NULL_THRESHOLD else ''
            log(f"  {col:<24} {nn:>12,} {nl:>12,} {ret:>11.2f}%{flag}")
        log(f"  {'─' * 68}")

        meta = {
            'created'           : datetime.now().isoformat(),
            'version'           : 'v8-final',
            'files_processed'   : len(all_parquet_files),
            'files_with_errors' : len(errors),
            'total_rows'        : len(final_df),
            'columns'           : list(final_df.columns),
            'column_count'      : len(final_df.columns),
            'errors'            : errors[:20],
            'column_summary'    : col_summary,
        }
        with open(output_path / 'metadata.json', 'w') as fp:
            json.dump(meta, fp, indent=2)

        del final_df
        gc.collect()

    if errors:
        log(f"\nFILES WITH ERRORS ({len(errors)}):")
        for fname_err, err_msg in errors:
            log(f"  - {fname_err}: {err_msg}")

    log_path = output_path / 'harmonization_log.txt'
    with open(log_path, 'w', encoding='utf-8') as fp:
        fp.write('\n'.join(log_messages))
    safe_print(f"\nLog saved: {log_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LFS Harmonizer v8 - 47 Core Columns')
    parser.add_argument('-i', '--input-dir', required=True, help='Input directory with CSV files')
    parser.add_argument('-o', '--output-dir', required=True, help='Output directory')
    parser.add_argument('-f', '--single-file', help='Process single file only')
    parser.add_argument('-b', '--batch-size', type=int, default=10, help='Batch size (default: 10)')
    args = parser.parse_args()

    safe_print("=" * 72)
    safe_print("LFS COMPLETE HARMONIZER v8")
    safe_print("47 core columns with full 2005-2024 coverage")
    safe_print("=" * 72)

    if args.single_file:
        log_messages = []
        result = process_file(args.single_file, log_messages)
        if result[0] is not None:
            df, file_report = result
            out_dir = Path(args.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            stem     = Path(args.single_file).stem
            out_file = out_dir / f"{stem}_harmonized.parquet"
            df.to_parquet(out_file, index=False)
            safe_print(f"\nParquet saved : {out_file}")
            report_file = out_dir / f"{stem}_column_report.json"
            with open(report_file, 'w', encoding='utf-8') as fp:
                json.dump(file_report, fp, indent=2)
            safe_print(f"Column report : {report_file}")
    else:
        process_all_batched(args.input_dir, args.output_dir, args.batch_size)