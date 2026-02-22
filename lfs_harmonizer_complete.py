"""
LFS Complete Harmonization Processor v7
========================================
Complete mappings including 2021-2024 monthly file variable renumbering.

Key changes from v5/v6:
- Added all 2021-2024 PUFC** variants (PSA renumbered variables)
- Added PUFC09_WORK, PUFC10_JOB, PUFC13_PROCC, etc.
- Complete coverage for all 105 LFS releases

Usage:
    python lfs_harmonizer_complete_v7.py -i ./raw -o ./output --batch-size 5
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

# Fix Windows console encoding
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
    
    # ========== CLASSIFICATION ==========
    'PUFURB2015': ['PUFURB2015', 'PUFURB2K10', 'URB2K1970', 'URB2K70'],
}

# Output schema
OUTPUT_SCHEMA = [
    'PUFREG', 'PUFSVYYR', 'PUFSVYMO', 'PUFHHNUM', 'PUFPSU', 'PUFHHSIZE', 'PUFRPL', 'PUFPWGTPRV',
    'PUFC01_LNO', 'PUFC03_REL', 'PUFC04_SEX', 'PUFC05_AGE', 'PUFC06_MSTAT',
    'PUFC07_GRADE', 'PUFC08_CURSCH', 'PUFC09_GRADTECH',
    'PUFC10_CONWR', 'PUFC11_WORK', 'PUFC12_JOB', 'PUFNEWEMPSTAT',
    'PUFC14_PROCC', 'PUFC16_PKB', 'PUFC17_NATEM', 'PUFC18_PNWHRS', 'PUFC19_PHOURS',
    'PUFC20_PWMORE', 'PUFC21_PLADDW', 'PUFC22_PFWRK', 'PUFC23_PCLASS',
    'PUFC24_PBASIS', 'PUFC25_PBASIC', 'PUFC26_OJOB',
    'PUFC27_NJOBS', 'PUFC28_THOURS', 'PUFC29_WWM48H',
    'PUFC30_LOOKW', 'PUFC31_FLWRK', 'PUFC32_JOBSM', 'PUFC33_WEEKS',
    'PUFC34_WYNOT', 'PUFC35_LTLOOKW', 'PUFC36_AVAIL', 'PUFC37_WILLING',
    'PUFC38_PREVJOB', 'PUFC39_YEAR', 'PUFC39_MONTH', 'PUFC41_POCC', 'PUFC43_QKB',
    'PUFURB2015',
    'PUFC14_PROCC_1DIG', 'PUFC16_PKB_1DIG', 'PUFC07_GRADE_HARM', 'PUFC06_MSTAT_HARM',
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
    """Get first non-empty column from priority list."""
    for src in COLUMN_PRIORITY.get(target, [target]):
        if src.upper() in col_map_upper:
            col = df[col_map_upper[src.upper()]].copy()
            non_empty = col.dropna()
            if len(non_empty) > 0:
                if col.dtype == object:
                    non_whitespace = non_empty[non_empty.astype(str).str.strip() != '']
                    if len(non_whitespace) > 0:
                        return col
                else:
                    return col
            continue
    return None

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

def harm_occ_1dig(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    if year < 2012:
        m = {1:0,11:1,12:1,13:1,14:1,21:2,22:2,23:2,24:2,31:3,32:3,33:3,34:3,41:4,42:4,51:5,52:5,61:6,62:6,63:6,64:6,65:6,71:7,72:7,73:7,74:7,81:8,82:8,83:8,91:9,92:9,93:9}
        return m.get(code, np.nan)
    return code//1000 if code>=1000 else (code//100 if code>=100 else code//10 if code>=10 else code)

def harm_ind_1dig(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    if year < 2012:
        m = {1:1,2:1,3:1,4:1,5:1,6:1,10:2,11:2,15:3,16:3,17:3,18:3,19:3,20:3,21:3,22:3,23:3,24:3,25:3,26:3,27:3,28:3,29:3,30:3,31:3,32:3,33:3,34:3,35:3,36:3,37:3,40:4,41:4,45:5,50:6,51:6,52:6,55:7,60:8,61:8,62:8,63:8,64:8,65:9,66:9,67:9,70:10,71:10,72:10,73:10,74:10,75:11,80:12,85:13,90:14,91:14,92:14,93:14,95:15,99:16}
        return m.get(code, np.nan)
    d = code//100 if code>=100 else code
    if d in [1,2,3]: return 1
    if d in [5,6,7,8,9]: return 2
    if 10<=d<=33: return 3
    if d==35: return 4
    if d in [36,37,38,39]: return 5
    if d in [41,42,43]: return 6
    if d in [45,46,47]: return 7
    if d in [49,50,51,52,53]: return 8
    if d in [55,56]: return 9
    if d in [58,59,60,61,62,63]: return 10
    if d in [64,65,66]: return 11
    if d==68: return 12
    if d in [69,70,71,72,73,74,75]: return 13
    if d in [77,78,79,80,81,82]: return 14
    if d==84: return 15
    if d==85: return 16
    if d in [86,87,88]: return 17
    if d in [90,91,92,93]: return 18
    if d in [94,95,96]: return 19
    if d==97: return 20
    if d==99: return 21
    return np.nan

def harm_edu_summary(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    if year <= 2011:
        if code==0: return 0
        if code==1: return 2
        if code==2: return 3
        if code==3: return 4
        if code==4: return 5
        if code==5: return 7
        if 60<=code<=68: return 8
        if 70<=code<=76: return 9
        return np.nan
    elif year <= 2016:
        if code==0: return 0
        if code==10: return 1
        if 210<=code<=260: return 2
        if code in [270,280]: return 3
        if 310<=code<=340: return 4
        if code==350: return 5
        if 410<=code<=499: return 6
        if 510<=code<=559: return 7
        if 560<=code<=599: return 8
        if 610<=code<=699: return 9
        return np.nan
    else:
        if code==0 or (0<=code<=1000): return 0
        if code==2000 or code in [1,2,10]: return 1
        if 10011<=code<=10015 or (110<=code<=160): return 2
        if 10016<=code<=10018 or code in [170,180,191,192]: return 3
        if 24011<=code<=24013 or (210<=code<=240): return 4
        if 24014<=code<=24015 or code==250 or code==35011 or code==350: return 5
        if code==34011 or (310<=code<=349) or (44011<=code<=44012) or (410<=code<=499): return 6
        if code==54011 or (510<=code<=559): return 7
        if code==55011 or (560<=code<=599): return 8
        if code==64011 or (610<=code<=699): return 9
        return np.nan

def harm_mstat_summary(code, year):
    code = safe_int(code)
    if pd.isna(code): return np.nan
    if year <= 2010: return {1:1,2:2,3:3,4:4,5:6}.get(code, np.nan)
    elif year <= 2014: return {1:1,2:2,3:3,4:4,5:6,6:5}.get(code, np.nan)
    elif year <= 2023: return {1:1,2:2,3:3,4:4,5:5,6:6}.get(code, np.nan)
    return {1:1,2:2,3:2,4:3,5:4,6:4,7:5,8:6}.get(code, np.nan)

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
                    low_memory=False,
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
        errors='ignore',
        low_memory=False,
        engine='python',
        on_bad_lines='warn'
    )

def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'replace').decode('ascii'))

def process_file(filepath, log_messages):
    fname = Path(filepath).name
    
    def log(msg):
        safe_print(msg)
        log_messages.append(msg)
    
    log(f"Processing: {fname}")
    
    try:
        df = read_csv(filepath)
    except Exception as e:
        log(f"  ERROR reading file: {e}")
        return None
    
    year, month = extract_year_month(filepath)
    col_map_upper = {c.upper(): c for c in df.columns}
    
    if year is None:
        for col in ['SVYYR','CYEAR','PUFSVYYR']:
            if col.upper() in col_map_upper:
                try:
                    year = int(pd.to_numeric(df[col_map_upper[col.upper()]], errors='coerce').mode().iloc[0])
                except: pass
                if year: break
        if year is None:
            log(f"  WARNING: Could not detect year, defaulting to 2020")
            year = 2020
    
    log(f"  Year: {year}, Month: {month}, Rows: {len(df):,}, Cols: {len(df.columns)}")
    
    out_df = pd.DataFrame(index=df.index)
    missing_cols = []
    
    for target in OUTPUT_SCHEMA:
        if target.endswith('_1DIG') or target.endswith('_HARM'):
            continue
        
        col_data = get_column(df, target, col_map_upper)
        if col_data is not None:
            col_data = clean_column(col_data)
            if target in TRANSLATION_MAP:
                out_df[target] = col_data.apply(lambda x: TRANSLATION_MAP[target](x, year))
            else:
                out_df[target] = col_data
        else:
            missing_cols.append(target)
    
    if missing_cols:
        log(f"  Missing columns: {', '.join(missing_cols[:5])}{'...' if len(missing_cols)>5 else ''}")
    
    if 'PUFSVYYR' not in out_df.columns:
        out_df['PUFSVYYR'] = year
    if 'PUFSVYMO' not in out_df.columns and month:
        out_df['PUFSVYMO'] = month
    
    for target, orig_target, harm_func in [
        ('PUFC14_PROCC_1DIG', 'PUFC14_PROCC', harm_occ_1dig),
        ('PUFC16_PKB_1DIG', 'PUFC16_PKB', harm_ind_1dig),
        ('PUFC07_GRADE_HARM', 'PUFC07_GRADE', harm_edu_summary),
        ('PUFC06_MSTAT_HARM', 'PUFC06_MSTAT', harm_mstat_summary),
    ]:
        orig_col = get_column(df, orig_target, col_map_upper)
        if orig_col is not None:
            orig_col = clean_column(orig_col)
            out_df[target] = orig_col.apply(lambda x: harm_func(x, year))
    
    final_cols = [c for c in OUTPUT_SCHEMA if c in out_df.columns]
    out_df = out_df[final_cols]
    
    log(f"  Output: {len(out_df.columns)} columns, {len(out_df):,} rows [OK]")
    return out_df

def process_all_batched(input_dir, output_dir, batch_size=10):
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    individual_dir = output_path / 'individual_files'
    individual_dir.mkdir(exist_ok=True)
    
    # Deduplicate files by lowercase name
    all_files = list(input_path.glob('*.csv')) + list(input_path.glob('*.CSV'))
    seen = set()
    files = []
    for f in sorted(all_files, key=lambda x: x.name.lower()):
        name_lower = f.name.lower()
        if name_lower not in seen:
            seen.add(name_lower)
            files.append(f)
    
    log_messages = []
    
    def log(msg):
        safe_print(msg)
        log_messages.append(msg)
    
    log("=" * 60)
    log("LFS HARMONIZER v7 - COMPLETE MAPPINGS")
    log(f"Started: {datetime.now().isoformat()}")
    log(f"Files found: {len(files)}")
    log(f"Batch size: {batch_size}")
    log("=" * 60)
    
    all_parquet_files = []
    errors = []
    total_rows = 0
    
    for i, f in enumerate(files, 1):
        log(f"\n[{i}/{len(files)}]")
        
        try:
            df = process_file(str(f), log_messages)
            if df is not None:
                out_name = f"{f.stem}_harmonized.parquet"
                out_file = individual_dir / out_name
                df.to_parquet(out_file, index=False, compression='snappy')
                all_parquet_files.append(out_file)
                total_rows += len(df)
                del df
                gc.collect()
        except Exception as e:
            log(f"  FATAL ERROR: {e}")
            errors.append((f.name, str(e)))
    
    log(f"\n{'=' * 60}")
    log("INDIVIDUAL FILES COMPLETE")
    log(f"  Processed: {len(all_parquet_files)} files")
    log(f"  Errors: {len(errors)} files")
    log(f"  Total rows: {total_rows:,}")
    log("=" * 60)
    
    if all_parquet_files:
        log(f"\nCombining files in batches of {batch_size}...")
        combined_file = output_path / 'lfs_harmonized_2024codes.parquet'
        
        first_batch = True
        for batch_start in range(0, len(all_parquet_files), batch_size):
            batch_end = min(batch_start + batch_size, len(all_parquet_files))
            batch_files = all_parquet_files[batch_start:batch_end]
            
            log(f"  Batch {batch_start//batch_size + 1}: files {batch_start+1}-{batch_end}")
            
            batch_dfs = [pd.read_parquet(f) for f in batch_files]
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
        
        log(f"\nFinal sorting by year/month...")
        final_df = pd.read_parquet(combined_file)
        if 'PUFSVYYR' in final_df.columns:
            final_df = final_df.sort_values(['PUFSVYYR', 'PUFSVYMO']).reset_index(drop=True)
        final_df.to_parquet(combined_file, index=False, compression='snappy')
        
        log(f"\n{'=' * 60}")
        log("HARMONIZATION COMPLETE!")
        log("=" * 60)
        log(f"Output: {combined_file}")
        log(f"Total rows: {len(final_df):,}")
        log(f"Columns: {len(final_df.columns)}")
        if 'PUFSVYYR' in final_df.columns:
            log(f"Years: {int(final_df['PUFSVYYR'].min())} - {int(final_df['PUFSVYYR'].max())}")
        
        meta = {
            'created': datetime.now().isoformat(),
            'version': 'v7-complete',
            'files_processed': len(all_parquet_files),
            'files_with_errors': len(errors),
            'total_rows': len(final_df),
            'columns': list(final_df.columns),
            'errors': errors[:20],
        }
        with open(output_path / 'metadata.json', 'w') as f:
            json.dump(meta, f, indent=2)
        
        del final_df
        gc.collect()
    
    log_path = output_path / 'harmonization_log.txt'
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(log_messages))
    safe_print(f"\nLog saved to: {log_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LFS Harmonizer v7 - Complete Mappings')
    parser.add_argument('-i', '--input-dir', required=True, help='Input directory with CSV files')
    parser.add_argument('-o', '--output-dir', required=True, help='Output directory')
    parser.add_argument('-f', '--single-file', help='Process single file only')
    parser.add_argument('-b', '--batch-size', type=int, default=10, help='Batch size (default: 10)')
    args = parser.parse_args()
    
    safe_print("=" * 60)
    safe_print("LFS COMPLETE HARMONIZER v7")
    safe_print("With 2021-2024 monthly file variable mappings")
    safe_print("=" * 60)
    
    if args.single_file:
        log_messages = []
        df = process_file(args.single_file, log_messages)
        if df is not None:
            Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            out_file = Path(args.output_dir) / f"{Path(args.single_file).stem}_harmonized.parquet"
            df.to_parquet(out_file, index=False)
            safe_print(f"\nSaved: {out_file}")
    else:
        process_all_batched(args.input_dir, args.output_dir, args.batch_size)