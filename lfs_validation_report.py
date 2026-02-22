"""
LFS Pre-Harmonization Validation Report
========================================
Run this BEFORE harmonizing to identify all potential issues across your 97 files.

Usage:
    python lfs_validation_report.py -i ./raw -o ./validation_report

Output:
    - validation_report.txt (human readable)
    - validation_details.json (machine readable)
    - column_inventory.csv (all columns across all files)
"""

import pandas as pd
import numpy as np
import json
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import warnings
import re
warnings.filterwarnings('ignore')

# Our target schema
TARGET_COLUMNS = {
    'PUFREG': ['PUFREG','CREG','REG'],
    'PUFSVYYR': ['PUFSVYYR','SVYYR','CYEAR'],
    'PUFSVYMO': ['PUFSVYMO','SVYMO','CMONTH'],
    'PUFHHNUM': ['PUFHHNUM','HHNUM'],
    'PUFPSU': ['PUFPSU','PSU','PSU_NO','STRATUM'],
    'PUFHHSIZE': ['PUFHHSIZE','HHID'],
    'PUFPWGT': ['PUFPWGT','PUFPWGTFIN','CFWGT','FWGT','PWGT','RFWGT'],
    'PUFC01_LNO': ['PUFC01_LNO','CC101_LNO','CC04_LNOPRV','C101_LNO','C04_LNO','A01_LNO','LNO'],
    'PUFC03_REL': ['PUFC03_REL','CC05_REL','C05_REL','A05_REL'],
    'PUFC04_SEX': ['PUFC04_SEX','CC06_SEX','C06_SEX','A06_SEX'],
    'PUFC05_AGE': ['PUFC05_AGE','CC07_AGE','C07_AGE','A07_AGE'],
    'PUFC06_MSTAT': ['PUFC06_MSTAT','CC08_MSTAT','CC08_MS','C08_MSTAT','C08_MS'],
    'PUFC07_GRADE': ['PUFC07_GRADE','CC09_GRADE','C09_GRADE','C09_GRD','J12C09_GRADE'],
    'PUFC08_CURSCH': ['PUFC08_CURSCH','A02_CURSCH','A02_CSCH','CURSCH'],
    'PUFC09_GRADTECH': ['PUFC09_GRADTECH','J12C11_GRADTECH','J12C11COURSE','C11_GRADTECH'],
    'PUFC10_CONWR': ['PUFC10_CONWR','CC10_CONWR','C10_CONWR','C10_CNWR'],
    'PUFC11_WORK': ['PUFC11_WORK','CC13_WORK','CC01_WORK','C13_WORK','C11_WORK','B01_WORK','A04_WORK'],
    'PUFC12_JOB': ['PUFC12_JOB','CC14_JOB','CC02_JOB','C14_JOB','B02_JOB','A04_JOB'],
    'PUFNEWEMPSTAT': ['PUFNEWEMPSTAT','NEWEMPSTAT','ANSOEMPSTAT','CEMPST1','CEMPST2'],
    'PUFC14_PROCC': ['PUFC14_PROCC','CC16_PROCC','C16_PROCC','CC12_USOCC','J01_USOCC','B04_OCC','A04_OCC'],
    'PUFC16_PKB': ['PUFC16_PKB','CC18_PKB','C18_PKB','B06_IND','A06_IND'],
    'PUFC17_NATEM': ['PUFC17_NATEM','CC20_NATEM','C20_NATEM'],
    'PUFC18_PNWHRS': ['PUFC18_PNWHRS','CC21_PNWHRS','C21_PNWHRS','A04_NWHRS'],
    'PUFC19_PHOURS': ['PUFC19_PHOURS','CC22_PHOURS','C22_PHOURS'],
    'PUFC20_PWMORE': ['PUFC20_PWMORE','CC23_PWMORE','C23_PWMORE'],
    'PUFC21_PLADDW': ['PUFC21_PLADDW','CC24_PLADDW','C24_PLADDW'],
    'PUFC22_PFWRK': ['PUFC22_PFWRK','CC25_PFWRK','C25_PFWRK'],
    'PUFC23_PCLASS': ['PUFC23_PCLASS','CC19_PCLASS','C19_PCLASS'],
    'PUFC24_PBASIS': ['PUFC24_PBASIS','CC26_PBASIS','C26_PBASIS'],
    'PUFC25_PBASIC': ['PUFC25_PBASIC','CC27_PBASIC','C27_PBASIC'],
    'PUFC26_OJOB': ['PUFC26_OJOB','CC28_OJOB','C28_OJOB'],
    'PUFC27_NJOBS': ['PUFC27_NJOBS','CC27_NJOBS','A03_JOBS','NJOBS'],
    'PUFC28_THOURS': ['PUFC28_THOURS','CC28_THOURS','A04_THOURS','THOURS'],
    'PUFC29_WWM48H': ['PUFC29_WWM48H','CC29_WWM48H','A05_RWM48H','RWM48H'],
    'PUFC30_LOOKW': ['PUFC30_LOOKW','CC38_LOOKW','CC30_LOOKW','C38_LOOKW','B08_LOOKW','A06_LOOKW'],
    'PUFC31_FLWRK': ['PUFC31_FLWRK','CC41_FLWRK','C41_FLWRK'],
    'PUFC32_JOBSM': ['PUFC32_JOBSM','CC39_JOBSM','CC32_JOBSM','C39_JOBSM','JOBSM'],
    'PUFC33_WEEKS': ['PUFC33_WEEKS','CC40_WEEKS','CC33_WEEKS','C40_WEEKS','WEEKS'],
    'PUFC34_WYNOT': ['PUFC34_WYNOT','CC42_WYNOT','C42_WYNOT'],
    'PUFC35_LTLOOKW': ['PUFC35_LTLOOKW','CC35_LTLOOKW','A06_LTLOOKW','A06_LLKW'],
    'PUFC36_AVAIL': ['PUFC36_AVAIL','CC37_AVAIL','CC36_AVAIL','C37_AVAIL','A07_AVAIL'],
    'PUFC37_WILLING': ['PUFC37_WILLING','A07_WILLING','A07_WLNG','WILLING'],
    'PUFC38_PREVJOB': ['PUFC38_PREVJOB','CC43_LBEF','C43_LBEF'],
    'PUFC41_POCC': ['PUFC41_POCC','CC45_POCC','C45_POCC','A10_POCC'],
    'PUFC43_QKB': ['PUFC43_QKB','PQKB','QKB','A09_PQKB'],
    'PUFURB2015': ['PUFURB2015','PUFURB2K10','URB2K1970','URB2K70'],
}

def extract_year_month(filepath):
    filename = Path(filepath).stem.upper()
    month_map = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    year_match = re.search(r'(20\d{2}|199\d)', filename)
    year = int(year_match.group(1)) if year_match else None
    month = next((m_num for m_name, m_num in month_map.items() if m_name in filename), None)
    return year, month

def read_csv_sample(filepath, nrows=1000):
    """Read sample of CSV for quick analysis."""
    for enc in ['utf-8','latin-1','cp1252']:
        try:
            return pd.read_csv(filepath, encoding=enc, low_memory=False, nrows=nrows,
                             na_values=['','\t',' ','  ','   ','.','NA','nan','NaN','N/A'])
        except:
            continue
    return pd.read_csv(filepath, encoding='utf-8', errors='ignore', low_memory=False, nrows=nrows)

def analyze_file(filepath):
    """Analyze a single file for potential issues."""
    fname = Path(filepath).name
    year, month = extract_year_month(filepath)
    
    results = {
        'filename': fname,
        'year': year,
        'month': month,
        'columns': [],
        'issues': [],
        'mapping_info': {},
    }
    
    try:
        # Read full file for row count, sample for analysis
        df_sample = read_csv_sample(filepath, nrows=5000)
        df_count = pd.read_csv(filepath, usecols=[0], encoding='latin-1', low_memory=False)
        total_rows = len(df_count)
        
        results['total_rows'] = total_rows
        results['columns'] = list(df_sample.columns)
        
        col_upper = {c.upper(): c for c in df_sample.columns}
        
        # Check each target variable
        for target, sources in TARGET_COLUMNS.items():
            found_sources = []
            for src in sources:
                if src.upper() in col_upper:
                    actual_col = col_upper[src.upper()]
                    col_data = df_sample[actual_col]
                    
                    # Check if column is empty/placeholder
                    non_null = col_data.notna().sum()
                    non_empty = (col_data.astype(str).str.strip() != '').sum() if col_data.dtype == object else non_null
                    
                    found_sources.append({
                        'source_name': actual_col,
                        'non_null_count': int(non_null),
                        'non_empty_count': int(non_empty),
                        'sample_size': len(df_sample),
                        'pct_filled': round(non_empty / len(df_sample) * 100, 1),
                        'is_empty_placeholder': non_empty < 10,  # Less than 10 values = placeholder
                    })
            
            results['mapping_info'][target] = {
                'found_sources': found_sources,
                'will_use': found_sources[0]['source_name'] if found_sources else None,
                'multiple_sources': len(found_sources) > 1,
            }
            
            # Flag issues
            if not found_sources:
                results['issues'].append(f"MISSING: {target} - no matching column found")
            elif len(found_sources) > 1:
                # Check if first source is empty but later ones have data
                if found_sources[0]['is_empty_placeholder'] and not found_sources[1]['is_empty_placeholder']:
                    results['issues'].append(
                        f"PLACEHOLDER: {target} - '{found_sources[0]['source_name']}' is empty, "
                        f"but '{found_sources[1]['source_name']}' has data ({found_sources[1]['pct_filled']}% filled)"
                    )
                elif found_sources[0]['pct_filled'] < found_sources[1]['pct_filled'] - 20:
                    results['issues'].append(
                        f"DATA_QUALITY: {target} - '{found_sources[0]['source_name']}' has less data "
                        f"({found_sources[0]['pct_filled']}%) than '{found_sources[1]['source_name']}' ({found_sources[1]['pct_filled']}%)"
                    )
        
        # Check for unmapped columns (might be important)
        all_mapped = set()
        for sources in TARGET_COLUMNS.values():
            all_mapped.update(s.upper() for s in sources)
        
        unmapped = [c for c in df_sample.columns if c.upper() not in all_mapped]
        important_unmapped = [c for c in unmapped if any(kw in c.upper() for kw in 
            ['OCC','JOB','WORK','EMP','IND','PKB','GRADE','AGE','SEX','REL','MSTAT','WAGE','PAY','HOUR'])]
        
        if important_unmapped:
            results['issues'].append(f"UNMAPPED_IMPORTANT: {', '.join(important_unmapped[:10])}")
        
        results['unmapped_columns'] = unmapped
        
    except Exception as e:
        results['issues'].append(f"READ_ERROR: {str(e)}")
    
    return results

def generate_report(input_dir, output_dir):
    """Generate comprehensive validation report."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find files
    files = sorted(set(list(input_path.glob('*.csv')) + list(input_path.glob('*.CSV'))))
    
    print(f"Analyzing {len(files)} files...")
    print("=" * 60)
    
    all_results = []
    column_inventory = defaultdict(list)  # column -> list of files
    
    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}] {f.name}...", end=" ")
        result = analyze_file(str(f))
        all_results.append(result)
        
        # Track column inventory
        for col in result.get('columns', []):
            column_inventory[col].append(f.name)
        
        issue_count = len(result.get('issues', []))
        if issue_count > 0:
            print(f"{issue_count} issues")
        else:
            print("OK")
    
    # Generate summary report
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("LFS PRE-HARMONIZATION VALIDATION REPORT")
    report_lines.append(f"Generated: {datetime.now().isoformat()}")
    report_lines.append(f"Files analyzed: {len(files)}")
    report_lines.append("=" * 80)
    
    # Summary statistics
    total_rows = sum(r.get('total_rows', 0) for r in all_results)
    files_with_issues = sum(1 for r in all_results if r.get('issues'))
    
    report_lines.append(f"\nSUMMARY:")
    report_lines.append(f"  Total rows across all files: {total_rows:,}")
    report_lines.append(f"  Files with issues: {files_with_issues}/{len(files)}")
    
    # Issue breakdown
    report_lines.append(f"\n{'=' * 80}")
    report_lines.append("ISSUE BREAKDOWN BY TYPE")
    report_lines.append("=" * 80)
    
    issue_types = defaultdict(list)
    for r in all_results:
        for issue in r.get('issues', []):
            issue_type = issue.split(':')[0]
            issue_types[issue_type].append((r['filename'], issue))
    
    for issue_type, occurrences in sorted(issue_types.items()):
        report_lines.append(f"\n{issue_type}: {len(occurrences)} occurrences")
        report_lines.append("-" * 40)
        for fname, issue in occurrences[:10]:
            report_lines.append(f"  {fname}: {issue}")
        if len(occurrences) > 10:
            report_lines.append(f"  ... and {len(occurrences) - 10} more")
    
    # Column mapping analysis
    report_lines.append(f"\n{'=' * 80}")
    report_lines.append("COLUMN MAPPING ANALYSIS")
    report_lines.append("=" * 80)
    
    for target in TARGET_COLUMNS.keys():
        sources_used = defaultdict(int)
        missing_count = 0
        placeholder_count = 0
        
        for r in all_results:
            mapping = r.get('mapping_info', {}).get(target, {})
            if mapping.get('will_use'):
                sources_used[mapping['will_use']] += 1
            else:
                missing_count += 1
            
            found = mapping.get('found_sources', [])
            if found and found[0].get('is_empty_placeholder'):
                placeholder_count += 1
        
        report_lines.append(f"\n{target}:")
        for src, count in sorted(sources_used.items(), key=lambda x: -x[1]):
            report_lines.append(f"  {src}: {count} files")
        if missing_count:
            report_lines.append(f"  MISSING: {missing_count} files")
        if placeholder_count:
            report_lines.append(f"  PLACEHOLDER (empty): {placeholder_count} files")
    
    # Potential problems summary
    report_lines.append(f"\n{'=' * 80}")
    report_lines.append("POTENTIAL PROBLEMS TO ADDRESS")
    report_lines.append("=" * 80)
    
    problems = []
    
    # Check for placeholder issues
    placeholder_issues = [i for i in issue_types.get('PLACEHOLDER', [])]
    if placeholder_issues:
        problems.append(f"1. PLACEHOLDER COLUMNS: {len(placeholder_issues)} files have empty placeholder columns")
        problems.append("   ACTION: Consider modifying priority order or adding smart fallback logic")
    
    # Check for missing critical columns
    missing_critical = []
    for r in all_results:
        for target in ['PUFC04_SEX', 'PUFC05_AGE', 'PUFC07_GRADE', 'PUFC14_PROCC', 'PUFPWGT']:
            mapping = r.get('mapping_info', {}).get(target, {})
            if not mapping.get('will_use'):
                missing_critical.append((r['filename'], target))
    
    if missing_critical:
        problems.append(f"2. MISSING CRITICAL COLUMNS: {len(missing_critical)} file-column pairs")
        problems.append("   ACTION: Review these files manually")
    
    # Check for year detection issues
    year_issues = [r for r in all_results if not r.get('year')]
    if year_issues:
        problems.append(f"3. YEAR DETECTION: {len(year_issues)} files could not detect year")
        problems.append("   ACTION: Check filename format or add manual year mapping")
    
    for p in problems:
        report_lines.append(p)
    
    if not problems:
        report_lines.append("No critical problems detected!")
    
    # Write report
    report_path = output_path / 'validation_report.txt'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    # Write detailed JSON
    json_path = output_path / 'validation_details.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    # Write column inventory
    inventory_data = []
    for col, files_list in sorted(column_inventory.items()):
        inventory_data.append({
            'column': col,
            'file_count': len(files_list),
            'files': ', '.join(files_list[:5]) + ('...' if len(files_list) > 5 else '')
        })
    
    inventory_df = pd.DataFrame(inventory_data)
    inventory_path = output_path / 'column_inventory.csv'
    inventory_df.to_csv(inventory_path, index=False)
    
    print(f"\n{'=' * 60}")
    print("VALIDATION COMPLETE")
    print(f"{'=' * 60}")
    print(f"Report: {report_path}")
    print(f"Details: {json_path}")
    print(f"Column Inventory: {inventory_path}")
    print(f"\nFiles with issues: {files_with_issues}/{len(files)}")
    
    return all_results

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LFS Pre-Harmonization Validation')
    parser.add_argument('-i', '--input-dir', required=True, help='Input directory with CSV files')
    parser.add_argument('-o', '--output-dir', required=True, help='Output directory for reports')
    args = parser.parse_args()
    
    generate_report(args.input_dir, args.output_dir)