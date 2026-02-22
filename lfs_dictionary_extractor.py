"""
LFS Dictionary Extractor
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
import os
warnings.filterwarnings('ignore')

def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'replace').decode('ascii'))

def extract_year_month(filepath):
    """Extract year and month from filename."""
    filename = Path(filepath).stem.upper()
    month_map = {'JAN':1,'JANUARY':1,'FEB':2,'FEBRUARY':2,'MAR':3,'MARCH':3,
                 'APR':4,'APRIL':4,'MAY':5,'JUN':6,'JUNE':6,
                 'JUL':7,'JULY':7,'AUG':8,'AUGUST':8,'SEP':9,'SEPT':9,'SEPTEMBER':9,
                 'OCT':10,'OCTOBER':10,'NOV':11,'NOVEMBER':11,'DEC':12,'DECEMBER':12}
    
    year_match = re.search(r'(20\d{2}|199\d)', filename)
    year = int(year_match.group(1)) if year_match else None
    month = None
    for m_name, m_num in month_map.items():
        if m_name in filename:
            month = m_num
            break
    
    return year, month

def parse_dictionary_sheet(df):
    """Parse the dictionary sheet (Sheet 1) to extract variable names and descriptions."""
    variables = []
    
    # The structure has variable names in column index 4 and descriptions in column index 5
    for idx, row in df.iterrows():
        # Check column 4 for variable name
        var_name = row.iloc[4] if len(row) > 4 else None
        var_desc = row.iloc[5] if len(row) > 5 else None
        
        # Skip if no variable name or if it's a filler/NaN
        if pd.isna(var_name) or var_name is None:
            continue
        
        var_name = str(var_name).strip()
        
        # Skip fillers and other non-variable entries
        if 'FILLER' in var_name.upper() or var_name.startswith('_'):
            continue
        
        # Skip if it looks like a header or section name
        if var_name in ['NaN', ''] or len(var_name) > 50:
            continue
            
        var_desc = str(var_desc).strip() if pd.notna(var_desc) else ''
        
        variables.append({
            'name': var_name,
            'description': var_desc
        })
    
    return variables

def parse_valueset_sheet(df):
    """Parse the valueset sheet (Sheet 2) to extract value codes."""
    valuesets = {}
    current_var = None
    current_values = []
    
    for idx, row in df.iterrows():
        # Check if this is a new variable (column 0 has the valueset name like CREG_VS1)
        vs_name = row.iloc[0] if len(row) > 0 else None
        
        if pd.notna(vs_name) and '_VS' in str(vs_name):
            # Save previous variable's values
            if current_var and current_values:
                valuesets[current_var] = current_values
            
            # Extract variable name from valueset name (e.g., CREG_VS1 -> CREG)
            current_var = str(vs_name).split('_VS')[0]
            current_values = []
            continue
        
        # Check if this is a value entry (column 2 has label, column 3 has code)
        label = row.iloc[2] if len(row) > 2 else None
        code = row.iloc[3] if len(row) > 3 else None
        
        if pd.notna(label) and pd.notna(code):
            try:
                code_val = int(float(code)) if str(code).replace('.','').replace('-','').isdigit() else code
                current_values.append({
                    'code': code_val,
                    'label': str(label).strip()
                })
            except:
                pass
    
    # Don't forget the last variable
    if current_var and current_values:
        valuesets[current_var] = current_values
    
    return valuesets

def analyze_dictionary_file(filepath):
    """Analyze a single dictionary Excel file."""
    result = {
        'filename': Path(filepath).name,
        'variables': [],
        'valuesets': {},
        'error': None
    }
    
    year, month = extract_year_month(filepath)
    result['year'] = year
    result['month'] = month
    
    try:
        xlsx = pd.ExcelFile(filepath)
        
        # Parse dictionary sheet (first sheet)
        if len(xlsx.sheet_names) >= 1:
            df_dict = pd.read_excel(xlsx, sheet_name=0, header=None)
            result['variables'] = parse_dictionary_sheet(df_dict)
        
        # Parse valueset sheet (second sheet)
        if len(xlsx.sheet_names) >= 2:
            df_vs = pd.read_excel(xlsx, sheet_name=1, header=None)
            result['valuesets'] = parse_valueset_sheet(df_vs)
            
    except Exception as e:
        result['error'] = str(e)
    
    return result

def analyze_all_dictionaries(input_dir, output_dir):
    """Analyze all dictionary files and generate comprehensive report."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all Excel files (deduplicate by lowercase name)
    all_files = (
        list(input_path.glob('*.xlsx')) + 
        list(input_path.glob('*.xls')) +
        list(input_path.glob('*.XLSX')) +
        list(input_path.glob('*.XLS'))
    )
    
    # Deduplicate by lowercase filename
    seen = set()
    files = []
    for f in all_files:
        name_lower = f.name.lower()
        if name_lower not in seen:
            seen.add(name_lower)
            files.append(f)
    
    files = sorted(files, key=lambda x: x.name.lower())
    
    safe_print(f"Found {len(files)} dictionary files")
    safe_print("=" * 60)
    
    # Data structures
    all_results = []
    var_by_year = defaultdict(lambda: defaultdict(dict))  # var -> year -> {desc, values}
    var_years = defaultdict(set)  # var -> set of years
    year_vars = defaultdict(set)  # year -> set of vars
    var_descriptions = defaultdict(set)  # var -> set of descriptions (to track changes)
    
    for i, f in enumerate(files, 1):
        safe_print(f"[{i}/{len(files)}] Processing: {f.name}")
        
        result = analyze_dictionary_file(str(f))
        all_results.append(result)
        
        year = result.get('year')
        if year is None:
            safe_print(f"  WARNING: Could not detect year")
            continue
        
        if result.get('error'):
            safe_print(f"  ERROR: {result['error']}")
            continue
        
        safe_print(f"  Year: {year}, Variables: {len(result['variables'])}, Valuesets: {len(result['valuesets'])}")
        
        for var in result['variables']:
            var_name = var['name'].upper()
            var_years[var_name].add(year)
            year_vars[year].add(var_name)
            var_descriptions[var_name].add(var['description'])
            var_by_year[var_name][year] = {
                'description': var['description'],
                'has_valueset': var_name in result['valuesets']
            }
    
    # Generate reports
    safe_print("\n" + "=" * 60)
    safe_print("GENERATING REPORTS")
    safe_print("=" * 60)
    
    # 1. Variable inventory
    all_years = sorted(year_vars.keys())
    inventory_data = []
    
    for var_name in sorted(var_years.keys()):
        years = sorted(var_years[var_name])
        descs = list(var_descriptions[var_name])
        
        row = {
            'variable': var_name,
            'first_year': min(years),
            'last_year': max(years),
            'year_count': len(years),
            'description': descs[0] if descs else '',
            'desc_changed': len(descs) > 1
        }
        
        # Add year columns
        for year in all_years:
            row[str(year)] = 1 if year in years else 0
        
        inventory_data.append(row)
    
    inventory_df = pd.DataFrame(inventory_data)
    inventory_df = inventory_df.sort_values(['year_count', 'variable'], ascending=[False, True])
    inventory_df.to_csv(output_path / 'variable_inventory.csv', index=False)
    
    # 2. Potential mappings (group similar variable names)
    base_names = defaultdict(list)
    for var in var_years.keys():
        # Extract base name by removing prefixes
        base = re.sub(r'^(PUFC?\d*_?|CC?\d*_?|A\d*_?|B\d*_?|J\d*[A-Z]*_?)', '', var)
        base = re.sub(r'^\d+_?', '', base)
        if base and len(base) >= 3:
            base_names[base].append(var)
    
    mappings_data = []
    for base, vars in sorted(base_names.items(), key=lambda x: -len(x[1])):
        if len(vars) > 1:
            for var in vars:
                years = sorted(var_years[var])
                descs = list(var_descriptions[var])
                mappings_data.append({
                    'base_name': base,
                    'variable': var,
                    'first_year': min(years) if years else None,
                    'last_year': max(years) if years else None,
                    'year_count': len(years),
                    'description': descs[0] if descs else ''
                })
    
    mappings_df = pd.DataFrame(mappings_data)
    mappings_df.to_csv(output_path / 'potential_mappings.csv', index=False)
    
    # 3. Text report
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("LFS DICTIONARY ANALYSIS REPORT")
    report_lines.append(f"Generated: {datetime.now().isoformat()}")
    report_lines.append(f"Dictionary files analyzed: {len(files)}")
    report_lines.append(f"Years covered: {min(all_years)} - {max(all_years)}")
    report_lines.append(f"Total unique variables: {len(var_years)}")
    report_lines.append("=" * 80)
    
    # Variables by prefix
    report_lines.append("\n\n1. VARIABLES BY NAMING PREFIX")
    report_lines.append("-" * 60)
    
    prefixes = defaultdict(list)
    for var in var_years.keys():
        if var.startswith('PUFC'):
            prefixes['PUFC*'].append(var)
        elif var.startswith('PUF'):
            prefixes['PUF* (non-PUFC)'].append(var)
        elif var.startswith('CC'):
            prefixes['CC*'].append(var)
        elif re.match(r'^C\d', var):
            prefixes['C##_*'].append(var)
        elif re.match(r'^A\d', var):
            prefixes['A##_*'].append(var)
        elif re.match(r'^J\d', var):
            prefixes['J##_*'].append(var)
        elif re.match(r'^B\d', var):
            prefixes['B##_*'].append(var)
        else:
            prefixes['Other'].append(var)
    
    for prefix, vars in sorted(prefixes.items(), key=lambda x: -len(x[1])):
        years_with_prefix = set()
        for var in vars:
            years_with_prefix.update(var_years[var])
        year_range = f"{min(years_with_prefix)}-{max(years_with_prefix)}" if years_with_prefix else "?"
        report_lines.append(f"\n{prefix}: {len(vars)} variables (Years: {year_range})")
        
        # Show sample variables
        for var in sorted(vars)[:15]:
            vy = sorted(var_years[var])
            desc = list(var_descriptions[var])[0][:50] if var_descriptions[var] else ''
            report_lines.append(f"    {var}: {vy[0]}-{vy[-1]} | {desc}")
        if len(vars) > 15:
            report_lines.append(f"    ... and {len(vars) - 15} more")
    
    # Potential mappings
    report_lines.append("\n\n2. POTENTIAL VARIABLE MAPPINGS (same base name)")
    report_lines.append("-" * 60)
    report_lines.append("These variables likely measure the same thing across different years:\n")
    
    for base, vars in sorted(base_names.items(), key=lambda x: -len(x[1]))[:40]:
        if len(vars) >= 2:
            report_lines.append(f"\n{base}:")
            for var in sorted(vars, key=lambda v: min(var_years[v])):
                vy = sorted(var_years[var])
                desc = list(var_descriptions[var])[0][:40] if var_descriptions[var] else ''
                report_lines.append(f"    {var}: {vy[0]}-{vy[-1]} ({len(vy)} yrs) | {desc}")
    
    # New variables by era
    report_lines.append("\n\n3. VARIABLES BY ERA")
    report_lines.append("-" * 60)
    
    eras = [
        ("2005-2011", range(2005, 2012)),
        ("2012-2016", range(2012, 2017)),
        ("2017-2020", range(2017, 2021)),
        ("2021-2024", range(2021, 2025))
    ]
    
    for era_name, era_years in eras:
        era_year_set = set(era_years) & set(all_years)
        if not era_year_set:
            continue
        
        era_vars = set()
        for y in era_year_set:
            era_vars.update(year_vars.get(y, set()))
        
        only_in_era = [v for v in era_vars if var_years[v].issubset(era_year_set)]
        
        report_lines.append(f"\n{era_name}:")
        report_lines.append(f"  Total variables: {len(era_vars)}")
        report_lines.append(f"  Variables ONLY in this era: {len(only_in_era)}")
        if only_in_era:
            for var in sorted(only_in_era)[:10]:
                desc = list(var_descriptions[var])[0][:40] if var_descriptions[var] else ''
                report_lines.append(f"    {var}: {desc}")
            if len(only_in_era) > 10:
                report_lines.append(f"    ... and {len(only_in_era) - 10} more")
    
    # Save report
    report_path = output_path / 'dictionary_analysis_report.txt'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    # Save JSON
    json_data = {
        'generated': datetime.now().isoformat(),
        'files_analyzed': len(files),
        'years': all_years,
        'total_variables': len(var_years),
        'variables': {var: {'years': sorted(years), 'descriptions': list(var_descriptions[var])} 
                      for var, years in var_years.items()},
        'prefixes': {k: v for k, v in prefixes.items()},
        'potential_mappings': {base: vars for base, vars in base_names.items() if len(vars) > 1}
    }
    
    with open(output_path / 'dictionary_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, default=str)
    
    safe_print(f"\nReports saved to: {output_path}")
    safe_print(f"  - variable_inventory.csv")
    safe_print(f"  - potential_mappings.csv")
    safe_print(f"  - dictionary_analysis_report.txt")
    safe_print(f"  - dictionary_analysis.json")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LFS Dictionary Extractor')
    parser.add_argument('-i', '--input-dir', required=True, help='Input directory with dictionary Excel files')
    parser.add_argument('-o', '--output-dir', required=True, help='Output directory for reports')
    args = parser.parse_args()
    
    safe_print("=" * 60)
    safe_print("LFS DICTIONARY EXTRACTOR")
    safe_print("=" * 60)
    
    analyze_all_dictionaries(args.input_dir, args.output_dir)