"""
LFS Variable Inventory Builder
================================
Reads all LFS data dictionary Excel files and produces three reference files:

  1. variable_inventory_full.csv   — one row per variable per release
                                     (release, year, month, variable, label)

  2. variable_presence_matrix.csv  — variables as rows, releases as columns
                                     cell = YES / NO

  3. variable_summary.csv          — one row per unique variable name
                                     (n_releases present, which releases, label samples)

Usage:
    python build_variable_inventory.py
    python build_variable_inventory.py -d raw_dicts/ -o output_summary/
"""

import pandas as pd
import re
import argparse
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
DICT_DIR    = Path('raw_dicts')
OUT_DIR     = Path('output_summary')
VAR_COL     = 4      # column index E  — variable name
LABEL_COL   = 5      # column index F  — variable label
SHEET_IDX   = 0      # first sheet = dictionary

MONTH_MAP = {
    'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
    'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12,
}

# ── Helpers ─────────────────────────────────────────────────────────────────

def parse_release(stem: str):
    """Extract year and month number from filename stem e.g. '2024-07JUL'."""
    year_match = re.search(r'(20\d{2}|199\d)', stem)
    year = int(year_match.group(1)) if year_match else None
    month = next(
        (num for abbr, num in MONTH_MAP.items() if abbr in stem.upper()),
        None
    )
    return year, month


def read_variables(filepath: Path) -> list[dict]:
    """Extract variable names + labels from column E/F of sheet 0."""
    try:
        df = pd.read_excel(
            filepath,
            sheet_name=SHEET_IDX,
            header=None,
            dtype=str,
            engine='openpyxl',
        )
    except Exception as e:
        print(f"  [ERROR] {filepath.name}: {e}")
        return []

    if df.shape[1] <= VAR_COL:
        print(f"  [SKIP]  {filepath.name}: fewer than {VAR_COL+1} columns")
        return []

    year, month = parse_release(filepath.stem)
    records = []

    for _, row in df.iterrows():
        raw_name = row.iloc[VAR_COL]
        if pd.isna(raw_name) or str(raw_name).strip() in ('', 'nan', 'None'):
            continue

        var_name = str(raw_name).strip()

        raw_label = row.iloc[LABEL_COL] if df.shape[1] > LABEL_COL else None
        label = str(raw_label).strip() if not pd.isna(raw_label) else ''

        records.append({
            'file'    : filepath.name,
            'release' : filepath.stem,
            'year'    : year,
            'month'   : month,
            'variable': var_name,
            'label'   : label,
        })

    return records


# ── Main ────────────────────────────────────────────────────────────────────

def main(dict_dir: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    # Collect all xlsx files, deduplicated
    all_files = sorted(dict_dir.glob('*.xlsx')) + sorted(dict_dir.glob('*.XLSX'))
    seen, files = set(), []
    for f in all_files:
        if f.name.lower() not in seen:
            seen.add(f.name.lower())
            files.append(f)

    print(f"\nLFS VARIABLE INVENTORY BUILDER")
    print(f"{'=' * 50}")
    print(f"Files found : {len(files)}")
    print(f"Output dir  : {out_dir}")
    print(f"{'=' * 50}\n")

    all_records = []
    errors = []

    for i, fp in enumerate(files, 1):
        records = read_variables(fp)
        if records:
            print(f"[{i:>3}/{len(files)}]  {fp.name:<30}  {len(records):>3} variables  "
                  f"({records[0]['year']}-{str(records[0]['month']).zfill(2)})")
            all_records.extend(records)
        else:
            errors.append(fp.name)
            print(f"[{i:>3}/{len(files)}]  {fp.name:<30}  !! FAILED")

    if not all_records:
        print("\nNo records extracted. Exiting.")
        return

    full_df = pd.DataFrame(all_records)
    full_df = full_df.sort_values(['year', 'month', 'variable']).reset_index(drop=True)

    # ── 1. Full inventory CSV ─────────────────────────────────────────
    full_path = out_dir / 'variable_inventory_full.csv'
    full_df.to_csv(full_path, index=False)

    # ── 2. Presence matrix ────────────────────────────────────────────
    releases     = full_df.sort_values(['year','month'])['release'].unique().tolist()
    all_vars     = sorted(full_df['variable'].unique())
    release_sets = {
        rel: set(full_df[full_df['release'] == rel]['variable'])
        for rel in releases
    }

    matrix_rows = []
    for var in all_vars:
        row = {'variable': var}
        for rel in releases:
            row[rel] = 'YES' if var in release_sets[rel] else 'NO'
        matrix_rows.append(row)

    matrix_df = pd.DataFrame(matrix_rows)
    matrix_path = out_dir / 'variable_presence_matrix.csv'
    matrix_df.to_csv(matrix_path, index=False)

    # ── 3. Summary per unique variable ────────────────────────────────
    summary_rows = []
    for var in all_vars:
        sub = full_df[full_df['variable'] == var]
        present_in  = sorted(sub['release'].unique())
        missing_in  = [r for r in releases if r not in present_in]
        years_seen  = sorted(sub['year'].dropna().unique().astype(int).tolist())
        label_sample = sub['label'].dropna().iloc[0] if not sub['label'].dropna().empty else ''

        summary_rows.append({
            'variable'        : var,
            'label_sample'    : label_sample,
            'n_releases'      : len(present_in),
            'n_missing'       : len(missing_in),
            'pct_coverage'    : round(len(present_in) / len(releases) * 100, 1),
            'first_year'      : min(years_seen) if years_seen else None,
            'last_year'       : max(years_seen) if years_seen else None,
            'present_in'      : ', '.join(present_in),
            'missing_in'      : ', '.join(missing_in),
        })

    summary_df = (
        pd.DataFrame(summary_rows)
        .sort_values('n_releases', ascending=False)
        .reset_index(drop=True)
    )
    summary_path = out_dir / 'variable_summary.csv'
    summary_df.to_csv(summary_path, index=False)

    # ── Console report ────────────────────────────────────────────────
    n_files   = len(files)
    n_success = n_files - len(errors)
    n_vars    = len(all_vars)
    n_rel     = len(releases)

    print(f"\n{'=' * 50}")
    print(f"DONE")
    print(f"{'=' * 50}")
    print(f"  Files processed   : {n_success}/{n_files}")
    print(f"  Unique variables  : {n_vars}")
    print(f"  Releases covered  : {n_rel}")
    print(f"  Year range        : {full_df['year'].min():.0f} – {full_df['year'].max():.0f}")

    # Variables present in ALL releases
    universal = summary_df[summary_df['n_missing'] == 0]
    print(f"\n  Present in ALL {n_rel} releases  : {len(universal)}")
    for _, r in universal.iterrows():
        print(f"    {r['variable']:<30} {r['label_sample']}")

    # Variables present in only ONE release
    unique_to_one = summary_df[summary_df['n_releases'] == 1]
    print(f"\n  Present in only 1 release     : {len(unique_to_one)}")

    # Variables missing from more than half
    often_missing = summary_df[summary_df['pct_coverage'] < 50].sort_values('pct_coverage')
    print(f"\n  Coverage < 50% ({len(often_missing)} variables):")
    for _, r in often_missing.head(20).iterrows():
        print(f"    {r['variable']:<30} {r['pct_coverage']:>5.1f}%  "
              f"({r['n_releases']}/{n_rel} releases)")

    if errors:
        print(f"\n  FAILED files ({len(errors)}):")
        for e in errors:
            print(f"    - {e}")

    print(f"\n  Output files:")
    print(f"    {full_path}")
    print(f"    {matrix_path}")
    print(f"    {summary_path}")
    print(f"{'=' * 50}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LFS Variable Inventory Builder')
    parser.add_argument('-d', '--dict-dir', default=str(DICT_DIR),
                        help=f'Folder with dictionary xlsx files (default: {DICT_DIR})')
    parser.add_argument('-o', '--output-dir', default=str(OUT_DIR),
                        help=f'Output folder (default: {OUT_DIR})')
    args = parser.parse_args()

    main(Path(args.dict_dir), Path(args.output_dir))
