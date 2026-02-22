"""
COLUMN_PRIORITY Cross-Reference
=================================
For each target column in the harmonizer, checks:
  - Which of its COLUMN_PRIORITY variants actually exist in the release dictionaries
  - How many releases are covered vs. uncovered
  - For uncovered releases, lists what variables ARE available (to spot missing aliases)

Outputs:
  output_summary/crossref_coverage.csv        — per-target summary
  output_summary/crossref_uncovered_detail.csv — uncovered releases per target with available vars

Usage:
    python crossref_column_priority.py
"""

import pandas as pd
import sys
from pathlib import Path

# ── Pull COLUMN_PRIORITY and OUTPUT_SCHEMA directly from the harmonizer ────
sys.path.insert(0, str(Path(__file__).parent))
from lfs_harmonizer_complete_v8 import COLUMN_PRIORITY, OUTPUT_SCHEMA

# ── Load inventory ─────────────────────────────────────────────────────────
INV_PATH = Path('output_summary/variable_inventory_full.csv')
OUT_DIR  = Path('output_summary')

inv = pd.read_csv(INV_PATH)
inv['variable_upper'] = inv['variable'].str.upper().str.strip()

# Build lookup: release → set of variable names (uppercased)
release_vars = (
    inv.groupby('release')['variable_upper']
    .apply(set)
    .to_dict()
)

all_releases = sorted(
    inv[['release','year','month']].drop_duplicates()
    .sort_values(['year','month'])
    ['release']
    .tolist()
)
n_releases = len(all_releases)

print(f"\nCOLUMN_PRIORITY CROSS-REFERENCE")
print(f"{'=' * 70}")
print(f"  Target columns   : {len(OUTPUT_SCHEMA)}")
print(f"  Releases         : {n_releases}")
print(f"  Inventory rows   : {len(inv)}")
print(f"{'=' * 70}\n")

# ── Per-target analysis ─────────────────────────────────────────────────────
coverage_rows   = []
uncovered_rows  = []

W = 70
print(f"  {'TARGET':<24} {'VARIANTS':>8} {'COVERED':>9} {'UNCOV':>7} {'COV%':>7}  VARIANTS IN PRIORITY")
print(f"  {'─' * W}")

for target in OUTPUT_SCHEMA:
    variants = [v.upper() for v in COLUMN_PRIORITY.get(target, [target])]

    covered_releases   = []
    uncovered_releases = []
    variant_used       = {}   # release -> which variant matched

    for rel in all_releases:
        rel_vars = release_vars.get(rel, set())
        matched  = next((v for v in variants if v in rel_vars), None)
        if matched:
            covered_releases.append(rel)
            variant_used[rel] = matched
        else:
            uncovered_releases.append(rel)

    cov_pct = round(len(covered_releases) / n_releases * 100, 1)

    # Variants that matched at least one release
    matched_variants = sorted(set(variant_used.values()))

    coverage_rows.append({
        'target'             : target,
        'n_variants_in_priority': len(variants),
        'n_releases_covered' : len(covered_releases),
        'n_releases_uncovered': len(uncovered_releases),
        'coverage_pct'       : cov_pct,
        'variants_in_priority': ', '.join(COLUMN_PRIORITY.get(target, [target])),
        'variants_matched'   : ', '.join(matched_variants),
        'covered_releases'   : ', '.join(covered_releases),
        'uncovered_releases' : ', '.join(uncovered_releases),
    })

    # For each uncovered release, record what variables are available
    for rel in uncovered_releases:
        rel_vars_list = sorted(release_vars.get(rel, set()))
        uncovered_rows.append({
            'target'              : target,
            'release'             : rel,
            'variants_searched'   : ', '.join(COLUMN_PRIORITY.get(target, [target])),
            'available_variables' : ', '.join(rel_vars_list),
        })

    # Console line
    var_short = ', '.join(COLUMN_PRIORITY.get(target, [target])[:4])
    if len(variants) > 4:
        var_short += f' (+{len(variants)-4})'
    bar = '#' * int(cov_pct / 5)   # out of 20 chars
    flag = '  !! GAP' if cov_pct < 50 else ('  ! partial' if cov_pct < 90 else '')
    print(f"  {target:<24} {len(variants):>8} {len(covered_releases):>9} "
          f"{len(uncovered_releases):>7} {cov_pct:>6.1f}%{flag}")

print(f"  {'─' * W}")

# ── Save CSVs ───────────────────────────────────────────────────────────────
cov_df  = pd.DataFrame(coverage_rows)
unc_df  = pd.DataFrame(uncovered_rows)

cov_path = OUT_DIR / 'crossref_coverage.csv'
unc_path = OUT_DIR / 'crossref_uncovered_detail.csv'

cov_df.to_csv(cov_path, index=False)
unc_df.to_csv(unc_path, index=False)

# ── Summary ──────────────────────────────────────────────────────────────────
fully_covered  = cov_df[cov_df['n_releases_uncovered'] == 0]
fully_uncovered= cov_df[cov_df['n_releases_covered']   == 0]
partial        = cov_df[(cov_df['n_releases_covered'] > 0) &
                        (cov_df['n_releases_uncovered'] > 0)]

print(f"\n  SUMMARY")
print(f"  {'─' * W}")
print(f"  Fully covered   (all {n_releases} releases) : {len(fully_covered)}")
print(f"  Partially covered                        : {len(partial)}")
print(f"  Fully uncovered (0 releases)             : {len(fully_uncovered)}")

if not fully_covered.empty:
    print(f"\n  FULLY COVERED ({len(fully_covered)}):")
    for _, r in fully_covered.iterrows():
        print(f"    {r['target']:<26} {r['coverage_pct']:>5.1f}%  variants: {r['variants_matched']}")

if not fully_uncovered.empty:
    print(f"\n  FULLY UNCOVERED — no alias works in ANY release ({len(fully_uncovered)}):")
    for _, r in fully_uncovered.iterrows():
        print(f"    {r['target']:<26} searched: {r['variants_in_priority']}")

if not partial.empty:
    print(f"\n  PARTIAL COVERAGE — sorted by coverage % asc ({len(partial)}):")
    for _, r in partial.sort_values('coverage_pct').iterrows():
        print(f"    {r['target']:<26} {r['coverage_pct']:>5.1f}%  "
              f"covered={r['n_releases_covered']}  uncovered={r['n_releases_uncovered']}")
        print(f"      matched variants : {r['variants_matched']}")

print(f"\n  Output files:")
print(f"    {cov_path}")
print(f"    {unc_path}")
print(f"{'=' * 70}\n")
