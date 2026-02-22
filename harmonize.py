#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import pandas as pd


# ======================================================
# EMPLOYMENT STATUS PRECEDENCE (RAW VARIABLES ONLY)
# ======================================================

EMPSTAT_PRECEDENCE = [
    "NEWEMPST",
    "CEMPST2",
    "CEMPST1",
    "ANSOEMPSTAT"
]


# ======================================================
# MAIN
# ======================================================

def main():
    ap = argparse.ArgumentParser(
        description="LFS Structural Harmonization (Crosswalk-Driven)"
    )
    ap.add_argument("--data", required=True, help="Raw LFS microdata CSV")
    ap.add_argument(
        "--crosswalk",
        required=True,
        help="final_variable_crosswalk_with_unmapped.json"
    )
    ap.add_argument("--out", required=True, help="Output parquet file")
    args = ap.parse_args()

    # ------------------------------
    # Load inputs
    # ------------------------------
    df = pd.read_csv(args.data)

    with open(args.crosswalk, "r", encoding="utf-8") as f:
        crosswalk_obj = json.load(f)

    variable_crosswalk = crosswalk_obj["variable_crosswalk"]

    harmonized = {}

    # ------------------------------
    # Crosswalk-driven harmonization
    # ------------------------------
    for block in variable_crosswalk:
        master = block["name"]
        source_vars = block.get("variables", [])

        # ==================================================
        # UNIVERSAL forward-compatibility rule
        # ==================================================
        if master in df.columns:
            harmonized[master] = df[master]
            continue

        # ==================================================
        # Special case: Employment status
        # ==================================================
        if master == "PUFNEWEMPSTAT":
            value = pd.NA
            for v in EMPSTAT_PRECEDENCE:
                if v in df.columns:
                    value = df[v]
                    break
            harmonized[master] = value
            continue

        # ==================================================
        # Default: numeric passthrough via crosswalk
        # ==================================================
        value = pd.NA
        for v in source_vars:
            if v in df.columns:
                value = df[v]
                break

        harmonized[master] = value

    # ------------------------------
    # Output
    # ------------------------------
    out_df = pd.DataFrame(harmonized)

    out_path = Path(args.out)
    if out_path.parent == Path("."):
        out_path = Path("out") / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_df.to_parquet(out_path, index=False)

    print(f"✔ Harmonized file written to: {out_path}")
    print(f"✔ Total master variables: {out_df.shape[1]}")


# ======================================================
if __name__ == "__main__":
    main()