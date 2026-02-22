"""
Valueset parser for PSA LFS dictionaries (Sheet 2)

Features:
- Blank-row block detection
- Range handling (A–B)
- Continuation rows (only end value)
- Conservative confidence logic
"""

import pandas as pd


def parse_dictionary_valuesets(xlsx_path):
    xls = pd.ExcelFile(xlsx_path)
    sheet = pd.read_excel(xls, sheet_name=1)

    blocks = split_into_blocks(sheet)
    valuesets = {}

    for block in blocks:
        pairs, confidence = extract_valueset(block)
        if confidence != "LOW":
            # NOTE: This assumes one valueset per block
            # Ambiguous blocks are intentionally ignored
            valuesets.update(pairs)

    return valuesets


def split_into_blocks(df):
    blocks = []
    current = []

    for _, row in df.iterrows():
        if row.isna().all():
            if current:
                blocks.append(pd.DataFrame(current))
                current = []
        else:
            current.append(row)

    if current:
        blocks.append(pd.DataFrame(current))

    return blocks


def extract_valueset(block):
    pairs = {}
    prev_end = None

    for _, row in block.iterrows():
        nums = []
        label = None

        for cell in row:
            if isinstance(cell, (int, float)):
                if not pd.isna(cell):
                    nums.append(int(cell))
            elif isinstance(cell, str) and label is None:
                label = cell.strip().upper()

        if not nums or not label:
            continue

        if len(nums) >= 2:
            start, end = nums[0], nums[1]
        elif len(nums) == 1 and prev_end is not None:
            start, end = prev_end + 1, nums[0]
        else:
            start = end = nums[0]

        for c in range(start, end + 1):
            pairs[c] = label

        prev_end = end

    confidence = "HIGH" if len(pairs) >= 2 else "LOW"
    return pairs, confidence
