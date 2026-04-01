"""
360 Clients Sending POS — Excel to JSON

Reads all tabs from the RAC POS User Review Excel file and saves them to
360clientssendingpos.json in the same directory as this script.

Usage:
    python 360clientssendingpos.py
"""

import json
import math
import os

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = (
    r"C:\Users\KevinKolb\360Incentives.com Canada ULC"
    r"\Channel Relationship Management - Channel Insights"
    r"\DATA\RAC POS User Review - All Clients EC.xlsx"
)
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "360clientssendingpos.json")


def load_excel(path: str) -> dict:
    xl = pd.ExcelFile(path, engine="openpyxl")
    result = {}
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        records = df.to_dict(orient="records")
        # Replace NaN/NaT/inf with None so JSON serialization works
        cleaned = [
            {k: (None if (v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))) else v)
             for k, v in row.items()}
            for row in records
        ]
        result[sheet] = cleaned
        print(f"  {sheet}: {len(cleaned)} rows")
    return result


def main() -> None:
    print(f"Reading: {INPUT_FILE}")
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: File not found:\n  {INPUT_FILE}")
        return

    data = load_excel(INPUT_FILE)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    total_rows = sum(len(rows) for rows in data.values())
    print(f"\nSaved {len(data)} sheets / {total_rows} total rows to:")
    print(f"  {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
