"""
update_missing_columns.py
=========================
Regenerate missing_column_data.csv from all current joined outputs.
Run this after any merge_demographics.py run.

Rows are sorted descending by pct_empty so structural gaps (e.g. 100% empty
elected in 2008, 64% empty party_code in 2026) appear first, and small
census-mismatch gaps (e.g. 0.9% unmatched communes) appear at the bottom.
"""

import pandas as pd
from pathlib import Path

BASE = Path("new_france_joined_outputs")

FILES = [
    ("2008", "plus_1000", "tour1", BASE / "france_joined_2008/joined_plus_1000_tour1_2008.csv"),
    ("2008", "plus_1000", "tour2", BASE / "france_joined_2008/joined_plus_1000_tour2_2008.csv"),
    ("2014", "plus_1000", "tour1", BASE / "france_joined_2014/joined_plus_1000_tour1_2014.csv"),
    ("2014", "plus_1000", "tour2", BASE / "france_joined_2014/joined_plus_1000_tour2_2014.csv"),
    ("2014", "less_1000", "tour1", BASE / "france_joined_2014/joined_less_1000_tour1_2014.csv"),
    ("2014", "less_1000", "tour2", BASE / "france_joined_2014/joined_less_1000_tour2_2014.csv"),
    ("2020", "plus_1000", "tour1", BASE / "france_joined_2020/joined_plus_1000_tour1_2020.csv"),
    ("2020", "plus_1000", "tour2", BASE / "france_joined_2020/joined_plus_1000_tour2_2020.csv"),
    ("2020", "less_1000", "tour1", BASE / "france_joined_2020/joined_less_1000_tour1_2020.csv"),
    ("2020", "less_1000", "tour2", BASE / "france_joined_2020/joined_less_1000_tour2_2020.csv"),
    ("2026", "all",       "tour1", BASE / "france_joined_2026/joined_tour1_2026.csv"),
    ("2026", "all",       "tour2", BASE / "france_joined_2026/joined_tour2_2026.csv"),
]

TRACK_COLS = [
    "last_name", "first_name", "list_name", "party_code", "elected",
    "P08_POP", "P14_POP", "P19_POP", "P22_POP",
    "pct_female", "pct_male",
    "pct_age_0_14", "pct_age_15_29", "pct_age_30_44",
    "pct_age_45_59", "pct_age_60_74", "pct_age_75_plus",
]

OUT_PATH = Path("newest_missing_column_data.csv")

if __name__ == "__main__":
    rows = []
    for year, size, tour, path in FILES:
        if not path.exists():
            print(f"  MISSING file: {path}")
            continue
        df = pd.read_csv(path, dtype={"commune_code": str}, low_memory=False)
        total = len(df)
        for col in TRACK_COLS:
            if col not in df.columns:
                continue
            empty = df[col].isna().sum()
            if empty == 0:
                continue
            rows.append({
                "year":         year,
                "commune_size": size,
                "tour":         tour,
                "column":       col,
                "empty_rows":   int(empty),
                "total_rows":   total,
                "pct_empty":    round(empty / total * 100, 2),
            })
        print(f"  {year} {size} {tour}: {total:,} rows")

    out = pd.DataFrame(rows).sort_values("pct_empty", ascending=False).reset_index(drop=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"\nWritten {len(out)} rows → {OUT_PATH}")
