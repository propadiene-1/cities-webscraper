"""
summarize_joined_outputs.py
===========================
Print a summary table of rows and communes in each joined output,
grouped by year (combining plus_1000 and less_1000 within each year/tour).
"""

import pandas as pd
from pathlib import Path

BASE = Path("new_france_joined_outputs")

FILES = [
    ("2008", "tour1", [BASE / "france_joined_2008/joined_plus_1000_tour1_2008.csv"]),
    ("2008", "tour2", [BASE / "france_joined_2008/joined_plus_1000_tour2_2008.csv"]),
    ("2014", "tour1", [
        BASE / "france_joined_2014/joined_plus_1000_tour1_2014.csv",
        BASE / "france_joined_2014/joined_less_1000_tour1_2014.csv",
    ]),
    ("2014", "tour2", [
        BASE / "france_joined_2014/joined_plus_1000_tour2_2014.csv",
        BASE / "france_joined_2014/joined_less_1000_tour2_2014.csv",
    ]),
    ("2020", "tour1", [
        BASE / "france_joined_2020/joined_plus_1000_tour1_2020.csv",
        BASE / "france_joined_2020/joined_less_1000_tour1_2020.csv",
    ]),
    ("2020", "tour2", [
        BASE / "france_joined_2020/joined_plus_1000_tour2_2020.csv",
        BASE / "france_joined_2020/joined_less_1000_tour2_2020.csv",
    ]),
    ("2026", "tour1", [BASE / "france_joined_2026/joined_tour1_2026.csv"]),
    ("2026", "tour2", [BASE / "france_joined_2026/joined_tour2_2026.csv"]),
]

rows = []
for year, tour, paths in FILES:
    dfs = []
    for path in paths:
        if not path.exists():
            print(f"  MISSING: {path}")
            continue
        dfs.append(pd.read_csv(path, dtype={"commune_code": str}, low_memory=False))

    if not dfs:
        continue

    df = pd.concat(dfs, ignore_index=True)
    rows.append({
        "year":     year,
        "tour":     tour,
        "rows":     len(df),
        "communes": df["commune_code"].nunique(),
    })

OUT_PATH = Path("joined_outputs_summary.csv")

out = pd.DataFrame(rows)
print(out.to_string(index=False))
out.to_csv(OUT_PATH, index=False)
print(f"\nWritten → {OUT_PATH}")
