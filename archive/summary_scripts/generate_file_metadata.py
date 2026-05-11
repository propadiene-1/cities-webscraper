"""
generate_file_metadata.py
=========================
Generate file_metadata.csv — one row per joined output file with key
counts and data quality flags. Run after any merge_demographics.py run.
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

# Which census population column each year uses
CENSUS_POP_COL = {"2008": "P08_POP", "2014": "P14_POP", "2020": "P19_POP", "2026": "P22_POP"}

OUT_PATH = Path("newest_file_metadata.csv")


def pct_null(df, col):
    if col not in df.columns:
        return None
    return round(df[col].isna().mean() * 100, 2)


def flag(val):
    """Return True/False string for presence columns."""
    return "yes" if val else "no"


rows = []
for year, size, tour, path in FILES:
    if not path.exists():
        print(f"  MISSING: {path}")
        continue

    df = pd.read_csv(path, dtype={"commune_code": str}, low_memory=False)
    pop_col = CENSUS_POP_COL[year]

    elected_col_present = "elected" in df.columns
    elected_always_null = elected_col_present and df["elected"].isna().all()

    rows.append({
        "year":                  year,
        "commune_size":          size,
        "tour":                  tour,
        "file_path":             str(path),
        "total_rows":            len(df),
        "communes":              df["commune_code"].nunique(),
        "elected_candidates":    int(df["elected"].sum()) if elected_col_present and not elected_always_null else None,
        "elected_available":     flag(elected_col_present and not elected_always_null),
        "list_voting":           flag(size in ("plus_1000", "all") or year == "2026"),
        "census_pop_col":        pop_col,
        "pct_census_missing":    pct_null(df, pop_col),
        "pct_party_code_missing": pct_null(df, "party_code"),
        "pct_list_name_missing": pct_null(df, "list_name"),
        "pct_votes_missing":     pct_null(df, "votes"),
        "has_list_name":         flag("list_name" in df.columns),
        "has_party_code":        flag("party_code" in df.columns),
    })
    print(f"  {year} {size} {tour}: {len(df):,} rows, {df['commune_code'].nunique():,} communes")

out = pd.DataFrame(rows)
out.to_csv(OUT_PATH, index=False)
print(f"\nWritten {len(out)} rows → {OUT_PATH}")
print(out.to_string(index=False))
