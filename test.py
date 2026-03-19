"""
test.py
=========
Verifies election output files against their source files.

Usage:
    python3 test.py

Update the paths at the top to point to your files.
"""

import pandas as pd
from pathlib import Path

# ── Point these at your files ─────────────────────────────────────────────────
OUTPUT_FILE  = Path("/Users/propadiene/cloned-repos/cities-webscraper/france_2020/outputs/plus_1000_tour2_2020.json")
SOURCE_FILE  = Path("/Users/propadiene/cloned-repos/cities-webscraper/france_2020/tour_2/2020-06-29-resultats-t2-communes-de-1000-hab-et-plus.txt")
#"/Users/propadiene/cloned-repos/cities-webscraper/france_2020/tour_1/2020-05-18-resultats-communes-de-1000-et-plus.txt"
SOURCE_SEP   = ";"       # "\t" for tour 1, ";" for tour 2
COMMUNE_SIZE = "more"     # "more" or "less"
TOUR = 2

# ── Spot-check this commune ───────────────────────────────────────────────────
CHECK_COMMUNE = "01034"   # Ambérieu-en-Bugey


def load_output(path: Path) -> pd.DataFrame:
    if path.suffix == ".json":
        return pd.read_json(path, dtype={"commune_code": str})
    return pd.read_csv(path, dtype={"commune_code": str})


def load_source(path: Path, sep: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=sep, encoding="latin-1",
                       dtype=str, low_memory=False, on_bad_lines="skip")


def check_row_counts(df: pd.DataFrame):
    print(f"\n{'='*50}")
    print("1. ROW COUNTS")
    print(f"{'='*50}")
    print(f"  Total candidates:  {len(df):,}")
    print(f"  Communes covered:  {df['commune_code'].nunique():,}")
    print(f"  Elected:           {df['elected'].sum():,}")
    if COMMUNE_SIZE == "more":
        if TOUR == 2:
            print(f"  (Expected ~10k-15k elected for more_1000 tour 2 — only ~500 communes go to round 2)")
        else:
            print(f"  (Expected ~150k-200k elected for more_1000 tour 1 all of France)")
    else:
        print(f"  (Expected ~300k-350k elected for less_1000 tour 1 all of France)")


def check_gender_balance(df: pd.DataFrame):
    print(f"\n{'='*50}")
    print("2. GENDER BALANCE")
    print(f"{'='*50}")
    counts = df["gender"].value_counts()
    total  = counts.sum()
    for g, n in counts.items():
        print(f"  {g}: {n:,} ({n/total*100:.1f}%)")
    if COMMUNE_SIZE == "more":
        print("  (Should be ~50/50 for more_1000 due to parity law)")


def check_duplicates(df: pd.DataFrame):
    print(f"\n{'='*50}")
    print("3. DUPLICATE CANDIDATES")
    print(f"{'='*50}")
    dupes = df.groupby(["commune_code", "last_name", "first_name"]).size()
    dupes = dupes[dupes > 1]
    if len(dupes) == 0:
        print("  No duplicates found ✓")
    else:
        print(f"  {len(dupes):,} duplicate name+commune combinations")
        print("  Sample:")
        print(dupes.head(10).to_string())
        if len(dupes) > 1000:
            print("  WARNING: Very high duplicate count — possible join error")


def check_spot_commune(df: pd.DataFrame, source: pd.DataFrame, commune_code: str):
    print(f"\n{'='*50}")
    print(f"4. SPOT CHECK — commune {commune_code}")
    print(f"{'='*50}")

    out_rows = df[df["commune_code"] == commune_code]
    if out_rows.empty:
        print(f"  Commune {commune_code} not found in output")
        return

    print(f"\n  Output ({len(out_rows)} candidates):")
    print(out_rows[["last_name", "first_name", "gender",
                     "party_code", "votes", "elected"]].to_string(index=False))

    # Find matching row in source — source uses local commune code (last 3 digits)
    local_code = commune_code[2:].lstrip("0")  # "01004" → "4"
    dep_code   = commune_code[:2].lstrip("0")  # "01004" → "1"

    # Source file columns vary so just search raw text
    source_rows = source[source.iloc[:, 4].str.strip().isin([local_code, commune_code])]
    if source_rows.empty:
        print(f"\n  Could not locate commune in source file for cross-check")
        return

    print(f"\n  Source row (raw):")
    print(source_rows.iloc[0].dropna().to_string())


def check_null_rates(df: pd.DataFrame):
    print(f"\n{'='*50}")
    print("5. NULL RATES PER COLUMN")
    print(f"{'='*50}")
    for col in df.columns:
        null_pct = df[col].isna().sum() / len(df) * 100
        flag = " ← HIGH" if null_pct > 10 else ""
        print(f"  {col:<20} {null_pct:5.1f}% null{flag}")


def check_vote_totals(df: pd.DataFrame, source: pd.DataFrame, commune_code: str):
    print(f"\n{'='*50}")
    print(f"6. VOTE TOTAL CROSS-CHECK — commune {commune_code}")
    print(f"{'='*50}")
    if COMMUNE_SIZE != "more":
        print("  (Skipped for less_1000 — individual votes don't sum to valid_votes)")
        return

    out_rows = df[df["commune_code"] == commune_code]
    if out_rows.empty or "votes" not in out_rows.columns:
        print("  No data for this commune")
        return

    # For more_1000, each list has one vote total — sum unique list votes
    list_vote_totals = out_rows.drop_duplicates(
        subset=["commune_code", "list_name"]
    )["votes"].sum()
    print(f"  Sum of list vote totals in output: {list_vote_totals:,}")
    print(f"  (Cross-check against 'Exprimés' in source file manually)")


if __name__ == "__main__":
    print(f"Loading output:  {OUTPUT_FILE.name}")
    df = load_output(OUTPUT_FILE)

    print(f"Loading source:  {SOURCE_FILE.name}")
    source = load_source(SOURCE_FILE, SOURCE_SEP)

    check_row_counts(df)
    check_gender_balance(df)
    check_duplicates(df)
    check_null_rates(df)
    check_spot_commune(df, source, CHECK_COMMUNE)
    check_vote_totals(df, source, CHECK_COMMUNE)

    print(f"\n{'='*50}")
    print("Done.")