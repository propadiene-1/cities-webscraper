"""
Notes (process_plus_1000.py)
=========================
Communes of 1,000+ (list voting).
One row per candidate.
Update YEAR and TOUR to adjust

File naming conventions:
  results:       {tour_folder}/*communes-de-1000*.txt
  registrations: {year_folder}/candidats*.csv

For elected status:
  results = seats_won per list.
  registrations = each candidate's rank on their list.
  elected = list_rank <= seats_won  (seats fill from the top of the list down)

All candidates on same list have the same # votes (votes are list-level)
Does not include age.
"""

from pathlib import Path
import pandas as pd
from utils import (
    to_int, to_float, clean,
    read_wide_file, extract_commune_metadata, parse_registrations,
    get_config,
)
 
# --- Adjust year/tour ------
YEAR = 2014
TOUR = 2  # 1 or 2
 
# --- Not adjusted ------
BASE_DIR   = Path("/Users/propadiene/cloned-repos/cities-webscraper")
YEAR_DIR   = BASE_DIR / f"france_{YEAR}"
TOUR_DIR   = YEAR_DIR / f"tour_{TOUR}"
 
FILE_RESULTS       = next(TOUR_DIR.glob("*1000*plus*.txt"))
FILE_REGISTRATIONS = next(YEAR_DIR.glob("candidats*.csv"), None)
OUT_PATH           = YEAR_DIR / f"candidate_outputs/plus_1000_tour{TOUR}_{YEAR}.csv"
 
OUTPUT_COLS = [
    "commune_code", "commune_name",
    "last_name", "first_name", "gender",
    "party_code", "list_name",
    "votes", "elected",
]
 
 
def parse_results(path: Path, year: int) -> pd.DataFrame:
    """Unpack wide result file into one row per list."""
    # 2014 plus_1000 files are UTF-8; all others are latin-1
    encoding = "utf-8" if year == 2014 else "latin-1"

    # detect separator -- tabs for tour 1 2014, semicolons for all others
    with open(path, encoding=encoding) as f:
        first_line = f.readline()
    sep = ";" if first_line.count(";") > first_line.count("\t") else "\t"

    cfg        = get_config(year)
    n_fixed    = cfg["N_FIXED"]
    block_size = cfg["BLOCK_SIZE"]
    block      = cfg["BLOCK_MORE"]

    raw = read_wide_file(path, sep=sep, year=year, encoding=encoding)
    n_blocks = (len(raw.columns) - n_fixed) // block_size
    print(f"  {len(raw):,} communes × {n_blocks} list blocks")
 
    rows = []
    for _, row in raw.iterrows():
        commune = extract_commune_metadata(row)
        for i in range(n_blocks):
            offset = n_fixed + i * block_size
            b = [row.iloc[offset + j] if offset + j < len(row) else None
                 for j in range(block_size)]
 
            list_number = clean(b[block["list_number"]]) if "list_number" in block else str(i + 1)
            votes       = to_int(b[block["votes"]])
            if list_number is None and votes is None:
                continue
 
            rows.append({
                **commune,
                "list_number": list_number,
                "party_code":  clean(b[block["party_code"]]),
                "gender_raw":  clean(b[block["gender_raw"]]) if "gender_raw" in block else None,
                "last_name":   clean(b[block["last_name"]]).upper() if "last_name" in block and clean(b[block["last_name"]]) else None,
                "first_name":  clean(b[block["first_name"]]) if "first_name" in block else None,
                "list_name":   clean(b[block["list_name"]]),
                "seats_won":   to_int(b[block["seats_won"]]),
                "votes":       votes,
            })
 
    return pd.DataFrame(rows)
 
 
if __name__ == "__main__":
    print(f"Processing plus_1000 — {YEAR} tour {TOUR}")

    print("  Parsing results (list-level)...")
    df_lists = parse_results(FILE_RESULTS, YEAR)

    if FILE_REGISTRATIONS:
        print("  Parsing registrations (candidate-level)...")
        df_reg = parse_registrations(FILE_REGISTRATIONS)
        df_reg = df_reg[df_reg["list_number"].notna() & (df_reg["list_number"] != "")]
        print("  Joining + computing elected status...")
        df = df_reg.merge(df_lists, on=["commune_code", "list_number"], how="left")
        df = df[df["votes"].notna()]
        df["votes"] = df["votes"].astype(int)
        df["seats_won"] = pd.to_numeric(df["seats_won"], errors="coerce").astype("Int64")
        df["elected"] = df["list_rank"] <= df["seats_won"]
        df["gender"]  = df["gender"].fillna(df["gender_raw"])
        sort_cols = ["commune_code", "list_number", "list_rank"]
    else:
        print("  No registrations — list-head output, elected = seats_won > 0")
        df = df_lists[df_lists["votes"].notna()].copy()
        df["votes"] = df["votes"].astype(int)
        df["elected"] = df["seats_won"].apply(lambda x: x > 0 if x is not None else False) #because only list heads are in the dataset
        df["gender"]  = df["gender_raw"]
        sort_cols = ["commune_code", "list_number"]

    df["elected"] = df["elected"].fillna(False).astype(bool)

    cols = [c for c in OUTPUT_COLS if c in df.columns]
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.sort_values(sort_cols)[cols].to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    df.sort_values(sort_cols)[cols].to_json(OUT_PATH.with_suffix(".json"), orient="records", force_ascii=False, indent=2)
    print(f"DONE:    {len(df):,} rows → {OUT_PATH}")
    print(f"    Communes: {df['commune_code'].nunique():,}  |  Elected: {df['elected'].sum():,}")