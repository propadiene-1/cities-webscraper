"""
Notes (process_less_1000.py)
=========================
Communes of <1,000 (individual voting)
One row per candidate.
Update YEAR and TOUR to adjust

File naming assumptions:
  results:       {tour_folder}/*communes-de-moins-de-1000*.txt
  registrations: {year_folder}/candidats*.csv

Elected status checked with results = "Oui"
Votes are per individual candidate.
Does not include age.
"""

from pathlib import Path
import pandas as pd
from utils import (
    to_int, to_float, clean,
    read_wide_file, extract_commune_metadata, parse_registrations,
    get_config,
)
 
# --- Adjust year/tour -----
YEAR = 2020
TOUR = 1   # 1 or 2
 
# --- Not adjusted -----
BASE_DIR   = Path("/Users/propadiene/cloned-repos/cities-webscraper")
YEAR_DIR   = BASE_DIR / f"france_{YEAR}"
TOUR_DIR   = YEAR_DIR / f"tour_{TOUR}"
 
FILE_RESULTS       = next(TOUR_DIR.glob("*moins*1000*.txt"))
FILE_REGISTRATIONS = next(YEAR_DIR.glob("candidats*.csv"), None)
OUT_PATH           = YEAR_DIR / f"candidate_outputs/less_1000_tour{TOUR}_{YEAR}.csv"
 
OUTPUT_COLS = [
    "commune_code", "commune_name",
    "last_name", "first_name", "gender",
    "party_code", "votes", "elected",
]
 
def parse_results(path: Path, year: int) -> pd.DataFrame:
    """Unpack wide result file into one row per candidate."""
    cfg        = get_config(year)
    n_fixed    = cfg["N_FIXED"]
    block_size = cfg["BLOCK_SIZE"]
    block      = cfg["BLOCK_LESS"]
 
    raw = read_wide_file(path, sep=";", year=year)
    n_blocks = (len(raw.columns) - n_fixed) // block_size
    print(f"  {len(raw):,} communes × {n_blocks} candidate blocks")
 
    rows = []
    for _, row in raw.iterrows():
        commune = extract_commune_metadata(row)
        for i in range(n_blocks):
            offset = n_fixed + i * block_size
            b = [row.iloc[offset + j] if offset + j < len(row) else None
                 for j in range(block_size)]
 
            last_name = clean(b[block["last_name"]])
            votes     = to_int(b[block["votes"]])
            if not last_name and votes is None:
                continue
 
            rows.append({
                **commune,
                "party_code": clean(b[block["party_code"]]),
                "gender_raw": clean(b[block["gender_raw"]]),
                "last_name":  last_name.upper() if last_name else None,
                "first_name": clean(b[block["first_name"]]),
                "elected":    clean(b[block["elected"]]) == "Oui",
                "votes":      votes,
            })
 
    return pd.DataFrame(rows)
 
 
if __name__ == "__main__":
    print(f"Processing less_1000 — {YEAR} tour {TOUR}")

    if YEAR == 2014 and TOUR == 1:
        # tour 1 is already long format — one row per candidate
        print("  Reading 2014 tour 1 (already long format)...")
        raw = pd.read_csv(FILE_RESULTS, sep=";", encoding="latin-1", encoding_errors="replace", dtype=str)
        df = pd.DataFrame({
            "commune_code": (raw["CODDPT"].str.zfill(2) + raw["CODSUBCOM"].str.zfill(3)),
            "commune_name": raw["LIBSUBCOM"],
            "last_name":    raw["NOMPSNEXT"].str.upper(),
            "first_name":   raw["PREPSN"],
            "gender":       raw["SEXPSN"],
            "party_code":   None,
            "votes":        raw["NBRVOIX"].apply(to_int),
            "elected":      raw["ELU"] == "Elu",
    })

    elif YEAR == 2014 and TOUR == 2:
        # tour 2 uses wide format — same as plus_1000 2014
        print("  Parsing results...")
        df_results = parse_results(FILE_RESULTS, YEAR)
        df = df_results.copy()
        df["gender"] = df["gender_raw"]
    elif YEAR == 2020:
        print("  Parsing results...")
        df_results = parse_results(FILE_RESULTS, YEAR)
        print("  Parsing registrations...")
        df_reg = parse_registrations(FILE_REGISTRATIONS)[["commune_code", "last_name", "first_name", "gender"]]
        # Restrict to communes in the less_1000 results so we don't flag plus_1000 candidates
        less_communes = set(df_results["commune_code"].unique())
        df_reg_less = df_reg[df_reg["commune_code"].isin(less_communes)]
        print("  Joining...")
        # Join on last_name + first_name to avoid many-to-many matches when two candidates
        # in the same commune share a surname; unmatched first names fall back to gender_raw
        df_full = df_results.merge(df_reg_less, on=["commune_code", "last_name", "first_name"], how="outer", indicator=True)
        df_dropped = df_full[df_full["_merge"] == "right_only"].drop(columns=["_merge"])
        df = df_full[df_full["_merge"] != "right_only"].drop(columns=["_merge"])
        df["gender"] = df["gender"].fillna(df["gender_raw"])
        dropped_path = YEAR_DIR / f"dropped_outputs/dropped_less_1000_tour{TOUR}_{YEAR}.csv"
        dropped_path.parent.mkdir(parents=True, exist_ok=True)
        df_dropped.to_csv(dropped_path, index=False, encoding="utf-8-sig")
        df_dropped.to_json(dropped_path.with_suffix(".json"), orient="records", force_ascii=False, indent=2)
        print(f"  Dropped (registration in less_1000 commune, no result match): {len(df_dropped):,} → {dropped_path}")

    else:
        raise ValueError(f"No pipeline defined for year {YEAR}. Add one above.")

    df["elected"] = df["elected"].fillna(False).astype(bool)

    cols = [c for c in OUTPUT_COLS if c in df.columns]
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df[cols].sort_values(["commune_code"]).to_csv(
        OUT_PATH, index=False, encoding="utf-8-sig"
    )
    df[cols].sort_values(["commune_code"]).to_json(
        OUT_PATH.with_suffix(".json"), orient="records", force_ascii=False, indent=2
    )
    print(f"DONE:   {len(df):,} candidates → {OUT_PATH}")
    print(f"    Communes: {df['commune_code'].nunique():,}  |  Elected: {df['elected'].sum():,}")