"""
process_2008.py
===============
Communes of 1,000+ (list voting) — 2008 municipal elections.
One row per list (list-head only — source provides one candidate per list).
Update TOUR to adjust.

Source files
------------
results:   france_2008/municipales-2008-résultats-bureaux_vote-tour{TOUR}.csv
  - comma-separated, UTF-8
  - one row per bureau de vote (polling station)
  - each row: 17 fixed commune/bureau columns + N repeating 9-column blocks
    (block fields: party_code, gender, last_name, first_name, list_name,
                   seats_won, votes, pct_voix_ins, pct_voix_exp)
  - NOTE: seats_won (Sieges) is 0 for every row — seat allocation is not
    recorded at the bureau level. Elected status cannot be derived from this
    field; see 'elected' note below.

candidats: france_2008/municipales-2008-candidats-tour{TOUR}.csv
  - comma-separated, UTF-8
  - one row per list (list head only — no individual candidate ranks)
  - used to enrich results with: full list name (results file truncates names),
    panel number (for sorting), and canonical party code
  - joined to results on (commune_code, last_name, first_name) because the
    results file has no list panel number — name is the only shared identifier
  - ~89% match rate; unmatched rows retain values from results blocks

Elected status — DATA MISSING
------------------------------
The Sieges (seats won) field is 0 for every bureau row in both tours. Seat
allocation data is simply not stored in the bureau-level file format. As a
result, 'elected' CANNOT be determined from the available 2008 source data.

  elected = None  (NaN in output — flagged as unknown for all rows)

This is unlike:
  2014: elected = seats_won > 0  (commune-level results include seat data)
  2020: elected = list_rank <= seats_won  (seat data + individual candidate ranks)

If you locate a separate 2008 commune-level results file with seat allocation,
elected can be back-filled by joining on commune_code + list_name.

Census note (for merge_demographics.py)
-----------------------------------------
  CENSUS_YEAR = "11"  — 2011 census, the oldest vintage in dossier_complet.csv
                         with full age/gender/education/employment breakdowns.
                         P06 only contains total population + housing columns.
  INCOME_YEAR         — Only MED21/TP6021 (2021) exist in dossier_complet.csv.
                         No P06 or P11 income data is available in that file.
                         Leave INCOME_YEAR = "21" with the caveat that income
                         reflects 2021 conditions, not 2008.
"""

import csv
from pathlib import Path

import pandas as pd

from utils import clean, to_int, save_outputs

# --- Adjust tour ------
TOUR = 1  # 1 or 2

# --- Paths ------
BASE_DIR       = Path(__file__).resolve().parent
YEAR_DIR       = BASE_DIR / "france_2008"
FILE_RESULTS   = YEAR_DIR / f"municipales-2008-résultats-bureaux_vote-tour{TOUR}.csv"
FILE_CANDIDATS = YEAR_DIR / f"municipales-2008-candidats-tour{TOUR}.csv"
OUT_PATH       = YEAR_DIR / f"candidate_outputs/plus_1000_tour{TOUR}_2008.csv"

# Results file layout
N_FIXED    = 17  # fixed commune/bureau columns before candidate blocks
BLOCK_SIZE = 9   # columns per candidate/list block
# 0-indexed positions within each block
BLOCK = {
    "party_code": 0,
    "gender":     1,
    "last_name":  2,
    "first_name": 3,
    "list_name":  4,
    # index 5 = seats_won — always 0 in this file format, see docstring
    "votes":      6,
}

OUTPUT_COLS = [
    "commune_code", "commune_name",
    "last_name", "first_name", "gender",
    "party_code", "list_name",
    "votes", "elected",
]


def build_commune_code(dept: str, comm: str) -> str:
    """Reconstruct 5-digit INSEE commune code from dept + commune fields."""
    dept = str(dept).strip()
    comm = str(comm).strip()
    if not dept.isdigit():      # overseas depts: ZA, ZB, etc. — pad commune only
        return dept + comm.zfill(3)
    return dept.zfill(2) + comm.zfill(3)


def parse_results(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse bureau-level results and aggregate to commune level.

    Returns:
      df_lists    — one row per (commune_code, last_name, first_name)
                    with aggregated votes; last_name/first_name are the list head
      df_communes — one row per commune with aggregated voter stats
    """
    bureau_stats = []
    list_rows    = []

    with open(path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if not any(row):
                continue

            dept_code = row[1]
            comm_code = row[3]
            comm_name = row[4]
            dept_name = row[2]

            commune_code = build_commune_code(dept_code, comm_code)
            dept_out = dept_code.strip().zfill(2) if dept_code.strip().isdigit() else dept_code.strip()

            bureau_stats.append({
                "commune_code":      commune_code,
                "commune_name":      clean(comm_name),
                "department_code":   dept_out,
                "department_name":   clean(dept_name),
                "registered_voters": to_int(row[6]),
                "abstentions":       to_int(row[7]),
                "voters":            to_int(row[9]),
                "blank_null_votes":  to_int(row[11]),
                "valid_votes":       to_int(row[14]),
            })

            n_blocks = (len(row) - N_FIXED) // BLOCK_SIZE
            for i in range(n_blocks):
                offset = N_FIXED + i * BLOCK_SIZE
                b = row[offset : offset + BLOCK_SIZE]
                if len(b) < BLOCK_SIZE:
                    continue

                votes     = to_int(b[BLOCK["votes"]])
                last_name = clean(b[BLOCK["last_name"]])
                if votes is None and last_name is None:
                    continue

                list_rows.append({
                    "commune_code": commune_code,
                    "last_name":    last_name.upper() if last_name else None,
                    "first_name":   clean(b[BLOCK["first_name"]]),
                    "list_name":    clean(b[BLOCK["list_name"]]),
                    "party_code":   clean(b[BLOCK["party_code"]]),
                    "gender":       clean(b[BLOCK["gender"]]),
                    "votes":        votes,
                })

    # Aggregate voter stats to commune level (each bureau appears once per row)
    df_bureaux  = pd.DataFrame(bureau_stats)
    df_communes = (
        df_bureaux
        .groupby("commune_code", as_index=False)
        .agg({
            "commune_name":      "first",
            "department_code":   "first",
            "department_name":   "first",
            "registered_voters": "sum",
            "abstentions":       "sum",
            "voters":            "sum",
            "blank_null_votes":  "sum",
            "valid_votes":       "sum",
        })
    )
    df_communes["turnout_pct"] = (
        df_communes["voters"] / df_communes["registered_voters"] * 100
    ).round(2)

    # Aggregate list votes to commune level
    # Group by (commune, list head name) — list names are truncated in the results
    # file so names are the only reliable identifier within a commune
    df_lists = (
        pd.DataFrame(list_rows)
        .groupby(["commune_code", "last_name", "first_name"], as_index=False)
        .agg({
            "list_name":  "first",
            "party_code": "first",
            "gender":     "first",
            "votes":      "sum",
        })
    )

    return df_lists, df_communes


def parse_candidats(path: Path) -> pd.DataFrame:
    """
    Parse candidats file (list heads only — one row per list).
    Provides: full list name, panel number, and canonical party code.
    """
    df = pd.read_csv(path, dtype=str, encoding="utf-8")
    df.columns = [c.strip() for c in df.columns]

    commune_code = df.apply(
        lambda r: build_commune_code(r["Code département"], r["Code commune"]),
        axis=1,
    )

    return pd.DataFrame({
        "commune_code":    commune_code,
        "last_name":       df["Nom"].str.strip().str.upper(),
        "first_name":      df["Prénom"].str.strip(),
        "list_panel":      df["N° Panneau Liste"].str.strip(),
        "list_name_full":  df["Libellé abrégé liste"].str.strip(),
        "party_code_cand": df["Nuance Liste"].str.strip(),
        "gender_cand":     df["Sexe"].str.strip(),
    })


if __name__ == "__main__":
    print(f"Processing 2008 tour {TOUR}")

    print("  Parsing results (bureau-level → commune-level)...")
    df_lists, df_communes = parse_results(FILE_RESULTS)
    print(f"  {len(df_communes):,} communes, {len(df_lists):,} list entries")

    print("  Parsing candidats (list heads)...")
    df_cands = parse_candidats(FILE_CANDIDATS)
    print(f"  {len(df_cands):,} entries in candidats file")

    # Join on (commune_code, last_name, first_name)
    # Reason: results file has no list panel number — name is the only shared key.
    # Candidats file provides: full list name (results truncates), panel number,
    # and cross-validated party code / gender.
    # ~89% match; unmatched rows keep values from results blocks.
    print("  Enriching from candidats (join on commune_code + name)...")
    df_full = df_lists.merge(
        df_cands,
        on=["commune_code", "last_name", "first_name"],
        how="outer",
        indicator=True,
    )
    # Save candidats rows with no matching results entry
    df_dropped = df_full[df_full["_merge"] == "right_only"].drop(columns=["_merge"])
    dropped_path = YEAR_DIR / f"dropped_outputs/dropped_plus_1000_tour{TOUR}_2008.csv"
    dropped_path.parent.mkdir(parents=True, exist_ok=True)
    df_dropped.to_csv(dropped_path, index=False, encoding="utf-8-sig")
    df_dropped.to_json(dropped_path.with_suffix(".json"), orient="records", force_ascii=False, indent=2)
    print(f"  Dropped (candidats with no results match): {len(df_dropped):,} → {dropped_path}")

    df = df_full[df_full["_merge"] != "right_only"].drop(columns=["_merge"])
    # Use candidats values where available (cleaner data), fall back to results
    df["list_name"]  = df["list_name_full"].combine_first(df["list_name"])
    df["party_code"] = df["party_code_cand"].combine_first(df["party_code"])
    df["gender"]     = df["gender_cand"].combine_first(df["gender"])

    matched = df["list_panel"].notna().sum()
    print(f"  Matched: {matched:,} / {len(df):,} ({matched / len(df) * 100:.1f}%)")
    if len(df) - matched > 0:
        sample = df[df["list_panel"].isna()]["commune_code"].unique()[:5]
        print(f"  Unmatched (no candidats entry): {len(df) - matched:,} — sample communes: {sample}")

    # Attach commune-level voter stats
    df = df.merge(df_communes, on="commune_code", how="left")

    # elected = None — seat allocation data is absent from bureau-level source files.
    # See module docstring for details and comparison with 2014/2020.
    df["elected"] = None

    df["votes"] = df["votes"].astype("Int64")
    save_outputs(df, OUT_PATH, cols=OUTPUT_COLS, sort_by=["commune_code"])
    print(f"  Communes: {df['commune_code'].nunique():,}")
    print()
    print("  NOTE: elected = None for all rows.")
    print("  The Sieges field is 0 throughout the bureau-level source files.")
    print("  Seat allocation data is not available in the 2008 source files.")
