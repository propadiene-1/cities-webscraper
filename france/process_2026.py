"""
process_2026.py
===============
Municipal elections 2026 — all communes use list voting.

Key differences from prior years
----------------------------------
2026 reform: list voting (scrutin de liste) applies to ALL communes regardless
of population. There is no plus_1000 / less_1000 split — a single results file
covers all ~34,800 communes.

Source files
------------
results:      france_2026/tour_{TOUR}/municipales-2026-resultats-communes-*.csv
  - semicolon-separated, UTF-8
  - one row per commune, wide format with repeating 13-column blocks per list
    (panneau, nom candidat [NULL], prénom [NULL], sexe [NULL], nuance,
     libellé abrégé, libellé, voix, %voix/inscrits, %voix/exprimés,
     elu [NULL], sièges au CM, sièges au CC)
  - 'Nom candidat' / 'Prénom candidat' / 'Sexe candidat' are NULL in results —
    names come only from the candidatures file

candidatures: france_2026/municipales-2026-candidatures-france-entiere-tour-1-*.csv
  - semicolon-separated, UTF-8
  - one row per candidate, already long format
  - join key: (Code circonscription, Numéro de panneau) = (commune_code, list_number)
  - provides: individual candidate names, list rank (Ordre), gender, party code
  - tour 1 file only — same candidates apply to tour 2

Elected status
--------------
  'Elu' column in results is NULL for all rows (not published in this format).
  elected = list_rank (Ordre) <= seats_won (Sièges au CM)
  Same logic as 2020 plus_1000.

Output
------
  One row per candidate (all communes, both tours).
  Columns match prior-year candidate_outputs:
    commune_code, commune_name, last_name, first_name, gender,
    party_code, list_name, votes, elected
"""

from pathlib import Path
import pandas as pd
from utils import clean, to_int, save_outputs

# --- Adjust tour ------
TOUR = 1   # 1 or 2

# --- Paths ------
BASE_DIR       = Path(__file__).resolve().parent
YEAR_DIR       = BASE_DIR / "france_2026"
TOUR_DIR       = YEAR_DIR / f"tour_{TOUR}"
FILE_RESULTS   = next(TOUR_DIR.glob("municipales-2026-resultats-communes-*.csv"))
FILE_CAND      = next(YEAR_DIR.glob("municipales-2026-candidatures-*.csv"))
OUT_PATH       = YEAR_DIR / f"candidate_outputs/tour{TOUR}_2026.csv"

# Results file layout
N_FIXED    = 18   # fixed commune/voter-stats columns before list blocks
BLOCK_SIZE = 13   # columns per list block
# 0-indexed positions within each block
BLOCK = {
    "panneau":    0,
    # 1: nom candidat  — always null, skipped
    # 2: prénom        — always null, skipped
    # 3: sexe          — always null, skipped
    "party_code": 4,
    "list_name":  5,   # libellé abrégé
    # 6: libellé long  — skipped (use abrégé)
    "votes":      7,
    # 8: % voix/inscrits — skipped
    # 9: % voix/exprimés — skipped
    # 10: elu          — always null
    "seats_won":  11,  # sièges au CM
    # 12: sièges au CC — skipped
}

OUTPUT_COLS = [
    "commune_code", "commune_name",
    "last_name", "first_name", "gender",
    "party_code", "list_name",
    "votes", "elected",
]


def parse_results(path: Path) -> pd.DataFrame:
    """Unpack wide results file into one row per list per commune."""
    raw = pd.read_csv(path, sep=";", dtype=str, on_bad_lines="skip")
    n_blocks = (len(raw.columns) - N_FIXED) // BLOCK_SIZE
    print(f"  {len(raw):,} communes × up to {n_blocks} list blocks")

    rows = []
    for _, row in raw.iterrows():
        commune_code = str(row.iloc[2]).strip()
        commune_name = clean(str(row.iloc[3]))

        for i in range(n_blocks):
            offset = N_FIXED + i * BLOCK_SIZE
            b = [row.iloc[offset + j] if offset + j < len(row) else None
                 for j in range(BLOCK_SIZE)]

            panneau = clean(b[BLOCK["panneau"]])
            votes   = to_int(b[BLOCK["votes"]])
            if panneau is None and votes is None:
                continue

            rows.append({
                "commune_code": commune_code,
                "commune_name": commune_name,
                "list_number":  panneau,
                "party_code":   clean(b[BLOCK["party_code"]]),
                "list_name":    clean(b[BLOCK["list_name"]]),
                "votes":        votes,
                "seats_won":    to_int(b[BLOCK["seats_won"]]),
            })

    return pd.DataFrame(rows)


def parse_candidatures(path: Path) -> pd.DataFrame:
    """Parse candidatures file into one row per candidate."""
    df = pd.read_csv(path, sep=";", dtype=str, on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]

    return pd.DataFrame({
        "commune_code": df["Code circonscription"].str.strip(),
        "list_number":  df["Numéro de panneau"].str.strip(),
        "list_rank":    pd.to_numeric(df["Ordre"], errors="coerce"),
        "is_list_head": df["Tête de liste"].str.strip().map({"OUI": True}),
        "gender":       df["Sexe"].str.strip(),
        "last_name":    df["Nom sur le bulletin de vote"].str.strip().str.upper(),
        "first_name":   df["Prénom sur le bulletin de vote"].str.strip(),
        "party_code_cand": df["Code nuance de liste"].str.strip(),
    })


if __name__ == "__main__":
    print(f"Processing 2026 tour {TOUR}")

    print("  Parsing results (list-level)...")
    df_lists = parse_results(FILE_RESULTS)
    print(f"  {len(df_lists):,} list entries across {df_lists['commune_code'].nunique():,} communes")

    print("  Parsing candidatures (candidate-level)...")
    df_cand = parse_candidatures(FILE_CAND)
    print(f"  {len(df_cand):,} candidates across {df_cand['commune_code'].nunique():,} communes")

    # For tour 2, restrict candidatures to communes that actually appear in tour 2 results
    if TOUR == 2:
        t2_communes = set(df_lists["commune_code"])
        df_cand = df_cand[df_cand["commune_code"].isin(t2_communes)]
        print(f"  Restricted to {len(df_cand):,} candidates in {len(t2_communes):,} tour 2 communes")

    print("  Joining...")
    df_full = df_cand.merge(
        df_lists,
        on=["commune_code", "list_number"],
        how="outer",
        indicator=True,
    )

    # Save dropped rows. Two flavours:
    #   left_only  — registered candidat whose list isn't in this tour's results
    #                (in tour 2: their tour-1 list didn't advance — the main
    #                source of the previous bug where 60k tour-2 rows had empty
    #                commune_name / list_name / votes).
    #   right_only — a result list with no matching candidatures rows.
    df_dropped = df_full[df_full["_merge"] != "both"].drop(columns=["_merge"])
    dropped_path = YEAR_DIR / f"dropped_outputs/dropped_tour{TOUR}_2026.csv"
    dropped_path.parent.mkdir(parents=True, exist_ok=True)
    df_dropped.to_csv(dropped_path, index=False, encoding="utf-8-sig")
    df_dropped.to_json(dropped_path.with_suffix(".json"), orient="records", force_ascii=False, indent=2)
    print(f"  Dropped (no candidat/list match): {len(df_dropped):,} → {dropped_path}")

    df = df_full[df_full["_merge"] == "both"].drop(columns=["_merge"])

    # Use candidatures party_code where available (more canonical), fall back to results
    df["party_code"] = df["party_code_cand"].combine_first(df["party_code"])

    df["votes"]     = pd.to_numeric(df["votes"], errors="coerce").astype("Int64")
    df["seats_won"] = pd.to_numeric(df["seats_won"], errors="coerce").astype("Int64")

    # elected = list_rank <= seats_won (seats fill from top of list down)
    df["elected"] = (df["list_rank"] <= df["seats_won"]).fillna(False)

    matched = df["list_rank"].notna().sum()
    print(f"  Matched candidates: {matched:,} / {len(df):,} ({matched/len(df)*100:.1f}%)")

    save_outputs(df, OUT_PATH, cols=OUTPUT_COLS, sort_by=["commune_code", "list_number", "list_rank"])
    print(f"  Communes: {df['commune_code'].nunique():,}  |  Elected: {df['elected'].sum():,}")
