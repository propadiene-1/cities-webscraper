"""
merge_demographics.py
=====================
Merge election data w/ INSEE census demographics per commune.

Using 2022 census (proxy for 2020); for other years swap out P22 prefix.

Inputs:
  - election CSV (output of process_less_1000.py or process_more_1000.py)
  - dossier complet CSV (INSEE)

Output:
  - election file w/ appended demographic columns

Join key: commune_code (elections) = CODGEO (census)

Update ELECTION_FILE, CENSUS_FILE, OUT_FILE to adjust.
"""

import pandas as pd
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = Path("/Users/propadiene/cloned-repos/cities-webscraper")
ELECTION_FILE = BASE_DIR / "france_2020/candidate_outputs/less_1000_tour1_2020.csv"
CENSUS_FILE   = BASE_DIR / "france_census/dossier_complet.csv"
OUT_FILE      = BASE_DIR / "france_2020/joined_outputs/joined_less_1000_tour1_2020.csv"

CENSUS_COLS = [
    "CODGEO",

    "P22_POP", #population

    "P22_POPH",         # gender- male population
    "P22_POPF",         # gender- female population

    "P22_POP0014", #age structure
    "P22_POP1529",
    "P22_POP3044",
    "P22_POP4559",
    "P22_POP6074",
    "P22_POP7589",
    "P22_POP90P",

    #education (population 15+ by highest diploma)
    "P22_NSCOL15P",         # total non-students 15+
    "P22_NSCOL15P_DIPLMIN", # no diploma or minimum
    "P22_NSCOL15P_BEPC",    # BEPC / brevet
    "P22_NSCOL15P_CAPBEP",  # vocational (CAP/BEP)
    "P22_NSCOL15P_BAC",     # baccalauréat
    "P22_NSCOL15P_SUP2",    # 2-year higher education (BTS/DUT)
    "P22_NSCOL15P_SUP34",   # 3-4 year higher education (licence/master)
    "P22_NSCOL15P_SUP5",    # 5+ year higher education (grande école/PhD)

    #unemployment
    "P22_CHOM1564",     # unemployed aged 15-64
    "P22_ACT1564",      # active population 15-64 (denominator for unemployment rate)

    #nationality (foreign nationality/immigrant- closest available proxy to ethnicity)
    "P22_POP_ETRG" if "P22_POP_ETRG" in [] else None,  # checked below

    "MED21",            # median income per consumption unit (2021)
    "TP6021",           # poverty rate (% below 60% median income, 2021)
]

#remove None entries (placeholder for columns we'll check at runtime)
CENSUS_COLS = [c for c in CENSUS_COLS if c is not None]


def compute_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute percentage statistics from raw census counts
    """
    pop = df["P22_POP"].replace(0, float("nan"))
    edu = df["P22_NSCOL15P"].replace(0, float("nan"))
    act = df["P22_ACT1564"].replace(0, float("nan"))

    df["pct_female"]      = df["P22_POPF"]  / pop * 100 #gender
    df["pct_male"]        = df["P22_POPH"]  / pop * 100

    df["pct_age_0_14"]    = df["P22_POP0014"] / pop * 100 #age
    df["pct_age_15_29"]   = df["P22_POP1529"] / pop * 100
    df["pct_age_30_44"]   = df["P22_POP3044"] / pop * 100
    df["pct_age_45_59"]   = df["P22_POP4559"] / pop * 100
    df["pct_age_60_74"]   = df["P22_POP6074"] / pop * 100
    df["pct_age_75_plus"] = (df["P22_POP7589"] + df["P22_POP90P"]) / pop * 100

    df["pct_edu_none"]        = df["P22_NSCOL15P_DIPLMIN"] / edu * 100 #education
    df["pct_edu_bepc"]        = df["P22_NSCOL15P_BEPC"]    / edu * 100
    df["pct_edu_vocational"]  = df["P22_NSCOL15P_CAPBEP"]  / edu * 100
    df["pct_edu_bac"]         = df["P22_NSCOL15P_BAC"]     / edu * 100
    df["pct_edu_higher_2yr"]  = df["P22_NSCOL15P_SUP2"]    / edu * 100
    df["pct_edu_higher_34yr"] = df["P22_NSCOL15P_SUP34"]   / edu * 100
    df["pct_edu_higher_5yr"]  = df["P22_NSCOL15P_SUP5"]    / edu * 100

    df["pct_unemployed"]  = df["P22_CHOM1564"] / act * 100 #unemployment

    if "P22_POP_ETRG" in df.columns: #foreign nationals (ethnicity)
        df["pct_foreign_nationals"] = df["P22_POP_ETRG"] / pop * 100

    return df.round(2)


if __name__ == "__main__":
    print("Loading election data...")
    elections = pd.read_csv(ELECTION_FILE, dtype={"commune_code": str})
    print(f"  {len(elections):,} candidates, {elections['commune_code'].nunique():,} communes")

    print("Loading census data...")
    census_raw = pd.read_csv(
        CENSUS_FILE, sep=";", dtype={"CODGEO": str},
        low_memory=False, on_bad_lines="skip"
    )
    print(f"  {len(census_raw):,} communes in census file")
    print(f"  {len(census_raw.columns):,} columns available")

    available = [c for c in CENSUS_COLS if c in census_raw.columns] #check if cols exist
    missing   = [c for c in CENSUS_COLS if c not in census_raw.columns]
    if missing:
        print(f"  Note: {len(missing)} columns not found and skipped: {missing}")

    census = census_raw[available].copy()

    print("Merging...")
    merged = elections.merge(
        census,
        left_on="commune_code",
        right_on="CODGEO",
        how="left",
    )

    matched = merged["P22_POP"].notna().sum() #check match rate (validation)
    print(f"  Matched: {matched:,} / {len(merged):,} candidates ({matched/len(merged)*100:.1f}%)")

    print("Computing derived percentage columns...")
    merged = compute_derived_columns(merged)

    #drop raw counts (keep only % and totals)
    raw_cols_to_drop = [
        c for c in available
        if c != "CODGEO" and c not in ("P22_POP", "MED21", "TP6021")
    ]
    merged = merged.drop(columns=raw_cols_to_drop, errors="ignore")
    merged = merged.drop(columns=["CODGEO"], errors="ignore") #drop merge key

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
    merged.to_json(
        OUT_FILE.with_suffix(".json"),
        orient="records", force_ascii=False, indent=2
    )

    print(f"\n✓ {len(merged):,} rows → {OUT_FILE}")
    print(f"  Final columns: {list(merged.columns)}")