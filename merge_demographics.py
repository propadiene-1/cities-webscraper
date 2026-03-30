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

# --- Adjust for census / results files -----
CENSUS_YEAR = "22"   # 2-digit year: "06", "11", "16", "22"
PATCH_YEAR  = "16"   # fallback census year for unmatched communes — set to None to disable
                     # e.g. CENSUS_YEAR="16", PATCH_YEAR="11" tries 2011 if 2016 has no match
INCOME_YEAR = "22"     # income data year: "06", "11", "16", "21" — set to None to omit income columns
YEAR = "2026" #2014 or 2020
TOUR = "2"
COMMUNE_TYPE = "" #plus or less
SUFFIX = ""#"_1000_"

# --- Paths based on values ------
BASE_DIR      = Path("/Users/propadiene/cloned-repos/cities-webscraper")
ELECTION_FILE = BASE_DIR / f"france_{YEAR}/candidate_outputs/{COMMUNE_TYPE}{SUFFIX}tour{TOUR}_{YEAR}.csv"
CENSUS_FILE   = BASE_DIR / "france_census/dossier_complet.csv"
OUT_FILE      = BASE_DIR / f"new_france_joined_outputs/france_joined_{YEAR}/joined_{COMMUNE_TYPE}{SUFFIX}tour{TOUR}_{YEAR}.csv"

# --- Build census columns based on YEAR prefix ------
p = f"P{CENSUS_YEAR}_"   # e.g. "P22_"

CENSUS_COLS = [
    "CODGEO",

    f"{p}POP",              # total population

    f"{p}POPH",             # gender — male
    f"{p}POPF",             # gender — female

    f"{p}POP0014",          # age structure
    f"{p}POP1529",
    f"{p}POP3044",
    f"{p}POP4559",
    f"{p}POP6074",
    f"{p}POP7589",
    f"{p}POP90P",

    f"{p}NSCOL15P",         # total non-students 15+
    f"{p}NSCOL15P_CAPBEP",  # vocational
    f"{p}NSCOL15P_BAC",     # baccalauréat
    f"{p}NSCOL15P_SUP",     # any higher education (note: 2022 splits this into SUP2/SUP34/SUP5)
    f"{p}NSCOL15P_SUP2",    # 2022 only
    f"{p}NSCOL15P_SUP34",   # 2022 only
    f"{p}NSCOL15P_SUP5",    # 2022 only

    f"{p}CHOM1564",         # unemployed 15-64
    f"{p}ACT1564",          # active population 15-64 (denominator)

    *(
        [f"MED{INCOME_YEAR}", f"TP60{INCOME_YEAR}"]
        if INCOME_YEAR else []
    ),
]


def compute_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Compute percentage statistics from raw census counts."""
    pop = df[f"{p}POP"].replace(0, float("nan"))
    edu = df[f"{p}NSCOL15P"].replace(0, float("nan"))
    act = df[f"{p}ACT1564"].replace(0, float("nan"))

    df["pct_female"]      = df[f"{p}POPF"] / pop * 100
    df["pct_male"]        = df[f"{p}POPH"] / pop * 100

    df["pct_age_0_14"]    = df[f"{p}POP0014"] / pop * 100
    df["pct_age_15_29"]   = df[f"{p}POP1529"] / pop * 100
    df["pct_age_30_44"]   = df[f"{p}POP3044"] / pop * 100
    df["pct_age_45_59"]   = df[f"{p}POP4559"] / pop * 100
    df["pct_age_60_74"]   = df[f"{p}POP6074"] / pop * 100
    df["pct_age_75_plus"] = (df[f"{p}POP7589"] + df[f"{p}POP90P"]) / pop * 100

    df["pct_edu_vocational"] = df[f"{p}NSCOL15P_CAPBEP"] / edu * 100
    df["pct_edu_bac"]        = df[f"{p}NSCOL15P_BAC"]    / edu * 100

    if f"{p}NSCOL15P_SUP" in df.columns:
            df["pct_edu_higher"] = df[f"{p}NSCOL15P_SUP"] / edu * 100
    else:
        df["pct_edu_higher"] = ( #for 2022 sum the three higher education levels
            df[f"{p}NSCOL15P_SUP2"].fillna(0) +
            df[f"{p}NSCOL15P_SUP34"].fillna(0) +
            df[f"{p}NSCOL15P_SUP5"].fillna(0)
        ) / edu * 100

    df["pct_unemployed"] = df[f"{p}CHOM1564"] / act * 100

    return df.round(2)


def load_census(cols: list) -> pd.DataFrame:
    return pd.read_csv(
        CENSUS_FILE, sep=";", dtype={"CODGEO": str},
        usecols=lambda col: col in cols,
        low_memory=False, on_bad_lines="skip"
    )


if __name__ == "__main__":
    income_label = f"20{INCOME_YEAR}" if INCOME_YEAR else "omitted"
    print(f"Census year: 20{CENSUS_YEAR} | Patch year: {f'20{PATCH_YEAR}' if PATCH_YEAR else 'disabled'} | Income year: {income_label}")

    print("Loading election data...")
    elections = pd.read_csv(ELECTION_FILE, dtype={"commune_code": str})
    print(f"  {len(elections):,} candidates, {elections['commune_code'].nunique():,} communes")

    print("Loading census data...")
    census_raw = load_census(CENSUS_COLS)
    print(f"  {len(census_raw):,} communes loaded")

    missing = [c for c in CENSUS_COLS if c not in census_raw.columns]
    if missing:
        print(f"  Note: {len(missing)} columns not found and skipped: {missing}")

    print("Merging...")
    #get rid of PLM SR/SN suffix before merging, e.g. "13055SR01" → "13055"
    merge_code = elections["commune_code"].str.replace(r"(SR|SN)\d+$", "", regex=True)
    merged = elections.assign(merge_code=merge_code).merge(
        census_raw, left_on="merge_code", right_on="CODGEO", how="left"
    ).drop(columns=["merge_code"])

    total_rows = len(merged)
    main_matched = merged[f"{p}POP"].notna().sum()
    unmatched = merged[merged[f"{p}POP"].isna()]["commune_code"].unique()
    print(f"  Matched (20{CENSUS_YEAR} only):  {main_matched:,} / {total_rows:,} ({main_matched/total_rows*100:.1f}%)")
    if len(unmatched) > 0:
        print(f"  Unmatched after main:   {total_rows - main_matched:,} rows across {len(unmatched)} communes")

    # --- Patch fallback ---
    if PATCH_YEAR and len(unmatched) > 0:
        pp = f"P{PATCH_YEAR}_"
        # Build patch column list: swap main prefix for patch prefix (skip income cols)
        patch_cols = ["CODGEO"] + [
            c.replace(p, pp) for c in CENSUS_COLS if c.startswith(p)
        ]
        print(f"Loading patch census (20{PATCH_YEAR})...")
        patch_raw = load_census(patch_cols)

        missing_patch = [c for c in patch_cols if c not in patch_raw.columns]
        if missing_patch:
            print(f"  Note: {len(missing_patch)} patch columns not found and skipped: {missing_patch}")

        # Rename patch prefix → main prefix so compute_derived_columns works unchanged
        rename_map = {c: c.replace(pp, p) for c in patch_raw.columns if c.startswith(pp)}
        patch_raw = patch_raw.rename(columns=rename_map).set_index("CODGEO")

        unmatched_mask = merged[f"{p}POP"].isna()
        merge_codes = (
            merged.loc[unmatched_mask, "commune_code"]
            .str.replace(r"(SR|SN)\d+$", "", regex=True)
        )

        patch_fill_cols = [c for c in patch_raw.columns if c in merged.columns]
        for col in patch_fill_cols:
            merged.loc[unmatched_mask, col] = merge_codes.map(patch_raw[col]).values

        patched_mask = unmatched_mask & merged[f"{p}POP"].notna()
        patched_codes = sorted(
            merge_codes[patched_mask[unmatched_mask].values].unique()
        )
        print(f"  Patched {len(patched_codes)} communes ({patched_mask.sum():,} rows) using 20{PATCH_YEAR} census")

        total_matched = main_matched + patched_mask.sum()
        print(f"  Patched (20{PATCH_YEAR}):          {patched_mask.sum():,} rows across {len(patched_codes)} communes")
        print(f"  Matched (incl. patch):  {total_matched:,} / {total_rows:,} ({total_matched/total_rows*100:.1f}%)")
        print(f"  Still unmatched:        {total_rows - total_matched:,} rows")

        if patched_codes:
            patch_path = OUT_FILE.parent / f"patched_communes_{COMMUNE_TYPE}{SUFFIX}tour{TOUR}_{YEAR}.csv"
            pd.DataFrame({
                "commune_code": patched_codes,
                "census_year_used": f"20{PATCH_YEAR}",
            }).to_csv(patch_path, index=False, encoding="utf-8-sig")
            print(f"  Saved → {patch_path}")

    still_unmatched = merged[merged[f"{p}POP"].isna()]["commune_code"].unique()
    unmatched_path = OUT_FILE.parent / f"unmatched_communes_{COMMUNE_TYPE}{SUFFIX}tour{TOUR}_{YEAR}.txt"
    unmatched_path.parent.mkdir(parents=True, exist_ok=True)
    unmatched_path.write_text("\n".join(sorted(still_unmatched)), encoding="utf-8")
    if len(still_unmatched) > 0:
        print(f"  Unmatched communes saved → {unmatched_path}")
    else:
        print(f"  No unmatched communes — {unmatched_path} cleared")

    print("Computing derived percentages...")
    merged = compute_derived_columns(merged)

    #keep only percentages + total pop + income (no raw counts)
    keep = {"CODGEO", f"{p}POP"}
    if INCOME_YEAR:
        keep |= {f"MED{INCOME_YEAR}", f"TP60{INCOME_YEAR}"}
    raw_to_drop = [c for c in census_raw.columns if c not in keep]
    merged = merged.drop(columns=raw_to_drop + ["CODGEO"], errors="ignore")

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
    merged.to_json(OUT_FILE.with_suffix(".json"), orient="records", force_ascii=False, indent=2)

    print(f"\n✓ {len(merged):,} rows → {OUT_FILE}")
    print(f"  Columns: {list(merged.columns)}")
