"""
merge_btt_census.py
===================
Merge election data with INSEE BTT_TD_POP1A census files (population by age
and sex) plus FILOSOFI income/poverty files, and write to
newest_france_joined_outputs/.

Census file → election year mapping
------------------------------------
  BTT_TD_POP1A_2008.txt  →  2008 elections
  BTT_TD_POP1A_2014.txt  →  2014 elections
  BTT_TD_POP1A_2019.csv  →  2020 elections  (closest available proxy)
  BTT_TD_POP1A_2022.csv  →  2026 elections  (closest available proxy)

FILOSOFI vintage → election year mapping (income/poverty)
----------------------------------------------------------
  FILOSOFI 2012  →  2008 elections  (earliest vintage available)
  FILOSOFI 2014  →  2014 elections
  FILOSOFI 2019  →  2020 elections
  FILOSOFI 2021  →  2026 elections  (latest available; FILOSOFI 2022 was not
                                     released due to quality issues)

Available demographics (from BTT files)
-----------------------------------------
  P{YY}_POP     — total commune population
  pct_female / pct_male
  pct_age_0_14 … pct_age_75_plus  (approximated from BTT 10 age bands using
                                   uniform-distribution interpolation)

Available income indicators (from FILOSOFI)
--------------------------------------------
  median_income  — Q2{YY}: median disposable income per consumption unit (€)
  poverty_rate   — TP60{YY}: % of population below 60% median income threshold

  Poverty rate basis: DISP (disposable income) for 2014/2019/2021; DEC
  (declared income) for 2012 only — FILOSOFI 2012 did not publish the
  DISP_Pauvres file. The two definitions differ by ~1-2 pts on average.

NOT available from BTT files (absent in output)
------------------------------------------------
  pct_edu_vocational, pct_edu_bac, pct_edu_higher, pct_unemployed

Age band interpolation
-----------------------
BTT AGEPYR10 groups → target 15-year bands (assuming uniform distribution
within each BTT group):
  00 (0–2)    03 (3–5)    06 (6–10)    11 (11–17)    18 (18–24)
  25 (25–39)  40 (40–54)  55 (55–64)   65 (65–79)    80 (80+)

  0–14  = 00+03+06 + (4/7)×11
  15–29 = (3/7)×11 + 18 + (1/3)×25
  30–44 = (2/3)×25 + (1/3)×40
  45–59 = (2/3)×40 + (1/2)×55
  60–74 = (1/2)×55 + (2/3)×65
  75+   = (1/3)×65 + 80

Commune code crosswalk
-----------------------
  new_france_census/table_passage_annuelle_2026.xlsx maps commune codes across
  years 2003–2026. Used for the FILOSOFI income join when the election year's
  geography differs from the FILOSOFI vintage year's geography (e.g. 2026
  election codes → 2021 FILOSOFI codes: ~128 communes get translated).

Join key
---------
  election commune_code → PLM SR/SN suffix stripped → match CODGEO in BTT
  (no crosswalk needed — BTT vintage already matches the election year's
  geography for the years we use).
  For FILOSOFI: same code stripping, then crosswalk-translate from election
  year to FILOSOFI vintage year before the join.
"""

import re
import pandas as pd
from pathlib import Path

BASE            = Path(__file__).resolve().parent
DOSSIER_COMPLET = BASE / "archive/france_census/dossier_complet.csv"
BTT_DIR    = BASE / "new_france_census"
INCOME_DIR = BASE / "new_france_income"
OUT_DIR    = BASE / "newest_france_joined_outputs"

# BTT file configs: (path, encoding, CODGEO year used in crosswalk)
BTT_FILES = {
    "2008": (BTT_DIR / "BTT_TD_POP1A_2008.txt",  "latin-1", "2008"),
    "2014": (BTT_DIR / "BTT_TD_POP1A_2014.txt",  "latin-1", "2014"),
    "2019": (BTT_DIR / "BTT_TD_POP1A_2019.csv",  "utf-8",   "2019"),
    "2022": (BTT_DIR / "BTT_TD_POP1A_2022.csv",  "utf-8",   "2022"),
}

# FILOSOFI file configs per vintage:
#   disp_path     — DISP_COM file (contains median Q2{yy})
#   pauvres_path  — *_Pauvres_COM file (contains poverty rate TP60{yy})
#   vintage_yy    — 2-digit suffix on column names
#   geo_year      — CODGEO year used for crosswalk lookup
# FILOSOFI 2012 only published the DEC_Pauvres file (no DISP variant), so its
# poverty rate is computed on declared income; 2014+ uses disposable income.
INCOME_FILES = {
    "2012": {
        "disp_path":    INCOME_DIR / "extract_2012/indic-struct-distrib-revenu-communes-2012/FILO_DISP_COM.xls",
        "pauvres_path": INCOME_DIR / "extract_2012/indic-struct-distrib-revenu-communes-2012/FILO_DEC_Pauvres_COM.xls",
        "vintage_yy":   "12",
        "geo_year":     "2012",
    },
    "2014": {
        "disp_path":    INCOME_DIR / "extract_2014/indic-struct-distrib-revenu-2014-COMMUNES/FILO_DISP_COM.xls",
        "pauvres_path": INCOME_DIR / "extract_2014/indic-struct-distrib-revenu-2014-COMMUNES/FILO_DISP_Pauvres_COM.xls",
        "vintage_yy":   "14",
        "geo_year":     "2014",
    },
    "2019": {
        "disp_path":    INCOME_DIR / "extract_2019/FILO2019_DISP_COM.xlsx",
        "pauvres_path": INCOME_DIR / "extract_2019/FILO2019_DISP_Pauvres_COM.xlsx",
        "vintage_yy":   "19",
        "geo_year":     "2019",
    },
    "2021": {
        "disp_path":    INCOME_DIR / "extract_2021/FILO2021_DISP_COM.xlsx",
        "pauvres_path": INCOME_DIR / "extract_2021/FILO2021_DISP_PAUVRES_COM.xlsx",
        "vintage_yy":   "21",
        "geo_year":     "2021",
    },
}

# Election year → (BTT key, census_year [unused, kept for tuple stability],
#                  pop column name, FILOSOFI vintage)
YEAR_CONFIG = {
    "2008": ("2008", "2008", "P08_POP", "2012"),
    "2014": ("2014", "2014", "P14_POP", "2014"),
    "2020": ("2019", "2019", "P19_POP", "2019"),
    "2026": ("2022", "2022", "P22_POP", "2021"),
}

# Input files: (election_year, label, absolute path to candidate_outputs CSV)
INPUT_FILES = [
    ("2008", "plus_1000_tour1",  BASE / "france_2008/candidate_outputs/plus_1000_tour1_2008.csv"),
    ("2008", "plus_1000_tour2",  BASE / "france_2008/candidate_outputs/plus_1000_tour2_2008.csv"),
    ("2014", "plus_1000_tour1",  BASE / "france_2014/candidate_outputs/plus_1000_tour1_2014.csv"),
    ("2014", "plus_1000_tour2",  BASE / "france_2014/candidate_outputs/plus_1000_tour2_2014.csv"),
    ("2014", "less_1000_tour1",  BASE / "france_2014/candidate_outputs/less_1000_tour1_2014.csv"),
    ("2014", "less_1000_tour2",  BASE / "france_2014/candidate_outputs/less_1000_tour2_2014.csv"),
    ("2020", "plus_1000_tour1",  BASE / "france_2020/candidate_outputs/plus_1000_tour1_2020.csv"),
    ("2020", "plus_1000_tour2",  BASE / "france_2020/candidate_outputs/plus_1000_tour2_2020.csv"),
    ("2020", "less_1000_tour1",  BASE / "france_2020/candidate_outputs/less_1000_tour1_2020.csv"),
    ("2020", "less_1000_tour2",  BASE / "france_2020/candidate_outputs/less_1000_tour2_2020.csv"),
    ("2026", "tour1",            BASE / "france_2026/candidate_outputs/tour1_2026.csv"),
    ("2026", "tour2",            BASE / "france_2026/candidate_outputs/tour2_2026.csv"),
]

ELECTION_COLS = [
    "commune_code", "commune_name",
    "last_name", "first_name", "gender",
    "party_code", "list_name",
    "votes", "elected",
]


# ---------------------------------------------------------------------------
# Census loading & pivoting
# ---------------------------------------------------------------------------

def load_btt(btt_key: str) -> pd.DataFrame:
    """Load BTT file and pivot to one row per commune with pop/gender/age cols."""
    path, enc, _ = BTT_FILES[btt_key]
    df = pd.read_csv(path, sep=";", dtype=str, encoding=enc, low_memory=False)
    df["NB"] = pd.to_numeric(df["NB"], errors="coerce").fillna(0)

    # Normalise column names (2008 uses different names)
    df = df.rename(columns={
        "NIVEAU":     "NIVGEO",
        "C_SEXE":     "SEXE",
        "C_AGEPYR10": "AGEPYR10",
    })

    df = df[df["NIVGEO"] == "COM"].copy()

    total_pop  = df.groupby("CODGEO")["NB"].sum()
    gender_pop = df.groupby(["CODGEO", "SEXE"])["NB"].sum().unstack(fill_value=0)
    # SEXE 1=male, 2=female
    gender_pop = gender_pop.rename(columns={"1": "POP_M", "2": "POP_F"})
    for col in ["POP_M", "POP_F"]:
        if col not in gender_pop.columns:
            gender_pop[col] = 0.0

    age_pop = df.groupby(["CODGEO", "AGEPYR10"])["NB"].sum().unstack(fill_value=0)
    age_pop.columns = [f"AGE_{c}" for c in age_pop.columns]
    # Ensure all 10 age group columns exist
    for grp in ["00", "03", "06", "11", "18", "25", "40", "55", "65", "80"]:
        if f"AGE_{grp}" not in age_pop.columns:
            age_pop[f"AGE_{grp}"] = 0.0

    result = pd.concat([total_pop.rename("POP_TOTAL"), gender_pop, age_pop], axis=1).reset_index()
    print(f"    BTT {btt_key}: {len(result):,} communes loaded")
    return result


def compute_derived(btt: pd.DataFrame, pop_col: str) -> pd.DataFrame:
    """Compute percentage columns from pivoted BTT data."""
    pop = btt["POP_TOTAL"].replace(0, float("nan"))

    btt[pop_col]          = btt["POP_TOTAL"]
    btt["pct_female"]     = btt["POP_F"] / pop * 100
    btt["pct_male"]       = btt["POP_M"] / pop * 100

    # Age interpolation (uniform distribution within BTT groups)
    a = {g: btt[f"AGE_{g}"] for g in ["00","03","06","11","18","25","40","55","65","80"]}
    btt["pct_age_0_14"]   = (a["00"] + a["03"] + a["06"] + (4/7)  * a["11"]) / pop * 100
    btt["pct_age_15_29"]  = ((3/7)  * a["11"] + a["18"] + (1/3)  * a["25"]) / pop * 100
    btt["pct_age_30_44"]  = ((2/3)  * a["25"] + (1/3)  * a["40"]) / pop * 100
    btt["pct_age_45_59"]  = ((2/3)  * a["40"] + (1/2)  * a["55"]) / pop * 100
    btt["pct_age_60_74"]  = ((1/2)  * a["55"] + (2/3)  * a["65"]) / pop * 100
    btt["pct_age_75_plus"]= ((1/3)  * a["65"] + a["80"]) / pop * 100

    pct_cols = [pop_col, "pct_female", "pct_male",
                "pct_age_0_14", "pct_age_15_29", "pct_age_30_44",
                "pct_age_45_59", "pct_age_60_74", "pct_age_75_plus"]
    btt[pct_cols] = btt[pct_cols].round(2)
    return btt[["CODGEO"] + pct_cols]


# ---------------------------------------------------------------------------
# FILOSOFI income loading
# ---------------------------------------------------------------------------

_income_cache: dict[str, pd.DataFrame] = {}

def load_income(vintage_key: str) -> pd.DataFrame:
    """Load FILOSOFI median income and poverty rate for a vintage.
    Returns one row per CODGEO with columns: median_income, poverty_rate."""
    if vintage_key in _income_cache:
        return _income_cache[vintage_key]

    cfg     = INCOME_FILES[vintage_key]
    yy      = cfg["vintage_yy"]
    med_col = f"Q2{yy}"
    pov_col = f"TP60{yy}"

    # calamine engine is more lenient with INSEE's older xls and the 2021 xlsx
    # (openpyxl rejects FILOSOFI 2021's non-standard stylesheet colours).
    engine = "calamine" if cfg["disp_path"].suffix == ".xlsx" else None
    disp = pd.read_excel(cfg["disp_path"], sheet_name="ENSEMBLE", header=5,
                         dtype={"CODGEO": str}, engine=engine)
    pauv = pd.read_excel(cfg["pauvres_path"], sheet_name="ENSEMBLE", header=5,
                         dtype={"CODGEO": str}, engine=engine)

    disp = disp[["CODGEO", med_col]].rename(columns={med_col: "median_income"})
    pauv = pauv[["CODGEO", pov_col]].rename(columns={pov_col: "poverty_rate"})

    df = disp.merge(pauv, on="CODGEO", how="outer")
    # FILOSOFI 2021 stores numbers as strings with French decimal separators
    # (e.g. "17,0") and "s" for suppressed values; normalise both before parsing.
    for col in ("median_income", "poverty_rate"):
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["median_income"] = df["median_income"].round(0)
    df["poverty_rate"]  = df["poverty_rate"].round(1)

    print(f"    FILOSOFI {vintage_key}: {len(df):,} communes "
          f"(median: {df['median_income'].notna().sum():,}, "
          f"poverty: {df['poverty_rate'].notna().sum():,})")
    _income_cache[vintage_key] = df
    return df


# ---------------------------------------------------------------------------
# dossier_complet fallback (2008 only — BTT 2008 suppresses small communes)
# ---------------------------------------------------------------------------

_dossier_cache = None

def load_dossier_complet_2008() -> pd.DataFrame:
    """
    Load the P11 columns from dossier_complet.csv and derive the same
    percentage columns as compute_derived(), but using the wide-format
    dossier_complet structure (same logic as merge_demographics.py P11 path).
    Output: one row per commune with CODGEO + P08_POP + pct_* columns.
    Note: P11_POP is used as a proxy for 2008 (same census product, closest year).
    """
    global _dossier_cache
    if _dossier_cache is not None:
        return _dossier_cache

    p = "P11_"
    cols_needed = [
        "CODGEO",
        f"{p}POP", f"{p}POPH", f"{p}POPF",
        f"{p}POP0014", f"{p}POP1529", f"{p}POP3044",
        f"{p}POP4559", f"{p}POP6074", f"{p}POP7589", f"{p}POP90P",
    ]
    df = pd.read_csv(
        DOSSIER_COMPLET, sep=";", dtype={"CODGEO": str},
        usecols=lambda c: c in cols_needed,
        low_memory=False, on_bad_lines="skip",
    )
    for col in cols_needed[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    pop = df[f"{p}POP"].replace(0, float("nan"))
    df["P08_POP"]        = df[f"{p}POP"]
    df["pct_female"]     = df[f"{p}POPF"]  / pop * 100
    df["pct_male"]       = df[f"{p}POPH"]  / pop * 100
    df["pct_age_0_14"]   = df[f"{p}POP0014"] / pop * 100
    df["pct_age_15_29"]  = df[f"{p}POP1529"] / pop * 100
    df["pct_age_30_44"]  = df[f"{p}POP3044"] / pop * 100
    df["pct_age_45_59"]  = df[f"{p}POP4559"] / pop * 100
    df["pct_age_60_74"]  = df[f"{p}POP6074"] / pop * 100
    df["pct_age_75_plus"]= (df[f"{p}POP7589"] + df[f"{p}POP90P"]) / pop * 100

    pct_cols = ["P08_POP", "pct_female", "pct_male",
                "pct_age_0_14", "pct_age_15_29", "pct_age_30_44",
                "pct_age_45_59", "pct_age_60_74", "pct_age_75_plus"]
    df[pct_cols] = df[pct_cols].round(2)
    _dossier_cache = df[["CODGEO"] + pct_cols].copy()
    print(f"    dossier_complet (P11 proxy for 2008): {len(_dossier_cache):,} communes loaded")
    return _dossier_cache


# ---------------------------------------------------------------------------
# Crosswalk loading
# ---------------------------------------------------------------------------

_crosswalk_cache = None

def load_crosswalk() -> pd.DataFrame:
    global _crosswalk_cache
    if _crosswalk_cache is None:
        xl = pd.read_excel(
            BTT_DIR / "table_passage_annuelle_2026.xlsx",
            header=5, dtype=str  # row 5 has machine-readable names (NIVGEO, CODGEO_2003, ...)
        )
        _crosswalk_cache = xl  # keep all rows; filter per-lookup in crosswalk_lookup()
        print(f"  Crosswalk loaded: {len(_crosswalk_cache):,} rows")
    return _crosswalk_cache


def crosswalk_lookup(codes: pd.Series, from_year: str, to_year: str) -> pd.Series:
    """Map commune codes from one year's geography to another using the crosswalk."""
    cw = load_crosswalk()
    from_col = f"CODGEO_{from_year}"
    to_col   = f"CODGEO_{to_year}"
    if from_col not in cw.columns or to_col not in cw.columns:
        print(f"  WARNING: crosswalk columns {from_col} or {to_col} not found")
        return pd.Series([None] * len(codes), index=codes.index)
    if from_col == to_col:
        # Same year: identity mapping — codes are already in the right geography
        return codes.copy()
    mapping = (
        cw.dropna(subset=[from_col, to_col])
        .drop_duplicates(subset=[from_col])
        .set_index(from_col)[to_col]
    )
    return codes.map(mapping)


# ---------------------------------------------------------------------------
# Main merge function
# ---------------------------------------------------------------------------

def process_file(election_year: str, label: str, in_path: Path):
    out_subdir = OUT_DIR / f"france_joined_{election_year}"
    out_path   = out_subdir / f"joined_{in_path.stem}.csv"

    print(f"\n{'='*60}")
    print(f"  {election_year} {label}")
    print(f"  Input:  {in_path}")
    print(f"  Output: {out_path}")

    btt_key, _, pop_col, income_key = YEAR_CONFIG[election_year]
    income_geo_year = INCOME_FILES[income_key]["geo_year"]

    # --- Load election data ---
    elections = pd.read_csv(in_path, dtype={"commune_code": str}, low_memory=False)
    total_rows = len(elections)
    election_cols_present = [c for c in ELECTION_COLS if c in elections.columns]
    df = elections[election_cols_present].copy()
    print(f"  Election rows: {total_rows:,}  |  Communes: {df['commune_code'].nunique():,}")

    # --- Load & derive census stats ---
    # 2008: BTT file suppresses small communes (NB=0); use dossier_complet P11 instead
    if election_year == "2008":
        btt = load_dossier_complet_2008()
    else:
        btt_raw = load_btt(btt_key)
        btt     = compute_derived(btt_raw, pop_col)

    # --- Primary join (strip PLM SR/SN suffix) ---
    merge_code = df["commune_code"].str.replace(r"(SR|SN)\d+$", "", regex=True)
    merged = df.assign(merge_code=merge_code).merge(
        btt, left_on="merge_code", right_on="CODGEO", how="left"
    ).drop(columns=["merge_code", "CODGEO"])

    main_matched = merged[pop_col].notna().sum()
    unmatched_codes = merged[merged[pop_col].isna()]["commune_code"].unique()
    print(f"  Matched (direct):  {main_matched:,} / {total_rows:,} "
          f"({main_matched/total_rows*100:.1f}%)")
    if len(unmatched_codes) > 0:
        print(f"  Unmatched after direct join: {total_rows - main_matched:,} rows "
              f"across {len(unmatched_codes):,} communes")

    # --- Save unmatched list ---
    out_subdir.mkdir(parents=True, exist_ok=True)
    unmatched_path = out_subdir / f"unmatched_communes_{label}.txt"
    unmatched_path.write_text("\n".join(sorted(unmatched_codes)), encoding="utf-8")
    if len(unmatched_codes) > 0:
        print(f"  Unmatched communes → {unmatched_path}")
    else:
        print(f"  No unmatched communes — {unmatched_path} cleared")

    # --- FILOSOFI income join (median_income, poverty_rate) ---
    income = load_income(income_key).set_index("CODGEO")
    base_code = merged["commune_code"].str.replace(r"(SR|SN)\d+$", "", regex=True)
    translated = crosswalk_lookup(base_code, election_year, income_geo_year)
    income_key_s = translated.where(translated.notna(), base_code)
    merged["median_income"] = income_key_s.map(income["median_income"])
    merged["poverty_rate"]  = income_key_s.map(income["poverty_rate"])
    income_matched = merged["median_income"].notna().sum()
    print(f"  FILOSOFI {income_key} matched: {income_matched:,} / {total_rows:,} "
          f"({income_matched/total_rows*100:.1f}%)")

    # --- Coerce count columns to nullable Int64 so CSV/JSON don't write "2887.0" ---
    for col in ("votes", pop_col, "median_income"):
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").round().astype("Int64")

    # --- Write output ---
    merged = merged.drop(columns=["CODGEO"], errors="ignore")
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    merged.to_json(out_path.with_suffix(".json"), orient="records",
                   force_ascii=False, indent=2)
    print(f"  Written: {len(merged):,} rows → {out_path}")
    print(f"  Columns: {list(merged.columns)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("merge_btt_census.py — merging BTT population census into election data")
    print(f"Output directory: {OUT_DIR}\n")

    for election_year, label, in_path in INPUT_FILES:
        process_file(election_year, label, in_path)

    print("\n\nDone.")
