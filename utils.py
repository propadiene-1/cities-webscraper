"""
utils.py — helper functions for election processing
"""

import numpy as np
import pandas as pd
from pathlib import Path


def to_int(v):
    try:
        return int(str(v).replace(" ", "").replace("\xa0", "").replace(",", ""))
    except:
        return None


def to_float(v):
    try:
        return float(str(v).replace(",", ".").replace(" ", "").replace("\xa0", ""))
    except:
        return None


def clean(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    return str(v).strip().strip('"') or None


def pad_commune_code(v):
    s = str(v).strip()
    if "SR" in s: 
        return s #excluding PLM
    return s.zfill(5)


# --- File format configs for each election years --------
# N_FIXED    = number of fixed commune-level columns before candidate blocks start
# BLOCK_SIZE = number of columns per candidate/list block
# COMMUNE_COLS = names for the fixed columns (must match N_FIXED in length)
# BLOCK      = field positions within each repeating block (0-indexed)

YEAR_CONFIGS = {
    2020: {
        "N_FIXED":    18,
        "BLOCK_SIZE": 12,
        "COMMUNE_COLS": [
            "department_code", "department_name", "commune_code", "commune_name",
            "registered_voters", "abstentions", "pct_abstentions",
            "voters", "pct_voters",
            "blank_votes", "pct_blank_of_registered", "pct_blank_of_voters",
            "null_votes", "pct_null_of_registered", "pct_null_of_voters",
            "valid_votes", "pct_valid_of_registered", "pct_valid_of_voters",
        ],
        # Block layout for plus_1000 (list-level)
        "BLOCK_MORE": {
            "list_number": 0,
            "party_code":  1,
            "list_name":   5,
            "seats_won":   6,
            "votes":       9,
        },
        # Block layout for less_1000 (candidate-level)
        "BLOCK_LESS": {
            "party_code":  1,
            "gender_raw":  2,
            "last_name":   3,
            "first_name":  4,
            "elected":     6,
            "votes":       9,
        },
    },
    2014: {
        "N_FIXED":    17,
        "BLOCK_SIZE": 11,
        # 2014 has "Date de l'export" and "Type de scrutin" as extra leading cols,
        # and merges blank+null into one column "Blancs et nuls"
        "COMMUNE_COLS": [
            "export_date", "department_code", "scrutin_type",
            "department_name", "commune_code", "commune_name",
            "registered_voters", "abstentions", "pct_abstentions",
            "voters", "pct_voters",
            "blank_null_votes", "pct_blank_null_of_registered", "pct_blank_null_of_voters",
            "valid_votes", "pct_valid_of_registered", "pct_valid_of_voters",
        ],
        "BLOCK_MORE": {
            "party_code":  0,
            "gender_raw":  1,
            "last_name":   2,
            "first_name":  3,
            "list_name":   4,
            "seats_won":   5,
            "votes":       8,
        },
        "BLOCK_LESS": {
            "party_code":  0,
            "gender_raw":  1,
            "last_name":   2,
            "first_name":  3,
            "elected":     5,
            "votes":       8,
        },
    },
}

# add new config for 2008
# YEAR_CONFIGS[2008] = { ... }


def get_config(year: int) -> dict:
    if year not in YEAR_CONFIGS:
        raise ValueError(f"No format config for year {year}. "
                         f"Available: {list(YEAR_CONFIGS.keys())}")
    return YEAR_CONFIGS[year]


def read_wide_file(path: Path, sep: str, year: int) -> pd.DataFrame:
    cfg = get_config(year)
    n_fixed = cfg["N_FIXED"]

    lines = [l for l in open(path, encoding="latin-1", errors="replace").readlines()[1:] if l.strip()]
    col_counts = [len(l.split(sep)) for l in lines]
    max_cols = max(col_counts)
    n_diff = sum(1 for c in col_counts if c != max_cols)

    print(f"  File scan: {len(lines):,} rows, col counts {min(col_counts)}–{max_cols}")
    if n_diff > 0:
        print(f"WARNING: {n_diff:,} rows shorter than max (trailing NaN fill applied)")

    df = pd.read_csv(
        path, sep=sep, encoding="latin-1",
        dtype=str, low_memory=False,
        header=None, skiprows=1,
        names=range(max_cols),
    )
    df.columns = cfg["COMMUNE_COLS"] + list(range(n_fixed, len(df.columns)))

    sr_rows = df["commune_code"].str.contains("SR", na=False).sum()
    if sr_rows > 0:
        print(f"DONE: {sr_rows:,} PLM sector rows (SR codes) loaded successfully")

    return df


def extract_commune_metadata(row: pd.Series) -> dict:
    # results file stores commune code as a local number (e.g. "4"),
    # reconstruct INSEE code as dept + commune. e.g. department "01" + commune "004" = "01004".
    dep = str(row["department_code"]).strip().zfill(2)
    com = str(row["commune_code"]).strip()
    if "SR" not in com: #excluding PLM
        com = com.zfill(3)
    return {
        "department_code":   dep,
        "department_name":   clean(row["department_name"]),
        "commune_code":      dep + com,
        "commune_name":      clean(row["commune_name"]),
        "registered_voters": to_int(row["registered_voters"]),
        "abstentions":       to_int(row["abstentions"]),
        "voters":            to_int(row.get("voters")),
        "valid_votes":       to_int(row["valid_votes"]),
        "turnout_pct":       to_float(row.get("pct_voters")),
    }


def parse_registrations(path: Path) -> pd.DataFrame:
    """
    Parse the candidate registration file.
    Same file is used for both tours — candidates don't re-register.
    Age is not published by French authorities — not available here.
    """
    df = pd.read_csv(path, sep=";", encoding="utf-8", dtype=str, on_bad_lines="warn")
    df.columns = [c.strip().strip('"') for c in df.columns]

    return pd.DataFrame({
        "commune_code": df["Insee"].apply(pad_commune_code),
        "list_number":  df["NumListe"].str.strip(),
        "last_name":    df["NomPsn"].str.strip().str.upper(),
        "first_name":   df["PrePsn"].str.strip(),
        "gender":       df["CivilitePsn"].str.strip().map({"M.": "M", "Mme": "F"}),
        "list_rank":    pd.to_numeric(df["NumOrdCand"], errors="coerce"),
        "is_list_head": df["TeteListe"].str.strip().map({"O": True, "N": False}),
    })